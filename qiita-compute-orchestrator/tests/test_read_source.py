"""Unit tests for the shared read-binding seam (`read_source.bind_step_reads`).

Both branches bind the SAME relation name so a job body is source-agnostic; the
tests pin that, plus the two properties the module note calls load-bearing:

* the Parquet branch binds a LAZY VIEW (materializing a whole sample would
  reintroduce the memory-scales-with-input shape that OOM-killed PacBio ingest);
* the stream branch MATERIALIZES (a Flight reader is consumed once, and miint
  resolves relation names on a separate connection).
"""

from contextlib import asynccontextmanager
from pathlib import Path

import duckdb
import pytest

from qiita_compute_orchestrator import read_source
from qiita_compute_orchestrator.read_source import READS_RELATION, bind_step_reads

_COLUMNS = "prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2"


def _job_conn() -> duckdb.DuckDBPyConnection:
    """A connection configured the way every real caller's is.

    `bind_step_reads` writes with the shared PARQUET_OPTS, whose
    ROW_GROUP_SIZE_BYTES DuckDB rejects while preserving insertion order — the
    jobs get `preserve_insertion_order=false` from `apply_duckdb_settings`. Set
    it here too rather than testing against a connection no job ever uses.
    """
    conn = duckdb.connect()
    conn.execute("SET preserve_insertion_order = false")
    return conn


def _write_reads_parquet(dest: Path, rows: int) -> None:
    """Write a reads Parquet in the data plane's export column shape."""
    with _job_conn() as conn:
        conn.execute(
            f"COPY (SELECT i::BIGINT AS prep_sample_idx, i::BIGINT AS sequence_idx,"
            f"      ('r' || i) AS read_id, 'ACGT' AS sequence1, 'IIII' AS qual1,"
            f"      NULL::VARCHAR AS sequence2, NULL::VARCHAR AS qual2"
            f" FROM range({rows}) t(i)) TO '{dest}' (FORMAT PARQUET)"
        )


async def test_parquet_branch_binds_a_lazy_view(tmp_path):
    """A staged Parquet binds as a VIEW, not a TABLE — the difference is whether
    peak memory scales with the sample."""
    reads = tmp_path / "reads.parquet"
    _write_reads_parquet(reads, 5)

    with _job_conn() as conn:
        async with bind_step_reads(
            conn, reads=reads, work_ticket_idx=1, workspace=tmp_path / "ws"
        ) as rel:
            assert rel == READS_RELATION
            kind = conn.execute(
                "SELECT table_type FROM information_schema.tables WHERE table_name = ?",
                [rel],
            ).fetchone()
            assert kind[0] == "VIEW", f"expected a lazy view, got {kind[0]}"
            assert conn.execute(f"SELECT count(*) FROM {rel}").fetchone()[0] == 5
            # Source-agnostic column shape.
            assert conn.execute(f"SELECT {_COLUMNS} FROM {rel} LIMIT 1").fetchone() is not None
        # Cleaned up on exit.
        assert (
            conn.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
                [READS_RELATION],
            ).fetchone()[0]
            == 0
        )


async def test_missing_parquet_fails_fast(tmp_path):
    """Fail before any DuckDB work rather than surfacing a confusing bind error."""
    with _job_conn() as conn, pytest.raises(FileNotFoundError):
        async with bind_step_reads(
            conn,
            reads=tmp_path / "absent.parquet",
            work_ticket_idx=1,
            workspace=tmp_path / "ws",
        ):
            pass


async def test_stream_branch_drains_to_a_local_parquet(monkeypatch, tmp_path):
    """No staged Parquet ⇒ stream the block, and MATERIALIZE it: a Flight reader
    is single-consumption, and miint resolves names on a separate connection."""
    source = tmp_path / "streamed.parquet"
    _write_reads_parquet(source, 3)
    captured: dict = {}

    @asynccontextmanager
    async def fake_stream(conn, *, work_ticket_idx, relation):
        captured["work_ticket_idx"] = work_ticket_idx
        # Stand in for the registered Flight reader with a real relation.
        conn.execute(f"CREATE VIEW {relation} AS SELECT * FROM read_parquet('{source}')")
        yield relation
        conn.execute(f"DROP VIEW IF EXISTS {relation}")

    monkeypatch.setattr(read_source, "open_read_block_stream", fake_stream)

    with _job_conn() as conn:
        async with bind_step_reads(
            conn, reads=None, work_ticket_idx=4834, workspace=tmp_path / "ws"
        ) as rel:
            assert rel == READS_RELATION
            kind = conn.execute(
                "SELECT table_type FROM information_schema.tables WHERE table_name = ?",
                [rel],
            ).fetchone()
            # A VIEW over the drained Parquet, NOT a base table: the block is
            # never held in DuckDB's heap (see the module note — a 10M-read block
            # against an 8 GB cap).
            assert kind[0] == "VIEW", f"the stream must be drained to disk, got {kind[0]}"
            # Drained inside the stream context, so it survives the reader
            # closing — and is re-scannable, which a raw stream is not.
            assert conn.execute(f"SELECT count(*) FROM {rel}").fetchone()[0] == 3
            assert conn.execute(f"SELECT count(*) FROM {rel}").fetchone()[0] == 3
        assert (
            conn.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
                [READS_RELATION],
            ).fetchone()[0]
            == 0
        )

    assert captured["work_ticket_idx"] == 4834


async def test_empty_stream_is_not_an_error(monkeypatch, tmp_path):
    """A completed mask can carry 0 passing reads (a blank/control, or a fully
    host/QC-filtered sample). A zero-row Arrow stream still carries its schema,
    so the job must bind a valid empty relation and run to a clean no-op."""
    source = tmp_path / "empty.parquet"
    _write_reads_parquet(source, 0)

    @asynccontextmanager
    async def fake_stream(conn, *, work_ticket_idx, relation):
        conn.execute(f"CREATE VIEW {relation} AS SELECT * FROM read_parquet('{source}')")
        yield relation
        conn.execute(f"DROP VIEW IF EXISTS {relation}")

    monkeypatch.setattr(read_source, "open_read_block_stream", fake_stream)

    with _job_conn() as conn:
        async with bind_step_reads(
            conn, reads=None, work_ticket_idx=1, workspace=tmp_path / "ws"
        ) as rel:
            assert conn.execute(f"SELECT count(*) FROM {rel}").fetchone()[0] == 0
            assert conn.execute(f"SELECT {_COLUMNS} FROM {rel}").fetchall() == []


async def test_stream_spill_file_is_removed_even_on_failure(monkeypatch, tmp_path):
    """The drained Parquet must not survive the binding.

    It lives in the job workspace, and the SLURM launcher's manifest walker scans
    that workspace for `*.parquet` outputs AFTER execute() returns — a leftover
    would be promoted as a step result. Cleanup is in a `finally`, so it holds on
    the failure path too.
    """
    source = tmp_path / "streamed.parquet"
    _write_reads_parquet(source, 3)
    ws = tmp_path / "ws"

    @asynccontextmanager
    async def fake_stream(conn, *, work_ticket_idx, relation):
        conn.execute(f"CREATE VIEW {relation} AS SELECT * FROM read_parquet('{source}')")
        yield relation
        conn.execute(f"DROP VIEW IF EXISTS {relation}")

    monkeypatch.setattr(read_source, "open_read_block_stream", fake_stream)

    with _job_conn() as conn:
        with pytest.raises(RuntimeError, match="boom"):
            async with bind_step_reads(conn, reads=None, work_ticket_idx=1, workspace=ws):
                raise RuntimeError("boom")
        assert list(ws.glob("*.parquet")) == [], "spill file leaked into the workspace"
