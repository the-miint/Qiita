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


def _write_reads_parquet(dest: Path, rows: int) -> None:
    """Write a reads Parquet in the data plane's export column shape."""
    with duckdb.connect() as conn:
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

    with duckdb.connect() as conn:
        async with bind_step_reads(conn, reads=reads, work_ticket_idx=1) as rel:
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
    with duckdb.connect() as conn, pytest.raises(FileNotFoundError):
        async with bind_step_reads(conn, reads=tmp_path / "absent.parquet", work_ticket_idx=1):
            pass


async def test_stream_branch_materializes_a_table(monkeypatch, tmp_path):
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

    with duckdb.connect() as conn:
        async with bind_step_reads(conn, reads=None, work_ticket_idx=4834) as rel:
            assert rel == READS_RELATION
            kind = conn.execute(
                "SELECT table_type FROM information_schema.tables WHERE table_name = ?",
                [rel],
            ).fetchone()
            assert kind[0] == "BASE TABLE", f"a stream must be materialized, got {kind[0]}"
            # Materialized inside the stream context, so it survives the reader
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

    with duckdb.connect() as conn:
        async with bind_step_reads(conn, reads=None, work_ticket_idx=1) as rel:
            assert conn.execute(f"SELECT count(*) FROM {rel}").fetchone()[0] == 0
            assert conn.execute(f"SELECT {_COLUMNS} FROM {rel}").fetchall() == []
