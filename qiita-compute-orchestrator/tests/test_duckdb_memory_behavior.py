"""Pins two DuckDB memory behaviours that job code REASONS ABOUT in comments.

Both are load-bearing for `align_sharded`'s decision to materialize the aligner's
query relation into an in-memory table (see the materialization note in
`jobs/align_sharded.py`), and neither was probed in this repo before — the argument
sat in prose only, which is exactly the kind of claim that rots without noticing:

1. **An over-limit in-memory table SPILLS rather than failing.** This is why a
   materialized copy that turns out not to fit is a performance problem and not an
   OOM. Note what that means in production, where `temp_directory` is the ticket
   workspace on a SHARED filesystem: the fallback is real but expensive, so it is a
   safety net, not a plan.
2. **`DROP TABLE` returns the table's bytes immediately**, rather than deferring the
   release to connection close. This is why dropping the copy between the two write
   phases does anything at all.

A third, added for the shard index builders: **the chunk-reassembly aggregate does
NOT spill — it raises OutOfMemoryException.** `stage_subject` is what both
`build_minimap2_index` and `build_bowtie2_index` reassemble a shard's subject with,
and each caps DuckDB's share under SLURM. Whether that cap is a graceful degradation
or a hard ceiling on shard size turns entirely on this, and the two are opposite
answers — behaviour 1 above shows a plain over-limit TABLE spilling, which is the
easy thing to assume also holds here. It does not.

Plain DuckDB, no miint extension and no staged indexes, so these run in the fast
`make test` tier. Kept deliberately small: each builds a table a few times the
memory limit it is tested against, which is enough to force the behaviour without
making the unit tier slow.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

# Real entropy is required. A `repeat()`-based sequence dictionary-compresses to
# almost nothing in DuckDB's own storage, which makes a table look free and would
# make both tests below pass for the wrong reason (nothing to spill, nothing to
# release). Five md5 hex digests translated onto ACGT give 160 high-entropy bases —
# the same generator the align cost measurements used.
_SEQ = (
    "translate(md5((i*1+1)::VARCHAR)||md5((i*7+7)::VARCHAR)||md5((i*13+13)::VARCHAR)"
    "||md5((i*17+17)::VARCHAR)||md5((i*19+19)::VARCHAR),"
    "'0123456789abcdef','ACGTACGTACGTACGT')"
)
_ROWS = 1_500_000  # ~240 MB of sequence


def _create_reads_table(conn: duckdb.DuckDBPyConnection, name: str) -> None:
    conn.execute(
        f"CREATE TABLE {name} AS SELECT i AS read_id, {_SEQ} AS sequence1 FROM range({_ROWS}) t(i)"
    )


def _duckdb_memory_bytes(conn: duckdb.DuckDBPyConnection) -> int:
    return conn.execute("SELECT sum(memory_usage_bytes) FROM duckdb_memory()").fetchone()[0]


def test_an_over_limit_in_memory_table_spills_instead_of_failing(tmp_path: Path) -> None:
    """A CREATE TABLE far larger than `memory_limit` succeeds, spilling to
    `temp_directory` — so a materialized relation that does not fit degrades rather
    than killing the job.

    The spill files themselves are the assertion: without them the table would have
    had to fit, and the test would be proving nothing about the over-limit path."""
    spill = tmp_path / "duckdb_tmp"
    with duckdb.connect(":memory:") as conn:
        conn.execute("SET threads=4")
        conn.execute("SET memory_limit='200MB'")
        conn.execute(f"SET temp_directory='{spill}'")
        conn.execute("SET preserve_insertion_order=false")

        _create_reads_table(conn, "reads")

        assert conn.execute("SELECT count(*) FROM reads").fetchone()[0] == _ROWS
        spilled = sum(f.stat().st_size for f in spill.rglob("*") if f.is_file())
        assert spilled > 0, "table fit inside the limit; this no longer tests the spill path"


def test_dropping_an_in_memory_table_releases_its_bytes() -> None:
    """`DROP TABLE` hands the table's memory back to the buffer manager right away.

    Asserted as a return to the pre-CREATE baseline rather than as an absolute number,
    so it does not encode DuckDB's per-row overhead. If a future DuckDB deferred the
    release to connection close, dropping the query copy between the two align write
    phases would become a no-op and this fails."""
    with duckdb.connect(":memory:") as conn:
        conn.execute("SET threads=4")
        conn.execute("SET memory_limit='8GB'")

        baseline = _duckdb_memory_bytes(conn)
        _create_reads_table(conn, "reads")
        held = _duckdb_memory_bytes(conn)
        assert held - baseline > 100_000_000, "table did not register in the buffer manager"

        conn.execute("DROP TABLE reads")

        assert _duckdb_memory_bytes(conn) - baseline < 1_000_000


# One chunk of the size `qiita_common.chunking` splits sequences into. Built from
# the same md5-onto-ACGT generator as `_SEQ`, for the reason stated there: a
# `repeat()` blob dictionary-compresses to nothing, and the aggregate would then be
# measuring an empty table rather than real chunk bytes.
_CHUNK_BLOB = f"(SELECT string_agg({_SEQ}, '') FROM range(seed, seed + 400) t(i))::BLOB"


def _create_chunks_table(conn: duckdb.DuckDBPyConnection, features: int, per_feature: int) -> int:
    """The `(feature_idx, chunk_index, chunk_data)` chunk shape `stage_subject`
    reassembles. Returns the total chunk_data bytes."""
    conn.execute(f"""
        CREATE OR REPLACE TABLE chunks AS
        SELECT feature_idx, chunk_index, {_CHUNK_BLOB} AS chunk_data
        FROM (
            SELECT f.fi::BIGINT AS feature_idx, c.ci::BIGINT AS chunk_index,
                   (f.fi * {per_feature} + c.ci) * 400 AS seed
            FROM range({features}) AS f(fi), range({per_feature}) AS c(ci)
        )
    """)
    return conn.execute("SELECT sum(octet_length(chunk_data)) FROM chunks").fetchone()[0]


def test_chunk_reassembly_raises_rather_than_spilling(tmp_path: Path) -> None:
    """`string_agg(chunk_data ORDER BY chunk_index) GROUP BY feature_idx` over more
    chunk bytes than `memory_limit` allows raises OutOfMemoryException — it does NOT
    spill the way an over-limit TABLE does above.

    This is the ceiling on how large a shard the index builders can reassemble.
    Both cap DuckDB's share under SLURM (`_MINIMAP2_SHARD_DUCKDB_CAP_GB`,
    `_BOWTIE2_SHARD_DUCKDB_CAP_GB`), and because this raises rather than degrading,
    a shard whose chunks exceed what that cap allows fails on the first attempt and
    on every retry — the cap is hard, so OOM escalation grows only the aligner's
    side. Measured alongside this: the aggregate wants roughly 2.2x the chunk bytes
    as `memory_limit` (3.6x at 0.25 GiB, 2.4x at 1 GiB, 2.2x at 4 GiB), which is
    where the ~5 Gbp figure in those constants' comments comes from.

    The control is the point of the test as much as the failure is: the identical
    fixture and query complete at a limit above the data, so the raise is the limit
    binding and not a broken fixture. If a future DuckDB spills this instead, this
    fails and the shard ceiling those comments describe is gone — which is a
    resource-sizing question worth re-opening, not a test to delete.
    """
    with duckdb.connect(":memory:") as conn:
        conn.execute("SET threads=4")
        conn.execute("SET memory_limit='8GB'")  # build the fixture unconstrained
        conn.execute(f"SET temp_directory='{tmp_path / 'build'}'")
        total = _create_chunks_table(conn, features=48, per_feature=64)
        assert total > 150_000_000, f"fixture too small to exceed the tight limit: {total} bytes"

        reassemble = """
            SELECT feature_idx, string_agg(chunk_data, '' ORDER BY chunk_index)
              FROM chunks GROUP BY feature_idx
        """

        # CONTROL — limit above the data volume: completes.
        conn.execute("SET memory_limit='8GB'")
        assert len(conn.execute(reassemble).fetchall()) == 48

        # Tight limit, well under the data: raises rather than spilling.
        conn.execute(f"SET temp_directory='{tmp_path / 'spill'}'")
        conn.execute("SET memory_limit='150MB'")
        with pytest.raises(duckdb.OutOfMemoryException):
            conn.execute(reassemble).fetchall()
