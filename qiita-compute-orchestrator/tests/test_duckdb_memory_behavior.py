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

Plain DuckDB, no miint extension and no staged indexes, so these run in the fast
`make test` tier. Kept deliberately small: each builds a table a few times the
memory limit it is tested against, which is enough to force the behaviour without
making the unit tier slow.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

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
