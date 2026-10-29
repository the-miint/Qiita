"""`run_on_connection`: a cancelled caller gets its `CancelledError`, and the
connection it shares with the worker thread can then be closed."""

from __future__ import annotations

import asyncio
import time

import duckdb
import pytest

from qiita_control_plane.miint import run_on_connection

# Minutes of work uninterrupted, so a cancel that does not interrupt it shows up as
# a slow test, not a hang.
_LONG_COPY = (
    "COPY (SELECT sum(a.range * b.range) AS s FROM range(40000) a, range(2000000) b) "
    "TO '{out}' (FORMAT PARQUET)"
)


async def _copy_then_close(con: duckdb.DuckDBPyConnection, out: str) -> None:
    # The shape of the control plane's callers: the connection is closed in `finally`.
    try:
        await run_on_connection(con, con.execute, _LONG_COPY.format(out=out))
    finally:
        con.close()


@pytest.mark.parametrize("delay", [0.3, 0.0], ids=["mid-statement", "before-the-statement"])
async def test_a_cancelled_copy_is_interrupted_and_the_connection_closes(tmp_path, delay):
    out = tmp_path / "out.parquet"
    task = asyncio.create_task(_copy_then_close(duckdb.connect(), str(out)))
    await asyncio.sleep(delay)
    started = time.monotonic()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert time.monotonic() - started < 10
    assert not out.exists()


async def test_the_result_comes_back():
    con = duckdb.connect()
    cursor = await run_on_connection(con, con.execute, "SELECT 42")
    assert cursor.fetchall() == [(42,)]


async def test_a_failure_in_the_thread_propagates_unchanged():
    con = duckdb.connect()
    with pytest.raises(duckdb.CatalogException, match="no_such_table"):
        await run_on_connection(con, con.execute, "SELECT * FROM no_such_table")
