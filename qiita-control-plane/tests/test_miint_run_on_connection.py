"""`run_on_connection`: a cancelled caller gets its `CancelledError`, and the
connection it shares with the worker thread can then be closed."""

from __future__ import annotations

import asyncio
import contextlib
import gc
import threading
import time
from collections.abc import Awaitable, Callable

import duckdb
import pytest

from qiita_control_plane.miint import run_on_connection

# Minutes of work uninterrupted, so a cancel that does not interrupt it shows up as
# a slow test, not a hang.
_LONG_COPY = (
    "COPY (SELECT sum(a.range * b.range) AS s FROM range(40000) a, range(2000000) b) "
    "TO '{out}' (FORMAT PARQUET)"
)


async def _cancel(task: asyncio.Task) -> None:
    task.cancel()


async def _cancel_twice(task: asyncio.Task) -> None:
    task.cancel()
    await asyncio.sleep(0.005)
    task.cancel()


async def _cancel_every_other_task(task: asyncio.Task) -> None:
    # What the event loop does at shutdown: the task inside the helper is cancelled too.
    task.cancel()
    await asyncio.sleep(0.005)
    for other in asyncio.all_tasks():
        if other is not asyncio.current_task():
            other.cancel()


def _cancelled_copy(
    tmp_path,
    *,
    pause: float,
    settle: float,
    cancel: Callable[[asyncio.Task], Awaitable[None]],
    copies: int,
) -> tuple[float, list[dict]]:
    """Run the long COPY `copies` times in one thread through the helper, with the
    connection closed in `finally` as the control plane's callers do. Wait `pause` in
    the thread before the first statement starts and `settle` on the loop after the
    thread is entered, then `cancel`. Returns how long the cancellation took to come
    back and what the loop's exception handler was given."""
    entered = threading.Event()
    con = duckdb.connect()

    def copy() -> None:
        entered.set()
        time.sleep(pause)
        for _ in range(copies):
            # A thread that goes on using the connection after an interrupt, so a
            # helper that stops waiting early closes it under a running statement.
            with contextlib.suppress(duckdb.InterruptException):
                con.execute(_LONG_COPY.format(out=tmp_path / "out.parquet"))

    async def copy_then_close() -> None:
        try:
            await run_on_connection(con, copy)
        finally:
            con.close()

    async def scenario() -> tuple[float, list[dict]]:
        reported: list[dict] = []
        asyncio.get_running_loop().set_exception_handler(lambda _, ctx: reported.append(ctx))
        task = asyncio.create_task(copy_then_close())
        await asyncio.to_thread(entered.wait)
        await asyncio.sleep(settle)
        started = time.monotonic()
        await cancel(task)
        with pytest.raises(asyncio.CancelledError):
            await task
        elapsed = time.monotonic() - started
        gc.collect()
        await asyncio.sleep(0)
        return elapsed, reported

    return asyncio.run(scenario())


@pytest.mark.parametrize(
    ("pause", "settle", "cancel", "copies"),
    [
        (0.0, 0.3, _cancel, 1),
        # The first interrupts land before the statement starts, and DuckDB drops them.
        (0.3, 0.0, _cancel, 1),
        (0.0, 0.3, _cancel_twice, 2),
        (0.0, 0.3, _cancel_every_other_task, 2),
    ],
    ids=["mid-statement", "before-the-statement", "cancelled-twice", "loop-shutdown"],
)
def test_a_cancelled_copy_is_interrupted_and_the_connection_closes(
    tmp_path, pause, settle, cancel, copies
):
    elapsed, reported = _cancelled_copy(
        tmp_path, pause=pause, settle=settle, cancel=cancel, copies=copies
    )
    assert elapsed < 10
    # Nothing reports the interrupt's error as unhandled.
    assert reported == []


async def test_the_result_comes_back():
    con = duckdb.connect()
    cursor = await run_on_connection(con, con.execute, "SELECT 42")
    assert cursor.fetchall() == [(42,)]


async def test_a_failure_in_the_thread_propagates_unchanged():
    con = duckdb.connect()
    with pytest.raises(duckdb.CatalogException, match="no_such_table"):
        await run_on_connection(con, con.execute, "SELECT * FROM no_such_table")
