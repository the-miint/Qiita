"""`run_on_connection`: a cancelled caller gets its `CancelledError`, and the
connection it shares with the worker thread can then be closed."""

from __future__ import annotations

import asyncio
import contextlib
import gc
import threading
import time
from collections.abc import Awaitable, Callable, Coroutine
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pytest

from qiita_control_plane.miint import run_on_connection

# Minutes of work uninterrupted, so a cancel that does not interrupt it shows up as
# a slow test, not a hang.
_LONG_COPY = (
    "COPY (SELECT sum(a.range * b.range) AS s FROM range(40000) a, range(2000000) b) "
    "TO '{out}' (FORMAT PARQUET)"
)

# Longer than the helper's interval between interrupts.
_POLL = 0.1


def _run_bounded[T](scenario: Coroutine[object, object, T], timeout: float) -> T:
    """`asyncio.run(scenario)` on a thread of its own, failing after `timeout` s. A
    helper that never stops waiting would otherwise hang the run, since it absorbs the
    cancellation `asyncio.run`'s teardown sends it too."""
    outcome: dict[str, object] = {}

    def run() -> None:
        try:
            outcome["value"] = asyncio.run(scenario)
        except BaseException as exc:
            outcome["error"] = exc

    runner = threading.Thread(target=run, daemon=True)
    runner.start()
    runner.join(timeout)
    assert not runner.is_alive(), f"still waiting after {timeout} s"
    if "error" in outcome:
        raise outcome["error"]
    return outcome["value"]


async def _cancel(task: asyncio.Task) -> None:
    task.cancel()


async def _cancel_twice(task: asyncio.Task) -> None:
    task.cancel()
    await asyncio.sleep(0.005)
    task.cancel()


async def _cancel_every_other_task(task: asyncio.Task) -> None:
    # What `asyncio.run` does at shutdown. Against a helper that runs the thread in a
    # task of its own, this cancels that task too.
    task.cancel()
    await asyncio.sleep(0.005)
    for other in asyncio.all_tasks():
        if other is not asyncio.current_task():
            other.cancel()


def _cancelled_copy(
    tmp_path,
    *,
    cancel_first: bool,
    cancel: Callable[[asyncio.Task], Awaitable[None]],
    copies: int,
) -> tuple[float, list[dict]]:
    """Run the long COPY `copies` times in one thread through the helper, with the
    connection closed in `finally` as the control plane's callers do, and `cancel` the
    task: before the first statement starts when `cancel_first`, else while it runs.
    Returns how long the cancellation took to come back and what the loop's exception
    handler was given."""
    entered = threading.Event()
    go = threading.Event()
    con = duckdb.connect()

    def copy() -> None:
        entered.set()
        go.wait()
        out = tmp_path / "out.parquet"
        for _ in range(copies - 1):
            # A thread that goes on using the connection after an interrupt, so a
            # helper that stops waiting early closes it under a running statement.
            with contextlib.suppress(duckdb.InterruptException):
                con.execute(_LONG_COPY.format(out=out))
        # The last interrupt is raised, and nothing may report it as unhandled.
        con.execute(_LONG_COPY.format(out=out))

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
        if not cancel_first:
            go.set()
            await asyncio.sleep(0.3)
        started = time.monotonic()
        await cancel(task)
        if cancel_first:
            # The interrupts sent so far land before the statement, and DuckDB drops them.
            await asyncio.sleep(_POLL)
            go.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        elapsed = time.monotonic() - started
        gc.collect()
        await asyncio.sleep(0)
        return elapsed, reported

    return _run_bounded(scenario(), timeout=60)


@pytest.mark.parametrize(
    ("cancel_first", "cancel", "copies"),
    [
        (False, _cancel, 1),
        (True, _cancel, 1),
        (False, _cancel_twice, 2),
        (False, _cancel_every_other_task, 2),
    ],
    ids=["mid-statement", "before-the-statement", "cancelled-twice", "loop-shutdown"],
)
def test_a_cancelled_copy_is_interrupted_and_the_connection_closes(
    tmp_path, cancel_first, cancel, copies
):
    elapsed, reported = _cancelled_copy(
        tmp_path, cancel_first=cancel_first, cancel=cancel, copies=copies
    )
    assert elapsed < 10
    # Nothing reports the interrupt's error as unhandled.
    assert reported == []


def test_a_call_cancelled_while_it_waits_for_a_worker_is_not_waited_for():
    # One worker, held busy, so the call is still queued when it is cancelled.
    ran = threading.Event()
    hold = threading.Event()

    async def scenario() -> None:
        asyncio.get_running_loop().set_default_executor(ThreadPoolExecutor(max_workers=1))
        holder = asyncio.create_task(asyncio.to_thread(hold.wait))
        task = asyncio.create_task(run_on_connection(duckdb.connect(), ran.set))
        await asyncio.sleep(_POLL)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        hold.set()
        await holder

    try:
        _run_bounded(scenario(), timeout=10)
    finally:
        hold.set()
    assert not ran.is_set()


class _HoldingExecutor(ThreadPoolExecutor):
    """An executor whose worker, once it takes an item, waits for `proceed` before
    running it: the item can no longer be cancelled, and the call has not started."""

    def __init__(self) -> None:
        super().__init__(max_workers=1)
        self.taken = threading.Event()
        self.proceed = threading.Event()

    def submit(self, fn, /, *args, **kwargs):
        def held():
            self.taken.set()
            self.proceed.wait()
            return fn(*args, **kwargs)

        return super().submit(held)


def test_a_call_cancelled_after_a_worker_took_it_does_not_run():
    ran = threading.Event()
    executor = _HoldingExecutor()

    async def scenario() -> None:
        asyncio.get_running_loop().set_default_executor(executor)
        task = asyncio.create_task(run_on_connection(duckdb.connect(), ran.set))
        # Polled, not `to_thread`: the executor's one worker is the one being held.
        while not executor.taken.is_set():
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        executor.proceed.set()

    try:
        _run_bounded(scenario(), timeout=10)
    finally:
        executor.proceed.set()
    # `asyncio.run` joined the executor, so the call has run by now.
    assert not ran.is_set()


async def test_the_result_comes_back():
    con = duckdb.connect()
    cursor = await run_on_connection(con, con.execute, "SELECT 42")
    assert cursor.fetchall() == [(42,)]


async def test_a_failure_in_the_thread_propagates_unchanged():
    con = duckdb.connect()
    with pytest.raises(duckdb.CatalogException, match="no_such_table"):
        await run_on_connection(con, con.execute, "SELECT * FROM no_such_table")
