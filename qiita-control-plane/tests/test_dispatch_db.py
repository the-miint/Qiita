"""DB-bound guard for dispatch's process-wide concurrency cap.

The sizing guards (`_DISPATCH_CONCURRENCY` vs the production pool, and vs the
fan-out default) are pure and live in test_dispatch.py. This one needs a real
pool: it drives `schedule_dispatch` itself through a burst and shows a request
can still take a connection while the cap's worth of dispatches hold theirs.
The fakes are deliberately harsher than reality — each holds one connection
for its whole run, where a real dispatch acquires per call — so a pass here
covers the real shape too.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import asyncpg
import pytest

from qiita_control_plane.dispatch import (
    _DISPATCH_CONCURRENCY,
    build_dispatch_semaphore,
    schedule_dispatch,
)

pytestmark = pytest.mark.db


async def test_request_acquires_a_connection_while_many_tickets_dispatch(postgres_url, monkeypatch):
    """A saturated dispatch must still leave the pool a connection for a request.

    Each fake dispatch task holds a connection for its whole run, the worst
    case a real task approximates (a real one acquires per call, never for the
    workflow). Without the process-wide cap every scheduled ticket runs at once
    and takes the pool; with it, at most `_DISPATCH_CONCURRENCY` hold and the
    request's acquire below succeeds.
    """
    bound = _DISPATCH_CONCURRENCY
    # Sized from the constant rather than the shared fixture pool: that pool's
    # max_size sits below the production-shaped bound this exercises, so the
    # spare connection the assertion needs must be headroom over `bound`.
    holders = 0
    max_holders = 0
    all_slots_held = asyncio.Event()
    release = asyncio.Event()

    async def _gated_run_and_log(_app, _work_ticket_idx, **_kwargs):
        nonlocal holders, max_holders
        async with pool.acquire():
            holders += 1
            max_holders = max(max_holders, holders)
            if holders == bound:
                all_slots_held.set()
            await release.wait()
            holders -= 1

    monkeypatch.setattr("qiita_control_plane.dispatch._run_and_log", _gated_run_and_log)
    app = SimpleNamespace(
        state=SimpleNamespace(
            compute_backend_client=object(),
            running_dispatches=set(),
            dispatch_semaphore=build_dispatch_semaphore(),
        )
    )

    pool: asyncpg.Pool | None = None
    tasks: list[asyncio.Task] = []
    try:
        # Inside the try: a raise while creating or scheduling must still close
        # the pool below, or the session leaks `bound + 2` connections.
        pool = await asyncpg.create_pool(postgres_url, min_size=1, max_size=bound + 2, timeout=5)
        tasks = [schedule_dispatch(app, work_ticket_idx=idx) for idx in range(1, bound + 5)]
        # Hang guard only -- the assertions below are what prove the cap.
        await asyncio.wait_for(all_slots_held.wait(), timeout=10)
        assert max_holders == bound

        # Every query uses an explicit timeout so a cap that leaks past the
        # pool's max_size fails fast here instead of hanging on an
        # un-timed-out acquire.
        conn = await pool.acquire(timeout=2)
        try:
            assert await conn.fetchval("SELECT 1") == 1
        finally:
            # Awaited: an un-awaited release never returns the connection, and
            # the close below waits for exactly that return.
            await pool.release(conn)

        release.set()
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=10)
        assert max_holders == bound
    finally:
        # Unblock the gate and let every task finish even on assertion
        # failure, so no fake dispatch (or its held connection) outlives the
        # test.
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        if pool is not None:
            await pool.close()
