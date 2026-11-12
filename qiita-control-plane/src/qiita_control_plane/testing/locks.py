"""Wait, in a DB-bound test, until the work under test is queued behind a lock
that the test holds."""

import asyncio

import asyncpg


async def wait_until_blocked_by(
    pool: asyncpg.Pool, blocker: asyncpg.Connection, task: asyncio.Task, *, timeout_s: float
) -> None:
    """Return once some backend waits on a lock that `blocker` holds, while
    `task` (the only candidate) is still running, rather than after a fixed
    sleep that would not show that the task ever reached the lock. Fails when
    the task finishes first or nothing waits within timeout_s."""
    blocker_pid = blocker.get_server_pid()
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while loop.time() < deadline:
        if task.done():
            raise AssertionError("the task finished without waiting on the held lock")
        waiting = await pool.fetchval(
            "SELECT count(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))",
            blocker_pid,
        )
        if waiting:
            return
        await asyncio.sleep(0.02)
    raise AssertionError("the task never waited on the held lock")
