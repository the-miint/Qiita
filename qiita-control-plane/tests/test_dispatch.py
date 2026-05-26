"""Unit tests for qiita_control_plane.dispatch.

Covers the asyncio-task lifecycle pieces (`schedule_dispatch`,
`drain_running_dispatches`, `build_compute_backend_client`) without
requiring a live DB. The DB-bound piece (`recover_orphaned_tickets`)
is exercised by the route tests in tests/routes/test_work_ticket.py
once those land.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from qiita_control_plane.dispatch import (
    build_compute_backend_client,
    drain_running_dispatches,
    schedule_dispatch,
)


def _fake_app(*, compute_backend_client=object(), pool=object()) -> SimpleNamespace:
    """Build the minimum app.state surface schedule_dispatch reads. The
    dispatcher only touches `app.state.compute_backend_client`,
    `app.state.running_dispatches`, `app.state.pool`, and
    `app.state.settings`. Real values are not exercised — `_run_and_log`
    is monkeypatched in each test."""
    state = SimpleNamespace(
        compute_backend_client=compute_backend_client,
        running_dispatches=set(),
        pool=pool,
        settings=SimpleNamespace(
            hmac_secret_key=b"x" * 16,
            data_plane_url="grpc://unused",
            # _run_and_log reads both before dispatching to run_workflow.
            # Real Paths keep the defensive None-check happy without
            # making the test reach any filesystem operation (run_workflow
            # is monkeypatched in every test that actually dispatches).
            work_ticket_workspace_root=Path("/tmp/qiita-test-ws-unused"),
            upload_staging_root=Path("/tmp/qiita-test-staging-unused"),
        ),
    )
    return SimpleNamespace(state=state)


def test_build_compute_backend_client_returns_none_when_url_unset():
    assert build_compute_backend_client(base_url=None, token_path="/dev/null") is None


def test_schedule_dispatch_raises_when_client_unconfigured():
    app = _fake_app(compute_backend_client=None)
    with pytest.raises(RuntimeError, match="compute_backend_client is not configured"):
        schedule_dispatch(app, work_ticket_idx=42)


@pytest.mark.asyncio
async def test_schedule_dispatch_registers_and_removes_task(monkeypatch):
    """The task should land in app.state.running_dispatches at create
    time and be removed once it completes."""
    app = _fake_app()
    started = asyncio.Event()
    finish = asyncio.Event()

    async def _fake_run(_app, ticket_idx):
        started.set()
        await finish.wait()

    monkeypatch.setattr("qiita_control_plane.dispatch._run_and_log", _fake_run)

    task = schedule_dispatch(app, work_ticket_idx=7)
    assert task in app.state.running_dispatches
    await started.wait()
    assert task in app.state.running_dispatches  # still running

    finish.set()
    await task
    # done-callback runs synchronously *after* task completion; let the
    # event loop turn so the discard fires before we assert.
    await asyncio.sleep(0)
    assert task not in app.state.running_dispatches


@pytest.mark.asyncio
async def test_schedule_dispatch_swallows_runner_exceptions(monkeypatch, caplog):
    """`_run_and_log` is supposed to log and swallow runner exceptions
    so the asyncio Task completes cleanly. Patch `run_workflow` (the
    symbol `_run_and_log` calls) so the swallow wrapper actually runs;
    awaiting the task must not raise."""

    async def _raising_workflow(*args, **kwargs):
        raise RuntimeError("workflow blew up")

    monkeypatch.setattr("qiita_control_plane.dispatch.run_workflow", _raising_workflow)

    app = _fake_app()
    task = schedule_dispatch(app, work_ticket_idx=99)
    # _run_and_log catches the RuntimeError and logs it; the asyncio
    # task completes cleanly with no exception escaping.
    await task
    assert task.done() and not task.cancelled()
    assert task.exception() is None


@pytest.mark.asyncio
async def test_drain_running_dispatches_waits_for_completion():
    """Tasks that complete inside the timeout drain cleanly."""
    finish = asyncio.Event()

    async def _quick():
        await finish.wait()

    running: set[asyncio.Task] = set()
    task = asyncio.create_task(_quick())
    running.add(task)
    task.add_done_callback(running.discard)

    finish.set()
    await drain_running_dispatches(running, timeout_seconds=2.0)
    assert task.done()
    assert not task.cancelled()


@pytest.mark.asyncio
async def test_drain_running_dispatches_cancels_stuck_tasks():
    """A task still running past the timeout should be cancelled."""

    async def _stuck():
        await asyncio.sleep(60)

    running: set[asyncio.Task] = set()
    task = asyncio.create_task(_stuck())
    running.add(task)
    task.add_done_callback(running.discard)

    await drain_running_dispatches(running, timeout_seconds=0.05)
    # Cancellation propagates on the next event-loop turn — drain only
    # *requests* cancellation. Await with suppression so the
    # CancelledError doesn't escape this test.
    await asyncio.gather(task, return_exceptions=True)
    assert task.cancelled()


@pytest.mark.asyncio
async def test_drain_running_dispatches_no_op_on_empty_set():
    await drain_running_dispatches(set(), timeout_seconds=1.0)


@pytest.mark.asyncio
async def test_run_and_log_swallows_runner_exception(monkeypatch, caplog):
    """`_run_and_log` is the actual wrapper that's expected to swallow
    exceptions (since the runner has already marked the ticket FAILED).
    Direct test of the helper, not through schedule_dispatch."""
    from qiita_control_plane.dispatch import _run_and_log

    async def _raise(*args, **kwargs):
        raise RuntimeError("simulated step failure")

    monkeypatch.setattr("qiita_control_plane.dispatch.run_workflow", _raise)

    app = _fake_app()
    # Should NOT raise — the wrapper logs and swallows.
    await _run_and_log(app, 17)
