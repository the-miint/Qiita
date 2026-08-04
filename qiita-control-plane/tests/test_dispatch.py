"""Unit tests for qiita_control_plane.dispatch.

Covers the asyncio-task lifecycle pieces (`schedule_dispatch`,
`drain_running_dispatches`, `build_compute_backend_client`) without
requiring a live DB. The DB-bound piece (`reconcile_inflight_tickets`)
is exercised by the route tests in tests/routes/test_work_ticket.py.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from qiita_control_plane.dispatch import (
    build_compute_backend_client,
    drain_running_dispatches,
    schedule_dispatch,
)
from qiita_control_plane.fanout_dispatch import shard_cohort


def _async_return(value):
    """An async stub returning `value`, for monkeypatching awaited collaborators."""

    async def _stub(*_args, **_kwargs):
        return value

    return _stub


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
            flight_signing_key=b"x" * 32,
            data_plane_url="grpc://unused",
            fanout_max_inflight=8,
            # _run_and_log reads both before dispatching to run_workflow.
            # Real Paths keep the defensive None-check happy without
            # making the test reach any filesystem operation (run_workflow
            # is monkeypatched in every test that actually dispatches).
            path_scratch_ticket=Path("/tmp/qiita-test-scratch-unused/ticket"),
            path_scratch_staging=Path("/tmp/qiita-test-scratch-unused/staging"),
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

    async def _fake_run(_app, ticket_idx, **_kwargs):
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


# ---------------------------------------------------------------------------
# _pump_ticket_cohort — a pump failure must not strand the cohort silently
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pump_ticket_cohort_retries_once_after_a_transient_failure(monkeypatch):
    """A transient pump failure at the tail of a fan-out would otherwise strand it.

    The completion hook is the only thing that re-triggers a pump, so if it throws
    while the failing child was the LAST in flight, every remaining ticket stays
    held with no future terminal transition to retry it — until a CP restart.
    """
    from qiita_control_plane import dispatch as dispatch_mod

    app = _fake_app()
    monkeypatch.setattr(
        dispatch_mod, "cohort_for_work_ticket", _async_return(shard_cohort(7)), raising=True
    )
    calls: list[int] = []

    async def flaky(*_args, **_kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("transient")
        return []

    monkeypatch.setattr(dispatch_mod, "top_up_dispatch", flaky, raising=True)
    await dispatch_mod._pump_ticket_cohort(app, 42)
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_pump_ticket_cohort_logs_error_naming_the_cohort_when_the_retry_fails(
    monkeypatch, caplog
):
    from qiita_control_plane import dispatch as dispatch_mod

    app = _fake_app()
    monkeypatch.setattr(
        dispatch_mod, "cohort_for_work_ticket", _async_return(shard_cohort(7)), raising=True
    )

    async def always_fails(*_args, **_kwargs):
        raise RuntimeError("still broken")

    monkeypatch.setattr(dispatch_mod, "top_up_dispatch", always_fails, raising=True)
    with caplog.at_level(logging.ERROR):
        await dispatch_mod._pump_ticket_cohort(app, 42)
    # Naming the cohort is the point: it is what an operator needs to re-pump.
    assert shard_cohort(7).label in caplog.text
    # Still swallowed — a pump failure must not fail the dispatch task.


@pytest.mark.asyncio
async def test_pump_ticket_cohort_is_a_no_op_for_a_non_fanout_ticket(monkeypatch):
    from qiita_control_plane import dispatch as dispatch_mod

    app = _fake_app()
    monkeypatch.setattr(dispatch_mod, "cohort_for_work_ticket", _async_return(None), raising=True)

    async def must_not_run(*_args, **_kwargs):
        raise AssertionError("top_up_dispatch called for a non-fan-out ticket")

    monkeypatch.setattr(dispatch_mod, "top_up_dispatch", must_not_run, raising=True)
    await dispatch_mod._pump_ticket_cohort(app, 42)
