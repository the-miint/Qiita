"""Unit tests for qiita_control_plane.dispatch.

Covers the asyncio-task lifecycle pieces (`schedule_dispatch`,
`drain_running_dispatches`, `build_compute_backend_client`) without
requiring a live DB. The DB-bound piece (`reconcile_inflight_tickets`)
is exercised by the route tests in tests/routes/test_work_ticket.py.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from qiita_common.models import DEFAULT_DISPATCH_MAX_INFLIGHT, DISPATCH_MAX_INFLIGHT_CHOICES

from qiita_control_plane.db import PRODUCTION_POOL_FLOOR
from qiita_control_plane.dispatch import (
    build_compute_backend_client,
    build_dispatch_semaphore,
    dispatch_cap_fits_pool,
    drain_running_dispatches,
    max_settable_dispatch_max_inflight,
    production_pool_max_size,
    reconcile_inflight_tickets,
    schedule_dispatch,
)
from qiita_control_plane.ena_import.batch import _STUDY_CONCURRENCY
from qiita_control_plane.fanout_dispatch import DEFAULT_FANOUT_MAX_INFLIGHT, shard_cohort


def _async_return(value):
    """An async stub returning `value`, for monkeypatching awaited collaborators."""

    async def _stub(*_args, **_kwargs):
        return value

    return _stub


def _fake_app(*, compute_backend_client=object(), pool=object()) -> SimpleNamespace:
    """Build the minimum app.state surface schedule_dispatch reads. The
    dispatcher only touches `app.state.compute_backend_client`,
    `app.state.running_dispatches`, `app.state.dispatch_semaphore`,
    `app.state.pool`, and `app.state.settings`. Real values are not exercised
    — `_run_and_log` is monkeypatched in each test."""
    state = SimpleNamespace(
        compute_backend_client=compute_backend_client,
        running_dispatches=set(),
        reconcile_resume_task=None,
        dispatch_semaphore=build_dispatch_semaphore(),
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


def test_schedule_dispatch_reads_the_semaphore_before_creating_a_task():
    """Unwired lifespan state must fail here — naming the missing wiring, not
    with a bare AttributeError — and must not orphan a background task."""
    app = SimpleNamespace(
        state=SimpleNamespace(compute_backend_client=object(), running_dispatches=set())
    )
    with pytest.raises(RuntimeError, match="dispatch_semaphore is not wired"):
        schedule_dispatch(app, work_ticket_idx=1)
    assert app.state.running_dispatches == set()


@pytest.mark.parametrize("cap", DISPATCH_MAX_INFLIGHT_CHOICES)
def test_every_allowed_cap_stays_well_below_the_pool_it_sizes(cap):
    """Well below means at most half of the pool main.py builds for that boot cap.
    Dispatch and the ENA study gate also draw on that one pool with no joint
    accounting anywhere else, so their sum is pinned here too. Spelled out rather
    than through `dispatch_cap_fits_pool`, so a change to that predicate cannot
    loosen the check it is checked by."""
    pool_max_size = production_pool_max_size(cap)
    assert pool_max_size >= PRODUCTION_POOL_FLOOR
    assert cap * 2 <= pool_max_size
    assert cap + _STUDY_CONCURRENCY <= pool_max_size
    assert dispatch_cap_fits_pool(cap, pool_max_size)


def test_pool_sizes_for_the_allowed_caps():
    """The pool sizes whose Postgres connection budget was added up when the set
    was chosen; a change to the formula or the set changes them and fails here."""
    assert {c: production_pool_max_size(c) for c in DISPATCH_MAX_INFLIGHT_CHOICES} == {
        8: 25,
        12: 25,
        16: 32,
        24: 48,
    }


def test_dispatch_cap_fits_pool_rejects_a_cap_over_half_the_pool():
    assert dispatch_cap_fits_pool(12, 25)
    assert not dispatch_cap_fits_pool(13, 25)
    assert not dispatch_cap_fits_pool(16, 25)


@pytest.mark.parametrize(
    ("boot", "settable"),
    [(8, 12), (12, 12), (16, 16), (24, 24)],
)
def test_max_settable_override_is_what_the_boot_pool_carries(boot, settable):
    """The pool is sized once at boot, so a runtime override can go only as high as
    that pool allows: booted at 8 or 12 (pool 25) the most is 12."""
    assert max_settable_dispatch_max_inflight(boot) == settable


def test_default_dispatch_cap_is_an_allowed_value():
    assert DEFAULT_DISPATCH_MAX_INFLIGHT in DISPATCH_MAX_INFLIGHT_CHOICES


def test_dispatch_concurrency_reaches_one_cohort_at_its_default_cap():
    """A fan-out cohort at FANOUT_MAX_INFLIGHT's default must be schedulable on
    its own at any dispatch cap: the process-wide cap shares its slots with every
    other dispatch, so a cap below the per-cohort default would silently clamp the
    knob its own docs tell operators to raise."""
    assert DEFAULT_FANOUT_MAX_INFLIGHT <= min(DISPATCH_MAX_INFLIGHT_CHOICES)


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
async def test_schedule_dispatch_detaches_from_a_surrounding_step_scope(monkeypatch):
    """A ticket dispatched from inside a step's scope (plan-shards fans out children
    through `dispatch_cb` → here) is its own unit of work: `_bounded_run` wraps the run
    in `no_step_scope`, so the child's `summarize()` does not land on the dispatching
    step. `create_task` copies the scope, so without the wrap it would."""
    from qiita_common.step_log import step_scope, summarize

    app = _fake_app()
    done = asyncio.Event()

    async def _fake_run(_app, ticket_idx, **_kwargs):
        # Without the detach this lands on the parent scope copied into the task.
        summarize(child_count=1)
        done.set()

    monkeypatch.setattr("qiita_control_plane.dispatch._run_and_log", _fake_run)

    with step_scope("plan-shards") as parent:
        task = schedule_dispatch(app, work_ticket_idx=11)
        await asyncio.wait_for(done.wait(), timeout=5)
    await task
    assert parent.summary == {}


@pytest.mark.asyncio
async def test_a_dispatch_queued_behind_the_cap_logs_the_wait(monkeypatch, caplog):
    """A dispatch waiting for a slot must be visible in the log: no runner has
    started it yet, so it carries no `transient_reason` and no `dispatch_held`,
    and the queue line is the only thing telling it apart from a dead one."""
    app = _fake_app()
    semaphore = app.state.dispatch_semaphore
    for _ in range(semaphore.limit):
        await semaphore.acquire()

    started = asyncio.Event()
    finish = asyncio.Event()

    async def _fake_run(_app, ticket_idx, **_kwargs):
        started.set()
        await finish.wait()

    monkeypatch.setattr("qiita_control_plane.dispatch._run_and_log", _fake_run)

    def _logged(fragment: str) -> bool:
        return any(fragment in record.getMessage() for record in caplog.records)

    with caplog.at_level(logging.INFO, logger="qiita_control_plane.dispatch"):
        task = schedule_dispatch(app, work_ticket_idx=8)
        try:
            # Poll the log itself (not a fixed sleep): the task logs the queue
            # before it blocks on the acquire.
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and not _logged("queued behind the dispatch cap"):
                await asyncio.sleep(0.01)
            assert _logged("queued behind the dispatch cap")
        finally:
            for _ in range(semaphore.limit):
                semaphore.release()
            await asyncio.wait_for(started.wait(), timeout=5)
            finish.set()
            await asyncio.wait_for(task, timeout=5)
    assert _logged("dispatched after waiting")


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


@pytest.fixture
def reconcile_events(monkeypatch):
    """Record the order of wait / schedule / top-up calls made by a reconcile."""
    from qiita_control_plane import dispatch as dispatch_mod

    events: list[str] = []
    release = asyncio.Event()

    async def wait(url):
        events.append("wait")
        await release.wait()
        events.append("ready")

    async def top_up(*_args, **_kwargs):
        events.append("top_up")

    monkeypatch.setattr(dispatch_mod, "wait_for_data_plane", wait)
    monkeypatch.setattr(dispatch_mod, "top_up_dispatch", top_up)
    monkeypatch.setattr(
        dispatch_mod, "schedule_dispatch", lambda _app, idx, **_kw: events.append(f"schedule {idx}")
    )
    return SimpleNamespace(events=events, release=release)


def _stub_work(monkeypatch, *, idxs, cohorts):
    from qiita_control_plane import dispatch as dispatch_mod

    monkeypatch.setattr(
        dispatch_mod, "_inflight_ticket_versions", _async_return({i: 0 for i in idxs})
    )
    monkeypatch.setattr(dispatch_mod, "held_cohorts", _async_return(list(cohorts)))


async def _settle(app):
    await app.state.reconcile_resume_task


async def test_reconcile_schedules_nothing_before_the_data_plane_answers(
    monkeypatch, reconcile_events
):
    app = _fake_app()
    _stub_work(monkeypatch, idxs=[1, 2], cohorts=[])

    count = await reconcile_inflight_tickets(app)
    await asyncio.sleep(0.01)
    assert reconcile_events.events == ["wait"]

    reconcile_events.release.set()
    await _settle(app)
    assert reconcile_events.events == ["wait", "ready", "schedule 1", "schedule 2"]
    assert count == 2


async def test_reconcile_returns_while_the_wait_is_still_pending(monkeypatch, reconcile_events):
    app = _fake_app()
    _stub_work(monkeypatch, idxs=[1], cohorts=[])

    await asyncio.wait_for(reconcile_inflight_tickets(app), timeout=1)
    assert not app.state.reconcile_resume_task.done()
    assert not app.state.running_dispatches

    reconcile_events.release.set()
    await _settle(app)


async def test_reconcile_schedules_nothing_while_the_data_plane_stays_down(monkeypatch):
    from qiita_control_plane import dispatch as dispatch_mod
    from qiita_control_plane import health as health_module

    async def never_ok(_url, *, log_failures=True):
        return health_module.HealthStatus.UNREACHABLE

    scheduled: list[int] = []
    monkeypatch.setattr(health_module, "_probe_dp", never_ok)
    monkeypatch.setattr(health_module, "_DP_WAIT_INTERVAL_SECONDS", 0.001)
    monkeypatch.setattr(
        dispatch_mod, "schedule_dispatch", lambda _a, idx, **_k: scheduled.append(idx)
    )
    _stub_work(monkeypatch, idxs=[1], cohorts=[])

    app = _fake_app()
    await reconcile_inflight_tickets(app)
    await asyncio.sleep(0.05)
    task = app.state.reconcile_resume_task
    assert not task.done()
    assert scheduled == []

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert scheduled == []


async def test_reconcile_never_waits_without_in_flight_work(monkeypatch, reconcile_events):
    app = _fake_app()
    _stub_work(monkeypatch, idxs=[], cohorts=[])

    assert await reconcile_inflight_tickets(app) == 0
    assert reconcile_events.events == []
    assert app.state.reconcile_resume_task is None


async def test_reconcile_waits_before_pumping_held_cohorts_with_no_tickets(
    monkeypatch, reconcile_events
):
    app = _fake_app()
    _stub_work(monkeypatch, idxs=[], cohorts=[shard_cohort(7)])

    await reconcile_inflight_tickets(app)
    await asyncio.sleep(0.01)
    assert reconcile_events.events == ["wait"]

    reconcile_events.release.set()
    await _settle(app)
    assert reconcile_events.events == ["wait", "ready", "top_up"]


async def test_reconcile_retries_a_failed_re_read_until_it_schedules_every_ticket(
    monkeypatch, reconcile_events, caplog
):
    from qiita_control_plane import dispatch as dispatch_mod

    reads = 0

    async def flaky_versions(_pool):
        nonlocal reads
        reads += 1
        if reads == 2:
            raise OSError("connection reset")
        return {1: 0, 2: 0}

    monkeypatch.setattr(dispatch_mod, "_inflight_ticket_versions", flaky_versions)
    monkeypatch.setattr(dispatch_mod, "held_cohorts", _async_return([]))
    monkeypatch.setattr(dispatch_mod, "_RESUME_RETRY_BASE_SECONDS", 0)
    reconcile_events.release.set()
    app = _fake_app()

    with caplog.at_level(logging.ERROR):
        await reconcile_inflight_tickets(app)
        await asyncio.wait_for(_settle(app), timeout=1)

    assert "connection reset" in caplog.text
    assert reads == 3
    assert [e for e in reconcile_events.events if e.startswith("schedule")] == [
        "schedule 1",
        "schedule 2",
    ]


async def test_reconcile_does_not_reschedule_a_ticket_when_the_retry_follows_a_partial_run(
    monkeypatch,
):
    from qiita_control_plane import dispatch as dispatch_mod

    scheduled: list[int] = []
    failed_once = False

    def flaky_schedule(_app, idx, **_kw):
        nonlocal failed_once
        if idx == 2 and not failed_once:
            failed_once = True
            raise RuntimeError("boom")
        scheduled.append(idx)

    monkeypatch.setattr(dispatch_mod, "schedule_dispatch", flaky_schedule)
    monkeypatch.setattr(dispatch_mod, "wait_for_data_plane", _async_return(None))
    monkeypatch.setattr(dispatch_mod, "_RESUME_RETRY_BASE_SECONDS", 0)
    app = _fake_app()
    _stub_work(monkeypatch, idxs=[1, 2], cohorts=[])

    await reconcile_inflight_tickets(app)
    await asyncio.wait_for(_settle(app), timeout=1)

    assert scheduled == [1, 2]
