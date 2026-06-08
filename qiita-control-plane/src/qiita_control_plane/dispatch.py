"""Work-ticket dispatch — fires `runner.run_workflow` as a background task.

The CP itself is the dispatcher (option C in the design discussion):
the route that creates a ticket also fires an `asyncio.create_task`,
and the workflow runs in-process. No polling worker, no separate dispatch
service. Long-running steps don't block the originating HTTP request.

Failure modes:

- If the CP restarts mid-dispatch, the ticket sits in PROCESSING with no
  live owner. `reconcile_inflight_tickets` (called from lifespan startup)
  re-drives every non-terminal ticket through `run_workflow(resume=True)`,
  which re-attaches to a still-running SLURM job (or finalizes one that
  succeeded while the CP was down) rather than failing live work. Deploys
  stop/start the CP without draining queues, so a restart with in-flight
  tickets is routine, not a crash — failing them all would nuke running
  work on every deploy.

- If the runner raises, `run_workflow` itself transitions the ticket to
  FAILED. The done-callback installed here only handles task-level errors
  (asyncio cancellation, lost-pool, etc.) and logs them.

State machine guard (atomic conditional UPDATE in
`runner._atomic_transition` / `_transition_to_processing_for_resume`)
prevents double-dispatch even if /run races with the implicit on-create
dispatch or a startup reconcile.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from qiita_common.compute_backend_client import ComputeBackendClient
from qiita_common.models import WorkTicketState

from .runner import run_workflow

if TYPE_CHECKING:
    import asyncpg
    from fastapi import FastAPI

_log = logging.getLogger(__name__)


# The orchestrator-managed lifecycle states. A ticket in any of these
# is "in flight" — it has not reached a terminal outcome (COMPLETED /
# FAILED). Used by `reconcile_inflight_tickets` to scope the startup resume
# sweep and by the work-ticket route's disallow-without-delete check.
NON_TERMINAL_WORK_TICKET_STATES: tuple[str, ...] = (
    WorkTicketState.PENDING.value,
    WorkTicketState.QUEUED.value,
    WorkTicketState.PROCESSING.value,
)


async def _run_and_log(app: FastAPI, work_ticket_idx: int, *, resume: bool = False) -> None:
    """Inner task body: call `run_workflow` and log task-level errors.

    Workflow-level failures (a step raising) are already handled by
    `run_workflow` itself — it transitions the ticket to FAILED and
    re-raises. We catch the re-raise here only so the outer asyncio
    machinery doesn't see an "unhandled exception in task" warning;
    the ticket state is already correct.

    `resume` is forwarded to `run_workflow` — set by startup reconcile to
    re-attach an in-flight ticket instead of requiring it be PENDING."""
    workspace_root = app.state.settings.path_scratch_ticket
    upload_staging_root = app.state.settings.path_scratch_staging
    if workspace_root is None or upload_staging_root is None:
        # Defensive — Settings.from_env() requires PATH_SCRATCH, so the only
        # way to reach this is a Settings(...) construction that omits these
        # (tests, programmatic boot). Raise so the symptom isn't a silent
        # /tmp/None or AttributeError downstream.
        raise RuntimeError(
            "schedule_dispatch reached _run_and_log but"
            f" path_scratch_ticket={workspace_root!r},"
            f" path_scratch_staging={upload_staging_root!r};"
            " set PATH_SCRATCH or construct Settings with both"
        )
    try:
        await run_workflow(
            work_ticket_idx,
            app.state.pool,
            app.state.compute_backend_client,
            hmac_secret=app.state.settings.hmac_secret_key,
            data_plane_url=app.state.settings.data_plane_url,
            work_ticket_workspace_root=workspace_root,
            upload_staging_root=upload_staging_root,
            resume=resume,
        )
    except Exception:
        # run_workflow has already transitioned to FAILED. Log and swallow
        # so the asyncio task completes cleanly.
        _log.exception(
            "dispatch_ticket %d failed (ticket marked FAILED by runner)",
            work_ticket_idx,
        )


def schedule_dispatch(app: FastAPI, work_ticket_idx: int, *, resume: bool = False) -> asyncio.Task:
    """Fire-and-forget dispatch of one work ticket.

    Caller is the route handler (fresh dispatch) or startup reconcile
    (`resume=True`); the task runs in the background and the caller returns
    immediately (typically with HTTP 202 Accepted). The task is registered in
    `app.state.running_dispatches` so the GC can't drop it mid-run, and removed
    by a done-callback when complete.

    Pre-conditions enforced by the caller, not here:
      * Without `resume`, the ticket must be PENDING. The runner enforces this
        via its own atomic transition; if it's not PENDING, the runner raises
        and the ticket stays where it was. With `resume`, the runner accepts
        any non-terminal ticket.
      * `app.state.compute_backend_client` must be non-None. The route
        should 503 before reaching this if the orchestrator URL is unset.
    """
    if app.state.compute_backend_client is None:
        # Defensive — the route layer should have already 503'd. Raising
        # here surfaces the wiring bug rather than silently dropping work.
        raise RuntimeError(
            "schedule_dispatch called but compute_backend_client is not configured;"
            " set COMPUTE_ORCHESTRATOR_URL or block this route at the dependency layer"
        )

    task = asyncio.create_task(
        _run_and_log(app, work_ticket_idx, resume=resume),
        name=f"dispatch_ticket_{work_ticket_idx}",
    )
    app.state.running_dispatches.add(task)
    task.add_done_callback(app.state.running_dispatches.discard)
    return task


async def _inflight_ticket_idxs(pool: asyncpg.Pool) -> list[int]:
    """Every non-terminal (PENDING / QUEUED / PROCESSING) ticket id, ascending.
    These are the tickets a startup reconcile re-drives — at startup nothing
    else holds them."""
    rows = await pool.fetch(
        "SELECT work_ticket_idx FROM qiita.work_ticket"
        " WHERE state = ANY($1::qiita.work_ticket_state[])"
        " ORDER BY work_ticket_idx",
        list(NON_TERMINAL_WORK_TICKET_STATES),
    )
    return [r["work_ticket_idx"] for r in rows]


async def reconcile_inflight_tickets(app: FastAPI) -> int:
    """Re-attach every in-flight ticket at startup instead of failing it.

    Call from the lifespan startup hook *before* opening listeners. Any ticket
    in PENDING / QUEUED / PROCESSING is by definition orphaned — we just
    started; nothing else holds it. Each is re-dispatched through
    `run_workflow(resume=True)`, which fast-forwards already-completed entries
    (rebuilding their outputs from the shared workspace) and resumes the first
    incomplete one — re-attaching to a live SLURM job by its persisted id,
    finalizing one that succeeded while the CP was down, or deciding a purged
    job from its on-disk manifest. A CO outage during reconcile leaves the
    ticket PROCESSING and keeps retrying (the runner's poll loop never fails on
    an unreachable orchestrator), so a deploy that stops both services is safe.

    Resume policy (vs. the old fail-all sweep): deploys stop/start the CP
    undrained, so a restart with in-flight tickets is routine, not a crash —
    failing them all would nuke running work on every deploy.

    Single-CP-process contract: assumes no other CP process is concurrently
    dispatching against the same database. With multiple CP processes, one's
    startup reconcile would re-drive tickets another is actively running. CP HA
    needs fencing (owner column or advisory lock) before that assumption can be
    lifted; see docs/architecture.md "Work Ticket Lifecycle".

    Returns the number of tickets scheduled for resume, for logging.
    """
    idxs = await _inflight_ticket_idxs(app.state.pool)
    if not idxs:
        return 0
    if app.state.compute_backend_client is None:
        # No orchestrator configured → nothing can run a compute step, so we
        # can't resume. Leave the tickets in place (a CP without
        # COMPUTE_ORCHESTRATOR_URL shouldn't have in-flight compute tickets)
        # and surface it loudly rather than silently dropping work.
        _log.warning(
            "%d in-flight work_ticket(s) at startup but no compute_backend_client"
            " configured; cannot resume %s — set COMPUTE_ORCHESTRATOR_URL",
            len(idxs),
            idxs,
        )
        return 0
    _log.warning(
        "resuming %d in-flight work_ticket(s) at startup: %s",
        len(idxs),
        idxs,
    )
    for idx in idxs:
        schedule_dispatch(app, idx, resume=True)
    return len(idxs)


async def drain_running_dispatches(running: set[asyncio.Task], *, timeout_seconds: float) -> None:
    """Wait for in-flight dispatches at shutdown.

    Bounded by `timeout_seconds` so a stuck workflow can't block service
    restart. Anything still running after the deadline is cancelled; the
    cancellation leaves the ticket non-terminal (a CancelledError is not
    caught by the runner's `except Exception`), and the next CP startup
    re-attaches it via `reconcile_inflight_tickets`.

    Snapshots `running` at call time. Relies on FastAPI lifespan
    ordering — uvicorn closes the listener and finishes outstanding
    requests before yielding to the lifespan-exit block where this runs,
    so no new dispatches register after the snapshot."""
    if not running:
        return
    pending = list(running)
    _log.info(
        "draining %d in-flight dispatch task(s) (timeout=%.0fs)",
        len(pending),
        timeout_seconds,
    )
    _, still_pending = await asyncio.wait(pending, timeout=timeout_seconds)
    for task in still_pending:
        task.cancel()
    if still_pending:
        _log.warning(
            "cancelled %d dispatch task(s) that did not drain in time; "
            "their tickets will be re-attached by reconcile_inflight_tickets on next startup",
            len(still_pending),
        )


def build_compute_backend_client(
    *, base_url: str | None, token_path
) -> ComputeBackendClient | None:
    """Lifespan helper. Returns None when no orchestrator URL is set, so
    the rest of the app can branch on a single nullable flag."""
    if base_url is None:
        return None
    return ComputeBackendClient(base_url=base_url, api_token_path=token_path)
