"""Work-ticket dispatch — fires `runner.run_workflow` as a background task.

The CP itself is the dispatcher (option C in the design discussion):
the route that creates a ticket also fires an `asyncio.create_task`,
and the workflow runs in-process. No polling worker, no separate dispatch
service. Long-running steps don't block the originating HTTP request.

Failure modes:

- If the CP restarts mid-dispatch, the ticket sits in PROCESSING with no
  live owner. `recover_orphaned_tickets` (called from lifespan startup)
  marks every non-terminal ticket as FAILED with a 'cp restarted' reason.
  Operators decide whether to redrive via `POST /work-ticket/{idx}/run`.

- If the runner raises, `run_workflow` itself transitions the ticket to
  FAILED. The done-callback installed here only handles task-level errors
  (asyncio cancellation, lost-pool, etc.) and logs them.

Auto-retry is not implemented in v1 — see task #11. Today every failure
ends up FAILED and requires a human `/run` to reset. State machine guard
(atomic conditional UPDATE in `runner._atomic_transition`) prevents
double-dispatch even if /run races with the implicit on-create dispatch.
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


async def _run_and_log(app: FastAPI, work_ticket_idx: int) -> None:
    """Inner task body: call `run_workflow` and log task-level errors.

    Workflow-level failures (a step raising) are already handled by
    `run_workflow` itself — it transitions the ticket to FAILED and
    re-raises. We catch the re-raise here only so the outer asyncio
    machinery doesn't see an "unhandled exception in task" warning;
    the ticket state is already correct."""
    try:
        await run_workflow(
            work_ticket_idx,
            app.state.pool,
            app.state.compute_backend_client,
            hmac_secret=app.state.settings.hmac_secret_key,
            data_plane_url=app.state.settings.data_plane_url,
        )
    except Exception:
        # run_workflow has already transitioned to FAILED. Log and swallow
        # so the asyncio task completes cleanly.
        _log.exception("dispatch_ticket %d failed (ticket marked FAILED by runner)", work_ticket_idx)


def schedule_dispatch(app: FastAPI, work_ticket_idx: int) -> asyncio.Task:
    """Fire-and-forget dispatch of one work ticket.

    Caller is the route handler; the task runs in the background and the
    caller returns immediately (typically with HTTP 202 Accepted). The
    task is registered in `app.state.running_dispatches` so the GC can't
    drop it mid-run, and removed by a done-callback when complete.

    Pre-conditions enforced by the caller, not here:
      * Ticket must be in PENDING state. The runner enforces this via its
        own atomic transition; if it's not PENDING, the runner raises and
        the ticket stays where it was.
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
        _run_and_log(app, work_ticket_idx),
        name=f"dispatch_ticket_{work_ticket_idx}",
    )
    app.state.running_dispatches.add(task)
    task.add_done_callback(app.state.running_dispatches.discard)
    return task


async def recover_orphaned_tickets(pool: asyncpg.Pool) -> int:
    """Mark every non-terminal ticket as FAILED at startup.

    Call from the lifespan startup hook *before* opening listeners. Any
    ticket in PENDING / QUEUED / PROCESSING is by definition orphaned —
    we just started; nothing else holds it. v1's recovery policy is
    fail-and-let-operator-decide rather than auto-resume, because the
    workflow library hasn't been audited for idempotency across the full
    step graph.

    Returns the number of tickets transitioned, for logging.
    """
    rows = await pool.fetch(
        "UPDATE qiita.work_ticket"
        " SET state = $1::qiita.work_ticket_state"
        " WHERE state IN ($2::qiita.work_ticket_state,"
        "                 $3::qiita.work_ticket_state,"
        "                 $4::qiita.work_ticket_state)"
        " RETURNING work_ticket_idx",
        WorkTicketState.FAILED.value,
        WorkTicketState.PENDING.value,
        WorkTicketState.QUEUED.value,
        WorkTicketState.PROCESSING.value,
    )
    if rows:
        _log.warning(
            "recovered %d orphaned work_ticket(s) by marking FAILED: %s",
            len(rows),
            [r["work_ticket_idx"] for r in rows],
        )
    return len(rows)


async def drain_running_dispatches(
    running: set[asyncio.Task], *, timeout_seconds: float
) -> None:
    """Wait for in-flight dispatches at shutdown.

    Bounded by `timeout_seconds` so a stuck workflow can't block service
    restart. Anything still running after the deadline is cancelled; the
    runner's exception handler then transitions the ticket to FAILED. The
    next CP startup will catch any leftover via `recover_orphaned_tickets`
    as a safety net."""
    if not running:
        return
    pending = list(running)
    _log.info("draining %d in-flight dispatch task(s) (timeout=%.0fs)", len(pending), timeout_seconds)
    done, still_pending = await asyncio.wait(pending, timeout=timeout_seconds)
    for task in still_pending:
        task.cancel()
    if still_pending:
        _log.warning(
            "cancelled %d dispatch task(s) that did not drain in time; "
            "their tickets will be picked up by recover_orphaned_tickets on next startup",
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
