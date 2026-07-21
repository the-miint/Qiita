"""Operator-cancel / reap of a work_ticket: flip it terminal, then scancel its
SLURM job(s).

The load-bearing ordering is **terminal-state-first, then scancel**. The runner's
poll loop calls `_raise_if_ticket_terminal` once per iteration and aborts on a
terminal state (`WorkflowAborted`), so committing the `cancelled` flip BEFORE the
reap is what stops the runner from submitting a *further* attempt — a scancel-first
(or scancel-without-flip) just gets the job re-submitted as the next attempt, the
job-past-the-block race the cancel command exists to kill.

The reap is best-effort AT THE INSTANT it runs, and idempotent on re-run — NOT an
absolute "no job can be in flight" guarantee. One residual window remains: if the
runner is already mid-`sbatch` when the flip commits (past its terminal check but
before slurmrestd registers the new job), that attempt can be launched and missed
by this reap's job-list read, and the runner then aborts on its next iteration,
leaving it orphaned. Re-running cancel reaps it (the scancel matches by prefix, so
it catches any attempt now visible); the orphan-reaping direction below is the
standing sweep that closes it without operator action.

The reap covers ALL attempts by the deterministic `qiita-wt{idx}-` job-name prefix
(`ComputeBackendClient.cancel`), not just the recorded `slurm_job_id`, because a
retriable failure bumps `attempt` and launches a fresh job under a new name.

This primitive is shared with the orphan-reaping direction (a ticket that went
terminal while its job kept running): the flip is a no-op on an already-terminal
ticket, but the scancel still runs — reaping any stray job — which is why cancel is
idempotent AND defensively reaps.
"""

from __future__ import annotations

import asyncpg
from qiita_common.compute_backend_client import ComputeBackendClient
from qiita_common.models import NON_TERMINAL_WORK_TICKET_STATES, WorkTicketState


class WorkTicketNotFound(Exception):
    """No work_ticket exists at the given idx."""

    def __init__(self, work_ticket_idx: int) -> None:
        super().__init__(f"no work_ticket with idx={work_ticket_idx}")
        self.work_ticket_idx = work_ticket_idx


async def cancel_work_ticket(
    pool: asyncpg.Pool,
    backend_client: ComputeBackendClient,
    work_ticket_idx: int,
) -> dict:
    """Cancel one work_ticket: flip it terminal (`cancelled`) if non-terminal, then
    scancel its live SLURM job(s). Idempotent.

    Returns a dict:
      * `work_ticket_idx`
      * `previous_state`   — the state before the flip
      * `state`            — `cancelled` if flipped, else `previous_state` (unchanged)
      * `cancelled`        — True iff this call flipped it (False = already terminal)
      * `cancelled_job_ids`— the SLURM job ids the reap scancelled (may be [])
      * `reap_error`       — a message iff the scancel failed AFTER the flip (the flip
                             stands; the operator can re-run cancel to retry the reap)

    Raises `WorkTicketNotFound` if the idx does not exist. The flip is a single
    `FOR UPDATE` transaction so a concurrent CP write can't interleave a half-state;
    the scancel runs only after that transaction commits (terminal-first)."""
    async with pool.acquire() as conn, conn.transaction():
        previous_state = await conn.fetchval(
            "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1 FOR UPDATE",
            work_ticket_idx,
        )
        if previous_state is None:
            raise WorkTicketNotFound(work_ticket_idx)
        flipped = previous_state in NON_TERMINAL_WORK_TICKET_STATES
        if flipped:
            # A cancelled ticket carries NULL failure_* (like no_data) — distinct
            # from failed. Clear any in-place-retry marker so the now-terminal
            # ticket doesn't show a stale "stuck since T" reason.
            await conn.execute(
                "UPDATE qiita.work_ticket"
                " SET state = 'cancelled', transient_reason = NULL, transient_since = NULL"
                " WHERE work_ticket_idx = $1",
                work_ticket_idx,
            )

    # Flip committed → the runner's poll loop aborts on its next terminal check, so
    # no new attempt spawns while we reap. A reap failure (CO unreachable) does NOT
    # undo the flip — the important half (stop driving) already landed; surface the
    # reap error so the operator can re-run cancel to reap the still-running job.
    reap_error: str | None = None
    cancelled_job_ids: list[int] = []
    try:
        cancelled_job_ids = await backend_client.cancel(work_ticket_idx)
    except Exception as exc:  # noqa: BLE001 — record any reap failure, don't lose the flip
        reap_error = f"{type(exc).__name__}: {exc}"

    return {
        "work_ticket_idx": work_ticket_idx,
        "previous_state": previous_state,
        "state": WorkTicketState.CANCELLED.value if flipped else previous_state,
        "cancelled": flipped,
        "cancelled_job_ids": cancelled_job_ids,
        "reap_error": reap_error,
    }
