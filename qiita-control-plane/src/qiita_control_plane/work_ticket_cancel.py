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

A `failed` ticket is cancellable too (`CANCELLABLE_WORK_TICKET_STATES`). A failed
ticket can still have a live job (`ticket_redrive.drop_dead_step_rows` says how), and
the reap below scancels it like any other. The reap names jobs
by ticket idx only and reads no workspace, so a ticket whose scratch workspace was
reclaimed or removed reaps the same way.

This primitive is shared with the orphan-reaping direction (a ticket that went
terminal while its job kept running): the flip is a no-op on a ticket whose state is
not in `CANCELLABLE_WORK_TICKET_STATES`, but the scancel still runs — reaping
any stray job — which is why cancel is idempotent AND defensively reaps.
"""

from __future__ import annotations

import logging

import asyncpg
from qiita_common.compute_backend_client import ComputeBackendClient
from qiita_common.models import NON_TERMINAL_WORK_TICKET_STATES, WorkTicketState

_log = logging.getLogger(__name__)

# The states `cancel_work_ticket` flips to `cancelled`: the non-terminal ones, and
# `failed`, so that an operator can withdraw a failed ticket, which then reads as a
# deliberate stop rather than a failure. A ticket in any other state is left as it is
# and only reaped.
CANCELLABLE_WORK_TICKET_STATES: tuple[str, ...] = (
    *NON_TERMINAL_WORK_TICKET_STATES,
    WorkTicketState.FAILED.value,
)


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
    """Cancel one work_ticket: flip it to `cancelled` if its state is in
    `CANCELLABLE_WORK_TICKET_STATES`, then scancel its live SLURM job(s). Idempotent.

    Returns a dict:
      * `work_ticket_idx`
      * `previous_state`   — the state before the flip
      * `state`            — `cancelled` if flipped, else `previous_state` (unchanged)
      * `cancelled`        — True iff this call flipped it (False = its state is not
                             in `CANCELLABLE_WORK_TICKET_STATES`)
      * `cancelled_job_ids`— the SLURM job ids the reap scancelled (may be [])
      * `reap_error`       — a message iff the scancel failed AFTER the flip (the flip
                             stands; the operator can re-run cancel to retry the reap).
                             Also recorded on the ticket (`reap_failed_at`,
                             `reap_failure`), where it blocks a redrive until a later
                             cancel reaps without error and clears it.

    Raises `WorkTicketNotFound` if the idx does not exist. The flip is a single
    `FOR UPDATE` transaction so a concurrent CP write can't interleave a half-state;
    the scancel runs only after that transaction commits (terminal-first)."""
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            "SELECT state, failure_type, failure_stage, failure_step_name, failure_reason"
            " FROM qiita.work_ticket WHERE work_ticket_idx = $1 FOR UPDATE",
            work_ticket_idx,
        )
        if row is None:
            raise WorkTicketNotFound(work_ticket_idx)
        previous_state = row["state"]
        flipped = previous_state in CANCELLABLE_WORK_TICKET_STATES
        if flipped:
            if previous_state == WorkTicketState.FAILED.value:
                # work_ticket_failure_consistent requires NULL failure_* on every
                # state but failed, so the flip below drops them; the log keeps them.
                _log.info(
                    "cancelling failed work_ticket %d; clearing failure_type=%s"
                    " failure_stage=%s failure_step_name=%s failure_reason=%r",
                    work_ticket_idx,
                    row["failure_type"],
                    row["failure_stage"],
                    row["failure_step_name"],
                    row["failure_reason"],
                )
            # Clear any in-place-retry marker too, so the now-terminal ticket doesn't
            # show a stale "stuck since T" reason.
            await conn.execute(
                "UPDATE qiita.work_ticket"
                " SET state = $2::qiita.work_ticket_state,"
                "     failure_type = NULL, failure_stage = NULL,"
                "     failure_step_name = NULL, failure_reason = NULL,"
                "     transient_reason = NULL, transient_since = NULL"
                " WHERE work_ticket_idx = $1",
                work_ticket_idx,
                WorkTicketState.CANCELLED.value,
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
    # Recorded on the ticket so a redrive refuses it until a reap succeeds
    # (`ticket_redrive.ReapFailed`); a reap without error clears it.
    await pool.execute(
        "UPDATE qiita.work_ticket"
        " SET reap_failed_at = CASE WHEN $2::text IS NULL THEN NULL ELSE now() END,"
        "     reap_failure = $2::text"
        " WHERE work_ticket_idx = $1",
        work_ticket_idx,
        reap_error,
    )

    return {
        "work_ticket_idx": work_ticket_idx,
        "previous_state": previous_state,
        "state": WorkTicketState.CANCELLED.value if flipped else previous_state,
        "cancelled": flipped,
        "cancelled_job_ids": cancelled_job_ids,
        "reap_error": reap_error,
    }
