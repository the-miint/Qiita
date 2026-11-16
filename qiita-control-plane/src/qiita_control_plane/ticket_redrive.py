"""The two writes that put a finished work ticket back to PENDING for another run.

Both run inside the caller's transaction, after the caller's own checks, and neither
dispatches: the caller dispatches after commit.
"""

from __future__ import annotations

import asyncpg
from qiita_common.models import StepProgressState, WorkTicketState

from .repositories import require_transaction
from .work_ticket_cancel import REAP_IN_FLIGHT


class ReapPending(Exception):
    """A cancel's reap of the ticket's SLURM jobs has not succeeded: it is running, was
    interrupted, or failed (`work_ticket.reap_pending_since`, `reap_note`). A job it has
    not reached may still be running under the ticket, so the ticket is not redriven
    until a cancel reaps without error."""

    # The `reason` code of the 409 every redrive route answers it with.
    REASON = "reap_pending"

    def __init__(self, work_ticket_idx: int, pending_since: object, note: str) -> None:
        self.work_ticket_idx = work_ticket_idx
        self.pending_since = pending_since
        self.note = note
        super().__init__(self.message(with_note=False))

    def message(self, *, with_note: bool) -> str:
        """The refusal text. A failed reap's note (the orchestrator client's error) is
        included only when `with_note`; who may see it is the routes' call
        (`routes/work_ticket.sees_reap_note`)."""
        idx, since = self.work_ticket_idx, self.pending_since
        if self.note == REAP_IN_FLIGHT:
            what = (
                f"the reap of its SLURM jobs by a cancel started at {since} has not"
                " finished: it is running, or the cancel was interrupted"
            )
        else:
            what = f"the reap of its SLURM jobs by a cancel started at {since} failed"
            if with_note:
                what += f": {self.note}"
        return (
            f"work_ticket {idx}: {what}. It is not redriven until a reap"
            f" succeeds; reap it with `qiita-admin ticket cancel {idx}`"
            f" (POST /work-ticket/cancel with work_ticket_idxs [{idx}])"
        )


async def reset_ticket_to_pending(
    conn: asyncpg.Connection, *, work_ticket_idx: int, from_state: str
) -> bool:
    """Set the ticket PENDING and clear what its last outcome left on it. Guarded on
    `from_state`, so a ticket that changed state since the caller read it is left
    alone: returns False then, True when the row was updated. Raises `ReapPending`
    while a cancel's reap of the ticket has not succeeded, so every
    redrive path refuses it."""
    require_transaction(conn)
    updated = await conn.fetchval(
        "UPDATE qiita.work_ticket"
        " SET state = $1::qiita.work_ticket_state,"
        "     retry_count = 0,"
        "     failure_type = NULL,"
        "     failure_stage = NULL,"
        "     failure_step_name = NULL,"
        "     failure_reason = NULL,"
        # The last outcome may already have been notified; the next run's own
        # terminal state must be notified too.
        "     notified_at = NULL,"
        "     notify_attempts = 0,"
        # The caller dispatches the ticket itself, so it is no longer held: the
        # pump must not release it a second time, and abandon-plan must see it as
        # running (`actions/align_abandon.py`).
        "     dispatch_held = false"
        " WHERE work_ticket_idx = $2"
        "   AND state = $3::qiita.work_ticket_state"
        "   AND reap_pending_since IS NULL"
        " RETURNING work_ticket_idx",
        WorkTicketState.PENDING.value,
        work_ticket_idx,
        from_state,
    )
    if updated is not None:
        return True
    # Read only on refusal, so a redrive that goes through runs one statement here.
    reap = await conn.fetchrow(
        "SELECT reap_pending_since, reap_note FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    if reap is not None and reap["reap_pending_since"] is not None:
        raise ReapPending(work_ticket_idx, reap["reap_pending_since"], reap["reap_note"])
    return False


async def drop_dead_step_rows(
    conn: asyncpg.Connection, *, work_ticket_idx: int, after_cancel: bool
) -> int:
    """Delete the progress rows a redrive must not keep, and return how many went.

    `completed` survives, so the runner still fast-forwards finished entries. Of
    the rest, the question is whether the row still names a job worth adopting —
    `_adopt_or_submit` re-attaches by the persisted `slurm_job_id`, and the runner
    may only adopt a LIVE attempt.

      * `failed` — always dropped. Re-using that attempt number would collide
        (record_submitted rejects anything but submitting→, and record_failed
        refuses failed→failed), wedging the redrive on the dead row. Dropped, the
        runner re-enters the entry, finds the orphaned on-disk attempt dir and
        advances past it to a fresh one (it can neither reuse the stale read-only
        output nor delete the SLURM-job-owned dir — see `_attempt_is_unowned`).
      * in-flight WITH a job id — kept when redriving a FAILED ticket. A FAILED
        ticket CAN have a live job: when a step OOM-kills, the runner escalates and
        submits attempt N+1, so a ticket that then fails leaves that attempt live
        with a real id. Dropping it would strand the job outright — the CP forgets
        the id, nothing polls it, and no later redrive can reclaim it.
      * in-flight WITHOUT a job id (a `submitting` write-ahead row) — dropped. Its
        only handle is the deterministic `job_name`, and the find-by-name orphan
        closer that would use it runs solely under `resume=True`; a redrive
        dispatches with resume=False, so a kept row would fall through to a fresh
        submit writing into the same attempt dir a possibly-live orphan is already
        using. Dropping it restores the advance-to-a-fresh-dir guard.

    `after_cancel` is the carve-out: cancel is terminal-flip-then-scancel, and the
    runner's abort path stops without recording the step row terminal — so a
    cancelled ticket keeps an in-flight row naming a job the reap already killed.
    Adopting it would re-attach to a dead job, so a post-cancel redrive drops every
    non-completed row."""
    require_transaction(conn)
    status = await conn.execute(
        "DELETE FROM qiita.work_ticket_step"
        " WHERE work_ticket_idx = $1"
        "   AND state <> $2"
        "   AND ($3::boolean OR state = $4 OR slurm_job_id IS NULL)",
        work_ticket_idx,
        StepProgressState.COMPLETED.value,
        after_cancel,
        StepProgressState.FAILED.value,
    )
    return int(status.split()[-1])
