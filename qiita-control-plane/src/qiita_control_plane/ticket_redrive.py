"""The two writes that put a finished work ticket back to PENDING for another run.

Both run inside the caller's transaction, after the caller's own checks, and neither
dispatches: the caller dispatches after commit.
"""

from __future__ import annotations

import asyncpg
from qiita_common.models import StepProgressState, WorkTicketState

from .repositories import require_transaction


async def reset_ticket_to_pending(
    conn: asyncpg.Connection, *, work_ticket_idx: int, from_state: str
) -> bool:
    """Set the ticket PENDING and clear what its last outcome left on it. Guarded on
    `from_state`, so a ticket that changed state since the caller read it is left
    alone: returns False then, True when the row was updated."""
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
        " RETURNING work_ticket_idx",
        WorkTicketState.PENDING.value,
        work_ticket_idx,
        from_state,
    )
    return updated is not None


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
