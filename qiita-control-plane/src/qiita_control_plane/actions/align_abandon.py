"""Abandon an align plan's unfinished blocks so their prep_samples can be re-planned.

The align planner refuses a fresh plan for any prep_sample already gated under the
resolved alignment (`AlignResubmitError`), and `only_missing=true` skips every gated
prep_sample — including the PENDING gates a failed plan leaves behind. This gives up on
the blocks that did not complete and clears only the pending gates they covered.

Two phases.

The first is one Postgres transaction:

1. Lock the alignment_definition row (404 if absent) and the align fan-out cohort's
   advisory lock, so no pump releases a held ticket while this reads them.
2. Collect the blocks under this alignment that are neither 'completed' nor already
   'abandoned', locking them and their tickets. A ticket is held when it is flagged
   `dispatch_held` and still 'pending'. Refuse (`AlignPlanInFlight`) if any other
   ticket is non-terminal: it may have a job, and a running block would register
   rows and reconcile after its gates were cleared.
3. Cancel the held tickets (never released, so there is no SLURM job to reap),
   set every collected block 'abandoned', and delete the 'pending' gates those blocks
   covered.

The second purges DuckLake, after the commit: the `alignment` rows of every
prep_sample an abandoned block covered that now has no gate under this alignment,
across every block of the alignment that covered it (a completed sibling block of a
split prep_sample registered part of it). Ordering it after the commit means a crash
can leave rows unpurged under no gate, which nothing reads, but never purged rows
under a gate a redrive could still complete. The purge set is derived from state
rather than carried from phase one, so re-running the route after a failed purge
redoes it; a prep_sample re-planned in the meantime has a gate again and drops out,
and its new blocks replace their own footprint.

Completed and invalidated gates are never touched. An abandoned block stays as the
record of the attempt; its ticket's /run is refused (routes/work_ticket.py) and its
write steps refuse it (actions/library.py).
"""

from __future__ import annotations

import asyncpg
from qiita_common.api_paths import URL_WORK_TICKET_CANCEL
from qiita_common.models import NON_TERMINAL_WORK_TICKET_STATES, WorkTicketState

from ..fanout_dispatch import align_block_cohort
from ..repositories.alignment_definition import (
    ALIGNMENT_SAMPLE_PENDING,
    AlignmentDefinitionNotFound,
)
from ..repositories.block import BLOCK_ABANDONED, BLOCK_COMPLETED
from .library import delete_alignment_block_data


class AlignPlanInFlight(Exception):
    """A block of the plan is running, so its gates cannot be cleared under it."""

    def __init__(self, alignment_idx: int, work_ticket_idxs: list[int]) -> None:
        self.alignment_idx = alignment_idx
        self.work_ticket_idxs = work_ticket_idxs
        super().__init__(
            f"alignment {alignment_idx} has {len(work_ticket_idxs)} block ticket(s) still"
            f" running: {work_ticket_idxs}. Wait for them to finish or cancel them"
            f" (POST {URL_WORK_TICKET_CANCEL}), then abandon the plan"
        )


async def abandon_align_plan(
    pool: asyncpg.Pool,
    *,
    alignment_idx: int,
    signing_key: bytes,
    data_plane_url: str,
) -> dict:
    """See the module docstring. Returns the fields of `AlignmentPlanAbandonResponse`.

    Raises AlignmentDefinitionNotFound, AlignPlanInFlight, or pyarrow.flight.FlightError (the
    purge failed after the abandon committed; re-running this redoes the purge)."""
    cohort = align_block_cohort(alignment_idx)
    in_flight = NON_TERMINAL_WORK_TICKET_STATES
    async with pool.acquire() as conn, conn.transaction():
        found = await conn.fetchval(
            "SELECT 1 FROM qiita.alignment_definition WHERE alignment_idx = $1 FOR UPDATE",
            alignment_idx,
        )
        if found is None:
            raise AlignmentDefinitionNotFound(alignment_idx)
        await conn.execute(
            "SELECT pg_advisory_xact_lock($1, $2)", cohort.lock_class, cohort.lock_key
        )

        blocks = await conn.fetch(
            "SELECT b.block_idx, wt.work_ticket_idx, wt.state::text AS state,"
            "       wt.dispatch_held"
            "  FROM qiita.block b"
            "  JOIN qiita.work_ticket wt ON wt.work_ticket_idx = b.work_ticket_idx"
            " WHERE wt.alignment_idx = $1 AND b.state <> ALL($2::text[])"
            " ORDER BY b.block_idx"
            " FOR UPDATE OF b, wt",
            alignment_idx,
            [BLOCK_COMPLETED, BLOCK_ABANDONED],
        )
        # The flag alone is not enough: a row can carry `dispatch_held` past
        # 'pending', and such a row may have a job, so it counts as running.
        held = [
            r["work_ticket_idx"]
            for r in blocks
            if r["dispatch_held"] and r["state"] == WorkTicketState.PENDING.value
        ]
        running = [
            r["work_ticket_idx"]
            for r in blocks
            if r["state"] in in_flight and r["work_ticket_idx"] not in held
        ]
        if running:
            raise AlignPlanInFlight(alignment_idx, running)

        block_idxs = [r["block_idx"] for r in blocks]
        if held:
            # The flip of work_ticket_cancel.cancel_work_ticket without its scancel: a
            # held ticket was never released, so no job exists.
            await conn.execute(
                "UPDATE qiita.work_ticket"
                "   SET state = $2::qiita.work_ticket_state,"
                "       transient_reason = NULL, transient_since = NULL"
                " WHERE work_ticket_idx = ANY($1::bigint[])",
                held,
                WorkTicketState.CANCELLED.value,
            )
        cleared: list[int] = []
        if block_idxs:
            await conn.execute(
                "UPDATE qiita.block SET state = $2 WHERE block_idx = ANY($1::bigint[])",
                block_idxs,
                BLOCK_ABANDONED,
            )
            cleared = [
                r["prep_sample_idx"]
                for r in await conn.fetch(
                    "DELETE FROM qiita.alignment_sample als"
                    " WHERE als.alignment_idx = $1 AND als.state = $2"
                    "   AND EXISTS (SELECT 1 FROM qiita.block_member bm"
                    "                WHERE bm.prep_sample_idx = als.prep_sample_idx"
                    "                  AND bm.block_idx = ANY($3::bigint[]))"
                    " RETURNING als.prep_sample_idx",
                    alignment_idx,
                    ALIGNMENT_SAMPLE_PENDING,
                    block_idxs,
                )
            ]

    rows_deleted = await purge_abandoned_prep_samples(
        pool,
        alignment_idx=alignment_idx,
        signing_key=signing_key,
        data_plane_url=data_plane_url,
    )
    return {
        "alignment_idx": alignment_idx,
        "abandoned_block_idx": block_idxs,
        "cancelled_work_ticket_idx": held,
        "cleared_prep_sample_idx": sorted(cleared),
        "rows_deleted": rows_deleted,
    }


async def purge_abandoned_prep_samples(
    pool: asyncpg.Pool, *, alignment_idx: int, signing_key: bytes, data_plane_url: str
) -> int:
    """Purge the DuckLake rows of every prep_sample an abandoned block of this
    alignment covered that has no gate under it now, and return the row count. 0 with
    no Flight call when there is nothing to purge."""
    members = [
        {
            "prep_sample_idx": r["prep_sample_idx"],
            "sequence_idx_start": r["min_sequence_idx"],
            "sequence_idx_stop": r["max_sequence_idx"],
        }
        for r in await pool.fetch(
            "SELECT bm.prep_sample_idx, bm.min_sequence_idx, bm.max_sequence_idx"
            "  FROM qiita.block_member bm"
            "  JOIN qiita.block b ON b.block_idx = bm.block_idx"
            "  JOIN qiita.work_ticket wt ON wt.work_ticket_idx = b.work_ticket_idx"
            " WHERE wt.alignment_idx = $1"
            "   AND bm.prep_sample_idx IN ("
            "       SELECT abm.prep_sample_idx FROM qiita.block_member abm"
            "         JOIN qiita.block ab ON ab.block_idx = abm.block_idx"
            "         JOIN qiita.work_ticket awt ON awt.work_ticket_idx = ab.work_ticket_idx"
            "        WHERE awt.alignment_idx = $1 AND ab.state = $2)"
            "   AND NOT EXISTS (SELECT 1 FROM qiita.alignment_sample als"
            "                    WHERE als.alignment_idx = $1"
            "                      AND als.prep_sample_idx = bm.prep_sample_idx)"
            " ORDER BY bm.prep_sample_idx, bm.min_sequence_idx",
            alignment_idx,
            BLOCK_ABANDONED,
        )
    ]
    if not members:
        return 0
    return await delete_alignment_block_data(
        alignment_idx=alignment_idx,
        members=members,
        signing_key=signing_key,
        data_plane_url=data_plane_url,
    )
