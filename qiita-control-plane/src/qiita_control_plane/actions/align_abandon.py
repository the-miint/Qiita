"""Abandon an align plan's unfinished blocks so their samples can be re-planned.

The align planner refuses a fresh plan for any sample already gated under the
resolved alignment (`AlignResubmitError`), and `only_missing=true` skips every gated
sample — including the PENDING gates a failed plan leaves behind. Before the
alignment lifecycle the only way out was deleting the whole alignment, which also
removed every sample that had completed. This is the narrower tool: it gives up on
the blocks that did not complete and clears only the pending gates they covered.

What it does, in one transaction:

1. Lock the alignment_definition row (404 if absent) and the align fan-out cohort's
   advisory lock, so no pump releases a held ticket while this reads them.
2. Collect the blocks under this alignment that are neither 'completed' nor already
   'abandoned'. Refuse (`AlignPlanInFlight`) if any of their tickets was released
   and is still pending/queued/processing: a running block would register rows and
   reconcile after its gates were cleared.
3. Cancel the still-held tickets (never released, so there is no SLURM job to reap)
   and set every collected block 'abandoned'.
4. Collect the samples whose 'pending' gate one of those blocks covered, purge their
   DuckLake `alignment` rows across every block of this alignment that covered them
   (a completed sibling block of a split sample registered part of it), then delete
   those gate rows.

The purge runs before the commit and a FlightError rolls everything back, so a
failure leaves the plan as it was. Completed and invalidated gates are never
touched. An abandoned block stays as the record of the attempt; its ticket's /run is
refused (routes/work_ticket.py) and its write steps refuse it (actions/library.py).
"""

from __future__ import annotations

import asyncpg
from qiita_common.models import WorkTicketState

from ..fanout_dispatch import align_block_cohort
from ..repositories.alignment_definition import ALIGNMENT_SAMPLE_PENDING
from ..repositories.block import BLOCK_ABANDONED
from .library import delete_alignment_block_data

_RELEASED_IN_FLIGHT = (
    WorkTicketState.PENDING.value,
    WorkTicketState.QUEUED.value,
    WorkTicketState.PROCESSING.value,
)


class AlignmentNotFound(Exception):
    def __init__(self, alignment_idx: int) -> None:
        self.alignment_idx = alignment_idx
        super().__init__(f"no alignment_definition with alignment_idx={alignment_idx}")


class AlignPlanInFlight(Exception):
    """A block of the plan is running, so its gates cannot be cleared under it."""

    def __init__(self, alignment_idx: int, work_ticket_idxs: list[int]) -> None:
        self.alignment_idx = alignment_idx
        self.work_ticket_idxs = work_ticket_idxs
        super().__init__(
            f"alignment {alignment_idx} has {len(work_ticket_idxs)} block ticket(s) still"
            f" running: {work_ticket_idxs}. Wait for them to finish or cancel them"
            " (POST /work-ticket/cancel), then abandon the plan"
        )


async def abandon_align_plan(
    pool: asyncpg.Pool,
    *,
    alignment_idx: int,
    signing_key: bytes,
    data_plane_url: str,
) -> dict:
    """See the module docstring. Returns the fields of `AlignmentPlanAbandonResponse`.

    Raises AlignmentNotFound, AlignPlanInFlight, or pyarrow.flight.FlightError (the
    purge failed; nothing was changed)."""
    cohort = align_block_cohort(alignment_idx)
    async with pool.acquire() as conn, conn.transaction():
        found = await conn.fetchval(
            "SELECT 1 FROM qiita.alignment_definition WHERE alignment_idx = $1 FOR UPDATE",
            alignment_idx,
        )
        if found is None:
            raise AlignmentNotFound(alignment_idx)
        await conn.execute(
            "SELECT pg_advisory_xact_lock($1, $2)", cohort.lock_class, cohort.lock_key
        )

        blocks = await conn.fetch(
            "SELECT b.block_idx, wt.work_ticket_idx, wt.state::text AS state,"
            "       wt.dispatch_held"
            "  FROM qiita.block b"
            "  JOIN qiita.work_ticket wt ON wt.work_ticket_idx = b.work_ticket_idx"
            " WHERE wt.alignment_idx = $1 AND b.state NOT IN ('completed', $2)"
            " ORDER BY b.block_idx"
            " FOR UPDATE OF b, wt",
            alignment_idx,
            BLOCK_ABANDONED,
        )
        running = [
            r["work_ticket_idx"]
            for r in blocks
            if not r["dispatch_held"] and r["state"] in _RELEASED_IN_FLIGHT
        ]
        if running:
            raise AlignPlanInFlight(alignment_idx, running)

        block_idxs = [r["block_idx"] for r in blocks]
        held = [
            r["work_ticket_idx"]
            for r in blocks
            if r["dispatch_held"] and r["state"] in _RELEASED_IN_FLIGHT
        ]
        if held:
            # Same flip as work_ticket_cancel.cancel_work_ticket, without its
            # scancel: a held ticket was never released, so no job exists.
            await conn.execute(
                "UPDATE qiita.work_ticket"
                "   SET state = 'cancelled', transient_reason = NULL, transient_since = NULL"
                " WHERE work_ticket_idx = ANY($1::bigint[])",
                held,
            )
        if block_idxs:
            await conn.execute(
                "UPDATE qiita.block SET state = $2 WHERE block_idx = ANY($1::bigint[])",
                block_idxs,
                BLOCK_ABANDONED,
            )

        cleared = [
            r["prep_sample_idx"]
            for r in await conn.fetch(
                "SELECT als.prep_sample_idx FROM qiita.alignment_sample als"
                " WHERE als.alignment_idx = $1 AND als.state = $2"
                "   AND EXISTS (SELECT 1 FROM qiita.block_member bm"
                "                WHERE bm.prep_sample_idx = als.prep_sample_idx"
                "                  AND bm.block_idx = ANY($3::bigint[]))"
                " ORDER BY als.prep_sample_idx"
                " FOR UPDATE",
                alignment_idx,
                ALIGNMENT_SAMPLE_PENDING,
                block_idxs,
            )
        ]

        rows_deleted = 0
        if cleared:
            members = [
                {
                    "prep_sample_idx": r["prep_sample_idx"],
                    "sequence_idx_start": r["min_sequence_idx"],
                    "sequence_idx_stop": r["max_sequence_idx"],
                }
                for r in await conn.fetch(
                    "SELECT bm.prep_sample_idx, bm.min_sequence_idx, bm.max_sequence_idx"
                    "  FROM qiita.block_member bm"
                    "  JOIN qiita.block b ON b.block_idx = bm.block_idx"
                    "  JOIN qiita.work_ticket wt ON wt.work_ticket_idx = b.work_ticket_idx"
                    " WHERE wt.alignment_idx = $1 AND bm.prep_sample_idx = ANY($2::bigint[])"
                    " ORDER BY bm.prep_sample_idx, bm.min_sequence_idx",
                    alignment_idx,
                    cleared,
                )
            ]
            rows_deleted = await delete_alignment_block_data(
                alignment_idx=alignment_idx,
                members=members,
                signing_key=signing_key,
                data_plane_url=data_plane_url,
            )
            await conn.execute(
                "DELETE FROM qiita.alignment_sample"
                " WHERE alignment_idx = $1 AND prep_sample_idx = ANY($2::bigint[])"
                "   AND state = $3",
                alignment_idx,
                cleared,
                ALIGNMENT_SAMPLE_PENDING,
            )

    return {
        "alignment_idx": alignment_idx,
        "abandoned_block_idx": block_idxs,
        "cancelled_work_ticket_idx": held,
        "cleared_prep_sample_idx": cleared,
        "rows_deleted": rows_deleted,
    }
