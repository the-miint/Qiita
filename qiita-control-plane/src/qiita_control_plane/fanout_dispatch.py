"""Control-plane fan-out dispatch throttle — the "pump".

A fan-out action inserts MANY child work_tickets at once:

  * sharded reference-index build  — up to ~1000 (one per shard) for a big
    reference (`shard_orchestration.plan_and_submit_shards`);
  * bulk read-mask block           — one per read block of a pool
    (`block_planner.plan_and_submit_blocks`);
  * bulk sharded-alignment block   — one per align block of a pool
    (`align_planner.plan_and_submit_alignments`).

Dispatching them all at once opens that many concurrent data-plane DoGet streams
against a single data-plane instance. That is exactly what took down the WOL3
(reference 16) build: each shard stream opens ~all of the reference's chunk part
files, so ~1000 concurrent streams exhausted the data plane's file descriptors
("Too many open files") and the submission backlog outlived the ~1h Flight
ticket lifetime ("ticket expired"). The router build, running concurrently,
died the same way.

This module bounds the concurrency. Each fan-out INSERTs its children
`dispatch_held = true` (durable + reconcile-visible, but NOT dispatched) and
this pump releases them a capped number at a time:

  * A cohort's slot is occupied by a ticket that is non-terminal AND NOT held —
    i.e. one the pump has already released and that is actually
    pending-submit / queued / processing. A HELD ticket occupies no slot. So
    "running" reflects work that is genuinely in flight, not merely rows that
    exist.
  * `top_up_dispatch` releases up to `max_inflight - running` held tickets. It
    is called once by the fan-out itself (initial fill) and again on every
    terminal transition of a child (see `dispatch._run_and_log`), so the fan-out
    advances exactly as fast as children finish — self-clocking, no timer.
  * FAIL-STOP: if ANY ticket in the cohort is `failed`, the pump releases
    nothing. One failing child halts the whole fan-out rather than burning
    through the remaining shards against a sick backend. The operator
    investigates and redrives the failed child(ren) directly (a `/run` redrive
    dispatches a specific ticket regardless of `dispatch_held`); once no failed
    ticket remains, the next child completion re-starts the pump automatically.

Startup reconcile (`dispatch.reconcile_inflight_tickets`) re-dispatches only
non-held in-flight tickets and then calls the pump for every cohort that still
has held tickets — so the throttle survives a CP restart (it does not
re-dispatch the whole held backlog) and a crash between the last completion and
its top-up is covered.

Concurrency: `top_up_dispatch` takes a per-cohort Postgres transaction-level
advisory lock, so two pumps for the same cohort can't both read the same
free-slot count and over-release. The lock identity is (class, key); a distinct
class per cohort type keeps e.g. reference_idx=5 and mask_idx=5 from serialising
against each other. Key collisions across DIFFERENT cohorts are harmless (they'd
only serialise two unrelated pumps briefly) because every query is still scoped
by the cohort's own predicate — the lock is a correctness aid for same-cohort
races only.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import asyncpg

_log = logging.getLogger(__name__)

# Default per-cohort in-flight cap, the single source of truth for the number.
# `config.Settings` reads it as the FANOUT_MAX_INFLIGHT env-var fallback; the
# runner threads it as the default when a caller (a test) doesn't pass one.
DEFAULT_FANOUT_MAX_INFLIGHT = 8

# Advisory-lock class per cohort type (arbitrary distinct 31-bit ints). Paired
# with the cohort id as pg_advisory_xact_lock(class, key).
_LOCK_CLASS_SHARD = 0x0FA0_0001
_LOCK_CLASS_READ_MASK_BLOCK = 0x0FA0_0002
_LOCK_CLASS_ALIGN_BLOCK = 0x0FA0_0003

# pg advisory-lock keys are int4; mask the (bigint) cohort id into positive
# int4. A wrap collision only serialises two unrelated cohorts of the SAME type
# for a moment — harmless (see module docstring).
_INT4_MASK = 0x7FFF_FFFF


@dataclass(frozen=True, slots=True)
class FanoutCohort:
    """One fan-out's child-ticket set: a SQL predicate over qiita.work_ticket
    (positional ``$1..`` placeholders, filled by ``args``) plus the advisory-lock
    identity that serialises pumps for it. The predicate must select exactly the
    fan-out's children and nothing else."""

    label: str
    where_sql: str
    args: tuple[Any, ...]
    lock_class: int
    lock_key: int


def shard_cohort(reference_idx: int) -> FanoutCohort:
    """The sharded-index build children of one reference."""
    return FanoutCohort(
        label=f"shard(reference_idx={reference_idx})",
        where_sql="reference_idx = $1 AND shard_id IS NOT NULL",
        args=(reference_idx,),
        lock_class=_LOCK_CLASS_SHARD,
        lock_key=reference_idx & _INT4_MASK,
    )


def read_mask_block_cohort(mask_idx: int) -> FanoutCohort:
    """The bulk read-mask block children of one mask partition. Discriminated
    from align blocks by ``alignment_idx IS NULL`` (a read-mask block carries no
    alignment)."""
    return FanoutCohort(
        label=f"read_mask_block(mask_idx={mask_idx})",
        where_sql="mask_idx = $1 AND block_idx IS NOT NULL AND alignment_idx IS NULL",
        args=(mask_idx,),
        lock_class=_LOCK_CLASS_READ_MASK_BLOCK,
        lock_key=mask_idx & _INT4_MASK,
    )


def align_block_cohort(alignment_idx: int) -> FanoutCohort:
    """The bulk sharded-alignment block children of one alignment."""
    return FanoutCohort(
        label=f"align_block(alignment_idx={alignment_idx})",
        where_sql="alignment_idx = $1 AND block_idx IS NOT NULL",
        args=(alignment_idx,),
        lock_class=_LOCK_CLASS_ALIGN_BLOCK,
        lock_key=alignment_idx & _INT4_MASK,
    )


def cohort_for_ticket_row(row: asyncpg.Record | dict[str, Any]) -> FanoutCohort | None:
    """Derive the fan-out cohort of a work_ticket from its discriminating
    columns, or None if the ticket is not a fan-out child. The row must carry
    ``reference_idx``, ``shard_id``, ``block_idx``, ``mask_idx``,
    ``alignment_idx``. The order matches the three fan-out INSERT shapes:

      * shard build  → reference_idx scope + shard_id set;
      * align block  → block_idx set + alignment_idx set;
      * read-mask block → block_idx set + mask_idx set (alignment_idx NULL).
    """
    if row["shard_id"] is not None and row["reference_idx"] is not None:
        return shard_cohort(row["reference_idx"])
    if row["block_idx"] is not None:
        if row["alignment_idx"] is not None:
            return align_block_cohort(row["alignment_idx"])
        if row["mask_idx"] is not None:
            return read_mask_block_cohort(row["mask_idx"])
    return None


_TICKET_COHORT_COLUMNS = "reference_idx, shard_id, block_idx, mask_idx, alignment_idx"


async def cohort_for_work_ticket(pool: asyncpg.Pool, work_ticket_idx: int) -> FanoutCohort | None:
    """Load a ticket's discriminating columns and return its fan-out cohort (or
    None for a non-fan-out ticket / unknown idx)."""
    row = await pool.fetchrow(
        f"SELECT {_TICKET_COHORT_COLUMNS} FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    if row is None:
        return None
    return cohort_for_ticket_row(row)


async def top_up_dispatch(
    pool: asyncpg.Pool,
    cohort: FanoutCohort,
    *,
    max_inflight: int,
    dispatch_cb: Callable[[int], Any],
) -> list[int]:
    """Release up to ``max_inflight - running`` held tickets in ``cohort`` and
    dispatch each — unless the cohort has any failed ticket, in which case
    release nothing (fail-stop). Returns the freshly-released
    ``work_ticket_idx`` list (possibly empty).

    Idempotent to redundant calls: the per-cohort advisory lock serialises
    concurrent pumps, and the returned set is exactly the rows this call flipped
    from held to released. Dispatch fires post-commit, so a released ticket is
    durable before its background task starts."""
    where = cohort.where_sql
    limit_placeholder = len(cohort.args) + 1
    async with pool.acquire() as conn, conn.transaction():
        # Serialise pumps for THIS cohort so two can't both see the same free
        # slots and over-release. Transaction-scoped: auto-released on commit.
        await conn.execute(
            "SELECT pg_advisory_xact_lock($1, $2)", cohort.lock_class, cohort.lock_key
        )

        # Fail-stop circuit breaker: one failed child halts the fan-out.
        has_failed = await conn.fetchval(
            f"SELECT EXISTS (SELECT 1 FROM qiita.work_ticket WHERE {where} AND state = 'failed')",
            *cohort.args,
        )
        if has_failed:
            _log.info(
                "fan-out pump %s: fail-stop (a child is failed); releasing nothing",
                cohort.label,
            )
            return []

        # Occupied slots = released (NOT held) tickets still in flight. A held
        # ticket occupies no slot; a just-released-but-still-'pending' one does.
        running = await conn.fetchval(
            f"SELECT count(*) FROM qiita.work_ticket"
            f" WHERE {where} AND NOT dispatch_held"
            f"   AND state IN ('pending', 'queued', 'processing')",
            *cohort.args,
        )
        slots = max_inflight - int(running)
        if slots <= 0:
            return []

        released = await conn.fetch(
            f"UPDATE qiita.work_ticket SET dispatch_held = false"
            f" WHERE work_ticket_idx IN ("
            f"   SELECT work_ticket_idx FROM qiita.work_ticket"
            f"   WHERE {where} AND dispatch_held"
            f"   ORDER BY work_ticket_idx"
            f"   LIMIT ${limit_placeholder}"
            f" ) RETURNING work_ticket_idx",
            *cohort.args,
            slots,
        )
        # The subquery selects the lowest-idx held tickets (FIFO by shard/block
        # order); UPDATE ... RETURNING order is unspecified, so sort to dispatch
        # (and log) lowest-first deterministically.
        released_idxs = sorted(r["work_ticket_idx"] for r in released)

    if released_idxs:
        _log.info(
            "fan-out pump %s: released %d ticket(s) (%d slot(s) free): %s",
            cohort.label,
            len(released_idxs),
            slots,
            released_idxs,
        )
    for work_ticket_idx in released_idxs:
        dispatch_cb(work_ticket_idx)
    return released_idxs


async def held_cohorts(pool: asyncpg.Pool) -> list[FanoutCohort]:
    """Every distinct cohort that currently has at least one held ticket, across
    all three fan-out types. Used by startup reconcile to re-pump held fan-outs
    that a CP restart left un-topped-up. Cheap: the ``work_ticket_dispatch_held``
    partial index covers the held set."""
    cohorts: list[FanoutCohort] = []
    for row in await pool.fetch(
        "SELECT DISTINCT reference_idx FROM qiita.work_ticket"
        " WHERE dispatch_held AND shard_id IS NOT NULL AND reference_idx IS NOT NULL"
    ):
        cohorts.append(shard_cohort(row["reference_idx"]))
    for row in await pool.fetch(
        "SELECT DISTINCT mask_idx FROM qiita.work_ticket"
        " WHERE dispatch_held AND block_idx IS NOT NULL AND alignment_idx IS NULL"
        "   AND mask_idx IS NOT NULL"
    ):
        cohorts.append(read_mask_block_cohort(row["mask_idx"]))
    for row in await pool.fetch(
        "SELECT DISTINCT alignment_idx FROM qiita.work_ticket"
        " WHERE dispatch_held AND block_idx IS NOT NULL AND alignment_idx IS NOT NULL"
    ):
        cohorts.append(align_block_cohort(row["alignment_idx"]))
    return cohorts
