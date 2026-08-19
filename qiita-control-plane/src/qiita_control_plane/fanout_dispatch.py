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
from qiita_common.actions import ALIGN_ACTION_ID, BLOCK_MASK_ACTION_ID

# The cohort vocabulary and the override ceiling are wire contract (they name a
# cohort in the fan-out routes), so they live with the models rather than here.
from qiita_common.models import MAX_FANOUT_OVERRIDE, FanoutCohortKind, FanoutCohortStatus

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

# The only two ticket predicates `_cohorts_matching` will interpolate. Constants, not
# literals at the call sites, so the "module-internal SQL only" constraint is checkable
# by identity where the fragment is built — see the assert in `_cohorts_matching`.
_PRED_HELD = "dispatch_held"
_PRED_ACTIVE = "(dispatch_held OR state IN ('pending', 'queued', 'processing'))"

# Operator-set per-cohort caps, keyed (kind, key). Deliberately in-memory and
# process-local; a table buys durability we do not want for an incident-time knob.
# The CP runs a single uvicorn process (no --workers), so a plain dict is the whole
# mechanism — under `--workers` a set on one worker would be invisible to the next,
# silently, which is why that stays a single process.
#
# A restart drops every override, reverting to the default. **That revert is only
# conservative in one direction, and the asymmetry is the trap:**
#
#   * RAISED above the default (the case this exists to serve) — the restart
#     reverts DOWN. Fewer tickets in flight than the operator asked for; the
#     cohort simply drains at the default rate. Safe.
#   * LOWERED below the default — the restart reverts UP. `reconcile_inflight_tickets`
#     pumps every held cohort with `settings.fanout_max_inflight`, and this dict is
#     already empty by then, so the cohort re-pumps at the DEFAULT and lands over
#     what the operator set (8 in flight after a 2 was asked for, measured). The
#     lowering has to be re-applied by hand after any restart — `set_override`
#     WARNs when it records one, so the journal says so at the moment it happens.
#
# The restart is not necessarily operator-initiated: both units are
# `Restart=on-failure`, so the plausible case is a crash during the very incident
# the operator was throttling.
#
# Nothing expires an override either, so one left set outlives its incident and
# reapplies if its (kind, key) is ever re-run. `overridden_cohorts()` keeps that
# enumerable — `GET /fanout` unions it in, so an override whose cohort has fully
# drained still shows up instead of vanishing from the only surface that lists them.
_OVERRIDES: dict[tuple[str, int], int] = {}


@dataclass(frozen=True, slots=True)
class FanoutCohort:
    """One fan-out's child-ticket set: a SQL predicate over qiita.work_ticket
    (positional ``$1..`` placeholders, filled by ``args``) plus the advisory-lock
    identity that serialises pumps for it. The predicate must select exactly the
    fan-out's children and nothing else.

    ``(kind, key)`` is the cohort's exact identity — what the override registry
    and the admin routes address it by. Distinct from ``lock_key``, which is
    masked into int4 and therefore lossy, and from ``label``, which is prose."""

    label: str
    kind: FanoutCohortKind
    key: int
    where_sql: str
    args: tuple[Any, ...]
    lock_class: int
    lock_key: int


def shard_cohort(reference_idx: int) -> FanoutCohort:
    """The sharded-index build children of one reference."""
    return FanoutCohort(
        label=f"shard(reference_idx={reference_idx})",
        kind=FanoutCohortKind.SHARD,
        key=reference_idx,
        where_sql="reference_idx = $1 AND shard_id IS NOT NULL",
        args=(reference_idx,),
        lock_class=_LOCK_CLASS_SHARD,
        lock_key=reference_idx & _INT4_MASK,
    )


def read_mask_block_cohort(mask_idx: int) -> FanoutCohort:
    """The bulk read-mask block children of one mask partition. Discriminated
    from align blocks by ``action_id`` — NOT by ``alignment_idx IS NULL``, which a
    purge of the alignment produces from an align ticket (see
    `qiita_common.actions`)."""
    return FanoutCohort(
        label=f"read_mask_block(mask_idx={mask_idx})",
        kind=FanoutCohortKind.READ_MASK_BLOCK,
        key=mask_idx,
        where_sql="mask_idx = $1 AND block_idx IS NOT NULL AND action_id = $2",
        args=(mask_idx, BLOCK_MASK_ACTION_ID),
        lock_class=_LOCK_CLASS_READ_MASK_BLOCK,
        lock_key=mask_idx & _INT4_MASK,
    )


def align_block_cohort(alignment_idx: int) -> FanoutCohort:
    """The bulk sharded-alignment block children of one alignment.

    ``alignment_idx`` is already unambiguous (only an align block sets it, and a
    purge NULLs it, so a purged ticket drops out either way) — unlike ``mask_idx``,
    which both block kinds carry. The ``action_id`` test is here because a cohort
    is a throttle over ONE workflow's fan-out, so scoping it to that workflow is
    what the cohort means, not because the idx needs disambiguating.

    **If a second action ever sets `alignment_idx`, these two diverge.** This
    cohort would exclude its tickets from the in-flight cap while
    `repositories.block.has_incomplete_covering_alignment_block` — deliberately
    un-filtered, since any covering block counts toward a sample's completion —
    would still count them. Throttling less than you gate on is the wrong way
    round: revisit both together, don't add the filter to only one."""
    return FanoutCohort(
        label=f"align_block(alignment_idx={alignment_idx})",
        kind=FanoutCohortKind.ALIGN_BLOCK,
        key=alignment_idx,
        where_sql="alignment_idx = $1 AND block_idx IS NOT NULL AND action_id = $2",
        args=(alignment_idx, ALIGN_ACTION_ID),
        lock_class=_LOCK_CLASS_ALIGN_BLOCK,
        lock_key=alignment_idx & _INT4_MASK,
    )


# Kind → builder, so a (kind, key) pair off the wire or out of `_OVERRIDES` can be
# turned back into its cohort. Lives here with the builders rather than in the routes:
# `overridden_cohorts` needs it too, and one registry cannot drift from itself.
COHORT_BUILDERS: dict[FanoutCohortKind, Callable[[int], FanoutCohort]] = {
    FanoutCohortKind.SHARD: shard_cohort,
    FanoutCohortKind.READ_MASK_BLOCK: read_mask_block_cohort,
    FanoutCohortKind.ALIGN_BLOCK: align_block_cohort,
}


def set_override(cohort: FanoutCohort, max_inflight: int, *, default: int | None = None) -> None:
    """Set `cohort`'s runtime cap. Raises ValueError outside 1..MAX_FANOUT_OVERRIDE.

    Pass `default` (the boot-time `FANOUT_MAX_INFLIGHT` this override displaces) to
    get the restart warning: a cap set BELOW the default is silently undone by a
    restart, which reverts the cohort UP to the default rather than down, so it
    re-pumps to more in flight than was asked for. The WARNING is the only thing
    that puts "re-apply this" in the journal — see the `_OVERRIDES` comment for the
    full asymmetry. Omitting `default` skips the check, never the set.
    """
    if not 1 <= max_inflight <= MAX_FANOUT_OVERRIDE:
        raise ValueError(
            f"max_inflight must be between 1 and {MAX_FANOUT_OVERRIDE}, got {max_inflight}"
        )
    if default is not None and max_inflight < default:
        _log.warning(
            "fan-out cohort %s capped at %d, BELOW the FANOUT_MAX_INFLIGHT default of %d. "
            "Overrides are in-memory: a control-plane restart drops this and the cohort "
            "re-pumps at %d, i.e. MORE in flight than you asked for. Re-apply after any "
            "restart (the unit is Restart=on-failure, so a crash counts).",
            cohort.label,
            max_inflight,
            default,
            default,
        )
    _OVERRIDES[(cohort.kind, cohort.key)] = max_inflight


def clear_override(cohort: FanoutCohort) -> None:
    """Drop `cohort`'s override so it falls back to the caller's default. No-op if unset."""
    _OVERRIDES.pop((cohort.kind, cohort.key), None)


def get_override(cohort: FanoutCohort) -> int | None:
    """`cohort`'s runtime cap, or None when it has none."""
    return _OVERRIDES.get((cohort.kind, cohort.key))


def overridden_cohorts() -> list[FanoutCohort]:
    """Every cohort carrying a runtime override, whether or not it still has tickets.

    Exists because nothing evicts from `_OVERRIDES` except an explicit clear, while
    `active_cohorts` drops a cohort the moment its last child goes terminal — so an
    override could outlive its cohort with no surface left to show it, and an operator
    who set three during an incident could not ask "what have I set?". `GET /fanout`
    unions this in for exactly that.

    Rebuilt from the (kind, key) identity rather than stored, so a cohort listed here
    carries the same predicate and lock identity it would anywhere else."""
    return [COHORT_BUILDERS[FanoutCohortKind(kind)](key) for kind, key in _OVERRIDES]


def cohort_for_ticket_row(row: asyncpg.Record | dict[str, Any]) -> FanoutCohort | None:
    """Derive the fan-out cohort of a work_ticket from its discriminating
    columns, or None if the ticket is not a fan-out child. The row must carry
    ``reference_idx``, ``shard_id``, ``block_idx``, ``mask_idx``,
    ``alignment_idx``, ``action_id``. The order matches the three fan-out INSERT
    shapes:

      * shard build  → reference_idx scope + shard_id set;
      * align block  → block_idx set + the align action;
      * read-mask block → block_idx set + the read-mask-block action.

    A PURGED align ticket (align action, ``alignment_idx`` NULLed by the delete)
    therefore belongs to NO cohort, and a held one is never released — not by a
    later top-up, not by startup reconcile. That is the intended end state here:
    the alternative is releasing a block whose alignment no longer exists, which
    is what the pre-`action_id` routing did. It leaves a permanently-pending held
    ticket behind, which is litter rather than a hazard; making the purge flip
    those to `cancelled` is the abandon primitive tracked separately, not
    something this routing can do.

    A block ticket is routed by its ACTION, not by whether ``alignment_idx`` is
    set: purging an alignment NULLs that column, which would otherwise route the
    align ticket into the read-mask cohort of the ``mask_idx`` it still carries
    (see `qiita_common.actions`). A block ticket whose action is neither — or
    missing the idx its cohort is keyed by — is not a fan-out child.
    """
    if row["shard_id"] is not None and row["reference_idx"] is not None:
        return shard_cohort(row["reference_idx"])
    if row["block_idx"] is not None:
        if row["action_id"] == ALIGN_ACTION_ID and row["alignment_idx"] is not None:
            return align_block_cohort(row["alignment_idx"])
        if row["action_id"] == BLOCK_MASK_ACTION_ID and row["mask_idx"] is not None:
            return read_mask_block_cohort(row["mask_idx"])
    return None


_TICKET_COHORT_COLUMNS = "reference_idx, shard_id, block_idx, mask_idx, alignment_idx, action_id"


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

    ``max_inflight`` is the *default*: a runtime override set for this cohort
    (`set_override`) wins over it. Resolving here rather than at the call sites
    means all three trigger paths — the initial fan-out fill, the per-child
    completion hook, and startup reconcile — honour it without threading it
    through.

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

        # Read inside the lock so the cap and the running count below form one
        # consistent snapshot, and so a pump that queued on the lock picks up an
        # override set while it was waiting rather than a pre-queue value.
        override = get_override(cohort)
        if override is not None:
            max_inflight = override

        # Fail-stop circuit breaker: one failed child halts the fan-out.
        has_failed = await conn.fetchval(
            f"SELECT EXISTS (SELECT 1 FROM qiita.work_ticket WHERE {where} AND state = 'failed')",
            *cohort.args,
        )
        if has_failed:
            # WARNING, not INFO: the cohort is now frozen until an operator redrives
            # the failed child, and at INFO this is indistinguishable from the
            # ordinary "no free slots" return below.
            _log.warning(
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
        # Per-ticket, because the release above is ALREADY COMMITTED. A raise here
        # would otherwise abandon the rest of the batch in the one state nothing can
        # recover: no longer held (so no pump will re-release it — `top_up_dispatch`
        # only touches `dispatch_held` rows) yet never dispatched, while still
        # counting as `running` and so looking healthy on the status route. Naming
        # the idx is what makes `POST /work-ticket/{idx}/run` possible.
        try:
            dispatch_cb(work_ticket_idx)
        except Exception:
            _log.exception(
                "fan-out pump %s: released work_ticket %d but dispatching it failed;"
                " it is no longer held and no pump will retry it — recover with"
                " POST /work-ticket/%d/run",
                cohort.label,
                work_ticket_idx,
                work_ticket_idx,
            )
    return released_idxs


async def cohort_status(
    pool: asyncpg.Pool, cohort: FanoutCohort, *, max_inflight: int
) -> FanoutCohortStatus:
    """Count `cohort`'s tickets by disposition and resolve its effective cap.

    Returns the wire model directly — there is no internal shape worth keeping
    separate from what the route serialises.

    `total` is every ticket the cohort predicate matches, terminal ones included:
    it is how a caller distinguishes a cohort that has finished from one that never
    existed (a typo'd key), which the other counts cannot express.

    Read-only, and deliberately NOT advisory-locked: this is a snapshot for a human,
    not an input to a release decision, so blocking a status read behind a running
    pump would buy nothing."""
    where = cohort.where_sql
    row = await pool.fetchrow(
        f"SELECT"
        f"   count(*) AS total,"
        f"   count(*) FILTER (WHERE dispatch_held) AS held,"
        f"   count(*) FILTER (WHERE NOT dispatch_held"
        f"                      AND state IN ('pending', 'queued', 'processing')) AS running,"
        f"   count(*) FILTER (WHERE state = 'failed') AS failed"
        f" FROM qiita.work_ticket WHERE {where}",
        *cohort.args,
    )
    override = get_override(cohort)
    return FanoutCohortStatus(
        kind=cohort.kind,
        key=cohort.key,
        label=cohort.label,
        total=int(row["total"]),
        held=int(row["held"]),
        running=int(row["running"]),
        failed=int(row["failed"]),
        # Same predicate the pump's circuit breaker uses, so what an operator reads
        # here is exactly what the next pump will decide.
        fail_stopped=int(row["failed"]) > 0,
        max_inflight=override if override is not None else max_inflight,
        override=override,
    )


async def active_cohorts(pool: asyncpg.Pool) -> list[FanoutCohort]:
    """Every cohort with held OR in-flight tickets, across all three fan-out types.

    Wider than `held_cohorts`, which reconcile uses: a cohort that has released
    everything still has work running and must stay visible to an operator. A cohort
    whose tickets are all terminal drops out of both."""
    return await _cohorts_matching(pool, _PRED_ACTIVE)


async def held_cohorts(pool: asyncpg.Pool) -> list[FanoutCohort]:
    """Every distinct cohort that currently has at least one held ticket, across
    all three fan-out types. Used by startup reconcile to re-pump held fan-outs
    that a CP restart left un-topped-up. Cheap: the ``work_ticket_dispatch_held``
    partial index covers the held set."""
    return await _cohorts_matching(pool, _PRED_HELD)


async def _cohorts_matching(pool: asyncpg.Pool, ticket_predicate: str) -> list[FanoutCohort]:
    """Distinct cohorts of all three kinds whose tickets satisfy `ticket_predicate`.

    `ticket_predicate` is interpolated into SQL, so it must be one of this module's
    own constants — never caller input. The assert is the guard rather than the
    docstring being the guard: with exactly two legal values, pinning them by identity
    means a later refactor that threads a caller-supplied predicate here fails at once
    instead of opening an injection.

    Both block scans key on action_id, not on whether alignment_idx is set: a purged
    align ticket keeps its mask_idx and would otherwise be reported here as a
    read-mask block (see `qiita_common.actions`)."""
    assert ticket_predicate in (_PRED_HELD, _PRED_ACTIVE), (
        f"ticket_predicate is interpolated into SQL and must be a module constant, "
        f"got {ticket_predicate!r}"
    )
    cohorts: list[FanoutCohort] = []
    for row in await pool.fetch(
        f"SELECT DISTINCT reference_idx FROM qiita.work_ticket"
        f" WHERE {ticket_predicate} AND shard_id IS NOT NULL AND reference_idx IS NOT NULL"
    ):
        cohorts.append(shard_cohort(row["reference_idx"]))
    for row in await pool.fetch(
        f"SELECT DISTINCT mask_idx FROM qiita.work_ticket"
        f" WHERE {ticket_predicate} AND block_idx IS NOT NULL AND action_id = $1"
        f"   AND mask_idx IS NOT NULL",
        BLOCK_MASK_ACTION_ID,
    ):
        cohorts.append(read_mask_block_cohort(row["mask_idx"]))
    for row in await pool.fetch(
        f"SELECT DISTINCT alignment_idx FROM qiita.work_ticket"
        f" WHERE {ticket_predicate} AND block_idx IS NOT NULL AND action_id = $1"
        f"   AND alignment_idx IS NOT NULL",
        ALIGN_ACTION_ID,
    ):
        cohorts.append(align_block_cohort(row["alignment_idx"]))
    return cohorts
