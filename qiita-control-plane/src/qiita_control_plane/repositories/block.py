"""Repository functions for the block-compute core.

Owns qiita.block (the compute unit — a fixed ~10M-read slice from prep_samples
sharing one mask_idx, run as one work ticket), qiita.block_member (the
block↔sample cover-map), and qiita.mask_sample (the per-(mask_idx, prep_sample)
completion gate; its contract — read ONLY on 'completed' — lives on
`fetch_mask_sample_state`, which consumers point to rather than restate).

The mint-ordering cycle (block.work_ticket_idx ↔ work_ticket.block_idx) is
broken by creating the block first with a NULL work_ticket_idx (create_block),
creating the ticket scoped to that block, then back-filling the link
(set_block_work_ticket).

State transitions use atomic UPDATE ... WHERE, never SELECT-then-UPDATE — the
state machine is serialized by the database, not by application-level reads.
"""

from collections.abc import Sequence

import asyncpg
from qiita_common.actions import BLOCK_MASK_ACTION_ID

from . import require_transaction


async def create_block(conn: asyncpg.Connection) -> int:
    """Insert a fresh qiita.block (state 'pending', work_ticket_idx NULL) and
    return its block_idx.

    require_transaction: the planner creates the block, its block_member cover
    map, and the mask_sample gate rows as one atomic unit — a partial plan must
    roll back rather than leave orphaned rows for a later run to trip over.
    """
    require_transaction(conn)
    return await conn.fetchval(
        "INSERT INTO qiita.block (state) VALUES ('pending') RETURNING block_idx"
    )


async def fetch_block_members(
    conn: asyncpg.Connection | asyncpg.Pool,
    block_idx: int,
) -> list[tuple[int, int, int]]:
    """Return `block_idx`'s cover-map rows as `(prep_sample_idx, min_sequence_idx,
    max_sequence_idx)` tuples, ordered by prep_sample_idx.

    The block-read DoGet mint route reads these to scope a ticket (each row is a
    block-read selector member's sub-range) and the reconcile primitive reads them
    to walk the block's samples. Ordered deterministically (by prep_sample_idx) so
    concurrent reconcile finalizers lock mask_sample rows in a consistent order
    (deadlock-free). Accepts a pool or a connection so it composes standalone or
    inside a transaction."""
    rows = await conn.fetch(
        "SELECT prep_sample_idx, min_sequence_idx, max_sequence_idx"
        "  FROM qiita.block_member"
        " WHERE block_idx = $1"
        " ORDER BY prep_sample_idx",
        block_idx,
    )
    return [(r["prep_sample_idx"], r["min_sequence_idx"], r["max_sequence_idx"]) for r in rows]


async def add_block_members(
    conn: asyncpg.Connection,
    *,
    block_idx: int,
    members: Sequence[tuple[int, int, int]],
) -> None:
    """Insert the block↔sample cover-map rows for `block_idx`.

    Each member is `(prep_sample_idx, min_sequence_idx, max_sequence_idx)` — the
    contiguous sub-range of that sample's reads this block covers. The
    `(block_idx, prep_sample_idx)` PK rejects a duplicate sample within one
    block; the `min <= max` CHECK rejects an inverted range. Empty `members`
    is caller misuse (a block always covers at least one sample).
    """
    require_transaction(conn)
    if not members:
        raise ValueError("add_block_members requires at least one member")
    await conn.executemany(
        "INSERT INTO qiita.block_member"
        " (block_idx, prep_sample_idx, min_sequence_idx, max_sequence_idx)"
        " VALUES ($1, $2, $3, $4)",
        [(block_idx, ps, lo, hi) for (ps, lo, hi) in members],
    )


async def set_block_state(
    conn: asyncpg.Connection,
    *,
    block_idx: int,
    new_state: str,
    expected_states: Sequence[str] | None = None,
) -> bool:
    """Atomically transition a block's state; return True iff a row was updated.

    When `expected_states` is given the UPDATE fires only from one of those
    states (a guarded transition — the WHERE does the check, so there is no
    SELECT-then-UPDATE race); otherwise it is unconditional. A False return
    under a guard means the block was not in an expected state (already advanced
    by a concurrent actor, or gone).
    """
    if expected_states is None:
        updated = await conn.fetchval(
            "UPDATE qiita.block SET state = $2 WHERE block_idx = $1 RETURNING block_idx",
            block_idx,
            new_state,
        )
    else:
        updated = await conn.fetchval(
            "UPDATE qiita.block SET state = $2"
            " WHERE block_idx = $1 AND state = ANY($3::text[])"
            " RETURNING block_idx",
            block_idx,
            new_state,
            list(expected_states),
        )
    return updated is not None


async def set_block_work_ticket(
    conn: asyncpg.Connection,
    *,
    block_idx: int,
    work_ticket_idx: int,
) -> None:
    """Back-fill block.work_ticket_idx after the block's ticket is created — the
    second half of the mint-ordering cycle break (the block was minted first so
    the ticket's scope target could reference block_idx)."""
    require_transaction(conn)
    await conn.execute(
        "UPDATE qiita.block SET work_ticket_idx = $2 WHERE block_idx = $1",
        block_idx,
        work_ticket_idx,
    )


async def create_mask_sample_pending(
    conn: asyncpg.Connection,
    *,
    mask_idx: int,
    prep_sample_idxs: Sequence[int],
) -> None:
    """Materialize the per-sample completion gate for a mask at PENDING.

    One row per `(mask_idx, prep_sample_idx)`. Idempotent via ON CONFLICT DO
    NOTHING so re-planning the same partition does not error and — critically —
    does not resurrect a row already flipped to 'completed' back to 'pending'
    (DO NOTHING leaves the existing row untouched). The row is flipped to
    'completed' at reconcile. Empty input is caller misuse.
    """
    require_transaction(conn)
    if not prep_sample_idxs:
        raise ValueError("create_mask_sample_pending requires at least one prep_sample_idx")
    await conn.executemany(
        "INSERT INTO qiita.mask_sample (mask_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, 'pending')"
        " ON CONFLICT (mask_idx, prep_sample_idx) DO NOTHING",
        [(mask_idx, ps) for ps in prep_sample_idxs],
    )


async def lock_mask_sample(
    conn: asyncpg.Connection,
    *,
    mask_idx: int,
    prep_sample_idx: int,
) -> str | None:
    """`SELECT ... FOR UPDATE` the `(mask_idx, prep_sample_idx)` gate row and
    return its state, or None if no row exists.

    require_transaction: the lock is the crux of the concurrent-finalize
    serialization — two blocks that both cover a sample race to finalize it, and
    holding this row lock for the duration of the check-and-flip means exactly one
    wins (the other, once it acquires the lock, sees the row already 'completed'
    and skips). A None return under a live block is a bug: the gate row is
    materialized PENDING at plan time before any block runs."""
    require_transaction(conn)
    return await conn.fetchval(
        "SELECT state FROM qiita.mask_sample"
        " WHERE mask_idx = $1 AND prep_sample_idx = $2"
        " FOR UPDATE",
        mask_idx,
        prep_sample_idx,
    )


async def lock_mask_sample_gate_advisory(
    conn: asyncpg.Connection,
    *,
    mask_idx: int,
    prep_sample_idx: int,
) -> None:
    """Take a transaction-scoped advisory lock keyed on `(mask_idx,
    prep_sample_idx)`, held until commit/rollback.

    Serializes the two mask-writer paths that both check-then-write the gate for
    the same footprint but cannot see each other's uncommitted state: the
    per-sample `finalize_mask_sample_gate` (`has_incomplete_covering_block` SELECT
    → `upsert_mask_sample_completed`) and the block planner (conflicting-gate SELECT
    → `create_mask_sample_pending`). Without a shared lock both can read "no
    conflict" before either writes, then both proceed — the cross-path double-mask.
    Holding this lock across each path's check→write means whoever is second
    observes the first's committed row and refuses.

    A single-bigint key (`hashtextextended` of the pair) — NOT the two-int32 form,
    whose halves would overflow a bigint idx. This is a distinct lock namespace from
    `lock_mask_sample`'s row-level FOR UPDATE; the two compose (a writer may hold
    both). Callers acquiring locks for many samples must order them (e.g. sorted by
    the pair) to avoid deadlock between two concurrent planners."""
    require_transaction(conn)
    await conn.execute(
        "SELECT pg_advisory_xact_lock("
        "  hashtextextended(format('%s:%s', $1::bigint, $2::bigint), 0))",
        mask_idx,
        prep_sample_idx,
    )


async def fetch_mask_sample_state(
    conn: asyncpg.Connection,
    *,
    mask_idx: int,
    prep_sample_idx: int,
) -> str | None:
    """Read-only companion to `lock_mask_sample`: return the `(mask_idx,
    prep_sample_idx)` gate row's state, or None when no row exists.

    THE mask_sample gate contract (canonical statement; the other consumers point
    here rather than restate it). The gate has two states, 'pending' and
    'completed', and every masking workflow writes it first-class: the block path
    materializes 'pending' at plan time and flips it to 'completed' at reconcile;
    the per-sample mask-model workflows (read-mask, fastq-to-parquet) write
    'completed' at their `finalize-mask-sample` terminal step. So absence (`None`)
    means "no mask has completed for this (mask_idx, prep_sample)" — NOT an exempt
    sample. THE INVARIANT: any consumer that must not read an absent or PARTIAL
    pass-set proceeds ONLY on 'completed', rejecting both `None` (absence is never
    "pass") and 'pending'. Stated as a contract, not a roster — callers point here;
    an enumerated consumer list would only go stale.

    Point-in-time read: no FOR UPDATE / no transaction requirement — it gates a
    read, it does not finalize."""
    return await conn.fetchval(
        "SELECT state FROM qiita.mask_sample WHERE mask_idx = $1 AND prep_sample_idx = $2",
        mask_idx,
        prep_sample_idx,
    )


async def finalize_mask_sample(
    conn: asyncpg.Connection,
    *,
    mask_idx: int,
    prep_sample_idx: int,
) -> bool:
    """Atomically flip a `(mask_idx, prep_sample_idx)` gate row to 'completed';
    return True iff a row moved (it was not already completed).

    Guarded UPDATE (WHERE state <> 'completed'), never SELECT-then-UPDATE — the
    caller holds the row's FOR UPDATE lock (`lock_mask_sample`) across the
    check-and-flip, but the guard is belt-and-suspenders against a double
    finalize. A False return means the row was already completed (an idempotent
    re-run, or a concurrent finalizer that won the race)."""
    require_transaction(conn)
    updated = await conn.fetchval(
        "UPDATE qiita.mask_sample SET state = 'completed'"
        " WHERE mask_idx = $1 AND prep_sample_idx = $2 AND state <> 'completed'"
        " RETURNING prep_sample_idx",
        mask_idx,
        prep_sample_idx,
    )
    return updated is not None


async def upsert_mask_sample_completed(
    conn: asyncpg.Connection,
    *,
    mask_idx: int,
    prep_sample_idx: int,
) -> None:
    """Upsert the `(mask_idx, prep_sample_idx)` gate row straight to 'completed'.

    The per-sample read-mask path's writer (the `finalize-mask-sample` terminal
    action). Unlike the block path — which materializes a PENDING row at plan time
    (`create_mask_sample_pending`) and flips it at reconcile (`finalize_mask_sample`)
    — per-sample masking is atomic per ticket, so there is no PENDING phase: the row
    is written 'completed' in one idempotent upsert. ON CONFLICT keeps it robust to a
    workflow retry and composes with a block-path row already present for the same
    pair (it can only move a stale row forward to 'completed', never backward)."""
    require_transaction(conn)
    await conn.execute(
        "INSERT INTO qiita.mask_sample (mask_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, 'completed')"
        " ON CONFLICT (mask_idx, prep_sample_idx) DO UPDATE SET state = 'completed'",
        mask_idx,
        prep_sample_idx,
    )


async def has_incomplete_covering_block(
    conn: asyncpg.Connection,
    *,
    mask_idx: int,
    prep_sample_idx: int,
) -> bool:
    """True iff some block covering `prep_sample_idx` under `mask_idx` is not yet
    'completed' — the finalize gate for the sample.

    A block covers the sample via `block_member`; its mask identity is its
    ticket's `work_ticket.mask_idx`. The sample's mask is COMPLETE only when EVERY
    covering block has reached 'completed'; a still-running (pending/processing)
    OR a failed sibling block leaves reads unmasked, so the sample must not
    finalize. Checking `state <> 'completed'` (rather than "non-terminal") means a
    failed block correctly blocks finalize until it is re-driven to completion —
    the strict, fail-closed reading of the export gate this invariant protects.

    Only READ-MASK blocks count: `wt.action_id` names the bulk-masking workflow.
    Without that filter a pending/failed ALIGN block over the same (mask_idx,
    sample) would spuriously wedge this sample's read-mask finalize — align blocks
    carry BOTH mask_idx and alignment_idx. The filter is deliberately NOT
    `wt.alignment_idx IS NULL`: purging an alignment NULLs that column
    (`ON DELETE SET NULL`), which would hand this query exactly the align blocks it
    exists to exclude. See `qiita_common.actions`."""
    incomplete = await conn.fetchval(
        "SELECT 1 FROM qiita.block b"
        "  JOIN qiita.block_member bm ON bm.block_idx = b.block_idx"
        "  JOIN qiita.work_ticket wt ON wt.work_ticket_idx = b.work_ticket_idx"
        " WHERE bm.prep_sample_idx = $1 AND wt.mask_idx = $2"
        "   AND wt.action_id = $3 AND b.state <> 'completed'"
        " LIMIT 1",
        prep_sample_idx,
        mask_idx,
        BLOCK_MASK_ACTION_ID,
    )
    return incomplete is not None


# =============================================================================
# alignment_sample gate (per-(alignment_idx, prep_sample) completion)
# =============================================================================
# Exact twins of the mask_sample gate primitives above, keyed on alignment_idx
# instead of mask_idx. Alignment consumes the SAME block / block_member core
# (WHY-agnostic) and the SAME per-sample-completion pattern; the only difference
# is the WHY column the covering-block join reads (work_ticket.alignment_idx).
# See qiita.alignment_sample (twin of qiita.mask_sample).


async def create_alignment_sample_pending(
    conn: asyncpg.Connection,
    *,
    alignment_idx: int,
    prep_sample_idxs: Sequence[int],
) -> None:
    """Materialize the per-sample completion gate for an alignment at PENDING.

    One row per `(alignment_idx, prep_sample_idx)`. Idempotent via ON CONFLICT DO
    NOTHING so re-planning the same partition does not error and — critically —
    does not resurrect a row already flipped to 'completed' back to 'pending'.
    The row is flipped to 'completed' at reconcile. Empty input is caller misuse.
    Twin of `create_mask_sample_pending`.
    """
    require_transaction(conn)
    if not prep_sample_idxs:
        raise ValueError("create_alignment_sample_pending requires at least one prep_sample_idx")
    await conn.executemany(
        "INSERT INTO qiita.alignment_sample (alignment_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, 'pending')"
        " ON CONFLICT (alignment_idx, prep_sample_idx) DO NOTHING",
        [(alignment_idx, ps) for ps in prep_sample_idxs],
    )


async def lock_alignment_sample(
    conn: asyncpg.Connection,
    *,
    alignment_idx: int,
    prep_sample_idx: int,
) -> str | None:
    """`SELECT ... FOR UPDATE` the `(alignment_idx, prep_sample_idx)` gate row and
    return its state, or None if no row exists.

    require_transaction: the lock is the crux of the concurrent-finalize
    serialization — two blocks that both cover a sample race to finalize it, and
    holding this row lock for the duration of the check-and-flip means exactly one
    wins. A None return under a live block is a bug: the gate row is materialized
    PENDING at plan time before any block runs. Twin of `lock_mask_sample`."""
    require_transaction(conn)
    return await conn.fetchval(
        "SELECT state FROM qiita.alignment_sample"
        " WHERE alignment_idx = $1 AND prep_sample_idx = $2"
        " FOR UPDATE",
        alignment_idx,
        prep_sample_idx,
    )


async def finalize_alignment_sample(
    conn: asyncpg.Connection,
    *,
    alignment_idx: int,
    prep_sample_idx: int,
) -> bool:
    """Atomically flip a `(alignment_idx, prep_sample_idx)` gate row to
    'completed'; return True iff a row moved (it was not already completed).

    Guarded UPDATE (WHERE state <> 'completed'), never SELECT-then-UPDATE — the
    caller holds the row's FOR UPDATE lock (`lock_alignment_sample`) across the
    check-and-flip, but the guard is belt-and-suspenders against a double
    finalize. A False return means the row was already completed. Twin of
    `finalize_mask_sample`."""
    require_transaction(conn)
    updated = await conn.fetchval(
        "UPDATE qiita.alignment_sample SET state = 'completed'"
        " WHERE alignment_idx = $1 AND prep_sample_idx = $2 AND state <> 'completed'"
        " RETURNING prep_sample_idx",
        alignment_idx,
        prep_sample_idx,
    )
    return updated is not None


async def list_incomplete_alignment_samples(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    alignment_idx: int,
    prep_sample_idxs: Sequence[int],
) -> list[int]:
    """Return the `prep_sample_idx` values (from `prep_sample_idxs`) that are NOT
    'completed' for `alignment_idx` — those carrying a 'pending' gate row OR no
    `qiita.alignment_sample` row at all (never part of this alignment).

    The feature-table resolver calls this at SUBMIT to refuse building an OGU table
    over an incomplete cohort. alignment rows are NOT 1:1 with reads, so presence
    of rows is never 'done' — completion is a first-class state (see
    qiita.alignment_sample). Accepts a pool or a connection so it composes inside a
    transaction. Result is sorted for a deterministic error message; empty input
    returns []."""
    if not prep_sample_idxs:
        return []
    rows = await pool_or_conn.fetch(
        "SELECT prep_sample_idx FROM qiita.alignment_sample"
        " WHERE alignment_idx = $1 AND prep_sample_idx = ANY($2::bigint[])"
        "   AND state = 'completed'",
        alignment_idx,
        list(prep_sample_idxs),
    )
    completed = {r["prep_sample_idx"] for r in rows}
    return sorted(set(prep_sample_idxs) - completed)


async def has_incomplete_covering_alignment_block(
    conn: asyncpg.Connection,
    *,
    alignment_idx: int,
    prep_sample_idx: int,
) -> bool:
    """True iff some block covering `prep_sample_idx` under `alignment_idx` is not
    yet 'completed' — the finalize gate for the sample.

    A block covers the sample via `block_member`; its alignment identity is its
    ticket's `work_ticket.alignment_idx`. The sample's alignment is COMPLETE only
    when EVERY covering block has reached 'completed'; a still-running or failed
    sibling block leaves the sample's alignment partial, so it must not finalize.
    Checking `state <> 'completed'` (not "non-terminal") means a failed block
    correctly blocks finalize until re-driven — the strict, fail-closed reading.
    Twin of `has_incomplete_covering_block` (joins on alignment_idx not mask_idx).

    Needs no `action_id` filter, unlike that twin: `alignment_idx` is set only by
    an align block, so the join column is already unambiguous — whereas `mask_idx`
    is carried by both block kinds. Any block covering this sample under this
    alignment should count here, whatever workflow produced it — which is why the
    filter is absent by intent, not by omission. Note `fanout_dispatch.align_block_cohort`
    DOES carry one (a cohort is a per-workflow throttle): if a second action ever
    sets `alignment_idx`, the two diverge — it would stop throttling those tickets
    while this keeps gating on them. Revisit both together."""
    incomplete = await conn.fetchval(
        "SELECT 1 FROM qiita.block b"
        "  JOIN qiita.block_member bm ON bm.block_idx = b.block_idx"
        "  JOIN qiita.work_ticket wt ON wt.work_ticket_idx = b.work_ticket_idx"
        " WHERE bm.prep_sample_idx = $1 AND wt.alignment_idx = $2 AND b.state <> 'completed'"
        " LIMIT 1",
        prep_sample_idx,
        alignment_idx,
    )
    return incomplete is not None
