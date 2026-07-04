"""Repository functions for the block-compute core.

Owns qiita.block (the compute unit — a fixed ~10M-read slice from prep_samples
sharing one mask_idx, run as one work ticket), qiita.block_member (the
block↔sample cover-map), and qiita.mask_sample (the per-(mask_idx, prep_sample)
completion gate the masked-read export path reads).

The mint-ordering cycle (block.work_ticket_idx ↔ work_ticket.block_idx) is
broken by creating the block first with a NULL work_ticket_idx (create_block),
creating the ticket scoped to that block, then back-filling the link
(set_block_work_ticket).

State transitions use atomic UPDATE ... WHERE, never SELECT-then-UPDATE — the
state machine is serialized by the database, not by application-level reads.
"""

from collections.abc import Sequence

import asyncpg

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

    The runner reads these to bind a block ticket's `reads` (each row is an
    `export_read_block` member's sub-range) and the reconcile primitive reads them
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
    the strict, fail-closed reading of the export gate this invariant protects."""
    incomplete = await conn.fetchval(
        "SELECT 1 FROM qiita.block b"
        "  JOIN qiita.block_member bm ON bm.block_idx = b.block_idx"
        "  JOIN qiita.work_ticket wt ON wt.work_ticket_idx = b.work_ticket_idx"
        " WHERE bm.prep_sample_idx = $1 AND wt.mask_idx = $2 AND b.state <> 'completed'"
        " LIMIT 1",
        prep_sample_idx,
        mask_idx,
    )
    return incomplete is not None
