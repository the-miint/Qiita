"""Repository-layer tests for the block-compute core.

Exercises qiita.block / qiita.block_member / qiita.mask_sample via the
repositories.block helpers: block creation with the NULL-then-backfill
work_ticket link, the cover-map member inserts (PK + min<=max CHECK), the
atomic state transition, the idempotent PENDING gate materialization, and the
work_ticket back-fill that closes the mint-ordering cycle.

Each test seeds its own principal + sequenced prep_samples + a mask_definition
so cleanup runs in FK-reverse order and the suite can run against the shared
postgres_pool fixture. Blocks a test creates are tracked in `blk['created_blocks']`
so teardown targets exactly those rows (parallel-safe — no global block sweep).
"""

import secrets

import asyncpg
import pytest
import pytest_asyncio
from qiita_common.actions import ALIGN_ACTION_ID, BLOCK_MASK_ACTION_ID

from qiita_control_plane.align_planner import ALIGN_ACTION_VERSION
from qiita_control_plane.block_planner import BLOCK_MASK_ACTION_VERSION
from qiita_control_plane.repositories.block import (
    add_block_members,
    create_block,
    create_mask_sample_pending,
    fetch_block_members,
    finalize_mask_sample,
    has_incomplete_covering_block,
    lock_mask_sample,
    lock_mask_sample_gate_advisory,
    set_block_state,
    set_block_work_ticket,
    upsert_mask_sample_completed,
)
from qiita_control_plane.repositories.mask_definition import mint_mask_definition
from qiita_control_plane.testing.db_seeds import (
    delete_block_action_if_created,
    seed_biosample_with_sequenced_prep_sample,
    seed_block_action_if_absent,
    seed_user_principal,
)

pytestmark = pytest.mark.db

# The (action_id -> version) pairs these fixtures seed. Taken from the planners
# that actually mint these tickets, so a submitter version bump reaches the tests
# instead of leaving them exercising a version production no longer submits.
_BLOCK_ACTION_VERSIONS = {
    BLOCK_MASK_ACTION_ID: BLOCK_MASK_ACTION_VERSION,
    ALIGN_ACTION_ID: ALIGN_ACTION_VERSION,
}


async def _new_block(blk) -> int:
    """create_block in its own transaction, tracked for cleanup."""
    async with blk["pool"].acquire() as conn, conn.transaction():
        block_idx = await create_block(conn)
    blk["created_blocks"].append(block_idx)
    return block_idx


@pytest_asyncio.fixture
async def blk(postgres_pool):
    """Seed a principal, two sequenced prep_samples, and a mask_definition.

    Yields the ids + pool + a `created_blocks` list tests append to. FK-reverse
    cleanup sweeps exactly the tracked block rows (block_member cascades), the
    mask_sample gate rows for this mask, then the sample chain, the mask, the
    user, and the principal."""
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="block-test", suffix=suffix)
    bs1, ps1 = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    bs2, ps2 = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    async with postgres_pool.acquire() as conn:
        mask = await mint_mask_definition(
            conn,
            filter_workflow="host_filter",
            filter_version="1.0.0",
            params={"workflow": "host_filter", "version": "1.0.0", "s": suffix},
            principal_idx=principal_idx,
        )
    mask_idx = mask["mask_idx"]
    created_blocks: list[int] = []

    yield {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "prep_sample_idxs": [ps1, ps2],
        "biosample_idxs": [bs1, bs2],
        "mask_idx": mask_idx,
        "created_blocks": created_blocks,
    }

    # FK-reverse. Any work_ticket referencing a tracked block first (NO ACTION
    # on work_ticket.block_idx), then the blocks (block_member cascades), then
    # the gate rows, sample chain, mask, user, principal.
    if created_blocks:
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE block_idx = ANY($1::bigint[])", created_blocks
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.block WHERE block_idx = ANY($1::bigint[])", created_blocks
        )
    await postgres_pool.execute("DELETE FROM qiita.mask_sample WHERE mask_idx = $1", mask_idx)
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", [ps1, ps2]
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", [bs1, bs2]
    )
    await postgres_pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


# ---------------------------------------------------------------------------
# create_block
# ---------------------------------------------------------------------------


async def test_create_block_starts_pending_without_ticket(blk):
    block_idx = await _new_block(blk)
    row = await blk["pool"].fetchrow(
        "SELECT state, work_ticket_idx FROM qiita.block WHERE block_idx = $1", block_idx
    )
    assert row["state"] == "pending"
    assert row["work_ticket_idx"] is None


async def test_create_block_requires_transaction(blk):
    async with blk["pool"].acquire() as conn:
        with pytest.raises(RuntimeError):
            await create_block(conn)


# ---------------------------------------------------------------------------
# add_block_members
# ---------------------------------------------------------------------------


async def test_add_block_members_inserts_cover_map(blk):
    pool = blk["pool"]
    ps1, ps2 = blk["prep_sample_idxs"]
    block_idx = await _new_block(blk)
    async with pool.acquire() as conn, conn.transaction():
        await add_block_members(
            conn, block_idx=block_idx, members=[(ps1, 0, 999), (ps2, 1000, 4999)]
        )
    rows = await pool.fetch(
        "SELECT prep_sample_idx, min_sequence_idx, max_sequence_idx"
        " FROM qiita.block_member WHERE block_idx = $1 ORDER BY prep_sample_idx",
        block_idx,
    )
    got = {(r["prep_sample_idx"], r["min_sequence_idx"], r["max_sequence_idx"]) for r in rows}
    assert got == {(ps1, 0, 999), (ps2, 1000, 4999)}


async def test_fetch_block_members_returns_ordered_cover_map(blk):
    pool = blk["pool"]
    ps1, ps2 = sorted(blk["prep_sample_idxs"])
    block_idx = await _new_block(blk)
    async with pool.acquire() as conn, conn.transaction():
        # Insert out of prep_sample order; fetch must sort by prep_sample_idx.
        await add_block_members(
            conn, block_idx=block_idx, members=[(ps2, 1000, 4999), (ps1, 0, 999)]
        )
    async with pool.acquire() as conn:
        members = await fetch_block_members(conn, block_idx)
    assert members == [(ps1, 0, 999), (ps2, 1000, 4999)]
    # Also works standalone on the pool (no open transaction).
    assert await fetch_block_members(pool, block_idx) == [(ps1, 0, 999), (ps2, 1000, 4999)]


async def test_add_block_members_rejects_inverted_range(blk):
    pool = blk["pool"]
    ps1, _ = blk["prep_sample_idxs"]
    block_idx = await _new_block(blk)
    with pytest.raises(asyncpg.CheckViolationError):
        async with pool.acquire() as conn, conn.transaction():
            await add_block_members(conn, block_idx=block_idx, members=[(ps1, 500, 100)])


async def test_add_block_members_rejects_duplicate_sample(blk):
    pool = blk["pool"]
    ps1, _ = blk["prep_sample_idxs"]
    block_idx = await _new_block(blk)
    with pytest.raises(asyncpg.UniqueViolationError):
        async with pool.acquire() as conn, conn.transaction():
            await add_block_members(
                conn, block_idx=block_idx, members=[(ps1, 0, 10), (ps1, 20, 30)]
            )


# ---------------------------------------------------------------------------
# set_block_state (atomic UPDATE-WHERE)
# ---------------------------------------------------------------------------


async def test_set_block_state_unconditional(blk):
    pool = blk["pool"]
    block_idx = await _new_block(blk)
    async with pool.acquire() as conn:
        assert await set_block_state(conn, block_idx=block_idx, new_state="processing") is True
    state = await pool.fetchval("SELECT state FROM qiita.block WHERE block_idx = $1", block_idx)
    assert state == "processing"


async def test_set_block_state_guarded_only_fires_from_expected(blk):
    pool = blk["pool"]
    block_idx = await _new_block(blk)
    async with pool.acquire() as conn:
        # Guard mismatch: block is 'pending', we require 'processing' → no-op.
        assert (
            await set_block_state(
                conn, block_idx=block_idx, new_state="completed", expected_states=["processing"]
            )
            is False
        )
        assert (
            await conn.fetchval("SELECT state FROM qiita.block WHERE block_idx = $1", block_idx)
            == "pending"
        )
        # Guard match fires.
        assert (
            await set_block_state(
                conn, block_idx=block_idx, new_state="completed", expected_states=["pending"]
            )
            is True
        )


# ---------------------------------------------------------------------------
# create_mask_sample_pending (the completion gate)
# ---------------------------------------------------------------------------


async def test_create_mask_sample_pending_materializes_gate(blk):
    pool = blk["pool"]
    mask_idx = blk["mask_idx"]
    ps1, ps2 = blk["prep_sample_idxs"]
    async with pool.acquire() as conn, conn.transaction():
        await create_mask_sample_pending(conn, mask_idx=mask_idx, prep_sample_idxs=[ps1, ps2])
    rows = await pool.fetch(
        "SELECT prep_sample_idx, state FROM qiita.mask_sample WHERE mask_idx = $1", mask_idx
    )
    assert {(r["prep_sample_idx"], r["state"]) for r in rows} == {
        (ps1, "pending"),
        (ps2, "pending"),
    }


async def test_create_mask_sample_pending_is_idempotent(blk):
    pool = blk["pool"]
    mask_idx = blk["mask_idx"]
    ps1, _ = blk["prep_sample_idxs"]
    async with pool.acquire() as conn, conn.transaction():
        await create_mask_sample_pending(conn, mask_idx=mask_idx, prep_sample_idxs=[ps1])
    # Flip to completed, then re-run PENDING materialization: ON CONFLICT DO
    # NOTHING must NOT resurrect the completed row back to pending.
    await pool.execute(
        "UPDATE qiita.mask_sample SET state = 'completed'"
        " WHERE mask_idx = $1 AND prep_sample_idx = $2",
        mask_idx,
        ps1,
    )
    async with pool.acquire() as conn, conn.transaction():
        await create_mask_sample_pending(conn, mask_idx=mask_idx, prep_sample_idxs=[ps1])
    state = await pool.fetchval(
        "SELECT state FROM qiita.mask_sample WHERE mask_idx = $1 AND prep_sample_idx = $2",
        mask_idx,
        ps1,
    )
    assert state == "completed"


# ---------------------------------------------------------------------------
# lock_mask_sample + finalize_mask_sample (the reconcile finalize)
# ---------------------------------------------------------------------------


async def test_lock_mask_sample_returns_state_and_requires_txn(blk):
    pool = blk["pool"]
    mask_idx = blk["mask_idx"]
    ps1, _ = blk["prep_sample_idxs"]
    async with pool.acquire() as conn, conn.transaction():
        await create_mask_sample_pending(conn, mask_idx=mask_idx, prep_sample_idxs=[ps1])
    async with pool.acquire() as conn, conn.transaction():
        assert await lock_mask_sample(conn, mask_idx=mask_idx, prep_sample_idx=ps1) == "pending"
    # No transaction → fail loud.
    async with pool.acquire() as conn:
        with pytest.raises(RuntimeError):
            await lock_mask_sample(conn, mask_idx=mask_idx, prep_sample_idx=ps1)


async def test_lock_mask_sample_gate_advisory_serializes_and_requires_txn(blk):
    """The advisory gate lock is a real (mask_idx, prep_sample) mutex: while one
    transaction holds it, a second transaction cannot acquire the SAME key, and a
    DIFFERENT key is unaffected; once the holder rolls back, the key frees. And it
    requires a transaction (xact-scoped)."""
    pool = blk["pool"]
    mask_idx = blk["mask_idx"]
    ps1, ps2 = blk["prep_sample_idxs"]
    key_sql = "SELECT hashtextextended(format('%s:%s', $1::bigint, $2::bigint), 0)"
    async with pool.acquire() as a, pool.acquire() as b:
        key1 = await a.fetchval(key_sql, mask_idx, ps1)
        key2 = await a.fetchval(key_sql, mask_idx, ps2)
        tx_a = a.transaction()
        await tx_a.start()
        try:
            await lock_mask_sample_gate_advisory(a, mask_idx=mask_idx, prep_sample_idx=ps1)
            async with b.transaction():
                # A holds ps1's key → B cannot take it, but ps2's key is free.
                assert await b.fetchval("SELECT pg_try_advisory_xact_lock($1)", key1) is False
                assert await b.fetchval("SELECT pg_try_advisory_xact_lock($1)", key2) is True
        finally:
            await tx_a.rollback()
        # A released → B can now take ps1's key.
        async with b.transaction():
            assert await b.fetchval("SELECT pg_try_advisory_xact_lock($1)", key1) is True
    # No transaction → fail loud.
    async with pool.acquire() as conn:
        with pytest.raises(RuntimeError):
            await lock_mask_sample_gate_advisory(conn, mask_idx=mask_idx, prep_sample_idx=ps1)


async def test_finalize_mask_sample_flips_once(blk):
    pool = blk["pool"]
    mask_idx = blk["mask_idx"]
    ps1, _ = blk["prep_sample_idxs"]
    async with pool.acquire() as conn, conn.transaction():
        await create_mask_sample_pending(conn, mask_idx=mask_idx, prep_sample_idxs=[ps1])
    # First finalize moves the row; a second is a no-op (already completed).
    async with pool.acquire() as conn, conn.transaction():
        assert await finalize_mask_sample(conn, mask_idx=mask_idx, prep_sample_idx=ps1) is True
    assert (
        await pool.fetchval(
            "SELECT state FROM qiita.mask_sample WHERE mask_idx = $1 AND prep_sample_idx = $2",
            mask_idx,
            ps1,
        )
        == "completed"
    )
    async with pool.acquire() as conn, conn.transaction():
        assert await finalize_mask_sample(conn, mask_idx=mask_idx, prep_sample_idx=ps1) is False


# ---------------------------------------------------------------------------
# upsert_mask_sample_completed (the per-sample read-mask writer)
# ---------------------------------------------------------------------------


async def test_upsert_mask_sample_completed_creates_completed_row(blk):
    """The per-sample path has no PENDING phase: the upsert materializes the gate
    straight to 'completed' with no prior row."""
    pool = blk["pool"]
    mask_idx = blk["mask_idx"]
    ps1, _ = blk["prep_sample_idxs"]
    async with pool.acquire() as conn, conn.transaction():
        await upsert_mask_sample_completed(conn, mask_idx=mask_idx, prep_sample_idx=ps1)
    state = await pool.fetchval(
        "SELECT state FROM qiita.mask_sample WHERE mask_idx = $1 AND prep_sample_idx = $2",
        mask_idx,
        ps1,
    )
    assert state == "completed"


async def test_upsert_mask_sample_completed_is_idempotent_and_moves_pending_forward(blk):
    """A re-run stays completed; and an existing PENDING row (e.g. a block-path
    row for the same pair) is moved forward to completed, never backward."""
    pool = blk["pool"]
    mask_idx = blk["mask_idx"]
    ps1, ps2 = blk["prep_sample_idxs"]
    # ps2 starts PENDING (block-path materialization); ps1 has no row yet.
    async with pool.acquire() as conn, conn.transaction():
        await create_mask_sample_pending(conn, mask_idx=mask_idx, prep_sample_idxs=[ps2])
    async with pool.acquire() as conn, conn.transaction():
        await upsert_mask_sample_completed(conn, mask_idx=mask_idx, prep_sample_idx=ps1)
        await upsert_mask_sample_completed(conn, mask_idx=mask_idx, prep_sample_idx=ps2)
    # Re-run ps1 — still completed (idempotent).
    async with pool.acquire() as conn, conn.transaction():
        await upsert_mask_sample_completed(conn, mask_idx=mask_idx, prep_sample_idx=ps1)
    rows = await pool.fetch(
        "SELECT prep_sample_idx, state FROM qiita.mask_sample WHERE mask_idx = $1", mask_idx
    )
    assert {(r["prep_sample_idx"], r["state"]) for r in rows} == {
        (ps1, "completed"),
        (ps2, "completed"),
    }


async def test_upsert_mask_sample_completed_requires_txn(blk):
    """Guarded like the sibling gate writers — no transaction ⇒ fail loud."""
    pool = blk["pool"]
    mask_idx = blk["mask_idx"]
    ps1, _ = blk["prep_sample_idxs"]
    async with pool.acquire() as conn:
        with pytest.raises(RuntimeError):
            await upsert_mask_sample_completed(conn, mask_idx=mask_idx, prep_sample_idx=ps1)


# ---------------------------------------------------------------------------
# has_incomplete_covering_block (finalize gate: all covering blocks completed)
# ---------------------------------------------------------------------------


async def _seed_block_action(pool, action_id: str, created: dict) -> tuple[str, str]:
    """Ensure the named block-scoped action exists; returns (action_id, version).

    Seeds the REAL action_id (not a throwaway) because `has_incomplete_covering_block`
    discriminates read-mask blocks from align blocks by it. `created` records whether
    this call made the row, so teardown drops only what it created — the
    `(action_id, version)` PK is shared with every other test on this worker's DB.
    """
    version = _BLOCK_ACTION_VERSIONS[action_id]
    created[action_id] = await seed_block_action_if_absent(
        pool, action_id=action_id, version=version
    )
    return action_id, version


async def _drop_seeded_actions(pool, created: dict) -> None:
    for action_id, was_created in created.items():
        await delete_block_action_if_created(
            pool,
            action_id=action_id,
            version=_BLOCK_ACTION_VERSIONS[action_id],
            created=was_created,
        )


async def _block_with_ticket(blk, *, action, mask_idx, state, members, alignment_idx=None) -> int:
    """Create a block covering `members` under a ticket carrying `mask_idx`, set
    the block's state, and return its block_idx. Tracked for fixture cleanup.

    `alignment_idx` mirrors the align-block shape (which carries BOTH ids); left
    None it also reproduces a PURGED align ticket, whose alignment_idx the
    alignment delete NULLed.
    """
    pool = blk["pool"]
    action_id, version = action
    async with pool.acquire() as conn, conn.transaction():
        block_idx = await create_block(conn)
        await add_block_members(conn, block_idx=block_idx, members=members)
    blk["created_blocks"].append(block_idx)
    wt_idx = await pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx, scope_target_kind,"
        "  block_idx, mask_idx, alignment_idx)"
        " VALUES ($1, $2, $3, 'block', $4, $5, $6) RETURNING work_ticket_idx",
        action_id,
        version,
        blk["principal_idx"],
        block_idx,
        mask_idx,
        alignment_idx,
    )
    async with pool.acquire() as conn, conn.transaction():
        await set_block_work_ticket(conn, block_idx=block_idx, work_ticket_idx=wt_idx)
    async with pool.acquire() as conn:
        await set_block_state(conn, block_idx=block_idx, new_state=state)
    return block_idx


async def test_has_incomplete_covering_block_gates_on_all_blocks_completed(blk):
    """A sample split across two blocks (same mask) is finalize-eligible only when
    BOTH blocks are completed; a still-processing OR a failed sibling blocks it."""
    pool = blk["pool"]
    mask_idx = blk["mask_idx"]
    ps1, _ = blk["prep_sample_idxs"]
    created_actions: dict = {}
    action = await _seed_block_action(pool, BLOCK_MASK_ACTION_ID, created_actions)
    try:
        # Two blocks cover ps1 under this mask; block A completed, block B still
        # processing → incomplete (True).
        await _block_with_ticket(
            blk, action=action, mask_idx=mask_idx, state="completed", members=[(ps1, 0, 999)]
        )
        block_b = await _block_with_ticket(
            blk, action=action, mask_idx=mask_idx, state="processing", members=[(ps1, 1000, 1999)]
        )
        assert (
            await has_incomplete_covering_block(pool, mask_idx=mask_idx, prep_sample_idx=ps1)
            is True
        )
        # Complete block B → no incomplete covering block remains.
        async with pool.acquire() as conn:
            await set_block_state(conn, block_idx=block_b, new_state="completed")
        assert (
            await has_incomplete_covering_block(pool, mask_idx=mask_idx, prep_sample_idx=ps1)
            is False
        )
        # A FAILED sibling counts as incomplete (fail-closed): mark B failed.
        async with pool.acquire() as conn:
            await set_block_state(conn, block_idx=block_b, new_state="failed")
        assert (
            await has_incomplete_covering_block(pool, mask_idx=mask_idx, prep_sample_idx=ps1)
            is True
        )
    finally:
        await pool.execute(
            "DELETE FROM qiita.work_ticket WHERE block_idx = ANY($1::bigint[])",
            blk["created_blocks"],
        )
        await _drop_seeded_actions(pool, created_actions)


async def test_has_incomplete_covering_block_ignores_a_purged_align_block(blk):
    """A PURGED align block must not wedge the read-mask finalize gate.

    An align block ticket carries both mask_idx and alignment_idx, and
    `DELETE /alignment-definition` NULLs the alignment (`ON DELETE SET NULL`) while
    leaving mask_idx and block_idx. On column shape alone the leftover is
    indistinguishable from a read-mask block covering the same sample under the same
    mask — so a discriminator of `alignment_idx IS NULL` would count it here and
    refuse to finalize a sample whose masking is genuinely complete. Discriminating
    by action_id keeps it out.
    """
    pool = blk["pool"]
    mask_idx = blk["mask_idx"]
    ps1, _ = blk["prep_sample_idxs"]
    created_actions: dict = {}
    mask_action = await _seed_block_action(pool, BLOCK_MASK_ACTION_ID, created_actions)
    align_action = await _seed_block_action(pool, ALIGN_ACTION_ID, created_actions)
    try:
        # Masking of ps1 is genuinely finished: its one read-mask block is completed.
        await _block_with_ticket(
            blk,
            action=mask_action,
            mask_idx=mask_idx,
            state="completed",
            members=[(ps1, 0, 999)],
        )
        assert (
            await has_incomplete_covering_block(pool, mask_idx=mask_idx, prep_sample_idx=ps1)
            is False
        )
        # A FAILED align block over the same footprint, already purged (alignment_idx
        # NULL) — the exact leftover an alignment delete produces. Masking is still
        # complete, so the gate must stay open.
        await _block_with_ticket(
            blk,
            action=align_action,
            mask_idx=mask_idx,
            state="failed",
            members=[(ps1, 0, 999)],
            alignment_idx=None,
        )
        assert (
            await has_incomplete_covering_block(pool, mask_idx=mask_idx, prep_sample_idx=ps1)
            is False
        )
    finally:
        await pool.execute(
            "DELETE FROM qiita.work_ticket WHERE block_idx = ANY($1::bigint[])",
            blk["created_blocks"],
        )
        await _drop_seeded_actions(pool, created_actions)


# ---------------------------------------------------------------------------
# set_block_work_ticket (mint-ordering back-fill)
# ---------------------------------------------------------------------------


async def test_set_block_work_ticket_backfills(blk):
    """The block is created NULL-ticket, a ticket is created scoped to it, then
    the block's work_ticket_idx is back-filled — closing the mint-ordering
    cycle (work_ticket.block_idx ↔ block.work_ticket_idx)."""
    pool = blk["pool"]
    principal_idx = blk["principal_idx"]
    block_idx = await _new_block(blk)
    action_id = f"blk-act-{secrets.token_hex(4)}"
    version = "1.0.0"
    await pool.execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status)"
        " VALUES ($1, $2, 'block', '{}'::text[], $3::jsonb, '{}'::jsonb, '[]'::jsonb,"
        "         1, 1, '1 minute', 'active', 'failed')",
        action_id,
        version,
        '{"service": false, "human_roles": ["system_admin"]}',
    )
    try:
        wt_idx = await pool.fetchval(
            "INSERT INTO qiita.work_ticket"
            " (action_id, action_version, originator_principal_idx, scope_target_kind, block_idx)"
            " VALUES ($1, $2, $3, 'block', $4) RETURNING work_ticket_idx",
            action_id,
            version,
            principal_idx,
            block_idx,
        )
        async with pool.acquire() as conn, conn.transaction():
            await set_block_work_ticket(conn, block_idx=block_idx, work_ticket_idx=wt_idx)
        got = await pool.fetchval(
            "SELECT work_ticket_idx FROM qiita.block WHERE block_idx = $1", block_idx
        )
        assert got == wt_idx
    finally:
        # Delete the ticket before the action (work_ticket → action is RESTRICT).
        # Deleting the ticket also cascades away this block (block.work_ticket_idx
        # → work_ticket is ON DELETE CASCADE), so the fixture's later block sweep
        # is a harmless no-op.
        await pool.execute("DELETE FROM qiita.work_ticket WHERE block_idx = $1", block_idx)
        await pool.execute(
            "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, version
        )
