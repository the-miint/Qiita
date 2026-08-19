"""Repository-layer tests for the alignment_sample completion gate.

Exercises qiita.alignment_sample via the repositories.block helpers
(create_alignment_sample_pending / lock_alignment_sample /
finalize_alignment_sample / has_incomplete_covering_alignment_block). The gate
is the exact twin of qiita.mask_sample (see tests/repositories/test_block.py),
keyed on alignment_idx instead of mask_idx and joining the covering-block check
on work_ticket.alignment_idx.

Each test seeds its own principal + sequenced prep_samples + an
alignment_definition so cleanup runs in FK-reverse order and the suite can run
against the shared postgres_pool fixture. Blocks a test creates are tracked in
`blk['created_blocks']` so teardown targets exactly those rows.
"""

import secrets

import pytest
import pytest_asyncio

from qiita_control_plane.repositories.alignment_definition import mint_alignment_definition
from qiita_control_plane.repositories.block import (
    add_block_members,
    create_alignment_sample_pending,
    create_block,
    finalize_alignment_sample,
    has_incomplete_covering_alignment_block,
    lock_alignment_sample,
    set_block_state,
    set_block_work_ticket,
)
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def blk(postgres_pool):
    """Seed a principal, two sequenced prep_samples, and an alignment_definition."""
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="align-gate", suffix=suffix)
    bs1, ps1 = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    bs2, ps2 = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    async with postgres_pool.acquire() as conn:
        alignment = await mint_alignment_definition(
            conn,
            params={"reference_idx": 1, "aligner": "minimap2", "mask_idx": 1, "shard_ids": [0]},
            principal_idx=principal_idx,
        )
    alignment_idx = alignment["alignment_idx"]
    created_blocks: list[int] = []

    yield {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "prep_sample_idxs": [ps1, ps2],
        "alignment_idx": alignment_idx,
        "created_blocks": created_blocks,
    }

    if created_blocks:
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE block_idx = ANY($1::bigint[])", created_blocks
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.block WHERE block_idx = ANY($1::bigint[])", created_blocks
        )
    await postgres_pool.execute(
        "DELETE FROM qiita.alignment_sample WHERE alignment_idx = $1", alignment_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", [ps1, ps2]
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", [bs1, bs2]
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1", alignment_idx
    )
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


# ---------------------------------------------------------------------------
# create_alignment_sample_pending (the completion gate)
# ---------------------------------------------------------------------------


async def test_create_alignment_sample_pending_materializes_gate(blk):
    pool = blk["pool"]
    alignment_idx = blk["alignment_idx"]
    ps1, ps2 = blk["prep_sample_idxs"]
    async with pool.acquire() as conn, conn.transaction():
        await create_alignment_sample_pending(
            conn, alignment_idx=alignment_idx, prep_sample_idxs=[ps1, ps2]
        )
    rows = await pool.fetch(
        "SELECT prep_sample_idx, state FROM qiita.alignment_sample WHERE alignment_idx = $1",
        alignment_idx,
    )
    assert {(r["prep_sample_idx"], r["state"]) for r in rows} == {
        (ps1, "pending"),
        (ps2, "pending"),
    }


async def test_create_alignment_sample_pending_is_idempotent(blk):
    pool = blk["pool"]
    alignment_idx = blk["alignment_idx"]
    ps1, _ = blk["prep_sample_idxs"]
    async with pool.acquire() as conn, conn.transaction():
        await create_alignment_sample_pending(
            conn, alignment_idx=alignment_idx, prep_sample_idxs=[ps1]
        )
    # Flip to completed, then re-run PENDING materialization: ON CONFLICT DO
    # NOTHING must NOT resurrect the completed row back to pending.
    await pool.execute(
        "UPDATE qiita.alignment_sample SET state = 'completed'"
        " WHERE alignment_idx = $1 AND prep_sample_idx = $2",
        alignment_idx,
        ps1,
    )
    async with pool.acquire() as conn, conn.transaction():
        await create_alignment_sample_pending(
            conn, alignment_idx=alignment_idx, prep_sample_idxs=[ps1]
        )
    state = await pool.fetchval(
        "SELECT state FROM qiita.alignment_sample"
        " WHERE alignment_idx = $1 AND prep_sample_idx = $2",
        alignment_idx,
        ps1,
    )
    assert state == "completed"


# ---------------------------------------------------------------------------
# lock + finalize (the reconcile finalize)
# ---------------------------------------------------------------------------


async def test_lock_alignment_sample_returns_state_and_requires_txn(blk):
    pool = blk["pool"]
    alignment_idx = blk["alignment_idx"]
    ps1, _ = blk["prep_sample_idxs"]
    async with pool.acquire() as conn, conn.transaction():
        await create_alignment_sample_pending(
            conn, alignment_idx=alignment_idx, prep_sample_idxs=[ps1]
        )
    async with pool.acquire() as conn, conn.transaction():
        assert (
            await lock_alignment_sample(conn, alignment_idx=alignment_idx, prep_sample_idx=ps1)
            == "pending"
        )
    async with pool.acquire() as conn:
        with pytest.raises(RuntimeError):
            await lock_alignment_sample(conn, alignment_idx=alignment_idx, prep_sample_idx=ps1)


async def test_finalize_alignment_sample_flips_once(blk):
    pool = blk["pool"]
    alignment_idx = blk["alignment_idx"]
    ps1, _ = blk["prep_sample_idxs"]
    async with pool.acquire() as conn, conn.transaction():
        await create_alignment_sample_pending(
            conn, alignment_idx=alignment_idx, prep_sample_idxs=[ps1]
        )
    async with pool.acquire() as conn, conn.transaction():
        assert (
            await finalize_alignment_sample(conn, alignment_idx=alignment_idx, prep_sample_idx=ps1)
            is True
        )
    assert (
        await pool.fetchval(
            "SELECT state FROM qiita.alignment_sample"
            " WHERE alignment_idx = $1 AND prep_sample_idx = $2",
            alignment_idx,
            ps1,
        )
        == "completed"
    )
    async with pool.acquire() as conn, conn.transaction():
        assert (
            await finalize_alignment_sample(conn, alignment_idx=alignment_idx, prep_sample_idx=ps1)
            is False
        )


# ---------------------------------------------------------------------------
# has_incomplete_covering_alignment_block (finalize gate: all blocks completed)
# ---------------------------------------------------------------------------


async def _seed_block_action(pool) -> tuple[str, str]:
    """Create a throwaway block-scoped action; returns (action_id, version)."""
    action_id = f"align-cov-{secrets.token_hex(4)}"
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
    return action_id, version


async def _block_with_ticket(blk, *, action, alignment_idx, state, members) -> int:
    """Create a block covering `members` under a ticket carrying `alignment_idx`,
    set the block's state, and return its block_idx. Tracked for fixture cleanup."""
    pool = blk["pool"]
    action_id, version = action
    async with pool.acquire() as conn, conn.transaction():
        block_idx = await create_block(conn)
        await add_block_members(conn, block_idx=block_idx, members=members)
    blk["created_blocks"].append(block_idx)
    wt_idx = await pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx, scope_target_kind,"
        "  block_idx, alignment_idx)"
        " VALUES ($1, $2, $3, 'block', $4, $5) RETURNING work_ticket_idx",
        action_id,
        version,
        blk["principal_idx"],
        block_idx,
        alignment_idx,
    )
    async with pool.acquire() as conn, conn.transaction():
        await set_block_work_ticket(conn, block_idx=block_idx, work_ticket_idx=wt_idx)
    async with pool.acquire() as conn:
        await set_block_state(conn, block_idx=block_idx, new_state=state)
    return block_idx


async def test_has_incomplete_covering_alignment_block_gates_on_all_blocks_completed(blk):
    """A sample split across two blocks (same alignment) is finalize-eligible only
    when BOTH blocks are completed; a still-processing OR a failed sibling blocks
    it (fail-closed)."""
    pool = blk["pool"]
    alignment_idx = blk["alignment_idx"]
    ps1, _ = blk["prep_sample_idxs"]
    action = await _seed_block_action(pool)
    try:
        await _block_with_ticket(
            blk,
            action=action,
            alignment_idx=alignment_idx,
            state="completed",
            members=[(ps1, 0, 999)],
        )
        block_b = await _block_with_ticket(
            blk,
            action=action,
            alignment_idx=alignment_idx,
            state="processing",
            members=[(ps1, 1000, 1999)],
        )
        assert (
            await has_incomplete_covering_alignment_block(
                pool, alignment_idx=alignment_idx, prep_sample_idx=ps1
            )
            is True
        )
        async with pool.acquire() as conn:
            await set_block_state(conn, block_idx=block_b, new_state="completed")
        assert (
            await has_incomplete_covering_alignment_block(
                pool, alignment_idx=alignment_idx, prep_sample_idx=ps1
            )
            is False
        )
        async with pool.acquire() as conn:
            await set_block_state(conn, block_idx=block_b, new_state="failed")
        assert (
            await has_incomplete_covering_alignment_block(
                pool, alignment_idx=alignment_idx, prep_sample_idx=ps1
            )
            is True
        )
    finally:
        await pool.execute(
            "DELETE FROM qiita.work_ticket WHERE block_idx = ANY($1::bigint[])",
            blk["created_blocks"],
        )
        await pool.execute(
            "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action[0], action[1]
        )
