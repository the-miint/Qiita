"""DB tests for the reconcile-alignment-block + delete-alignment-block library
primitives (the align twins of test_reconcile_block_action.py).

`reconcile_alignment_block` is the terminal step of the `align` workflow. In one
transaction it flips its block to 'completed', then for each covered sample flips
the `alignment_sample` gate to 'completed' ONLY once every covering block for that
(prep_sample, alignment) is completed. Unlike `reconcile_block` it has NO
count-assertion and NO data-plane hop (alignment rows are not 1:1 with reads —
cross-shard + PE multiplicity), so it takes only the pool + block_idx +
alignment_idx and is trivially DB-tier testable with no stubbing.
"""

import asyncio
import secrets

import pytest
import pytest_asyncio

from qiita_control_plane.actions import library
from qiita_control_plane.actions.library import reconcile_alignment_block
from qiita_control_plane.repositories.alignment_definition import mint_alignment_definition
from qiita_control_plane.repositories.block import (
    add_block_members,
    create_alignment_sample_pending,
    create_block,
    set_block_state,
    set_block_work_ticket,
)
from qiita_control_plane.repositories.sequence_range import mint_sequence_range
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
    seed_user_principal,
)

pytestmark = pytest.mark.db

_SAMPLE_READS = 1000


@pytest_asyncio.fixture
async def ab(postgres_pool):
    """Seed principal + one sequenced prep_sample (+ sequence_range) + a minted
    alignment_definition + the PENDING alignment_sample gate. Yields the ids + a
    `make_block(members, state)` helper (block + a ticket carrying the
    alignment_idx + the cover-map), tracked for FK-reverse cleanup."""
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="ab-test", suffix=suffix)
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    run_idx, pool_idx, ss_idx = await seed_sequenced_sample_subtype(
        postgres_pool,
        prep_sample_idx=prep_sample_idx,
        owner_idx=principal_idx,
        sequenced_pool_item_id=f"item-{suffix}",
    )
    async with postgres_pool.acquire() as conn:
        rng = await mint_sequence_range(
            conn, prep_sample_idx=prep_sample_idx, count=_SAMPLE_READS, principal_idx=principal_idx
        )
        align = await mint_alignment_definition(
            conn,
            params={"reference_idx": 1, "aligner": "minimap2", "mask_idx": 1, "s": suffix},
            principal_idx=principal_idx,
        )
    alignment_idx = align["alignment_idx"]
    seq_start = rng["sequence_idx_start"]
    async with postgres_pool.acquire() as conn, conn.transaction():
        await create_alignment_sample_pending(
            conn, alignment_idx=alignment_idx, prep_sample_idxs=[prep_sample_idx]
        )

    action_id = f"ab-act-{suffix}"
    version = "1.0.0"
    await postgres_pool.execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status)"
        " VALUES ($1, $2, 'block', '{}'::text[], $3::jsonb, '{}'::jsonb, '[]'::jsonb,"
        "         1, 1, '1 minute', NULL, NULL)",
        action_id,
        version,
        '{"service": false, "human_roles": ["system_admin"]}',
    )

    created_blocks: list[int] = []

    async def make_block(*, members, state) -> int:
        async with postgres_pool.acquire() as conn, conn.transaction():
            block_idx = await create_block(conn)
            await add_block_members(conn, block_idx=block_idx, members=members)
        created_blocks.append(block_idx)
        wt_idx = await postgres_pool.fetchval(
            "INSERT INTO qiita.work_ticket"
            " (action_id, action_version, originator_principal_idx, scope_target_kind,"
            "  block_idx, alignment_idx)"
            " VALUES ($1, $2, $3, 'block', $4, $5) RETURNING work_ticket_idx",
            action_id,
            version,
            principal_idx,
            block_idx,
            alignment_idx,
        )
        async with postgres_pool.acquire() as conn, conn.transaction():
            await set_block_work_ticket(conn, block_idx=block_idx, work_ticket_idx=wt_idx)
        async with postgres_pool.acquire() as conn:
            await set_block_state(conn, block_idx=block_idx, new_state=state)
        return block_idx

    yield {
        "pool": postgres_pool,
        "prep_sample_idx": prep_sample_idx,
        "alignment_idx": alignment_idx,
        "seq_start": seq_start,
        "make_block": make_block,
    }

    if created_blocks:
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE block_idx = ANY($1::bigint[])", created_blocks
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.block WHERE block_idx = ANY($1::bigint[])", created_blocks
        )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, version
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.alignment_sample WHERE alignment_idx = $1", alignment_idx
    )
    await postgres_pool.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", prep_sample_idx)
    await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx)
    await postgres_pool.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1", alignment_idx
    )
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


async def _alignment_sample_state(pool, alignment_idx, prep_sample_idx):
    return await pool.fetchval(
        "SELECT state FROM qiita.alignment_sample"
        " WHERE alignment_idx = $1 AND prep_sample_idx = $2",
        alignment_idx,
        prep_sample_idx,
    )


async def _block_state(pool, block_idx):
    return await pool.fetchval("SELECT state FROM qiita.block WHERE block_idx = $1", block_idx)


async def test_reconcile_single_block_finalizes_gate(ab):
    """A single-block sample: reconcile completes the block and flips the gate —
    no metrics rollup, no count-assertion."""
    pool, ps, alignment_idx = ab["pool"], ab["prep_sample_idx"], ab["alignment_idx"]
    start = ab["seq_start"]
    block = await ab["make_block"](
        members=[(ps, start, start + _SAMPLE_READS - 1)], state="processing"
    )
    result = await reconcile_alignment_block(pool, block_idx=block, alignment_idx=alignment_idx)
    assert result == {"block_idx": block, "finalized_samples": [ps]}
    assert await _block_state(pool, block) == "completed"
    assert await _alignment_sample_state(pool, alignment_idx, ps) == "completed"


async def test_reconcile_split_sample_waits_for_last_block(ab):
    pool, ps, alignment_idx = ab["pool"], ab["prep_sample_idx"], ab["alignment_idx"]
    start = ab["seq_start"]
    half = _SAMPLE_READS // 2
    block_a = await ab["make_block"](members=[(ps, start, start + half - 1)], state="processing")
    block_b = await ab["make_block"](
        members=[(ps, start + half, start + _SAMPLE_READS - 1)], state="processing"
    )

    res_a = await reconcile_alignment_block(pool, block_idx=block_a, alignment_idx=alignment_idx)
    assert res_a["finalized_samples"] == []
    assert await _block_state(pool, block_a) == "completed"
    assert await _alignment_sample_state(pool, alignment_idx, ps) == "pending"

    res_b = await reconcile_alignment_block(pool, block_idx=block_b, alignment_idx=alignment_idx)
    assert res_b["finalized_samples"] == [ps]
    assert await _alignment_sample_state(pool, alignment_idx, ps) == "completed"


async def test_reconcile_concurrent_finalize_exactly_once(ab):
    """Two blocks both complete 'at once' → the alignment_sample FOR UPDATE lock
    serializes them so the sample finalizes in exactly ONE result."""
    pool, ps, alignment_idx = ab["pool"], ab["prep_sample_idx"], ab["alignment_idx"]
    start = ab["seq_start"]
    half = _SAMPLE_READS // 2
    block_a = await ab["make_block"](members=[(ps, start, start + half - 1)], state="processing")
    block_b = await ab["make_block"](
        members=[(ps, start + half, start + _SAMPLE_READS - 1)], state="processing"
    )
    res_a, res_b = await asyncio.gather(
        reconcile_alignment_block(pool, block_idx=block_a, alignment_idx=alignment_idx),
        reconcile_alignment_block(pool, block_idx=block_b, alignment_idx=alignment_idx),
    )
    finalized = res_a["finalized_samples"] + res_b["finalized_samples"]
    assert finalized == [ps], f"sample must finalize exactly once, got {finalized!r}"
    assert await _alignment_sample_state(pool, alignment_idx, ps) == "completed"


async def test_reconcile_is_idempotent(ab):
    pool, ps, alignment_idx = ab["pool"], ab["prep_sample_idx"], ab["alignment_idx"]
    start = ab["seq_start"]
    block = await ab["make_block"](
        members=[(ps, start, start + _SAMPLE_READS - 1)], state="processing"
    )
    first = await reconcile_alignment_block(pool, block_idx=block, alignment_idx=alignment_idx)
    assert first["finalized_samples"] == [ps]
    second = await reconcile_alignment_block(pool, block_idx=block, alignment_idx=alignment_idx)
    assert second["finalized_samples"] == []
    assert await _alignment_sample_state(pool, alignment_idx, ps) == "completed"


async def test_delete_alignment_block_builds_footprint_from_members(ab, monkeypatch):
    """delete_alignment_block reads the block's cover-map and passes it to the
    footprint-delete DoAction as `{prep_sample_idx, sequence_idx_start,
    sequence_idx_stop}` members under the ticket's alignment_idx — the exact wire
    shape the Rust `delete_alignment_block` verifies. Postgres is untouched (the
    delete lands in DuckLake); the block state is NOT flipped here."""
    from qiita_control_plane.actions.library import delete_alignment_block

    pool, ps, alignment_idx = ab["pool"], ab["prep_sample_idx"], ab["alignment_idx"]
    start = ab["seq_start"]
    half = _SAMPLE_READS // 2
    block = await ab["make_block"](members=[(ps, start, start + half - 1)], state="processing")

    recorded: dict = {}

    async def fake_delete_data(*, alignment_idx, members, hmac_secret, data_plane_url):
        recorded.update(alignment_idx=alignment_idx, members=members)
        return 6

    monkeypatch.setattr(library, "delete_alignment_block_data", fake_delete_data)

    result = await delete_alignment_block(
        pool,
        block_idx=block,
        alignment_idx=alignment_idx,
        hmac_secret=b"s",
        data_plane_url="grpc://x",
    )
    assert result == {"block_idx": block, "rows_deleted": 6}
    assert recorded["alignment_idx"] == alignment_idx
    assert recorded["members"] == [
        {"prep_sample_idx": ps, "sequence_idx_start": start, "sequence_idx_stop": start + half - 1}
    ]
    assert await _block_state(pool, block) == "processing"
