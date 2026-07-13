"""DB tests for the reconcile-block library primitive.

`reconcile_block` is the terminal step of the bulk-block read-mask workflow. In
one transaction it flips its block to 'completed', then for each covered sample
finalizes it (rolls per-stage read counts onto sequenced_sample + flips the
mask_sample gate to 'completed') ONLY once every covering block for that
(prep_sample, mask) is completed — the invariant the masked-read export gate
depends on. The per-sample FOR UPDATE lock on mask_sample serializes concurrent
block finalizers so exactly one wins.

The metrics rollup reads DuckLake via the `mask_metrics` DoAction; these tests
stub `mask_metrics_data` so the reconcile control flow (block state, finalize
gate, count assertion, idempotency, the finalize race) is exercised without a
live data plane. The DoAction itself is covered by the Rust
`mask_metrics_counts` test.
"""

import asyncio
import secrets

import pytest
import pytest_asyncio

from qiita_control_plane.actions import library
from qiita_control_plane.actions.library import reconcile_block
from qiita_control_plane.repositories.block import (
    add_block_members,
    create_block,
    set_block_state,
    set_block_work_ticket,
)
from qiita_control_plane.repositories.mask_definition import mint_mask_definition
from qiita_control_plane.repositories.sequence_range import mint_sequence_range
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
    seed_user_principal,
)

pytestmark = pytest.mark.db

# The reads-per-sample the seeded sequence_range mints; the stubbed mask_metrics
# returns row_count == this so the reconcile count-assertion passes.
_SAMPLE_READS = 1000


@pytest_asyncio.fixture
async def rb(postgres_pool):
    """Seed principal + one sequenced prep_sample with its sequenced_sample
    subtype + a minted sequence_range + a mask_definition + the PENDING
    mask_sample gate. Yields the ids + a `make_block(members, state)` helper that
    creates a block, a ticket carrying the mask_idx, and the cover-map, tracked
    for FK-reverse cleanup."""
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="rb-test", suffix=suffix)
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
            conn,
            prep_sample_idx=prep_sample_idx,
            count=_SAMPLE_READS,
            principal_idx=principal_idx,
            work_ticket_idx=None,
        )
        mask = await mint_mask_definition(
            conn,
            filter_workflow="read-mask",
            filter_version="1.0.0",
            params={"workflow": "read-mask", "s": suffix},
            principal_idx=principal_idx,
        )
    mask_idx = mask["mask_idx"]
    seq_start = rng["sequence_idx_start"]
    # Materialize the PENDING gate for this sample under the mask (plan-time step).
    await postgres_pool.execute(
        "INSERT INTO qiita.mask_sample (mask_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, 'pending')",
        mask_idx,
        prep_sample_idx,
    )

    action_id = f"rb-act-{suffix}"
    version = "1.0.0"
    await postgres_pool.execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status)"
        " VALUES ($1, $2, 'block', '{}'::text[], $3::jsonb, '{}'::jsonb, '[]'::jsonb,"
        "         1, 1, '1 minute', 'active', 'failed')",
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
            "  block_idx, mask_idx)"
            " VALUES ($1, $2, $3, 'block', $4, $5) RETURNING work_ticket_idx",
            action_id,
            version,
            principal_idx,
            block_idx,
            mask_idx,
        )
        async with postgres_pool.acquire() as conn, conn.transaction():
            await set_block_work_ticket(conn, block_idx=block_idx, work_ticket_idx=wt_idx)
        async with postgres_pool.acquire() as conn:
            await set_block_state(conn, block_idx=block_idx, new_state=state)
        return block_idx

    yield {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "prep_sample_idx": prep_sample_idx,
        "ss_idx": ss_idx,
        "mask_idx": mask_idx,
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
    await postgres_pool.execute("DELETE FROM qiita.mask_sample WHERE mask_idx = $1", mask_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", prep_sample_idx)
    await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx)
    await postgres_pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


def _stub_metrics(monkeypatch, *, row_count=_SAMPLE_READS, raw=None, biological=None, qf=None):
    """Stub mask_metrics_data with fixed counts (default: consistent with the
    seeded sequence_range so the count assertion passes)."""
    counts = {
        "raw": raw if raw is not None else row_count,
        "biological": biological if biological is not None else row_count,
        "quality_filtered": qf if qf is not None else row_count,
        "row_count": row_count,
    }

    async def fake(*, mask_idx, prep_sample_idx, signing_key, data_plane_url):
        return dict(counts)

    monkeypatch.setattr(library, "mask_metrics_data", fake)
    return counts


async def _mask_sample_state(pool, mask_idx, prep_sample_idx):
    return await pool.fetchval(
        "SELECT state FROM qiita.mask_sample WHERE mask_idx = $1 AND prep_sample_idx = $2",
        mask_idx,
        prep_sample_idx,
    )


async def _block_state(pool, block_idx):
    return await pool.fetchval("SELECT state FROM qiita.block WHERE block_idx = $1", block_idx)


async def _metrics(pool, ss_idx):
    return await pool.fetchrow(
        "SELECT raw_read_count_r1r2, biological_read_count_r1r2,"
        " quality_filtered_read_count_r1r2 FROM qiita.sequenced_sample WHERE idx = $1",
        ss_idx,
    )


# ---------------------------------------------------------------------------
# Single-block sample: reconcile completes the block and finalizes the sample.
# ---------------------------------------------------------------------------


async def test_reconcile_single_block_finalizes_sample(rb, monkeypatch):
    pool, ps, mask_idx = rb["pool"], rb["prep_sample_idx"], rb["mask_idx"]
    start = rb["seq_start"]
    _stub_metrics(monkeypatch, raw=1000, biological=900, qf=850)
    block = await rb["make_block"](
        members=[(ps, start, start + _SAMPLE_READS - 1)], state="processing"
    )

    result = await reconcile_block(
        pool, block_idx=block, mask_idx=mask_idx, signing_key=b"s", data_plane_url="grpc://x"
    )

    assert result == {"block_idx": block, "finalized_samples": [ps]}
    assert await _block_state(pool, block) == "completed"
    assert await _mask_sample_state(pool, mask_idx, ps) == "completed"
    row = await _metrics(pool, rb["ss_idx"])
    assert (row["raw_read_count_r1r2"], row["biological_read_count_r1r2"]) == (1000, 900)
    assert row["quality_filtered_read_count_r1r2"] == 850


# ---------------------------------------------------------------------------
# Split sample: two blocks cover it; finalize waits for the LAST one.
# ---------------------------------------------------------------------------


async def test_reconcile_split_sample_waits_for_last_block(rb, monkeypatch):
    pool, ps, mask_idx = rb["pool"], rb["prep_sample_idx"], rb["mask_idx"]
    start = rb["seq_start"]
    _stub_metrics(monkeypatch)
    half = _SAMPLE_READS // 2
    block_a = await rb["make_block"](members=[(ps, start, start + half - 1)], state="processing")
    block_b = await rb["make_block"](
        members=[(ps, start + half, start + _SAMPLE_READS - 1)], state="processing"
    )

    # Reconcile block A while B is still processing → A completes, sample NOT
    # finalized (a sibling still owes reads).
    res_a = await reconcile_block(
        pool, block_idx=block_a, mask_idx=mask_idx, signing_key=b"s", data_plane_url="grpc://x"
    )
    assert res_a["finalized_samples"] == []
    assert await _block_state(pool, block_a) == "completed"
    assert await _mask_sample_state(pool, mask_idx, ps) == "pending"

    # Reconcile block B (now the last) → sample finalizes.
    res_b = await reconcile_block(
        pool, block_idx=block_b, mask_idx=mask_idx, signing_key=b"s", data_plane_url="grpc://x"
    )
    assert res_b["finalized_samples"] == [ps]
    assert await _mask_sample_state(pool, mask_idx, ps) == "completed"


# ---------------------------------------------------------------------------
# Concurrent finalize race: two blocks, both complete "at once" → exactly one
# finalize.
# ---------------------------------------------------------------------------


async def test_reconcile_concurrent_finalize_exactly_once(rb, monkeypatch):
    pool, ps, mask_idx = rb["pool"], rb["prep_sample_idx"], rb["mask_idx"]
    start = rb["seq_start"]
    _stub_metrics(monkeypatch)
    half = _SAMPLE_READS // 2
    block_a = await rb["make_block"](members=[(ps, start, start + half - 1)], state="processing")
    block_b = await rb["make_block"](
        members=[(ps, start + half, start + _SAMPLE_READS - 1)], state="processing"
    )

    # Both reconciles run concurrently; the mask_sample FOR UPDATE lock serializes
    # them so the sample finalizes in exactly ONE of the two results.
    res_a, res_b = await asyncio.gather(
        reconcile_block(
            pool, block_idx=block_a, mask_idx=mask_idx, signing_key=b"s", data_plane_url="grpc://x"
        ),
        reconcile_block(
            pool, block_idx=block_b, mask_idx=mask_idx, signing_key=b"s", data_plane_url="grpc://x"
        ),
    )
    finalized = res_a["finalized_samples"] + res_b["finalized_samples"]
    assert finalized == [ps], f"sample must finalize exactly once, got {finalized!r}"
    assert await _mask_sample_state(pool, mask_idx, ps) == "completed"


# ---------------------------------------------------------------------------
# Idempotent re-run: reconciling a block whose sample is already finalized is a
# no-op.
# ---------------------------------------------------------------------------


async def test_reconcile_is_idempotent(rb, monkeypatch):
    pool, ps, mask_idx = rb["pool"], rb["prep_sample_idx"], rb["mask_idx"]
    start = rb["seq_start"]
    _stub_metrics(monkeypatch)
    block = await rb["make_block"](
        members=[(ps, start, start + _SAMPLE_READS - 1)], state="processing"
    )

    first = await reconcile_block(
        pool, block_idx=block, mask_idx=mask_idx, signing_key=b"s", data_plane_url="grpc://x"
    )
    assert first["finalized_samples"] == [ps]
    # Re-run (a redrive): the block is already completed, the sample already
    # finalized → nothing new is finalized.
    second = await reconcile_block(
        pool, block_idx=block, mask_idx=mask_idx, signing_key=b"s", data_plane_url="grpc://x"
    )
    assert second["finalized_samples"] == []
    assert await _mask_sample_state(pool, mask_idx, ps) == "completed"


# ---------------------------------------------------------------------------
# Count assertion: a read_mask row_count that disagrees with sequence_range is a
# cover-map / masking defect — fail loud, do not finalize.
# ---------------------------------------------------------------------------


async def test_reconcile_count_mismatch_raises(rb, monkeypatch):
    pool, ps, mask_idx = rb["pool"], rb["prep_sample_idx"], rb["mask_idx"]
    start = rb["seq_start"]
    # row_count short of the sample's 1000 reads → assertion trips.
    _stub_metrics(monkeypatch, row_count=999)
    block = await rb["make_block"](
        members=[(ps, start, start + _SAMPLE_READS - 1)], state="processing"
    )

    with pytest.raises(RuntimeError, match="does not fully tile"):
        await reconcile_block(
            pool, block_idx=block, mask_idx=mask_idx, signing_key=b"s", data_plane_url="grpc://x"
        )
    # The transaction rolled back: neither the gate nor (in the same txn) the
    # block flip persisted a finalize.
    assert await _mask_sample_state(pool, mask_idx, ps) == "pending"


# ---------------------------------------------------------------------------
# delete-block-mask (idempotent block replace): the primitive builds the footprint
# payload from block_member and hands it to the DoAction under the ticket mask_idx.
# ---------------------------------------------------------------------------


async def test_delete_read_mask_block_builds_footprint_from_members(rb, monkeypatch):
    """The delete_read_mask_block primitive reads the block's cover-map and passes
    it to the footprint-delete DoAction as `{prep_sample_idx, sequence_idx_start,
    sequence_idx_stop}` members under the ticket's mask_idx — the exact wire shape
    the Rust `delete_read_mask_block` verifies. Postgres is untouched (the delete
    lands in DuckLake); the primitive is pure read + DoAction."""
    from qiita_control_plane.actions.library import delete_read_mask_block

    pool, ps, mask_idx = rb["pool"], rb["prep_sample_idx"], rb["mask_idx"]
    start = rb["seq_start"]
    # A split sample: this block owns only the first half.
    half = _SAMPLE_READS // 2
    block = await rb["make_block"](members=[(ps, start, start + half - 1)], state="processing")

    recorded: dict = {}

    async def fake_delete_data(*, mask_idx, members, signing_key, data_plane_url):
        recorded.update(
            mask_idx=mask_idx,
            members=members,
            signing_key=signing_key,
            data_plane_url=data_plane_url,
        )
        return 3

    monkeypatch.setattr(library, "delete_read_mask_block_data", fake_delete_data)

    result = await delete_read_mask_block(
        pool, block_idx=block, mask_idx=mask_idx, signing_key=b"s", data_plane_url="grpc://x"
    )

    assert result == {"block_idx": block, "rows_deleted": 3}
    assert recorded["mask_idx"] == mask_idx
    assert recorded["members"] == [
        {
            "prep_sample_idx": ps,
            "sequence_idx_start": start,
            "sequence_idx_stop": start + half - 1,
        }
    ]
    # The block state is NOT touched by this step (reconcile-block flips it later).
    assert await _block_state(pool, block) == "processing"
