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
from qiita_common.actions import ALIGN_ACTION_ID, BLOCK_MASK_ACTION_ID

from qiita_control_plane.actions import library
from qiita_control_plane.actions.library import finalize_mask_sample_gate, reconcile_block
from qiita_control_plane.align_planner import ALIGN_ACTION_VERSION
from qiita_control_plane.block_planner import BLOCK_MASK_ACTION_VERSION
from qiita_control_plane.repositories.block import (
    add_block_members,
    create_block,
    has_incomplete_covering_block,
    set_block_state,
    set_block_work_ticket,
)
from qiita_control_plane.repositories.mask_definition import mint_mask_definition
from qiita_control_plane.repositories.sequence_range import mint_sequence_range
from qiita_control_plane.testing.db_seeds import (
    delete_action_if_created,
    seed_action_if_absent,
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
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

    # The REAL block action ids, because a block ticket's kind is its action_id
    # (see qiita_common.actions) and the read-mask finalize gate keys on it.
    # + delete-only-what-we-made: the (action_id, version) PK is shared with every
    # other test on this worker's database.
    created_actions = {
        block_action_id: await seed_action_if_absent(
            postgres_pool, action_id=block_action_id, version=block_version
        )
        for block_action_id, block_version in _BLOCK_ACTION_VERSIONS.items()
    }

    created_blocks: list[int] = []

    async def make_block(*, members, state, alignment_idx=None, action_id=None) -> int:
        # A block's KIND is its action_id. By default that follows alignment_idx —
        # set → an ALIGN block (whose ticket carries BOTH ids), unset → a read-mask
        # block — which is how the planners actually write them. Pass `action_id`
        # explicitly to build the one shape that combination cannot express: a
        # PURGED align block, still the align action but with its alignment_idx
        # NULLed by `DELETE /alignment-definition`.
        if action_id is None:
            action_id = ALIGN_ACTION_ID if alignment_idx is not None else BLOCK_MASK_ACTION_ID
        async with postgres_pool.acquire() as conn, conn.transaction():
            block_idx = await create_block(conn)
            await add_block_members(conn, block_idx=block_idx, members=members)
        created_blocks.append(block_idx)
        wt_idx = await postgres_pool.fetchval(
            "INSERT INTO qiita.work_ticket"
            " (action_id, action_version, originator_principal_idx, scope_target_kind,"
            "  block_idx, mask_idx, alignment_idx)"
            " VALUES ($1, $2, $3, 'block', $4, $5, $6) RETURNING work_ticket_idx",
            action_id,
            _BLOCK_ACTION_VERSIONS[action_id],
            principal_idx,
            block_idx,
            mask_idx,
            alignment_idx,
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
    for block_action_id, was_created in created_actions.items():
        await delete_action_if_created(
            postgres_pool,
            action_id=block_action_id,
            version=_BLOCK_ACTION_VERSIONS[block_action_id],
            created=was_created,
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
# finalize-mask-sample (per-sample path) must not stomp an in-flight block gate.
# ---------------------------------------------------------------------------


async def test_finalize_mask_sample_gate_refuses_when_covering_block_in_flight(rb):
    """The per-sample writer shares mask_idx with the block path. If a covering
    block is still masking the same footprint under this mask_idx, finalizing the
    gate would stomp the block's legitimate 'pending' row (and make reconcile
    short-circuit over double-written reads). It must refuse loudly and leave the
    gate untouched."""
    pool, ps, mask_idx = rb["pool"], rb["prep_sample_idx"], rb["mask_idx"]
    start = rb["seq_start"]
    await rb["make_block"](members=[(ps, start, start + _SAMPLE_READS - 1)], state="processing")

    with pytest.raises(RuntimeError, match="cross-path double-mask"):
        await finalize_mask_sample_gate(pool, mask_idx=mask_idx, prep_sample_idx=ps)

    # The block's PENDING gate row is untouched — not stomped to 'completed'.
    assert await _mask_sample_state(pool, mask_idx, ps) == "pending"


async def test_finalize_mask_sample_gate_completes_when_no_covering_block(rb):
    """With no covering block (the ordinary per-sample path), the writer upserts the
    gate straight to 'completed'."""
    pool, ps, mask_idx = rb["pool"], rb["prep_sample_idx"], rb["mask_idx"]
    await finalize_mask_sample_gate(pool, mask_idx=mask_idx, prep_sample_idx=ps)
    assert await _mask_sample_state(pool, mask_idx, ps) == "completed"


async def test_finalize_mask_sample_gate_takes_advisory_lock(rb, monkeypatch):
    """NB1: the per-sample finalize takes the (mask_idx, prep_sample) advisory gate
    lock across its check→write, serializing against the concurrent block planner
    (which takes the same lock). Spies on the helper, calling through so the txn
    still commits normally."""
    pool, ps, mask_idx = rb["pool"], rb["prep_sample_idx"], rb["mask_idx"]
    real = library.lock_mask_sample_gate_advisory
    locked: list[tuple[int, int]] = []

    async def spy(conn, *, mask_idx, prep_sample_idx):
        locked.append((mask_idx, prep_sample_idx))
        await real(conn, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx)

    monkeypatch.setattr(library, "lock_mask_sample_gate_advisory", spy)
    await finalize_mask_sample_gate(pool, mask_idx=mask_idx, prep_sample_idx=ps)
    assert locked == [(mask_idx, ps)]
    assert await _mask_sample_state(pool, mask_idx, ps) == "completed"


async def test_finalize_mask_sample_gate_ignores_inflight_align_block(rb):
    """Regression: an in-flight ALIGN block over the same (mask_idx, sample) must
    NOT wedge the per-sample read-mask finalize. Align blocks carry BOTH mask_idx
    and alignment_idx, so `has_incomplete_covering_block` (keyed on mask_idx alone
    before the fix) matched them and refused the per-sample gate forever. The
    `action_id` discriminator restricts the gate to read-mask blocks.

    Covers both align shapes, because they are not the same row: a live align block
    (alignment_idx set) and a PURGED one, whose alignment_idx
    `DELETE /alignment-definition` NULLed while leaving the mask_idx and block_idx
    that make it look like a read-mask block. Only the action tells them apart.
    """
    pool, ps, mask_idx = rb["pool"], rb["prep_sample_idx"], rb["mask_idx"]
    start = rb["seq_start"]
    align_idx = await pool.fetchval(
        "INSERT INTO qiita.alignment_definition (params_hash, params, created_by_idx)"
        " VALUES ($1, '{}'::jsonb, $2) RETURNING alignment_idx",
        secrets.token_bytes(32),
        rb["principal_idx"],
    )
    # A processing ALIGN block covering the sample under this same mask_idx.
    await rb["make_block"](
        members=[(ps, start, start + _SAMPLE_READS - 1)],
        state="processing",
        alignment_idx=align_idx,
    )
    # And the wreckage of a purged, failed align plan over the same footprint.
    await rb["make_block"](
        members=[(ps, start, start + _SAMPLE_READS - 1)],
        state="failed",
        alignment_idx=None,
        action_id=ALIGN_ACTION_ID,
    )
    try:
        # The read-mask finalize gate ignores both align blocks (action_id
        # discriminator) → no incomplete READ-MASK block covers the sample.
        assert (
            await has_incomplete_covering_block(pool, mask_idx=mask_idx, prep_sample_idx=ps)
            is False
        )
        # So the per-sample gate finalizes cleanly rather than raising cross-path.
        await finalize_mask_sample_gate(pool, mask_idx=mask_idx, prep_sample_idx=ps)
        assert await _mask_sample_state(pool, mask_idx, ps) == "completed"
    finally:
        # work_ticket rows detach (alignment_idx FK is ON DELETE SET NULL) at fixture
        # teardown; drop the alignment_definition here so its cascade can't outlive it.
        await pool.execute("DELETE FROM qiita.work_ticket WHERE alignment_idx = $1", align_idx)
        await pool.execute(
            "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1", align_idx
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
