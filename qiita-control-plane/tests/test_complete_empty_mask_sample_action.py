"""Tests for `complete_empty_mask_sample`: a prep_sample that ingest found empty is
recorded as masked under a mask, with the rows a finished mask leaves behind."""

import json
import uuid

import pytest
import pytest_asyncio

from qiita_control_plane.actions.library import complete_empty_mask_sample
from qiita_control_plane.repositories.block import MaskSampleInvalidated
from qiita_control_plane.testing.db_seeds import (
    cleanup_reference_graph,
    seed_bare_feature,
    seed_bare_reference,
    seed_biosample_with_sequenced_prep_sample,
    seed_reference_membership,
    seed_sequenced_sample_subtype,
    seed_user_principal,
)
from qiita_control_plane.testing.db_teardown import (
    delete_idxs,
    delete_principal,
    teardown_entity_graph,
)

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def seeded(postgres_pool):
    """A pooled prep_sample with no reads, a SynDNA reference of three inserts, and
    two masks: one carrying that reference and one without."""
    principal_idx = await seed_user_principal(postgres_pool, prefix="empty-mask", suffix="owner")
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    run_idx, pool_idx, _ss_idx = await seed_sequenced_sample_subtype(
        postgres_pool,
        prep_sample_idx=prep_sample_idx,
        owner_idx=principal_idx,
        sequenced_pool_item_id=f"item-{uuid.uuid4().hex[:8]}",
    )
    reference_idx = await seed_bare_reference(postgres_pool, label="empty-mask-syndna")
    inserts = [await seed_bare_feature(postgres_pool) for _ in range(3)]
    for feature_idx in inserts:
        await seed_reference_membership(
            postgres_pool, reference_idx=reference_idx, feature_idx=feature_idx
        )
    masks = {}
    for name, syndna in (("syndna", {"reference_idx": reference_idx}), ("plain", None)):
        masks[name] = await postgres_pool.fetchval(
            "INSERT INTO qiita.mask_definition"
            " (params_hash, filter_workflow, filter_version, params, created_by_idx)"
            " VALUES ($1, 'read-mask', '1.0.0', $2::jsonb, $3) RETURNING mask_idx",
            uuid.uuid4().bytes + uuid.uuid4().bytes,
            json.dumps({"resolved_syndna": syndna}),
            principal_idx,
        )
    yield {
        "principal_idx": principal_idx,
        "prep_sample_idx": prep_sample_idx,
        "inserts": inserts,
        "masks": masks,
    }
    await teardown_entity_graph(
        postgres_pool,
        study_idxs=[],
        biosample_idxs=[biosample_idx],
        prep_sample_idxs=[prep_sample_idx],
    )
    await delete_idxs(postgres_pool, "sequenced_pool", pool_idx)
    await delete_idxs(postgres_pool, "sequencing_run", run_idx)
    await postgres_pool.execute(
        "DELETE FROM qiita.mask_definition WHERE mask_idx = ANY($1::bigint[])",
        list(masks.values()),
    )
    await cleanup_reference_graph(postgres_pool, reference_idx=reference_idx, feature_idxs=inserts)
    await delete_principal(postgres_pool, [principal_idx])


async def _gate(pool, mask_idx, prep_sample_idx):
    return await pool.fetchval(
        "SELECT state FROM qiita.mask_sample WHERE mask_idx = $1 AND prep_sample_idx = $2",
        mask_idx,
        prep_sample_idx,
    )


async def _counts(pool, prep_sample_idx):
    row = await pool.fetchrow(
        "SELECT raw_read_count_r1r2, biological_read_count_r1r2,"
        "       quality_filtered_read_count_r1r2, spikein_read_count_r1r2"
        "  FROM qiita.sequenced_sample WHERE prep_sample_idx = $1",
        prep_sample_idx,
    )
    return tuple(row)


async def _syndna(pool, mask_idx, prep_sample_idx):
    rows = await pool.fetch(
        "SELECT feature_idx, read_count FROM qiita.syndna_read_count"
        " WHERE mask_idx = $1 AND prep_sample_idx = $2",
        mask_idx,
        prep_sample_idx,
    )
    return {r["feature_idx"]: r["read_count"] for r in rows}


async def test_completes_the_gate_with_zero_counts(postgres_pool, seeded):
    ps, mask_idx = seeded["prep_sample_idx"], seeded["masks"]["plain"]

    await complete_empty_mask_sample(postgres_pool, mask_idx=mask_idx, prep_sample_idx=ps)

    assert await _gate(postgres_pool, mask_idx, ps) == "completed"
    assert await _counts(postgres_pool, ps) == (0, 0, 0, 0)
    assert await _syndna(postgres_pool, mask_idx, ps) == {}


async def test_writes_a_zero_per_syndna_insert(postgres_pool, seeded):
    """The SynDNA export refuses a completed sample with no count rows."""
    ps, mask_idx = seeded["prep_sample_idx"], seeded["masks"]["syndna"]

    await complete_empty_mask_sample(postgres_pool, mask_idx=mask_idx, prep_sample_idx=ps)

    assert await _gate(postgres_pool, mask_idx, ps) == "completed"
    assert await _syndna(postgres_pool, mask_idx, ps) == dict.fromkeys(seeded["inserts"], 0)


async def test_is_idempotent(postgres_pool, seeded):
    ps, mask_idx = seeded["prep_sample_idx"], seeded["masks"]["syndna"]

    for _ in range(2):
        await complete_empty_mask_sample(postgres_pool, mask_idx=mask_idx, prep_sample_idx=ps)

    assert await _gate(postgres_pool, mask_idx, ps) == "completed"
    assert await _syndna(postgres_pool, mask_idx, ps) == dict.fromkeys(seeded["inserts"], 0)


async def test_a_withdrawn_gate_stays_withdrawn_and_nothing_is_written(postgres_pool, seeded):
    ps, mask_idx = seeded["prep_sample_idx"], seeded["masks"]["syndna"]
    await postgres_pool.execute(
        "INSERT INTO qiita.mask_sample"
        "  (mask_idx, prep_sample_idx, state,"
        "   invalidated_at, invalidated_by_idx, invalidation_reason)"
        " VALUES ($1, $2, 'invalidated', now(), $3, 'test withdrawal')",
        mask_idx,
        ps,
        seeded["principal_idx"],
    )

    with pytest.raises(MaskSampleInvalidated):
        await complete_empty_mask_sample(postgres_pool, mask_idx=mask_idx, prep_sample_idx=ps)

    assert await _gate(postgres_pool, mask_idx, ps) == "invalidated"
    assert await _counts(postgres_pool, ps) == (None, None, None, None)
    assert await _syndna(postgres_pool, mask_idx, ps) == {}
