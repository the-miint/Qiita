"""Repository-layer tests for qiita.reference_membership.

Exercises count_reference_shards / reference_shard_ids — the two derivations of
a reference's shard-set (COUNT(DISTINCT shard_id) and the sorted DISTINCT list
over the non-NULL membership rows). These centralise a query that was
byte-identical across the reference-add finalizer, the plan-shards resume gate,
the shard-index-status route, and the alignment planner; the tests pin the
count/list agreement (and the NULL-exclusion) a copy-paste drift would silently
break.

Each test seeds its own principal + reference + features so cleanup runs in
FK-reverse order and the suite can run in parallel against postgres_pool.
"""

import secrets
import uuid

import pytest
import pytest_asyncio

from qiita_control_plane.repositories.reference_membership import (
    count_reference_shards,
    reference_shard_ids,
)
from qiita_control_plane.testing.db_seeds import seed_user_principal

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def ref(postgres_pool):
    """Seed one principal + one reference; yield a context dict.

    The test body seeds reference_membership rows via the `_seed_member` helper.
    Cleanup runs in FK-reverse order (membership → feature → reference → user →
    principal) so the suite is parallel-safe against the shared pool.
    """
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="refmem-test", suffix=suffix)
    reference_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, created_by_idx)"
        " VALUES ($1, $2, 'sequence_reference', $3) RETURNING reference_idx",
        f"refmem-{suffix}",
        "1.0",
        principal_idx,
    )
    feature_idxs: list[int] = []

    yield {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "reference_idx": reference_idx,
        "feature_idxs": feature_idxs,
    }

    await postgres_pool.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1",
        reference_idx,
    )
    if feature_idxs:
        await postgres_pool.execute(
            "DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])",
            feature_idxs,
        )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


async def _seed_member(ref, shard_id):
    """Insert one fresh feature + a reference_membership row on `ref` with the
    given shard_id (None for an unassigned / deferred feature). The feature's
    sequence_hash is a random UUID so each row is a distinct content-hash."""
    pool = ref["pool"]
    feature_idx = await pool.fetchval(
        "INSERT INTO qiita.feature (sequence_hash) VALUES ($1) RETURNING feature_idx",
        uuid.uuid4(),
    )
    ref["feature_idxs"].append(feature_idx)
    await pool.execute(
        "INSERT INTO qiita.reference_membership (reference_idx, feature_idx, shard_id)"
        " VALUES ($1, $2, $3)",
        ref["reference_idx"],
        feature_idx,
        shard_id,
    )
    return feature_idx


async def test_count_and_list_over_multiple_shards(ref):
    """N distinct shard_ids across several features (with a shard shared by two
    features) → count = N, list = sorted distinct, insertion order irrelevant."""
    for shard_id in (2, 0, 1, 1):
        await _seed_member(ref, shard_id)
    pool, reference_idx = ref["pool"], ref["reference_idx"]
    assert await count_reference_shards(pool, reference_idx) == 3
    assert await reference_shard_ids(pool, reference_idx) == [0, 1, 2]


async def test_null_shard_rows_are_excluded(ref):
    """shard_id NULL rows (unassigned / deferred features) don't count toward N."""
    await _seed_member(ref, 0)
    await _seed_member(ref, 1)
    await _seed_member(ref, None)
    await _seed_member(ref, None)
    pool, reference_idx = ref["pool"], ref["reference_idx"]
    assert await count_reference_shards(pool, reference_idx) == 2
    assert await reference_shard_ids(pool, reference_idx) == [0, 1]


async def test_empty_reference_is_zero_and_empty(ref):
    """A reference with no membership rows → count 0, list []."""
    pool, reference_idx = ref["pool"], ref["reference_idx"]
    assert await count_reference_shards(pool, reference_idx) == 0
    assert await reference_shard_ids(pool, reference_idx) == []


async def test_all_null_reference_is_zero_and_empty(ref):
    """An unsharded reference (every membership row shard_id NULL) → 0 / []."""
    await _seed_member(ref, None)
    await _seed_member(ref, None)
    pool, reference_idx = ref["pool"], ref["reference_idx"]
    assert await count_reference_shards(pool, reference_idx) == 0
    assert await reference_shard_ids(pool, reference_idx) == []


async def test_accepts_connection_inside_transaction(ref):
    """finalize_shard calls the count on a txn conn — assert both helpers accept
    an acquired Connection, not only the pool."""
    await _seed_member(ref, 0)
    await _seed_member(ref, 1)
    pool, reference_idx = ref["pool"], ref["reference_idx"]
    async with pool.acquire() as conn, conn.transaction():
        assert await count_reference_shards(conn, reference_idx) == 2
        assert await reference_shard_ids(conn, reference_idx) == [0, 1]
