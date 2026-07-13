"""DB tests for the write-shard-assignment persistence primitive.

`write_shard_assignment` records a shard planner's output onto
qiita.reference_membership.shard_id (one shard per feature within a reference).
It is the DB side of shard assignment; the pure tiler lives in shard_planner.py and
the ingest-time wiring (feed lineages, expand genome→feature) is a later
milestone.
"""

import pytest

from qiita_control_plane.actions.library import write_shard_assignment

pytestmark = pytest.mark.db


async def _make_reference(pool, name):
    return await pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference',"
        "         (SELECT MIN(idx) FROM qiita.principal)) RETURNING reference_idx",
        name,
    )


async def _add_features(pool, reference_idx, hashes):
    """Mint features by hash and link them into the reference; return their
    feature_idxs in the given order."""
    feature_idxs = []
    for h in hashes:
        feat = await pool.fetchval(
            "INSERT INTO qiita.feature (sequence_hash) VALUES ($1::uuid)"
            " ON CONFLICT (sequence_hash) DO UPDATE SET sequence_hash = EXCLUDED.sequence_hash"
            " RETURNING feature_idx",
            h,
        )
        await pool.execute(
            "INSERT INTO qiita.reference_membership (reference_idx, feature_idx) VALUES ($1, $2)",
            reference_idx,
            feat,
        )
        feature_idxs.append(feat)
    return feature_idxs


async def _cleanup(pool, idx):
    await pool.execute("DELETE FROM qiita.reference_membership WHERE reference_idx = $1", idx)
    await pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", idx)


def _hashes(prefix, n):
    return [f"{prefix}-0000-0000-0000-{i:012d}" for i in range(n)]


async def test_write_shard_assignment_sets_shard_ids(postgres_pool):
    """shards[i] is the list of feature_idxs assigned to shard i; each feature's
    reference_membership row gets that index."""
    idx = await _make_reference(postgres_pool, "shard-assign-basic")
    try:
        f0, f1, f2 = await _add_features(postgres_pool, idx, _hashes("a0000000", 3))
        total = await write_shard_assignment(postgres_pool, idx, [[f0, f1], [f2]])
        assert total == 3
        rows = await postgres_pool.fetch(
            "SELECT feature_idx, shard_id FROM qiita.reference_membership WHERE reference_idx = $1",
            idx,
        )
        by_feature = {r["feature_idx"]: r["shard_id"] for r in rows}
        assert by_feature == {f0: 0, f1: 0, f2: 1}
    finally:
        await _cleanup(postgres_pool, idx)


async def test_write_shard_assignment_leaves_unlisted_features_null(postgres_pool):
    """A membership feature omitted from every shard list keeps shard_id NULL
    (e.g. a deferred 16S / no-genome feature)."""
    idx = await _make_reference(postgres_pool, "shard-assign-partial")
    try:
        f0, f1, f2 = await _add_features(postgres_pool, idx, _hashes("a1000000", 3))
        await write_shard_assignment(postgres_pool, idx, [[f0], [f1]])
        assert (
            await postgres_pool.fetchval(
                "SELECT shard_id FROM qiita.reference_membership"
                " WHERE reference_idx = $1 AND feature_idx = $2",
                idx,
                f2,
            )
            is None
        )
    finally:
        await _cleanup(postgres_pool, idx)


async def test_write_shard_assignment_is_idempotent(postgres_pool):
    """Re-running the same assignment (replay) sets the same values and does not
    error; the second call reports the same row count."""
    idx = await _make_reference(postgres_pool, "shard-assign-replay")
    try:
        f0, f1 = await _add_features(postgres_pool, idx, _hashes("a2000000", 2))
        first = await write_shard_assignment(postgres_pool, idx, [[f0], [f1]])
        second = await write_shard_assignment(postgres_pool, idx, [[f0], [f1]])
        assert first == second == 2
        rows = await postgres_pool.fetch(
            "SELECT feature_idx, shard_id FROM qiita.reference_membership WHERE reference_idx = $1",
            idx,
        )
        assert {r["feature_idx"]: r["shard_id"] for r in rows} == {f0: 0, f1: 1}
    finally:
        await _cleanup(postgres_pool, idx)


async def test_write_shard_assignment_clears_dropped_features_on_replan(postgres_pool):
    """A re-plan that DROPS a feature (present in the first assignment, absent
    from the second) must leave its shard_id NULL, not stale. write_shard_
    assignment clears every membership row for the reference first, then sets
    the new layout — so a shrinking re-plan can't leave orphaned shard_ids."""
    idx = await _make_reference(postgres_pool, "shard-assign-replan")
    try:
        f0, f1 = await _add_features(postgres_pool, idx, _hashes("a4000000", 2))
        await write_shard_assignment(postgres_pool, idx, [[f0], [f1]])
        # Re-plan drops f1 entirely (only f0 assigned this time).
        await write_shard_assignment(postgres_pool, idx, [[f0]])
        rows = await postgres_pool.fetch(
            "SELECT feature_idx, shard_id FROM qiita.reference_membership WHERE reference_idx = $1",
            idx,
        )
        by_feature = {r["feature_idx"]: r["shard_id"] for r in rows}
        assert by_feature == {f0: 0, f1: None}
    finally:
        await _cleanup(postgres_pool, idx)


async def test_write_shard_assignment_scoped_to_reference(postgres_pool):
    """A feature shared across two references (same sequence_hash) gets its shard
    assignment written only for the target reference's membership row."""
    idx_a = await _make_reference(postgres_pool, "shard-assign-scope-a")
    idx_b = await _make_reference(postgres_pool, "shard-assign-scope-b")
    try:
        shared = _hashes("a3000000", 2)
        fa = await _add_features(postgres_pool, idx_a, shared)
        # Same feature hashes → same feature_idxs, linked into reference B too.
        fb = await _add_features(postgres_pool, idx_b, shared)
        assert fa == fb  # dedup by sequence_hash

        await write_shard_assignment(postgres_pool, idx_a, [[fa[0], fa[1]]])

        # Reference A's rows are assigned; reference B's rows stay NULL.
        assert all(
            r["shard_id"] == 0
            for r in await postgres_pool.fetch(
                "SELECT shard_id FROM qiita.reference_membership WHERE reference_idx = $1", idx_a
            )
        )
        assert all(
            r["shard_id"] is None
            for r in await postgres_pool.fetch(
                "SELECT shard_id FROM qiita.reference_membership WHERE reference_idx = $1", idx_b
            )
        )
    finally:
        await _cleanup(postgres_pool, idx_a)
        await _cleanup(postgres_pool, idx_b)
