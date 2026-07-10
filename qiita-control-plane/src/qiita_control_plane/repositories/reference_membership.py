"""Repository functions for the qiita.reference_membership table.

reference_membership is the (reference_idx, feature_idx) junction carrying the
shard planner's assignment on a nullable `shard_id`: NULL for an unsharded or
deferred feature, 0..N-1 for the lineage-sorted shard a feature belongs to.
There is no `shard_count` column — the shard-set is derived as
COUNT(DISTINCT shard_id) (or the sorted DISTINCT list) over the non-NULL rows.

That derivation is a correctness invariant shared across the arc: the
reference-add finalizer's completion threshold, the plan-shards resume gate, the
shard-index-status route's `expected_shards`, and the alignment planner's
shard-set all MUST agree on it. Centralising the two queries here removes the
copy-paste drift hazard (a finalizer reading a different threshold than the one
the planner assigned would fail *wrong*, not fail loud).

Both helpers accept a pool or a connection so they compose standalone (on the
pool) or inside an open transaction (the finalizer counts on a txn `conn`).
"""

import asyncpg


async def count_reference_shards(db: asyncpg.Pool | asyncpg.Connection, reference_idx: int) -> int:
    """Return N — the number of shards the planner assigned this reference.

    COUNT(DISTINCT shard_id) over the non-NULL reference_membership rows; 0 for
    an unsharded reference (all shard_id NULL) or one that has not been planned.
    Never NULL — SQL COUNT returns 0 on no rows.
    """
    return await db.fetchval(
        "SELECT count(DISTINCT shard_id) FROM qiita.reference_membership"
        " WHERE reference_idx = $1 AND shard_id IS NOT NULL",
        reference_idx,
    )


async def reference_shard_ids(
    db: asyncpg.Pool | asyncpg.Connection, reference_idx: int
) -> list[int]:
    """Return the reference's shard-set — the sorted DISTINCT non-NULL
    `reference_membership.shard_id` values ([] for an unsharded reference).

    The list twin of `count_reference_shards` (same predicate); baked into the
    alignment identity so a grown reference (a different shard-set) mints a new
    alignment_idx over only its new shards.
    """
    rows = await db.fetch(
        "SELECT DISTINCT shard_id FROM qiita.reference_membership"
        " WHERE reference_idx = $1 AND shard_id IS NOT NULL"
        " ORDER BY shard_id",
        reference_idx,
    )
    return [r["shard_id"] for r in rows]
