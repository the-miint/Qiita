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

The junction is also how a reference's SEQUENCE SET is addressed without going
near the stored bytes — `reference_sequence_hashes` reads the per-sequence content
hashes the features were minted on, which is what the read mask's adapter identity
is derived from.

Joined to `feature_genome` it is also the reference's GENOME MAP — the
feature_idx → genome lookup a client rolls alignment rows up through
(`fetch_genome_map`). That is the same row set `actions.library.export_member_genome`
streams to Parquet for the compute side, so the two are required to agree.

Every helper accepts a pool or a connection so they compose standalone (on the
pool) or inside an open transaction (the finalizer counts on a txn `conn`).
"""

import hashlib

import asyncpg


async def reference_sequence_hashes(
    db: asyncpg.Pool | asyncpg.Connection, reference_idx: int
) -> list[bytes]:
    """Return the reference's per-sequence content hashes, sorted, as raw bytes.

    `qiita.feature.sequence_hash` is the content hash the feature was
    deduplicated on at mint (`qiita_common.chunking.canonical_sequence_hash_expr`,
    which every minter is required to use), so this is a function of the
    reference's SEQUENCES, independent of how they are stored or serialized. That
    expression is strand-canonical, so a sequence and its reverse complement are
    one member here. Sorted so the list is order-independent, and returned as the
    UUID's raw 16 bytes rather than its hex form (the repo's fixed-width-hash
    rule).

    The row set is the reference's membership, which is exactly what a DoGet of
    `reference_sequence_chunks` scoped to this reference returns: that read has no
    reference_idx column of its own and resolves the scope by joining the data
    plane's `reference_membership` mirror of this table. Annotated intervals are
    deliberately absent from membership (`actions.library.mint_annotation_features`),
    so a GFF-bearing reference does not leak interval features in here.

    [] for an unknown reference or one with no members — callers that treat an
    empty adapter set as a misconfiguration check for it themselves, as
    `_write_adapter_parquet` already does.
    """
    rows = await db.fetch(
        "SELECT f.sequence_hash"
        "  FROM qiita.reference_membership m"
        "  JOIN qiita.feature f ON f.feature_idx = m.feature_idx"
        " WHERE m.reference_idx = $1"
        " ORDER BY f.sequence_hash",
        reference_idx,
    )
    return [r["sequence_hash"].bytes for r in rows]


async def reference_sequence_set_hash(
    db: asyncpg.Pool | asyncpg.Connection, reference_idx: int
) -> str | None:
    """SHA-256 hex over the reference's sorted per-sequence content hashes — a
    stable identity for its sequence SET.

    The read mask folds this in as `resolved_qc.adapter_set_hash`, so it is keyed
    on the sequences (`reference_sequence_hashes`) rather than on the reference
    idx: a re-pointed-but-identical adapter set collapses to one mask. Digest
    input is the UUIDs' raw 16 bytes in sorted order.

    Those hashes are strand-canonical, so a sequence and its reverse complement
    are one member and produce one identity.

    Returns None for a reference with no members. `runner._reference` refuses an
    empty adapter set on the path that materializes it, so this is reachable only
    for a caller that resolves the identity without materializing. A digest over
    no input would be one value shared by every memberless reference; None is not.
    """
    hashes = await reference_sequence_hashes(db, reference_idx)
    if not hashes:
        return None
    digest = hashlib.sha256()
    for sequence_hash in hashes:
        digest.update(sequence_hash)
    return digest.hexdigest()


# The genome map's row set, shared verbatim by the fetch and the count so the
# size a 413 reports is provably the size the fetch refused — an independently
# written count would drift into a lie.
_GENOME_MAP_FROM = (
    " FROM qiita.reference_membership rm"
    " JOIN qiita.feature_genome fg USING (feature_idx)"
    " JOIN qiita.genome g ON g.genome_idx = fg.genome_idx"
    " WHERE rm.reference_idx = $1"
)


async def fetch_genome_map(
    db: asyncpg.Pool | asyncpg.Connection, reference_idx: int, *, limit: int
) -> list[asyncpg.Record]:
    """The reference's feature_idx → genome lookup: one row per (feature, genome)
    pair with the genome's `source` / `source_id` provenance, ordered by
    (feature_idx, genome_idx), at most `limit` rows.

    `actions.library.export_member_genome`'s row set, widened with the genome
    columns — the two MUST agree on which features have genomes, since the compute
    job consumes the Parquet and the client consumes this. Same INNER JOIN, so a
    feature with no genome is dropped by both: it cannot be rolled up.

    Ordered, unlike `export_member_genome` (which dropped its ORDER BY because no
    consumer needed one): a capped read needs a stable order for the cap to mean
    anything, and a client diffing two pulls of an unchanged reference should see
    no churn. The many-to-many `feature_genome` means a shared plasmid yields one
    row per genome — this returns PAIRS, not features.
    """
    return await db.fetch(
        "SELECT rm.feature_idx, fg.genome_idx, g.source, g.source_id"
        + _GENOME_MAP_FROM
        + " ORDER BY rm.feature_idx, fg.genome_idx LIMIT $2",
        reference_idx,
        limit,
    )


async def count_genome_map(db: asyncpg.Pool | asyncpg.Connection, reference_idx: int) -> int:
    """How many (feature, genome) pairs `fetch_genome_map` would return uncapped.

    Only the refusal path pays for this — the route over-fetches by one to detect
    the overflow, then counts to name the real size, so a caller learns whether
    they are 2x or 100x over the cap.
    """
    return await db.fetchval("SELECT count(*)" + _GENOME_MAP_FROM, reference_idx)


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
