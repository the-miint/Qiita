"""Repository-layer tests for qiita.reference_exclusion — the global blocklist
of bad genome_idx / feature_idx.

Exercises the resolution (a genome block expands to all its features via
feature_genome; a direct feature block stands alone; UNION dedups a feature
blocked both ways), the idempotent add / reversible remove, the exactly-one
CHECK, and the reference-scoped listing (blocked features in a reference, with
provenance + external ids).

Each test seeds its own principal + reference + genomes/features so cleanup runs
in FK-reverse order and the suite is parallel-safe against postgres_pool.
"""

import secrets
import uuid

import asyncpg
import pytest
import pytest_asyncio

from qiita_control_plane.repositories.reference_exclusion import (
    add_exclusion,
    list_for_reference,
    remove_exclusion,
    resolve_excluded_features,
)
from qiita_control_plane.testing.db_seeds import seed_user_principal

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def ref(postgres_pool):
    """Seed one principal + one reference; yield a context dict with seed
    helpers' bookkeeping lists. Cleanup runs FK-reverse (exclusion → membership
    → feature_genome → feature → genome → reference → user → principal)."""
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="refexcl-test", suffix=suffix)
    reference_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, created_by_idx)"
        " VALUES ($1, $2, 'sequence_reference', $3) RETURNING reference_idx",
        f"refexcl-{suffix}",
        "1.0",
        principal_idx,
    )
    ctx = {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "reference_idx": reference_idx,
        "feature_idxs": [],
        "genome_idxs": [],
    }
    yield ctx

    pool = postgres_pool
    if ctx["feature_idxs"]:
        await pool.execute(
            "DELETE FROM qiita.reference_exclusion WHERE feature_idx = ANY($1::bigint[])",
            ctx["feature_idxs"],
        )
    if ctx["genome_idxs"]:
        await pool.execute(
            "DELETE FROM qiita.reference_exclusion WHERE genome_idx = ANY($1::bigint[])",
            ctx["genome_idxs"],
        )
    await pool.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", reference_idx
    )
    if ctx["feature_idxs"]:
        await pool.execute(
            "DELETE FROM qiita.feature_genome WHERE feature_idx = ANY($1::bigint[])",
            ctx["feature_idxs"],
        )
        await pool.execute(
            "DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])", ctx["feature_idxs"]
        )
    if ctx["genome_idxs"]:
        await pool.execute(
            "DELETE FROM qiita.genome WHERE genome_idx = ANY($1::bigint[])", ctx["genome_idxs"]
        )
    await pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx)
    await pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


async def _seed_feature(ctx, *, in_reference=True, accession=None):
    """Insert a fresh feature (random content-hash); optionally link it into the
    fixture's reference with an accession. Returns feature_idx."""
    pool = ctx["pool"]
    feature_idx = await pool.fetchval(
        "INSERT INTO qiita.feature (sequence_hash) VALUES ($1) RETURNING feature_idx", uuid.uuid4()
    )
    ctx["feature_idxs"].append(feature_idx)
    if in_reference:
        await pool.execute(
            "INSERT INTO qiita.reference_membership (reference_idx, feature_idx, accession)"
            " VALUES ($1, $2, $3)",
            ctx["reference_idx"],
            feature_idx,
            accession,
        )
    return feature_idx


async def _seed_genome(ctx, *, source="genbank", source_id=None, n_features=1, in_reference=True):
    """Insert a genome + n_features features linked to it via feature_genome;
    optionally link the features into the reference. Returns (genome_idx,
    [feature_idx, ...])."""
    pool = ctx["pool"]
    source_id = source_id or f"GCF_{secrets.token_hex(4)}"
    genome_idx = await pool.fetchval(
        "INSERT INTO qiita.genome (source, source_id) VALUES ($1, $2) RETURNING genome_idx",
        source,
        source_id,
    )
    ctx["genome_idxs"].append(genome_idx)
    features = []
    for i in range(n_features):
        feature_idx = await _seed_feature(
            ctx, in_reference=in_reference, accession=f"{source_id}.{i}"
        )
        await pool.execute(
            "INSERT INTO qiita.feature_genome (feature_idx, genome_idx) VALUES ($1, $2)",
            feature_idx,
            genome_idx,
        )
        features.append(feature_idx)
    return genome_idx, features


async def test_resolve_direct_feature_block(ref):
    """A direct feature block resolves to exactly that feature."""
    pool = ref["pool"]
    feat = await _seed_feature(ref)
    other = await _seed_feature(ref)  # noqa: F841 — present but not blocked
    inserted = await add_exclusion(
        pool, feature_idx=feat, reason="misassembly", excluded_by_idx=ref["principal_idx"]
    )
    assert inserted is True
    assert await resolve_excluded_features(pool) == [feat]


async def test_resolve_genome_block_expands_to_all_features(ref):
    """A genome block resolves to EVERY feature of that genome (via
    feature_genome), even features not (yet) in any reference."""
    pool = ref["pool"]
    genome_idx, feats = await _seed_genome(ref, n_features=3)
    await add_exclusion(
        pool, genome_idx=genome_idx, reason="contamination", excluded_by_idx=ref["principal_idx"]
    )
    assert sorted(await resolve_excluded_features(pool)) == sorted(feats)


async def test_resolve_dedups_feature_blocked_directly_and_via_genome(ref):
    """A feature blocked BOTH directly and through its genome appears once
    (UNION dedups)."""
    pool = ref["pool"]
    genome_idx, feats = await _seed_genome(ref, n_features=1)
    await add_exclusion(
        pool, genome_idx=genome_idx, reason="contamination", excluded_by_idx=ref["principal_idx"]
    )
    await add_exclusion(
        pool, feature_idx=feats[0], reason="also bad", excluded_by_idx=ref["principal_idx"]
    )
    assert await resolve_excluded_features(pool) == [feats[0]]


async def test_add_is_idempotent(ref):
    """Re-blocking the same genome returns False and leaves one row."""
    pool = ref["pool"]
    genome_idx, _ = await _seed_genome(ref, n_features=1)
    assert (
        await add_exclusion(
            pool, genome_idx=genome_idx, reason="bad", excluded_by_idx=ref["principal_idx"]
        )
        is True
    )
    assert (
        await add_exclusion(
            pool, genome_idx=genome_idx, reason="bad again", excluded_by_idx=ref["principal_idx"]
        )
        is False
    )
    count = await pool.fetchval(
        "SELECT count(*) FROM qiita.reference_exclusion WHERE genome_idx = $1", genome_idx
    )
    assert count == 1


async def test_remove_unblocks_and_reports_rows(ref):
    """remove returns rows deleted; a second remove returns 0; resolve empties."""
    pool = ref["pool"]
    feat = await _seed_feature(ref)
    await add_exclusion(pool, feature_idx=feat, reason="bad", excluded_by_idx=ref["principal_idx"])
    assert await remove_exclusion(pool, feature_idx=feat) == 1
    assert await remove_exclusion(pool, feature_idx=feat) == 0
    assert await resolve_excluded_features(pool) == []


async def test_add_requires_exactly_one_target(ref):
    """The repo guard rejects both / neither target before hitting the DB."""
    pool = ref["pool"]
    genome_idx, feats = await _seed_genome(ref, n_features=1)
    with pytest.raises(ValueError, match="exactly one"):
        await add_exclusion(
            pool,
            genome_idx=genome_idx,
            feature_idx=feats[0],
            reason="x",
            excluded_by_idx=ref["principal_idx"],
        )
    with pytest.raises(ValueError, match="exactly one"):
        await add_exclusion(pool, reason="x", excluded_by_idx=ref["principal_idx"])


async def test_db_check_rejects_both_targets(ref):
    """The num_nonnulls CHECK is the backstop for a raw INSERT bypassing the repo."""
    pool = ref["pool"]
    genome_idx, feats = await _seed_genome(ref, n_features=1)
    with pytest.raises(asyncpg.CheckViolationError):
        await pool.execute(
            "INSERT INTO qiita.reference_exclusion"
            " (genome_idx, feature_idx, reason, excluded_by_idx) VALUES ($1, $2, 'x', $3)",
            genome_idx,
            feats[0],
            ref["principal_idx"],
        )


async def test_list_for_reference_genome_block_carries_provenance(ref):
    """A genome block lists its in-reference features with source/source_id and
    via_genome=True."""
    pool = ref["pool"]
    genome_idx, feats = await _seed_genome(
        ref, source="genbank", source_id="GCF_000123", n_features=2
    )
    await add_exclusion(
        pool, genome_idx=genome_idx, reason="contamination", excluded_by_idx=ref["principal_idx"]
    )
    rows = await list_for_reference(pool, ref["reference_idx"])
    assert {r["feature_idx"] for r in rows} == set(feats)
    for r in rows:
        assert r["genome_idx"] == genome_idx
        assert r["source"] == "genbank"
        assert r["source_id"] == "GCF_000123"
        assert r["reason"] == "contamination"
        assert r["via_genome"] is True


async def test_list_for_reference_feature_block_carries_accession(ref):
    """A direct feature block lists the reference's accession for it and
    via_genome=False."""
    pool = ref["pool"]
    feat = await _seed_feature(ref, accession="NZ_CP_ACC")
    await add_exclusion(
        pool, feature_idx=feat, reason="chimera", excluded_by_idx=ref["principal_idx"]
    )
    rows = await list_for_reference(pool, ref["reference_idx"])
    assert len(rows) == 1
    (r,) = rows
    assert r["feature_idx"] == feat
    assert r["accession"] == "NZ_CP_ACC"
    assert r["reason"] == "chimera"
    assert r["via_genome"] is False


async def test_deleting_a_blocked_feature_cascades_the_exclusion(ref):
    """ON DELETE CASCADE: hard-deleting a feature (as delete_reference_cascade's
    orphan-GC does) removes its block — a block on a feature that no longer
    exists is moot. Documented accepted tradeoff (see the migration comment)."""
    pool = ref["pool"]
    feat = await _seed_feature(ref, in_reference=False)
    await add_exclusion(pool, feature_idx=feat, reason="bad", excluded_by_idx=ref["principal_idx"])
    await pool.execute("DELETE FROM qiita.feature WHERE feature_idx = $1", feat)
    assert await resolve_excluded_features(pool) == []


async def test_deleting_a_blocked_genome_cascades_the_exclusion(ref):
    """ON DELETE CASCADE on the genome FK: orphan-GCing a blocked genome (after
    its feature_genome / feature rows go, mirroring delete_reference_cascade)
    removes its block."""
    pool = ref["pool"]
    genome_idx, feats = await _seed_genome(ref, n_features=1, in_reference=False)
    await add_exclusion(
        pool, genome_idx=genome_idx, reason="bad", excluded_by_idx=ref["principal_idx"]
    )
    # Reach the genome delete in orphan-GC order (feature_genome + feature first).
    await pool.execute("DELETE FROM qiita.feature_genome WHERE genome_idx = $1", genome_idx)
    await pool.execute("DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])", feats)
    await pool.execute("DELETE FROM qiita.genome WHERE genome_idx = $1", genome_idx)
    assert await resolve_excluded_features(pool) == []


async def test_list_for_reference_ignores_blocks_outside_the_reference(ref):
    """A blocked feature not linked into this reference is absent from the
    listing (the block is global, the listing is reference-scoped)."""
    pool = ref["pool"]
    outside = await _seed_feature(ref, in_reference=False)
    await add_exclusion(
        pool, feature_idx=outside, reason="bad", excluded_by_idx=ref["principal_idx"]
    )
    assert await list_for_reference(pool, ref["reference_idx"]) == []
    # ...but it IS in the global resolved set.
    assert await resolve_excluded_features(pool) == [outside]
