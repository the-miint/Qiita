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


async def test_remove_is_a_soft_delete_and_reports_rows(ref):
    """remove SOFT-deletes: the row is kept and stamped unblocked_at/unblocked_by;
    it reports 1 affected, a second remove reports 0 (idempotent), resolve empties,
    and the curatorial record (who/when) survives."""
    pool = ref["pool"]
    actor = ref["principal_idx"]
    feat = await _seed_feature(ref)
    await add_exclusion(pool, feature_idx=feat, reason="bad", excluded_by_idx=actor)
    assert await remove_exclusion(pool, feature_idx=feat, unblocked_by_idx=actor) == 1
    assert await remove_exclusion(pool, feature_idx=feat, unblocked_by_idx=actor) == 0
    assert await resolve_excluded_features(pool) == []
    # Soft delete: the row persists with the unblock stamped, not a hard DELETE.
    row = await pool.fetchrow(
        "SELECT unblocked_at, unblocked_by_idx FROM qiita.reference_exclusion"
        " WHERE feature_idx = $1",
        feat,
    )
    assert row is not None
    assert row["unblocked_at"] is not None
    assert row["unblocked_by_idx"] == ref["principal_idx"]


async def test_reblock_after_unblock_creates_a_new_active_row(ref):
    """Re-blocking a soft-deleted entity inserts a FRESH active row (changed=True)
    and re-arms resolution — the active partial unique excludes the historical
    unblocked row, so both coexist (a durable block/unblock/re-block trail)."""
    pool = ref["pool"]
    feat = await _seed_feature(ref)
    assert await add_exclusion(
        pool, feature_idx=feat, reason="first", excluded_by_idx=ref["principal_idx"]
    )
    await remove_exclusion(pool, feature_idx=feat, unblocked_by_idx=ref["principal_idx"])
    # Re-block: not a conflict (the prior row is unblocked), so a new active row.
    assert await add_exclusion(
        pool, feature_idx=feat, reason="again", excluded_by_idx=ref["principal_idx"]
    )
    assert await resolve_excluded_features(pool) == [feat]
    total = await pool.fetchval(
        "SELECT count(*) FROM qiita.reference_exclusion WHERE feature_idx = $1", feat
    )
    active = await pool.fetchval(
        "SELECT count(*) FROM qiita.reference_exclusion"
        " WHERE feature_idx = $1 AND unblocked_at IS NULL",
        feat,
    )
    assert (total, active) == (2, 1)


async def test_db_check_rejects_out_of_bounds_reason(ref):
    """The reason length CHECK is the backstop for a raw INSERT (empty or
    over-long) bypassing the Pydantic bound."""
    pool = ref["pool"]
    feat = await _seed_feature(ref)
    with pytest.raises(asyncpg.CheckViolationError):
        await pool.execute(
            "INSERT INTO qiita.reference_exclusion (feature_idx, reason, excluded_by_idx)"
            " VALUES ($1, $2, $3)",
            feat,
            "x" * 2001,
            ref["principal_idx"],
        )


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


async def _seed_shared_feature(ctx, *, accession, genome_idxs):
    """Insert a feature into the reference and link it to several genomes via
    feature_genome (a shared plasmid). Links are inserted in the given order so a
    test can control which genome's fan-out is scanned first. Returns feature_idx."""
    pool = ctx["pool"]
    feature_idx = await _seed_feature(ctx, accession=accession)
    for genome_idx in genome_idxs:
        await pool.execute(
            "INSERT INTO qiita.feature_genome (feature_idx, genome_idx) VALUES ($1, $2)",
            feature_idx,
            genome_idx,
        )
    return feature_idx


async def test_list_multi_genome_direct_block_reports_the_blocked_genome(ref):
    """A feature shared by two genomes, blocked DIRECTLY, where one of its genomes
    is ALSO blocked: the listing must report the BLOCKED genome's provenance, not
    an arbitrary/unblocked one, with direct precedence for the flag. Guards the
    many-to-many regression — with feature_genome fanning a shared feature to one
    candidate row per genome, the DISTINCT ON picker needs a tiebreak that prefers
    a genome that is itself blocked. The shared feature's link to the UNBLOCKED
    genome is inserted first so a picker with no such tiebreak would surface it."""
    pool = ref["pool"]
    actor = ref["principal_idx"]
    g_blocked, _ = await _seed_genome(ref, source_id="GCF_BLOCKED", n_features=1)
    g_unblocked, _ = await _seed_genome(ref, source_id="GCF_UNBLOCKED", n_features=1)
    plasmid = await _seed_shared_feature(
        ref, accession="shared_plasmid", genome_idxs=[g_unblocked, g_blocked]
    )
    await add_exclusion(pool, feature_idx=plasmid, reason="chimeric plasmid", excluded_by_idx=actor)
    await add_exclusion(pool, genome_idx=g_blocked, reason="contaminated", excluded_by_idx=actor)

    rows = await list_for_reference(pool, ref["reference_idx"])
    (r,) = [row for row in rows if row["feature_idx"] == plasmid]
    assert r["direct_block"] is True
    assert r["via_genome"] is False
    assert r["genome_idx"] == g_blocked


async def test_list_multi_genome_via_block_reports_lowest_blocked_genome(ref):
    """A feature shared by two genomes, both blocked via their genome (no direct
    block): reported once, via_genome=True, deterministically naming the lowest
    blocked genome_idx. The higher genome's link is inserted first so a scan-order
    pick would surface it."""
    pool = ref["pool"]
    actor = ref["principal_idx"]
    g_low, _ = await _seed_genome(ref, source_id="GCF_LOW", n_features=1)
    g_high, _ = await _seed_genome(ref, source_id="GCF_HIGH", n_features=1)
    assert g_low < g_high
    plasmid = await _seed_shared_feature(ref, accession="shared", genome_idxs=[g_high, g_low])
    await add_exclusion(pool, genome_idx=g_low, reason="a", excluded_by_idx=actor)
    await add_exclusion(pool, genome_idx=g_high, reason="b", excluded_by_idx=actor)

    rows = await list_for_reference(pool, ref["reference_idx"])
    (r,) = [row for row in rows if row["feature_idx"] == plasmid]
    assert r["via_genome"] is True
    assert r["direct_block"] is False
    assert r["genome_idx"] == g_low


async def test_list_multi_genome_direct_block_no_genome_blocked_is_lowest(ref):
    """A feature shared by two genomes, blocked DIRECTLY, with NEITHER genome
    blocked: reported once, direct_block=True, deterministically naming the lowest
    genome_idx (no blocked genome to prefer). The higher genome's link is inserted
    first so a scan-order pick would surface it."""
    pool = ref["pool"]
    actor = ref["principal_idx"]
    g_low, _ = await _seed_genome(ref, source_id="GCF_LO", n_features=1)
    g_high, _ = await _seed_genome(ref, source_id="GCF_HI", n_features=1)
    assert g_low < g_high
    plasmid = await _seed_shared_feature(ref, accession="shared", genome_idxs=[g_high, g_low])
    await add_exclusion(pool, feature_idx=plasmid, reason="chimera", excluded_by_idx=actor)

    rows = await list_for_reference(pool, ref["reference_idx"])
    (r,) = [row for row in rows if row["feature_idx"] == plasmid]
    assert r["direct_block"] is True
    assert r["via_genome"] is False
    assert r["genome_idx"] == g_low


async def test_direct_feature_block_survives_feature_deletion(ref):
    """A block SURVIVES hard-deletion of its feature (targets are NOT foreign
    keys). delete_reference_cascade's orphan-GC can drop the feature row, but the
    block persists — and since feature_idx is content-hash-stable, a direct
    feature block still resolves immediately (ready to re-mask the moment the same
    bytes are re-ingested). "I blocked this, it stays blocked."""
    pool = ref["pool"]
    feat = await _seed_feature(ref, in_reference=False)
    await add_exclusion(pool, feature_idx=feat, reason="bad", excluded_by_idx=ref["principal_idx"])
    await pool.execute("DELETE FROM qiita.feature WHERE feature_idx = $1", feat)
    # Row survives (no FK cascade) and a direct feature block still resolves.
    survived = await pool.fetchval(
        "SELECT count(*) FROM qiita.reference_exclusion WHERE feature_idx = $1", feat
    )
    assert survived == 1
    assert await resolve_excluded_features(pool) == [feat]


async def test_genome_block_survives_genome_deletion_and_reattaches(ref):
    """A genome block SURVIVES orphan-GC of the genome. While the genome/features
    are gone the block resolves to nothing (no feature_genome to expand), but the
    row persists; re-ingesting the same (source, source_id) genome reuses the
    genome_idx and its features re-appear in feature_genome, so the surviving
    block re-arms without any operator action."""
    pool = ref["pool"]
    genome_idx, feats = await _seed_genome(ref, n_features=1, in_reference=False)
    await add_exclusion(
        pool, genome_idx=genome_idx, reason="bad", excluded_by_idx=ref["principal_idx"]
    )
    # Orphan-GC order (feature_genome + feature first, then genome).
    await pool.execute("DELETE FROM qiita.feature_genome WHERE genome_idx = $1", genome_idx)
    await pool.execute("DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])", feats)
    await pool.execute("DELETE FROM qiita.genome WHERE genome_idx = $1", genome_idx)
    # Block row survives; dormant until re-ingest (no feature_genome to expand).
    survived = await pool.fetchval(
        "SELECT count(*) FROM qiita.reference_exclusion WHERE genome_idx = $1", genome_idx
    )
    assert survived == 1
    assert await resolve_excluded_features(pool) == []
    # Re-ingest: same genome_idx reused, feature re-linked -> the block re-arms.
    refeat = await pool.fetchval(
        "INSERT INTO qiita.feature (sequence_hash) VALUES ($1) RETURNING feature_idx", uuid.uuid4()
    )
    ref["feature_idxs"].append(refeat)
    await pool.execute(
        "INSERT INTO qiita.genome (genome_idx, source, source_id)"
        " OVERRIDING SYSTEM VALUE VALUES ($1, 'genbank', $2)",
        genome_idx,
        f"reattach_{secrets.token_hex(4)}",
    )
    await pool.execute(
        "INSERT INTO qiita.feature_genome (feature_idx, genome_idx) VALUES ($1, $2)",
        refeat,
        genome_idx,
    )
    assert await resolve_excluded_features(pool) == [refeat]


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
