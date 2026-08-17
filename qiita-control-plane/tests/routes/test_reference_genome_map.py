"""Route tests for GET /reference/{reference_idx}/genome-map.

The feature_idx → genome lookup the client-side feature-table recipe joins its
alignment rows against. Unlike the genome-member read (one genome, many features)
this is the WHOLE reference in one body, and unlike every other capped list route
it REFUSES over its cap instead of truncating: a feature missing from the map is
silently dropped from the caller's roll-up, so a short map yields a wrong feature
table rather than a partial one.

The load-bearing test is `test_genome_map_agrees_with_export_member_genome` — the
map and the Parquet the compute job already consumes must not disagree about
which features have genomes.
"""

import pyarrow.parquet as pq
import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_REFERENCE_GENOME_MAP

from qiita_control_plane.testing.db_seeds import (
    cleanup_reference_graph,
    seed_bare_feature,
    seed_bare_reference,
    seed_feature_genome,
    seed_genome,
    seed_reference_membership,
)

pytestmark = pytest.mark.db


@pytest.fixture
async def client(postgres_pool, human_admin_session):
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {human_admin_session['token']}"},
    ) as ac:
        yield ac


async def test_genome_map_returns_the_four_columns(client, postgres_pool):
    """One entry per (feature, genome) pair, carrying the genome's provenance.

    `source` AND `source_id` both ship because `UNIQUE (source, source_id)` is
    composite — `source_id` alone is unique only within a source, so the relabel
    step's collision assertion needs the pair.
    """
    ref = await seed_bare_reference(postgres_pool, label="genome-map")
    feat = await seed_bare_feature(postgres_pool)
    genome, source_id = await seed_genome(postgres_pool)
    try:
        await seed_feature_genome(postgres_pool, feature_idx=feat, genome_idx=genome)
        await seed_reference_membership(postgres_pool, reference_idx=ref, feature_idx=feat)

        resp = await client.get(URL_REFERENCE_GENOME_MAP.format(reference_idx=ref))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["reference_idx"] == ref
        assert body["count"] == 1
        # No `truncated`: a 200 is always the whole map (over the cap is a 413), so
        # the field that every other capped read carries would never vary here.
        assert "truncated" not in body
        assert body["entries"] == [
            {
                "feature_idx": feat,
                "genome_idx": genome,
                "source": "refseq",
                "source_id": source_id,
            }
        ]
    finally:
        await cleanup_reference_graph(
            postgres_pool, reference_idx=ref, feature_idxs=[feat], genome_idxs=[genome]
        )


async def test_genome_map_fans_out_a_shared_plasmid(client, postgres_pool):
    """A feature under two genomes yields TWO entries — the map has more rows
    than distinct features. `feature_genome`'s standalone UNIQUE (feature_idx) was
    dropped to allow exactly this, and the recipe's alignment→map join at step 7
    depends on both rows being present (a read hitting the shared plasmid counts
    toward both genomes)."""
    ref = await seed_bare_reference(postgres_pool, label="genome-map")
    plasmid = await seed_bare_feature(postgres_pool)
    g1, _ = await seed_genome(postgres_pool)
    g2, _ = await seed_genome(postgres_pool)
    try:
        await seed_feature_genome(postgres_pool, feature_idx=plasmid, genome_idx=g1)
        await seed_feature_genome(postgres_pool, feature_idx=plasmid, genome_idx=g2)
        await seed_reference_membership(postgres_pool, reference_idx=ref, feature_idx=plasmid)

        resp = await client.get(URL_REFERENCE_GENOME_MAP.format(reference_idx=ref))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["count"] == 2
        assert [(e["feature_idx"], e["genome_idx"]) for e in body["entries"]] == [
            (plasmid, min(g1, g2)),
            (plasmid, max(g1, g2)),
        ]
    finally:
        await cleanup_reference_graph(
            postgres_pool, reference_idx=ref, feature_idxs=[plasmid], genome_idxs=[g1, g2]
        )


async def test_genome_map_drops_a_feature_with_no_genome(client, postgres_pool):
    """The join to feature_genome is INNER, matching export_member_genome: a
    feature with no genome (16S, deferred) cannot be rolled up and has no place in
    a map whose entire purpose is the roll-up."""
    ref = await seed_bare_reference(postgres_pool, label="genome-map")
    with_genome = await seed_bare_feature(postgres_pool)
    without_genome = await seed_bare_feature(postgres_pool)
    genome, _ = await seed_genome(postgres_pool)
    feats = [with_genome, without_genome]
    try:
        await seed_feature_genome(postgres_pool, feature_idx=with_genome, genome_idx=genome)
        await seed_reference_membership(postgres_pool, reference_idx=ref, feature_idx=with_genome)
        await seed_reference_membership(
            postgres_pool, reference_idx=ref, feature_idx=without_genome
        )

        resp = await client.get(URL_REFERENCE_GENOME_MAP.format(reference_idx=ref))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert [e["feature_idx"] for e in body["entries"]] == [with_genome]
    finally:
        await cleanup_reference_graph(
            postgres_pool, reference_idx=ref, feature_idxs=feats, genome_idxs=[genome]
        )


async def test_genome_map_of_a_reference_with_no_genomes_is_empty_not_404(client, postgres_pool):
    """A 16S reference legitimately has zero genome-bearing features, so [] is a
    meaningful clean answer — the exclusion-listing posture, not the
    genome-member one (which 404s on empty because there a zero result means one
    of its two identifiers is wrong)."""
    ref = await seed_bare_reference(postgres_pool, label="genome-map")
    feat = await seed_bare_feature(postgres_pool)
    try:
        await seed_reference_membership(postgres_pool, reference_idx=ref, feature_idx=feat)

        resp = await client.get(URL_REFERENCE_GENOME_MAP.format(reference_idx=ref))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body == {"reference_idx": ref, "entries": [], "count": 0}
    finally:
        await cleanup_reference_graph(postgres_pool, reference_idx=ref, feature_idxs=[feat])


async def test_genome_map_is_ordered_stably(client, postgres_pool):
    """(feature_idx, genome_idx)-ordered. Membership rows are inserted in
    DESCENDING feature order so heap/insertion order would fail this — the cap is
    only meaningful over a stable order, and a client diffing two pulls of the same
    reference should see no spurious churn."""
    ref = await seed_bare_reference(postgres_pool, label="genome-map")
    feats = [await seed_bare_feature(postgres_pool) for _ in range(3)]
    genome, _ = await seed_genome(postgres_pool)
    try:
        for feat in reversed(feats):
            await seed_feature_genome(postgres_pool, feature_idx=feat, genome_idx=genome)
            await seed_reference_membership(postgres_pool, reference_idx=ref, feature_idx=feat)

        resp = await client.get(URL_REFERENCE_GENOME_MAP.format(reference_idx=ref))
        assert resp.status_code == 200, resp.text
        returned = [e["feature_idx"] for e in resp.json()["entries"]]
        assert returned == sorted(feats)
    finally:
        await cleanup_reference_graph(
            postgres_pool, reference_idx=ref, feature_idxs=feats, genome_idxs=[genome]
        )


async def test_genome_map_refuses_a_reference_over_the_cap_413(client, postgres_pool, monkeypatch):
    """Over the cap is a 413, NOT a truncated 200. The detail names the real size
    (not just "more than the cap") so the caller can tell a 2x overshoot from a
    100x one."""
    monkeypatch.setattr("qiita_control_plane.routes.reference._GENOME_MAP_HARD_CAP", 2)
    ref = await seed_bare_reference(postgres_pool, label="genome-map")
    feats = [await seed_bare_feature(postgres_pool) for _ in range(3)]
    genome, _ = await seed_genome(postgres_pool)
    try:
        for feat in feats:
            await seed_feature_genome(postgres_pool, feature_idx=feat, genome_idx=genome)
            await seed_reference_membership(postgres_pool, reference_idx=ref, feature_idx=feat)

        resp = await client.get(URL_REFERENCE_GENOME_MAP.format(reference_idx=ref))
        assert resp.status_code == 413, resp.text
        detail = resp.json()["detail"]
        assert "3" in detail and "2" in detail
    finally:
        await cleanup_reference_graph(
            postgres_pool, reference_idx=ref, feature_idxs=feats, genome_idxs=[genome]
        )


async def test_genome_map_at_exactly_the_cap_is_a_200(client, postgres_pool, monkeypatch):
    """The boundary is inclusive: a map of exactly `cap` entries is served. Pins
    the over-fetch-by-one arithmetic — an off-by-one here would 413 a reference
    that fits."""
    monkeypatch.setattr("qiita_control_plane.routes.reference._GENOME_MAP_HARD_CAP", 2)
    ref = await seed_bare_reference(postgres_pool, label="genome-map")
    feats = [await seed_bare_feature(postgres_pool) for _ in range(2)]
    genome, _ = await seed_genome(postgres_pool)
    try:
        for feat in feats:
            await seed_feature_genome(postgres_pool, feature_idx=feat, genome_idx=genome)
            await seed_reference_membership(postgres_pool, reference_idx=ref, feature_idx=feat)

        resp = await client.get(URL_REFERENCE_GENOME_MAP.format(reference_idx=ref))
        assert resp.status_code == 200, resp.text
        assert resp.json()["count"] == 2
    finally:
        await cleanup_reference_graph(
            postgres_pool, reference_idx=ref, feature_idxs=feats, genome_idxs=[genome]
        )


async def test_genome_map_agrees_with_export_member_genome(client, postgres_pool, tmp_path):
    """ACCEPTANCE: the map's (feature_idx, genome_idx) pairs are exactly what
    export_member_genome writes for the same reference.

    Asserted against the real Parquet rather than hand-written expectations
    because the compute job already consumes that file — if the two ever disagree
    about which features have genomes, the client's roll-up silently diverges from
    the cluster's. Seeded with a shared plasmid and a genome-less feature so both
    the fan-out and the INNER-JOIN drop are inside the comparison.
    """
    from qiita_control_plane.actions.library import export_member_genome

    ref = await seed_bare_reference(postgres_pool, label="genome-map")
    chrom = await seed_bare_feature(postgres_pool)
    plasmid = await seed_bare_feature(postgres_pool)
    orphan = await seed_bare_feature(postgres_pool)  # no genome — dropped by both
    g1, _ = await seed_genome(postgres_pool)
    g2, _ = await seed_genome(postgres_pool)
    feats = [chrom, plasmid, orphan]
    try:
        await seed_feature_genome(postgres_pool, feature_idx=chrom, genome_idx=g1)
        await seed_feature_genome(postgres_pool, feature_idx=plasmid, genome_idx=g1)
        await seed_feature_genome(postgres_pool, feature_idx=plasmid, genome_idx=g2)
        for feat in feats:
            await seed_reference_membership(postgres_pool, reference_idx=ref, feature_idx=feat)

        out = tmp_path / "member_genome.parquet"
        await export_member_genome(postgres_pool, ref, out)
        table = pq.read_table(out)
        exported = set(
            zip(table.column("feature_idx").to_pylist(), table.column("genome_idx").to_pylist())
        )

        resp = await client.get(URL_REFERENCE_GENOME_MAP.format(reference_idx=ref))
        assert resp.status_code == 200, resp.text
        mapped = {(e["feature_idx"], e["genome_idx"]) for e in resp.json()["entries"]}

        assert mapped == exported
        assert (orphan, g1) not in mapped and (orphan, g2) not in mapped
    finally:
        await cleanup_reference_graph(
            postgres_pool, reference_idx=ref, feature_idxs=feats, genome_idxs=[g1, g2]
        )


async def test_genome_map_unknown_reference_is_404(client):
    resp = await client.get(URL_REFERENCE_GENOME_MAP.format(reference_idx=99_999_999))
    assert resp.status_code == 404, resp.text


async def test_genome_map_below_scope_is_403(make_pat_client):
    """A token without reference:read is refused before the handler runs — so this
    holds even for a reference that doesn't exist."""
    from qiita_common.auth_constants import Scope

    client = await make_pat_client(label="map-no-ref-read", scopes=[Scope.SELF_PROFILE])
    resp = await client.get(URL_REFERENCE_GENOME_MAP.format(reference_idx=1))
    assert resp.status_code == 403, resp.text


async def test_genome_map_requires_auth(postgres_pool):
    """Unlike GET /reference/{idx}, the map is not anonymous-OK: it is scoped to
    reference:read like every other reference read beyond the bare metadata."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get(URL_REFERENCE_GENOME_MAP.format(reference_idx=1))
    assert resp.status_code == 401, resp.text
