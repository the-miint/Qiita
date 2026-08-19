"""Route tests for GET /reference/{reference_idx}/genome/{genome_idx}/member.

Resolves a genome to its member features (feature_idx + the reference's accession)
within one reference — the inverse of export_member_genome, and the resolver the
genome-export CLI builds on. The many-to-many test (a plasmid shared by two
genomes) is the end-to-end proof of the feature_genome many-to-many fix: the
shared feature must appear in BOTH genomes' member lists.
"""

import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_REFERENCE_GENOME_MEMBER

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


async def test_genome_members_include_a_shared_plasmid(client, postgres_pool):
    """A genome's member list is its chromosome contig(s) PLUS a plasmid it shares
    with a second genome. The shared plasmid (one content-hash-global feature_idx
    under both genomes, post the feature_genome many-to-many fix) must appear in
    BOTH genomes' member lists — the end-to-end proof of that fix."""
    ref = await seed_bare_reference(postgres_pool, label="genome-member")
    chrom_a = await seed_bare_feature(postgres_pool)
    chrom_b = await seed_bare_feature(postgres_pool)
    plasmid = await seed_bare_feature(postgres_pool)  # shared by both genomes
    g1, _ = await seed_genome(postgres_pool)
    g2, _ = await seed_genome(postgres_pool)
    feats = [chrom_a, chrom_b, plasmid]
    try:
        await seed_feature_genome(postgres_pool, feature_idx=chrom_a, genome_idx=g1)
        await seed_feature_genome(postgres_pool, feature_idx=chrom_b, genome_idx=g2)
        await seed_feature_genome(postgres_pool, feature_idx=plasmid, genome_idx=g1)
        await seed_feature_genome(postgres_pool, feature_idx=plasmid, genome_idx=g2)
        # Insert membership rows in DESCENDING feature_idx order so g1's members
        # arrive as [plasmid, chrom_a] — divergent from feature_idx-sorted order,
        # making the ORDER BY assertion below load-bearing (a dropped ORDER BY
        # would return heap/insertion order and fail).
        await seed_reference_membership(
            postgres_pool, reference_idx=ref, feature_idx=plasmid, accession="NZ_PLASMID.1"
        )
        await seed_reference_membership(
            postgres_pool, reference_idx=ref, feature_idx=chrom_b, accession="NZ_CHROM_B.1"
        )
        await seed_reference_membership(
            postgres_pool, reference_idx=ref, feature_idx=chrom_a, accession="NZ_CHROM_A.1"
        )

        r1 = await client.get(URL_REFERENCE_GENOME_MEMBER.format(reference_idx=ref, genome_idx=g1))
        assert r1.status_code == 200, r1.text
        members1 = r1.json()
        assert {m["feature_idx"] for m in members1} == {chrom_a, plasmid}
        by_feat1 = {m["feature_idx"]: m["accession"] for m in members1}
        assert by_feat1[chrom_a] == "NZ_CHROM_A.1"
        assert by_feat1[plasmid] == "NZ_PLASMID.1"

        r2 = await client.get(URL_REFERENCE_GENOME_MEMBER.format(reference_idx=ref, genome_idx=g2))
        assert r2.status_code == 200, r2.text
        members2 = r2.json()
        # The shared plasmid appears in g2's list too — the many-to-many payoff.
        assert {m["feature_idx"] for m in members2} == {chrom_b, plasmid}
        # feature_idx-ordered (mirrors export_member_genome / write_shard_assignment).
        assert [m["feature_idx"] for m in members1] == sorted([chrom_a, plasmid])
    finally:
        await cleanup_reference_graph(
            postgres_pool, reference_idx=ref, feature_idxs=feats, genome_idxs=[g1, g2]
        )


async def test_genome_members_null_accession_round_trips(client, postgres_pool):
    """A member with no FASTA-header accession (a non-FASTA ingest path, or a row
    predating the accession column) round-trips as accession: null, 200 — the
    model field is `str | None` and the column is nullable."""
    ref = await seed_bare_reference(postgres_pool, label="genome-member")
    feat = await seed_bare_feature(postgres_pool)
    g, _ = await seed_genome(postgres_pool)
    try:
        await seed_feature_genome(postgres_pool, feature_idx=feat, genome_idx=g)
        await seed_reference_membership(
            postgres_pool, reference_idx=ref, feature_idx=feat, accession=None
        )  # NULL accession
        resp = await client.get(URL_REFERENCE_GENOME_MEMBER.format(reference_idx=ref, genome_idx=g))
        assert resp.status_code == 200, resp.text
        assert resp.json() == [{"feature_idx": feat, "accession": None}]
    finally:
        await cleanup_reference_graph(
            postgres_pool, reference_idx=ref, feature_idxs=[feat], genome_idxs=[g]
        )


async def test_genome_members_unknown_reference_is_404(client):
    resp = await client.get(
        URL_REFERENCE_GENOME_MEMBER.format(reference_idx=99_999_999, genome_idx=1)
    )
    assert resp.status_code == 404, resp.text


async def test_genome_members_genome_not_in_reference_is_404(client, postgres_pool):
    """A genome with no members in this reference (unknown genome, or one that
    belongs only to a different reference) is a fail-loud 404 — an empty export is
    a caller error, unlike the exclusion listing where [] is a meaningful state."""
    ref = await seed_bare_reference(postgres_pool, label="genome-member")
    try:
        resp = await client.get(
            URL_REFERENCE_GENOME_MEMBER.format(reference_idx=ref, genome_idx=88_888_888)
        )
        assert resp.status_code == 404, resp.text
    finally:
        await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", ref)


async def test_genome_members_below_scope_is_403(make_pat_client):
    """A principal WHOSE TOKEN lacks reference:read is refused 403 — the route's
    require_scope guard runs before the handler, so this holds even for a
    reference/genome that doesn't exist. A token's granted scopes are
    authoritative; the role ceiling only bounds what may be minted, it does not
    grant a scope the token was issued without."""
    from qiita_common.auth_constants import Scope

    client = await make_pat_client(label="member-no-ref-read", scopes=[Scope.SELF_PROFILE])
    resp = await client.get(URL_REFERENCE_GENOME_MEMBER.format(reference_idx=1, genome_idx=1))
    assert resp.status_code == 403, resp.text


async def test_genome_members_requires_auth(postgres_pool):
    """An unauthenticated request is rejected 401, proving the scope dependency is
    wired. (A principal whose token lacks reference:read gets 403 —
    test_genome_members_below_scope_is_403.)"""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get(URL_REFERENCE_GENOME_MEMBER.format(reference_idx=1, genome_idx=1))
    assert resp.status_code == 401, resp.text
