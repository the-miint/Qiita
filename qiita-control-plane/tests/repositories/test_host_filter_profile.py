"""Repository-layer tests for qiita.host_filter_profile.

Exercises get / insert / list plus the two constraints the resolver leans on:
UNIQUE (host_term_idx, platform) — which is what lets the lookup fetchrow a
single unambiguous row — and ON DELETE RESTRICT on the reference FKs, which
stops a referenced host build from being deleted out from under a live profile.

Seeds its own principal, references, and terminology term so cleanup runs in
FK-reverse order and the file can run alongside the rest of the suite against
the shared postgres_pool fixture.
"""

import secrets

import asyncpg
import pytest
import pytest_asyncio
from qiita_common.models import Platform

from qiita_control_plane.repositories.host_filter_profile import (
    get_host_filter_profile,
    insert_host_filter_profile,
    list_host_filter_profiles,
)
from qiita_control_plane.testing.db_seeds import (
    NCBI_TAXONOMY_HUMAN_TERM_ID,
    fetch_ncbi_taxonomy_term,
    seed_host_reference,
    seed_user_principal,
)

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def ctx(postgres_pool):
    """Seed a principal, two host references, and grab the seeded human taxon.

    Human (9606) is seeded by the water-metadata terminology migration, so the
    tests read it rather than minting their own — that keeps them honest about
    the term the live seed will actually point at.
    """
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="hfp", suffix=suffix)
    rype_idx = await seed_host_reference(
        postgres_pool, name=f"hfp-rype-{suffix}", created_by_idx=principal_idx
    )
    minimap2_idx = await seed_host_reference(
        postgres_pool, name=f"hfp-mm2-{suffix}", created_by_idx=principal_idx
    )
    human_term = await fetch_ncbi_taxonomy_term(postgres_pool, NCBI_TAXONOMY_HUMAN_TERM_ID)
    human_term_idx = human_term["idx"] if human_term else None
    assert human_term_idx is not None, "NCBI 9606 should be seeded by migration"

    yield {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "rype_idx": rype_idx,
        "minimap2_idx": minimap2_idx,
        "human_term_idx": human_term_idx,
    }

    # FK-reverse: profiles reference both the references and the principal.
    await postgres_pool.execute(
        "DELETE FROM qiita.host_filter_profile WHERE created_by_idx = $1", principal_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = ANY($1::bigint[])",
        [rype_idx, minimap2_idx],
    )
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


# ---------------------------------------------------------------------------
# insert + get round-trip
# ---------------------------------------------------------------------------


async def test_insert_and_get_round_trip_with_minimap2(ctx):
    """A short-read profile carries both stages and reads back intact."""
    async with ctx["pool"].acquire() as conn:
        inserted = await insert_host_filter_profile(
            conn,
            host_term_idx=ctx["human_term_idx"],
            platform=Platform.ILLUMINA,
            rype_reference_idx=ctx["rype_idx"],
            minimap2_reference_idx=ctx["minimap2_idx"],
            principal_idx=ctx["principal_idx"],
        )

    fetched = await get_host_filter_profile(
        ctx["pool"], host_term_idx=ctx["human_term_idx"], platform=Platform.ILLUMINA
    )
    assert fetched == inserted
    assert fetched.rype_reference_idx == ctx["rype_idx"]
    assert fetched.minimap2_reference_idx == ctx["minimap2_idx"]


async def test_insert_and_get_round_trip_without_minimap2(ctx):
    """A long-read profile omits the minimap2 stage; the column stays NULL and
    surfaces as None on the model rather than as a sentinel."""
    async with ctx["pool"].acquire() as conn:
        await insert_host_filter_profile(
            conn,
            host_term_idx=ctx["human_term_idx"],
            platform=Platform.PACBIO_SMRT,
            rype_reference_idx=ctx["rype_idx"],
            principal_idx=ctx["principal_idx"],
        )

    fetched = await get_host_filter_profile(
        ctx["pool"], host_term_idx=ctx["human_term_idx"], platform=Platform.PACBIO_SMRT
    )
    assert fetched.rype_reference_idx == ctx["rype_idx"]
    assert fetched.minimap2_reference_idx is None


# ---------------------------------------------------------------------------
# constraints
# ---------------------------------------------------------------------------


async def test_duplicate_host_platform_pair_is_rejected(ctx):
    """UNIQUE (host_term_idx, platform). This is the constraint that makes the
    resolver's single-row lookup unambiguous — a host-DB rebuild UPDATEs the
    existing row rather than inserting a competing one."""
    async with ctx["pool"].acquire() as conn:
        await insert_host_filter_profile(
            conn,
            host_term_idx=ctx["human_term_idx"],
            platform=Platform.ILLUMINA,
            rype_reference_idx=ctx["rype_idx"],
            principal_idx=ctx["principal_idx"],
        )

    with pytest.raises(asyncpg.UniqueViolationError):
        async with ctx["pool"].acquire() as conn:
            await insert_host_filter_profile(
                conn,
                host_term_idx=ctx["human_term_idx"],
                platform=Platform.ILLUMINA,
                rype_reference_idx=ctx["minimap2_idx"],
                principal_idx=ctx["principal_idx"],
            )


async def test_same_host_on_a_different_platform_is_allowed(ctx):
    """The same host on two platforms is two profiles, not a duplicate — the
    stages are chosen per platform, which is why platform is in the key.

    The assertion is that BOTH rows exist and are distinct: the previous test
    shows the same pair collides, so this one has to show the platform is what
    breaks the tie, not merely that the second insert didn't raise.
    """
    async with ctx["pool"].acquire() as conn:
        await insert_host_filter_profile(
            conn,
            host_term_idx=ctx["human_term_idx"],
            platform=Platform.ILLUMINA,
            rype_reference_idx=ctx["rype_idx"],
            minimap2_reference_idx=ctx["minimap2_idx"],
            principal_idx=ctx["principal_idx"],
        )
        await insert_host_filter_profile(
            conn,
            host_term_idx=ctx["human_term_idx"],
            platform=Platform.PACBIO_SMRT,
            rype_reference_idx=ctx["rype_idx"],
            principal_idx=ctx["principal_idx"],
        )

    # Scope to this fixture's own rows — the table is shared with other tests.
    ours = [
        p
        for p in await list_host_filter_profiles(ctx["pool"])
        if p.rype_reference_idx == ctx["rype_idx"]
    ]
    assert sorted(p.platform for p in ours) == ["illumina", "pacbio_smrt"]
    assert len({p.idx for p in ours}) == 2


async def test_referenced_reference_cannot_be_deleted(ctx):
    """ON DELETE RESTRICT: a host build cannot be deleted while a profile points
    at it. Otherwise a submit would resolve to a reference that no longer exists."""
    async with ctx["pool"].acquire() as conn:
        await insert_host_filter_profile(
            conn,
            host_term_idx=ctx["human_term_idx"],
            platform=Platform.ILLUMINA,
            rype_reference_idx=ctx["rype_idx"],
            principal_idx=ctx["principal_idx"],
        )

    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await ctx["pool"].execute(
            "DELETE FROM qiita.reference WHERE reference_idx = $1", ctx["rype_idx"]
        )


# ---------------------------------------------------------------------------
# get / list
# ---------------------------------------------------------------------------


async def test_get_returns_none_for_unknown_pair(ctx):
    """A host with no profile on this platform is None, not an error — the
    resolver decides whether that is fatal."""
    assert (
        await get_host_filter_profile(
            ctx["pool"], host_term_idx=ctx["human_term_idx"], platform=Platform.OXFORD_NANOPORE
        )
        is None
    )


async def test_list_filters_by_platform(ctx):
    """The platform filter narrows the set; omitting it returns everything."""
    async with ctx["pool"].acquire() as conn:
        await insert_host_filter_profile(
            conn,
            host_term_idx=ctx["human_term_idx"],
            platform=Platform.ILLUMINA,
            rype_reference_idx=ctx["rype_idx"],
            principal_idx=ctx["principal_idx"],
        )
        await insert_host_filter_profile(
            conn,
            host_term_idx=ctx["human_term_idx"],
            platform=Platform.PACBIO_SMRT,
            rype_reference_idx=ctx["rype_idx"],
            principal_idx=ctx["principal_idx"],
        )

    # Scope the assertions to this fixture's own rows: the table is shared and
    # a parallel test may have seeded its own profiles.
    ours = [
        p
        for p in await list_host_filter_profiles(ctx["pool"], platform=Platform.ILLUMINA)
        if p.rype_reference_idx == ctx["rype_idx"]
    ]
    assert [p.platform for p in ours] == ["illumina"]

    both = [
        p
        for p in await list_host_filter_profiles(ctx["pool"])
        if p.rype_reference_idx == ctx["rype_idx"]
    ]
    assert sorted(p.platform for p in both) == ["illumina", "pacbio_smrt"]
