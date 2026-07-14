"""Route tests for GET /host-filter-profile — the catalog of what we can deplete.

The catalog is the menu an operator reads before overriding a sample's resolved
host filtering: you cannot sensibly force a sample onto a profile you cannot see.
So the two things worth pinning are that the platform filter actually narrows
(a long-read profile must not show up as an option for an Illumina run) and that
the gate matches the audience that submits masks.
"""

import secrets

import pytest
import pytest_asyncio
from qiita_common.api_paths import URL_HOST_FILTER_PROFILE_LIST
from qiita_common.models import Platform

from qiita_control_plane.testing.db_seeds import (
    NCBI_TAXONOMY_HUMAN_TERM_ID,
    fetch_ncbi_taxonomy_term,
    seed_host_filter_profile,
    seed_host_reference,
)

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def ctx(role_keyed_clients):
    """Seed a host reference pair plus two profiles for human — one illumina
    (both stages) and one pacbio_smrt (rype only). Between them the platform
    filter has something to actually discriminate.
    """
    pool = role_keyed_clients["pool"]
    principal_idx = role_keyed_clients["wet_session"]["principal_idx"]
    suffix = secrets.token_hex(4)

    rype_idx = await seed_host_reference(
        pool, name=f"cat-rype-{suffix}", created_by_idx=principal_idx
    )
    minimap2_idx = await seed_host_reference(
        pool, name=f"cat-mm2-{suffix}", created_by_idx=principal_idx
    )
    human_term = await fetch_ncbi_taxonomy_term(pool, NCBI_TAXONOMY_HUMAN_TERM_ID)

    illumina_idx = await seed_host_filter_profile(
        pool,
        host_term_idx=human_term["idx"],
        platform=Platform.ILLUMINA,
        rype_reference_idx=rype_idx,
        minimap2_reference_idx=minimap2_idx,
        created_by_idx=principal_idx,
    )
    pacbio_idx = await seed_host_filter_profile(
        pool,
        host_term_idx=human_term["idx"],
        platform=Platform.PACBIO_SMRT,
        rype_reference_idx=rype_idx,
        created_by_idx=principal_idx,
    )

    yield {
        **role_keyed_clients,
        "human_term_idx": human_term["idx"],
        "rype_idx": rype_idx,
        "minimap2_idx": minimap2_idx,
        "illumina_idx": illumina_idx,
        "pacbio_idx": pacbio_idx,
    }

    await pool.execute(
        "DELETE FROM qiita.host_filter_profile WHERE idx = ANY($1::bigint[])",
        [illumina_idx, pacbio_idx],
    )
    await pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = ANY($1::bigint[])",
        [rype_idx, minimap2_idx],
    )


def _ours(body, rype_idx):
    """Scope a response to this test's own profiles — the table is shared."""
    return [p for p in body if p["rype_reference_idx"] == rype_idx]


async def test_platform_filter_narrows_to_that_platform(ctx):
    """?platform=illumina must not surface the pacbio profile. Getting this wrong
    would offer an operator a rype-only long-read build as an option for a
    short-read run."""
    resp = await ctx["wet"].get(
        URL_HOST_FILTER_PROFILE_LIST, params={"platform": Platform.ILLUMINA.value}
    )
    assert resp.status_code == 200, resp.text

    ours = _ours(resp.json(), ctx["rype_idx"])
    assert [p["idx"] for p in ours] == [ctx["illumina_idx"]]
    assert ours[0]["platform"] == Platform.ILLUMINA
    assert ours[0]["minimap2_reference_idx"] == ctx["minimap2_idx"]


async def test_unfiltered_list_returns_every_platform(ctx):
    """No platform param -> both profiles, and the long-read one reports no
    minimap2 stage (None, not a sentinel)."""
    resp = await ctx["wet"].get(URL_HOST_FILTER_PROFILE_LIST)
    assert resp.status_code == 200, resp.text

    ours = {p["platform"]: p for p in _ours(resp.json(), ctx["rype_idx"])}
    assert sorted(ours) == [Platform.ILLUMINA, Platform.PACBIO_SMRT]
    assert ours[Platform.PACBIO_SMRT]["minimap2_reference_idx"] is None
    assert ours[Platform.ILLUMINA]["minimap2_reference_idx"] == ctx["minimap2_idx"]


async def test_platform_with_no_profiles_returns_empty(ctx):
    """A platform nobody has a build for is an empty list, not a 404 — "nothing
    available here" is a legitimate answer for a catalog."""
    resp = await ctx["wet"].get(
        URL_HOST_FILTER_PROFILE_LIST, params={"platform": Platform.OXFORD_NANOPORE.value}
    )
    assert resp.status_code == 200, resp.text
    assert _ours(resp.json(), ctx["rype_idx"]) == []


async def test_unknown_platform_is_422(ctx):
    """The platform param is the Platform enum, so a bogus value is rejected at
    the boundary rather than reaching Postgres as a bad ENUM cast."""
    resp = await ctx["wet"].get(URL_HOST_FILTER_PROFILE_LIST, params={"platform": "nanopore"})
    assert resp.status_code == 422, resp.text


async def test_regular_user_is_403(ctx):
    """Gated to the audience that submits read masks (wet_lab_admin) — stricter
    than the reference reads whose rows this route points at, which are
    anonymous-OK. See the route docstring for why that asymmetry is deliberate."""
    resp = await ctx["user"].get(URL_HOST_FILTER_PROFILE_LIST)
    assert resp.status_code == 403, resp.text
