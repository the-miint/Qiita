"""Integration tests for GET /api/v1/prep-sample/{idx}/study/list.

Covers the happy path (active studies returned ascending by idx, each with
its accessions), accession surfacing, retired-link exclusion, the empty case
(prep_sample with no active study links), the 404 on an unknown
prep_sample_idx, and the auth gates (wet_lab_admin role, prep_sample:read
scope, and the anonymous 401).
"""

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_PREP_SAMPLE_STUDY_LIST

from qiita_control_plane.main import app
from qiita_control_plane.testing.db_seeds import (
    retire_prep_sample_to_study_link,
    seed_biosample,
    seed_biosample_to_study_link,
    seed_sequenced_prep_sample,
)

from .conftest import _seed_study, delete_idxs

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# FK-reverse cleanup
# ---------------------------------------------------------------------------


async def _cleanup_tracked(pool, created: dict) -> None:
    """Drop tracked rows in FK-reverse order (ON DELETE RESTRICT throughout):
    prep_sample_to_study, prep_sample, biosample_to_study, biosample, study."""
    for ps, st in created["prep_sample_to_study"]:
        await pool.execute(
            "DELETE FROM qiita.prep_sample_to_study WHERE prep_sample_idx = $1 AND study_idx = $2",
            ps,
            st,
        )
    await delete_idxs(pool, "prep_sample", created["prep_sample"])
    for bs, st in created["biosample_to_study"]:
        await pool.execute(
            "DELETE FROM qiita.biosample_to_study WHERE biosample_idx = $1 AND study_idx = $2",
            bs,
            st,
        )
    await delete_idxs(pool, "biosample", created["biosample"])
    await delete_idxs(pool, "study", created["study"])


@pytest_asyncio.fixture
async def ctx(role_keyed_clients):
    """Per-test fixture: route-keyed clients plus a `created` tracker for
    FK-reverse teardown over every table the seeds touch."""
    created: dict = {
        "prep_sample_to_study": [],
        "prep_sample": [],
        "biosample_to_study": [],
        "biosample": [],
        "study": [],
    }
    yield {**role_keyed_clients, "created": created}
    await _cleanup_tracked(role_keyed_clients["pool"], created)


# ---------------------------------------------------------------------------
# Seeding helpers
# ---------------------------------------------------------------------------


async def _seed_prep_sample_linked_to_studies(ctx, *, owner_idx: int, study_idxs: list[int]) -> int:
    """Seed one biosample + sequenced prep_sample, link both the biosample and
    the prep_sample (non-retired) to each study, and track every row. The
    biosample-to-study link must exist first — a prep_sample_to_study link is
    rejected unless its biosample is already linked to the study. Returns the
    prep_sample idx."""
    biosample_idx = await seed_biosample(ctx["pool"], owner_idx=owner_idx, created_by_idx=owner_idx)
    ctx["created"]["biosample"].append(biosample_idx)
    prep_sample_idx = await seed_sequenced_prep_sample(
        ctx["pool"], biosample_idx=biosample_idx, owner_idx=owner_idx
    )
    ctx["created"]["prep_sample"].append(prep_sample_idx)
    for study_idx in study_idxs:
        await seed_biosample_to_study_link(
            ctx["pool"],
            biosample_idx=biosample_idx,
            study_idx=study_idx,
            created_by_idx=owner_idx,
        )
        ctx["created"]["biosample_to_study"].append((biosample_idx, study_idx))
        await ctx["pool"].execute(
            "INSERT INTO qiita.prep_sample_to_study (prep_sample_idx, study_idx, created_by_idx)"
            " VALUES ($1, $2, $3)",
            prep_sample_idx,
            study_idx,
            owner_idx,
        )
        ctx["created"]["prep_sample_to_study"].append((prep_sample_idx, study_idx))
    return prep_sample_idx


# ===========================================================================
# GET /api/v1/prep-sample/{idx}/study/list
# ===========================================================================


def _study_item(study_idx: int, *, bioproject=None, ena_study=None) -> dict:
    """The StudyListItem dict the route surfaces for one linked study."""
    return {
        "study_idx": study_idx,
        "bioproject_accession": bioproject,
        "ena_study_accession": ena_study,
    }


async def test_list_studies_for_prep_sample_returns_sorted_studies(ctx):
    """Tests the case where a prep_sample links to two studies: the route
    returns both studies ascending by idx in the StudyListResponse envelope,
    each with its (here null) accession fields."""
    owner_idx = ctx["wet_session"]["principal_idx"]
    study_a = await _seed_study(ctx, owner_idx=owner_idx, suffix="A")
    study_b = await _seed_study(ctx, owner_idx=owner_idx, suffix="B")
    prep_sample_idx = await _seed_prep_sample_linked_to_studies(
        ctx, owner_idx=owner_idx, study_idxs=[study_a, study_b]
    )

    resp = await ctx["wet"].get(URL_PREP_SAMPLE_STUDY_LIST.format(prep_sample_idx=prep_sample_idx))
    assert resp.status_code == 200, resp.text
    rj = resp.json()
    expected = {
        "studies": [_study_item(s) for s in sorted([study_a, study_b])],
        "count": 2,
        "truncated": False,
        "caller_system_role": "wet_lab_admin",
    }
    assert rj == expected


async def test_list_studies_for_prep_sample_surfaces_accessions(ctx):
    """Tests the case where the linked study carries accessions: the route
    surfaces its BioProject and ENA study accessions on the item."""
    owner_idx = ctx["wet_session"]["principal_idx"]
    study_idx = await _seed_study(ctx, owner_idx=owner_idx, suffix="ACC")
    prep_sample_idx = await _seed_prep_sample_linked_to_studies(
        ctx, owner_idx=owner_idx, study_idxs=[study_idx]
    )
    # idx-derived accessions keep the study UNIQUE constraints collision-free.
    await ctx["pool"].execute(
        "UPDATE qiita.study SET bioproject_accession = $2, ena_study_accession = $3 WHERE idx = $1",
        study_idx,
        f"PRJ-{study_idx}",
        f"ERP-{study_idx}",
    )

    resp = await ctx["wet"].get(URL_PREP_SAMPLE_STUDY_LIST.format(prep_sample_idx=prep_sample_idx))
    assert resp.status_code == 200, resp.text
    assert resp.json()["studies"] == [
        _study_item(study_idx, bioproject=f"PRJ-{study_idx}", ena_study=f"ERP-{study_idx}")
    ]


async def test_list_studies_for_prep_sample_excludes_retired_links(ctx):
    """Tests the case where one of two links is retired: only the active
    study is returned."""
    owner_idx = ctx["wet_session"]["principal_idx"]
    study_a = await _seed_study(ctx, owner_idx=owner_idx, suffix="ACTIVE")
    study_b = await _seed_study(ctx, owner_idx=owner_idx, suffix="RETIRED")
    prep_sample_idx = await _seed_prep_sample_linked_to_studies(
        ctx, owner_idx=owner_idx, study_idxs=[study_a, study_b]
    )
    await retire_prep_sample_to_study_link(
        ctx["pool"],
        prep_sample_idx=prep_sample_idx,
        study_idx=study_b,
        retired_by_idx=owner_idx,
    )

    resp = await ctx["wet"].get(URL_PREP_SAMPLE_STUDY_LIST.format(prep_sample_idx=prep_sample_idx))
    assert resp.status_code == 200, resp.text
    assert resp.json()["studies"] == [_study_item(study_a)]


async def test_list_studies_for_prep_sample_no_links_empty(ctx):
    """Tests the case where the prep_sample has no study links: the route
    returns an empty studies list."""
    owner_idx = ctx["wet_session"]["principal_idx"]
    prep_sample_idx = await _seed_prep_sample_linked_to_studies(
        ctx, owner_idx=owner_idx, study_idxs=[]
    )

    resp = await ctx["wet"].get(URL_PREP_SAMPLE_STUDY_LIST.format(prep_sample_idx=prep_sample_idx))
    assert resp.status_code == 200, resp.text
    assert resp.json()["studies"] == []


async def test_list_studies_for_prep_sample_unknown_idx_404(ctx):
    """Tests the case where the prep_sample_idx has no row: the
    require_prep_sample_exists guard returns 404."""
    resp = await ctx["wet"].get(URL_PREP_SAMPLE_STUDY_LIST.format(prep_sample_idx=2_000_000_000))
    assert resp.status_code == 404, resp.text


async def test_list_studies_for_prep_sample_regular_user_403(ctx):
    """Tests the case where a regular user (system_role below wet_lab_admin)
    calls the route: the role gate rejects with 403 even for a real
    prep_sample."""
    owner_idx = ctx["wet_session"]["principal_idx"]
    study_a = await _seed_study(ctx, owner_idx=owner_idx, suffix="ROLE")
    prep_sample_idx = await _seed_prep_sample_linked_to_studies(
        ctx, owner_idx=owner_idx, study_idxs=[study_a]
    )

    resp = await ctx["user"].get(URL_PREP_SAMPLE_STUDY_LIST.format(prep_sample_idx=prep_sample_idx))
    assert resp.status_code == 403, resp.text


async def test_list_studies_for_prep_sample_anonymous_401(ctx):
    """Tests the case where an unauthenticated caller hits the route: the
    require_human gate rejects with 401 even for a real prep_sample."""
    owner_idx = ctx["wet_session"]["principal_idx"]
    study_a = await _seed_study(ctx, owner_idx=owner_idx, suffix="ANON")
    prep_sample_idx = await _seed_prep_sample_linked_to_studies(
        ctx, owner_idx=owner_idx, study_idxs=[study_a]
    )

    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.get(URL_PREP_SAMPLE_STUDY_LIST.format(prep_sample_idx=prep_sample_idx))
    assert resp.status_code == 401


async def test_list_studies_for_prep_sample_missing_scope_403(ctx, no_prep_sample_read_client):
    """Tests the case where the caller lacks Scope.PREP_SAMPLE_READ: the scope
    gate rejects with 403 even for a real prep_sample."""
    owner_idx = ctx["wet_session"]["principal_idx"]
    study_a = await _seed_study(ctx, owner_idx=owner_idx, suffix="NOSCOPE")
    prep_sample_idx = await _seed_prep_sample_linked_to_studies(
        ctx, owner_idx=owner_idx, study_idxs=[study_a]
    )

    resp = await no_prep_sample_read_client.get(
        URL_PREP_SAMPLE_STUDY_LIST.format(prep_sample_idx=prep_sample_idx)
    )
    assert resp.status_code == 403
    assert "prep_sample:read" in resp.json()["detail"]
