"""Integration tests for the prep-sample routes."""

import secrets

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import (
    URL_PREP_SAMPLE_RETIRED,
    URL_PREP_SAMPLE_STUDY_LIST,
)
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX
from qiita_common.models import FieldDataType

from qiita_control_plane.main import app
from qiita_control_plane.testing.db_seeds import (
    fetch_seeded_metagenome_term,
    retire_prep_sample_to_study_link,
    seed_biosample,
    seed_biosample_to_study_link,
    seed_prep_sample_global_field,
    seed_sequenced_prep_sample,
)
from qiita_control_plane.testing.unique_names import unique_field_name

from .conftest import (
    PREP_SAMPLE_FIELD_SURFACE,
    STUDY_FIELD_CREATE_AUTHZ_CASES,
    STUDY_FIELD_CREATE_CONFLICT_CASES,
    STUDY_FIELD_LIST_AUTHZ_CASES,
    _grant_study_access,
    _seed_study,
    assert_study_field_create_authz,
    assert_study_field_create_conflict,
    assert_study_field_list_authz,
    delete_idxs,
    post_study_field,
)

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# FK-reverse cleanup
# ---------------------------------------------------------------------------


async def _cleanup_tracked(pool, created: dict) -> None:
    """Drop tracked rows in FK-reverse order (ON DELETE RESTRICT throughout):
    prep_sample_to_study, prep_sample, biosample_to_study, biosample,
    prep_sample_study_field, prep_sample_global_field, study_access, study."""
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
    # Study fields reference both their study and, when linked, a global field,
    # so they drop before either.
    await delete_idxs(pool, "prep_sample_study_field", created["prep_sample_study_field"])
    await delete_idxs(pool, "prep_sample_global_field", created["prep_sample_global_field"])
    for st, principal in created["study_access"]:
        await pool.execute(
            "DELETE FROM qiita.study_access WHERE study_idx = $1 AND principal_idx = $2",
            st,
            principal,
        )
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
        "prep_sample_study_field": [],
        "prep_sample_global_field": [],
        "study_access": [],
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


# ===========================================================================
# PATCH /api/v1/prep-sample/{idx}/retired
# ===========================================================================


async def _retired_flag(ctx, prep_sample_idx: int) -> bool:
    return await ctx["pool"].fetchval(
        "SELECT retired FROM qiita.prep_sample WHERE idx = $1", prep_sample_idx
    )


async def test_retire_prep_sample_sets_flag(ctx):
    """A wet_lab_admin retire sets retired=true plus the audit columns
    (retired_by_idx, retired_at), honouring the consistency CHECK."""
    owner_idx = ctx["wet_session"]["principal_idx"]
    prep_sample_idx = await _seed_prep_sample_linked_to_studies(
        ctx, owner_idx=owner_idx, study_idxs=[]
    )

    resp = await ctx["wet"].patch(
        URL_PREP_SAMPLE_RETIRED.format(prep_sample_idx=prep_sample_idx),
        json={"retired": True, "reason": "empty well"},
    )
    assert resp.status_code == 204, resp.text

    row = await ctx["pool"].fetchrow(
        "SELECT retired, retired_by_idx, retired_at, retire_reason"
        " FROM qiita.prep_sample WHERE idx = $1",
        prep_sample_idx,
    )
    assert row["retired"] is True
    assert row["retired_by_idx"] == owner_idx
    assert row["retired_at"] is not None
    assert row["retire_reason"] == "empty well"


async def test_retire_prep_sample_is_idempotent(ctx):
    """Re-retiring an already-retired prep_sample is a no-op success (204)."""
    owner_idx = ctx["wet_session"]["principal_idx"]
    prep_sample_idx = await _seed_prep_sample_linked_to_studies(
        ctx, owner_idx=owner_idx, study_idxs=[]
    )
    url = URL_PREP_SAMPLE_RETIRED.format(prep_sample_idx=prep_sample_idx)

    assert (await ctx["wet"].patch(url, json={"retired": True})).status_code == 204
    assert (await ctx["wet"].patch(url, json={"retired": True})).status_code == 204
    assert await _retired_flag(ctx, prep_sample_idx) is True


async def test_un_retire_prep_sample_clears_flag(ctx):
    """Un-retiring (retired=false) clears the flag and the audit columns —
    a misclassified well is recoverable, so retirement is reversible."""
    owner_idx = ctx["wet_session"]["principal_idx"]
    prep_sample_idx = await _seed_prep_sample_linked_to_studies(
        ctx, owner_idx=owner_idx, study_idxs=[]
    )
    url = URL_PREP_SAMPLE_RETIRED.format(prep_sample_idx=prep_sample_idx)

    assert (await ctx["wet"].patch(url, json={"retired": True, "reason": "x"})).status_code == 204
    assert (await ctx["wet"].patch(url, json={"retired": False})).status_code == 204

    row = await ctx["pool"].fetchrow(
        "SELECT retired, retired_by_idx, retired_at, retire_reason"
        " FROM qiita.prep_sample WHERE idx = $1",
        prep_sample_idx,
    )
    assert row["retired"] is False
    assert row["retired_by_idx"] is None
    assert row["retired_at"] is None
    assert row["retire_reason"] is None


async def test_retire_prep_sample_unknown_idx_404(ctx):
    resp = await ctx["wet"].patch(
        URL_PREP_SAMPLE_RETIRED.format(prep_sample_idx=2_000_000_000),
        json={"retired": True},
    )
    assert resp.status_code == 404, resp.text


async def test_retire_prep_sample_regular_user_403(ctx):
    """A regular user (below wet_lab_admin) cannot retire — the role gate 403s."""
    owner_idx = ctx["wet_session"]["principal_idx"]
    prep_sample_idx = await _seed_prep_sample_linked_to_studies(
        ctx, owner_idx=owner_idx, study_idxs=[]
    )
    resp = await ctx["user"].patch(
        URL_PREP_SAMPLE_RETIRED.format(prep_sample_idx=prep_sample_idx),
        json={"retired": True},
    )
    assert resp.status_code == 403, resp.text
    # Not retired — the gate fired before the write.
    assert await _retired_flag(ctx, prep_sample_idx) is False


async def test_retire_prep_sample_missing_scope_403(ctx, no_prep_sample_write_client):
    """A caller lacking Scope.PREP_SAMPLE_WRITE is rejected by the scope gate."""
    owner_idx = ctx["wet_session"]["principal_idx"]
    prep_sample_idx = await _seed_prep_sample_linked_to_studies(
        ctx, owner_idx=owner_idx, study_idxs=[]
    )
    resp = await no_prep_sample_write_client.patch(
        URL_PREP_SAMPLE_RETIRED.format(prep_sample_idx=prep_sample_idx),
        json={"retired": True},
    )
    assert resp.status_code == 403, resp.text
    assert "prep_sample:write" in resp.json()["detail"]


async def test_retire_prep_sample_anonymous_401(ctx):
    """An unauthenticated caller is rejected by require_human."""
    owner_idx = ctx["wet_session"]["principal_idx"]
    prep_sample_idx = await _seed_prep_sample_linked_to_studies(
        ctx, owner_idx=owner_idx, study_idxs=[]
    )
    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.patch(
            URL_PREP_SAMPLE_RETIRED.format(prep_sample_idx=prep_sample_idx),
            json={"retired": True},
        )
    assert resp.status_code == 401


# ===========================================================================
# POST /api/v1/study/{study_idx}/prep-sample-field — create study-local field
# ===========================================================================


async def _post_prep_sample_field(client, ctx, study_idx: int, **body):
    """POST the create-field route and, on 201, track the created row."""
    return await post_study_field(
        ctx,
        surface=PREP_SAMPLE_FIELD_SURFACE,
        client=client,
        study_idx=study_idx,
        **body,
    )


async def _seed_prep_global_field(
    ctx,
    *,
    data_type=FieldDataType.NUMERIC,
    terminology_idx: int | None = None,
    required: bool = False,
) -> tuple[int, str]:
    """Seed one prep_sample_global_field and return (idx, display_name)."""
    suffix = secrets.token_hex(4)
    display_name = f"Global {suffix}"
    global_idx = await seed_prep_sample_global_field(
        ctx["pool"],
        internal_name=f"psf_{suffix}",
        display_name=display_name,
        data_type=data_type,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
        terminology_idx=terminology_idx,
        required=required,
    )
    ctx["created"]["prep_sample_global_field"].append(global_idx)
    return global_idx, display_name


async def test_create_prep_sample_field_admin_local(ctx):
    """Tests the case where an ADMIN-grant user creates a purely-local field:
    the 201 body is the created resource, with the local data_type and a
    defaulted-False required.
    """
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="pf-adm"
    )
    await _grant_study_access(
        ctx,
        study_idx=study_idx,
        principal_idx=ctx["user_session"]["principal_idx"],
        tier="admin",
        granted_by_idx=ctx["wet_session"]["principal_idx"],
    )
    display_name = unique_field_name("Local")

    resp = await _post_prep_sample_field(
        ctx["user"], ctx, study_idx, display_name=display_name, data_type="numeric"
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    expected = {
        # Auto-generated; copy actual into expected so the equality confirms
        # presence without pinning the minted idx or the DB-assigned timestamp.
        "prep_sample_study_field_idx": body["prep_sample_study_field_idx"],
        "created_at": body["created_at"],
        "study_idx": study_idx,
        "prep_sample_global_field_idx": None,
        "display_name": display_name,
        "description": None,
        "data_type": "numeric",
        "required": False,
        "terminology_idx": None,
        "tier_override": None,
        "created_by_idx": ctx["user_session"]["principal_idx"],
    }
    assert body == expected


async def test_create_prep_sample_field_linked_inherits(ctx):
    """Tests the case where the create links the field to a global field: the
    response resolves data_type and required from the global row even though
    the study-field columns are NULL.
    """
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["user_session"]["principal_idx"], suffix="pf-link"
    )
    global_idx, _ = await _seed_prep_global_field(ctx)
    display_name = unique_field_name("Linked")

    resp = await _post_prep_sample_field(
        ctx["user"],
        ctx,
        study_idx,
        display_name=display_name,
        prep_sample_global_field_idx=global_idx,
        description="linked",
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    expected = {
        "prep_sample_study_field_idx": body["prep_sample_study_field_idx"],
        "created_at": body["created_at"],
        "study_idx": study_idx,
        "prep_sample_global_field_idx": global_idx,
        "display_name": display_name,
        "description": "linked",
        "data_type": "numeric",
        "required": False,
        "terminology_idx": None,
        "tier_override": None,
        "created_by_idx": ctx["user_session"]["principal_idx"],
    }
    assert body == expected


async def test_create_prep_sample_field_linked_with_data_type_422(ctx):
    """Tests the case where a globally-linked create also supplies an inherited
    column: the mode-coupling validator rejects it at the wire boundary.
    """
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["user_session"]["principal_idx"], suffix="pf-both"
    )
    global_idx, _ = await _seed_prep_global_field(ctx)

    resp = await _post_prep_sample_field(
        ctx["user"],
        ctx,
        study_idx,
        display_name=unique_field_name("Both"),
        prep_sample_global_field_idx=global_idx,
        data_type="numeric",
    )
    assert resp.status_code == 422


async def test_create_prep_sample_field_local_without_data_type_422(ctx):
    """Tests the case where a purely-local create omits data_type: the type
    lives on this row, so the mode-coupling validator rejects it.
    """
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["user_session"]["principal_idx"], suffix="pf-notype"
    )

    resp = await _post_prep_sample_field(
        ctx["user"], ctx, study_idx, display_name=unique_field_name("NoType")
    )
    assert resp.status_code == 422


async def test_create_prep_sample_field_terminology_coupling_422(ctx):
    """Tests the case where a local terminology-typed create omits
    terminology_idx: the coupling validator requires them together.
    """
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["user_session"]["principal_idx"], suffix="pf-term"
    )

    resp = await _post_prep_sample_field(
        ctx["user"], ctx, study_idx, display_name=unique_field_name("Term"), data_type="terminology"
    )
    assert resp.status_code == 422


@pytest.mark.parametrize("case", STUDY_FIELD_CREATE_AUTHZ_CASES)
async def test_create_prep_sample_field_authz(ctx, case, no_prep_sample_write_client):
    """Tests the case where each row of the shared access matrix calls the
    create-field route: owner, admin grant, and wet_lab_admin bypass are
    admitted; no access, sub-ADMIN tier, and a missing scope are refused; a
    nonexistent study is 404 even for a role-bypass caller.
    """
    await assert_study_field_create_authz(
        ctx,
        case=case,
        surface=PREP_SAMPLE_FIELD_SURFACE,
        no_scope_client=no_prep_sample_write_client,
    )


@pytest.mark.parametrize("case", STUDY_FIELD_CREATE_CONFLICT_CASES)
async def test_create_prep_sample_field_conflict(ctx, case):
    """Tests the case where each row of the shared conflict matrix calls the
    create-field route: a name already on the study is a 409, rebinding that
    name to a different global field is a 409, and a global-field link naming
    no row is a 422.
    """
    await assert_study_field_create_conflict(ctx, case=case, surface=PREP_SAMPLE_FIELD_SURFACE)


# ===========================================================================
# GET /api/v1/study/{study_idx}/prep-sample-field
# ===========================================================================


async def _list_prep_sample_fields(client, study_idx: int):
    return await client.get(PREP_SAMPLE_FIELD_SURFACE.url_template.format(study_idx=study_idx))


async def test_list_prep_sample_fields_in_study_resolves_linked_and_local(ctx):
    """Tests the case where a study carries one globally-linked and one
    purely-local field: both come back ordered by display_name, the linked one
    with data_type, required, and terminology_idx resolved from its global row.

    The global row's three inherited columns all differ from the local field's,
    so each one discriminates whether the read resolved it or fell back to the
    study row.
    """
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["user_session"]["principal_idx"], suffix="pf-list"
    )
    # Reuse the seeded NCBI Taxonomy so the global field can be TERMINOLOGY,
    # which the local field cannot be without a terminology of its own.
    terminology_idx = (await fetch_seeded_metagenome_term(ctx["pool"]))["terminology_idx"]
    global_idx, _ = await _seed_prep_global_field(
        ctx,
        data_type=FieldDataType.TERMINOLOGY,
        terminology_idx=terminology_idx,
        required=True,
    )

    # Names chosen so the local field sorts first, proving the ORDER BY rather
    # than insertion order.
    local_name = unique_field_name("AAA-local")
    linked_name = unique_field_name("ZZZ-linked")
    local_resp = await _post_prep_sample_field(
        ctx["user"], ctx, study_idx, display_name=local_name, data_type="text"
    )
    assert local_resp.status_code == 201, local_resp.text
    linked_resp = await _post_prep_sample_field(
        ctx["user"],
        ctx,
        study_idx,
        display_name=linked_name,
        prep_sample_global_field_idx=global_idx,
    )
    assert linked_resp.status_code == 201, linked_resp.text

    resp = await _list_prep_sample_fields(ctx["user"], study_idx)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    expected = [
        {
            "prep_sample_study_field_idx": body[0]["prep_sample_study_field_idx"],
            "created_at": body[0]["created_at"],
            "study_idx": study_idx,
            "prep_sample_global_field_idx": None,
            "display_name": local_name,
            "description": None,
            "data_type": "text",
            "required": False,
            "terminology_idx": None,
            "tier_override": None,
            "created_by_idx": ctx["user_session"]["principal_idx"],
        },
        {
            "prep_sample_study_field_idx": body[1]["prep_sample_study_field_idx"],
            "created_at": body[1]["created_at"],
            "study_idx": study_idx,
            "prep_sample_global_field_idx": global_idx,
            "display_name": linked_name,
            "description": None,
            "data_type": "terminology",
            "required": True,
            "terminology_idx": terminology_idx,
            "tier_override": None,
            "created_by_idx": ctx["user_session"]["principal_idx"],
        },
    ]
    assert body == expected


async def test_list_prep_sample_fields_in_study_no_fields_empty(ctx):
    """Tests the case where the study exists but carries no fields: the route
    returns an empty list rather than a 404."""
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["user_session"]["principal_idx"], suffix="pf-empty"
    )

    resp = await _list_prep_sample_fields(ctx["user"], study_idx)
    assert resp.status_code == 200, resp.text
    assert resp.json() == []


async def test_list_prep_sample_fields_in_study_excludes_other_studies(ctx):
    """Tests the case where two studies each carry a field: the response is
    scoped to the path's study only."""
    owner_idx = ctx["user_session"]["principal_idx"]
    study_a = await _seed_study(ctx, owner_idx=owner_idx, suffix="pf-sa")
    study_b = await _seed_study(ctx, owner_idx=owner_idx, suffix="pf-sb")
    name_a = unique_field_name("OnlyA")
    for study_idx, name in ((study_a, name_a), (study_b, unique_field_name("OnlyB"))):
        created = await _post_prep_sample_field(
            ctx["user"], ctx, study_idx, display_name=name, data_type="text"
        )
        assert created.status_code == 201, created.text

    resp = await _list_prep_sample_fields(ctx["user"], study_a)
    assert resp.status_code == 200, resp.text
    assert [row["display_name"] for row in resp.json()] == [name_a]


async def test_list_prep_sample_fields_in_study_anonymous_401(ctx):
    """Tests the case where an unauthenticated caller hits the route: the
    require_human gate rejects with 401 even for a real study."""
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="pf-anon"
    )

    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await _list_prep_sample_fields(anon, study_idx)
    assert resp.status_code == 401


@pytest.mark.parametrize("case", STUDY_FIELD_LIST_AUTHZ_CASES)
async def test_list_prep_sample_fields_in_study_authz(ctx, case, no_prep_sample_read_client):
    """Tests the case where each row of the shared list access matrix calls the
    route: owner, viewer/member/admin grants, and wet_lab_admin bypass are
    admitted; no access and a missing scope are refused; a nonexistent study is
    404 even for a role-bypass caller.
    """
    await assert_study_field_list_authz(
        ctx,
        case=case,
        surface=PREP_SAMPLE_FIELD_SURFACE,
        no_scope_client=no_prep_sample_read_client,
    )
