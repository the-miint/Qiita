"""Integration tests for the POST /api/v1/study route.

Covers happy-path creation across the three caller-role variants,
the lab-tech-on-behalf rule, auth / scope guards, the collapsed
owner-eligibility 422 surface (parametrised across the six
ineligibility shapes), Pydantic validation, and the route's
exception-mapping path (PI FK 422, owner non-user 422).
"""

import secrets

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.auth_constants import Scope

from qiita_control_plane.testing.db_seeds import (
    seed_service_principal,
    seed_user_principal,
)

from .conftest import (
    OWNER_INELIGIBILITY_KINDS,
    IneligibilityKind,
    assert_owner_ineligibility_422,
    delete_idxs,
    resolve_ineligible_owner_idx,
)

pytestmark = pytest.mark.db


_SEED_PREFIX = "st-route"
_ELIGIBILITY_DETAIL = "owner is not eligible to own studies"


def _unique_title(prefix: str = "study") -> str:
    return f"{prefix}_{secrets.token_hex(4)}"


# ---------------------------------------------------------------------------
# FK-reverse cleanup
# ---------------------------------------------------------------------------


async def _cleanup_tracked(pool, created: dict) -> None:
    """Drop every test-created row in FK-reverse order: study_access →
    study → user / service subtype rows → principal."""
    for st, p in created["study_access"]:
        await pool.execute(
            "DELETE FROM qiita.study_access WHERE study_idx = $1 AND principal_idx = $2",
            st,
            p,
        )
    await delete_idxs(pool, "study", created["study"])
    if created["user_principals"]:
        await pool.execute(
            "DELETE FROM qiita.user WHERE principal_idx = ANY($1::bigint[])",
            created["user_principals"],
        )
    if created["service_account_principals"]:
        await pool.execute(
            "DELETE FROM qiita.service_account WHERE principal_idx = ANY($1::bigint[])",
            created["service_account_principals"],
        )
    all_principals = created["user_principals"] + created["service_account_principals"]
    await delete_idxs(pool, "principal", all_principals)


# ---------------------------------------------------------------------------
# Per-test ctx fixture
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def ctx(role_keyed_clients):
    """Per-test fixture wrapping role_keyed_clients with a route-specific
    `created` tracker for FK-reverse cleanup at teardown.

    The session principals (admin, wet_lab_admin, regular_user) are
    fixture-managed and never go in the cleanup list."""
    created: dict = {
        "study_access": [],
        "study": [],
        "user_principals": [],
        "service_account_principals": [],
    }
    yield {**role_keyed_clients, "created": created}
    await _cleanup_tracked(role_keyed_clients["pool"], created)


@pytest_asyncio.fixture
async def no_study_write_client(make_pat_client):
    """A regular_user PAT with a scope set that EXCLUDES Scope.STUDY_WRITE —
    drives the require_scope guard's missing-scope 403."""
    return await make_pat_client(label="st-no-write", scopes=[Scope.SELF_PROFILE])


# ---------------------------------------------------------------------------
# Route-call helper
# ---------------------------------------------------------------------------


async def _post_study(client, ctx, **body):
    """POST the create-study route and, on 201, track created rows for cleanup."""
    resp = await client.post("/api/v1/study", json=body)
    if resp.status_code == 201:
        rj = resp.json()
        ctx["created"]["study"].append(rj["study_idx"])
        ctx["created"]["study_access"].append((rj["study_idx"], rj["owner_idx"]))
    return resp


# ===========================================================================
# Happy paths
# ===========================================================================


async def test_post_study_regular_user_self_owner_idx_omitted(ctx):
    """Body without owner_idx defaults to caller-creates-own-study; the
    auto-grant ADMIN row targets the caller, granted_by_idx = caller."""
    title = _unique_title("self-omitted")
    resp = await _post_study(ctx["user"], ctx, title=title)
    assert resp.status_code == 201, resp.text
    rj = resp.json()
    assert rj["study_idx"] > 0
    assert rj["owner_idx"] == ctx["user_session"]["principal_idx"]
    assert rj["created_by_idx"] == ctx["user_session"]["principal_idx"]
    assert rj["title"] == title
    # Schema default applies for default_tier when the body omits it.
    assert rj["default_tier"] == "member"

    grant = await ctx["pool"].fetchrow(
        "SELECT access_tier, granted_by_idx"
        " FROM qiita.study_access"
        " WHERE study_idx = $1 AND principal_idx = $2",
        rj["study_idx"],
        rj["owner_idx"],
    )
    assert grant["access_tier"] == "admin"
    assert grant["granted_by_idx"] == ctx["user_session"]["principal_idx"]


async def test_post_study_regular_user_self_owner_idx_explicit(ctx):
    """An explicit owner_idx that equals the caller's principal_idx is
    treated the same as omitting it — no on-behalf rule trip."""
    resp = await _post_study(
        ctx["user"],
        ctx,
        title=_unique_title("self-explicit"),
        owner_idx=ctx["user_session"]["principal_idx"],
    )
    assert resp.status_code == 201, resp.text


async def test_post_study_full_body_round_trips(ctx):
    """Every settable column on StudyCreate, including extra_metadata
    and a non-default default_tier, is reflected in the response."""
    extra = {"site": "ucsd", "project_type": "METAGENOMIC"}
    title = _unique_title("full")
    resp = await _post_study(
        ctx["user"],
        ctx,
        title=title,
        alias="p-1",
        description="desc",
        abstract="abs",
        funding="NIH-R01",
        ebi_study_accession="ERP000001",
        vamps_id="VAMPS-1",
        notes="notes-1",
        extra_metadata=extra,
        default_tier="viewer",
    )
    assert resp.status_code == 201, resp.text
    rj = resp.json()
    caller_idx = ctx["user_session"]["principal_idx"]
    expected = {
        # Auto-generated by the DB; copy actual into expected so the
        # equality confirms field presence without pinning the values.
        "study_idx": rj["study_idx"],
        "created_at": rj["created_at"],
        "updated_at": rj["updated_at"],
        "owner_idx": caller_idx,
        "principal_investigator_idx": None,
        "title": title,
        "alias": "p-1",
        "description": "desc",
        "abstract": "abs",
        "funding": "NIH-R01",
        "ebi_study_accession": "ERP000001",
        "vamps_id": "VAMPS-1",
        "notes": "notes-1",
        "extra_metadata": extra,
        "default_tier": "viewer",
        "created_by_idx": caller_idx,
    }
    assert rj == expected


async def test_post_study_wet_lab_admin_on_behalf_of_other_user(ctx):
    """A wet_lab_admin can name a different user as owner; the admin is
    `created_by_idx`, the named user is `owner_idx`, and the auto-grant
    ADMIN row targets the named user (not the admin)."""
    target_idx = await seed_user_principal(ctx["pool"], prefix=_SEED_PREFIX, suffix="wet-target")
    ctx["created"]["user_principals"].append(target_idx)

    resp = await _post_study(
        ctx["wet"],
        ctx,
        title=_unique_title("wet-onbehalf"),
        owner_idx=target_idx,
    )
    assert resp.status_code == 201, resp.text
    rj = resp.json()
    assert rj["owner_idx"] == target_idx
    assert rj["created_by_idx"] == ctx["wet_session"]["principal_idx"]

    grant = await ctx["pool"].fetchrow(
        "SELECT access_tier, granted_by_idx"
        " FROM qiita.study_access"
        " WHERE study_idx = $1 AND principal_idx = $2",
        rj["study_idx"],
        target_idx,
    )
    assert grant["access_tier"] == "admin"
    assert grant["granted_by_idx"] == ctx["wet_session"]["principal_idx"]

    # The admin caller does NOT get a study_access row from the auto-grant.
    admin_grant = await ctx["pool"].fetchval(
        "SELECT 1 FROM qiita.study_access WHERE study_idx = $1 AND principal_idx = $2",
        rj["study_idx"],
        ctx["wet_session"]["principal_idx"],
    )
    assert admin_grant is None


async def test_post_study_system_admin_on_behalf_of_other_user(ctx):
    """A system_admin is also above the wet_lab_admin threshold so the
    on-behalf path is open to them too."""
    target_idx = await seed_user_principal(ctx["pool"], prefix=_SEED_PREFIX, suffix="adm-target")
    ctx["created"]["user_principals"].append(target_idx)

    resp = await _post_study(
        ctx["admin"],
        ctx,
        title=_unique_title("adm-onbehalf"),
        owner_idx=target_idx,
    )
    assert resp.status_code == 201, resp.text
    assert resp.json()["owner_idx"] == target_idx


async def test_post_study_with_principal_investigator(ctx):
    """A valid principal_investigator_idx pointing at a user-kind
    principal is accepted and round-trips through the response."""
    pi_idx = await seed_user_principal(ctx["pool"], prefix=_SEED_PREFIX, suffix="pi-good")
    ctx["created"]["user_principals"].append(pi_idx)

    resp = await _post_study(
        ctx["user"],
        ctx,
        title=_unique_title("with-pi"),
        principal_investigator_idx=pi_idx,
    )
    assert resp.status_code == 201, resp.text
    assert resp.json()["principal_investigator_idx"] == pi_idx


# ===========================================================================
# Lab-tech-on-behalf rule
# ===========================================================================


async def test_post_study_regular_user_setting_other_owner_idx_403(ctx):
    """A regular USER caller cannot set owner_idx to a different
    principal — only wet_lab_admin or higher passes the on-behalf rule."""
    target_idx = await seed_user_principal(ctx["pool"], prefix=_SEED_PREFIX, suffix="reg-target")
    ctx["created"]["user_principals"].append(target_idx)

    resp = await _post_study(
        ctx["user"],
        ctx,
        title=_unique_title("reg-onbehalf"),
        owner_idx=target_idx,
    )
    assert resp.status_code == 403
    assert "wet_lab_admin or higher" in resp.json()["detail"]


# ===========================================================================
# Auth / scope guards
# ===========================================================================


async def test_post_study_anonymous_401(ctx):
    """No Authorization header → require_complete_profile chain raises 401."""
    from qiita_control_plane.main import app

    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.post("/api/v1/study", json={"title": _unique_title("anon")})
    assert resp.status_code == 401


async def test_post_study_user_without_study_write_scope_403(ctx, no_study_write_client):
    """A regular_user PAT that omits Scope.STUDY_WRITE is rejected by
    require_scope before the route body runs."""
    resp = await no_study_write_client.post(
        "/api/v1/study", json={"title": _unique_title("no-scope")}
    )
    assert resp.status_code == 403
    assert "study:write" in resp.json()["detail"]


# ===========================================================================
# Owner eligibility — collapsed 422 surface
# ===========================================================================
# Like the biosample handler, all ineligibility cases collapse to the
# same 422 detail to avoid leaking principal-state to callers probing
# arbitrary owner_idx values. Kept as one parametrised test so each kind
# still locks in that the matching backend code path emits 422 — a
# regression where one input accidentally yields 500 / 409 / 201 still
# surfaces here.


@pytest.mark.parametrize("kind", OWNER_INELIGIBILITY_KINDS)
async def test_post_study_owner_ineligibility_422(ctx, kind: IneligibilityKind):
    owner_idx = await resolve_ineligible_owner_idx(
        ctx["pool"],
        kind=kind,
        prefix=f"{_SEED_PREFIX}-elig",
        created=ctx["created"],
    )

    async def _post(idx: int):
        return await _post_study(
            ctx["wet"],
            ctx,
            title=_unique_title(f"elig-{kind}"),
            owner_idx=idx,
        )

    await assert_owner_ineligibility_422(
        post_with_owner_idx=_post,
        expected_detail=_ELIGIBILITY_DETAIL,
        owner_idx=owner_idx,
    )


# ===========================================================================
# Body validation
# ===========================================================================


async def test_post_study_empty_body_422(ctx):
    """Pydantic rejects {} because title is required."""
    resp = await ctx["user"].post("/api/v1/study", json={})
    assert resp.status_code == 422


async def test_post_study_empty_title_422(ctx):
    """Pydantic min_length=1 rejects an empty title."""
    resp = await ctx["user"].post("/api/v1/study", json={"title": ""})
    assert resp.status_code == 422


# ===========================================================================
# Repository exception mapping
# ===========================================================================


async def test_post_study_unknown_pi_idx_422(ctx):
    """A principal_investigator_idx past the highest existing idx hits
    tg_principal_must_be_user (BEFORE-INSERT trigger fires ahead of the
    FK constraint), so all bad-PI inputs collapse to one 422 message."""
    max_idx = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.principal")
    resp = await _post_study(
        ctx["user"],
        ctx,
        title=_unique_title("bad-pi"),
        principal_investigator_idx=max_idx + 100_000,
    )
    assert resp.status_code == 422
    assert (
        resp.json()["detail"] == "principal_investigator_idx must reference a user-kind principal"
    )


async def test_post_study_pi_idx_is_service_account_422(ctx):
    """A service-account-kind PI trips the user-kind trigger; the route
    maps it to 422 with the disambiguated principal_investigator message."""
    svc_idx = await seed_service_principal(ctx["pool"], prefix=_SEED_PREFIX, suffix="pi-svc")
    ctx["created"]["service_account_principals"].append(svc_idx)

    resp = await _post_study(
        ctx["user"],
        ctx,
        title=_unique_title("pi-svc"),
        principal_investigator_idx=svc_idx,
    )
    assert resp.status_code == 422
    assert (
        resp.json()["detail"] == "principal_investigator_idx must reference a user-kind principal"
    )


# ===========================================================================
# GET /api/v1/study/{study_idx} — wiring tests
# ===========================================================================
# Tier-policy edge cases (default_tier resolution, public-by-absence,
# bypass_role semantics) are exhaustively covered at the guard level in
# tests/auth/test_guards.py. The route tests below confirm only that the
# guard is wired to the GET handler and that the happy-path response
# matches what POST returned for the same row.


@pytest_asyncio.fixture
async def no_study_read_client(make_pat_client):
    """A regular_user PAT with a scope set that EXCLUDES Scope.STUDY_READ —
    drives the GET route's require_scope guard's missing-scope 403."""
    return await make_pat_client(label="st-no-read", scopes=[Scope.SELF_PROFILE])


async def test_get_study_returns_same_shape_as_post(ctx):
    """A study created via POST round-trips through GET unchanged — same
    fields, same values, including JSONB extra_metadata."""
    extra = {"site": "ucsd", "project_type": "METAGENOMIC"}
    title = _unique_title("get-roundtrip")
    create_resp = await _post_study(
        ctx["user"],
        ctx,
        title=title,
        alias="alias-1",
        description="desc",
        abstract="abs",
        funding="NIH-R01",
        ebi_study_accession="ERP000001",
        vamps_id="VAMPS-1",
        notes="notes-1",
        extra_metadata=extra,
        default_tier="viewer",
    )
    assert create_resp.status_code == 201, create_resp.text
    posted = create_resp.json()

    # Owner has ADMIN auto-grant which beats the viewer default_tier;
    # the same client therefore passes the read-access policy.
    get_resp = await ctx["user"].get(f"/api/v1/study/{posted['study_idx']}")
    assert get_resp.status_code == 200, get_resp.text
    assert get_resp.json() == posted


async def test_get_study_anonymous_401(ctx):
    """No Authorization header → require_scope chain raises 401 ahead of
    any DB lookup."""
    from qiita_control_plane.main import app

    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.get("/api/v1/study/1")
    assert resp.status_code == 401


async def test_get_study_user_without_study_read_scope_403(ctx, no_study_read_client):
    """A regular_user PAT that omits Scope.STUDY_READ is rejected by
    require_scope before the access guard runs."""
    create_resp = await _post_study(
        ctx["user"],
        ctx,
        title=_unique_title("get-no-scope"),
    )
    assert create_resp.status_code == 201, create_resp.text
    study_idx = create_resp.json()["study_idx"]

    resp = await no_study_read_client.get(f"/api/v1/study/{study_idx}")
    assert resp.status_code == 403
    assert "study:read" in resp.json()["detail"]


async def test_get_study_nonexistent_404(ctx):
    """An idx past the highest existing study yields 404 for a
    wet_lab_admin caller. The require_study_access bypass path returns
    without a DB lookup, so this 404 is sourced from the
    require_study_exists guard composed alongside it."""
    max_idx = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.study")
    resp = await ctx["wet"].get(f"/api/v1/study/{max_idx + 100_000}")
    assert resp.status_code == 404


async def test_get_study_below_default_tier_403(ctx):
    """A regular user with no study_access row on a study whose
    default_tier is 'member' has effective tier public-by-absence, fails
    the access guard, and receives 403."""
    # Create the study as a wet_lab_admin on behalf of a fresh user so
    # the regular_user caller in ctx is NOT the owner (owner has the
    # ADMIN auto-grant which would otherwise pass any tier check).
    other_owner = await seed_user_principal(
        ctx["pool"], prefix=_SEED_PREFIX, suffix="get-403-owner"
    )
    ctx["created"]["user_principals"].append(other_owner)

    create_resp = await _post_study(
        ctx["wet"],
        ctx,
        title=_unique_title("get-403"),
        owner_idx=other_owner,
        default_tier="member",
    )
    assert create_resp.status_code == 201, create_resp.text
    study_idx = create_resp.json()["study_idx"]

    resp = await ctx["user"].get(f"/api/v1/study/{study_idx}")
    assert resp.status_code == 403
    assert "'member'" in resp.json()["detail"]
