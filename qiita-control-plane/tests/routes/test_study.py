"""Integration tests for /api/v1/study routes: POST, GET, PATCH, and the
bulk lookup-by-accession endpoint.

POST: covers happy-path creation across the three caller-role variants,
the lab-tech-on-behalf rule, auth / scope guards, the collapsed
owner-eligibility 422 surface (parametrised across the six
ineligibility shapes), Pydantic validation, and the route's
exception-mapping path (PI FK 422, owner non-user 422).

GET: covers the wiring-level cases (round-trip with POST, 401, missing
scope 403, 404, below-default-tier 403); exhaustive tier-policy edge
cases live at the guard level in tests/auth/test_guards.py.

PATCH: covers the auth-bar variants (owner self, study admin tier,
wet_lab_admin role bypass), If-Match concurrency (428 / 412), body
validation (empty, extra-forbidden, explicit-null title, empty title),
the exception-mapping paths (PI FK 422, duplicate accession 409), and
the ETag bump.

POST /lookup-by-accession: covers resolved/missing wire shape, input
dedup preserving order, the no-per-row-access-predicate behavior, auth
and scope guards, and the request-model rejection paths.
"""

import secrets
from datetime import UTC, datetime

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import (
    URL_STUDY_BY_IDX,
    URL_STUDY_LOOKUP_BY_ACCESSION,
    URL_STUDY_PREFIX,
)
from qiita_common.auth_constants import Scope

from qiita_control_plane.testing.db_seeds import (
    seed_service_principal,
    seed_user_principal,
)
from qiita_control_plane.testing.unique_names import unique_accession

from .conftest import (
    OWNER_INELIGIBILITY_KINDS,
    IneligibilityKind,
    _grant_study_access,
    assert_owner_ineligibility_422,
    delete_idxs,
    etag_for_row,
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
    resp = await client.post(URL_STUDY_PREFIX, json=body)
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
    extra = {"site": "ucsd", "project_type": "METAGENOMIC", "vamps_id": "VAMPS-1"}
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
        "notes": "notes-1",
        "last_submission_at": None,
        "submission_error": None,
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
        resp = await anon.post(URL_STUDY_PREFIX, json={"title": _unique_title("anon")})
    assert resp.status_code == 401


async def test_post_study_user_without_study_write_scope_403(ctx, no_study_write_client):
    """A regular_user PAT that omits Scope.STUDY_WRITE is rejected by
    require_scope before the route body runs."""
    resp = await no_study_write_client.post(
        URL_STUDY_PREFIX, json={"title": _unique_title("no-scope")}
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
    resp = await ctx["user"].post(URL_STUDY_PREFIX, json={})
    assert resp.status_code == 422


async def test_post_study_empty_title_422(ctx):
    """Pydantic min_length=1 rejects an empty title."""
    resp = await ctx["user"].post(URL_STUDY_PREFIX, json={"title": ""})
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


async def test_post_study_duplicate_ebi_accession_409(ctx):
    """Tests the case where two POSTs supply the same non-null
    ebi_study_accession: the second trips the
    study_ebi_study_accession_unique constraint and the route maps it
    to 409 with the per-column detail."""
    shared_accession = f"ERP{secrets.token_hex(4)}"

    first = await _post_study(
        ctx["user"],
        ctx,
        title=_unique_title("dup-first"),
        ebi_study_accession=shared_accession,
    )
    assert first.status_code == 201, first.text

    resp = await _post_study(
        ctx["user"],
        ctx,
        title=_unique_title("dup-second"),
        ebi_study_accession=shared_accession,
    )
    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"] == "ebi_study_accession already in use"


async def test_post_study_two_null_ebi_accessions_both_succeed(ctx):
    """Tests the case where two POSTs both omit ebi_study_accession:
    Postgres UNIQUE treats NULLs as distinct, so two NULLs do not
    collide."""
    first = await _post_study(
        ctx["user"],
        ctx,
        title=_unique_title("null-first"),
    )
    assert first.status_code == 201, first.text
    assert first.json()["ebi_study_accession"] is None

    second = await _post_study(
        ctx["user"],
        ctx,
        title=_unique_title("null-second"),
    )
    assert second.status_code == 201, second.text
    assert second.json()["ebi_study_accession"] is None


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
    extra = {"site": "ucsd", "project_type": "METAGENOMIC", "vamps_id": "VAMPS-1"}
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
        notes="notes-1",
        extra_metadata=extra,
        default_tier="viewer",
    )
    assert create_resp.status_code == 201, create_resp.text
    posted = create_resp.json()

    # Owner has ADMIN auto-grant which beats the viewer default_tier;
    # the same client therefore passes the read-access policy.
    get_resp = await ctx["user"].get(URL_STUDY_BY_IDX.format(study_idx=posted["study_idx"]))
    assert get_resp.status_code == 200, get_resp.text
    assert get_resp.json() == posted


async def test_get_study_anonymous_401(ctx):
    """No Authorization header → require_scope chain raises 401 ahead of
    any DB lookup."""
    from qiita_control_plane.main import app

    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.get(URL_STUDY_BY_IDX.format(study_idx=1))
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

    resp = await no_study_read_client.get(URL_STUDY_BY_IDX.format(study_idx=study_idx))
    assert resp.status_code == 403
    assert "study:read" in resp.json()["detail"]


async def test_get_study_nonexistent_404(ctx):
    """An idx past the highest existing study yields 404 for a
    wet_lab_admin caller. The require_study_access bypass path returns
    without a DB lookup, so this 404 is sourced from the
    require_study_exists guard composed alongside it."""
    max_idx = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.study")
    resp = await ctx["wet"].get(URL_STUDY_BY_IDX.format(study_idx=max_idx + 100_000))
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

    resp = await ctx["user"].get(URL_STUDY_BY_IDX.format(study_idx=study_idx))
    assert resp.status_code == 403
    assert "'member'" in resp.json()["detail"]


async def test_get_study_sets_etag_header(ctx):
    """Tests the case where GET returns the row's ETag header so a client can
    feed it as the If-Match value on a subsequent PATCH (mirrors the biosample
    and sequenced-sample read handlers)."""
    create_resp = await _post_study(ctx["user"], ctx, title=_unique_title("get-etag"))
    assert create_resp.status_code == 201, create_resp.text
    study_idx = create_resp.json()["study_idx"]

    resp = await ctx["user"].get(URL_STUDY_BY_IDX.format(study_idx=study_idx))
    assert resp.status_code == 200, resp.text
    _assert_etag_quoted(resp)


# ===========================================================================
# PATCH /api/v1/study/{study_idx}
# ===========================================================================


def _assert_etag_quoted(resp) -> None:
    """Confirm the ETag header is present and wrapped in double quotes
    (RFC 7232 entity-tag grammar). The inside is opaque-by-contract."""
    etag = resp.headers.get("ETag")
    assert etag is not None and etag.startswith('"') and etag.endswith('"')


async def _post_study_owned_by_other(ctx) -> int:
    """Seed a study owned by a fresh user (not any of the role-keyed
    session principals), via wet_lab_admin on-behalf. Returns the
    study_idx; tracks the owner principal and the study for cleanup."""
    other_owner = await seed_user_principal(ctx["pool"], prefix=_SEED_PREFIX, suffix="patch-other")
    ctx["created"]["user_principals"].append(other_owner)
    resp = await _post_study(
        ctx["wet"],
        ctx,
        title=_unique_title("patch-other"),
        owner_idx=other_owner,
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["study_idx"]


async def test_patch_study_owner_self_patch_happy_path(ctx):
    """Tests the case where a study's owner PATCHes their own study via
    the owner-bypass path inside require_study_access (no explicit
    study_access row needed). Response is a full StudyResponse and only
    the targeted column changed."""
    create_resp = await _post_study(
        ctx["user"], ctx, title=_unique_title("patch-self"), alias="alias-pre"
    )
    assert create_resp.status_code == 201, create_resp.text
    posted = create_resp.json()
    study_idx = posted["study_idx"]
    if_match = await etag_for_row(ctx["pool"], table="study", row_idx=study_idx)

    resp = await ctx["user"].patch(
        URL_STUDY_BY_IDX.format(study_idx=study_idx),
        json={"alias": "alias-post"},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 200, resp.text
    _assert_etag_quoted(resp)
    rj = resp.json()
    expected = {**posted, "alias": "alias-post", "updated_at": rj["updated_at"]}
    assert rj == expected


async def test_patch_study_admin_tier_grant_happy_path(ctx):
    """Tests the case where a non-owner caller who holds an explicit
    Tier.ADMIN study_access grant patches the study successfully
    (Tier.ADMIN is the min_tier the route requires)."""
    study_idx = await _post_study_owned_by_other(ctx)
    await _grant_study_access(
        ctx,
        study_idx=study_idx,
        principal_idx=ctx["user_session"]["principal_idx"],
        tier="admin",
        granted_by_idx=ctx["wet_session"]["principal_idx"],
    )
    if_match = await etag_for_row(ctx["pool"], table="study", row_idx=study_idx)

    resp = await ctx["user"].patch(
        URL_STUDY_BY_IDX.format(study_idx=study_idx),
        json={"notes": "patched-by-tier-admin"},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["notes"] == "patched-by-tier-admin"


async def test_patch_study_wet_lab_admin_role_bypass(ctx):
    """Tests the case where a wet_lab_admin who has no study_access on
    the study patches successfully via the bypass_role path inside
    require_study_access (no DB access-tier lookup runs)."""
    study_idx = await _post_study_owned_by_other(ctx)
    if_match = await etag_for_row(ctx["pool"], table="study", row_idx=study_idx)

    resp = await ctx["wet"].patch(
        URL_STUDY_BY_IDX.format(study_idx=study_idx),
        json={"notes": "patched-by-wet"},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["notes"] == "patched-by-wet"


async def test_patch_study_etag_advances_on_ebi_accession_round_trip(ctx):
    """Tests the case where a PATCH writes a new ebi_study_accession
    and the response's ETag advances past the If-Match value (the
    study_set_updated_at trigger bumps updated_at on every UPDATE, and
    the route surfaces that bump as the new ETag)."""
    create_resp = await _post_study(ctx["user"], ctx, title=_unique_title("patch-etag"))
    assert create_resp.status_code == 201, create_resp.text
    study_idx = create_resp.json()["study_idx"]
    if_match = await etag_for_row(ctx["pool"], table="study", row_idx=study_idx)
    new_acc = f"ERP{secrets.token_hex(4)}"

    resp = await ctx["user"].patch(
        URL_STUDY_BY_IDX.format(study_idx=study_idx),
        json={"ebi_study_accession": new_acc},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 200, resp.text
    new_etag = resp.headers["ETag"]
    assert new_etag != if_match
    assert resp.json()["ebi_study_accession"] == new_acc


async def test_study_clear_submission_error_on_new_attempt_trigger(ctx):
    """Tests the case where bumping last_submission_at on a study row nulls a
    previously set submission_error via the shared clear-on-new-attempt
    trigger. Driven at the DB layer because the submission-tracking columns
    are subsystem-owned and are not on the study PATCH surface."""
    create_resp = await _post_study(ctx["user"], ctx, title=_unique_title("trig"))
    assert create_resp.status_code == 201, create_resp.text
    study_idx = create_resp.json()["study_idx"]
    pool = ctx["pool"]

    # Seed a submission_error without touching last_submission_at; the trigger
    # keys off last_submission_at, so it does not fire on this UPDATE.
    await pool.execute(
        "UPDATE qiita.study SET submission_error = $2 WHERE idx = $1",
        study_idx,
        "ENA timed out",
    )
    # Bump last_submission_at alone; the trigger fires and nulls the error.
    await pool.execute(
        "UPDATE qiita.study SET last_submission_at = $2 WHERE idx = $1",
        study_idx,
        datetime(2026, 2, 1, 8, 30, tzinfo=UTC),
    )

    row = await pool.fetchrow(
        "SELECT last_submission_at, submission_error FROM qiita.study WHERE idx = $1",
        study_idx,
    )
    assert row["last_submission_at"] is not None
    assert row["submission_error"] is None


@pytest.mark.parametrize(
    "field, value",
    [
        ("last_submission_at", "2026-02-01T08:30:00+00:00"),
        ("submission_error", "boom"),
    ],
)
async def test_patch_study_submission_field_forbidden_422(ctx, field, value):
    """Tests the case where a PATCH targets a submission-tracking column.
    These columns are subsystem-owned and absent from StudyPatchRequest, so
    extra='forbid' rejects the body with 422 — no owner or admin can write
    submission state through the (owner-accessible) study PATCH route."""
    create_resp = await _post_study(ctx["user"], ctx, title=_unique_title("patch-sub"))
    assert create_resp.status_code == 201, create_resp.text
    study_idx = create_resp.json()["study_idx"]
    if_match = await etag_for_row(ctx["pool"], table="study", row_idx=study_idx)

    resp = await ctx["user"].patch(
        URL_STUDY_BY_IDX.format(study_idx=study_idx),
        json={field: value},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 422


async def test_patch_study_anonymous_401(ctx):
    """Tests the case where the request carries no Authorization header.
    require_scope raises 401 before any DB lookup."""
    from qiita_control_plane.main import app

    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.patch(
            URL_STUDY_BY_IDX.format(study_idx=1),
            json={"alias": "x"},
            headers={"If-Match": '"unused"'},
        )
    assert resp.status_code == 401


async def test_patch_study_caller_without_study_write_scope_403(ctx, no_study_write_client):
    """Tests the case where the caller's PAT carries a scope set that
    excludes Scope.STUDY_WRITE. require_scope rejects with 403 before
    any access guard runs."""
    create_resp = await _post_study(ctx["user"], ctx, title=_unique_title("patch-no-scope"))
    assert create_resp.status_code == 201, create_resp.text
    study_idx = create_resp.json()["study_idx"]
    if_match = await etag_for_row(ctx["pool"], table="study", row_idx=study_idx)

    resp = await no_study_write_client.patch(
        URL_STUDY_BY_IDX.format(study_idx=study_idx),
        json={"alias": "x"},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 403
    assert "study:write" in resp.json()["detail"]


async def test_patch_study_caller_with_scope_without_tier_403(ctx):
    """Tests the case where a regular user holds Scope.STUDY_WRITE but
    is not the owner and has no Tier.ADMIN study_access grant on the
    study. require_study_access returns 403."""
    study_idx = await _post_study_owned_by_other(ctx)
    if_match = await etag_for_row(ctx["pool"], table="study", row_idx=study_idx)

    resp = await ctx["user"].patch(
        URL_STUDY_BY_IDX.format(study_idx=study_idx),
        json={"alias": "x"},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 403


async def test_patch_study_nonexistent_404(ctx):
    """Tests the case where a wet_lab_admin attempts to PATCH an idx
    past the highest existing study. require_study_access's bypass-role
    path returns without a DB lookup, so the 404 surfaces from the
    require_study_exists guard composed alongside it."""
    max_idx = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.study")
    resp = await ctx["wet"].patch(
        URL_STUDY_BY_IDX.format(study_idx=max_idx + 100_000),
        json={"alias": "x"},
        headers={"If-Match": '"unused"'},
    )
    assert resp.status_code == 404


async def test_patch_study_mismatched_if_match_412(ctx):
    """Tests the case where the caller sends an If-Match value that does
    not equal the row's current ETag (e.g., a stale value). The
    FOR UPDATE preflight returns 412 and no UPDATE runs."""
    create_resp = await _post_study(ctx["user"], ctx, title=_unique_title("patch-412"))
    assert create_resp.status_code == 201, create_resp.text
    study_idx = create_resp.json()["study_idx"]

    resp = await ctx["user"].patch(
        URL_STUDY_BY_IDX.format(study_idx=study_idx),
        json={"alias": "x"},
        headers={"If-Match": '"stale"'},
    )
    assert resp.status_code == 412
    assert resp.json()["detail"] == "If-Match did not match"


async def test_patch_study_missing_if_match_428(ctx):
    """Tests the case where the caller omits the If-Match header.
    require_if_match raises 428 once the auth and existence gates pass."""
    create_resp = await _post_study(ctx["user"], ctx, title=_unique_title("patch-428"))
    assert create_resp.status_code == 201, create_resp.text
    study_idx = create_resp.json()["study_idx"]

    resp = await ctx["user"].patch(
        URL_STUDY_BY_IDX.format(study_idx=study_idx), json={"alias": "x"}
    )
    assert resp.status_code == 428
    assert resp.json()["detail"] == "If-Match header required"


async def test_patch_study_empty_body_422(ctx):
    """Tests the case where the body is `{}`. PatchRequestModel's
    at_least_one_field validator rejects empty bodies with 422."""
    create_resp = await _post_study(ctx["user"], ctx, title=_unique_title("patch-empty"))
    assert create_resp.status_code == 201, create_resp.text
    study_idx = create_resp.json()["study_idx"]
    if_match = await etag_for_row(ctx["pool"], table="study", row_idx=study_idx)

    resp = await ctx["user"].patch(
        URL_STUDY_BY_IDX.format(study_idx=study_idx),
        json={},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 422


async def test_patch_study_extra_forbidden_field_422(ctx):
    """Tests the case where the body names a field outside
    StudyPatchRequest's field set (owner_idx is intentionally excluded
    from the patchable set). Pydantic extra='forbid' rejects with 422."""
    create_resp = await _post_study(ctx["user"], ctx, title=_unique_title("patch-extra"))
    assert create_resp.status_code == 201, create_resp.text
    study_idx = create_resp.json()["study_idx"]
    if_match = await etag_for_row(ctx["pool"], table="study", row_idx=study_idx)

    resp = await ctx["user"].patch(
        URL_STUDY_BY_IDX.format(study_idx=study_idx),
        json={"owner_idx": ctx["user_session"]["principal_idx"]},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 422


async def test_patch_study_explicit_null_title_422(ctx):
    """Tests the case where the caller sends `{"title": null}`. The
    shared NOT_NULL_FIELDS validator on PatchRequestModel rejects with
    422 since title backs a NOT NULL column."""
    create_resp = await _post_study(ctx["user"], ctx, title=_unique_title("patch-null"))
    assert create_resp.status_code == 201, create_resp.text
    study_idx = create_resp.json()["study_idx"]
    if_match = await etag_for_row(ctx["pool"], table="study", row_idx=study_idx)

    resp = await ctx["user"].patch(
        URL_STUDY_BY_IDX.format(study_idx=study_idx),
        json={"title": None},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 422
    assert "title" in resp.text


async def test_patch_study_empty_title_422(ctx):
    """Tests the case where the caller sends `{"title": ""}`. Pydantic
    min_length=1 on the field rejects with 422 before any DB work."""
    create_resp = await _post_study(ctx["user"], ctx, title=_unique_title("patch-empty-title"))
    assert create_resp.status_code == 201, create_resp.text
    study_idx = create_resp.json()["study_idx"]
    if_match = await etag_for_row(ctx["pool"], table="study", row_idx=study_idx)

    resp = await ctx["user"].patch(
        URL_STUDY_BY_IDX.format(study_idx=study_idx),
        json={"title": ""},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 422


async def test_patch_study_unknown_pi_idx_422(ctx):
    """Tests the case where a PATCH supplies a principal_investigator_idx
    past the highest existing principal. tg_principal_must_be_user
    fires (BEFORE-INSERT trigger ahead of the FK constraint); the route
    maps the RaiseError to 422 with the disambiguated PI message."""
    create_resp = await _post_study(ctx["user"], ctx, title=_unique_title("patch-bad-pi"))
    assert create_resp.status_code == 201, create_resp.text
    study_idx = create_resp.json()["study_idx"]
    if_match = await etag_for_row(ctx["pool"], table="study", row_idx=study_idx)
    max_idx = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.principal")

    resp = await ctx["user"].patch(
        URL_STUDY_BY_IDX.format(study_idx=study_idx),
        json={"principal_investigator_idx": max_idx + 100_000},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 422
    assert (
        resp.json()["detail"] == "principal_investigator_idx must reference a user-kind principal"
    )


async def test_patch_study_pi_is_service_account_422(ctx):
    """Tests the case where the candidate principal_investigator_idx
    points at a service-account-kind principal. tg_principal_must_be_user
    trips; the route maps it to 422 with the disambiguated PI message."""
    create_resp = await _post_study(ctx["user"], ctx, title=_unique_title("patch-pi-svc"))
    assert create_resp.status_code == 201, create_resp.text
    study_idx = create_resp.json()["study_idx"]
    if_match = await etag_for_row(ctx["pool"], table="study", row_idx=study_idx)
    svc_idx = await seed_service_principal(ctx["pool"], prefix=_SEED_PREFIX, suffix="patch-pi-svc")
    ctx["created"]["service_account_principals"].append(svc_idx)

    resp = await ctx["user"].patch(
        URL_STUDY_BY_IDX.format(study_idx=study_idx),
        json={"principal_investigator_idx": svc_idx},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 422
    assert (
        resp.json()["detail"] == "principal_investigator_idx must reference a user-kind principal"
    )


async def test_patch_study_duplicate_ebi_accession_409(ctx):
    """Tests the case where a PATCH would set ebi_study_accession to a
    value already held by another study. The
    study_ebi_study_accession_unique constraint fires; the route maps
    the unique-violation to 409 via the shared
    raise_for_unique_violation helper."""
    shared_accession = f"ERP{secrets.token_hex(4)}"
    first = await _post_study(
        ctx["user"], ctx, title=_unique_title("patch-dup-1"), ebi_study_accession=shared_accession
    )
    assert first.status_code == 201, first.text
    second = await _post_study(ctx["user"], ctx, title=_unique_title("patch-dup-2"))
    assert second.status_code == 201, second.text
    study_idx = second.json()["study_idx"]
    if_match = await etag_for_row(ctx["pool"], table="study", row_idx=study_idx)

    resp = await ctx["user"].patch(
        URL_STUDY_BY_IDX.format(study_idx=study_idx),
        json={"ebi_study_accession": shared_accession},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 409
    assert resp.json()["detail"] == "ebi_study_accession already in use"


# ===========================================================================
# POST /api/v1/study/lookup-by-accession — bulk accession → idx resolver
# ===========================================================================
# Tests below cover the resolved/missing wire shape, input-order dedup,
# the no-per-row-access-predicate security invariant (Scope.STUDY_READ
# resolves the idx; reading row contents still requires GET /study/{idx}),
# and the request-model rejection paths.


async def _create_study_with_accession(ctx, *, accession: str) -> int:
    """Create a study via POST carrying the given ebi_study_accession and
    return its idx; cleanup is handled by _post_study's tracker."""
    resp = await _post_study(
        ctx["user"],
        ctx,
        title=_unique_title("lookup"),
        ebi_study_accession=accession,
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["study_idx"]


async def test_lookup_study_by_accession_returns_resolved_map_and_missing_list(ctx):
    """Tests the case where the lookup body mixes seeded and absent
    accessions: `resolved` carries the hits and `missing` echoes the
    absent value in input order."""
    acc_a = unique_accession("ERP-LOOKUP-A")
    acc_b = unique_accession("ERP-LOOKUP-B")
    acc_missing = unique_accession("ERP-LOOKUP-MISS")
    study_a = await _create_study_with_accession(ctx, accession=acc_a)
    study_b = await _create_study_with_accession(ctx, accession=acc_b)

    resp = await ctx["user"].post(
        URL_STUDY_LOOKUP_BY_ACCESSION,
        json={"accessions": [acc_a, acc_b, acc_missing]},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "resolved": {acc_a: study_a, acc_b: study_b},
        "missing": [acc_missing],
    }


async def test_lookup_study_by_accession_dedups_input_preserving_order(ctx):
    """Tests the case where the body repeats accessions: each value is
    deduped and `missing` echoes the deduped values in first-occurrence
    order."""
    acc = unique_accession("ERP-LOOKUP-DUP")
    study_idx = await _create_study_with_accession(ctx, accession=acc)
    acc_miss = unique_accession("ERP-LOOKUP-DUP-MISS")

    resp = await ctx["user"].post(
        URL_STUDY_LOOKUP_BY_ACCESSION,
        json={"accessions": [acc_miss, acc, acc_miss, acc]},
    )
    assert resp.status_code == 200, resp.text
    # Input-order dedup: first occurrence wins.
    assert resp.json() == {"resolved": {acc: study_idx}, "missing": [acc_miss]}


async def test_lookup_study_by_accession_no_access_caller_still_resolves(ctx):
    """Tests the case where a regular user with no qiita.study_access row
    on a wet-lab-admin-owned study still gets the resolved idx — the
    lookup route runs no per-row access predicate (response carries only
    the natural-key → idx map; GET /study/{idx} still gates access)."""
    acc = unique_accession("ERP-LOOKUP-NOACC")
    # wet_lab_admin owns the study; the regular_user has no study_access row.
    create_resp = await _post_study(
        ctx["wet"],
        ctx,
        title=_unique_title("lookup-noacc"),
        ebi_study_accession=acc,
    )
    assert create_resp.status_code == 201, create_resp.text
    study_idx = create_resp.json()["study_idx"]

    resp = await ctx["user"].post(
        URL_STUDY_LOOKUP_BY_ACCESSION,
        json={"accessions": [acc]},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"resolved": {acc: study_idx}, "missing": []}


async def test_lookup_study_by_accession_anonymous_401(ctx):
    """Tests the case where the call carries no auth — require_human
    raises 401 before any DB work runs."""
    from qiita_control_plane.main import app

    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.post(
            URL_STUDY_LOOKUP_BY_ACCESSION,
            json={"accessions": ["ERP000001"]},
        )
    assert resp.status_code == 401


async def test_lookup_study_by_accession_missing_scope_403(ctx, no_study_read_client):
    """Tests the case where the caller's PAT scope set excludes
    Scope.STUDY_READ — require_scope raises 403 before any DB work."""
    resp = await no_study_read_client.post(
        URL_STUDY_LOOKUP_BY_ACCESSION,
        json={"accessions": ["ERP000001"]},
    )
    assert resp.status_code == 403
    assert "study:read" in resp.json()["detail"]


async def test_lookup_study_by_accession_rejects_empty_list_422(ctx):
    """Tests the case where `accessions` is an empty list — the request
    model's min_length=1 rejects it at the wire boundary with 422."""
    resp = await ctx["user"].post(
        URL_STUDY_LOOKUP_BY_ACCESSION,
        json={"accessions": []},
    )
    assert resp.status_code == 422


async def test_lookup_study_by_accession_rejects_empty_string_422(ctx):
    """Tests the case where any element of `accessions` is an empty
    string — the per-element min_length=1 rejects it at the wire boundary
    with 422."""
    resp = await ctx["user"].post(
        URL_STUDY_LOOKUP_BY_ACCESSION,
        json={"accessions": [""]},
    )
    assert resp.status_code == 422


async def test_lookup_study_by_accession_rejects_extra_field_422(ctx):
    """Tests the case where the body carries an unknown key —
    extra='forbid' rejects with 422 rather than silently dropping it."""
    resp = await ctx["user"].post(
        URL_STUDY_LOOKUP_BY_ACCESSION,
        json={"accessions": ["ERP000001"], "unknown": "x"},
    )
    assert resp.status_code == 422
