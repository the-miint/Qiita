"""Integration tests for the POST /api/v1/study/{study_idx}/biosample route.

Exercises wet_lab_admin and system_admin happy paths, the role / scope /
study-existence guards, the parametrised owner-eligibility 422 surface,
the metadata dict's validation 422s (unknown field, parse failure,
owner-id collision), Pydantic body validation, and DB-level
exception-mapping (409 / 422). Regular users are forbidden by the role
gate; one negative test covers it.
"""

import secrets
from datetime import date
from decimal import Decimal

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.auth_constants import Scope, SystemRole
from qiita_common.models import FieldDataType

from qiita_control_plane.testing.db_seeds import (
    SYSTEM_PRINCIPAL_IDX,
    retire_biosample,
    retire_biosample_to_study_link,
    seed_biosample,
    seed_biosample_global_field,
    seed_biosample_to_study_link,
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


_SEED_PREFIX = "bs-route"
_ELIGIBILITY_DETAIL = "owner is not eligible to own biosamples"


# ---------------------------------------------------------------------------
# Biosample-specific seed helpers and unique-name helpers
# ---------------------------------------------------------------------------


def _unique_field_name(prefix: str = "owner_bs_id") -> str:
    return f"{prefix}_{secrets.token_hex(4)}"


def _unique_accession(prefix: str = "BS") -> str:
    return f"{prefix}-{secrets.token_hex(4)}"


async def _seed_study(pool, *, owner_idx: int, suffix: str) -> int:
    return await pool.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        owner_idx,
        f"bs-route-study-{suffix}-{secrets.token_hex(4)}",
    )


async def _seed_metadata_checklist(pool, *, suffix: str) -> int:
    return await pool.fetchval(
        "INSERT INTO qiita.metadata_checklist (name) VALUES ($1) RETURNING idx",
        f"bs-route-cl-{suffix}-{secrets.token_hex(4)}",
    )


# ---------------------------------------------------------------------------
# FK-reverse cleanup
# ---------------------------------------------------------------------------


async def _cleanup_tracked(pool, created: dict) -> None:
    """Drop every row tracked in `created` in FK-reverse order.

    Ordering: biosample_metadata → biosample_study_field →
    biosample_global_field → biosample_to_study → biosample → study_access
    → study → metadata_checklist → user / service subtype rows → principal.
    The two principal-subtype lists are tracked separately because the FK
    from those subtype tables back to qiita.principal is ON DELETE RESTRICT,
    so the subtype row must go first.
    """
    await delete_idxs(pool, "biosample_metadata", created["biosample_metadata"])
    await delete_idxs(pool, "biosample_study_field", created["biosample_study_field"])
    await delete_idxs(pool, "biosample_global_field", created["biosample_global_field"])
    for bs, st in created["biosample_to_study"]:
        await pool.execute(
            "DELETE FROM qiita.biosample_to_study WHERE biosample_idx = $1 AND study_idx = $2",
            bs,
            st,
        )
    await delete_idxs(pool, "biosample", created["biosample"])
    for st, p in created["study_access"]:
        await pool.execute(
            "DELETE FROM qiita.study_access WHERE study_idx = $1 AND principal_idx = $2",
            st,
            p,
        )
    await delete_idxs(pool, "study", created["study"])
    await delete_idxs(pool, "metadata_checklist", created["metadata_checklist"])
    # api_token has an ON DELETE RESTRICT FK to qiita.principal, so any
    # tokens minted against per-test principals (currently only the
    # service-account PATCH test does so) must go before the principal
    # sweep. Tokens for the session-scoped fixtures never appear here
    # because their principals are never tracked in `created`.
    all_principals = created["user_principals"] + created["service_account_principals"]
    if all_principals:
        await pool.execute(
            "DELETE FROM qiita.api_token WHERE principal_idx = ANY($1::bigint[])",
            all_principals,
        )
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
    await delete_idxs(pool, "principal", all_principals)


# ---------------------------------------------------------------------------
# Per-test ctx fixture
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def ctx(role_keyed_clients):
    """Per-test fixture wrapping role_keyed_clients with a route-specific
    `created` tracker for FK-reverse cleanup at teardown.

    `created` lists are populated either by the test (for seeded support
    rows) or by `_post_biosample` (for rows the route created on success).
    """
    created: dict = {
        "biosample_metadata": [],
        "biosample_study_field": [],
        "biosample_global_field": [],
        "biosample_to_study": [],
        "biosample": [],
        "study_access": [],
        "study": [],
        "metadata_checklist": [],
        "user_principals": [],
        "service_account_principals": [],
    }
    yield {**role_keyed_clients, "created": created}
    await _cleanup_tracked(role_keyed_clients["pool"], created)


@pytest_asyncio.fixture
async def no_biosample_write_client(make_pat_client):
    """A regular_user PAT with a scope set that EXCLUDES Scope.BIOSAMPLE_WRITE —
    drives the require_scope guard's missing-scope 403."""
    return await make_pat_client(label="bs-no-write", scopes=[Scope.SELF_PROFILE])


# ---------------------------------------------------------------------------
# Route-call helper
# ---------------------------------------------------------------------------


async def _post_biosample(client, ctx, study_idx: int, **body):
    """POST the route and, on 201, track the created rows for FK-reverse cleanup.

    Looks up the owner-biosample-id metadata row by natural key after a
    successful create — the route returns the field idx and the biosample
    idx but not the metadata idx. Tests that supply a non-empty `metadata`
    dict must additionally call `_track_global_metadata_outputs` to pick up
    the globally-linked field rows and per-key metadata rows the route
    auto-creates; this helper only tracks the owner-id surface.
    """
    resp = await client.post(f"/api/v1/study/{study_idx}/biosample", json=body)
    if resp.status_code == 201:
        rj = resp.json()
        ctx["created"]["biosample"].append(rj["biosample_idx"])
        ctx["created"]["biosample_to_study"].append((rj["biosample_idx"], study_idx))
        if rj["biosample_study_field_created"]:
            ctx["created"]["biosample_study_field"].append(rj["biosample_study_field_idx"])
        meta_idx = await ctx["pool"].fetchval(
            "SELECT idx FROM qiita.biosample_metadata"
            " WHERE biosample_idx = $1 AND is_owner_biosample_id = true",
            rj["biosample_idx"],
        )
        if meta_idx is not None:
            ctx["created"]["biosample_metadata"].append(meta_idx)
    return resp


async def _track_global_metadata_outputs(ctx, bs_idx, study_idx, global_idxs):
    """Track globally-linked study fields (by global concept idx) and every
    non-owner-id metadata row written for this biosample. Use after
    `_post_biosample` in tests that exercised the metadata dict path so
    the FK-reverse cleanup picks the new rows up. Mirrors the sibling
    helper in tests/repositories/test_biosample.py so the two layers stay
    parallel.
    """
    # Pick up every globally-linked study field row at this study tied to
    # one of the supplied global concepts.
    rows = await ctx["pool"].fetch(
        "SELECT idx FROM qiita.biosample_study_field"
        " WHERE study_idx = $1 AND biosample_global_field_idx = ANY($2::bigint[])",
        study_idx,
        list(global_idxs),
    )
    for r in rows:
        if r["idx"] not in ctx["created"]["biosample_study_field"]:
            ctx["created"]["biosample_study_field"].append(r["idx"])

    # Pick up every non-owner-id metadata row for this biosample. The
    # owner-id row is already tracked by _post_biosample.
    meta_rows = await ctx["pool"].fetch(
        "SELECT idx FROM qiita.biosample_metadata"
        " WHERE biosample_idx = $1 AND is_owner_biosample_id = false",
        bs_idx,
    )
    for r in meta_rows:
        if r["idx"] not in ctx["created"]["biosample_metadata"]:
            ctx["created"]["biosample_metadata"].append(r["idx"])


# ===========================================================================
# Happy paths
# ===========================================================================


async def test_post_biosample_wet_lab_admin_self_owner(ctx):
    # Wet_lab_admin caller names themselves as the owner — eligibility
    # pre-flight runs against the caller's own principal_idx (caller is
    # profile-complete via require_complete_profile, so the lookup
    # passes) and the create succeeds.
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["wet_session"]["principal_idx"], suffix="wet-self"
    )
    ctx["created"]["study"].append(study_idx)

    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=_unique_field_name(),
        owner_biosample_id_value="WET-SELF-1",
    )
    assert resp.status_code == 201, resp.text
    rj = resp.json()
    expected = {
        # Auto-generated; copy actual into expected so the equality
        # confirms field presence without pinning the idx value.
        # The Field(gt=0) constraint on BiosampleImportResponse already
        # rejects a zero idx at the route boundary.
        "biosample_idx": rj["biosample_idx"],
        "biosample_study_field_idx": rj["biosample_study_field_idx"],
        "biosample_study_field_created": True,
    }
    assert rj == expected

    owner_idx = await ctx["pool"].fetchval(
        "SELECT owner_idx FROM qiita.biosample WHERE idx = $1", rj["biosample_idx"]
    )
    assert owner_idx == ctx["wet_session"]["principal_idx"]


async def test_post_biosample_wet_lab_admin_on_behalf_of_other_user(ctx):
    # Wet_lab_admin owns a study and creates a biosample for a separate user.
    # Lab-tech rule passes (caller is wet_lab_admin); eligibility passes
    # (target is profile-complete user).
    target_idx = await seed_user_principal(
        ctx["pool"], prefix=_SEED_PREFIX, suffix="wet-target"
    )
    ctx["created"]["user_principals"].append(target_idx)
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["wet_session"]["principal_idx"], suffix="wet"
    )
    ctx["created"]["study"].append(study_idx)

    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=target_idx,
        owner_biosample_id_field_name=_unique_field_name(),
        owner_biosample_id_value="WET-1",
    )
    assert resp.status_code == 201, resp.text

    owner_idx = await ctx["pool"].fetchval(
        "SELECT owner_idx FROM qiita.biosample WHERE idx = $1", resp.json()["biosample_idx"]
    )
    assert owner_idx == target_idx


async def test_post_biosample_system_admin_on_behalf_of_other_user(ctx):
    # System admin creates a biosample on behalf of a fresh user, on a
    # study they do own. Distinct from the bypass test below — here the
    # focus is the on-behalf rule satisfaction by system_admin role.
    target_idx = await seed_user_principal(
        ctx["pool"], prefix=_SEED_PREFIX, suffix="adm-target"
    )
    ctx["created"]["user_principals"].append(target_idx)
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["admin_session"]["principal_idx"], suffix="adm"
    )
    ctx["created"]["study"].append(study_idx)

    resp = await _post_biosample(
        ctx["admin"],
        ctx,
        study_idx,
        owner_idx=target_idx,
        owner_biosample_id_field_name=_unique_field_name(),
        owner_biosample_id_value="ADM-1",
    )
    assert resp.status_code == 201, resp.text


async def test_post_biosample_response_reports_field_created_flag_states(ctx):
    # Two consecutive POSTs with the same owner_biosample_id_field_name
    # must report biosample_study_field_created=True then False, with both
    # responses pointing at the same biosample_study_field_idx.
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["wet_session"]["principal_idx"], suffix="reuse"
    )
    ctx["created"]["study"].append(study_idx)

    field_name = _unique_field_name()
    r1 = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=field_name,
        owner_biosample_id_value="V-A",
    )
    r2 = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=field_name,
        owner_biosample_id_value="V-B",
    )
    assert r1.status_code == 201, r1.text
    assert r2.status_code == 201, r2.text
    rj1, rj2 = r1.json(), r2.json()
    assert rj1["biosample_study_field_created"] is True
    assert rj2["biosample_study_field_created"] is False
    assert rj1["biosample_study_field_idx"] == rj2["biosample_study_field_idx"]


# ===========================================================================
# Auth / scope / role guards
# ===========================================================================


async def test_post_biosample_regular_user_403(ctx):
    # Regular user (system_role=user) cannot create biosamples at all —
    # require_role_at_least(WET_LAB_ADMIN) rejects with 403.
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["user_session"]["principal_idx"], suffix="reg"
    )
    ctx["created"]["study"].append(study_idx)

    resp = await _post_biosample(
        ctx["user"],
        ctx,
        study_idx,
        owner_idx=ctx["user_session"]["principal_idx"],
        owner_biosample_id_field_name=_unique_field_name(),
        owner_biosample_id_value="X",
    )
    assert resp.status_code == 403
    assert "wet_lab_admin" in resp.json()["detail"]


async def test_post_biosample_anonymous_401(ctx):
    # No Authorization header → require_complete_profile chain raises 401.
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["wet_session"]["principal_idx"], suffix="anon"
    )
    ctx["created"]["study"].append(study_idx)

    from qiita_control_plane.main import app

    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.post(
            f"/api/v1/study/{study_idx}/biosample",
            json={
                "owner_idx": ctx["wet_session"]["principal_idx"],
                "owner_biosample_id_field_name": _unique_field_name(),
                "owner_biosample_id_value": "X",
            },
        )
    assert resp.status_code == 401


async def test_post_biosample_user_without_biosample_write_scope_403(
    ctx, no_biosample_write_client
):
    # Regular user holds a PAT that omits Scope.BIOSAMPLE_WRITE; require_scope
    # rejects with 403 before the role check runs.
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["wet_session"]["principal_idx"], suffix="noscope"
    )
    ctx["created"]["study"].append(study_idx)

    resp = await no_biosample_write_client.post(
        f"/api/v1/study/{study_idx}/biosample",
        json={
            "owner_idx": ctx["user_session"]["principal_idx"],
            "owner_biosample_id_field_name": _unique_field_name(),
            "owner_biosample_id_value": "X",
        },
    )
    assert resp.status_code == 403
    assert "biosample:write" in resp.json()["detail"]


# ===========================================================================
# Owner eligibility — collapsed 422 surface
# ===========================================================================
# All ineligibility cases collapse to one 422 detail by design (avoids leaking
# principal-state to callers probing arbitrary owner_idx values). Kept as one
# parametrised test so each kind still locks in that the matching backend
# code path emits 422 — a regression where one input accidentally yields
# 500 / 409 / 201 still surfaces here.


@pytest.mark.parametrize("kind", OWNER_INELIGIBILITY_KINDS)
async def test_post_biosample_owner_ineligibility_422(ctx, kind: IneligibilityKind):
    # The wet_lab_admin owns the study so the auth/role/study-existence
    # guards all pass; only the owner_idx eligibility branch trips.
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["wet_session"]["principal_idx"], suffix=f"elig-{kind}"
    )
    ctx["created"]["study"].append(study_idx)

    owner_idx = await resolve_ineligible_owner_idx(
        ctx["pool"],
        kind=kind,
        prefix=f"{_SEED_PREFIX}-elig",
        created=ctx["created"],
    )

    async def _post(idx: int):
        return await _post_biosample(
            ctx["wet"],
            ctx,
            study_idx,
            owner_idx=idx,
            owner_biosample_id_field_name=_unique_field_name(),
            owner_biosample_id_value="X",
        )

    await assert_owner_ineligibility_422(
        post_with_owner_idx=_post,
        expected_detail=_ELIGIBILITY_DETAIL,
        owner_idx=owner_idx,
    )


# ===========================================================================
# Path / data validation
# ===========================================================================


async def test_post_biosample_nonexistent_study_404(ctx):
    # Pick a study_idx well past the current MAX. require_study_exists's
    # fetch_study_exists returns False → 404.
    max_idx = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.study")
    resp = await ctx["wet"].post(
        f"/api/v1/study/{max_idx + 100_000}/biosample",
        json={
            "owner_idx": ctx["wet_session"]["principal_idx"],
            "owner_biosample_id_field_name": _unique_field_name(),
            "owner_biosample_id_value": "X",
        },
    )
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"]


async def test_post_biosample_empty_body_422(ctx):
    # Pydantic validation rejects {} because owner_idx,
    # owner_biosample_id_field_name, and owner_biosample_id_value are
    # all required.
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["wet_session"]["principal_idx"], suffix="empty"
    )
    ctx["created"]["study"].append(study_idx)

    resp = await ctx["wet"].post(f"/api/v1/study/{study_idx}/biosample", json={})
    assert resp.status_code == 422
    # The Pydantic missing-fields list must include owner_idx now that it
    # is required at the model level.
    missing_locs = {tuple(err["loc"]) for err in resp.json()["detail"]}
    assert ("body", "owner_idx") in missing_locs
    assert ("body", "owner_biosample_id_field_name") in missing_locs
    assert ("body", "owner_biosample_id_value") in missing_locs


async def test_post_biosample_empty_owner_biosample_id_field_name_422(ctx):
    # Pydantic min_length=1 rejects an empty string for the field-name field.
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["wet_session"]["principal_idx"], suffix="emptyname"
    )
    ctx["created"]["study"].append(study_idx)

    resp = await ctx["wet"].post(
        f"/api/v1/study/{study_idx}/biosample",
        json={
            "owner_idx": ctx["wet_session"]["principal_idx"],
            "owner_biosample_id_field_name": "",
            "owner_biosample_id_value": "X",
        },
    )
    assert resp.status_code == 422


# ===========================================================================
# Repository exception mapping
# ===========================================================================


async def test_post_biosample_duplicate_biosample_accession_409(ctx):
    # First POST claims an accession; the second POST tripping the same
    # biosample_accession_unique constraint must return 409 with the
    # mapped message.
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["wet_session"]["principal_idx"], suffix="dup-acc"
    )
    ctx["created"]["study"].append(study_idx)

    accession = _unique_accession("BS-DUP")
    r1 = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=_unique_field_name(),
        owner_biosample_id_value="V-1",
        biosample_accession=accession,
    )
    assert r1.status_code == 201, r1.text

    r2 = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=_unique_field_name(),
        owner_biosample_id_value="V-2",
        biosample_accession=accession,
    )
    assert r2.status_code == 409
    assert r2.json()["detail"] == "biosample_accession already in use"


async def test_post_biosample_bad_metadata_checklist_idx_422(ctx):
    # A metadata_checklist_idx far beyond the current MAX trips the
    # biosample_metadata_checklist_idx_fkey FK; route maps it to 422 with
    # the user-friendly mapped message.
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["wet_session"]["principal_idx"], suffix="bad-cl"
    )
    ctx["created"]["study"].append(study_idx)
    max_cl = await ctx["pool"].fetchval(
        "SELECT COALESCE(MAX(idx), 0) FROM qiita.metadata_checklist"
    )

    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=_unique_field_name(),
        owner_biosample_id_value="V-1",
        metadata_checklist_idx=max_cl + 100_000,
    )
    assert resp.status_code == 422
    expected_detail = "metadata_checklist_idx does not reference an existing checklist"
    assert resp.json()["detail"] == expected_detail


# ===========================================================================
# Metadata dict
# ===========================================================================


async def test_post_biosample_metadata_writes_global_fields(ctx):
    # Seed two global concepts (DATE and NUMERIC) and post metadata that
    # references both by display_name. Verify the route round-trips the
    # parsed values into the matching value_* columns and creates one
    # globally-linked study field per concept.
    suffix = secrets.token_hex(4)
    date_global = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"r_date_{suffix}",
        display_name=f"Collection Date {suffix}",
        data_type=FieldDataType.DATE,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    num_global = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"r_num_{suffix}",
        display_name=f"Latitude {suffix}",
        data_type=FieldDataType.NUMERIC,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"]["biosample_global_field"].extend([date_global, num_global])

    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["wet_session"]["principal_idx"], suffix="meta"
    )
    ctx["created"]["study"].append(study_idx)

    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=_unique_field_name(),
        owner_biosample_id_value="META-WRITE-1",
        metadata={
            f"Collection Date {suffix}": "2026-05-06",
            f"Latitude {suffix}": "32.7",
        },
    )
    assert resp.status_code == 201, resp.text
    bs_idx = resp.json()["biosample_idx"]
    await _track_global_metadata_outputs(ctx, bs_idx, study_idx, [date_global, num_global])

    # Verify the metadata rows landed with the correct typed values.
    rows = await ctx["pool"].fetch(
        "SELECT global_field_idx, value_text, value_numeric, value_date"
        " FROM qiita.biosample_metadata"
        " WHERE biosample_idx = $1 AND is_owner_biosample_id = false"
        " ORDER BY global_field_idx",
        bs_idx,
    )
    expected = sorted(
        [
            {
                "global_field_idx": date_global,
                "value_text": None,
                "value_numeric": None,
                "value_date": date(2026, 5, 6),
            },
            {
                "global_field_idx": num_global,
                "value_text": None,
                "value_numeric": Decimal("32.7"),
                "value_date": None,
            },
        ],
        key=lambda r: r["global_field_idx"],
    )
    assert [dict(r) for r in rows] == expected


async def test_post_biosample_metadata_unknown_field_422(ctx):
    # Two metadata keys that have no matching biosample_global_field row.
    # The route's BiosampleMetadataUnknownFieldsError handler must return
    # 422 with both unknown names listed.
    suffix = secrets.token_hex(4)
    unknown_a = f"Unknown A {suffix}"
    unknown_b = f"Unknown B {suffix}"
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["wet_session"]["principal_idx"], suffix="meta-unk"
    )
    ctx["created"]["study"].append(study_idx)

    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=_unique_field_name(),
        owner_biosample_id_value="META-UNK-1",
        metadata={unknown_a: "x", unknown_b: "y"},
    )
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert unknown_a in detail
    assert unknown_b in detail


@pytest.mark.parametrize(
    "data_type, bad_value",
    [
        (FieldDataType.NUMERIC, "not-a-number"),
        (FieldDataType.DATE, "not-a-date"),
    ],
)
async def test_post_biosample_metadata_unparseable_value_422(ctx, data_type, bad_value):
    # Seed a global field with the given data_type, then post a metadata
    # entry whose text value cannot be coerced to that type. The route
    # must return 422 with the failing field name in the detail.
    suffix = secrets.token_hex(4)
    display_name = f"Field {data_type} {suffix}"
    global_idx = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"r_unp_{suffix}",
        display_name=display_name,
        data_type=data_type,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"]["biosample_global_field"].append(global_idx)

    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["wet_session"]["principal_idx"], suffix="meta-bad"
    )
    ctx["created"]["study"].append(study_idx)

    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=_unique_field_name(),
        owner_biosample_id_value="META-BAD-1",
        metadata={display_name: bad_value},
    )
    assert resp.status_code == 422
    assert display_name in resp.json()["detail"]


async def test_post_biosample_metadata_owner_id_collision_422(ctx):
    # The metadata dict carries a key equal to owner_biosample_id_field_name.
    # The composer raises BiosampleOwnerIdFieldCollisionError pre-write; the
    # route maps it to 422 with the colliding name in the detail.
    shared_name = _unique_field_name("collide")
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["wet_session"]["principal_idx"], suffix="meta-coll"
    )
    ctx["created"]["study"].append(study_idx)

    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=shared_name,
        owner_biosample_id_value="META-COLL-1",
        metadata={shared_name: "x"},
    )
    assert resp.status_code == 422
    assert shared_name in resp.json()["detail"]
    assert "owner_biosample_id_field_name" in resp.json()["detail"]


async def test_post_biosample_metadata_uses_seeded_globals(ctx):
    # Realistic 6-field MIxS-style import that resolves global concepts
    # against the rows seeded by migration 20260501000014 instead of
    # creating throwaway concepts. Cleanup tracks only the per-test
    # study-field and metadata rows so the seeded globals survive.
    display_names = [
        "collection date",
        "geographic location (country and/or sea)",
        "geographic location (latitude)",
        "geographic location (longitude)",
        "broad-scale environmental context",
        "local environmental context",
    ]
    rows = await ctx["pool"].fetch(
        "SELECT idx, display_name FROM qiita.biosample_global_field"
        " WHERE display_name = ANY($1::text[])",
        display_names,
    )
    display_to_idx = {r["display_name"]: r["idx"] for r in rows}
    # Sanity-check: every seeded display_name resolved before we run the route.
    assert set(display_to_idx) == set(display_names)

    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["wet_session"]["principal_idx"], suffix="seeded-meta"
    )
    ctx["created"]["study"].append(study_idx)

    # Metadata keys mirror the seeded display_names exactly so the composer's
    # display_name → global_field_idx lookup hits all six rows.
    metadata = {
        "collection date": "2026-04-01",
        "geographic location (country and/or sea)": "USA",
        "geographic location (latitude)": "32.7157",
        "geographic location (longitude)": "-117.1611",
        "broad-scale environmental context": "human-associated habitat [ENVO:00009003]",
        "local environmental context": "gastrointestinal tract [ENVO:00002041]",
    }

    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name="sample_name",
        owner_biosample_id_value="s1-00023",
        metadata=metadata,
    )
    assert resp.status_code == 201, resp.text

    # Track per-test study-field and metadata rows for teardown; the seeded
    # biosample_global_field rows are intentionally NOT tracked here so the
    # cross-study seed survives this test.
    bs_idx = resp.json()["biosample_idx"]
    await _track_global_metadata_outputs(
        ctx, bs_idx, study_idx, list(display_to_idx.values())
    )

    # Verify every metadata row landed in the correct typed value_* column.
    rows = await ctx["pool"].fetch(
        "SELECT global_field_idx, value_text, value_numeric, value_date"
        " FROM qiita.biosample_metadata"
        " WHERE biosample_idx = $1 AND is_owner_biosample_id = false"
        " ORDER BY global_field_idx",
        bs_idx,
    )
    expected = sorted(
        [
            {
                "global_field_idx": display_to_idx["collection date"],
                "value_text": None,
                "value_numeric": None,
                "value_date": date(2026, 4, 1),
            },
            {
                "global_field_idx": display_to_idx[
                    "geographic location (country and/or sea)"
                ],
                "value_text": "USA",
                "value_numeric": None,
                "value_date": None,
            },
            {
                "global_field_idx": display_to_idx["geographic location (latitude)"],
                "value_text": None,
                "value_numeric": Decimal("32.7157"),
                "value_date": None,
            },
            {
                "global_field_idx": display_to_idx["geographic location (longitude)"],
                "value_text": None,
                "value_numeric": Decimal("-117.1611"),
                "value_date": None,
            },
            {
                "global_field_idx": display_to_idx["broad-scale environmental context"],
                "value_text": "human-associated habitat [ENVO:00009003]",
                "value_numeric": None,
                "value_date": None,
            },
            {
                "global_field_idx": display_to_idx["local environmental context"],
                "value_text": "gastrointestinal tract [ENVO:00002041]",
                "value_numeric": None,
                "value_date": None,
            },
        ],
        key=lambda r: r["global_field_idx"],
    )
    assert [dict(r) for r in rows] == expected


# ===========================================================================
# GET /api/v1/study/{study_idx}/biosample/list-idxs
# ===========================================================================
#
# Read endpoint. Tests cover the auth/access matrix (anonymous, missing
# scope, no access, viewer access, admin role bypass), the
# require_study_exists+require_study_access composition (admin still gets
# 404 on a non-existent study_idx), and the row-level filters (retired
# link / retired biosample excluded).


@pytest_asyncio.fixture
async def no_study_read_client(make_pat_client):
    """A regular_user PAT with a scope set that EXCLUDES Scope.STUDY_READ —
    drives the require_scope guard's missing-scope 403 on the list-idxs route."""
    return await make_pat_client(label="bs-list-no-read", scopes=[Scope.SELF_PROFILE])


async def _seed_link_to_study(ctx, *, study_idx, owner_idx):
    """Seed a biosample owned by `owner_idx`, link it to `study_idx`, and
    track both rows in `ctx['created']` for FK-reverse cleanup. Wraps the
    db_seeds primitives so the per-test setup stays a single line."""
    bs_idx = await seed_biosample(
        ctx["pool"], owner_idx=owner_idx, created_by_idx=owner_idx
    )
    ctx["created"]["biosample"].append(bs_idx)
    await seed_biosample_to_study_link(
        ctx["pool"],
        biosample_idx=bs_idx,
        study_idx=study_idx,
        created_by_idx=owner_idx,
    )
    ctx["created"]["biosample_to_study"].append((bs_idx, study_idx))
    return bs_idx


async def _grant_study_access(ctx, *, study_idx, principal_idx, tier, granted_by_idx):
    """Insert a study_access row at the named tier; track for cleanup."""
    await ctx["pool"].execute(
        "INSERT INTO qiita.study_access (study_idx, principal_idx, access_tier, granted_by_idx)"
        " VALUES ($1, $2, $3::qiita.tier, $4)",
        study_idx,
        principal_idx,
        tier,
        granted_by_idx,
    )
    ctx["created"]["study_access"].append((study_idx, principal_idx))


async def test_list_biosample_idxs_anonymous_401(ctx):
    # No Authorization header → require_human chain raises 401.
    from qiita_control_plane.main import app

    app.state.pool = ctx["pool"]
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["user_session"]["principal_idx"], suffix="anon"
    )
    ctx["created"]["study"].append(study_idx)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.get(f"/api/v1/study/{study_idx}/biosample/list-idxs")
    assert resp.status_code == 401


async def test_list_biosample_idxs_missing_scope_403(ctx, no_study_read_client):
    # A regular_user PAT that omits Scope.STUDY_READ is rejected by
    # require_scope before the access-tier check runs.
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["user_session"]["principal_idx"], suffix="no-scope"
    )
    ctx["created"]["study"].append(study_idx)
    resp = await no_study_read_client.get(f"/api/v1/study/{study_idx}/biosample/list-idxs")
    assert resp.status_code == 403
    assert "study:read" in resp.json()["detail"]


async def test_list_biosample_idxs_nonexistent_study_regular_user_404(ctx):
    # require_study_exists fires before require_study_access for a regular
    # user, so a study_idx past the highest existing study returns 404.
    max_idx = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.study")
    resp = await ctx["user"].get(f"/api/v1/study/{max_idx + 100_000}/biosample/list-idxs")
    assert resp.status_code == 404


async def test_list_biosample_idxs_nonexistent_study_admin_404(ctx):
    # Even with the wet_lab_admin role bypass on require_study_access,
    # require_study_exists still surfaces 404 — the route composes both
    # so admin-bypass callers do not silently get an empty list for a
    # non-existent study.
    max_idx = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.study")
    resp = await ctx["admin"].get(f"/api/v1/study/{max_idx + 100_000}/biosample/list-idxs")
    assert resp.status_code == 404


async def test_list_biosample_idxs_no_access_403(ctx):
    # Regular user is neither owner nor study_access row holder; effective
    # tier is public-by-absence, below the route's viewer minimum → 403.
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["admin_session"]["principal_idx"], suffix="no-access"
    )
    ctx["created"]["study"].append(study_idx)
    resp = await ctx["user"].get(f"/api/v1/study/{study_idx}/biosample/list-idxs")
    assert resp.status_code == 403


async def test_list_biosample_idxs_owner_returns_payload(ctx):
    # Study owner bypasses the tier comparison; the response carries the
    # documented envelope with the regular-user system_role.
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["user_session"]["principal_idx"], suffix="owner"
    )
    ctx["created"]["study"].append(study_idx)
    bs_idxs = [
        await _seed_link_to_study(
            ctx,
            study_idx=study_idx,
            owner_idx=ctx["user_session"]["principal_idx"],
        )
        for _ in range(2)
    ]

    resp = await ctx["user"].get(f"/api/v1/study/{study_idx}/biosample/list-idxs")
    assert resp.status_code == 200, resp.text
    expected = {
        "biosample_idxs": list(reversed(bs_idxs)),
        "count": 2,
        "truncated": False,
        "caller_system_role": "user",
    }
    assert resp.json() == expected


async def test_list_biosample_idxs_viewer_access_returns_payload(ctx):
    # Regular user with an explicit viewer-tier study_access row passes
    # the tier check (viewer >= viewer); the empty study returns the
    # zero-row envelope.
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["admin_session"]["principal_idx"], suffix="viewer"
    )
    ctx["created"]["study"].append(study_idx)
    await _grant_study_access(
        ctx,
        study_idx=study_idx,
        principal_idx=ctx["user_session"]["principal_idx"],
        tier="viewer",
        granted_by_idx=ctx["admin_session"]["principal_idx"],
    )

    resp = await ctx["user"].get(f"/api/v1/study/{study_idx}/biosample/list-idxs")
    assert resp.status_code == 200, resp.text
    expected = {
        "biosample_idxs": [],
        "count": 0,
        "truncated": False,
        "caller_system_role": "user",
    }
    assert resp.json() == expected


async def test_list_biosample_idxs_wet_lab_admin_bypasses_access(ctx):
    # The wet_lab_admin role bypasses require_study_access without a
    # study_access row, and `caller_system_role` reflects the caller's
    # actual role.
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["admin_session"]["principal_idx"], suffix="wet-bypass"
    )
    ctx["created"]["study"].append(study_idx)
    bs_idx = await _seed_link_to_study(
        ctx,
        study_idx=study_idx,
        owner_idx=ctx["admin_session"]["principal_idx"],
    )

    resp = await ctx["wet"].get(f"/api/v1/study/{study_idx}/biosample/list-idxs")
    assert resp.status_code == 200, resp.text
    expected = {
        "biosample_idxs": [bs_idx],
        "count": 1,
        "truncated": False,
        "caller_system_role": "wet_lab_admin",
    }
    assert resp.json() == expected


async def test_list_biosample_idxs_system_admin_bypasses_access(ctx):
    # The system_admin role also bypasses require_study_access; the
    # caller_system_role reflects the actual database value.
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["wet_session"]["principal_idx"], suffix="adm-bypass"
    )
    ctx["created"]["study"].append(study_idx)

    resp = await ctx["admin"].get(f"/api/v1/study/{study_idx}/biosample/list-idxs")
    assert resp.status_code == 200, resp.text
    expected = {
        "biosample_idxs": [],
        "count": 0,
        "truncated": False,
        "caller_system_role": "system_admin",
    }
    assert resp.json() == expected


async def test_list_biosample_idxs_excludes_retired_link_and_retired_biosample(ctx):
    # Three links: one active, one with the link retired, one with the
    # underlying biosample retired entity-wide. Only the active row
    # surfaces.
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["user_session"]["principal_idx"], suffix="retired"
    )
    ctx["created"]["study"].append(study_idx)
    owner_idx = ctx["user_session"]["principal_idx"]
    active_idx = await _seed_link_to_study(ctx, study_idx=study_idx, owner_idx=owner_idx)
    retired_link_idx = await _seed_link_to_study(
        ctx, study_idx=study_idx, owner_idx=owner_idx
    )
    retired_bs_idx = await _seed_link_to_study(
        ctx, study_idx=study_idx, owner_idx=owner_idx
    )
    await retire_biosample_to_study_link(
        ctx["pool"],
        biosample_idx=retired_link_idx,
        study_idx=study_idx,
        retired_by_idx=owner_idx,
    )
    await retire_biosample(
        ctx["pool"],
        biosample_idx=retired_bs_idx,
        retired_by_idx=owner_idx,
    )

    resp = await ctx["user"].get(f"/api/v1/study/{study_idx}/biosample/list-idxs")
    assert resp.status_code == 200, resp.text
    expected = {
        "biosample_idxs": [active_idx],
        "count": 1,
        "truncated": False,
        "caller_system_role": "user",
    }
    assert resp.json() == expected


# ===========================================================================
# Single-biosample GET — /api/v1/biosample/{biosample_idx}
# ===========================================================================
#
# Access policy: wet_lab_admin and system_admin bypass; otherwise the caller
# must be the biosample's owner OR have a qiita.study_access row on a
# non-retired biosample_to_study link. Retired biosamples 404 unconditionally
# until the planned wet_lab_admin retired-retrieval surface lands.


@pytest_asyncio.fixture
async def no_biosample_read_client(make_pat_client):
    """A regular_user PAT with a scope set that EXCLUDES Scope.BIOSAMPLE_READ —
    drives the require_scope guard's missing-scope 403 on the GET route."""
    return await make_pat_client(label="bs-no-read", scopes=[Scope.SELF_PROFILE])


def _assert_etag_quoted(resp) -> None:
    """Confirm the ETag header is present and wrapped in double quotes
    per Decision 3. Format inside the quotes is opaque-by-contract."""
    etag = resp.headers.get("ETag")
    assert etag is not None and etag.startswith('"') and etag.endswith('"')


async def test_get_biosample_owner_returns_response(ctx):
    # The regular-user client (system_role=user) reads a biosample whose
    # owner_idx matches its own principal_idx. Caller's role is below
    # the wet_lab_admin bypass threshold, so access must be granted by
    # fetch_caller_has_biosample_access; no biosample_to_study link or
    # study_access row is seeded, so the predicate's only path to True
    # is the owner branch (caller.principal_idx == biosample.owner_idx).
    owner_idx = ctx["user_session"]["principal_idx"]
    bs_idx = await seed_biosample(
        ctx["pool"], owner_idx=owner_idx, created_by_idx=owner_idx
    )
    ctx["created"]["biosample"].append(bs_idx)

    resp = await ctx["user"].get(f"/api/v1/biosample/{bs_idx}")
    assert resp.status_code == 200, resp.text
    _assert_etag_quoted(resp)

    rj = resp.json()
    expected = {
        "biosample_idx": bs_idx,
        "owner_idx": owner_idx,
        "metadata_checklist_idx": None,
        "biosample_accession": None,
        "ena_sample_accession": None,
        "last_submission_at": None,
        "submission_error": None,
        "last_metadata_change_at": None,
        "created_by_idx": owner_idx,
        # Auto-generated by the DB; copy actual values into expected so
        # the equality confirms field presence without pinning timestamps.
        "created_at": rj["created_at"],
        "updated_at": rj["updated_at"],
        "retired": False,
        "retired_by_idx": None,
        "retired_at": None,
        "retire_reason": None,
        "global_metadata": {},
        "caller_system_role": "user",
    }
    assert rj == expected


async def test_get_biosample_via_study_access_returns_response(ctx):
    # The caller is not the owner; access comes through a study_access row
    # on the (active) biosample_to_study link. Grant viewer tier so the
    # repo predicate's "any study_access row" check is satisfied.
    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["wet_session"]["principal_idx"], suffix="get-via-access"
    )
    ctx["created"]["study"].append(study_idx)
    bs_idx = await _seed_link_to_study(
        ctx, study_idx=study_idx, owner_idx=ctx["wet_session"]["principal_idx"]
    )
    await _grant_study_access(
        ctx,
        study_idx=study_idx,
        principal_idx=ctx["user_session"]["principal_idx"],
        tier="viewer",
        granted_by_idx=ctx["wet_session"]["principal_idx"],
    )

    resp = await ctx["user"].get(f"/api/v1/biosample/{bs_idx}")
    assert resp.status_code == 200, resp.text
    _assert_etag_quoted(resp)

    rj = resp.json()
    wet_idx = ctx["wet_session"]["principal_idx"]
    expected = {
        "biosample_idx": bs_idx,
        "owner_idx": wet_idx,
        "metadata_checklist_idx": None,
        "biosample_accession": None,
        "ena_sample_accession": None,
        "last_submission_at": None,
        "submission_error": None,
        "last_metadata_change_at": None,
        "created_by_idx": wet_idx,
        # Auto-generated; copy actual into expected so the equality
        # confirms field presence without pinning timestamps.
        "created_at": rj["created_at"],
        "updated_at": rj["updated_at"],
        "retired": False,
        "retired_by_idx": None,
        "retired_at": None,
        "retire_reason": None,
        "global_metadata": {},
        "caller_system_role": "user",
    }
    assert rj == expected


async def test_get_biosample_wet_lab_admin_bypasses_access(ctx):
    # wet_lab_admin reads a biosample they have no owner relationship to
    # and no study_access row on. Role bypass alone authorizes the read,
    # and caller_system_role reflects the actual database value.
    owner_idx = ctx["user_session"]["principal_idx"]
    bs_idx = await seed_biosample(
        ctx["pool"], owner_idx=owner_idx, created_by_idx=owner_idx
    )
    ctx["created"]["biosample"].append(bs_idx)

    resp = await ctx["wet"].get(f"/api/v1/biosample/{bs_idx}")
    assert resp.status_code == 200, resp.text
    _assert_etag_quoted(resp)

    rj = resp.json()
    expected = {
        "biosample_idx": bs_idx,
        "owner_idx": owner_idx,
        "metadata_checklist_idx": None,
        "biosample_accession": None,
        "ena_sample_accession": None,
        "last_submission_at": None,
        "submission_error": None,
        "last_metadata_change_at": None,
        "created_by_idx": owner_idx,
        # Auto-generated; copy actual into expected so the equality
        # confirms field presence without pinning timestamps.
        "created_at": rj["created_at"],
        "updated_at": rj["updated_at"],
        "retired": False,
        "retired_by_idx": None,
        "retired_at": None,
        "retire_reason": None,
        "global_metadata": {},
        "caller_system_role": "wet_lab_admin",
    }
    assert rj == expected


async def test_get_biosample_system_admin_bypasses_access(ctx):
    # system_admin reads a biosample with no owner / access path.
    owner_idx = ctx["user_session"]["principal_idx"]
    bs_idx = await seed_biosample(
        ctx["pool"], owner_idx=owner_idx, created_by_idx=owner_idx
    )
    ctx["created"]["biosample"].append(bs_idx)

    resp = await ctx["admin"].get(f"/api/v1/biosample/{bs_idx}")
    assert resp.status_code == 200, resp.text
    _assert_etag_quoted(resp)

    rj = resp.json()
    expected = {
        "biosample_idx": bs_idx,
        "owner_idx": owner_idx,
        "metadata_checklist_idx": None,
        "biosample_accession": None,
        "ena_sample_accession": None,
        "last_submission_at": None,
        "submission_error": None,
        "last_metadata_change_at": None,
        "created_by_idx": owner_idx,
        # Auto-generated; copy actual into expected so the equality
        # confirms field presence without pinning timestamps.
        "created_at": rj["created_at"],
        "updated_at": rj["updated_at"],
        "retired": False,
        "retired_by_idx": None,
        "retired_at": None,
        "retire_reason": None,
        "global_metadata": {},
        "caller_system_role": "system_admin",
    }
    assert rj == expected


async def test_get_biosample_anonymous_401(ctx):
    # No Authorization header → require_human raises 401.
    owner_idx = ctx["user_session"]["principal_idx"]
    bs_idx = await seed_biosample(
        ctx["pool"], owner_idx=owner_idx, created_by_idx=owner_idx
    )
    ctx["created"]["biosample"].append(bs_idx)

    from qiita_control_plane.main import app

    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.get(f"/api/v1/biosample/{bs_idx}")
    assert resp.status_code == 401


async def test_get_biosample_missing_scope_403(ctx, no_biosample_read_client):
    # Regular user holds a PAT that omits Scope.BIOSAMPLE_READ; require_scope
    # rejects with 403 before any DB read runs.
    owner_idx = ctx["user_session"]["principal_idx"]
    bs_idx = await seed_biosample(
        ctx["pool"], owner_idx=owner_idx, created_by_idx=owner_idx
    )
    ctx["created"]["biosample"].append(bs_idx)

    resp = await no_biosample_read_client.get(f"/api/v1/biosample/{bs_idx}")
    assert resp.status_code == 403
    assert "biosample:read" in resp.json()["detail"]


async def test_get_biosample_no_access_403(ctx):
    # The regular user is not the biosample's owner and has no study_access
    # row on any non-retired link. The repo predicate returns False, the
    # route surfaces 403 (existence is already revealed by the prior 404
    # path being skipped — admin-bypass and tier-mismatch share the 403
    # spelling per project conventions).
    owner_idx = ctx["wet_session"]["principal_idx"]
    bs_idx = await seed_biosample(
        ctx["pool"], owner_idx=owner_idx, created_by_idx=owner_idx
    )
    ctx["created"]["biosample"].append(bs_idx)

    resp = await ctx["user"].get(f"/api/v1/biosample/{bs_idx}")
    assert resp.status_code == 403
    assert "no read path" in resp.json()["detail"]


async def test_get_biosample_nonexistent_404(ctx):
    # An idx well past MAX → fetch_biosample returns None → 404 before the
    # access predicate runs.
    max_idx = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.biosample")
    resp = await ctx["wet"].get(f"/api/v1/biosample/{max_idx + 100_000}")
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"]


async def test_get_biosample_retired_404_even_for_wet_lab_admin(ctx):
    # Retired biosamples 404 unconditionally for now, even for wet_lab_admin.
    # This pins the carve-out documented in the route docstring; once the
    # planned retired-retrieval surface lands, this test will be relaxed
    # for wet_lab_admin and a parallel test pinned for the non-admin path.
    owner_idx = ctx["user_session"]["principal_idx"]
    bs_idx = await seed_biosample(
        ctx["pool"], owner_idx=owner_idx, created_by_idx=owner_idx
    )
    ctx["created"]["biosample"].append(bs_idx)
    await retire_biosample(ctx["pool"], biosample_idx=bs_idx, retired_by_idx=owner_idx)

    resp = await ctx["wet"].get(f"/api/v1/biosample/{bs_idx}")
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"]


async def test_get_biosample_returns_only_global_metadata(ctx):
    # Round-trip the import POST through the GET to verify the GET surfaces
    # the globally-linked entry but not the purely-local owner-biosample-id
    # row. The POST writes one global metadata row + one owner-id row;
    # the GET response's global_metadata dict must contain only the global
    # entry, keyed on the field's internal_name.
    suffix = secrets.token_hex(4)
    internal_name = f"host_subject_id_{suffix}"
    display_name = f"Host Subject ID {suffix}"

    global_idx = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=internal_name,
        display_name=display_name,
        data_type=FieldDataType.TEXT,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"]["biosample_global_field"].append(global_idx)
    # Patch the description column directly; the seed helper carries only
    # the columns the import surface needs.
    await ctx["pool"].execute(
        "UPDATE qiita.biosample_global_field SET description = $2 WHERE idx = $1",
        global_idx,
        "Host's stable identifier",
    )

    study_idx = await _seed_study(
        ctx["pool"], owner_idx=ctx["wet_session"]["principal_idx"], suffix="get-md"
    )
    ctx["created"]["study"].append(study_idx)

    post_resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=_unique_field_name(),
        owner_biosample_id_value="GET-MD-1",
        metadata={display_name: "HOST-99"},
    )
    assert post_resp.status_code == 201, post_resp.text
    bs_idx = post_resp.json()["biosample_idx"]
    await _track_global_metadata_outputs(ctx, bs_idx, study_idx, [global_idx])

    resp = await ctx["wet"].get(f"/api/v1/biosample/{bs_idx}")
    assert resp.status_code == 200, resp.text
    rj = resp.json()
    expected_metadata = {
        internal_name: {
            "display_name": display_name,
            "description": "Host's stable identifier",
            "data_type": "text",
            "value": "HOST-99",
        }
    }
    assert rj["global_metadata"] == expected_metadata


# ===========================================================================
# PATCH /api/v1/biosample/{biosample_idx}
# ===========================================================================


@pytest_asyncio.fixture
async def no_biosample_write_patch_client(make_pat_client):
    """A regular_user PAT with a scope set that EXCLUDES Scope.BIOSAMPLE_WRITE
    — drives the require_scope guard's missing-scope 403 on the PATCH route."""
    return await make_pat_client(label="bs-patch-no-write", scopes=[Scope.SELF_PROFILE])


async def _etag_for(pool, bs_idx: int) -> str:
    """Build the quoted ISO-8601 ETag the route emits for a biosample row.

    Reads updated_at directly so the helper does not depend on the GET
    route's behavior; matches `_etag_for_updated_at` in routes/biosample.py.
    """
    updated_at = await pool.fetchval(
        "SELECT updated_at FROM qiita.biosample WHERE idx = $1", bs_idx
    )
    return f'"{updated_at.isoformat()}"'


async def _seed_biosample_for_patch(ctx) -> int:
    """Seed a wet_lab_admin-owned biosample with a known accession; track for cleanup.

    Returns the biosample idx. Caller pre-loads any field they want to test
    PATCHing later; the seed gives the row enough columns to round-trip.
    """
    owner_idx = ctx["wet_session"]["principal_idx"]
    bs_idx = await seed_biosample(
        ctx["pool"], owner_idx=owner_idx, created_by_idx=owner_idx
    )
    ctx["created"]["biosample"].append(bs_idx)
    return bs_idx


async def test_patch_biosample_wet_lab_admin_happy_path(ctx):
    # Wet_lab_admin patches a single column; response shape mirrors the
    # GET route's BiosampleResponse, ETag header is set, and the
    # full-object equality confirms only the targeted column changed.
    bs_idx = await _seed_biosample_for_patch(ctx)
    if_match = await _etag_for(ctx["pool"], bs_idx)
    new_acc = _unique_accession("PATCH-OK")

    resp = await ctx["wet"].patch(
        f"/api/v1/biosample/{bs_idx}",
        json={"biosample_accession": new_acc},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 200, resp.text
    _assert_etag_quoted(resp)

    rj = resp.json()
    owner_idx = ctx["wet_session"]["principal_idx"]
    expected = {
        "biosample_idx": bs_idx,
        "owner_idx": owner_idx,
        "metadata_checklist_idx": None,
        "biosample_accession": new_acc,
        "ena_sample_accession": None,
        "last_submission_at": None,
        "submission_error": None,
        "last_metadata_change_at": None,
        "created_by_idx": owner_idx,
        # Auto-generated; copy actual into expected so equality confirms
        # field presence without pinning timestamps.
        "created_at": rj["created_at"],
        "updated_at": rj["updated_at"],
        "retired": False,
        "retired_by_idx": None,
        "retired_at": None,
        "retire_reason": None,
        "global_metadata": {},
        "caller_system_role": "wet_lab_admin",
    }
    assert rj == expected


async def test_patch_biosample_explicit_null_clears_field(ctx):
    # Seed a biosample with metadata_checklist_idx set, then PATCH it to
    # explicit null. The model_fields_set distinction (absent vs. null)
    # is what makes this clear-the-column path work.
    checklist_idx = await _seed_metadata_checklist(ctx["pool"], suffix="patch-clear")
    ctx["created"]["metadata_checklist"].append(checklist_idx)
    owner_idx = ctx["wet_session"]["principal_idx"]
    bs_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.biosample (owner_idx, created_by_idx, metadata_checklist_idx)"
        " VALUES ($1, $1, $2) RETURNING idx",
        owner_idx,
        checklist_idx,
    )
    ctx["created"]["biosample"].append(bs_idx)
    if_match = await _etag_for(ctx["pool"], bs_idx)

    resp = await ctx["wet"].patch(
        f"/api/v1/biosample/{bs_idx}",
        json={"metadata_checklist_idx": None},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["metadata_checklist_idx"] is None


async def test_patch_biosample_etag_advances(ctx):
    # The response's ETag header must differ from the request's If-Match
    # value: the schema's biosample_set_updated_at trigger bumps
    # updated_at, and the route surfaces that bump in the new ETag.
    bs_idx = await _seed_biosample_for_patch(ctx)
    if_match = await _etag_for(ctx["pool"], bs_idx)

    resp = await ctx["wet"].patch(
        f"/api/v1/biosample/{bs_idx}",
        json={"submission_error": "transient"},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 200, resp.text
    new_etag = resp.headers["ETag"]
    assert new_etag != if_match
    assert new_etag.startswith('"') and new_etag.endswith('"')


async def test_patch_biosample_service_account_403_pending_restructure(ctx):
    # Pin the deferred-behavior contract: a service-account caller, even
    # one whose qiita.principal.system_role is wet_lab_admin and whose
    # PAT carries Scope.BIOSAMPLE_WRITE, currently 403s here.
    # require_role_at_least's ServiceAccount-always-fails rule (auth
    # model treats service-account authz as scope-only;
    # auth/principal.py ServiceAccount carries no system_role field) is
    # what produces the 403. The NCBI / ENA submission subsystem will
    # need a separate scope-gated surface, OR the auth model will need
    # to widen so ServiceAccount carries a role, before this PATCH can
    # accept service-account writes. When that work lands, this test
    # should flip to assert 200 (or be replaced by a happy-path test
    # against the new surface).
    suffix = secrets.token_hex(4)
    svc_name = f"bs-patch-svc-{suffix}"
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            svc_principal_idx = await conn.fetchval(
                "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
                " VALUES ($1, $2, 1) RETURNING idx",
                svc_name,
                SystemRole.WET_LAB_ADMIN,
            )
            await conn.execute(
                "INSERT INTO qiita.service_account (principal_idx, name) VALUES ($1, $2)",
                svc_principal_idx,
                svc_name,
            )
    ctx["created"]["service_account_principals"].append(svc_principal_idx)

    from qiita_control_plane.auth.token import mint_api_token
    from qiita_control_plane.main import app

    plaintext, _ = await mint_api_token(
        ctx["pool"],
        principal_idx=svc_principal_idx,
        label=f"svc-{suffix}",
        scopes=[Scope.BIOSAMPLE_WRITE],
    )

    bs_idx = await _seed_biosample_for_patch(ctx)
    if_match = await _etag_for(ctx["pool"], bs_idx)

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {plaintext}"},
    ) as svc:
        resp = await svc.patch(
            f"/api/v1/biosample/{bs_idx}",
            json={"submission_error": "subsystem-recorded"},
            headers={"If-Match": if_match},
        )
    assert resp.status_code == 403, resp.text
    assert "wet_lab_admin" in resp.json()["detail"]


async def test_patch_biosample_anonymous_401(ctx):
    # No Authorization header → require_role_at_least chain raises 401.
    bs_idx = await _seed_biosample_for_patch(ctx)
    if_match = await _etag_for(ctx["pool"], bs_idx)

    from qiita_control_plane.main import app

    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.patch(
            f"/api/v1/biosample/{bs_idx}",
            json={"submission_error": "x"},
            headers={"If-Match": if_match},
        )
    assert resp.status_code == 401


async def test_patch_biosample_regular_user_403(ctx):
    # Regular user (system_role=user) holds BIOSAMPLE_WRITE but is below
    # the wet_lab_admin role bar; require_role_at_least returns 403.
    bs_idx = await _seed_biosample_for_patch(ctx)
    if_match = await _etag_for(ctx["pool"], bs_idx)

    resp = await ctx["user"].patch(
        f"/api/v1/biosample/{bs_idx}",
        json={"submission_error": "x"},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 403


async def test_patch_biosample_missing_scope_403(ctx, no_biosample_write_patch_client):
    # Regular user with a PAT lacking Scope.BIOSAMPLE_WRITE: the role
    # guard runs first and rejects on role; that is still a 403, just
    # for the role reason rather than the scope reason. This pins that
    # callers without the write scope can never reach the route.
    bs_idx = await _seed_biosample_for_patch(ctx)
    if_match = await _etag_for(ctx["pool"], bs_idx)

    resp = await no_biosample_write_patch_client.patch(
        f"/api/v1/biosample/{bs_idx}",
        json={"submission_error": "x"},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 403


async def test_patch_biosample_missing_if_match_428(ctx):
    # No If-Match header → 428 before any DB read runs.
    bs_idx = await _seed_biosample_for_patch(ctx)

    resp = await ctx["wet"].patch(
        f"/api/v1/biosample/{bs_idx}",
        json={"submission_error": "x"},
    )
    assert resp.status_code == 428
    assert "If-Match" in resp.json()["detail"]


async def test_patch_biosample_mismatched_if_match_412(ctx):
    # If-Match supplied but does not match the current ETag → 412.
    bs_idx = await _seed_biosample_for_patch(ctx)

    resp = await ctx["wet"].patch(
        f"/api/v1/biosample/{bs_idx}",
        json={"submission_error": "x"},
        headers={"If-Match": '"2000-01-01T00:00:00+00:00"'},
    )
    assert resp.status_code == 412


async def test_patch_biosample_nonexistent_404(ctx):
    # Nonexistent idx → 404. The dummy If-Match value never gets compared
    # because existence trips first.
    max_idx = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.biosample")
    resp = await ctx["wet"].patch(
        f"/api/v1/biosample/{max_idx + 100_000}",
        json={"submission_error": "x"},
        headers={"If-Match": '"2000-01-01T00:00:00+00:00"'},
    )
    assert resp.status_code == 404


async def test_patch_biosample_retired_409(ctx):
    # Retired biosamples cannot be PATCHed; 409 distinguishes "row is in
    # a state that blocks the operation" from the missing-row 404.
    bs_idx = await _seed_biosample_for_patch(ctx)
    await retire_biosample(
        ctx["pool"],
        biosample_idx=bs_idx,
        retired_by_idx=ctx["wet_session"]["principal_idx"],
    )
    if_match = await _etag_for(ctx["pool"], bs_idx)

    resp = await ctx["wet"].patch(
        f"/api/v1/biosample/{bs_idx}",
        json={"submission_error": "x"},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 409
    assert "retired" in resp.json()["detail"]


async def test_patch_biosample_empty_body_422(ctx):
    # Empty JSON body trips BiosamplePatchRequest's "at least one field"
    # validator before the route runs.
    bs_idx = await _seed_biosample_for_patch(ctx)
    if_match = await _etag_for(ctx["pool"], bs_idx)

    resp = await ctx["wet"].patch(
        f"/api/v1/biosample/{bs_idx}",
        json={},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 422


async def test_patch_biosample_immutable_field_422(ctx):
    # `retired` is managed by retirement endpoints, not the PATCH
    # surface; extra="forbid" on BiosamplePatchRequest rejects it.
    bs_idx = await _seed_biosample_for_patch(ctx)
    if_match = await _etag_for(ctx["pool"], bs_idx)

    resp = await ctx["wet"].patch(
        f"/api/v1/biosample/{bs_idx}",
        json={"retired": True},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 422


@pytest.mark.parametrize("kind", OWNER_INELIGIBILITY_KINDS)
async def test_patch_biosample_owner_ineligibility_422(ctx, kind: IneligibilityKind):
    # Same eligibility surface as the import endpoint; one parametrised
    # pass over every ineligibility shape pins that the PATCH route
    # collapses each to the shared 422 detail.
    bs_idx = await _seed_biosample_for_patch(ctx)
    if_match = await _etag_for(ctx["pool"], bs_idx)
    bad_owner = await resolve_ineligible_owner_idx(
        ctx["pool"],
        kind=kind,
        prefix=f"{_SEED_PREFIX}-patch-elig",
        created=ctx["created"],
    )

    resp = await ctx["wet"].patch(
        f"/api/v1/biosample/{bs_idx}",
        json={"owner_idx": bad_owner},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"] == _ELIGIBILITY_DETAIL


async def test_patch_biosample_owner_self_incomplete_profile_422(ctx):
    # Pin the self-target eligibility check: a wet_lab_admin caller whose
    # own profile is incomplete must not be able to PATCH owner_idx to
    # their own principal_idx. The PATCH route is role-gated only (no
    # require_complete_profile), so the eligibility helper is the only
    # line of defense and must run on every call -- no caller-state
    # short-circuit.
    suffix = secrets.token_hex(4)
    caller_idx = await seed_user_principal(
        ctx["pool"],
        prefix=_SEED_PREFIX,
        suffix=f"patch-incomplete-wet-{suffix}",
        profile_complete=False,
        system_role=SystemRole.WET_LAB_ADMIN,
    )
    ctx["created"]["user_principals"].append(caller_idx)

    from qiita_control_plane.auth.token import mint_api_token
    from qiita_control_plane.main import app

    plaintext, _ = await mint_api_token(
        ctx["pool"],
        principal_idx=caller_idx,
        label=f"incomplete-wet-{suffix}",
        scopes=[Scope.BIOSAMPLE_WRITE],
    )

    # Seed a biosample owned by the session-scoped wet_lab_admin so the
    # candidate (the test caller's own idx) differs from the current
    # owner; the PATCH attempts an ownership transfer to self.
    bs_idx = await _seed_biosample_for_patch(ctx)
    if_match = await _etag_for(ctx["pool"], bs_idx)

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {plaintext}"},
    ) as caller:
        resp = await caller.patch(
            f"/api/v1/biosample/{bs_idx}",
            json={"owner_idx": caller_idx},
            headers={"If-Match": if_match},
        )
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"] == _ELIGIBILITY_DETAIL


async def test_patch_biosample_duplicate_accession_409(ctx):
    # Two biosamples; PATCH B's accession to A's value triggers
    # biosample_accession_unique → asyncpg.UniqueViolationError → 409.
    a_acc = _unique_accession("PATCH-A")
    owner_idx = ctx["wet_session"]["principal_idx"]
    bs_a = await ctx["pool"].fetchval(
        "INSERT INTO qiita.biosample (owner_idx, created_by_idx, biosample_accession)"
        " VALUES ($1, $1, $2) RETURNING idx",
        owner_idx,
        a_acc,
    )
    ctx["created"]["biosample"].append(bs_a)
    bs_b = await _seed_biosample_for_patch(ctx)
    if_match = await _etag_for(ctx["pool"], bs_b)

    resp = await ctx["wet"].patch(
        f"/api/v1/biosample/{bs_b}",
        json={"biosample_accession": a_acc},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 409
    assert "biosample_accession" in resp.json()["detail"]


async def test_patch_biosample_bad_metadata_checklist_idx_422(ctx):
    # Nonexistent metadata_checklist_idx trips the FK constraint →
    # asyncpg.ForeignKeyViolationError → 422 with the FK-specific detail.
    bs_idx = await _seed_biosample_for_patch(ctx)
    if_match = await _etag_for(ctx["pool"], bs_idx)
    bad_checklist = (
        await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.metadata_checklist")
    ) + 100_000

    resp = await ctx["wet"].patch(
        f"/api/v1/biosample/{bs_idx}",
        json={"metadata_checklist_idx": bad_checklist},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 422
    assert "metadata_checklist_idx" in resp.json()["detail"]
