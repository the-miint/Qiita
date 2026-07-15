"""Integration tests for the POST /api/v1/study/{study_idx}/biosample route.

Exercises wet_lab_admin and system_admin happy paths, the scope and
study-existence guards, the per-study ADMIN-access guard
(`require_study_access(min_tier=Tier.ADMIN, bypass_role=WET_LAB_ADMIN)`
— regular users who own a study or carry an ADMIN study_access row
may create biosamples there), the parametrised owner-eligibility 422
surface, the metadata dict's validation 422s (unknown field, parse
failure, owner-id collision), Pydantic body validation, and DB-level
exception-mapping (409 / 422). Regular-user 403 paths cover both the
no-access-row and the below-admin-tier cases.
"""

import secrets
from datetime import date
from decimal import Decimal
from typing import get_args

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import (
    URL_BIOSAMPLE_BY_IDX,
    URL_BIOSAMPLE_BY_STUDY,
    URL_BIOSAMPLE_LIST_BY_STUDY,
    URL_BIOSAMPLE_LOOKUP_BY_ACCESSION,
    URL_BIOSAMPLE_LOOKUP_BY_MATRIX_TUBE_ID,
)
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX, Scope, SystemRole
from qiita_common.models import BiosampleAccessionField, FieldDataType

from qiita_control_plane.testing.db_seeds import (
    fetch_seeded_metagenome_term,
    retire_biosample,
    retire_biosample_to_study_link,
    seed_biosample,
    seed_biosample_global_field,
    seed_biosample_to_study_link,
    seed_user_principal,
)
from qiita_control_plane.testing.unique_names import (
    unique_accession,
    unique_field_name,
    unique_matrix_tube_id,
)

from .conftest import (
    OWNER_INELIGIBILITY_KINDS,
    IneligibilityKind,
    _grant_study_access,
    _seed_study,
    assert_owner_ineligibility_422,
    delete_idxs,
    etag_for_row,
    resolve_ineligible_owner_idx,
)

pytestmark = pytest.mark.db


_SEED_PREFIX = "bs-route"
_ELIGIBILITY_DETAIL = "owner is not eligible to own biosamples"


# ---------------------------------------------------------------------------
# Biosample-specific seed helpers
# ---------------------------------------------------------------------------


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
    # biosample_metadata.value_missing_reason_idx FKs missing_value_reason
    # ON DELETE RESTRICT; sweep after the metadata rows are gone.
    await delete_idxs(pool, "missing_value_reason", created["missing_value_reason"])
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
        "missing_value_reason": [],
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

    `host_taxon_id` is a REQUIRED field the import now enforces, so it is injected
    (as 'not applicable' — a missing-value marker, which counts as supplied and
    needs no seeded NCBI term) unless the test already set it. A test that means to
    exercise the gate passes `metadata` WITHOUT it. The injection auto-creates one
    globally-linked study field plus one metadata row; both are tracked below (via
    `_track_global_metadata_outputs`) so FK-reverse cleanup sweeps them — otherwise
    the untracked host_taxon_id metadata row would block the biosample delete.
    """
    metadata = dict(body.get("metadata") or {})
    injected_host_taxon = "host taxon id" not in metadata
    if injected_host_taxon:
        metadata["host taxon id"] = "not applicable"
    body["metadata"] = metadata
    resp = await client.post(URL_BIOSAMPLE_BY_STUDY.format(study_idx=study_idx), json=body)
    if resp.status_code == 201:
        rj = resp.json()
        ctx["created"]["biosample"].append(rj["biosample_idx"])
        ctx["created"]["biosample_to_study"].append((rj["biosample_idx"], study_idx))
        if rj["owner_id_biosample_study_field_created"]:
            ctx["created"]["biosample_study_field"].append(rj["owner_id_biosample_study_field_idx"])
        meta_idx = await ctx["pool"].fetchval(
            "SELECT idx FROM qiita.biosample_metadata"
            " WHERE biosample_idx = $1 AND is_owner_biosample_id = true",
            rj["biosample_idx"],
        )
        if meta_idx is not None:
            ctx["created"]["biosample_metadata"].append(meta_idx)
        if injected_host_taxon:
            host_gf_idx = await ctx["pool"].fetchval(
                "SELECT idx FROM qiita.biosample_global_field WHERE internal_name = 'host_taxon_id'"
            )
            await _track_global_metadata_outputs(ctx, rj["biosample_idx"], study_idx, [host_gf_idx])
    return resp


async def _track_global_metadata_outputs(ctx, bs_idx, study_idx, global_idxs):
    """Track globally-linked study fields (by global field idx) and every
    non-owner-id metadata row written for this biosample. Use after
    `_post_biosample` in tests that exercised the metadata dict path so
    the FK-reverse cleanup picks the new rows up. Mirrors the sibling
    helper in tests/repositories/test_biosample.py so the two layers stay
    parallel.
    """
    # Pick up every globally-linked study field row at this study tied to
    # one of the supplied global fields.
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
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="wet-self"
    )

    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=unique_field_name(),
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
        "owner_id_biosample_study_field_idx": rj["owner_id_biosample_study_field_idx"],
        "owner_id_biosample_study_field_created": True,
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
    target_idx = await seed_user_principal(ctx["pool"], prefix=_SEED_PREFIX, suffix="wet-target")
    ctx["created"]["user_principals"].append(target_idx)
    study_idx = await _seed_study(ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="wet")

    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=target_idx,
        owner_biosample_id_field_name=unique_field_name(),
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
    target_idx = await seed_user_principal(ctx["pool"], prefix=_SEED_PREFIX, suffix="adm-target")
    ctx["created"]["user_principals"].append(target_idx)
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["admin_session"]["principal_idx"], suffix="adm"
    )

    resp = await _post_biosample(
        ctx["admin"],
        ctx,
        study_idx,
        owner_idx=target_idx,
        owner_biosample_id_field_name=unique_field_name(),
        owner_biosample_id_value="ADM-1",
    )
    assert resp.status_code == 201, resp.text


async def test_post_biosample_response_reports_field_created_flag_states(ctx):
    # Two consecutive POSTs with the same owner_biosample_id_field_name
    # must report owner_id_biosample_study_field_created=True then False, with both
    # responses pointing at the same owner_id_biosample_study_field_idx.
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="reuse"
    )

    field_name = unique_field_name()
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
    assert rj1["owner_id_biosample_study_field_created"] is True
    assert rj2["owner_id_biosample_study_field_created"] is False
    assert rj1["owner_id_biosample_study_field_idx"] == rj2["owner_id_biosample_study_field_idx"]


# ===========================================================================
# Auth / scope / role guards
# ===========================================================================


async def test_post_biosample_regular_user_owner_passes(ctx):
    # Regular user creates a biosample on a study they own; the owner
    # bypass in `require_study_access` admits the owner at every tier
    # without a study_access row.
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["user_session"]["principal_idx"], suffix="user-own"
    )

    resp = await _post_biosample(
        ctx["user"],
        ctx,
        study_idx,
        owner_idx=ctx["user_session"]["principal_idx"],
        owner_biosample_id_field_name=unique_field_name(),
        owner_biosample_id_value="USER-OWN-1",
    )
    assert resp.status_code == 201, resp.text


async def test_post_biosample_regular_user_no_access_403(ctx):
    # Regular user posts to a study owned by the wet_lab_admin with no
    # study_access row: tier resolves to public-by-absence, below
    # Tier.ADMIN, so require_study_access raises 403.
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="no-access"
    )

    resp = await _post_biosample(
        ctx["user"],
        ctx,
        study_idx,
        owner_idx=ctx["user_session"]["principal_idx"],
        owner_biosample_id_field_name=unique_field_name(),
        owner_biosample_id_value="X",
    )
    assert resp.status_code == 403
    assert "admin" in resp.json()["detail"]


async def test_post_biosample_regular_user_admin_tier_passes(ctx):
    # Regular user does not own the study but has Tier.ADMIN via
    # study_access; the tier check on the new route gate accepts the grant.
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="user-grant"
    )
    await _grant_study_access(
        ctx,
        study_idx=study_idx,
        principal_idx=ctx["user_session"]["principal_idx"],
        tier="admin",
        granted_by_idx=ctx["wet_session"]["principal_idx"],
    )

    resp = await _post_biosample(
        ctx["user"],
        ctx,
        study_idx,
        owner_idx=ctx["user_session"]["principal_idx"],
        owner_biosample_id_field_name=unique_field_name(),
        owner_biosample_id_value="USER-GRANT-1",
    )
    assert resp.status_code == 201, resp.text


async def test_post_biosample_regular_user_viewer_tier_403(ctx):
    # A study_access row at viewer tier is below the route's ADMIN floor;
    # require_study_access raises 403. Pins that the gate is genuinely
    # checking the tier rather than only the row's existence.
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="user-viewer"
    )
    await _grant_study_access(
        ctx,
        study_idx=study_idx,
        principal_idx=ctx["user_session"]["principal_idx"],
        tier="viewer",
        granted_by_idx=ctx["wet_session"]["principal_idx"],
    )

    resp = await _post_biosample(
        ctx["user"],
        ctx,
        study_idx,
        owner_idx=ctx["user_session"]["principal_idx"],
        owner_biosample_id_field_name=unique_field_name(),
        owner_biosample_id_value="X",
    )
    assert resp.status_code == 403
    assert "admin" in resp.json()["detail"]


async def test_post_biosample_anonymous_401(ctx):
    # No Authorization header → require_complete_profile chain raises 401.
    study_idx = await _seed_study(ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="anon")

    from qiita_control_plane.main import app

    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.post(
            URL_BIOSAMPLE_BY_STUDY.format(study_idx=study_idx),
            json={
                "owner_idx": ctx["wet_session"]["principal_idx"],
                "owner_biosample_id_field_name": unique_field_name(),
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
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="noscope"
    )

    resp = await no_biosample_write_client.post(
        URL_BIOSAMPLE_BY_STUDY.format(study_idx=study_idx),
        json={
            "owner_idx": ctx["user_session"]["principal_idx"],
            "owner_biosample_id_field_name": unique_field_name(),
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
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix=f"elig-{kind}"
    )

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
            owner_biosample_id_field_name=unique_field_name(),
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
        URL_BIOSAMPLE_BY_STUDY.format(study_idx=max_idx + 100_000),
        json={
            "owner_idx": ctx["wet_session"]["principal_idx"],
            "owner_biosample_id_field_name": unique_field_name(),
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
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="empty"
    )

    resp = await ctx["wet"].post(URL_BIOSAMPLE_BY_STUDY.format(study_idx=study_idx), json={})
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
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="emptyname"
    )

    resp = await ctx["wet"].post(
        URL_BIOSAMPLE_BY_STUDY.format(study_idx=study_idx),
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


@pytest.mark.parametrize(
    "body_field,make_value,expected_detail",
    [
        (
            "biosample_accession",
            lambda: unique_accession("BS-DUP"),
            "biosample_accession already in use",
        ),
        (
            "matrix_tube_id",
            unique_matrix_tube_id,
            "matrix_tube_id already in use",
        ),
    ],
)
async def test_post_biosample_duplicate_unique_column_409(
    ctx, body_field, make_value, expected_detail
):
    """Tests the case where a second POST tries to claim a value that
    another biosample already carries in a unique-constrained column: the
    route maps asyncpg.UniqueViolationError to 409 with a per-column
    mapped message. Parameterizing over the unique-constrained columns
    keeps the per-column 409 surface pinned through one definition.
    """
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="dup-col"
    )

    value = make_value()
    r1 = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=unique_field_name(),
        owner_biosample_id_value="V-1",
        **{body_field: value},
    )
    assert r1.status_code == 201, r1.text

    r2 = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=unique_field_name(),
        owner_biosample_id_value="V-2",
        **{body_field: value},
    )
    assert r2.status_code == 409
    assert r2.json()["detail"] == expected_detail


async def test_post_biosample_with_matrix_tube_id_round_trips(ctx):
    """Tests the case where a POST carries matrix_tube_id with a leading
    zero: the value reaches the DB with leading zeros intact and the
    subsequent GET surfaces the same string verbatim.
    """
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="tube-rt"
    )
    tube_id = unique_matrix_tube_id()
    # Sanity-check the test fixture: a generator that ever produced a
    # string with no leading zero would silently weaken this test.
    assert tube_id.startswith("0")

    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=unique_field_name(),
        owner_biosample_id_value="TUBE-1",
        matrix_tube_id=tube_id,
    )
    assert resp.status_code == 201, resp.text
    bs_idx = resp.json()["biosample_idx"]

    get_resp = await ctx["wet"].get(URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx))
    assert get_resp.status_code == 200, get_resp.text
    assert get_resp.json()["matrix_tube_id"] == tube_id


@pytest.mark.parametrize("bad_value", ["abc", "12-34", "", "0 1"])
async def test_post_biosample_bad_matrix_tube_id_format_422(ctx, bad_value):
    """Tests the case where matrix_tube_id violates the digits-only
    contract: the Pydantic validator on BiosampleImportRequest rejects
    the body at the wire boundary with 422 before reaching the DB CHECK.
    """
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="tube-bad"
    )

    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=unique_field_name(),
        owner_biosample_id_value="V-1",
        matrix_tube_id=bad_value,
    )
    assert resp.status_code == 422


@pytest.mark.parametrize("bad_value", ["1234567", "12345678", "123456789", "12345678901", "1" * 51])
async def test_post_biosample_bad_matrix_tube_id_length_422(ctx, bad_value):
    """Tests the case where matrix_tube_id is not exactly 10 digits long:
    the Pydantic validator on BiosampleImportRequest rejects the body at
    the wire boundary with 422.
    """
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="tube-long"
    )

    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=unique_field_name(),
        owner_biosample_id_value="V-1",
        matrix_tube_id=bad_value,
    )
    assert resp.status_code == 422


async def test_post_biosample_metadata_checklist_name_resolves(ctx):
    # A known checklist name resolves to its idx server-side and lands on
    # the created biosample.
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="cl-name"
    )
    checklist_name = f"ERC-test-{secrets.token_hex(4)}"
    checklist_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.metadata_checklist (name) VALUES ($1) RETURNING idx",
        checklist_name,
    )
    ctx["created"]["metadata_checklist"].append(checklist_idx)

    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=unique_field_name(),
        owner_biosample_id_value="V-1",
        metadata_checklist_name=checklist_name,
    )
    assert resp.status_code == 201
    bs_idx = resp.json()["biosample_idx"]
    stored = await ctx["pool"].fetchval(
        "SELECT metadata_checklist_idx FROM qiita.biosample WHERE idx = $1", bs_idx
    )
    assert stored == checklist_idx

    # Read-back surfaces the checklist as a ref carrying both idx and name.
    get_resp = await ctx["wet"].get(URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx))
    assert get_resp.status_code == 200
    assert get_resp.json()["metadata_checklist"] == {"idx": checklist_idx, "name": checklist_name}


async def test_post_biosample_unknown_metadata_checklist_name_422(ctx):
    # A checklist name with no matching row is rejected with a clean 422
    # before any write, rather than surfacing as an FK violation.
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="bad-cl"
    )
    missing_name = f"ERC-missing-{secrets.token_hex(4)}"

    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=unique_field_name(),
        owner_biosample_id_value="V-1",
        metadata_checklist_name=missing_name,
    )
    assert resp.status_code == 422
    expected_detail = (
        f"metadata_checklist_name {missing_name!r} does not reference an existing checklist"
    )
    assert resp.json()["detail"] == expected_detail


# ===========================================================================
# Metadata dict
# ===========================================================================


async def test_post_biosample_metadata_writes_global_fields(ctx):
    # Seed two global fields (DATE and NUMERIC) and post metadata that
    # references both by display_name. Verify the route round-trips the
    # parsed values into the matching value_* columns and creates one
    # globally-linked study field per global field.
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

    study_idx = await _seed_study(ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="meta")

    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=unique_field_name(),
        owner_biosample_id_value="META-WRITE-1",
        metadata={
            f"Collection Date {suffix}": "2026-05-06",
            f"Latitude {suffix}": "32.7",
        },
    )
    assert resp.status_code == 201, resp.text
    bs_idx = resp.json()["biosample_idx"]
    await _track_global_metadata_outputs(ctx, bs_idx, study_idx, [date_global, num_global])

    # Verify the metadata rows landed with the correct typed values. Scoped to the
    # two fields under test — _post_biosample injects the required host_taxon_id,
    # whose own non-owner-id row is not what this asserts.
    rows = await ctx["pool"].fetch(
        "SELECT global_field_idx, value_text, value_numeric, value_date"
        " FROM qiita.biosample_metadata"
        " WHERE biosample_idx = $1 AND global_field_idx = ANY($2::bigint[])"
        " ORDER BY global_field_idx",
        bs_idx,
        sorted([date_global, num_global]),
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


async def test_post_biosample_globally_linked_owner_field_409(ctx):
    # Seed a global field and post a metadata value against it so a
    # globally-linked biosample_study_field exists at (study, name).
    # A second POST that names that same display_name as the owner-id
    # field must be rejected: the owner-id row is purely-local PII and
    # cannot be written through a global slot. Maps to 409.
    suffix = secrets.token_hex(4)
    linked_name = f"Globally Linked {suffix}"
    global_idx = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"r_glob_{suffix}",
        display_name=linked_name,
        data_type=FieldDataType.TEXT,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"]["biosample_global_field"].append(global_idx)

    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="glob-owner"
    )

    seed_resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=unique_field_name(),
        owner_biosample_id_value="SEED-OWNER",
        metadata={linked_name: "seed-value"},
    )
    assert seed_resp.status_code == 201, seed_resp.text
    await _track_global_metadata_outputs(
        ctx, seed_resp.json()["biosample_idx"], study_idx, [global_idx]
    )

    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=linked_name,
        owner_biosample_id_value="DOOMED",
    )
    assert resp.status_code == 409, resp.text
    assert linked_name in resp.json()["detail"]


async def test_post_biosample_metadata_unknown_field_422(ctx):
    # Two metadata keys that have no matching biosample_global_field row.
    # The route's MetadataUnknownFieldsError handler must return
    # 422 with both unknown names listed.
    suffix = secrets.token_hex(4)
    unknown_a = f"Unknown A {suffix}"
    unknown_b = f"Unknown B {suffix}"
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="meta-unk"
    )

    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=unique_field_name(),
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
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="meta-bad"
    )

    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=unique_field_name(),
        owner_biosample_id_value="META-BAD-1",
        metadata={display_name: bad_value},
    )
    assert resp.status_code == 422
    assert display_name in resp.json()["detail"]


async def test_post_biosample_metadata_owner_id_collision_422(ctx):
    # The metadata dict carries a key equal to owner_biosample_id_field_name.
    # The composer raises BiosampleOwnerIdFieldCollisionError pre-write; the
    # route maps it to 422 with the colliding name in the detail.
    shared_name = unique_field_name("collide")
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="meta-coll"
    )

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


async def test_post_biosample_owner_id_missing_value_marker_422(ctx):
    """Tests the case where owner_biosample_id_value matches a known
    missing_value_reason name: the composer raises
    BiosampleOwnerIdMissingValueError and the route maps it to 422
    naming the offending value. No biosample row is created.
    """
    reason_name = f"reason_{secrets.token_hex(4)}"
    reason_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.missing_value_reason (name) VALUES ($1) RETURNING idx",
        reason_name,
    )
    ctx["created"]["missing_value_reason"].append(reason_idx)
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="owner-mv"
    )

    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=unique_field_name("owner_mv"),
        owner_biosample_id_value=reason_name,
        metadata={},
    )
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert reason_name in detail
    assert "missing-value marker" in detail

    # No biosample row landed: the pre-flight rejection fired before any
    # INSERT, and the route's transaction rolled back.
    bs_count = await ctx["pool"].fetchval(
        "SELECT COUNT(*) FROM qiita.biosample_to_study WHERE study_idx = $1",
        study_idx,
    )
    assert bs_count == 0


async def test_post_biosample_metadata_uses_seeded_globals(ctx):
    # Realistic 6-field MIxS-style import that resolves global fields against
    # the seeded biosample global-field rows instead of creating throwaway
    # global fields. The two environmental-context fields are ENVO terminology
    # fields, so their values are submitted as ENVO term_ids and resolve into
    # value_terminology_term_idx. Cleanup tracks only the per-test study-field
    # and metadata rows so the seeded globals survive.
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

    # The two environmental-context fields are ENVO terminology fields; resolve
    # the seeded term idxs their submitted term_ids will land on.
    envo_term_ids = {
        "broad-scale environmental context": "ENVO:01000249",
        "local environmental context": "ENVO:00000469",
    }
    term_rows = await ctx["pool"].fetch(
        "SELECT tt.term_id, tt.idx FROM qiita.terminology_term tt"
        " JOIN qiita.terminology t ON t.idx = tt.terminology_idx"
        " WHERE t.name = 'ENVO' AND tt.term_id = ANY($1::text[])",
        list(envo_term_ids.values()),
    )
    term_id_to_idx = {r["term_id"]: r["idx"] for r in term_rows}
    assert set(term_id_to_idx) == set(envo_term_ids.values())

    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="seeded-meta"
    )

    # Metadata keys mirror the seeded display_names exactly so the composer's
    # display_name → global_field_idx lookup hits all six rows.
    metadata = {
        "collection date": "2025",
        "geographic location (country and/or sea)": "USA",
        "geographic location (latitude)": "32.7157",
        "geographic location (longitude)": "-117.1611",
        "broad-scale environmental context": envo_term_ids["broad-scale environmental context"],
        "local environmental context": envo_term_ids["local environmental context"],
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
    await _track_global_metadata_outputs(ctx, bs_idx, study_idx, list(display_to_idx.values()))

    # Verify every metadata row landed in the correct typed value_* column:
    # typed scalars in value_text/value_numeric/value_date, ENVO terms in
    # value_terminology_term_idx.
    # Scoped to the six seeded fields under test; the required host_taxon_id that
    # _post_biosample injects writes its own row, which this assertion is not about.
    rows = await ctx["pool"].fetch(
        "SELECT global_field_idx, value_text, value_numeric, value_date,"
        " value_terminology_term_idx"
        " FROM qiita.biosample_metadata"
        " WHERE biosample_idx = $1 AND global_field_idx = ANY($2::bigint[])"
        " ORDER BY global_field_idx",
        bs_idx,
        sorted(display_to_idx.values()),
    )
    expected = sorted(
        [
            {
                "global_field_idx": display_to_idx["collection date"],
                "value_text": "2025",
                "value_numeric": None,
                "value_date": None,
                "value_terminology_term_idx": None,
            },
            {
                "global_field_idx": display_to_idx["geographic location (country and/or sea)"],
                "value_text": "USA",
                "value_numeric": None,
                "value_date": None,
                "value_terminology_term_idx": None,
            },
            {
                "global_field_idx": display_to_idx["geographic location (latitude)"],
                "value_text": None,
                "value_numeric": Decimal("32.7157"),
                "value_date": None,
                "value_terminology_term_idx": None,
            },
            {
                "global_field_idx": display_to_idx["geographic location (longitude)"],
                "value_text": None,
                "value_numeric": Decimal("-117.1611"),
                "value_date": None,
                "value_terminology_term_idx": None,
            },
            {
                "global_field_idx": display_to_idx["broad-scale environmental context"],
                "value_text": None,
                "value_numeric": None,
                "value_date": None,
                "value_terminology_term_idx": term_id_to_idx["ENVO:01000249"],
            },
            {
                "global_field_idx": display_to_idx["local environmental context"],
                "value_text": None,
                "value_numeric": None,
                "value_date": None,
                "value_terminology_term_idx": term_id_to_idx["ENVO:00000469"],
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


async def _seed_link_to_study(ctx, *, study_idx, owner_idx):
    """Seed a biosample owned by `owner_idx`, link it to `study_idx`, and
    track both rows in `ctx['created']` for FK-reverse cleanup. Wraps the
    db_seeds primitives so the per-test setup stays a single line."""
    bs_idx = await seed_biosample(ctx["pool"], owner_idx=owner_idx, created_by_idx=owner_idx)
    ctx["created"]["biosample"].append(bs_idx)
    await seed_biosample_to_study_link(
        ctx["pool"],
        biosample_idx=bs_idx,
        study_idx=study_idx,
        created_by_idx=owner_idx,
    )
    ctx["created"]["biosample_to_study"].append((bs_idx, study_idx))
    return bs_idx


async def test_list_biosample_idxs_anonymous_401(ctx):
    # No Authorization header → require_human chain raises 401.
    from qiita_control_plane.main import app

    app.state.pool = ctx["pool"]
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["user_session"]["principal_idx"], suffix="anon"
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.get(URL_BIOSAMPLE_LIST_BY_STUDY.format(study_idx=study_idx))
    assert resp.status_code == 401


async def test_list_biosample_idxs_missing_scope_403(ctx, no_study_read_client):
    # A regular_user PAT that omits Scope.STUDY_READ is rejected by
    # require_scope before the access-tier check runs.
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["user_session"]["principal_idx"], suffix="no-scope"
    )
    resp = await no_study_read_client.get(URL_BIOSAMPLE_LIST_BY_STUDY.format(study_idx=study_idx))
    assert resp.status_code == 403
    assert "study:read" in resp.json()["detail"]


async def test_list_biosample_idxs_nonexistent_study_regular_user_404(ctx):
    # require_study_exists fires before require_study_access for a regular
    # user, so a study_idx past the highest existing study returns 404.
    max_idx = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.study")
    resp = await ctx["user"].get(URL_BIOSAMPLE_LIST_BY_STUDY.format(study_idx=max_idx + 100_000))
    assert resp.status_code == 404


async def test_list_biosample_idxs_nonexistent_study_admin_404(ctx):
    # Even with the wet_lab_admin role bypass on require_study_access,
    # require_study_exists still surfaces 404 — the route composes both
    # so admin-bypass callers do not silently get an empty list for a
    # non-existent study.
    max_idx = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.study")
    resp = await ctx["admin"].get(URL_BIOSAMPLE_LIST_BY_STUDY.format(study_idx=max_idx + 100_000))
    assert resp.status_code == 404


async def test_list_biosample_idxs_no_access_403(ctx):
    # Regular user is neither owner nor study_access row holder; effective
    # tier is public-by-absence, below the route's viewer minimum → 403.
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["admin_session"]["principal_idx"], suffix="no-access"
    )
    resp = await ctx["user"].get(URL_BIOSAMPLE_LIST_BY_STUDY.format(study_idx=study_idx))
    assert resp.status_code == 403


async def test_list_biosample_idxs_owner_returns_payload(ctx):
    # Study owner bypasses the tier comparison; the response carries the
    # documented envelope with the regular-user system_role.
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["user_session"]["principal_idx"], suffix="owner"
    )
    bs_idxs = [
        await _seed_link_to_study(
            ctx,
            study_idx=study_idx,
            owner_idx=ctx["user_session"]["principal_idx"],
        )
        for _ in range(2)
    ]

    resp = await ctx["user"].get(URL_BIOSAMPLE_LIST_BY_STUDY.format(study_idx=study_idx))
    assert resp.status_code == 200, resp.text
    expected = {
        "idxs": list(reversed(bs_idxs)),
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
        ctx, owner_idx=ctx["admin_session"]["principal_idx"], suffix="viewer"
    )
    await _grant_study_access(
        ctx,
        study_idx=study_idx,
        principal_idx=ctx["user_session"]["principal_idx"],
        tier="viewer",
        granted_by_idx=ctx["admin_session"]["principal_idx"],
    )

    resp = await ctx["user"].get(URL_BIOSAMPLE_LIST_BY_STUDY.format(study_idx=study_idx))
    assert resp.status_code == 200, resp.text
    expected = {
        "idxs": [],
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
        ctx, owner_idx=ctx["admin_session"]["principal_idx"], suffix="wet-bypass"
    )
    bs_idx = await _seed_link_to_study(
        ctx,
        study_idx=study_idx,
        owner_idx=ctx["admin_session"]["principal_idx"],
    )

    resp = await ctx["wet"].get(URL_BIOSAMPLE_LIST_BY_STUDY.format(study_idx=study_idx))
    assert resp.status_code == 200, resp.text
    expected = {
        "idxs": [bs_idx],
        "count": 1,
        "truncated": False,
        "caller_system_role": "wet_lab_admin",
    }
    assert resp.json() == expected


async def test_list_biosample_idxs_system_admin_bypasses_access(ctx):
    # The system_admin role also bypasses require_study_access; the
    # caller_system_role reflects the actual database value.
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="adm-bypass"
    )

    resp = await ctx["admin"].get(URL_BIOSAMPLE_LIST_BY_STUDY.format(study_idx=study_idx))
    assert resp.status_code == 200, resp.text
    expected = {
        "idxs": [],
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
        ctx, owner_idx=ctx["user_session"]["principal_idx"], suffix="retired"
    )
    owner_idx = ctx["user_session"]["principal_idx"]
    active_idx = await _seed_link_to_study(ctx, study_idx=study_idx, owner_idx=owner_idx)
    retired_link_idx = await _seed_link_to_study(ctx, study_idx=study_idx, owner_idx=owner_idx)
    retired_bs_idx = await _seed_link_to_study(ctx, study_idx=study_idx, owner_idx=owner_idx)
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

    resp = await ctx["user"].get(URL_BIOSAMPLE_LIST_BY_STUDY.format(study_idx=study_idx))
    assert resp.status_code == 200, resp.text
    expected = {
        "idxs": [active_idx],
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
    """Confirm the ETag header is present and wrapped in double quotes.
    Format inside the quotes is opaque-by-contract."""
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
    bs_idx = await seed_biosample(ctx["pool"], owner_idx=owner_idx, created_by_idx=owner_idx)
    ctx["created"]["biosample"].append(bs_idx)

    resp = await ctx["user"].get(URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx))
    assert resp.status_code == 200, resp.text
    _assert_etag_quoted(resp)

    rj = resp.json()
    expected = {
        "biosample_idx": bs_idx,
        "owner_idx": owner_idx,
        "metadata_checklist": None,
        "biosample_accession": None,
        "ena_sample_accession": None,
        "matrix_tube_id": None,
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
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="get-via-access"
    )
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

    resp = await ctx["user"].get(URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx))
    assert resp.status_code == 200, resp.text
    _assert_etag_quoted(resp)

    rj = resp.json()
    wet_idx = ctx["wet_session"]["principal_idx"]
    expected = {
        "biosample_idx": bs_idx,
        "owner_idx": wet_idx,
        "metadata_checklist": None,
        "biosample_accession": None,
        "ena_sample_accession": None,
        "matrix_tube_id": None,
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
    bs_idx = await seed_biosample(ctx["pool"], owner_idx=owner_idx, created_by_idx=owner_idx)
    ctx["created"]["biosample"].append(bs_idx)

    resp = await ctx["wet"].get(URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx))
    assert resp.status_code == 200, resp.text
    _assert_etag_quoted(resp)

    rj = resp.json()
    expected = {
        "biosample_idx": bs_idx,
        "owner_idx": owner_idx,
        "metadata_checklist": None,
        "biosample_accession": None,
        "ena_sample_accession": None,
        "matrix_tube_id": None,
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
    bs_idx = await seed_biosample(ctx["pool"], owner_idx=owner_idx, created_by_idx=owner_idx)
    ctx["created"]["biosample"].append(bs_idx)

    resp = await ctx["admin"].get(URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx))
    assert resp.status_code == 200, resp.text
    _assert_etag_quoted(resp)

    rj = resp.json()
    expected = {
        "biosample_idx": bs_idx,
        "owner_idx": owner_idx,
        "metadata_checklist": None,
        "biosample_accession": None,
        "ena_sample_accession": None,
        "matrix_tube_id": None,
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


async def test_get_biosample_carries_missing_reason_marker(ctx):
    """Tests the case where a globally-linked metadata row is an
    intentionally-missing entry: the GET response surfaces the row's
    value as a MissingReasonRef on the wire (idx + name).
    """
    suffix = secrets.token_hex(4)
    reason_name = f"reason_{suffix}"
    reason_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.missing_value_reason (name) VALUES ($1) RETURNING idx",
        reason_name,
    )
    ctx["created"]["missing_value_reason"].append(reason_idx)

    # NUMERIC global field; a literal reason name would fail typed parsing
    # without the missing-reason routing, so the assertion pinpoints that
    # the read returned the marker shape (not a coerced typed value).
    internal_name = f"r_miss_{suffix}"
    display_name = f"Latitude {suffix}"
    global_idx = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=internal_name,
        display_name=display_name,
        data_type=FieldDataType.NUMERIC,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"]["biosample_global_field"].append(global_idx)

    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="get-miss"
    )
    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=unique_field_name(),
        owner_biosample_id_value="META-MISS-1",
        metadata={display_name: reason_name},
    )
    assert resp.status_code == 201, resp.text
    bs_idx = resp.json()["biosample_idx"]
    await _track_global_metadata_outputs(ctx, bs_idx, study_idx, [global_idx])

    resp = await ctx["wet"].get(URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx))
    assert resp.status_code == 200, resp.text
    _assert_etag_quoted(resp)

    rj = resp.json()
    expected = {
        "biosample_idx": bs_idx,
        "owner_idx": ctx["wet_session"]["principal_idx"],
        "metadata_checklist": None,
        "biosample_accession": None,
        "ena_sample_accession": None,
        "matrix_tube_id": None,
        "last_submission_at": None,
        "submission_error": None,
        "last_metadata_change_at": rj["last_metadata_change_at"],
        "created_by_idx": ctx["wet_session"]["principal_idx"],
        "created_at": rj["created_at"],
        "updated_at": rj["updated_at"],
        "retired": False,
        "retired_by_idx": None,
        "retired_at": None,
        "retire_reason": None,
        "global_metadata": {
            internal_name: {
                "display_name": display_name,
                "description": None,
                "data_type": "numeric",
                "value": {"kind": "missing_reason", "idx": reason_idx, "name": reason_name},
            },
        },
        "caller_system_role": "wet_lab_admin",
    }
    # _post_biosample injects the enforced-required host_taxon_id; it is not what
    # this test is about, so drop it before the whole-response comparison.
    rj["global_metadata"].pop("host_taxon_id", None)
    assert rj == expected


async def test_get_biosample_carries_terminology_term(ctx):
    """Tests the case where a globally-linked metadata row is a terminology
    term (value_terminology_term_idx populated): the GET response surfaces
    the row's value as a TerminologyTermRef on the wire (idx + term_id +
    label).
    """
    # Reuse the seeded NCBI Taxonomy + metagenome term.
    term_row = await fetch_seeded_metagenome_term(ctx["pool"])
    terminology_idx = term_row["terminology_idx"]

    # TERMINOLOGY global field bound to NCBI Taxonomy so the
    # value_terminology_term_idx write satisfies the field contract.
    suffix = secrets.token_hex(4)
    internal_name = f"r_term_{suffix}"
    display_name = f"Sample Taxon {suffix}"
    global_idx = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=internal_name,
        display_name=display_name,
        data_type=FieldDataType.TERMINOLOGY,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
        terminology_idx=terminology_idx,
    )
    ctx["created"]["biosample_global_field"].append(global_idx)

    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="get-term"
    )
    resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=unique_field_name(),
        owner_biosample_id_value="META-TERM-1",
        metadata={display_name: term_row["term_id"]},
    )
    assert resp.status_code == 201, resp.text
    bs_idx = resp.json()["biosample_idx"]
    await _track_global_metadata_outputs(ctx, bs_idx, study_idx, [global_idx])

    resp = await ctx["wet"].get(URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx))
    assert resp.status_code == 200, resp.text
    _assert_etag_quoted(resp)

    rj = resp.json()
    expected = {
        "biosample_idx": bs_idx,
        "owner_idx": ctx["wet_session"]["principal_idx"],
        "metadata_checklist": None,
        "biosample_accession": None,
        "ena_sample_accession": None,
        "matrix_tube_id": None,
        "last_submission_at": None,
        "submission_error": None,
        "last_metadata_change_at": rj["last_metadata_change_at"],
        "created_by_idx": ctx["wet_session"]["principal_idx"],
        "created_at": rj["created_at"],
        "updated_at": rj["updated_at"],
        "retired": False,
        "retired_by_idx": None,
        "retired_at": None,
        "retire_reason": None,
        "global_metadata": {
            internal_name: {
                "display_name": display_name,
                "description": None,
                "data_type": "terminology",
                "value": {
                    "kind": "terminology_term",
                    "idx": term_row["idx"],
                    "term_id": term_row["term_id"],
                    "label": term_row["label"],
                },
            },
        },
        "caller_system_role": "wet_lab_admin",
    }
    rj["global_metadata"].pop("host_taxon_id", None)
    assert rj == expected


async def test_get_biosample_anonymous_401(ctx):
    # No Authorization header → require_human raises 401.
    owner_idx = ctx["user_session"]["principal_idx"]
    bs_idx = await seed_biosample(ctx["pool"], owner_idx=owner_idx, created_by_idx=owner_idx)
    ctx["created"]["biosample"].append(bs_idx)

    from qiita_control_plane.main import app

    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.get(URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx))
    assert resp.status_code == 401


async def test_get_biosample_missing_scope_403(ctx, no_biosample_read_client):
    # Regular user holds a PAT that omits Scope.BIOSAMPLE_READ; require_scope
    # rejects with 403 before any DB read runs.
    owner_idx = ctx["user_session"]["principal_idx"]
    bs_idx = await seed_biosample(ctx["pool"], owner_idx=owner_idx, created_by_idx=owner_idx)
    ctx["created"]["biosample"].append(bs_idx)

    resp = await no_biosample_read_client.get(URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx))
    assert resp.status_code == 403
    assert "biosample:read" in resp.json()["detail"]


async def test_get_biosample_no_access_403(ctx):
    # The regular user is not the biosample's owner and has no study_access
    # row on any non-retired link. The repo predicate returns False, the
    # route surfaces 403 (existence is already revealed by the prior 404
    # path being skipped — admin-bypass and tier-mismatch share the 403
    # spelling per project conventions).
    owner_idx = ctx["wet_session"]["principal_idx"]
    bs_idx = await seed_biosample(ctx["pool"], owner_idx=owner_idx, created_by_idx=owner_idx)
    ctx["created"]["biosample"].append(bs_idx)

    resp = await ctx["user"].get(URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx))
    assert resp.status_code == 403
    assert "no read path" in resp.json()["detail"]


async def test_get_biosample_nonexistent_404(ctx):
    # An idx well past MAX → fetch_biosample returns None → 404 before the
    # access predicate runs.
    max_idx = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.biosample")
    resp = await ctx["wet"].get(URL_BIOSAMPLE_BY_IDX.format(biosample_idx=max_idx + 100_000))
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"]


async def test_get_biosample_retired_404_even_for_wet_lab_admin(ctx):
    # Retired biosamples 404 unconditionally for now, even for wet_lab_admin.
    # This pins the carve-out documented in the route docstring; once the
    # planned retired-retrieval surface lands, this test will be relaxed
    # for wet_lab_admin and a parallel test pinned for the non-admin path.
    owner_idx = ctx["user_session"]["principal_idx"]
    bs_idx = await seed_biosample(ctx["pool"], owner_idx=owner_idx, created_by_idx=owner_idx)
    ctx["created"]["biosample"].append(bs_idx)
    await retire_biosample(ctx["pool"], biosample_idx=bs_idx, retired_by_idx=owner_idx)

    resp = await ctx["wet"].get(URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx))
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
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="get-md"
    )

    post_resp = await _post_biosample(
        ctx["wet"],
        ctx,
        study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_biosample_id_field_name=unique_field_name(),
        owner_biosample_id_value="GET-MD-1",
        metadata={display_name: "HOST-99"},
    )
    assert post_resp.status_code == 201, post_resp.text
    bs_idx = post_resp.json()["biosample_idx"]
    await _track_global_metadata_outputs(ctx, bs_idx, study_idx, [global_idx])

    resp = await ctx["wet"].get(URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx))
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
    rj["global_metadata"].pop("host_taxon_id", None)
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

    Thin biosample-flavored wrapper around the shared etag_for_row
    conftest helper so the test-file call sites keep their existing
    two-argument shape.
    """
    return await etag_for_row(pool, table="biosample", row_idx=bs_idx)


async def _seed_biosample_for_patch(ctx) -> int:
    """Seed a wet_lab_admin-owned biosample with a known accession; track for cleanup.

    Returns the biosample idx. Caller pre-loads any field they want to test
    PATCHing later; the seed gives the row enough columns to round-trip.
    """
    owner_idx = ctx["wet_session"]["principal_idx"]
    bs_idx = await seed_biosample(ctx["pool"], owner_idx=owner_idx, created_by_idx=owner_idx)
    ctx["created"]["biosample"].append(bs_idx)
    return bs_idx


async def test_patch_biosample_wet_lab_admin_happy_path(ctx):
    # Wet_lab_admin patches a single column; response shape mirrors the
    # GET route's BiosampleResponse, ETag header is set, and the
    # full-object equality confirms only the targeted column changed.
    bs_idx = await _seed_biosample_for_patch(ctx)
    if_match = await _etag_for(ctx["pool"], bs_idx)
    new_acc = unique_accession("PATCH-OK")

    resp = await ctx["wet"].patch(
        URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx),
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
        "metadata_checklist": None,
        "biosample_accession": new_acc,
        "ena_sample_accession": None,
        "matrix_tube_id": None,
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
    # Seed a biosample with a checklist set, then PATCH metadata_checklist_name
    # to explicit null. The model_fields_set distinction (absent vs. null)
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
        URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx),
        json={"metadata_checklist_name": None},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["metadata_checklist"] is None


async def test_patch_biosample_etag_advances(ctx):
    # The response's ETag header must differ from the request's If-Match
    # value: the schema's biosample_set_updated_at trigger bumps
    # updated_at, and the route surfaces that bump in the new ETag.
    bs_idx = await _seed_biosample_for_patch(ctx)
    if_match = await _etag_for(ctx["pool"], bs_idx)

    resp = await ctx["wet"].patch(
        URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx),
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
                " VALUES ($1, $2, $3) RETURNING idx",
                svc_name,
                SystemRole.WET_LAB_ADMIN,
                SYSTEM_PRINCIPAL_IDX,
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
            URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx),
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
            URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx),
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
        URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx),
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
        URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx),
        json={"submission_error": "x"},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 403


async def test_patch_biosample_missing_if_match_428(ctx):
    # No If-Match header → 428 before any DB read runs.
    bs_idx = await _seed_biosample_for_patch(ctx)

    resp = await ctx["wet"].patch(
        URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx),
        json={"submission_error": "x"},
    )
    assert resp.status_code == 428
    assert "If-Match" in resp.json()["detail"]


async def test_patch_biosample_mismatched_if_match_412(ctx):
    # If-Match supplied but does not match the current ETag → 412.
    bs_idx = await _seed_biosample_for_patch(ctx)

    resp = await ctx["wet"].patch(
        URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx),
        json={"submission_error": "x"},
        headers={"If-Match": '"2000-01-01T00:00:00+00:00"'},
    )
    assert resp.status_code == 412


async def test_patch_biosample_nonexistent_404(ctx):
    # Nonexistent idx → 404. The dummy If-Match value never gets compared
    # because existence trips first.
    max_idx = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.biosample")
    resp = await ctx["wet"].patch(
        URL_BIOSAMPLE_BY_IDX.format(biosample_idx=max_idx + 100_000),
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
        URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx),
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
        URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx),
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
        URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx),
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
        URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx),
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
            URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx),
            json={"owner_idx": caller_idx},
            headers={"If-Match": if_match},
        )
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"] == _ELIGIBILITY_DETAIL


@pytest.mark.parametrize(
    "column,make_value",
    [
        ("biosample_accession", lambda: unique_accession("PATCH-A")),
        ("matrix_tube_id", unique_matrix_tube_id),
    ],
)
async def test_patch_biosample_duplicate_unique_column_409(ctx, column, make_value):
    """Tests the case where a PATCH tries to claim a value that another
    biosample already carries in a unique-constrained column: the route
    maps asyncpg.UniqueViolationError to 409 with a per-column detail
    that names the violated column. Parameterizing over the unique-
    constrained columns keeps the per-column 409 surface pinned through
    one definition.
    """
    value = make_value()
    owner_idx = ctx["wet_session"]["principal_idx"]
    bs_a = await ctx["pool"].fetchval(
        f"INSERT INTO qiita.biosample (owner_idx, created_by_idx, {column})"
        " VALUES ($1, $1, $2) RETURNING idx",
        owner_idx,
        value,
    )
    ctx["created"]["biosample"].append(bs_a)
    bs_b = await _seed_biosample_for_patch(ctx)
    if_match = await _etag_for(ctx["pool"], bs_b)

    resp = await ctx["wet"].patch(
        URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_b),
        json={column: value},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 409
    assert column in resp.json()["detail"]


async def test_patch_biosample_unknown_metadata_checklist_name_422(ctx):
    # A checklist name with no matching row is rejected with a clean 422
    # before the UPDATE, rather than surfacing as an FK violation.
    bs_idx = await _seed_biosample_for_patch(ctx)
    if_match = await _etag_for(ctx["pool"], bs_idx)
    missing_name = f"ERC-missing-{secrets.token_hex(4)}"

    resp = await ctx["wet"].patch(
        URL_BIOSAMPLE_BY_IDX.format(biosample_idx=bs_idx),
        json={"metadata_checklist_name": missing_name},
        headers={"If-Match": if_match},
    )
    assert resp.status_code == 422
    expected_detail = (
        f"metadata_checklist_name {missing_name!r} does not reference an existing checklist"
    )
    assert resp.json()["detail"] == expected_detail


# ---------------------------------------------------------------------------
# POST /biosample/lookup-by-accession — bulk accession → idx resolver
# ---------------------------------------------------------------------------
# Resolves an N-accession list in one round trip. Used by the bundled
# qiita submit-bcl-convert flow to translate preflight biosample_accession
# values into the biosample_idx the sequenced-sample composer requires.
# The auth surface intentionally exposes only (accession, idx) pairs — no
# row columns — so the route stays accessible to wet_lab_admin- callers
# whose pool may span studies they do not have access to. Per-row access
# enforcement still applies to GET /biosample/{idx}.


async def _seed_biosample_with_accession(
    ctx,
    *,
    accession: str,
    owner_idx: int,
    field: BiosampleAccessionField = "biosample_accession",
) -> int:
    """Seed a non-retired biosample carrying `accession` in the named
    accession column (default biosample_accession); track for cleanup and
    return its idx."""
    if field not in get_args(BiosampleAccessionField):
        raise ValueError(f"invalid biosample accession field: {field!r}")
    # Column name is interpolated because Postgres can't parameter-bind
    # identifiers; the guard above pins it to the accession-column set.
    idx = await ctx["pool"].fetchval(
        f"INSERT INTO qiita.biosample (owner_idx, created_by_idx, {field})"
        " VALUES ($1, $1, $2) RETURNING idx",
        owner_idx,
        accession,
    )
    ctx["created"]["biosample"].append(idx)
    return idx


async def test_lookup_by_accession_returns_resolved_map_and_missing_list(ctx):
    # Three accessions: two seeded, one absent. Expect the resolved map to
    # carry the two seeded mappings and `missing` to surface the absent one.
    owner_idx = ctx["wet_session"]["principal_idx"]
    acc_a = unique_accession("LOOKUP-A")
    acc_b = unique_accession("LOOKUP-B")
    acc_missing = unique_accession("LOOKUP-MISS")
    bs_a = await _seed_biosample_with_accession(ctx, accession=acc_a, owner_idx=owner_idx)
    bs_b = await _seed_biosample_with_accession(ctx, accession=acc_b, owner_idx=owner_idx)

    resp = await ctx["wet"].post(
        URL_BIOSAMPLE_LOOKUP_BY_ACCESSION,
        json={"accessions": [acc_a, acc_b, acc_missing]},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body == {
        "resolved": {acc_a: bs_a, acc_b: bs_b},
        "missing": [acc_missing],
    }


async def test_lookup_by_accession_resolves_by_ena_sample_accession_when_specified(ctx):
    # accession_field selects the ena_sample_accession column: a biosample
    # seeded with that column resolves under the selector, while one seeded
    # only with biosample_accession does not and is reported missing.
    owner_idx = ctx["wet_session"]["principal_idx"]
    ena_acc = unique_accession("LOOKUP-ENA")
    bs_acc = unique_accession("LOOKUP-BS-ONLY")
    ena_bs = await _seed_biosample_with_accession(
        ctx, accession=ena_acc, owner_idx=owner_idx, field="ena_sample_accession"
    )
    await _seed_biosample_with_accession(ctx, accession=bs_acc, owner_idx=owner_idx)

    resp = await ctx["wet"].post(
        URL_BIOSAMPLE_LOOKUP_BY_ACCESSION,
        json={"accessions": [ena_acc, bs_acc], "accession_field": "ena_sample_accession"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"resolved": {ena_acc: ena_bs}, "missing": [bs_acc]}


async def test_lookup_by_accession_dedups_input_preserving_order(ctx):
    # Repeated accessions are deduped before the fetch; the response shape
    # carries each accession once, in input-order.
    owner_idx = ctx["wet_session"]["principal_idx"]
    acc = unique_accession("LOOKUP-DUP")
    bs = await _seed_biosample_with_accession(ctx, accession=acc, owner_idx=owner_idx)
    acc_miss = unique_accession("LOOKUP-DUP-MISS")

    resp = await ctx["wet"].post(
        URL_BIOSAMPLE_LOOKUP_BY_ACCESSION,
        json={"accessions": [acc_miss, acc, acc_miss, acc]},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["resolved"] == {acc: bs}
    # Input-order dedup: first occurrence wins.
    assert body["missing"] == [acc_miss]


async def test_lookup_by_accession_excludes_retired_biosamples(ctx):
    # A retired row is not in `resolved` (the composer would refuse to FK
    # a fresh prep_sample to it anyway), so it lands in `missing`.
    owner_idx = ctx["wet_session"]["principal_idx"]
    acc = unique_accession("LOOKUP-RETIRED")
    bs = await _seed_biosample_with_accession(ctx, accession=acc, owner_idx=owner_idx)
    await retire_biosample(ctx["pool"], biosample_idx=bs, retired_by_idx=owner_idx)

    resp = await ctx["wet"].post(
        URL_BIOSAMPLE_LOOKUP_BY_ACCESSION,
        json={"accessions": [acc]},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"resolved": {}, "missing": [acc]}


async def test_lookup_by_accession_regular_user_passes(ctx):
    # The route has no per-row access predicate (see route docstring); a
    # regular user with biosample:read can resolve idxs for biosamples
    # they cannot otherwise read via GET /biosample/{idx}. This keeps the
    # bcl-convert flow accessible to operators whose pool spans studies
    # they are not a member of.
    owner_idx = ctx["wet_session"]["principal_idx"]
    acc = unique_accession("LOOKUP-USER")
    bs = await _seed_biosample_with_accession(ctx, accession=acc, owner_idx=owner_idx)

    resp = await ctx["user"].post(
        URL_BIOSAMPLE_LOOKUP_BY_ACCESSION,
        json={"accessions": [acc]},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"resolved": {acc: bs}, "missing": []}


async def test_lookup_by_accession_anonymous_401(ctx):
    from qiita_control_plane.main import app

    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.post(
            URL_BIOSAMPLE_LOOKUP_BY_ACCESSION,
            json={"accessions": ["SAMN00000001"]},
        )
    assert resp.status_code == 401


async def test_lookup_by_accession_missing_scope_403(ctx, no_biosample_read_client):
    resp = await no_biosample_read_client.post(
        URL_BIOSAMPLE_LOOKUP_BY_ACCESSION,
        json={"accessions": ["SAMN00000001"]},
    )
    assert resp.status_code == 403
    assert "biosample:read" in resp.json()["detail"]


async def test_lookup_by_accession_rejects_empty_list_422(ctx):
    # Pydantic min_length=1 rejects [] before the SQL runs.
    resp = await ctx["wet"].post(
        URL_BIOSAMPLE_LOOKUP_BY_ACCESSION,
        json={"accessions": []},
    )
    assert resp.status_code == 422


async def test_lookup_by_accession_rejects_empty_string_422(ctx):
    # Empty accession strings would silently match the empty string in the
    # column if accepted; the per-element min_length=1 keeps that out.
    resp = await ctx["wet"].post(
        URL_BIOSAMPLE_LOOKUP_BY_ACCESSION,
        json={"accessions": [""]},
    )
    assert resp.status_code == 422


async def test_lookup_by_accession_rejects_extra_field_422(ctx):
    # extra='forbid' on the request model rejects unknown fields rather
    # than silently dropping them.
    resp = await ctx["wet"].post(
        URL_BIOSAMPLE_LOOKUP_BY_ACCESSION,
        json={"accessions": ["SAMN00000001"], "unknown": "x"},
    )
    assert resp.status_code == 422


async def test_lookup_by_accession_rejects_invalid_accession_field_422(ctx):
    # accession_field outside BiosampleAccessionField is rejected by the
    # Literal at the wire boundary with 422 before any DB work runs;
    # matrix_tube_id is a lookup key but not an accession column, so it is
    # out of set here.
    resp = await ctx["wet"].post(
        URL_BIOSAMPLE_LOOKUP_BY_ACCESSION,
        json={"accessions": ["SAMN00000001"], "accession_field": "matrix_tube_id"},
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# POST /biosample/lookup-by-matrix-tube-id — bulk matrix_tube_id → idx resolver
# ---------------------------------------------------------------------------
# Mirrors the accession variant; the auth surface, dedup/missing semantics,
# and retired-row exclusion are shared with lookup_biosample_by_accession
# via resolve_idxs_by_natural_key in routes/_helpers.py. The tests below
# cover the per-key wire surface (route exists, format validator fires on
# bad input). The shared behavior (auth tiers, dedup, retired-row
# exclusion) is exercised once on the accession surface above.


async def _seed_biosample_with_matrix_tube_id(ctx, *, matrix_tube_id: str, owner_idx: int) -> int:
    """Seed a non-retired biosample carrying the given matrix_tube_id;
    track for cleanup and return its idx."""
    idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.biosample (owner_idx, created_by_idx, matrix_tube_id)"
        " VALUES ($1, $1, $2) RETURNING idx",
        owner_idx,
        matrix_tube_id,
    )
    ctx["created"]["biosample"].append(idx)
    return idx


async def test_lookup_by_matrix_tube_id_returns_resolved_map_and_missing_list(ctx):
    """Tests the case where the lookup body carries a mix of present and
    absent matrix_tube_id values: the response resolves the hits and
    surfaces the misses verbatim, with leading zeros preserved end-to-end.
    """
    owner_idx = ctx["wet_session"]["principal_idx"]
    tube_a = unique_matrix_tube_id()
    tube_b = unique_matrix_tube_id()
    tube_missing = unique_matrix_tube_id()
    bs_a = await _seed_biosample_with_matrix_tube_id(
        ctx, matrix_tube_id=tube_a, owner_idx=owner_idx
    )
    bs_b = await _seed_biosample_with_matrix_tube_id(
        ctx, matrix_tube_id=tube_b, owner_idx=owner_idx
    )

    resp = await ctx["wet"].post(
        URL_BIOSAMPLE_LOOKUP_BY_MATRIX_TUBE_ID,
        json={"matrix_tube_ids": [tube_a, tube_b, tube_missing]},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "resolved": {tube_a: bs_a, tube_b: bs_b},
        "missing": [tube_missing],
    }


async def test_lookup_by_matrix_tube_id_rejects_bad_format_422(ctx):
    """Tests the case where any element of `matrix_tube_ids` violates the
    digits-only format: the per-element validator on the request model
    rejects the body at the wire boundary with 422.
    """
    resp = await ctx["wet"].post(
        URL_BIOSAMPLE_LOOKUP_BY_MATRIX_TUBE_ID,
        json={"matrix_tube_ids": [unique_matrix_tube_id(), "abc"]},
    )
    assert resp.status_code == 422
