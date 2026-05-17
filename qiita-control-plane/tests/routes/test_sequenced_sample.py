"""Integration tests for the sequenced-sample composer route:

  POST /api/v1/sequencing-run/{run_idx}/sequenced-pool/{pool_idx}/sequenced-sample

Exercises happy paths (single-study and multi-study via primary +
secondary), the role / scope guards, regular-user rejection, Pydantic
body validation including the primary-in-secondary rejection, the
(run, pool) path-consistency 422, owner eligibility 422,
unknown-metadata-field 422, missing biosample-link 422, duplicate
sequenced_pool_item_id 409, and full transaction rollback on
trigger-raised failures.
"""

import secrets

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from qiita_control_plane.main import app
from qiita_control_plane.testing.db_seeds import (
    seed_biosample,
    seed_biosample_to_study_link,
    seed_user_principal,
)

from .conftest import (
    OWNER_INELIGIBILITY_KINDS,
    IneligibilityKind,
    assert_owner_ineligibility_422,
    delete_idxs,
    resolve_ineligible_owner_idx,
    unique_instrument_id,
)

pytestmark = pytest.mark.db


_SEED_PREFIX = "ss-route"
_ELIGIBILITY_DETAIL = "owner is not eligible to own prep samples"


def _unique_item_id(prefix: str = "ITEM") -> str:
    return f"{prefix}-{secrets.token_hex(6)}"


# ---------------------------------------------------------------------------
# FK-reverse cleanup
# ---------------------------------------------------------------------------


async def _cleanup_tracked(pool, created: dict) -> None:
    """Drop tracked rows in FK-reverse order.

    Order matters because of ON DELETE RESTRICT FKs throughout the chain:
      prep_sample_metadata
      prep_sample_study_field (bulk-scoped to test-owned studies)
      prep_sample_to_study (composite PK)
      sequenced_sample
      prep_sample
      sequenced_pool
      sequencing_run
      biosample_to_study (composite PK)
      biosample
      study
      principals

    prep_sample_study_field rows are bulk-deleted by parent study because
    each test seeds its own study (see _seed_study) — no other test can
    plant fields on a study this test owns, so the parent-FK delete is
    safe and avoids the per-row snapshot bookkeeping the response payload
    used to enable.
    """
    await delete_idxs(pool, "prep_sample_metadata", created["prep_sample_metadata"])
    if created["study"]:
        await pool.execute(
            "DELETE FROM qiita.prep_sample_study_field WHERE study_idx = ANY($1::bigint[])",
            created["study"],
        )
    for ps, st in created["prep_sample_to_study"]:
        await pool.execute(
            "DELETE FROM qiita.prep_sample_to_study WHERE prep_sample_idx = $1 AND study_idx = $2",
            ps,
            st,
        )
    await delete_idxs(pool, "sequenced_sample", created["sequenced_sample"])
    await delete_idxs(pool, "prep_sample", created["prep_sample"])
    await delete_idxs(pool, "sequenced_pool", created["sequenced_pool"])
    await delete_idxs(pool, "sequencing_run", created["sequencing_run"])
    for bs, st in created["biosample_to_study"]:
        await pool.execute(
            "DELETE FROM qiita.biosample_to_study WHERE biosample_idx = $1 AND study_idx = $2",
            bs,
            st,
        )
    await delete_idxs(pool, "biosample", created["biosample"])
    await delete_idxs(pool, "study", created["study"])
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
    """Per-test fixture: route-keyed clients plus a `created` tracker for
    FK-reverse teardown over every table the composer writes (plus its
    inputs the test seeds)."""
    created: dict = {
        "prep_sample_metadata": [],
        "prep_sample_to_study": [],
        "sequenced_sample": [],
        "prep_sample": [],
        "sequenced_pool": [],
        "sequencing_run": [],
        "biosample_to_study": [],
        "biosample": [],
        "study": [],
        "user_principals": [],
        "service_account_principals": [],
    }
    yield {**role_keyed_clients, "created": created}
    await _cleanup_tracked(role_keyed_clients["pool"], created)


# ---------------------------------------------------------------------------
# Test-local seed helpers
# ---------------------------------------------------------------------------


async def _seed_study(ctx, *, owner_idx: int, suffix: str) -> int:
    """Insert a minimal study row and track it."""
    idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        owner_idx,
        f"ss-route-study-{suffix}-{secrets.token_hex(4)}",
    )
    ctx["created"]["study"].append(idx)
    return idx


async def _seed_biosample_linked_to_study(ctx, *, owner_idx: int, study_idx: int) -> int:
    """Insert a biosample owned by `owner_idx`, link it (non-retired) to the
    study, and track both rows. The biosample-to-study link is required by
    the prep_sample_to_study_reject_without_biosample_link trigger.
    """
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


async def _seed_run_and_pool(ctx, suffix: str) -> tuple[int, int]:
    """Insert one sequencing_run and one sequenced_pool attached to it;
    track both. Returns (run_idx, pool_idx)."""
    creator = ctx["wet_session"]["principal_idx"]
    run_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.sequencing_run (instrument_run_id, platform, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, $2) RETURNING idx",
        unique_instrument_id(suffix),
        creator,
    )
    ctx["created"]["sequencing_run"].append(run_idx)
    pool_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.sequenced_pool"
        " (sequencing_run_idx, run_preflight_blob, run_preflight_filename, created_by_idx)"
        " VALUES ($1, $2, $3, $4) RETURNING idx",
        run_idx,
        b"\x00\x01\x02",
        f"preflight-{suffix}.sqlite",
        creator,
    )
    ctx["created"]["sequenced_pool"].append(pool_idx)
    return run_idx, pool_idx


async def _fetch_prep_protocol_idx(ctx, name: str = "short_read_metagenomics") -> int:
    """Resolve a seeded prep_protocol idx by name."""
    return await ctx["pool"].fetchval("SELECT idx FROM qiita.prep_protocol WHERE name = $1", name)


async def _post_sequenced_sample(client, ctx, run_idx, pool_idx, **body):
    """POST the composer route; on 201, track every row the composer wrote."""
    resp = await client.post(
        f"/api/v1/sequencing-run/{run_idx}/sequenced-pool/{pool_idx}/sequenced-sample",
        json=body,
    )
    if resp.status_code == 201:
        rj = resp.json()
        ps_idx = rj["prep_sample_idx"]
        ss_idx = rj["sequenced_sample_idx"]
        ctx["created"]["prep_sample"].append(ps_idx)
        ctx["created"]["sequenced_sample"].append(ss_idx)
        # Track every per-test study link the composer wrote: the primary
        # plus any secondaries.
        ctx["created"]["prep_sample_to_study"].append((ps_idx, body["primary_study_idx"]))
        for st in body.get("secondary_study_idxs", []):
            ctx["created"]["prep_sample_to_study"].append((ps_idx, st))
        # Track prep_sample_metadata rows by looking them up after the call.
        meta_rows = await ctx["pool"].fetch(
            "SELECT idx FROM qiita.prep_sample_metadata WHERE prep_sample_idx = $1",
            ps_idx,
        )
        for r in meta_rows:
            ctx["created"]["prep_sample_metadata"].append(r["idx"])
    return resp


# ===========================================================================
# Happy paths
# ===========================================================================


async def test_import_sequenced_sample_from_run_wet_lab_admin_minimal(ctx):
    # Single study, no metadata, no accessions. Verifies the prep_sample,
    # sequenced_sample, and prep_sample_to_study rows all land and that
    # the response shape matches the model.
    run_idx, pool_idx = await _seed_run_and_pool(ctx, "wet-min")
    study_idx = await _seed_study(ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="min")
    bs_idx = await _seed_biosample_linked_to_study(
        ctx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        study_idx=study_idx,
    )
    protocol_idx = await _fetch_prep_protocol_idx(ctx)
    item_id = _unique_item_id("WET-MIN")

    resp = await _post_sequenced_sample(
        ctx["wet"],
        ctx,
        run_idx,
        pool_idx,
        biosample_idx=bs_idx,
        prep_protocol_idx=protocol_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        sequenced_pool_item_id=item_id,
        primary_study_idx=study_idx,
    )
    assert resp.status_code == 201, resp.text
    rj = resp.json()
    expected = {
        # Auto-generated; copy actual into expected so the equality
        # confirms field presence without pinning idx values.
        "prep_sample_idx": rj["prep_sample_idx"],
        "sequenced_sample_idx": rj["sequenced_sample_idx"],
    }
    assert rj == expected

    # Verify the rows landed with the expected linkage.
    ps_row = await ctx["pool"].fetchrow(
        "SELECT biosample_idx, owner_idx, prep_protocol_idx, processing_kind,"
        " created_by_idx FROM qiita.prep_sample WHERE idx = $1",
        rj["prep_sample_idx"],
    )
    expected_ps_row = {
        "biosample_idx": bs_idx,
        "owner_idx": ctx["wet_session"]["principal_idx"],
        "prep_protocol_idx": protocol_idx,
        "processing_kind": "sequenced",
        "created_by_idx": ctx["wet_session"]["principal_idx"],
    }
    assert dict(ps_row) == expected_ps_row

    ss_row = await ctx["pool"].fetchrow(
        "SELECT prep_sample_idx, sequenced_pool_idx, sequenced_pool_item_id,"
        " processing_kind, ena_experiment_accession, ena_run_accession"
        " FROM qiita.sequenced_sample WHERE idx = $1",
        rj["sequenced_sample_idx"],
    )
    expected_ss_row = {
        "prep_sample_idx": rj["prep_sample_idx"],
        "sequenced_pool_idx": pool_idx,
        "sequenced_pool_item_id": item_id,
        "processing_kind": "sequenced",
        "ena_experiment_accession": None,
        "ena_run_accession": None,
    }
    assert dict(ss_row) == expected_ss_row


async def test_import_sequenced_sample_from_run_with_metadata(ctx):
    # Happy path that exercises the metadata path against a seeded global
    # field (alias). Verifies the row count, the prep_sample_study_field
    # created flag, and the value_text round-trip.
    run_idx, pool_idx = await _seed_run_and_pool(ctx, "wet-meta")
    study_idx = await _seed_study(ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="meta")
    bs_idx = await _seed_biosample_linked_to_study(
        ctx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        study_idx=study_idx,
    )
    protocol_idx = await _fetch_prep_protocol_idx(ctx)
    item_id = _unique_item_id("WET-META")

    resp = await _post_sequenced_sample(
        ctx["wet"],
        ctx,
        run_idx,
        pool_idx,
        biosample_idx=bs_idx,
        prep_protocol_idx=protocol_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        sequenced_pool_item_id=item_id,
        primary_study_idx=study_idx,
        metadata={"Alias": "amp-001", "Title": "Wet-lab amplicon prep 001"},
    )
    assert resp.status_code == 201, resp.text
    rj = resp.json()

    # Verify the two metadata rows landed with the right text values.
    # The join through prep_sample_study_field implicitly proves both
    # field rows were created on this fresh study.
    rows = await ctx["pool"].fetch(
        "SELECT psf.display_name, psm.value_text"
        " FROM qiita.prep_sample_metadata psm"
        " JOIN qiita.prep_sample_study_field psf"
        "   ON psf.idx = psm.prep_sample_study_field_idx"
        " WHERE psm.prep_sample_idx = $1"
        " ORDER BY psf.display_name",
        rj["prep_sample_idx"],
    )
    assert [(r["display_name"], r["value_text"]) for r in rows] == [
        ("Alias", "amp-001"),
        ("Title", "Wet-lab amplicon prep 001"),
    ]


# ===========================================================================
# Auth / scope / role guards
# ===========================================================================


async def test_import_sequenced_sample_from_run_anonymous_401(ctx):
    app.state.pool = ctx["pool"]
    run_idx, pool_idx = await _seed_run_and_pool(ctx, "anon")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.post(
            f"/api/v1/sequencing-run/{run_idx}/sequenced-pool/{pool_idx}/sequenced-sample",
            json={
                "biosample_idx": 1,
                "prep_protocol_idx": 1,
                "owner_idx": 1,
                "sequenced_pool_item_id": "X",
                "primary_study_idx": 1,
            },
        )
    assert resp.status_code == 401


async def test_import_sequenced_sample_from_run_missing_scope_403(ctx, no_prep_sample_write_client):
    run_idx, pool_idx = await _seed_run_and_pool(ctx, "noscope")
    resp = await no_prep_sample_write_client.post(
        f"/api/v1/sequencing-run/{run_idx}/sequenced-pool/{pool_idx}/sequenced-sample",
        json={
            "biosample_idx": 1,
            "prep_protocol_idx": 1,
            "owner_idx": 1,
            "sequenced_pool_item_id": "X",
            "primary_study_idx": 1,
        },
    )
    assert resp.status_code == 403
    assert "prep_sample:write" in resp.json()["detail"]


async def test_import_sequenced_sample_from_run_regular_user_role_403(
    ctx, regular_user_with_prep_sample_write_client
):
    # Regular user holding an explicit PREP_SAMPLE_WRITE-bearing PAT:
    # require_scope passes, then require_role_at_least(WET_LAB_ADMIN)
    # rejects. The session-fixture user token does not carry
    # PREP_SAMPLE_WRITE (excluded from the USER role ceiling), so a
    # dedicated PAT is needed to exercise the role gate independently.
    run_idx, pool_idx = await _seed_run_and_pool(ctx, "user")
    resp = await regular_user_with_prep_sample_write_client.post(
        f"/api/v1/sequencing-run/{run_idx}/sequenced-pool/{pool_idx}/sequenced-sample",
        json={
            "biosample_idx": 1,
            "prep_protocol_idx": 1,
            "owner_idx": 1,
            "sequenced_pool_item_id": "X",
            "primary_study_idx": 1,
        },
    )
    assert resp.status_code == 403
    assert "wet_lab_admin" in resp.json()["detail"]


# ===========================================================================
# Owner eligibility — collapsed 422 surface
# ===========================================================================


@pytest.mark.parametrize("kind", OWNER_INELIGIBILITY_KINDS)
async def test_import_sequenced_sample_from_run_owner_ineligible_422(ctx, kind: IneligibilityKind):
    # Every ineligibility case collapses to one 422 detail; same shape as
    # the biosample import route exercises.
    run_idx, pool_idx = await _seed_run_and_pool(ctx, f"elig-{kind}")
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix=f"elig-{kind}"
    )
    bs_idx = await _seed_biosample_linked_to_study(
        ctx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        study_idx=study_idx,
    )
    protocol_idx = await _fetch_prep_protocol_idx(ctx)
    owner_idx = await resolve_ineligible_owner_idx(
        ctx["pool"],
        kind=kind,
        prefix=f"{_SEED_PREFIX}-elig",
        created=ctx["created"],
    )

    async def _post(idx: int):
        return await _post_sequenced_sample(
            ctx["wet"],
            ctx,
            run_idx,
            pool_idx,
            biosample_idx=bs_idx,
            prep_protocol_idx=protocol_idx,
            owner_idx=idx,
            sequenced_pool_item_id=_unique_item_id("ELIG"),
            primary_study_idx=study_idx,
        )

    await assert_owner_ineligibility_422(
        post_with_owner_idx=_post,
        expected_detail=_ELIGIBILITY_DETAIL,
        owner_idx=owner_idx,
    )


# ===========================================================================
# Path / data validation
# ===========================================================================


async def test_import_sequenced_sample_from_run_nonexistent_pool_404(ctx):
    run_idx, _ = await _seed_run_and_pool(ctx, "nopool")
    max_pool = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.sequenced_pool")
    resp = await ctx["wet"].post(
        f"/api/v1/sequencing-run/{run_idx}/sequenced-pool/{max_pool + 100_000}/sequenced-sample",
        json={
            "biosample_idx": 1,
            "prep_protocol_idx": 1,
            "owner_idx": ctx["wet_session"]["principal_idx"],
            "sequenced_pool_item_id": "X",
            "primary_study_idx": 1,
        },
    )
    assert resp.status_code == 404
    assert "sequenced_pool" in resp.json()["detail"]


async def test_import_sequenced_sample_from_run_pool_belongs_to_different_run_422(ctx):
    # Two distinct runs and one pool attached to the second one. Posting
    # to (run_a, pool_for_b) must fail the path-consistency check with 422.
    run_a, _ = await _seed_run_and_pool(ctx, "path-a")
    _, pool_for_b = await _seed_run_and_pool(ctx, "path-b")
    resp = await ctx["wet"].post(
        f"/api/v1/sequencing-run/{run_a}/sequenced-pool/{pool_for_b}/sequenced-sample",
        json={
            "biosample_idx": 1,
            "prep_protocol_idx": 1,
            "owner_idx": ctx["wet_session"]["principal_idx"],
            "sequenced_pool_item_id": "X",
            "primary_study_idx": 1,
        },
    )
    assert resp.status_code == 422
    assert "does not belong to" in resp.json()["detail"]


async def test_import_sequenced_sample_from_run_primary_in_secondary_422(ctx):
    # The model_validator on SequencedSampleCreateRequest rejects a request
    # whose primary_study_idx also appears in secondary_study_idxs.
    run_idx, pool_idx = await _seed_run_and_pool(ctx, "p-in-s")
    resp = await ctx["wet"].post(
        f"/api/v1/sequencing-run/{run_idx}/sequenced-pool/{pool_idx}/sequenced-sample",
        json={
            "biosample_idx": 1,
            "prep_protocol_idx": 1,
            "owner_idx": ctx["wet_session"]["principal_idx"],
            "sequenced_pool_item_id": "X",
            "primary_study_idx": 1,
            "secondary_study_idxs": [1, 2],
        },
    )
    assert resp.status_code == 422
    assert any("primary_study_idx" in err.get("msg", "") for err in resp.json()["detail"])


async def test_import_sequenced_sample_from_run_multi_study_happy_path(ctx):
    # Two studies, both linked to the biosample. Primary owns the metadata
    # field row; secondary still gets a prep_sample_to_study row but does
    # not own a prep_sample_study_field.
    run_idx, pool_idx = await _seed_run_and_pool(ctx, "multi-ok")
    primary_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="multi-ok-p"
    )
    secondary_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="multi-ok-s"
    )
    # Biosample must be linked to every requested study or the
    # prep_sample_to_study_reject_without_biosample_link trigger fires.
    bs_idx = await _seed_biosample_linked_to_study(
        ctx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        study_idx=primary_idx,
    )
    await seed_biosample_to_study_link(
        ctx["pool"],
        biosample_idx=bs_idx,
        study_idx=secondary_idx,
        created_by_idx=ctx["wet_session"]["principal_idx"],
    )
    ctx["created"]["biosample_to_study"].append((bs_idx, secondary_idx))
    protocol_idx = await _fetch_prep_protocol_idx(ctx)

    resp = await _post_sequenced_sample(
        ctx["wet"],
        ctx,
        run_idx,
        pool_idx,
        biosample_idx=bs_idx,
        prep_protocol_idx=protocol_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        sequenced_pool_item_id=_unique_item_id("MULTI"),
        primary_study_idx=primary_idx,
        secondary_study_idxs=[secondary_idx],
        metadata={"Alias": "multi-001"},
    )
    assert resp.status_code == 201, resp.text
    rj = resp.json()

    # Both prep_sample_to_study rows landed.
    linked = await ctx["pool"].fetch(
        "SELECT study_idx FROM qiita.prep_sample_to_study WHERE prep_sample_idx = $1"
        " ORDER BY study_idx",
        rj["prep_sample_idx"],
    )
    assert sorted(r["study_idx"] for r in linked) == sorted([primary_idx, secondary_idx])


async def test_import_sequenced_sample_from_run_extra_field_422(ctx):
    # Request model carries extra="forbid".
    run_idx, pool_idx = await _seed_run_and_pool(ctx, "xtra")
    resp = await ctx["wet"].post(
        f"/api/v1/sequencing-run/{run_idx}/sequenced-pool/{pool_idx}/sequenced-sample",
        json={
            "biosample_idx": 1,
            "prep_protocol_idx": 1,
            "owner_idx": ctx["wet_session"]["principal_idx"],
            "sequenced_pool_item_id": "X",
            "primary_study_idx": 1,
            "not_a_field": 5,
        },
    )
    assert resp.status_code == 422


async def test_import_sequenced_sample_from_run_missing_required_field_422(ctx):
    run_idx, pool_idx = await _seed_run_and_pool(ctx, "missing")
    resp = await ctx["wet"].post(
        f"/api/v1/sequencing-run/{run_idx}/sequenced-pool/{pool_idx}/sequenced-sample",
        json={},
    )
    assert resp.status_code == 422
    missing_locs = {tuple(err["loc"]) for err in resp.json()["detail"]}
    assert ("body", "biosample_idx") in missing_locs
    assert ("body", "prep_protocol_idx") in missing_locs
    assert ("body", "owner_idx") in missing_locs
    assert ("body", "sequenced_pool_item_id") in missing_locs
    assert ("body", "primary_study_idx") in missing_locs


# ===========================================================================
# Composer error mappings
# ===========================================================================


async def test_import_sequenced_sample_from_run_unknown_metadata_field_422(ctx):
    run_idx, pool_idx = await _seed_run_and_pool(ctx, "unk")
    study_idx = await _seed_study(ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="unk")
    bs_idx = await _seed_biosample_linked_to_study(
        ctx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        study_idx=study_idx,
    )
    protocol_idx = await _fetch_prep_protocol_idx(ctx)
    bad_a = f"Unknown A {secrets.token_hex(4)}"
    bad_b = f"Unknown B {secrets.token_hex(4)}"

    resp = await _post_sequenced_sample(
        ctx["wet"],
        ctx,
        run_idx,
        pool_idx,
        biosample_idx=bs_idx,
        prep_protocol_idx=protocol_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        sequenced_pool_item_id=_unique_item_id("UNK"),
        primary_study_idx=study_idx,
        metadata={bad_a: "x", bad_b: "y"},
    )
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert bad_a in detail
    assert bad_b in detail


async def test_import_sequenced_sample_from_run_missing_biosample_link_422(ctx):
    # The biosample is owned by the wet_lab_admin but is NOT linked to the
    # study. The prep_sample_to_study_reject_without_biosample_link trigger
    # fires inside the composer's link INSERT; the route maps the marker
    # substring to 422.
    run_idx, pool_idx = await _seed_run_and_pool(ctx, "nolink")
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="nolink"
    )
    bs_idx = await seed_biosample(
        ctx["pool"],
        owner_idx=ctx["wet_session"]["principal_idx"],
        created_by_idx=ctx["wet_session"]["principal_idx"],
    )
    ctx["created"]["biosample"].append(bs_idx)
    protocol_idx = await _fetch_prep_protocol_idx(ctx)

    resp = await _post_sequenced_sample(
        ctx["wet"],
        ctx,
        run_idx,
        pool_idx,
        biosample_idx=bs_idx,
        prep_protocol_idx=protocol_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        sequenced_pool_item_id=_unique_item_id("NOLINK"),
        primary_study_idx=study_idx,
    )
    assert resp.status_code == 422
    assert "not linked" in resp.json()["detail"]

    # Verify the transaction rolled back — no prep_sample row exists for
    # this biosample.
    leftover = await ctx["pool"].fetchval(
        "SELECT COUNT(*) FROM qiita.prep_sample WHERE biosample_idx = $1",
        bs_idx,
    )
    assert leftover == 0


async def test_import_sequenced_sample_from_run_duplicate_pool_item_id_409(ctx):
    # Two POSTs to the same pool with the same sequenced_pool_item_id; the
    # second trips sequenced_sample_pool_item_id_unique and the route maps
    # to 409 with the human-readable detail.
    run_idx, pool_idx = await _seed_run_and_pool(ctx, "dup")
    study_idx = await _seed_study(ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="dup")
    bs_idx = await _seed_biosample_linked_to_study(
        ctx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        study_idx=study_idx,
    )
    protocol_idx = await _fetch_prep_protocol_idx(ctx)
    item_id = _unique_item_id("DUP")

    common = dict(
        biosample_idx=bs_idx,
        prep_protocol_idx=protocol_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        sequenced_pool_item_id=item_id,
        primary_study_idx=study_idx,
    )
    r1 = await _post_sequenced_sample(ctx["wet"], ctx, run_idx, pool_idx, **common)
    r2 = await _post_sequenced_sample(ctx["wet"], ctx, run_idx, pool_idx, **common)
    assert r1.status_code == 201, r1.text
    assert r2.status_code == 409
    assert r2.json()["detail"] == "sequenced_pool_item_id already in use for this pool"


async def test_import_sequenced_sample_from_run_unknown_biosample_idx_422(ctx):
    # An idx past MAX(biosample.idx) trips the prep_sample_biosample_idx_fkey
    # FK; the composer surfaces ForeignKeyViolationError and the route maps
    # it to 422 with the user-friendly detail.
    run_idx, pool_idx = await _seed_run_and_pool(ctx, "bad-bs")
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="bad-bs"
    )
    protocol_idx = await _fetch_prep_protocol_idx(ctx)
    max_bs = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.biosample")

    resp = await _post_sequenced_sample(
        ctx["wet"],
        ctx,
        run_idx,
        pool_idx,
        biosample_idx=max_bs + 100_000,
        prep_protocol_idx=protocol_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        sequenced_pool_item_id=_unique_item_id("BAD-BS"),
        primary_study_idx=study_idx,
    )
    assert resp.status_code == 422
    assert "biosample" in resp.json()["detail"]


# ===========================================================================
# Auth: system_admin happy path
# ===========================================================================


async def test_import_sequenced_sample_from_run_system_admin_happy_path(ctx):
    # system_admin caller acts on behalf of a separate user.
    target_idx = await seed_user_principal(ctx["pool"], prefix=_SEED_PREFIX, suffix="adm-target")
    ctx["created"]["user_principals"].append(target_idx)

    run_idx, pool_idx = await _seed_run_and_pool(ctx, "adm")
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["admin_session"]["principal_idx"], suffix="adm"
    )
    bs_idx = await _seed_biosample_linked_to_study(
        ctx,
        owner_idx=target_idx,
        study_idx=study_idx,
    )
    protocol_idx = await _fetch_prep_protocol_idx(ctx)

    resp = await _post_sequenced_sample(
        ctx["admin"],
        ctx,
        run_idx,
        pool_idx,
        biosample_idx=bs_idx,
        prep_protocol_idx=protocol_idx,
        owner_idx=target_idx,
        sequenced_pool_item_id=_unique_item_id("ADM"),
        primary_study_idx=study_idx,
    )
    assert resp.status_code == 201, resp.text

    # Verify created_by_idx is the system_admin caller, owner_idx is the
    # target user, and the row's processing_kind is 'sequenced'.
    ps = await ctx["pool"].fetchrow(
        "SELECT owner_idx, created_by_idx, processing_kind FROM qiita.prep_sample WHERE idx = $1",
        resp.json()["prep_sample_idx"],
    )
    expected_ps = {
        "owner_idx": target_idx,
        "created_by_idx": ctx["admin_session"]["principal_idx"],
        "processing_kind": "sequenced",
    }
    assert dict(ps) == expected_ps


# ===========================================================================
# Test-local seed helpers for the read endpoints
# ===========================================================================


async def _seed_one_sequenced_sample(
    ctx, suffix: str, *, metadata: dict[str, str] | None = None
) -> dict:
    """Drive the composer route to land one sequenced_sample on a fresh run,
    pool, study, biosample. Returns a dict carrying every idx the read-side
    tests need so callers do not have to re-derive them.
    """
    # Seed every upstream row the composer requires; created_by_idx is the
    # wet_lab_admin caller throughout so the response's created_by_idx is
    # deterministic.
    run_idx, pool_idx = await _seed_run_and_pool(ctx, suffix)
    study_idx = await _seed_study(ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix=suffix)
    bs_idx = await _seed_biosample_linked_to_study(
        ctx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        study_idx=study_idx,
    )
    protocol_idx = await _fetch_prep_protocol_idx(ctx)
    item_id = _unique_item_id(suffix.upper())

    # Land the composite; route tracks each row in ctx for FK-reverse cleanup.
    resp = await _post_sequenced_sample(
        ctx["wet"],
        ctx,
        run_idx,
        pool_idx,
        biosample_idx=bs_idx,
        prep_protocol_idx=protocol_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        sequenced_pool_item_id=item_id,
        primary_study_idx=study_idx,
        metadata=metadata or {},
    )
    assert resp.status_code == 201, resp.text
    rj = resp.json()
    return {
        "run_idx": run_idx,
        "pool_idx": pool_idx,
        "study_idx": study_idx,
        "biosample_idx": bs_idx,
        "protocol_idx": protocol_idx,
        "item_id": item_id,
        "prep_sample_idx": rj["prep_sample_idx"],
        "sequenced_sample_idx": rj["sequenced_sample_idx"],
    }


async def _retire_prep_sample(pool, *, prep_sample_idx: int, retired_by_idx: int) -> None:
    """Flip a prep_sample row to retired via direct SQL; mirrors the
    retire_biosample seed helper. No dedicated seed helper exists yet because
    retirement is otherwise driven by routes that have not landed.
    """
    # Single UPDATE satisfies the retirement-consistent CHECK by populating
    # all three NOT-NULL retirement audit columns alongside the flag flip.
    await pool.execute(
        "UPDATE qiita.prep_sample"
        " SET retired = true, retired_by_idx = $1, retired_at = now(), retire_reason = $2"
        " WHERE idx = $3",
        retired_by_idx,
        "test cleanup",
        prep_sample_idx,
    )


def _expected_read_response(
    seeded: dict,
    *,
    rj: dict,
    owner_idx: int,
    created_by_idx: int,
    caller_system_role: str,
    global_metadata: dict,
    has_metadata: bool,
) -> dict:
    """Build the full SequencedSampleResponse expected dict, copying every
    auto-generated timestamp from the actual response so the equality
    confirms presence-and-shape without pinning timing values.

    Centralises the column-by-column expectation so the parameter list says
    only what varies between tests (owner / creator / role / metadata).
    """
    # The composer touches last_metadata_change_at iff at least one metadata
    # row landed; the value is non-None in that case, otherwise None.
    expected_last_meta = rj["last_metadata_change_at"] if has_metadata else None
    return {
        "sequenced_sample_idx": seeded["sequenced_sample_idx"],
        "prep_sample_idx": seeded["prep_sample_idx"],
        "biosample_idx": seeded["biosample_idx"],
        "owner_idx": owner_idx,
        "prep_protocol_idx": seeded["protocol_idx"],
        "metadata_checklist_idx": None,
        "sequenced_pool_idx": seeded["pool_idx"],
        "sequenced_pool_item_id": seeded["item_id"],
        "ena_experiment_accession": None,
        "ena_run_accession": None,
        "last_submission_at": None,
        "submission_error": None,
        "last_metadata_change_at": expected_last_meta,
        "created_by_idx": created_by_idx,
        "created_at": rj["created_at"],
        "effective_updated_at": rj["effective_updated_at"],
        "retired": False,
        "retired_by_idx": None,
        "retired_at": None,
        "retire_reason": None,
        "global_metadata": global_metadata,
        "caller_system_role": caller_system_role,
    }


# ===========================================================================
# GET /sequencing-run/{idx}/sequenced-sample/list-idxs — happy paths
# ===========================================================================


async def test_list_sequenced_sample_idxs_wet_lab_admin_empty(ctx):
    # Empty run (no pool, no sequenced_sample) returns the zero-row envelope
    # without 404; require_sequencing_run_exists passes because the run row
    # itself exists.
    run_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.sequencing_run (instrument_run_id, platform, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, $2) RETURNING idx",
        unique_instrument_id("list-empty"),
        ctx["wet_session"]["principal_idx"],
    )
    ctx["created"]["sequencing_run"].append(run_idx)

    resp = await ctx["wet"].get(f"/api/v1/sequencing-run/{run_idx}/sequenced-sample/list-idxs")
    assert resp.status_code == 200, resp.text
    expected = {
        "idxs": [],
        "count": 0,
        "truncated": False,
        "caller_system_role": "wet_lab_admin",
    }
    assert resp.json() == expected


async def test_list_sequenced_sample_idxs_wet_lab_admin_returns_newest_first(ctx):
    # Two sequenced_samples on the same run: list must surface them in
    # (created_at DESC, idx DESC) order — second-landed first.
    run_idx, pool_idx = await _seed_run_and_pool(ctx, "list-two")
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="list-two"
    )
    bs_a = await _seed_biosample_linked_to_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], study_idx=study_idx
    )
    bs_b = await _seed_biosample_linked_to_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], study_idx=study_idx
    )
    protocol_idx = await _fetch_prep_protocol_idx(ctx)

    # Land two; track the order so the assertion knows the expected reverse.
    ss_ids = []
    for bs, suffix in ((bs_a, "A"), (bs_b, "B")):
        resp = await _post_sequenced_sample(
            ctx["wet"],
            ctx,
            run_idx,
            pool_idx,
            biosample_idx=bs,
            prep_protocol_idx=protocol_idx,
            owner_idx=ctx["wet_session"]["principal_idx"],
            sequenced_pool_item_id=_unique_item_id(f"LIST-{suffix}"),
            primary_study_idx=study_idx,
        )
        assert resp.status_code == 201, resp.text
        ss_ids.append(resp.json()["sequenced_sample_idx"])

    resp = await ctx["wet"].get(f"/api/v1/sequencing-run/{run_idx}/sequenced-sample/list-idxs")
    assert resp.status_code == 200, resp.text
    expected = {
        "idxs": list(reversed(ss_ids)),
        "count": 2,
        "truncated": False,
        "caller_system_role": "wet_lab_admin",
    }
    assert resp.json() == expected


async def test_list_sequenced_sample_idxs_truncated(ctx, monkeypatch):
    # Drop the hard cap to 1 so two seeded rows force the truncation
    # branch: the route fetches cap+1, flags truncated, then slices back
    # to the cap. The route reads the cap as a module global at call
    # time, so monkeypatching the attribute takes effect.
    monkeypatch.setattr(
        "qiita_control_plane.routes.sequenced_sample._SEQUENCED_SAMPLE_IDXS_HARD_CAP",
        1,
    )
    run_idx, pool_idx = await _seed_run_and_pool(ctx, "list-trunc")
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="list-trunc"
    )
    bs_a = await _seed_biosample_linked_to_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], study_idx=study_idx
    )
    bs_b = await _seed_biosample_linked_to_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], study_idx=study_idx
    )
    protocol_idx = await _fetch_prep_protocol_idx(ctx)

    # Land two sequenced_samples on the one run, newest last.
    ss_ids = []
    for bs, suffix in ((bs_a, "A"), (bs_b, "B")):
        resp = await _post_sequenced_sample(
            ctx["wet"],
            ctx,
            run_idx,
            pool_idx,
            biosample_idx=bs,
            prep_protocol_idx=protocol_idx,
            owner_idx=ctx["wet_session"]["principal_idx"],
            sequenced_pool_item_id=_unique_item_id(f"TRUNC-{suffix}"),
            primary_study_idx=study_idx,
        )
        assert resp.status_code == 201, resp.text
        ss_ids.append(resp.json()["sequenced_sample_idx"])

    resp = await ctx["wet"].get(f"/api/v1/sequencing-run/{run_idx}/sequenced-sample/list-idxs")
    assert resp.status_code == 200, resp.text
    # cap=1 keeps only the newest (second-landed) row after the slice.
    expected = {
        "idxs": [ss_ids[1]],
        "count": 1,
        "truncated": True,
        "caller_system_role": "wet_lab_admin",
    }
    assert resp.json() == expected


async def test_list_sequenced_sample_idxs_excludes_retired_prep_sample(ctx):
    # Two sequenced_samples; retire the supertype prep_sample of one. The
    # list response carries only the non-retired one.
    run_idx, pool_idx = await _seed_run_and_pool(ctx, "list-ret")
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="list-ret"
    )
    bs_a = await _seed_biosample_linked_to_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], study_idx=study_idx
    )
    bs_b = await _seed_biosample_linked_to_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], study_idx=study_idx
    )
    protocol_idx = await _fetch_prep_protocol_idx(ctx)

    # Land both, then retire the prep_sample under the first one.
    landed = []
    for bs, suffix in ((bs_a, "X"), (bs_b, "Y")):
        resp = await _post_sequenced_sample(
            ctx["wet"],
            ctx,
            run_idx,
            pool_idx,
            biosample_idx=bs,
            prep_protocol_idx=protocol_idx,
            owner_idx=ctx["wet_session"]["principal_idx"],
            sequenced_pool_item_id=_unique_item_id(f"RET-{suffix}"),
            primary_study_idx=study_idx,
        )
        assert resp.status_code == 201, resp.text
        landed.append(resp.json())
    await _retire_prep_sample(
        ctx["pool"],
        prep_sample_idx=landed[0]["prep_sample_idx"],
        retired_by_idx=ctx["wet_session"]["principal_idx"],
    )

    resp = await ctx["wet"].get(f"/api/v1/sequencing-run/{run_idx}/sequenced-sample/list-idxs")
    assert resp.status_code == 200, resp.text
    # Only the second sequenced_sample survives the retired-prep filter.
    expected = {
        "idxs": [landed[1]["sequenced_sample_idx"]],
        "count": 1,
        "truncated": False,
        "caller_system_role": "wet_lab_admin",
    }
    assert resp.json() == expected


async def test_list_sequenced_sample_idxs_system_admin_happy_path(ctx):
    # system_admin caller passes the require_role_at_least(WET_LAB_ADMIN)
    # gate via the hierarchical role check; response carries the actual role.
    seeded = await _seed_one_sequenced_sample(ctx, "list-adm")

    resp = await ctx["admin"].get(
        f"/api/v1/sequencing-run/{seeded['run_idx']}/sequenced-sample/list-idxs"
    )
    assert resp.status_code == 200, resp.text
    expected = {
        "idxs": [seeded["sequenced_sample_idx"]],
        "count": 1,
        "truncated": False,
        "caller_system_role": "system_admin",
    }
    assert resp.json() == expected


# ===========================================================================
# GET /sequencing-run/{idx}/sequenced-sample/list-idxs — auth / 404
# ===========================================================================


async def test_list_sequenced_sample_idxs_anonymous_401(ctx):
    seeded = await _seed_one_sequenced_sample(ctx, "list-anon")
    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.get(
            f"/api/v1/sequencing-run/{seeded['run_idx']}/sequenced-sample/list-idxs"
        )
    assert resp.status_code == 401


async def test_list_sequenced_sample_idxs_missing_scope_403(ctx, no_prep_sample_read_client):
    seeded = await _seed_one_sequenced_sample(ctx, "list-noscope")
    resp = await no_prep_sample_read_client.get(
        f"/api/v1/sequencing-run/{seeded['run_idx']}/sequenced-sample/list-idxs"
    )
    assert resp.status_code == 403
    assert "prep_sample:read" in resp.json()["detail"]


async def test_list_sequenced_sample_idxs_regular_user_role_403(ctx):
    # Regular user session carries PREP_SAMPLE_READ via the USER role ceiling,
    # so the scope gate passes and require_role_at_least(WET_LAB_ADMIN) is
    # what trips — distinct rejection from the missing-scope path above.
    seeded = await _seed_one_sequenced_sample(ctx, "list-user")
    resp = await ctx["user"].get(
        f"/api/v1/sequencing-run/{seeded['run_idx']}/sequenced-sample/list-idxs"
    )
    assert resp.status_code == 403
    assert "wet_lab_admin" in resp.json()["detail"]


async def test_list_sequenced_sample_idxs_nonexistent_run_404(ctx):
    max_run = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.sequencing_run")
    resp = await ctx["wet"].get(
        f"/api/v1/sequencing-run/{max_run + 100_000}/sequenced-sample/list-idxs"
    )
    assert resp.status_code == 404
    assert "sequencing_run" in resp.json()["detail"]


# ===========================================================================
# GET /sequenced-sample/{idx} — happy paths
# ===========================================================================


async def test_get_sequenced_sample_wet_lab_admin_no_metadata(ctx):
    # Minimal happy path: no metadata, no accessions; full-object equality
    # against the response with timestamps copied from actual.
    seeded = await _seed_one_sequenced_sample(ctx, "get-min")

    resp = await ctx["wet"].get(f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}")
    assert resp.status_code == 200, resp.text
    rj = resp.json()
    expected = _expected_read_response(
        seeded,
        rj=rj,
        owner_idx=ctx["wet_session"]["principal_idx"],
        created_by_idx=ctx["wet_session"]["principal_idx"],
        caller_system_role="wet_lab_admin",
        global_metadata={},
        has_metadata=False,
    )
    assert rj == expected

    # ETag is set in RFC 7232 quoted form; opaque to the test beyond shape.
    etag = resp.headers["ETag"]
    assert etag.startswith('"') and etag.endswith('"')


async def test_get_sequenced_sample_carries_global_metadata(ctx):
    # Two pre-seeded prep_sample_global_field display_names (Alias, Title)
    # land via the composer; the GET surfaces both keyed on internal_name.
    seeded = await _seed_one_sequenced_sample(
        ctx,
        "get-meta",
        metadata={"Alias": "amp-007", "Title": "Wet-lab amplicon prep 007"},
    )

    resp = await ctx["wet"].get(f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}")
    assert resp.status_code == 200, resp.text
    rj = resp.json()
    # Migration 20260501000010 pins the seeded global fields' internal_names
    # to 'alias' and 'title'; the GET keys global_metadata on internal_name.
    expected_metadata = {
        "alias": {
            "display_name": "Alias",
            "description": None,
            "data_type": "text",
            "value": "amp-007",
        },
        "title": {
            "display_name": "Title",
            "description": None,
            "data_type": "text",
            "value": "Wet-lab amplicon prep 007",
        },
    }
    expected = _expected_read_response(
        seeded,
        rj=rj,
        owner_idx=ctx["wet_session"]["principal_idx"],
        created_by_idx=ctx["wet_session"]["principal_idx"],
        caller_system_role="wet_lab_admin",
        global_metadata=expected_metadata,
        has_metadata=True,
    )
    assert rj == expected


async def test_get_sequenced_sample_system_admin_happy_path(ctx):
    # system_admin satisfies the role gate via the hierarchical check; full
    # response equality confirms caller_system_role surfaces correctly.
    seeded = await _seed_one_sequenced_sample(ctx, "get-adm")
    resp = await ctx["admin"].get(f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}")
    assert resp.status_code == 200, resp.text
    rj = resp.json()
    expected = _expected_read_response(
        seeded,
        rj=rj,
        owner_idx=ctx["wet_session"]["principal_idx"],
        created_by_idx=ctx["wet_session"]["principal_idx"],
        caller_system_role="system_admin",
        global_metadata={},
        has_metadata=False,
    )
    assert rj == expected


# ===========================================================================
# GET /sequenced-sample/{idx} — auth / 404
# ===========================================================================


async def test_get_sequenced_sample_anonymous_401(ctx):
    seeded = await _seed_one_sequenced_sample(ctx, "get-anon")
    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.get(f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}")
    assert resp.status_code == 401


async def test_get_sequenced_sample_missing_scope_403(ctx, no_prep_sample_read_client):
    seeded = await _seed_one_sequenced_sample(ctx, "get-noscope")
    resp = await no_prep_sample_read_client.get(
        f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}"
    )
    assert resp.status_code == 403
    assert "prep_sample:read" in resp.json()["detail"]


async def test_get_sequenced_sample_regular_user_role_403(ctx):
    # Default regular-user session carries PREP_SAMPLE_READ; scope passes,
    # require_role_at_least(WET_LAB_ADMIN) is what rejects.
    seeded = await _seed_one_sequenced_sample(ctx, "get-user")
    resp = await ctx["user"].get(f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}")
    assert resp.status_code == 403
    assert "wet_lab_admin" in resp.json()["detail"]


async def test_get_sequenced_sample_nonexistent_404(ctx):
    max_ss = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.sequenced_sample")
    resp = await ctx["wet"].get(f"/api/v1/sequenced-sample/{max_ss + 100_000}")
    assert resp.status_code == 404
    assert "sequenced_sample" in resp.json()["detail"]


async def test_get_sequenced_sample_retired_prep_sample_404(ctx):
    # The retired-row carve-out 404s a sequenced_sample whose supertype
    # prep_sample has been retired, matching the biosample GET surface.
    seeded = await _seed_one_sequenced_sample(ctx, "get-ret")
    await _retire_prep_sample(
        ctx["pool"],
        prep_sample_idx=seeded["prep_sample_idx"],
        retired_by_idx=ctx["wet_session"]["principal_idx"],
    )

    resp = await ctx["wet"].get(f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}")
    assert resp.status_code == 404
    assert "sequenced_sample" in resp.json()["detail"]


# ===========================================================================
# PATCH /sequenced-sample/{idx} — helpers
# ===========================================================================


async def _get_etag(client, sequenced_sample_idx: int) -> str:
    """GET the resource and return the ETag header value for use as If-Match.

    Avoids reproducing the route's effective_updated_at formula in test
    code — the GET endpoint is the canonical source for the ETag, and
    coupling PATCH tests to it (rather than to the SQL) means a future
    ETag-format change updates both endpoints in one place.
    """
    # Pull the resource and lift the header verbatim.
    resp = await client.get(f"/api/v1/sequenced-sample/{sequenced_sample_idx}")
    assert resp.status_code == 200, resp.text
    return resp.headers["ETag"]


# ===========================================================================
# PATCH /sequenced-sample/{idx} — happy paths
# ===========================================================================


async def test_patch_sequenced_sample_single_field_writes_and_bumps_etag(ctx):
    # PATCH one ENA accession; full-object equality on the response, with
    # the changed column overriding the seeded default and the ETag bumping.
    seeded = await _seed_one_sequenced_sample(ctx, "patch-ena-exp")
    pre_etag = await _get_etag(ctx["wet"], seeded["sequenced_sample_idx"])

    resp = await ctx["wet"].patch(
        f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}",
        json={"ena_experiment_accession": "ERX9000001"},
        headers={"If-Match": pre_etag},
    )
    assert resp.status_code == 200, resp.text
    rj = resp.json()
    expected = _expected_read_response(
        seeded,
        rj=rj,
        owner_idx=ctx["wet_session"]["principal_idx"],
        created_by_idx=ctx["wet_session"]["principal_idx"],
        caller_system_role="wet_lab_admin",
        global_metadata={},
        has_metadata=False,
    )
    expected["ena_experiment_accession"] = "ERX9000001"
    assert rj == expected

    # ETag bumps off the bumped sequenced_sample.updated_at via the trigger.
    new_etag = resp.headers["ETag"]
    assert new_etag != pre_etag
    assert new_etag.startswith('"') and new_etag.endswith('"')


async def test_patch_sequenced_sample_all_subtype_fields(ctx):
    # PATCH all four subtype-table columns in one request; full-object
    # equality confirms every field surfaces and the trigger does not
    # clobber the caller-set submission_error because last_submission_at
    # and submission_error are both supplied in the same UPDATE.
    seeded = await _seed_one_sequenced_sample(ctx, "patch-all")
    pre_etag = await _get_etag(ctx["wet"], seeded["sequenced_sample_idx"])

    body = {
        "ena_experiment_accession": "ERX9000002",
        "ena_run_accession": "ERR9000002",
        "last_submission_at": "2026-01-15T12:00:00+00:00",
        "submission_error": "rejected: schema validation failed",
    }
    resp = await ctx["wet"].patch(
        f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}",
        json=body,
        headers={"If-Match": pre_etag},
    )
    assert resp.status_code == 200, resp.text
    rj = resp.json()
    expected = _expected_read_response(
        seeded,
        rj=rj,
        owner_idx=ctx["wet_session"]["principal_idx"],
        created_by_idx=ctx["wet_session"]["principal_idx"],
        caller_system_role="wet_lab_admin",
        global_metadata={},
        has_metadata=False,
    )
    expected["ena_experiment_accession"] = "ERX9000002"
    expected["ena_run_accession"] = "ERR9000002"
    expected["last_submission_at"] = rj["last_submission_at"]
    expected["submission_error"] = "rejected: schema validation failed"
    assert rj == expected


async def test_patch_sequenced_sample_system_admin_happy_path(ctx):
    # system_admin satisfies the role gate via the hierarchical role check.
    seeded = await _seed_one_sequenced_sample(ctx, "patch-adm")
    pre_etag = await _get_etag(ctx["admin"], seeded["sequenced_sample_idx"])

    resp = await ctx["admin"].patch(
        f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}",
        json={"submission_error": "transient ENA outage"},
        headers={"If-Match": pre_etag},
    )
    assert resp.status_code == 200, resp.text
    rj = resp.json()
    expected = _expected_read_response(
        seeded,
        rj=rj,
        owner_idx=ctx["wet_session"]["principal_idx"],
        created_by_idx=ctx["wet_session"]["principal_idx"],
        caller_system_role="system_admin",
        global_metadata={},
        has_metadata=False,
    )
    expected["submission_error"] = "transient ENA outage"
    assert rj == expected


async def test_patch_sequenced_sample_submission_error_clearing_trigger(ctx):
    # Trigger sequenced_sample_clear_submission_error_on_new_attempt nulls
    # submission_error when last_submission_at changes and the same
    # UPDATE does not also set submission_error. First PATCH seeds an
    # error; second PATCH (last_submission_at only) clears it.
    seeded = await _seed_one_sequenced_sample(ctx, "patch-trig")

    # First PATCH plants a submission_error.
    etag1 = await _get_etag(ctx["wet"], seeded["sequenced_sample_idx"])
    r1 = await ctx["wet"].patch(
        f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}",
        json={"submission_error": "ENA timed out"},
        headers={"If-Match": etag1},
    )
    assert r1.status_code == 200, r1.text
    assert r1.json()["submission_error"] == "ENA timed out"

    # Second PATCH bumps last_submission_at without re-setting submission_error.
    # The trigger detects last_submission_at IS DISTINCT FROM OLD and
    # submission_error IS NOT DISTINCT FROM OLD, and nulls submission_error.
    etag2 = r1.headers["ETag"]
    r2 = await ctx["wet"].patch(
        f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}",
        json={"last_submission_at": "2026-02-01T08:30:00+00:00"},
        headers={"If-Match": etag2},
    )
    assert r2.status_code == 200, r2.text
    rj = r2.json()
    expected = _expected_read_response(
        seeded,
        rj=rj,
        owner_idx=ctx["wet_session"]["principal_idx"],
        created_by_idx=ctx["wet_session"]["principal_idx"],
        caller_system_role="wet_lab_admin",
        global_metadata={},
        has_metadata=False,
    )
    expected["last_submission_at"] = rj["last_submission_at"]
    # Trigger cleared the error from the first PATCH.
    expected["submission_error"] = None
    assert rj == expected


# ===========================================================================
# PATCH /sequenced-sample/{idx} — concurrency
# ===========================================================================


async def test_patch_sequenced_sample_missing_if_match_428(ctx):
    seeded = await _seed_one_sequenced_sample(ctx, "patch-no-im")
    resp = await ctx["wet"].patch(
        f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}",
        json={"ena_experiment_accession": "ERX1"},
    )
    assert resp.status_code == 428
    assert resp.json()["detail"] == "If-Match header required"


async def test_patch_sequenced_sample_stale_if_match_412(ctx):
    # A second PATCH that races against the first sees the post-commit
    # ETag and 412s on its stale If-Match.
    seeded = await _seed_one_sequenced_sample(ctx, "patch-stale")
    pre_etag = await _get_etag(ctx["wet"], seeded["sequenced_sample_idx"])

    # First PATCH commits and bumps the ETag.
    r1 = await ctx["wet"].patch(
        f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}",
        json={"ena_experiment_accession": "ERX1"},
        headers={"If-Match": pre_etag},
    )
    assert r1.status_code == 200, r1.text

    # Second PATCH submits with the stale ETag; 412 with no second write.
    r2 = await ctx["wet"].patch(
        f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}",
        json={"ena_run_accession": "ERR1"},
        headers={"If-Match": pre_etag},
    )
    assert r2.status_code == 412
    assert r2.json()["detail"] == "If-Match did not match"

    # Confirm the second PATCH did not land — ena_run_accession is still NULL.
    leftover = await ctx["pool"].fetchval(
        "SELECT ena_run_accession FROM qiita.sequenced_sample WHERE idx = $1",
        seeded["sequenced_sample_idx"],
    )
    assert leftover is None


# ===========================================================================
# PATCH /sequenced-sample/{idx} — auth / 404 / 409
# ===========================================================================


async def test_patch_sequenced_sample_anonymous_401(ctx):
    seeded = await _seed_one_sequenced_sample(ctx, "patch-anon")
    pre_etag = await _get_etag(ctx["wet"], seeded["sequenced_sample_idx"])
    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.patch(
            f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}",
            json={"ena_experiment_accession": "ERX1"},
            headers={"If-Match": pre_etag},
        )
    assert resp.status_code == 401


async def test_patch_sequenced_sample_non_admin_no_scope_403(ctx, no_prep_sample_write_client):
    # Caller is a regular user whose PAT also lacks Scope.PREP_SAMPLE_WRITE.
    # require_role_at_least is declared (and resolved) before require_scope,
    # so the role guard rejects first: a 403 for the role reason, not the
    # scope reason. This test therefore pins only that such a caller cannot
    # reach the route at all -- it does NOT exercise require_scope in
    # isolation, because no fixture mints a wet_lab_admin PAT stripped of
    # the write scope. The analogous biosample / sequencing_run scope
    # tests share this same limitation.
    seeded = await _seed_one_sequenced_sample(ctx, "patch-noscope")
    pre_etag = await _get_etag(ctx["wet"], seeded["sequenced_sample_idx"])
    resp = await no_prep_sample_write_client.patch(
        f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}",
        json={"ena_experiment_accession": "ERX1"},
        headers={"If-Match": pre_etag},
    )
    assert resp.status_code == 403


async def test_patch_sequenced_sample_regular_user_role_403(
    ctx, regular_user_with_prep_sample_write_client
):
    # PAT with prep_sample:write passes the scope gate; role gate rejects
    # because the default user session lacks the wet_lab_admin role.
    seeded = await _seed_one_sequenced_sample(ctx, "patch-user")
    pre_etag = await _get_etag(ctx["wet"], seeded["sequenced_sample_idx"])
    resp = await regular_user_with_prep_sample_write_client.patch(
        f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}",
        json={"ena_experiment_accession": "ERX1"},
        headers={"If-Match": pre_etag},
    )
    assert resp.status_code == 403
    assert "wet_lab_admin" in resp.json()["detail"]


async def test_patch_sequenced_sample_nonexistent_404(ctx):
    # Non-existent idx: the FOR UPDATE preflight returns None and the
    # route emits 404 before considering retired / ETag.
    max_ss = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.sequenced_sample")
    resp = await ctx["wet"].patch(
        f"/api/v1/sequenced-sample/{max_ss + 100_000}",
        json={"ena_experiment_accession": "ERX1"},
        headers={"If-Match": '"2026-01-01T00:00:00+00:00"'},
    )
    assert resp.status_code == 404
    assert "sequenced_sample" in resp.json()["detail"]


async def test_patch_sequenced_sample_retired_prep_sample_409(ctx):
    # Retired prep_sample under the sequenced_sample → 409 (distinct from
    # the GET surface, which 404s the retired row via the carve-out).
    seeded = await _seed_one_sequenced_sample(ctx, "patch-ret")
    pre_etag = await _get_etag(ctx["wet"], seeded["sequenced_sample_idx"])
    await _retire_prep_sample(
        ctx["pool"],
        prep_sample_idx=seeded["prep_sample_idx"],
        retired_by_idx=ctx["wet_session"]["principal_idx"],
    )
    resp = await ctx["wet"].patch(
        f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}",
        json={"ena_experiment_accession": "ERX1"},
        headers={"If-Match": pre_etag},
    )
    assert resp.status_code == 409
    assert "retired" in resp.json()["detail"]


async def test_patch_sequenced_sample_ena_experiment_accession_collision_409(ctx):
    # Two sequenced_samples; first claims an ENA experiment accession;
    # second PATCH that targets the same accession trips the unique
    # constraint and the route maps to 409 with the user-friendly detail.
    a = await _seed_one_sequenced_sample(ctx, "patch-exp-a")
    b = await _seed_one_sequenced_sample(ctx, "patch-exp-b")

    etag_a = await _get_etag(ctx["wet"], a["sequenced_sample_idx"])
    r1 = await ctx["wet"].patch(
        f"/api/v1/sequenced-sample/{a['sequenced_sample_idx']}",
        json={"ena_experiment_accession": "ERX_DUP"},
        headers={"If-Match": etag_a},
    )
    assert r1.status_code == 200, r1.text

    etag_b = await _get_etag(ctx["wet"], b["sequenced_sample_idx"])
    r2 = await ctx["wet"].patch(
        f"/api/v1/sequenced-sample/{b['sequenced_sample_idx']}",
        json={"ena_experiment_accession": "ERX_DUP"},
        headers={"If-Match": etag_b},
    )
    assert r2.status_code == 409
    assert r2.json()["detail"] == "ena_experiment_accession already in use"


async def test_patch_sequenced_sample_ena_run_accession_collision_409(ctx):
    # Same shape as the experiment-accession collision test but exercises
    # the second unique index so both error-map entries are covered.
    a = await _seed_one_sequenced_sample(ctx, "patch-run-a")
    b = await _seed_one_sequenced_sample(ctx, "patch-run-b")

    etag_a = await _get_etag(ctx["wet"], a["sequenced_sample_idx"])
    r1 = await ctx["wet"].patch(
        f"/api/v1/sequenced-sample/{a['sequenced_sample_idx']}",
        json={"ena_run_accession": "ERR_DUP"},
        headers={"If-Match": etag_a},
    )
    assert r1.status_code == 200, r1.text

    etag_b = await _get_etag(ctx["wet"], b["sequenced_sample_idx"])
    r2 = await ctx["wet"].patch(
        f"/api/v1/sequenced-sample/{b['sequenced_sample_idx']}",
        json={"ena_run_accession": "ERR_DUP"},
        headers={"If-Match": etag_b},
    )
    assert r2.status_code == 409
    assert r2.json()["detail"] == "ena_run_accession already in use"


# ===========================================================================
# PATCH /sequenced-sample/{idx} — body validation
# ===========================================================================


async def test_patch_sequenced_sample_empty_body_422(ctx):
    # at_least_one_field validator on PatchRequestModel rejects empty bodies.
    seeded = await _seed_one_sequenced_sample(ctx, "patch-empty")
    pre_etag = await _get_etag(ctx["wet"], seeded["sequenced_sample_idx"])
    resp = await ctx["wet"].patch(
        f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}",
        json={},
        headers={"If-Match": pre_etag},
    )
    assert resp.status_code == 422


async def test_patch_sequenced_sample_extra_field_422(ctx):
    # extra="forbid" on SequencedSamplePatchRequest rejects unknown columns.
    seeded = await _seed_one_sequenced_sample(ctx, "patch-extra")
    pre_etag = await _get_etag(ctx["wet"], seeded["sequenced_sample_idx"])
    resp = await ctx["wet"].patch(
        f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}",
        json={"ena_experiment_accession": "ERX1", "not_a_field": 5},
        headers={"If-Match": pre_etag},
    )
    assert resp.status_code == 422


async def test_patch_sequenced_sample_supertype_field_422(ctx):
    # owner_idx lives on the prep_sample supertype and is intentionally
    # not in the subtype-only patch body; extra="forbid" rejects it 422.
    seeded = await _seed_one_sequenced_sample(ctx, "patch-super")
    pre_etag = await _get_etag(ctx["wet"], seeded["sequenced_sample_idx"])
    resp = await ctx["wet"].patch(
        f"/api/v1/sequenced-sample/{seeded['sequenced_sample_idx']}",
        json={"owner_idx": ctx["wet_session"]["principal_idx"]},
        headers={"If-Match": pre_etag},
    )
    assert resp.status_code == 422
