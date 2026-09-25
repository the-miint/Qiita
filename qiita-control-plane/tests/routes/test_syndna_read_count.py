"""DB tests for GET /mask-definition/{mask_idx}/syndna-read-count.

The route returns a table, so it is all-or-nothing: every selected sample must be
readable by the caller at Tier.VIEWER on each linked study, completed under the
mask, and counted. These tests pin each refusal and the three selection filters.
"""

import json
import uuid

import pytest
from qiita_common.api_paths import URL_MASK_DEFINITION_SYNDNA_READ_COUNT
from qiita_common.models import Tier

from qiita_control_plane.testing.db_seeds import (
    cleanup_reference_graph,
    seed_bare_feature,
    seed_bare_reference,
    seed_reference_membership,
)

from . import test_mask_definition_read as mask_read
from .conftest import (  # noqa: F401
    _grant_study_access,
    _seed_study,
    role_keyed_clients,
)
from .test_mask_definition_read import _seed_gate_row, _seed_sample_on_pool

pytestmark = pytest.mark.db

# The mask-read fixture and its teardown ledger, shared rather than copied.
ctx = mask_read.ctx

_HEADERS = ("synDNA_16SrRNA_seq_1_gc=0.26", "synDNA_16SrRNA_seq_2_gc=0.36")


@pytest.fixture(autouse=True)
async def _syndna_reference(ctx):
    """Extend the mask-read `ctx` with a two-insert SynDNA reference, and clear the
    count rows the tests write before its own teardown drops the masks."""
    pool = ctx["pool"]
    reference_idx = await seed_bare_reference(pool, label="syndna-count")
    inserts = []
    for header in _HEADERS:
        feature_idx = await seed_bare_feature(pool)
        await seed_reference_membership(
            pool, reference_idx=reference_idx, feature_idx=feature_idx, accession=header
        )
        inserts.append(feature_idx)
    ctx["reference_idx"] = reference_idx
    ctx["inserts"] = inserts
    yield
    await pool.execute(
        "DELETE FROM qiita.syndna_read_count WHERE mask_idx = ANY($1::bigint[])",
        ctx["created"]["mask"],
    )
    await cleanup_reference_graph(pool, reference_idx=reference_idx, feature_idxs=inserts)


async def _seed_syndna_mask(ctx, *, syndna: bool = True) -> int:
    params = {"resolved_syndna": {"reference_idx": ctx["reference_idx"]} if syndna else None}
    mask_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.mask_definition"
        " (params_hash, filter_workflow, filter_version, params, created_by_idx)"
        " VALUES ($1, 'read-mask', '1.0.0', $2::jsonb, $3) RETURNING mask_idx",
        uuid.uuid4().bytes + uuid.uuid4().bytes,
        json.dumps(params),
        ctx["admin_session"]["principal_idx"],
    )
    ctx["created"]["mask"].append(mask_idx)
    return mask_idx


async def _seed_counts(ctx, *, mask_idx: int, prep_sample_idx: int, counts) -> None:
    await ctx["pool"].executemany(
        "INSERT INTO qiita.syndna_read_count (mask_idx, prep_sample_idx, feature_idx, read_count)"
        " VALUES ($1, $2, $3, $4)",
        [(mask_idx, prep_sample_idx, f, c) for f, c in zip(ctx["inserts"], counts, strict=True)],
    )


async def _counted_sample(ctx, *, mask_idx, study_idx, counts=(7, 0), state="completed"):
    prep_sample_idx, pool_idx = await _seed_sample_on_pool(
        ctx, owner_idx=ctx["admin_session"]["principal_idx"], study_idx=study_idx
    )
    await _seed_gate_row(ctx, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx, state=state)
    if counts is not None:
        await _seed_counts(ctx, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx, counts=counts)
    return prep_sample_idx, pool_idx


async def _viewer_study(ctx) -> int:
    study_idx = await _seed_study(
        ctx, owner_idx=ctx["admin_session"]["principal_idx"], suffix="syndna"
    )
    await _grant_study_access(
        ctx,
        study_idx=study_idx,
        principal_idx=ctx["user_session"]["principal_idx"],
        tier=Tier.VIEWER,
        granted_by_idx=ctx["admin_session"]["principal_idx"],
    )
    return study_idx


def _url(mask_idx: int) -> str:
    return URL_MASK_DEFINITION_SYNDNA_READ_COUNT.format(mask_idx=mask_idx)


async def test_a_study_viewer_gets_the_table_by_study(ctx):
    mask_idx = await _seed_syndna_mask(ctx)
    study_idx = await _viewer_study(ctx)
    ps1, pool1 = await _counted_sample(ctx, mask_idx=mask_idx, study_idx=study_idx, counts=(5, 0))
    ps2, _ = await _counted_sample(ctx, mask_idx=mask_idx, study_idx=study_idx, counts=(1, 9))

    resp = await ctx["user"].get(_url(mask_idx), params={"study_idx": study_idx})

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["reference_idx"] == ctx["reference_idx"]
    assert [i["accession"] for i in body["inserts"]] == list(_HEADERS)
    assert [(s["prep_sample_idx"], s["read_counts"]) for s in body["samples"]] == [
        (ps1, [5, 0]),
        (ps2, [1, 9]),
    ]
    assert body["samples"][0]["sequenced_pool_idx"] == pool1


async def test_pool_and_prep_sample_filters_select_and_intersect(ctx):
    mask_idx = await _seed_syndna_mask(ctx)
    study_idx = await _viewer_study(ctx)
    ps1, pool1 = await _counted_sample(ctx, mask_idx=mask_idx, study_idx=study_idx)
    ps2, _ = await _counted_sample(ctx, mask_idx=mask_idx, study_idx=study_idx)

    by_pool = await ctx["user"].get(_url(mask_idx), params={"sequenced_pool_idx": pool1})
    by_prep = await ctx["user"].get(_url(mask_idx), params={"prep_sample_idx": [ps2]})
    both = await ctx["user"].get(
        _url(mask_idx), params={"study_idx": study_idx, "prep_sample_idx": [ps1, ps2]}
    )

    assert [s["prep_sample_idx"] for s in by_pool.json()["samples"]] == [ps1]
    assert [s["prep_sample_idx"] for s in by_prep.json()["samples"]] == [ps2]
    assert [s["prep_sample_idx"] for s in both.json()["samples"]] == [ps1, ps2]


async def test_a_study_the_caller_cannot_read_is_refused_before_the_roster(ctx):
    """An empty selection must not answer 404 to a caller with no role on the study:
    that would say whether the study has prep_samples under the mask."""
    mask_idx = await _seed_syndna_mask(ctx)
    study_idx = await _seed_study(ctx, owner_idx=ctx["admin_session"]["principal_idx"], suffix="n")

    resp = await ctx["user"].get(_url(mask_idx), params={"study_idx": study_idx})

    assert resp.status_code == 403, resp.text


async def test_a_caller_without_viewer_on_a_linked_study_is_refused(ctx):
    mask_idx = await _seed_syndna_mask(ctx)
    readable = await _viewer_study(ctx)
    other = await _seed_study(ctx, owner_idx=ctx["admin_session"]["principal_idx"], suffix="x")
    ps, _ = await _counted_sample(ctx, mask_idx=mask_idx, study_idx=readable)
    # The same sample also linked to a study the caller holds nothing on.
    await ctx["pool"].execute(
        "INSERT INTO qiita.biosample_to_study (biosample_idx, study_idx, created_by_idx)"
        " SELECT biosample_idx, $2, $3 FROM qiita.prep_sample WHERE idx = $1",
        ps,
        other,
        ctx["admin_session"]["principal_idx"],
    )
    biosample_idx = await ctx["pool"].fetchval(
        "SELECT biosample_idx FROM qiita.prep_sample WHERE idx = $1", ps
    )
    ctx["created"]["biosample_to_study"].append((biosample_idx, other))
    await ctx["pool"].execute(
        "INSERT INTO qiita.prep_sample_to_study (prep_sample_idx, study_idx, created_by_idx)"
        " VALUES ($1, $2, $3)",
        ps,
        other,
        ctx["admin_session"]["principal_idx"],
    )
    ctx["created"]["prep_sample_to_study"].append((ps, other))

    by_study = await ctx["user"].get(_url(mask_idx), params={"study_idx": readable})
    by_name = await ctx["user"].get(_url(mask_idx), params={"prep_sample_idx": [ps]})

    assert by_study.status_code == 403, by_study.text
    assert by_name.status_code == 403, by_name.text


@pytest.mark.parametrize("client", ["wet", "admin"])
async def test_wet_lab_and_system_admins_need_no_study_role(ctx, client):
    mask_idx = await _seed_syndna_mask(ctx)
    study_idx = await _seed_study(ctx, owner_idx=ctx["user_session"]["principal_idx"], suffix="o")
    ps, _ = await _counted_sample(ctx, mask_idx=mask_idx, study_idx=study_idx)

    resp = await ctx[client].get(_url(mask_idx), params={"prep_sample_idx": [ps]})

    assert resp.status_code == 200, resp.text


async def test_a_sample_not_completed_is_refused(ctx):
    mask_idx = await _seed_syndna_mask(ctx)
    study_idx = await _viewer_study(ctx)
    await _counted_sample(ctx, mask_idx=mask_idx, study_idx=study_idx)
    await _counted_sample(ctx, mask_idx=mask_idx, study_idx=study_idx, state="pending")

    resp = await ctx["user"].get(_url(mask_idx), params={"study_idx": study_idx})

    assert resp.status_code == 409, resp.text
    assert "not completed" in resp.json()["detail"]


async def test_a_completed_sample_without_counts_names_the_backfill(ctx):
    mask_idx = await _seed_syndna_mask(ctx)
    study_idx = await _viewer_study(ctx)
    await _counted_sample(ctx, mask_idx=mask_idx, study_idx=study_idx, counts=None)

    resp = await ctx["user"].get(_url(mask_idx), params={"study_idx": study_idx})

    assert resp.status_code == 409, resp.text
    assert "backfill syndna-read-count" in resp.json()["detail"]


async def test_a_named_sample_not_masked_under_the_mask_is_refused(ctx):
    mask_idx = await _seed_syndna_mask(ctx)
    study_idx = await _viewer_study(ctx)
    ps, _ = await _seed_sample_on_pool(
        ctx, owner_idx=ctx["admin_session"]["principal_idx"], study_idx=study_idx
    )

    resp = await ctx["user"].get(_url(mask_idx), params={"prep_sample_idx": [ps]})

    assert resp.status_code == 409, resp.text
    assert "not masked" in resp.json()["detail"]


async def test_a_mask_without_syndna_is_refused(ctx):
    mask_idx = await _seed_syndna_mask(ctx, syndna=False)
    resp = await ctx["wet"].get(_url(mask_idx), params={"study_idx": 1})
    assert resp.status_code == 409, resp.text


async def test_absent_mask_and_empty_selection_404(ctx):
    absent = await ctx["wet"].get(_url(2**40), params={"study_idx": 1})
    mask_idx = await _seed_syndna_mask(ctx)
    study_idx = await _viewer_study(ctx)
    empty = await ctx["user"].get(_url(mask_idx), params={"study_idx": study_idx})
    assert absent.status_code == 404, absent.text
    assert empty.status_code == 404, empty.text


async def test_a_selector_is_required(ctx):
    mask_idx = await _seed_syndna_mask(ctx)
    resp = await ctx["wet"].get(_url(mask_idx))
    assert resp.status_code == 422, resp.text
