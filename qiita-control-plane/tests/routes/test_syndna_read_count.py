"""DB tests for GET /mask-definition/{mask_idx}/syndna-read-count.

The route returns a table, so its authorization is all-or-nothing: every selected
sample must be readable by the caller at Tier.VIEWER on each linked study. Each
selected sample is resolved across the mask's group; one that is not processed is
left out and listed, and a processed one must be counted under the member it
resolved to. These tests pin each refusal, the three selection filters, and the
resolution across a group.
"""

import json
import uuid

import pytest
from qiita_common.api_paths import URL_MASK_DEFINITION_SYNDNA_READ_COUNT
from qiita_common.models import DefinitionKind, DefinitionPairVerdict, Tier

from qiita_control_plane.definition_similarity import decide_pair, open_pair
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
from .test_mask_definition_read import _seed_gate_row, _seed_mask_ticket, _seed_sample_on_pool

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


async def _seed_syndna_mask(ctx, *, syndna: bool = True, reference_idx: int | None = None) -> int:
    reference_idx = reference_idx or ctx["reference_idx"]
    params = {
        "resolved_syndna": {"reference_idx": reference_idx} if syndna else None,
        "t": uuid.uuid4().hex,
    }
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
    await ctx["pool"].execute(
        "INSERT INTO qiita.prep_sample_to_study (prep_sample_idx, study_idx, created_by_idx)"
        " VALUES ($1, $2, $3)",
        ps,
        other,
        ctx["admin_session"]["principal_idx"],
    )

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


async def test_a_sample_not_processed_is_left_out_and_listed(ctx):
    """A pending gate with no live ticket is no run; a running ticket makes the
    sample incomplete. Both are left out of the table and listed with the reason."""
    mask_idx = await _seed_syndna_mask(ctx)
    study_idx = await _viewer_study(ctx)
    counted, _ = await _counted_sample(ctx, mask_idx=mask_idx, study_idx=study_idx)
    stale, _ = await _counted_sample(
        ctx, mask_idx=mask_idx, study_idx=study_idx, counts=None, state="pending"
    )
    running, _ = await _counted_sample(
        ctx, mask_idx=mask_idx, study_idx=study_idx, counts=None, state="pending"
    )
    ticket = await _seed_mask_ticket(
        ctx, mask_idx=mask_idx, prep_sample_idx=running, state="processing"
    )

    resp = await ctx["user"].get(_url(mask_idx), params={"study_idx": study_idx})

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert [(s["prep_sample_idx"], s["mask_idx"]) for s in body["samples"]] == [(counted, mask_idx)]
    assert [
        (r["prep_sample_idx"], r["state"], r["reason"], r["work_ticket_idx"])
        for r in body["left_out"]
    ] == [
        (stale, "not_processed", "no_run", None),
        (running, "incomplete", "in_progress", ticket),
    ]


async def test_nothing_processed_is_refused_with_the_reasons(ctx):
    mask_idx = await _seed_syndna_mask(ctx)
    study_idx = await _viewer_study(ctx)
    ps, _ = await _counted_sample(
        ctx, mask_idx=mask_idx, study_idx=study_idx, counts=None, state="pending"
    )

    resp = await ctx["user"].get(_url(mask_idx), params={"study_idx": study_idx})

    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert "none of the 1 selected prep_sample(s) is processed" in detail
    assert f"not_processed/no_run: 1 (e.g. {ps})" in detail


@pytest.fixture
async def similar(ctx):
    """`similar(a, b)` decides two masks similar; the pairs are deleted afterwards."""
    pool = ctx["pool"]
    owner = ctx["admin_session"]["principal_idx"]
    pairs: list[int] = []

    async def _similar(a: int, b: int) -> None:
        async with pool.acquire() as conn, conn.transaction():
            pair, _ = await open_pair(
                conn, kind=DefinitionKind.MASK, definition_idxs=[a, b], principal_idx=owner
            )
            await decide_pair(
                conn,
                definition_pair_idx=pair["definition_pair_idx"],
                verdict=DefinitionPairVerdict.SIMILAR,
                reason="same filter, checked against a mock community",
                principal_idx=owner,
            )
        pairs.append(pair["definition_pair_idx"])

    yield _similar
    await pool.execute(
        "DELETE FROM qiita.definition_pair WHERE definition_pair_idx = ANY($1::bigint[])", pairs
    )


async def test_each_sample_is_counted_under_the_mask_it_resolves_to(ctx, similar):
    """Across a group, a sample masked under both members is read under the newer
    one, whose counts differ from the older one's; a sample masked only under the
    older one is read there. Either member names the group."""
    low = await _seed_syndna_mask(ctx)
    high = await _seed_syndna_mask(ctx)
    await similar(low, high)
    study_idx = await _viewer_study(ctx)
    only_low, _ = await _counted_sample(ctx, mask_idx=low, study_idx=study_idx, counts=(5, 0))
    both, _ = await _counted_sample(ctx, mask_idx=low, study_idx=study_idx, counts=(1, 1))
    await _seed_gate_row(ctx, mask_idx=high, prep_sample_idx=both, state="completed")
    await _seed_counts(ctx, mask_idx=high, prep_sample_idx=both, counts=(2, 8))
    only_high, _ = await _counted_sample(ctx, mask_idx=high, study_idx=study_idx, counts=(3, 4))

    for anchor in (low, high):
        resp = await ctx["user"].get(_url(anchor), params={"study_idx": study_idx})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["mask_idx"] == anchor
        assert body["left_out"] == []
        assert [
            (s["prep_sample_idx"], s["mask_idx"], s["read_counts"]) for s in body["samples"]
        ] == [
            (only_low, low, [5, 0]),
            (both, high, [2, 8]),
            (only_high, high, [3, 4]),
        ]


@pytest.mark.parametrize("other", ["no_syndna", "other_reference"])
async def test_resolved_masks_with_a_different_syndna_reference_are_refused(ctx, similar, other):
    if other == "no_syndna":
        other_reference = None
        high = await _seed_syndna_mask(ctx, syndna=False)
    else:
        other_reference = await seed_bare_reference(ctx["pool"], label="syndna-other")
        high = await _seed_syndna_mask(ctx, reference_idx=other_reference)
    try:
        low = await _seed_syndna_mask(ctx)
        await similar(low, high)
        study_idx = await _viewer_study(ctx)
        await _counted_sample(ctx, mask_idx=low, study_idx=study_idx)
        await _counted_sample(ctx, mask_idx=high, study_idx=study_idx, counts=None)

        resp = await ctx["user"].get(_url(low), params={"study_idx": study_idx})

        assert resp.status_code == 409, resp.text
        assert f"mask {high}: {other_reference}" in resp.json()["detail"]
    finally:
        if other_reference is not None:
            await cleanup_reference_graph(
                ctx["pool"], reference_idx=other_reference, feature_idxs=[]
            )


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
