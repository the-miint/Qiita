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
from qiita_common.models import DefinitionKind, DefinitionPairRefusal, DefinitionPairVerdict, Tier

from qiita_control_plane.definition_similarity import PairRefused, decide_pair, open_pair
from qiita_control_plane.testing.db_seeds import (
    cleanup_reference_graph,
    seed_bare_feature,
    seed_bare_reference,
    seed_reference_membership,
)
from qiita_control_plane.testing.db_teardown import delete_definition_pairs

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
    # The route mints each counted mask's public handle; they name these masks.
    await pool.execute(
        "DELETE FROM qiita.exported_processing WHERE mask_idx = ANY($1::bigint[])",
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


async def test_an_unknown_decision_set_is_refused(ctx):
    mask_idx = await _seed_syndna_mask(ctx)
    study_idx = await _viewer_study(ctx)
    await _counted_sample(ctx, mask_idx=mask_idx, study_idx=study_idx)
    resp = await ctx["user"].get(
        _url(mask_idx), params={"study_idx": study_idx, "as_of_event": "sha256:" + "a" * 64}
    )
    assert resp.status_code == 422, resp.text


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
    assert [(r["prep_sample_idx"], r["state"], r["reason"]) for r in body["left_out"]] == [
        (stale, "not_processed", "no_run"),
        (running, "incomplete", "in_progress"),
    ]
    accessions = dict(
        await ctx["pool"].fetch(
            "SELECT ps.idx, bs.biosample_accession FROM qiita.prep_sample ps"
            " JOIN qiita.biosample bs ON bs.idx = ps.biosample_idx"
            " WHERE ps.idx = ANY($1::bigint[])",
            [stale, running],
        )
    )
    assert body["left_out_biosample_accession"] == {
        str(stale): accessions[stale],
        str(running): accessions[running],
    }
    # The ticket a sample waits on is a system_admin's to see only.
    assert all("work_ticket_idx" not in r for r in body["left_out"])
    admin = await ctx["admin"].get(_url(mask_idx), params={"study_idx": study_idx})
    assert admin.status_code == 200, admin.text
    assert [r["work_ticket_idx"] for r in admin.json()["left_out"]] == [None, ticket]


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
    """`similar(a, b)` decides two masks similar (or `verdict`); the pairs are deleted
    afterwards."""
    pool = ctx["pool"]
    owner = ctx["admin_session"]["principal_idx"]
    pairs: list[int] = []

    async def _similar(
        a: int, b: int, verdict: DefinitionPairVerdict = DefinitionPairVerdict.SIMILAR
    ) -> None:
        async with pool.acquire() as conn, conn.transaction():
            pair, _ = await open_pair(
                conn, kind=DefinitionKind.MASK, definition_idxs=[a, b], principal_idx=owner
            )
            await decide_pair(
                conn,
                definition_pair_idx=pair["definition_pair_idx"],
                verdict=verdict,
                reason="same filter, checked against a mock community",
                principal_idx=owner,
            )
        pairs.append(pair["definition_pair_idx"])

    yield _similar
    await delete_definition_pairs(pool, pairs)


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
        assert (body["decisions"]["as_of"], body["decisions"]["pairs_used"]) == (False, 1)
        assert [
            (s["prep_sample_idx"], s["mask_idx"], s["read_counts"]) for s in body["samples"]
        ] == [
            (only_low, low, [5, 0]),
            (both, high, [2, 8]),
            (only_high, high, [3, 4]),
        ]
        # Each sample cites the mask its counts came from by that mask's public handle.
        handles = dict(
            await ctx["pool"].fetch(
                "SELECT mask_idx, export_processing_id FROM qiita.exported_processing"
                " WHERE mask_idx = ANY($1::bigint[]) AND NOT retired",
                [low, high],
            )
        )
        assert set(handles) == {low, high}
        assert [s["mask_export_processing_id"] for s in body["samples"]] == [
            handles[low],
            handles[high],
            handles[high],
        ]


@pytest.mark.parametrize("other", ["no_syndna", "other_reference"])
async def test_masks_with_a_different_syndna_reference_cannot_be_decided_similar(
    ctx, similar, other
):
    """Through the repository's decide, against the masks' stored params; not_similar
    is still allowed. The export's own reference guard is unit-tested beside the route."""
    if other == "no_syndna":
        other_reference = None
        high = await _seed_syndna_mask(ctx, syndna=False)
    else:
        other_reference = await seed_bare_reference(ctx["pool"], label="syndna-other")
        high = await _seed_syndna_mask(ctx, reference_idx=other_reference)
    try:
        low = await _seed_syndna_mask(ctx)
        with pytest.raises(PairRefused) as ei:
            await similar(low, high)
        assert ei.value.reason is DefinitionPairRefusal.SYNDNA_REFERENCE_CHANGE
        await similar(low, high, DefinitionPairVerdict.NOT_SIMILAR)
    finally:
        if other_reference is not None:
            await cleanup_reference_graph(
                ctx["pool"], reference_idx=other_reference, feature_idxs=[]
            )


@pytest.mark.parametrize("deprecated", [False, True])
async def test_a_processed_prep_sample_without_counts_is_left_out(ctx, deprecated):
    """A completed gate with no stored counts (persist-syndna-read-count failed after
    finalize-mask-sample) is left out as `no_syndna_counts`; the rest are read. The
    same holds under the exact read of a deprecated mask, and for a named prep_sample."""
    mask_idx = await _seed_syndna_mask(ctx)
    study_idx = await _viewer_study(ctx)
    counted, _ = await _counted_sample(ctx, mask_idx=mask_idx, study_idx=study_idx)
    uncounted, _ = await _counted_sample(ctx, mask_idx=mask_idx, study_idx=study_idx, counts=None)
    params = {"study_idx": study_idx, "prep_sample_idx": [counted, uncounted]}
    if deprecated:
        await _deprecate_mask(ctx, mask_idx)
        params["include_deprecated"] = "true"

    resp = await ctx["user"].get(_url(mask_idx), params=params)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert [s["prep_sample_idx"] for s in body["samples"]] == [counted]
    assert [
        (r["prep_sample_idx"], r["state"], r["reason"], r["definition_idx"])
        for r in body["left_out"]
    ] == [(uncounted, "not_processed", "no_syndna_counts", mask_idx)]
    assert set(body["left_out_biosample_accession"]) == {str(uncounted)}


async def test_a_completed_sample_without_counts_names_the_backfill(ctx):
    """Every processed prep_sample without counts: nothing to write, so refused."""
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
    study_idx = await _viewer_study(ctx)
    await _counted_sample(ctx, mask_idx=mask_idx, study_idx=study_idx, counts=None)
    resp = await ctx["user"].get(_url(mask_idx), params={"study_idx": study_idx})
    assert resp.status_code == 409, resp.text
    assert "without SynDNA" in resp.json()["detail"]


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


# ---------------------------------------------------------------------------
# A deprecated mask, read on purpose
# ---------------------------------------------------------------------------


async def _deprecate_mask(ctx, mask_idx: int) -> None:
    await ctx["pool"].execute(
        "UPDATE qiita.mask_definition SET status = 'deprecated', deprecated_at = now(),"
        " deprecated_by_idx = $2, deprecation_reason = 'kept host reads'"
        " WHERE mask_idx = $1",
        mask_idx,
        ctx["admin_session"]["principal_idx"],
    )


async def test_a_deprecated_mask_is_refused_without_the_flag(ctx):
    mask_idx = await _seed_syndna_mask(ctx)
    study_idx = await _viewer_study(ctx)
    await _counted_sample(ctx, mask_idx=mask_idx, study_idx=study_idx)
    await _deprecate_mask(ctx, mask_idx)

    resp = await ctx["user"].get(_url(mask_idx), params={"study_idx": study_idx})

    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert (detail["reason"], detail["deprecation_reason"]) == ("deprecated", "kept host reads")
    assert "--include-deprecated" in detail["detail"]


async def test_a_deprecated_mask_read_under_the_flag_returns_its_own_counts(ctx):
    """The counts are the mask's stored ones, as an exact-mask read returns them (a
    completed gate, the counts in insert order); a withdrawn run and a sample with no
    run are left out; the response is marked deprecated."""
    mask_idx = await _seed_syndna_mask(ctx)
    study_idx = await _viewer_study(ctx)
    ps1, _ = await _counted_sample(ctx, mask_idx=mask_idx, study_idx=study_idx, counts=(5, 0))
    ps2, _ = await _counted_sample(ctx, mask_idx=mask_idx, study_idx=study_idx, counts=(1, 9))
    withdrawn, _ = await _counted_sample(
        ctx, mask_idx=mask_idx, study_idx=study_idx, counts=(3, 3), state="pending"
    )
    await ctx["pool"].execute(
        "UPDATE qiita.mask_sample SET state = 'invalidated', invalidated_at = now(),"
        " invalidated_by_idx = $3, invalidation_reason = 'withdrawn by a test'"
        " WHERE mask_idx = $1 AND prep_sample_idx = $2",
        mask_idx,
        withdrawn,
        ctx["admin_session"]["principal_idx"],
    )
    await _deprecate_mask(ctx, mask_idx)

    resp = await ctx["user"].get(
        _url(mask_idx), params={"study_idx": study_idx, "include_deprecated": "true"}
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert (body["deprecated"], body["deprecation_reason"]) == (True, "kept host reads")
    stored = await ctx["pool"].fetch(
        "SELECT prep_sample_idx, array_agg(read_count ORDER BY array_position($3::bigint[],"
        " feature_idx)) AS counts"
        "  FROM qiita.syndna_read_count rc"
        "  JOIN qiita.mask_sample ms USING (mask_idx, prep_sample_idx)"
        " WHERE rc.mask_idx = $1 AND rc.prep_sample_idx = ANY($2::bigint[])"
        "   AND ms.state = 'completed'"
        " GROUP BY prep_sample_idx ORDER BY prep_sample_idx",
        mask_idx,
        [ps1, ps2, withdrawn],
        ctx["inserts"],
    )
    assert [(s["prep_sample_idx"], s["mask_idx"], s["read_counts"]) for s in body["samples"]] == [
        (r["prep_sample_idx"], mask_idx, list(r["counts"])) for r in stored
    ]
    assert [(s["prep_sample_idx"], s["read_counts"]) for s in body["samples"]] == [
        (ps1, [5, 0]),
        (ps2, [1, 9]),
    ]
    assert [(r["prep_sample_idx"], r["reason"]) for r in body["left_out"]] == [
        (withdrawn, "withdrawn")
    ]


async def test_an_active_mask_with_the_flag_reads_as_without(ctx):
    mask_idx = await _seed_syndna_mask(ctx)
    study_idx = await _viewer_study(ctx)
    await _counted_sample(ctx, mask_idx=mask_idx, study_idx=study_idx)

    plain = await ctx["user"].get(_url(mask_idx), params={"study_idx": study_idx})
    flagged = await ctx["user"].get(
        _url(mask_idx), params={"study_idx": study_idx, "include_deprecated": "true"}
    )

    assert (plain.status_code, flagged.status_code) == (200, 200)
    assert plain.json() == flagged.json()
    assert flagged.json()["deprecated"] is False
