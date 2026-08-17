"""DB tests for the mask lifecycle: config deprecation and per-run invalidation.

A mask's `params_hash` covers the resolved thresholds, not the code that applies
them, so a config whose scoring turned out wrong re-resolves to the same
`mask_idx` and masks new data with the same defect. Two markers answer two
different questions, and the tests below are organized by which:

  * `mask_definition.status = 'deprecated'` — the CONFIG is void. Refused at the
    mint (the guard that stops new bad data) and at align planning.
  * `mask_sample.state = 'invalidated'` — a RUN of a sound config is not
    trustworthy. Refused wherever masked reads are consumed, which needs no new
    check: every consumer already proceeds only on `'completed'`.
"""

import json
import secrets

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import (
    URL_MASK_DEFINITION_BY_IDX,
    URL_MASK_DEFINITION_PREFIX,
    URL_MASK_DEFINITION_SAMPLE_STATUS,
    URL_MASK_DEFINITION_STATUS,
)
from qiita_common.models import MaskDefinitionStatus

from qiita_control_plane.repositories.mask_definition import (
    MaskDefinitionDeprecated,
    mint_mask_definition,
)
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
)

pytestmark = pytest.mark.db


def _install_settings(app):
    from qiita_control_plane.config import Settings

    app.state.settings = Settings(
        database_url="unused",
        flight_signing_key=b"\x00" * 32,
        data_plane_url="unused",
    )


@pytest_asyncio.fixture
async def client(postgres_pool, human_admin_session):
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    _install_settings(app)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {human_admin_session['token']}"},
    ) as ac:
        yield ac


@pytest_asyncio.fixture
async def lifecycle(postgres_pool, human_admin_session):
    """One mask plus two prep_samples gated under it: one 'completed' (the run that
    can be withdrawn) and one 'pending' (the run that must not be)."""
    principal_idx = human_admin_session["principal_idx"]
    suffix = secrets.token_hex(4)
    params = {"workflow": "read-mask", "probe": suffix}
    async with postgres_pool.acquire() as conn:
        row = await mint_mask_definition(
            conn,
            filter_workflow="read-mask",
            filter_version="1.0.0",
            params=params,
            principal_idx=principal_idx,
        )
    mask_idx = row["mask_idx"]
    bs_done, ps_done = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    bs_pend, ps_pend = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    await postgres_pool.executemany(
        "INSERT INTO qiita.mask_sample (mask_idx, prep_sample_idx, state) VALUES ($1, $2, $3)",
        [(mask_idx, ps_done, "completed"), (mask_idx, ps_pend, "pending")],
    )
    yield {
        "mask_idx": mask_idx,
        "params": params,
        "principal_idx": principal_idx,
        "ps_done": ps_done,
        "ps_pending": ps_pend,
    }
    await postgres_pool.execute("DELETE FROM qiita.mask_sample WHERE mask_idx = $1", mask_idx)
    await postgres_pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", [ps_done, ps_pend]
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", [bs_done, bs_pend]
    )


async def _deprecate(client, mask_idx, reason="rype_classify scored wrongly"):
    return await client.patch(
        URL_MASK_DEFINITION_STATUS.format(mask_idx=mask_idx),
        json={"status": "deprecated", "reason": reason},
    )


# ---------------------------------------------------------------------------
# Config lifecycle: refuse to PRODUCE
# ---------------------------------------------------------------------------


async def test_minting_against_a_deprecated_config_fails_loudly(postgres_pool, client, lifecycle):
    """The guard that stops NEW bad data. Enforced in qiita.mint_mask_definition,
    so it holds for every caller of the mint rather than per call site."""
    resp = await _deprecate(client, lifecycle["mask_idx"])
    assert resp.status_code == 200, resp.text

    with pytest.raises(MaskDefinitionDeprecated) as ei:
        async with postgres_pool.acquire() as conn:
            await mint_mask_definition(
                conn,
                filter_workflow="read-mask",
                filter_version="1.0.0",
                params=lifecycle["params"],
                principal_idx=lifecycle["principal_idx"],
            )
    assert str(lifecycle["mask_idx"]) in str(ei.value)
    assert "rype_classify scored wrongly" in str(ei.value)


async def test_minting_against_an_active_config_still_returns_it(postgres_pool, lifecycle):
    """The control for the test above: the same call on an un-deprecated mask
    resolves idempotently to the same row, so the refusal is attributable to the
    status and not to the mint being broken."""
    async with postgres_pool.acquire() as conn:
        row = await mint_mask_definition(
            conn,
            filter_workflow="read-mask",
            filter_version="1.0.0",
            params=lifecycle["params"],
            principal_idx=lifecycle["principal_idx"],
        )
    assert row["mask_idx"] == lifecycle["mask_idx"]
    assert row["status"] == MaskDefinitionStatus.ACTIVE.value


async def test_mint_mask_definition_has_exactly_one_overload(postgres_pool):
    """The lifecycle migration re-states the mint function whole (CREATE OR REPLACE
    has no partial form). Restating the wrong SIGNATURE adds an overload instead of
    replacing the body, leaving the guard on a function nothing calls and making
    every 5-argument call ambiguous — which is what this pins."""
    sigs = [
        r["sig"]
        for r in await postgres_pool.fetch(
            "SELECT p.oid::regprocedure::text AS sig FROM pg_proc p"
            " JOIN pg_namespace n ON n.oid = p.pronamespace"
            " WHERE n.nspname = 'qiita' AND p.proname = 'mint_mask_definition'"
        )
    ]
    assert len(sigs) == 1, sigs


# ---------------------------------------------------------------------------
# Config lifecycle: the PATCH route, and staying visible
# ---------------------------------------------------------------------------


async def test_deprecation_records_who_when_and_why(client, lifecycle):
    resp = await _deprecate(client, lifecycle["mask_idx"])
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "deprecated"
    assert body["deprecation_reason"] == "rype_classify scored wrongly"
    assert body["deprecated_by_idx"] == lifecycle["principal_idx"]
    assert body["deprecated_at"] is not None


async def test_deprecating_requires_a_reason(client, lifecycle):
    """A bare status flip leaves whoever finds a deprecated mask behind published
    data with no way to tell a wrong filter from a superseded one."""
    resp = await client.patch(
        URL_MASK_DEFINITION_STATUS.format(mask_idx=lifecycle["mask_idx"]),
        json={"status": "deprecated"},
    )
    assert resp.status_code == 422, resp.text


async def test_reactivating_clears_the_deprecation_provenance(client, lifecycle):
    await _deprecate(client, lifecycle["mask_idx"])
    resp = await client.patch(
        URL_MASK_DEFINITION_STATUS.format(mask_idx=lifecycle["mask_idx"]),
        json={"status": "active"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "active"
    assert body["deprecated_at"] is None
    assert body["deprecation_reason"] is None
    assert body["superseded_by"] is None


async def test_patch_status_on_an_unknown_mask_404s(client):
    resp = await _deprecate(client, 99_999_999)
    assert resp.status_code == 404


async def test_a_deprecated_mask_is_still_listed_and_readable(client, lifecycle):
    """Deprecation is not deletion: "what filter produced this published
    submission?" has to keep an answer."""
    await _deprecate(client, lifecycle["mask_idx"])

    one = await client.get(URL_MASK_DEFINITION_BY_IDX.format(mask_idx=lifecycle["mask_idx"]))
    assert one.status_code == 200
    assert one.json()["status"] == "deprecated"

    listed = await client.get(URL_MASK_DEFINITION_PREFIX)
    assert listed.status_code == 200
    found = [m for m in listed.json()["masks"] if m["mask_idx"] == lifecycle["mask_idx"]]
    assert found and found[0]["status"] == "deprecated"


async def test_status_query_filters_the_list_both_ways(client, lifecycle):
    mask_idx = lifecycle["mask_idx"]
    await _deprecate(client, mask_idx)

    dep = await client.get(URL_MASK_DEFINITION_PREFIX, params={"status": "deprecated"})
    assert mask_idx in {m["mask_idx"] for m in dep.json()["masks"]}

    act = await client.get(URL_MASK_DEFINITION_PREFIX, params={"status": "active"})
    assert mask_idx not in {m["mask_idx"] for m in act.json()["masks"]}


async def test_status_and_narrowing_filters_compose(client, lifecycle):
    """Both WHERE fragments at once. Each alone is covered above, and the SQL joins
    them with an explicit connective — a form that is wrong only when both are
    present."""
    mask_idx = lifecycle["mask_idx"]
    params = {"prep_sample_idx": lifecycle["ps_done"], "status": "active"}
    assert mask_idx in {
        m["mask_idx"]
        for m in (await client.get(URL_MASK_DEFINITION_PREFIX, params=params)).json()["masks"]
    }
    await _deprecate(client, mask_idx)
    assert mask_idx not in {
        m["mask_idx"]
        for m in (await client.get(URL_MASK_DEFINITION_PREFIX, params=params)).json()["masks"]
    }
    params["status"] = "deprecated"
    assert mask_idx in {
        m["mask_idx"]
        for m in (await client.get(URL_MASK_DEFINITION_PREFIX, params=params)).json()["masks"]
    }


async def test_superseded_by_records_the_replacement_and_rejects_self(client, lifecycle):
    """`superseded_by` is accepted only alongside a deprecation, must name a real
    mask, and cannot be the mask itself — the last of which the wire model cannot
    catch, since it does not see the path parameter."""
    mask_idx = lifecycle["mask_idx"]
    unknown = await client.patch(
        URL_MASK_DEFINITION_STATUS.format(mask_idx=mask_idx),
        json={"status": "deprecated", "reason": "r", "superseded_by": 99_999_999},
    )
    assert unknown.status_code == 422, unknown.text

    itself = await client.patch(
        URL_MASK_DEFINITION_STATUS.format(mask_idx=mask_idx),
        json={"status": "deprecated", "reason": "r", "superseded_by": mask_idx},
    )
    assert itself.status_code == 422, itself.text

    only_when_deprecated = await client.patch(
        URL_MASK_DEFINITION_STATUS.format(mask_idx=mask_idx),
        json={"status": "active", "superseded_by": mask_idx},
    )
    assert only_when_deprecated.status_code == 422, only_when_deprecated.text


# ---------------------------------------------------------------------------
# Run lifecycle: per-(mask, sample) invalidation
# ---------------------------------------------------------------------------


async def test_invalidating_a_run_reports_every_bucket(client, postgres_pool, lifecycle):
    """One call covers the four outcomes the response separates, because a bulk
    withdrawal that reported only a count would leave the operator unable to tell a
    typo'd prep_sample from one that was already withdrawn."""
    resp = await client.patch(
        URL_MASK_DEFINITION_SAMPLE_STATUS.format(mask_idx=lifecycle["mask_idx"]),
        json={
            "prep_sample_idx": [lifecycle["ps_done"], lifecycle["ps_pending"], 99_999_999],
            "state": "invalidated",
            "reason": "OOM-escalated into a larger Arrow batch",
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["updated"] == [lifecycle["ps_done"]]
    assert body["skipped_pending"] == [lifecycle["ps_pending"]]
    assert body["not_found"] == [99_999_999]
    assert body["unchanged"] == []

    row = await postgres_pool.fetchrow(
        "SELECT state, invalidation_reason, invalidated_by_idx, invalidated_at"
        "  FROM qiita.mask_sample WHERE mask_idx = $1 AND prep_sample_idx = $2",
        lifecycle["mask_idx"],
        lifecycle["ps_done"],
    )
    assert row["state"] == "invalidated"
    assert row["invalidation_reason"] == "OOM-escalated into a larger Arrow batch"
    assert row["invalidated_by_idx"] == lifecycle["principal_idx"]
    assert row["invalidated_at"] is not None

    # The pending row was left entirely alone — the masking pipeline owns it.
    assert (
        await postgres_pool.fetchval(
            "SELECT state FROM qiita.mask_sample WHERE mask_idx = $1 AND prep_sample_idx = $2",
            lifecycle["mask_idx"],
            lifecycle["ps_pending"],
        )
        == "pending"
    )


async def test_invalidation_is_idempotent(client, lifecycle):
    body = {
        "prep_sample_idx": [lifecycle["ps_done"]],
        "state": "invalidated",
        "reason": "same reason",
    }
    url = URL_MASK_DEFINITION_SAMPLE_STATUS.format(mask_idx=lifecycle["mask_idx"])
    first = await client.patch(url, json=body)
    second = await client.patch(url, json=body)
    assert first.json()["updated"] == [lifecycle["ps_done"]]
    assert second.json()["updated"] == []
    assert second.json()["unchanged"] == [lifecycle["ps_done"]]


async def test_restoring_a_run_clears_the_invalidation_provenance(client, postgres_pool, lifecycle):
    url = URL_MASK_DEFINITION_SAMPLE_STATUS.format(mask_idx=lifecycle["mask_idx"])
    await client.patch(
        url,
        json={
            "prep_sample_idx": [lifecycle["ps_done"]],
            "state": "invalidated",
            "reason": "withdrawn in error",
        },
    )
    resp = await client.patch(
        url, json={"prep_sample_idx": [lifecycle["ps_done"]], "state": "completed"}
    )
    assert resp.status_code == 200, resp.text
    row = await postgres_pool.fetchrow(
        "SELECT state, invalidation_reason, invalidated_by_idx, invalidated_at"
        "  FROM qiita.mask_sample WHERE mask_idx = $1 AND prep_sample_idx = $2",
        lifecycle["mask_idx"],
        lifecycle["ps_done"],
    )
    assert row["state"] == "completed"
    assert row["invalidation_reason"] is None
    assert row["invalidated_by_idx"] is None
    assert row["invalidated_at"] is None


async def test_invalidating_requires_a_reason(client, lifecycle):
    resp = await client.patch(
        URL_MASK_DEFINITION_SAMPLE_STATUS.format(mask_idx=lifecycle["mask_idx"]),
        json={"prep_sample_idx": [lifecycle["ps_done"]], "state": "invalidated"},
    )
    assert resp.status_code == 422, resp.text


async def test_sample_status_on_an_unknown_mask_404s(client, lifecycle):
    resp = await client.patch(
        URL_MASK_DEFINITION_SAMPLE_STATUS.format(mask_idx=99_999_999),
        json={
            "prep_sample_idx": [lifecycle["ps_done"]],
            "state": "invalidated",
            "reason": "x",
        },
    )
    assert resp.status_code == 404


async def test_the_list_tally_counts_an_invalidated_run_separately(client, lifecycle):
    """Not folded into `samples_pending`: an invalidated run is neither usable nor
    still coming, and counting it as pending reads as "wait for it"."""
    before = await client.get(
        URL_MASK_DEFINITION_PREFIX, params={"prep_sample_idx": lifecycle["ps_done"]}
    )
    row = next(m for m in before.json()["masks"] if m["mask_idx"] == lifecycle["mask_idx"])
    assert row["samples_completed"] == 1
    assert row["samples_invalidated"] == 0

    await client.patch(
        URL_MASK_DEFINITION_SAMPLE_STATUS.format(mask_idx=lifecycle["mask_idx"]),
        json={
            "prep_sample_idx": [lifecycle["ps_done"]],
            "state": "invalidated",
            "reason": "withdrawn",
        },
    )
    after = await client.get(
        URL_MASK_DEFINITION_PREFIX, params={"prep_sample_idx": lifecycle["ps_done"]}
    )
    row = next(m for m in after.json()["masks"] if m["mask_idx"] == lifecycle["mask_idx"])
    assert row["samples_completed"] == 0
    assert row["samples_invalidated"] == 1


# ---------------------------------------------------------------------------
# Run lifecycle: refuse to CONSUME
# ---------------------------------------------------------------------------


async def test_an_invalidated_run_is_refused_by_the_read_masked_doget(
    postgres_pool, compute_worker_service_account, lifecycle
):
    """The consumer refuses without having grown a check: the DoGet route compares
    the gate to 'completed' exactly, so a third value is refused by construction.
    Asserted through the route rather than the helper, because a helper returning
    the right string proves nothing about what the consumer does with it."""
    from qiita_common.api_paths import URL_READ_MASKED_DOGET

    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    _install_settings(app)
    body = {
        "prep_sample_idx": lifecycle["ps_done"],
        "mask_idx": lifecycle["mask_idx"],
    }
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {compute_worker_service_account['token']}"},
    ) as ac:
        # Control: while the run is 'completed' the gate lets it through, so the
        # refusal below is attributable to the state and not to the request.
        before = await ac.post(URL_READ_MASKED_DOGET, json=body)
        assert before.status_code == 201, before.text

        await postgres_pool.execute(
            "UPDATE qiita.mask_sample"
            "   SET state = 'invalidated', invalidated_at = now(),"
            "       invalidated_by_idx = $3, invalidation_reason = 'withdrawn'"
            " WHERE mask_idx = $1 AND prep_sample_idx = $2",
            lifecycle["mask_idx"],
            lifecycle["ps_done"],
            lifecycle["principal_idx"],
        )
        after = await ac.post(URL_READ_MASKED_DOGET, json=body)

    assert after.status_code == 409, after.text
    assert after.json()["detail"]["mask_state"] == "invalidated"


async def test_align_planning_refuses_a_deprecated_mask(postgres_pool, client, lifecycle):
    """Aligning builds NEW results, so it is refused for the same reason the mint
    is. Withdrawn individual runs need no check here: the cohort admits only gate
    state 'completed'."""
    from qiita_control_plane import align_planner
    from qiita_control_plane.testing.db_seeds import seed_sequenced_sample_subtype

    run_idx, pool_idx, ss_idx = await seed_sequenced_sample_subtype(
        postgres_pool,
        prep_sample_idx=lifecycle["ps_done"],
        owner_idx=lifecycle["principal_idx"],
        sequenced_pool_item_id=f"item-{secrets.token_hex(4)}",
    )
    try:
        kwargs = dict(
            app=None,
            sequencing_run_idx=run_idx,
            sequenced_pool_idx=pool_idx,
            reference_idx=1,
            mask_idx=lifecycle["mask_idx"],
            only_missing=False,
            originator_principal_idx=lifecycle["principal_idx"],
            align_action_id="align",
            align_action_version="1.0.0",
        )
        # Control: while the config is ACTIVE the plan gets past the mask guard and
        # fails on something else, so the refusal below is attributable to the
        # status rather than to the call being malformed.
        with pytest.raises(align_planner.AlignReferenceNotFound):
            await align_planner.plan_and_submit_alignments(postgres_pool, **kwargs)

        await _deprecate(client, lifecycle["mask_idx"])
        with pytest.raises(align_planner.AlignMaskDeprecated) as ei:
            await align_planner.plan_and_submit_alignments(postgres_pool, **kwargs)
        assert str(lifecycle["mask_idx"]) in str(ei.value)
    finally:
        await postgres_pool.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
        await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
        await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)


async def test_deprecation_does_not_touch_its_runs(client, postgres_pool, lifecycle):
    """The two markers are independent: voiding a config says nothing about the
    runs already made under it, which is the whole reason both exist. Deprecating
    mask 11 to flag 7 bad preps would otherwise have voided 19 sound ones."""
    await _deprecate(client, lifecycle["mask_idx"])
    states = {
        r["prep_sample_idx"]: r["state"]
        for r in await postgres_pool.fetch(
            "SELECT prep_sample_idx, state FROM qiita.mask_sample WHERE mask_idx = $1",
            lifecycle["mask_idx"],
        )
    }
    assert states == {lifecycle["ps_done"]: "completed", lifecycle["ps_pending"]: "pending"}


async def test_lifecycle_patches_require_the_lifecycle_scope(
    postgres_pool, wet_lab_admin_session, lifecycle
):
    """wet_lab_admin holds prep_sample:write but not mask_definition:lifecycle —
    deciding that published results came from a filter we no longer stand behind is
    a system_admin judgement."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    _install_settings(app)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {wet_lab_admin_session['token']}"},
    ) as ac:
        status_resp = await ac.patch(
            URL_MASK_DEFINITION_STATUS.format(mask_idx=lifecycle["mask_idx"]),
            json={"status": "deprecated", "reason": "x"},
        )
        sample_resp = await ac.patch(
            URL_MASK_DEFINITION_SAMPLE_STATUS.format(mask_idx=lifecycle["mask_idx"]),
            json={
                "prep_sample_idx": [lifecycle["ps_done"]],
                "state": "invalidated",
                "reason": "x",
            },
        )
    assert status_resp.status_code == 403
    assert sample_resp.status_code == 403
    assert json.loads(status_resp.text)["detail"]
