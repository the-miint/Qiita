"""Route tests for GET /qc-check/{prep_sample_idx}."""

import secrets

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_QC_CHECK_BY_PREP_SAMPLE
from qiita_common.models import QcCheck, QcCheckState, QcSeverity, Tier, WorkTicketState

from qiita_control_plane.testing.db_seeds import (
    delete_action_if_created,
    seed_action_if_absent,
    seed_biosample_to_study_link,
    seed_biosample_with_sequenced_prep_sample,
    seed_prep_sample_to_study_link,
    seed_user_principal,
)
from qiita_control_plane.testing.db_teardown import delete_principal, teardown_entity_graph

from .conftest import _grant_study_access, _seed_study

pytestmark = pytest.mark.db

_ACTION_ID = "qc-check-route-test"


async def _seed_ticket(pool, *, principal_idx: int, version: str, prep_sample_idx: int) -> int:
    return await pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, prep_sample_idx, action_context, state"
        ") VALUES ($1, $2, $3, 'prep_sample', $4, '{}'::jsonb, $5)"
        " RETURNING work_ticket_idx",
        _ACTION_ID,
        version,
        principal_idx,
        prep_sample_idx,
        WorkTicketState.COMPLETED,
    )


async def _add_check(
    pool, ticket_idx: int, prep_sample_idx: int, check: QcCheck, severity: QcSeverity, **kw
):
    await pool.execute(
        "INSERT INTO qiita.prep_sample_qc_check"
        ' (prep_sample_idx, "check", severity, reason, measured_value, threshold,'
        "  work_ticket_idx, state, invalidated_at, invalidated_by_idx, invalidation_reason)"
        " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
        prep_sample_idx,
        check,
        severity,
        kw.get("reason"),
        kw.get("measured_value"),
        kw.get("threshold"),
        ticket_idx,
        kw.get("state", QcCheckState.CURRENT),
        kw.get("invalidated_at"),
        kw.get("invalidated_by_idx"),
        kw.get("invalidation_reason"),
    )


def _url(prep_sample_idx: int) -> str:
    return URL_QC_CHECK_BY_PREP_SAMPLE.format(prep_sample_idx=prep_sample_idx)


@pytest_asyncio.fixture
async def ctx(
    postgres_pool, regular_user_session, wet_lab_admin_session, compute_worker_service_account
):
    from qiita_control_plane.main import app

    pool = postgres_pool
    app.state.pool = pool
    transport = ASGITransport(app=app)
    owner_idx = await seed_user_principal(pool, prefix="qc-route", suffix=secrets.token_hex(4))
    version = f"v-{secrets.token_hex(4)}"
    action_created = await seed_action_if_absent(
        pool, action_id=_ACTION_ID, version=version, target_kind="prep_sample"
    )
    created: dict[str, list[int]] = {
        "biosample": [],
        "prep_sample": [],
        "principal": [owner_idx],
        "study": [],
    }
    seed_ctx = {"pool": pool, "created": created}

    async def make_prep_sample(*, linked: bool = True) -> tuple[int, int, int | None]:
        bs_idx, ps_idx = await seed_biosample_with_sequenced_prep_sample(pool, owner_idx=owner_idx)
        created["biosample"].append(bs_idx)
        created["prep_sample"].append(ps_idx)
        study_idx = None
        if linked:
            study_idx = await _seed_study(seed_ctx, owner_idx=owner_idx, suffix="qc")
            await seed_biosample_to_study_link(
                pool, biosample_idx=bs_idx, study_idx=study_idx, created_by_idx=owner_idx
            )
            await seed_prep_sample_to_study_link(
                pool, prep_sample_idx=ps_idx, study_idx=study_idx, created_by_idx=owner_idx
            )
        ticket_idx = await _seed_ticket(
            pool, principal_idx=owner_idx, version=version, prep_sample_idx=ps_idx
        )
        return ps_idx, ticket_idx, study_idx

    def client(token: str | None = None) -> AsyncClient:
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        return AsyncClient(transport=transport, base_url="http://test", headers=headers)

    async with (
        client() as anon,
        client(regular_user_session["token"]) as user,
        client(wet_lab_admin_session["token"]) as wet,
        client(compute_worker_service_account["token"]) as sa,
    ):
        yield {
            "pool": pool,
            "anon": anon,
            "user": user,
            "wet": wet,
            "sa": sa,
            "user_idx": regular_user_session["principal_idx"],
            "owner_idx": owner_idx,
            "seed_ctx": seed_ctx,
            "make_prep_sample": make_prep_sample,
        }

    await pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        _ACTION_ID,
        version,
    )
    await teardown_entity_graph(
        pool,
        study_idxs=created["study"],
        biosample_idxs=created["biosample"],
        prep_sample_idxs=created["prep_sample"],
    )
    await delete_action_if_created(
        pool, action_id=_ACTION_ID, version=version, created=action_created
    )
    await delete_principal(pool, created["principal"])


async def _as_viewer(ctx, study_idx: int) -> None:
    await _grant_study_access(
        ctx["seed_ctx"],
        study_idx=study_idx,
        principal_idx=ctx["user_idx"],
        tier=Tier.VIEWER,
        granted_by_idx=ctx["owner_idx"],
    )


async def test_anonymous_401(ctx):
    ps_idx, _, _ = await ctx["make_prep_sample"]()
    assert (await ctx["anon"].get(_url(ps_idx))).status_code == 401


async def test_service_account_403(ctx):
    ps_idx, _, _ = await ctx["make_prep_sample"]()
    assert (await ctx["sa"].get(_url(ps_idx))).status_code == 403


async def test_viewer_reads_rows_and_verdict(ctx):
    ps_idx, ticket_idx, study_idx = await ctx["make_prep_sample"]()
    await _as_viewer(ctx, study_idx)
    pool = ctx["pool"]
    await _add_check(pool, ticket_idx, ps_idx, QcCheck.DATA_TYPE, QcSeverity.INFO)
    await _add_check(
        pool,
        ticket_idx,
        ps_idx,
        QcCheck.COMPOSITION,
        QcSeverity.FAIL,
        reason="high_unknown",
        measured_value=0.9,
        threshold=0.5,
    )
    await _add_check(
        pool,
        ticket_idx,
        ps_idx,
        QcCheck.CLASSIFIER,
        QcSeverity.WARNING,
        state=QcCheckState.INVALIDATED,
        invalidated_at=await pool.fetchval("SELECT now()"),
        invalidated_by_idx=ctx["owner_idx"],
        invalidation_reason="replaced",
    )

    resp = await ctx["user"].get(_url(ps_idx))

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["prep_sample_idx"] == ps_idx
    assert body["verdict"] == {"severity": QcSeverity.FAIL, "check": QcCheck.COMPOSITION}
    assert [c["check"] for c in body["checks"]] == [
        QcCheck.CLASSIFIER,
        QcCheck.COMPOSITION,
        QcCheck.DATA_TYPE,
    ]
    composition = body["checks"][1]
    assert composition["reason"] == "high_unknown"
    assert composition["measured_value"] == 0.9
    assert composition["threshold"] == 0.5
    assert composition["work_ticket_idx"] == ticket_idx
    classifier = body["checks"][0]
    assert classifier["state"] == QcCheckState.INVALIDATED
    assert classifier["invalidation_reason"] == "replaced"
    assert "invalidated_by_idx" not in classifier


async def test_viewer_on_prep_sample_with_no_rows_gets_empty(ctx):
    ps_idx, _, study_idx = await ctx["make_prep_sample"]()
    await _as_viewer(ctx, study_idx)
    resp = await ctx["user"].get(_url(ps_idx))
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"prep_sample_idx": ps_idx, "verdict": None, "checks": []}


async def test_no_access_403_for_existing_foreign_prep_sample_without_leaking_rows(ctx):
    ps_idx, ticket_idx, _ = await ctx["make_prep_sample"]()
    await _add_check(ctx["pool"], ticket_idx, ps_idx, QcCheck.DATA_TYPE, QcSeverity.FAIL)
    resp = await ctx["user"].get(_url(ps_idx))
    assert resp.status_code == 403, resp.text
    assert QcCheck.DATA_TYPE not in resp.text


async def test_no_access_403_for_unlinked_prep_sample(ctx):
    ps_idx, _, _ = await ctx["make_prep_sample"](linked=False)
    assert (await ctx["user"].get(_url(ps_idx))).status_code == 403


async def test_no_access_403_for_nonexistent_prep_sample(ctx):
    bogus = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.prep_sample") + 999
    assert (await ctx["user"].get(_url(bogus))).status_code == 403


async def test_admin_reads_foreign_prep_sample(ctx):
    ps_idx, ticket_idx, _ = await ctx["make_prep_sample"]()
    await _add_check(ctx["pool"], ticket_idx, ps_idx, QcCheck.DATA_TYPE, QcSeverity.WARNING)
    resp = await ctx["wet"].get(_url(ps_idx))
    assert resp.status_code == 200, resp.text
    assert resp.json()["verdict"] == {"severity": QcSeverity.WARNING, "check": QcCheck.DATA_TYPE}


async def test_admin_gets_404_for_nonexistent_prep_sample(ctx):
    bogus = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.prep_sample") + 999
    assert (await ctx["wet"].get(_url(bogus))).status_code == 404
