"""Route tests for POST /sequencing-run/{R}/sequenced-pool/{P}/block-mask-plan —
the bulk-block read-masking entrypoint.

Covers the HTTP wiring (request → planner → response model), the auth gate
(wet_lab_admin + prep_sample:write), the 503 when the block workflow isn't yet
synced, and the model-level minimap2⇒rype validation. The tiling / persistence
logic itself is exercised by the planner unit tests; here the wiring + gates.

schedule_dispatch is monkeypatched to a recorder (no orchestrator hop) and the
shared app is given a stub compute_backend_client + Settings (adapter reference
unset, so no data-plane adapter DoGet). App state is saved/restored so the
mutation doesn't leak to other route tests sharing the module-global app.
"""

import secrets

import pytest
import pytest_asyncio
from qiita_common.api_paths import URL_SEQUENCED_POOL_BLOCK_MASK_PLAN

from qiita_control_plane import block_planner
from qiita_control_plane.repositories.sequence_range import mint_sequence_range
from qiita_control_plane.testing.db_seeds import seed_biosample_with_sequenced_prep_sample

pytestmark = pytest.mark.db


@pytest.fixture
def ctx(role_keyed_clients):
    return role_keyed_clients


async def _seed_block_action(db, *, enabled: bool = True):
    """Seed the read-mask-block action so the block ticket FK resolves. Audience
    wet_lab_admin+ (matches read-mask); scope prep_sample:write."""
    await db.execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status, enabled)"
        " VALUES ($1, $2, 'block'::qiita.scope_target_kind, ARRAY['prep_sample:write']::text[],"
        "         $3::jsonb, '{}'::jsonb, '[]'::jsonb, 1, 1, '1 minute', 'active', 'failed', $4)",
        block_planner.BLOCK_MASK_ACTION_ID,
        block_planner.BLOCK_MASK_ACTION_VERSION,
        '{"service": false, "human_roles": ["wet_lab_admin", "system_admin"]}',
        enabled,
    )


@pytest_asyncio.fixture
async def planned(ctx, monkeypatch):
    """Configure the shared app for dispatch, seed a run + pool + two samples with
    reads, and yield the ids + a dispatch recorder. Restores app state + cleans up."""
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    db = ctx["pool"]
    owner = ctx["wet_session"]["principal_idx"]

    # App wiring: stub compute client + Settings (no adapter ref → no DP hop), and
    # a schedule_dispatch recorder so no real orchestrator work fires.
    saved = {
        "settings": getattr(app.state, "settings", None),
        "cbc": getattr(app.state, "compute_backend_client", None),
        "rd": getattr(app.state, "running_dispatches", None),
    }
    app.state.settings = Settings(
        database_url="unused", hmac_secret_key=b"\x00" * 32, data_plane_url="unused"
    )
    app.state.compute_backend_client = object()
    app.state.running_dispatches = set()
    dispatched: list[int] = []
    monkeypatch.setattr(
        block_planner, "schedule_dispatch", lambda app, wt, **kw: dispatched.append(wt)
    )

    suffix = secrets.token_hex(4)
    run_idx = await db.fetchval(
        "INSERT INTO qiita.sequencing_run"
        "  (instrument_run_id, platform, instrument_model, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, 'NovaSeq 6000', $2) RETURNING idx",
        f"blkplan-run-{suffix}",
        owner,
    )
    pool_idx = await db.fetchval(
        "INSERT INTO qiita.sequenced_pool (sequencing_run_idx, created_by_idx)"
        " VALUES ($1, $2) RETURNING idx",
        run_idx,
        owner,
    )
    prep_samples: list[int] = []
    biosamples: list[int] = []
    for _ in range(2):
        bs, ps = await seed_biosample_with_sequenced_prep_sample(db, owner_idx=owner)
        biosamples.append(bs)
        prep_samples.append(ps)
        await db.execute(
            "INSERT INTO qiita.sequenced_sample"
            "  (prep_sample_idx, sequenced_pool_idx, sequenced_pool_item_id, created_by_idx)"
            " VALUES ($1, $2, $3, $4)",
            ps,
            pool_idx,
            f"blk-item-{ps}",
            owner,
        )
        async with db.acquire() as conn, conn.transaction():
            await mint_sequence_range(conn, prep_sample_idx=ps, count=150, principal_idx=owner)

    yield {
        "db": db,
        "run_idx": run_idx,
        "pool_idx": pool_idx,
        "prep_samples": prep_samples,
        "dispatched": dispatched,
        "owner": owner,
    }

    # Cleanup (FK-reverse, id-scoped).
    await db.execute(
        "DELETE FROM qiita.work_ticket WHERE block_idx IN"
        " (SELECT bm.block_idx FROM qiita.block_member bm"
        "   WHERE bm.prep_sample_idx = ANY($1::bigint[]))",
        prep_samples,
    )
    await db.execute(
        "DELETE FROM qiita.block WHERE block_idx IN"
        " (SELECT block_idx FROM qiita.block_member WHERE prep_sample_idx = ANY($1::bigint[]))",
        prep_samples,
    )
    await db.execute(
        "DELETE FROM qiita.mask_sample WHERE prep_sample_idx = ANY($1::bigint[])", prep_samples
    )
    await db.execute(
        "DELETE FROM qiita.sequence_range WHERE prep_sample_idx = ANY($1::bigint[])", prep_samples
    )
    await db.execute(
        "DELETE FROM qiita.sequenced_sample WHERE prep_sample_idx = ANY($1::bigint[])", prep_samples
    )
    await db.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await db.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await db.execute("DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", prep_samples)
    await db.execute("DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", biosamples)
    await db.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
        block_planner.BLOCK_MASK_ACTION_ID,
        block_planner.BLOCK_MASK_ACTION_VERSION,
    )
    # NOTE: intentionally do NOT delete mask_definition here. `owner` is the
    # session-shared wet_lab_admin principal, so `WHERE created_by_idx = owner`
    # would cascade-delete masks (and their mask_sample rows) minted by OTHER
    # route tests under the same principal. mask_definition rows are idempotent
    # (deduped by params_hash), so leaving this test's masks is harmless.
    # Restore shared app state.
    app.state.settings = saved["settings"]
    app.state.compute_backend_client = saved["cbc"]
    app.state.running_dispatches = saved["rd"]


def _url(planned):
    return URL_SEQUENCED_POOL_BLOCK_MASK_PLAN.format(
        sequencing_run_idx=planned["run_idx"], sequenced_pool_idx=planned["pool_idx"]
    )


async def test_block_mask_plan_happy_path(ctx, planned):
    await _seed_block_action(planned["db"])
    resp = await ctx["wet"].post(_url(planned), json={})
    assert resp.status_code == 202, resp.text
    body = resp.json()
    # Two 150-read samples, default target 10M → one block covering all 300 reads.
    assert body["sequenced_pool_idx"] == planned["pool_idx"]
    assert body["samples_planned"] == 2
    assert body["blocks_created"] == 1
    assert body["host_filter_enabled"] is False
    assert body["instrument_model"] == "NovaSeq 6000"
    assert len(body["partitions"]) == 1
    assert len(body["blocks"]) == 1
    assert body["blocks"][0]["read_count"] == 300
    # The block's ticket was dispatched.
    assert planned["dispatched"] == [body["blocks"][0]["work_ticket_idx"]]

    # DB reflects the plan: a block-scoped ticket + a PENDING gate per sample.
    tickets = await planned["db"].fetch(
        "SELECT scope_target_kind, block_idx, mask_idx FROM qiita.work_ticket"
        " WHERE work_ticket_idx = $1",
        body["blocks"][0]["work_ticket_idx"],
    )
    assert tickets[0]["scope_target_kind"] == "block"
    gate = await planned["db"].fetchval(
        "SELECT count(*) FROM qiita.mask_sample WHERE prep_sample_idx = ANY($1::bigint[])"
        "   AND state = 'pending'",
        planned["prep_samples"],
    )
    assert gate == 2


async def test_block_mask_plan_host_filter_context(ctx, planned):
    await _seed_block_action(planned["db"])
    resp = await ctx["wet"].post(_url(planned), json={"host_rype_reference_idx": 7})
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["host_filter_enabled"] is True
    assert body["host_rype_reference_idx"] == 7


async def test_block_mask_plan_resubmit_over_completed_409(ctx, planned):
    """Re-planning a pool whose samples are already COMPLETED for the resolved
    mask is a 409 (mirrors the sequenced_pool COMPLETED-resubmit rule) — the
    operator DELETEs the mask or passes only_missing=true. only_missing then skips
    the completed samples and returns 202 with nothing new planned."""
    await _seed_block_action(planned["db"])
    first = await ctx["wet"].post(_url(planned), json={})
    assert first.status_code == 202, first.text
    mask_idx = first.json()["partitions"][0]["mask_idx"]
    # Reconcile would flip these to completed; do it directly.
    await planned["db"].execute(
        "UPDATE qiita.mask_sample SET state = 'completed' WHERE mask_idx = $1", mask_idx
    )

    resp = await ctx["wet"].post(_url(planned), json={})
    assert resp.status_code == 409, resp.text
    conflicting = resp.json()["detail"]["conflicting_prep_sample_idxs"]
    assert set(conflicting) == set(planned["prep_samples"])

    ok = await ctx["wet"].post(_url(planned), json={"only_missing": True})
    assert ok.status_code == 202, ok.text
    assert ok.json()["samples_planned"] == 0
    assert ok.json()["blocks_created"] == 0


async def test_block_mask_plan_missing_action_503(ctx, planned):
    # No block action seeded → the endpoint refuses with 503 (sync actions first)
    # rather than 500ing at the ticket FK.
    resp = await ctx["wet"].post(_url(planned), json={})
    assert resp.status_code == 503, resp.text
    assert "actions sync" in resp.json()["detail"]


async def test_block_mask_plan_requires_wet_lab_admin(ctx, planned):
    await _seed_block_action(planned["db"])
    resp = await ctx["user"].post(_url(planned), json={})
    assert resp.status_code == 403, resp.text


async def test_block_mask_plan_minimap2_requires_rype_422(ctx, planned):
    await _seed_block_action(planned["db"])
    resp = await ctx["wet"].post(_url(planned), json={"host_minimap2_reference_idx": 9})
    assert resp.status_code == 422, resp.text
