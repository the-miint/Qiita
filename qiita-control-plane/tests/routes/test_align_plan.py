"""Route tests for POST /sequencing-run/{R}/sequenced-pool/{P}/align-plan — the
bulk-block sharded-alignment entrypoint (the align analog of block-mask-plan).

Covers the HTTP wiring (request → planner → response model), the auth gate
(wet_lab_admin + prep_sample:write), the 503 when the align workflow isn't synced,
the mask-LOOKUP skip reasons (no mask / mask not completed), the reference-readiness
409, the disallow-without-delete / only_missing resubmit path, and the model-level
minimap2⇒rype validation.

schedule_dispatch is monkeypatched to a recorder (no orchestrator hop). The align
planner looks up each sample's already-minted mask (with adapter_set_hash None,
since the shared app's Settings leave the adapter reference unset), so the fixture
mints the matching masks + flips their mask_sample gate to 'completed'.
"""

import secrets

import pytest
import pytest_asyncio
from qiita_common.api_paths import URL_SEQUENCED_POOL_ALIGN_PLAN

from qiita_control_plane import align_planner
from qiita_control_plane.repositories.mask_definition import mint_mask_definition
from qiita_control_plane.repositories.sequence_range import mint_sequence_range
from qiita_control_plane.runner import _build_mask_params
from qiita_control_plane.testing.db_seeds import seed_biosample_with_sequenced_prep_sample

pytestmark = pytest.mark.db

_N_SHARDS = 2


@pytest.fixture
def ctx(role_keyed_clients):
    return role_keyed_clients


async def _seed_align_action(db, *, enabled: bool = True):
    """Seed the align action so the block ticket FK resolves. Audience wet_lab_admin+
    (matches the shipped align workflow); scope prep_sample:write."""
    await db.execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status, enabled)"
        " VALUES ($1, $2, 'block'::qiita.scope_target_kind, ARRAY['prep_sample:write']::text[],"
        "         $3::jsonb, '{}'::jsonb, '[]'::jsonb, 1, 1, '1 minute', NULL, NULL, $4)",
        align_planner.ALIGN_ACTION_ID,
        align_planner.ALIGN_ACTION_VERSION,
        '{"service": false, "human_roles": ["wet_lab_admin", "system_admin"]}',
        enabled,
    )


async def _seed_active_sharded_reference(db, owner, suffix) -> int:
    """An ACTIVE sharded reference: a reference row + a rype_router (shard_id NULL) +
    per-shard minimap2 index rows + reference_membership rows carrying shard_id
    (the shard-set the alignment identity folds in). Returns reference_idx."""
    reference_idx = await db.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', 'active', $2) RETURNING reference_idx",
        f"align-plan-ref-{suffix}",
        owner,
    )
    await db.execute(
        "INSERT INTO qiita.reference_index (reference_idx, index_type, fs_path, params, shard_id)"
        " VALUES ($1, 'rype_router', $2, '{}'::jsonb, NULL)",
        reference_idx,
        f"/derived/references/{reference_idx}/rype-router.ryxdi",
    )
    # A feature per shard + its membership row carrying shard_id, plus the per-shard
    # minimap2 index row the resolver requires.
    for shard_id in range(_N_SHARDS):
        feature_idx = await db.fetchval(
            "INSERT INTO qiita.feature (sequence_hash) VALUES (gen_random_uuid())"
            " RETURNING feature_idx"
        )
        await db.execute(
            "INSERT INTO qiita.reference_membership (reference_idx, feature_idx, shard_id)"
            " VALUES ($1, $2, $3)",
            reference_idx,
            feature_idx,
            shard_id,
        )
        await db.execute(
            "INSERT INTO qiita.reference_index"
            "  (reference_idx, index_type, fs_path, params, shard_id)"
            " VALUES ($1, 'minimap2', $2, '{}'::jsonb, $3)",
            reference_idx,
            f"/derived/references/{reference_idx}/minimap2-shards/{shard_id}.mmi",
            shard_id,
        )
    return reference_idx


@pytest_asyncio.fixture
async def planned(ctx, monkeypatch):
    """Configure the shared app, seed a run + pool + two samples with reads, an
    ACTIVE sharded reference, and a COMPLETED read-mask per sample (params matching
    what the align planner reconstructs). Yields the ids + a dispatch recorder."""
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    db = ctx["pool"]
    owner = ctx["wet_session"]["principal_idx"]

    saved = {
        "settings": getattr(app.state, "settings", None),
        "cbc": getattr(app.state, "compute_backend_client", None),
        "rd": getattr(app.state, "running_dispatches", None),
    }
    app.state.settings = Settings(
        database_url="unused", flight_signing_key=b"\x00" * 32, data_plane_url="unused"
    )
    app.state.compute_backend_client = object()
    app.state.running_dispatches = set()
    dispatched: list[int] = []
    monkeypatch.setattr(
        align_planner, "schedule_dispatch", lambda app, wt, **kw: dispatched.append(wt)
    )

    suffix = secrets.token_hex(4)
    instrument_model = "NovaSeq 6000"
    run_idx = await db.fetchval(
        "INSERT INTO qiita.sequencing_run"
        "  (instrument_run_id, platform, instrument_model, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, $2, $3) RETURNING idx",
        f"alignplan-run-{suffix}",
        instrument_model,
        owner,
    )
    pool_idx = await db.fetchval(
        "INSERT INTO qiita.sequenced_pool (sequencing_run_idx, created_by_idx)"
        " VALUES ($1, $2) RETURNING idx",
        run_idx,
        owner,
    )
    reference_idx = await _seed_active_sharded_reference(db, owner, suffix)

    prep_samples: list[int] = []
    biosamples: list[int] = []
    mask_idxs: set[int] = set()
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
            f"align-item-{ps}",
            owner,
        )
        async with db.acquire() as conn, conn.transaction():
            await mint_sequence_range(conn, prep_sample_idx=ps, count=150, principal_idx=owner)
        # Mint the sample's read-mask with the EXACT params the align planner
        # reconstructs (adapter_set_hash None; no host refs), then flip its gate
        # COMPLETED so the planner considers it.
        prep_protocol_idx = await db.fetchval(
            "SELECT prep_protocol_idx FROM qiita.prep_sample WHERE idx = $1", ps
        )
        params = _build_mask_params(
            action_id="read-mask",
            action_version="1.0.0",
            prep_protocol_idx=prep_protocol_idx,
            instrument_model=instrument_model,
            adapter_set_hash=None,
            host_rype_reference_idx=None,
            host_minimap2_reference_idx=None,
        )
        async with db.acquire() as conn:
            mask = await mint_mask_definition(
                conn,
                filter_workflow="read-mask",
                filter_version="1.0.0",
                params=params,
                principal_idx=owner,
            )
        mask_idxs.add(mask["mask_idx"])
        await db.execute(
            "INSERT INTO qiita.mask_sample (mask_idx, prep_sample_idx, state)"
            " VALUES ($1, $2, 'completed')",
            mask["mask_idx"],
            ps,
        )

    yield {
        "db": db,
        "run_idx": run_idx,
        "pool_idx": pool_idx,
        "reference_idx": reference_idx,
        "prep_samples": prep_samples,
        "mask_idxs": mask_idxs,
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
        "DELETE FROM qiita.alignment_sample WHERE prep_sample_idx = ANY($1::bigint[])", prep_samples
    )
    if mask_idxs:
        await db.execute(
            "DELETE FROM qiita.alignment_definition WHERE (params->>'mask_idx')::bigint"
            "   = ANY($1::bigint[])",
            list(mask_idxs),
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
    await db.execute("DELETE FROM qiita.reference_index WHERE reference_idx = $1", reference_idx)
    await db.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", reference_idx
    )
    await db.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx)
    await db.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
        align_planner.ALIGN_ACTION_ID,
        align_planner.ALIGN_ACTION_VERSION,
    )
    app.state.settings = saved["settings"]
    app.state.compute_backend_client = saved["cbc"]
    app.state.running_dispatches = saved["rd"]


def _url(planned):
    return URL_SEQUENCED_POOL_ALIGN_PLAN.format(
        sequencing_run_idx=planned["run_idx"], sequenced_pool_idx=planned["pool_idx"]
    )


def _body(planned, **overrides):
    return {"reference_idx": planned["reference_idx"], "aligner": "minimap2", **overrides}


async def test_align_plan_happy_path(ctx, planned):
    await _seed_align_action(planned["db"])
    resp = await ctx["wet"].post(_url(planned), json=_body(planned))
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["sequenced_pool_idx"] == planned["pool_idx"]
    assert body["reference_idx"] == planned["reference_idx"]
    assert body["aligner"] == "minimap2"
    assert body["samples_planned"] == 2
    assert body["samples_skipped_no_mask"] == 0
    assert body["samples_skipped_mask_incomplete"] == 0
    # Both samples share one prep_protocol → one mask → one alignment partition,
    # 150+150 reads under the default 10M target → one block.
    assert body["blocks_created"] == 1
    assert len(body["partitions"]) == 1
    assert body["partitions"][0]["alignment_idx"] > 0
    assert body["blocks"][0]["read_count"] == 300
    assert planned["dispatched"] == [body["blocks"][0]["work_ticket_idx"]]

    # DB: a block-scoped ticket carrying alignment_idx + a PENDING gate per sample.
    alignment_idx = body["partitions"][0]["alignment_idx"]
    ticket = await planned["db"].fetchrow(
        "SELECT scope_target_kind, block_idx, alignment_idx, mask_idx FROM qiita.work_ticket"
        " WHERE work_ticket_idx = $1",
        body["blocks"][0]["work_ticket_idx"],
    )
    assert ticket["scope_target_kind"] == "block"
    assert ticket["alignment_idx"] == alignment_idx
    assert ticket["mask_idx"] is not None
    gate = await planned["db"].fetchval(
        "SELECT count(*) FROM qiita.alignment_sample"
        " WHERE alignment_idx = $1 AND state = 'pending'",
        alignment_idx,
    )
    assert gate == 2


async def test_align_plan_skips_uncompleted_and_unmasked(ctx, planned):
    """A sample whose mask gate is still 'pending' is skipped (mask_incomplete);
    the planner aligns only fully-masked samples."""
    await _seed_align_action(planned["db"])
    # Flip ONE sample's mask gate back to pending → it must be skipped.
    ps0 = planned["prep_samples"][0]
    await planned["db"].execute(
        "UPDATE qiita.mask_sample SET state = 'pending' WHERE prep_sample_idx = $1", ps0
    )
    resp = await ctx["wet"].post(_url(planned), json=_body(planned))
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["samples_planned"] == 1
    assert body["samples_skipped_mask_incomplete"] == 1


async def test_align_plan_skips_unmasked_sample(ctx, planned):
    """A pool sample whose filtering config was NEVER masked (a different
    prep_protocol → a different mask identity that lookup_mask_idx_by_params misses)
    is skipped as `no_mask` — distinct from a masked-but-not-completed sample. The
    planner never mints a mask, so it aligns only already-masked samples."""
    await _seed_align_action(planned["db"])
    db = planned["db"]
    owner = planned["owner"]
    # A sample with a DIFFERENT prep_protocol → its _build_mask_params differ →
    # lookup_mask_idx_by_params returns None → no_mask. No mask is minted for it.
    bs, ps = await seed_biosample_with_sequenced_prep_sample(
        db, owner_idx=owner, protocol_name="short_read_amplicon"
    )
    try:
        await db.execute(
            "INSERT INTO qiita.sequenced_sample"
            "  (prep_sample_idx, sequenced_pool_idx, sequenced_pool_item_id, created_by_idx)"
            " VALUES ($1, $2, $3, $4)",
            ps,
            planned["pool_idx"],
            f"amplicon-{ps}",
            owner,
        )
        async with db.acquire() as conn, conn.transaction():
            await mint_sequence_range(conn, prep_sample_idx=ps, count=150, principal_idx=owner)

        resp = await ctx["wet"].post(_url(planned), json=_body(planned))
        assert resp.status_code == 202, resp.text
        body = resp.json()
        # The two default-protocol samples plan; the amplicon sample skips as no_mask.
        assert body["samples_planned"] == 2
        assert body["samples_skipped_no_mask"] == 1
    finally:
        await db.execute(
            "DELETE FROM qiita.work_ticket WHERE block_idx IN"
            " (SELECT block_idx FROM qiita.block_member WHERE prep_sample_idx = $1)",
            ps,
        )
        await db.execute(
            "DELETE FROM qiita.block WHERE block_idx IN"
            " (SELECT block_idx FROM qiita.block_member WHERE prep_sample_idx = $1)",
            ps,
        )
        await db.execute("DELETE FROM qiita.sequence_range WHERE prep_sample_idx = $1", ps)
        await db.execute("DELETE FROM qiita.sequenced_sample WHERE prep_sample_idx = $1", ps)
        await db.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", ps)
        await db.execute("DELETE FROM qiita.biosample WHERE idx = $1", bs)


async def test_align_plan_resubmit_over_completed_409(ctx, planned):
    """Re-planning a pool whose samples already carry an alignment gate is a 409;
    only_missing then skips them and returns 202 with nothing new planned."""
    await _seed_align_action(planned["db"])
    first = await ctx["wet"].post(_url(planned), json=_body(planned))
    assert first.status_code == 202, first.text

    resp = await ctx["wet"].post(_url(planned), json=_body(planned))
    assert resp.status_code == 409, resp.text
    conflicting = resp.json()["detail"]["conflicting_prep_sample_idxs"]
    assert set(conflicting) == set(planned["prep_samples"])

    ok = await ctx["wet"].post(_url(planned), json=_body(planned, only_missing=True))
    assert ok.status_code == 202, ok.text
    assert ok.json()["samples_planned"] == 0
    assert ok.json()["blocks_created"] == 0


async def test_align_plan_reference_not_active_409(ctx, planned):
    """A reference that isn't ACTIVE + sharded fails 409 (AlignReferenceNotReady)."""
    await _seed_align_action(planned["db"])
    await planned["db"].execute(
        "UPDATE qiita.reference SET status = 'indexing' WHERE reference_idx = $1",
        planned["reference_idx"],
    )
    resp = await ctx["wet"].post(_url(planned), json=_body(planned))
    assert resp.status_code == 409, resp.text


async def test_align_plan_unknown_reference_404(ctx, planned):
    await _seed_align_action(planned["db"])
    resp = await ctx["wet"].post(_url(planned), json=_body(planned, reference_idx=99999999))
    assert resp.status_code == 404, resp.text


async def test_align_plan_missing_action_503(ctx, planned):
    # No align action seeded → 503 (sync actions first) rather than a 500 at the FK.
    resp = await ctx["wet"].post(_url(planned), json=_body(planned))
    assert resp.status_code == 503, resp.text
    assert "actions sync" in resp.json()["detail"]


async def test_align_plan_requires_wet_lab_admin(ctx, planned):
    await _seed_align_action(planned["db"])
    resp = await ctx["user"].post(_url(planned), json=_body(planned))
    assert resp.status_code == 403, resp.text


async def test_align_plan_minimap2_body_minimap2_requires_rype_422(ctx, planned):
    await _seed_align_action(planned["db"])
    resp = await ctx["wet"].post(_url(planned), json=_body(planned, host_minimap2_reference_idx=9))
    assert resp.status_code == 422, resp.text
