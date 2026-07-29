"""Route tests for POST /sequencing-run/{R}/sequenced-pool/{P}/align-plan — the
bulk-block sharded-alignment entrypoint (the align analog of block-mask-plan).

Covers the HTTP wiring (request → planner → response model), the auth gate
(wet_lab_admin + prep_sample:write), the 503 when the align workflow isn't synced,
the mask-selection skip reasons (no gate row / gate not completed), the mask/
reference existence + readiness 4xx, and the disallow-without-delete / only_missing
resubmit path.

The caller names an explicit `mask_idx`; the planner selects the pool's samples
whose `mask_sample` gate is 'completed' under it (alignment does NOT re-derive the
mask config). So the fixture mints ONE mask and flips both samples' gate to
'completed' under it. schedule_dispatch is monkeypatched to a recorder (no
orchestrator hop).
"""

import secrets

import pytest
import pytest_asyncio
from qiita_common.api_paths import URL_SEQUENCED_POOL_ALIGN_PLAN

from qiita_control_plane import align_planner
from qiita_control_plane.repositories.mask_definition import mint_mask_definition
from qiita_control_plane.repositories.sequence_range import mint_sequence_range
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
    per-shard minimap2 AND bowtie2 index rows + reference_membership rows carrying
    shard_id (the shard-set the alignment identity folds in). Both per-aligner index
    sets are seeded because the CP derives the aligner from the run's platform, so
    the reference must be ready for whichever it picks. Returns reference_idx."""
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
    # minimap2 AND bowtie2 index rows the resolver requires (a real active sharded
    # reference builds both per shard).
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
            " VALUES"
            "   ($1, 'minimap2', $2, '{}'::jsonb, $4),"
            "   ($1, 'bowtie2', $3, '{}'::jsonb, $4)",
            reference_idx,
            f"/derived/references/{reference_idx}/minimap2-shards/{shard_id}.mmi",
            f"/derived/references/{reference_idx}/bowtie2-shards/{shard_id}",
            shard_id,
        )
    return reference_idx


@pytest_asyncio.fixture
async def planned(ctx, monkeypatch):
    """Configure the shared app, seed a run + pool + two samples with reads, an
    ACTIVE sharded reference, and ONE mask with both samples' mask_sample gate
    flipped 'completed' under it. Yields the ids (incl. the `mask_idx` to align
    under) + a dispatch recorder."""
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
    run_idx = await db.fetchval(
        "INSERT INTO qiita.sequencing_run"
        "  (instrument_run_id, platform, instrument_model, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, $2, $3) RETURNING idx",
        f"alignplan-run-{suffix}",
        "NovaSeq 6000",
        owner,
    )
    pool_idx = await db.fetchval(
        "INSERT INTO qiita.sequenced_pool (sequencing_run_idx, created_by_idx)"
        " VALUES ($1, $2) RETURNING idx",
        run_idx,
        owner,
    )
    reference_idx = await _seed_active_sharded_reference(db, owner, suffix)

    # ONE mask the caller names; both samples are masked-complete under it. The
    # planner does not re-derive the mask config, so the params are arbitrary.
    async with db.acquire() as conn:
        mask = await mint_mask_definition(
            conn,
            filter_workflow="read-mask",
            filter_version="1.0.0",
            params={"workflow": "read-mask", "s": suffix},
            principal_idx=owner,
        )
    mask_idx = mask["mask_idx"]

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
            f"align-item-{ps}",
            owner,
        )
        async with db.acquire() as conn, conn.transaction():
            await mint_sequence_range(
                conn, prep_sample_idx=ps, count=150, principal_idx=owner, work_ticket_idx=None
            )
        # Both samples masked-complete under the one mask_idx.
        await db.execute(
            "INSERT INTO qiita.mask_sample (mask_idx, prep_sample_idx, state)"
            " VALUES ($1, $2, 'completed')",
            mask_idx,
            ps,
        )

    yield {
        "db": db,
        "run_idx": run_idx,
        "pool_idx": pool_idx,
        "reference_idx": reference_idx,
        "mask_idx": mask_idx,
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
        "DELETE FROM qiita.alignment_sample WHERE prep_sample_idx = ANY($1::bigint[])", prep_samples
    )
    await db.execute(
        "DELETE FROM qiita.alignment_definition WHERE (params->>'mask_idx')::bigint = $1",
        mask_idx,
    )
    await db.execute("DELETE FROM qiita.mask_sample WHERE mask_idx = $1", mask_idx)
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
    await db.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)
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
    # No `aligner` — the server derives it from the run's platform (illumina here →
    # bowtie2). reference_idx + mask_idx are the mandatory fields; only_missing opt.
    return {
        "reference_idx": planned["reference_idx"],
        "mask_idx": planned["mask_idx"],
        **overrides,
    }


async def test_align_plan_happy_path(ctx, planned):
    await _seed_align_action(planned["db"])
    resp = await ctx["wet"].post(_url(planned), json=_body(planned))
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["sequenced_pool_idx"] == planned["pool_idx"]
    assert body["reference_idx"] == planned["reference_idx"]
    # Aligner derived from the run's platform (illumina → bowtie2), not caller-chosen.
    assert body["aligner"] == "bowtie2"
    assert body["samples_planned"] == 2
    assert body["samples_skipped_no_mask"] == 0
    assert body["samples_skipped_mask_incomplete"] == 0
    # Both samples gated completed under the one mask_idx → one alignment partition,
    # 150+150 reads under the default 10M target → one block.
    assert body["blocks_created"] == 1
    assert len(body["partitions"]) == 1
    assert body["partitions"][0]["alignment_idx"] > 0
    assert body["partitions"][0]["mask_idx"] == planned["mask_idx"]
    assert body["blocks"][0]["read_count"] == 300
    assert planned["dispatched"] == [body["blocks"][0]["work_ticket_idx"]]

    # DB: a block-scoped ticket carrying alignment_idx + mask_idx + a PENDING gate.
    alignment_idx = body["partitions"][0]["alignment_idx"]
    ticket = await planned["db"].fetchrow(
        "SELECT scope_target_kind, block_idx, alignment_idx, mask_idx FROM qiita.work_ticket"
        " WHERE work_ticket_idx = $1",
        body["blocks"][0]["work_ticket_idx"],
    )
    assert ticket["scope_target_kind"] == "block"
    assert ticket["alignment_idx"] == alignment_idx
    assert ticket["mask_idx"] == planned["mask_idx"]
    gate = await planned["db"].fetchval(
        "SELECT count(*) FROM qiita.alignment_sample"
        " WHERE alignment_idx = $1 AND state = 'pending'",
        alignment_idx,
    )
    assert gate == 2


async def test_align_plan_no_sample_masked_under_mask_422(ctx, planned):
    """A valid mask under which NO pool sample is masked (no gate rows) is a loud
    422, not a silent 202/0 — the caller named a mask this pool was not masked
    under."""
    await _seed_align_action(planned["db"])
    # A second, real mask with no mask_sample rows for this pool's samples.
    async with planned["db"].acquire() as conn:
        other = await mint_mask_definition(
            conn,
            filter_workflow="read-mask",
            filter_version="1.0.0",
            params={"workflow": "read-mask", "other": secrets.token_hex(4)},
            principal_idx=planned["owner"],
        )
    try:
        resp = await ctx["wet"].post(_url(planned), json=_body(planned, mask_idx=other["mask_idx"]))
        assert resp.status_code == 422, resp.text
        assert "nothing to align" in resp.json()["detail"]
    finally:
        await planned["db"].execute(
            "DELETE FROM qiita.mask_definition WHERE mask_idx = $1", other["mask_idx"]
        )


async def test_align_plan_unknown_mask_404(ctx, planned):
    """A nonexistent mask_idx (a client-supplied identifier) is a 404, distinct from
    a valid mask with no masked samples (422)."""
    await _seed_align_action(planned["db"])
    resp = await ctx["wet"].post(_url(planned), json=_body(planned, mask_idx=99999999))
    assert resp.status_code == 404, resp.text


async def test_align_plan_requires_mask_idx_422(ctx, planned):
    """mask_idx is mandatory — omitting it is a request-model 422 (the caller must
    name the mask to align under)."""
    await _seed_align_action(planned["db"])
    resp = await ctx["wet"].post(_url(planned), json={"reference_idx": planned["reference_idx"]})
    assert resp.status_code == 422, resp.text


async def test_align_plan_long_read_platform_selects_minimap2(ctx, planned):
    """A long-read platform (pacbio_smrt) resolves the aligner to minimap2 — the
    aligner is derived from the run's platform, not the caller."""
    await _seed_align_action(planned["db"])
    await planned["db"].execute(
        "UPDATE qiita.sequencing_run SET platform = 'pacbio_smrt'::qiita.platform WHERE idx = $1",
        planned["run_idx"],
    )
    resp = await ctx["wet"].post(_url(planned), json=_body(planned))
    assert resp.status_code == 202, resp.text
    assert resp.json()["aligner"] == "minimap2"


@pytest.mark.parametrize(
    ("platform", "expected_target"),
    [
        ("illumina", align_planner._BLOCK_TARGET_READS_BY_PLATFORM["illumina"]),
        ("pacbio_smrt", align_planner._LONG_READ_BLOCK_TARGET_READS),
        ("oxford_nanopore", align_planner._LONG_READ_BLOCK_TARGET_READS),
    ],
)
async def test_align_plan_tiles_at_the_platform_block_target(
    ctx, planned, monkeypatch, platform, expected_target
):
    """The platform's block target actually reaches the tiler.

    The map itself is unit-tested (`tests/test_align_planner.py`); what this pins is
    the WIRING — that the planner resolves the target from the run's platform and
    hands it to `tile_partition`, rather than tiling at the short-read default. That
    can't be observed from the response on these fixtures (300 reads fits one block at
    either target), and seeding >1M reads to make it observable would cost far more
    than recording the argument, so record the argument."""
    await _seed_align_action(planned["db"])
    await planned["db"].execute(
        "UPDATE qiita.sequencing_run SET platform = $2::qiita.platform WHERE idx = $1",
        planned["run_idx"],
        platform,
    )

    seen: list[int] = []
    real_tile = align_planner.tile_partition

    def _recording_tile(ranges, *, target_reads):
        seen.append(target_reads)
        return real_tile(ranges, target_reads=target_reads)

    monkeypatch.setattr(align_planner, "tile_partition", _recording_tile)

    resp = await ctx["wet"].post(_url(planned), json=_body(planned))
    assert resp.status_code == 202, resp.text
    assert seen == [expected_target]


async def test_align_plan_unsupported_platform_422(ctx, planned):
    """A platform with no defined sharded aligner (ls454) is refused 422 — fail
    loud rather than defaulting to an aligner."""
    await _seed_align_action(planned["db"])
    await planned["db"].execute(
        "UPDATE qiita.sequencing_run SET platform = 'ls454'::qiita.platform WHERE idx = $1",
        planned["run_idx"],
    )
    resp = await ctx["wet"].post(_url(planned), json=_body(planned))
    assert resp.status_code == 422, resp.text
    assert "no sharded aligner" in resp.text


async def test_align_plan_skips_uncompleted_sample(ctx, planned):
    """A sample whose mask gate is still 'pending' under the named mask is skipped
    (mask_incomplete); the planner aligns only fully-masked samples."""
    await _seed_align_action(planned["db"])
    # Flip ONE sample's mask gate back to pending → it must be skipped.
    ps0 = planned["prep_samples"][0]
    await planned["db"].execute(
        "UPDATE qiita.mask_sample SET state = 'pending'"
        " WHERE prep_sample_idx = $1 AND mask_idx = $2",
        ps0,
        planned["mask_idx"],
    )
    resp = await ctx["wet"].post(_url(planned), json=_body(planned))
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["samples_planned"] == 1
    assert body["samples_skipped_mask_incomplete"] == 1
    assert body["samples_skipped_no_mask"] == 0


async def test_align_plan_all_pending_pool_is_202_zero(ctx, planned):
    """A pool where EVERY sample's gate under the named mask is 'pending' (masking
    in flight) is a legitimate 202/0 — NOT a 422. The 422 (AlignNoMasksFound) is
    reserved for a mask this pool was never masked under (no gate rows at all)."""
    await _seed_align_action(planned["db"])
    await planned["db"].execute(
        "UPDATE qiita.mask_sample SET state = 'pending' WHERE mask_idx = $1",
        planned["mask_idx"],
    )
    resp = await ctx["wet"].post(_url(planned), json=_body(planned))
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["samples_planned"] == 0
    assert body["blocks_created"] == 0
    assert body["samples_skipped_mask_incomplete"] == 2
    assert body["samples_skipped_no_mask"] == 0


async def test_align_plan_skips_sample_with_no_gate_row(ctx, planned):
    """A pool sample with NO mask_sample row under the named mask (never masked
    under it) is skipped as `no_mask` — distinct from a masked-but-not-completed
    sample. The planner aligns only samples masked-complete under the named mask."""
    await _seed_align_action(planned["db"])
    db = planned["db"]
    owner = planned["owner"]
    # A third in-pool sample WITH reads but no gate row under the target mask.
    bs, ps = await seed_biosample_with_sequenced_prep_sample(db, owner_idx=owner)
    try:
        await db.execute(
            "INSERT INTO qiita.sequenced_sample"
            "  (prep_sample_idx, sequenced_pool_idx, sequenced_pool_item_id, created_by_idx)"
            " VALUES ($1, $2, $3, $4)",
            ps,
            planned["pool_idx"],
            f"unmasked-{ps}",
            owner,
        )
        async with db.acquire() as conn, conn.transaction():
            await mint_sequence_range(
                conn, prep_sample_idx=ps, count=150, principal_idx=owner, work_ticket_idx=None
            )
        resp = await ctx["wet"].post(_url(planned), json=_body(planned))
        assert resp.status_code == 202, resp.text
        body = resp.json()
        # The two gated samples plan; the third (no gate row) skips as no_mask.
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
