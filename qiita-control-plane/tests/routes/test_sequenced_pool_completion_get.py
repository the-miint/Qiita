"""Route tests for GET /sequencing-run/{run}/sequenced-pool/{pool}/completion —
the pool's end-to-end processing rollup (demux state + host-masking buckets).

Covers the happy path (per-sample read-mask tickets bucketed + the `complete`
flag), the all-completed pool, the empty pool, the demux `demux_state` /
`fully_processed` wiring, and the read gate (404 missing pool, 422
pool-not-in-run, 401 anonymous, 403 missing scope / regular user). Bucket
precedence is exercised at the repo layer in test_sequenced_pool_completion;
here the wiring, response model, and auth.
"""

import json
import secrets
import uuid
from datetime import UTC, datetime

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_SEQUENCED_POOL_COMPLETION

from qiita_control_plane.main import app
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
)

pytestmark = pytest.mark.db


@pytest.fixture
def ctx(role_keyed_clients):
    """Alias the shared role-keyed clients ({pool, wet, user, wet_session, ...})."""
    return role_keyed_clients


async def _seed_fastq_action(db):
    """Insert a read-mask action so work_ticket FK resolves; return its
    (action_id, version). The completion query matches on the bare action_id."""
    action_id = "read-mask"
    version = f"v-{uuid.uuid4()}"
    await db.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, target_processing_kinds,"
        "  scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status"
        ") VALUES ($1, $2, 'prep_sample'::qiita.scope_target_kind,"
        "          ARRAY['sequenced']::qiita.processing_kind[], ARRAY['prep_sample:write']::text[],"
        "          $3::jsonb, '{}'::jsonb, '[]'::jsonb, 1, 1, '1 minute', 'active', 'failed')",
        action_id,
        version,
        json.dumps({"service": False, "human_roles": ["user"]}),
    )
    return action_id, version


async def _seed_mask(db, owner):
    """One mask_definition, so a completed ticket has a gate row to write."""
    return await db.fetchval(
        "INSERT INTO qiita.mask_definition"
        "  (params_hash, filter_workflow, filter_version, params, created_by_idx)"
        " VALUES ($1, 'read-mask', '1.0', '{}'::jsonb, $2) RETURNING mask_idx",
        secrets.token_bytes(32),
        owner,
    )


async def _add_gate(db, *, mask_idx, prep_sample_idx, state, owner):
    """The qiita.mask_sample row the completion rollup reads.

    mask_sample_invalidation_fields is a biconditional: the three provenance
    columns are set exactly when the state is 'invalidated'."""
    withdrawn = state == "invalidated"
    await db.execute(
        "INSERT INTO qiita.mask_sample"
        "  (mask_idx, prep_sample_idx, state,"
        "   invalidated_at, invalidated_by_idx, invalidation_reason)"
        " VALUES ($1, $2, $3, $4, $5, $6)",
        mask_idx,
        prep_sample_idx,
        state,
        datetime.now(UTC) if withdrawn else None,
        owner if withdrawn else None,
        "test withdrawal" if withdrawn else None,
    )


async def _add_ticket(db, *, action, owner, prep_sample_idx, state, mask_idx=None, gate=True):
    """Attach a per-sample masking ticket.

    A COMPLETED one also writes its 'completed' gate row, mirroring the
    production terminal step (`upsert_mask_sample_completed`) that the completion
    rollup reads. `gate=False` seeds a ticket that completed under a gate row
    that did not."""
    action_id, version = action
    # work_ticket_failure_consistent requires the failure columns set together on
    # a FAILED ticket and all-NULL otherwise.
    failed = state == "failed"
    await db.execute(
        "INSERT INTO qiita.work_ticket"
        "  (action_id, action_version, originator_principal_idx,"
        "   scope_target_kind, prep_sample_idx, mask_idx, state,"
        "   failure_type, failure_stage, failure_reason)"
        " VALUES ($1, $2, $3, 'prep_sample'::qiita.scope_target_kind, $4, $5,"
        "         $6::qiita.work_ticket_state,"
        "         $7::qiita.failure_type, $8::qiita.work_ticket_failure_stage, $9)",
        action_id,
        version,
        owner,
        prep_sample_idx,
        # The mask the run produced. A masking ticket carries one from the
        # PROCESSING transition on, and the rollup's anti-join correlates on it,
        # so a NULL here would make the ticket uncorrelatable to its gate row.
        mask_idx,
        state,
        "permanent" if failed else None,
        "finalize" if failed else None,
        "test failure" if failed else None,
    )
    if state == "completed" and gate and mask_idx is not None:
        await _add_gate(
            db, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx, state="completed", owner=owner
        )


async def _seed_bcl_action(db):
    """Insert a bcl-convert (sequenced_pool-scoped) action so a pool demux
    work_ticket's FK resolves; return its (action_id, version). The
    action_processing_kinds_only_for_prep_sample CHECK requires an empty
    target_processing_kinds for a non-prep_sample target_kind."""
    action_id = "bcl-convert"
    version = f"v-{uuid.uuid4()}"
    await db.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, target_processing_kinds,"
        "  scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status"
        ") VALUES ($1, $2, 'sequenced_pool'::qiita.scope_target_kind,"
        "          ARRAY[]::qiita.processing_kind[], ARRAY['prep_sample:write']::text[],"
        "          $3::jsonb, '{}'::jsonb, '[]'::jsonb, 1, 1, '1 minute', 'active', 'failed')",
        action_id,
        version,
        json.dumps({"service": False, "human_roles": ["user"]}),
    )
    return action_id, version


async def _add_pool_ticket(db, *, action, owner, sequenced_pool_idx, state):
    """Insert a sequenced_pool-scoped (demux) work ticket."""
    action_id, version = action
    failed = state == "failed"
    await db.execute(
        "INSERT INTO qiita.work_ticket"
        "  (action_id, action_version, originator_principal_idx,"
        "   scope_target_kind, sequenced_pool_idx, state,"
        "   failure_type, failure_stage, failure_reason)"
        " VALUES ($1, $2, $3, 'sequenced_pool'::qiita.scope_target_kind, $4,"
        "         $5::qiita.work_ticket_state,"
        "         $6::qiita.failure_type, $7::qiita.work_ticket_failure_stage, $8)",
        action_id,
        version,
        owner,
        sequenced_pool_idx,
        state,
        "permanent" if failed else None,
        "finalize" if failed else None,
        "test failure" if failed else None,
    )


@pytest_asyncio.fixture
async def seeded_pool(ctx):
    """Seed a run + pool with two samples: one with a COMPLETED read-mask
    ticket, one with none (not-submitted). FK-reverse cleanup."""
    db = ctx["pool"]
    owner = ctx["wet_session"]["principal_idx"]
    action = await _seed_fastq_action(db)
    bcl_action = await _seed_bcl_action(db)
    mask_idx = await _seed_mask(db, owner)
    created = []

    bs0, ps0 = await seed_biosample_with_sequenced_prep_sample(db, owner_idx=owner)
    run_idx, pool_idx, ss0 = await seed_sequenced_sample_subtype(
        db, prep_sample_idx=ps0, owner_idx=owner, sequenced_pool_item_id="compl-item-0"
    )
    await _add_ticket(
        db, action=action, owner=owner, prep_sample_idx=ps0, state="completed", mask_idx=mask_idx
    )
    created.append((bs0, ps0, ss0))

    bs1, ps1 = await seed_biosample_with_sequenced_prep_sample(db, owner_idx=owner)
    ss1 = await db.fetchval(
        "INSERT INTO qiita.sequenced_sample"
        "  (prep_sample_idx, sequenced_pool_idx, sequenced_pool_item_id, created_by_idx)"
        " VALUES ($1, $2, $3, $4) RETURNING idx",
        ps1,
        pool_idx,
        "compl-item-1",
        owner,
    )
    created.append((bs1, ps1, ss1))

    yield {
        "run_idx": run_idx,
        "pool_idx": pool_idx,
        "owner": owner,
        "action": action,
        "bcl_action": bcl_action,
        "mask_idx": mask_idx,
    }

    await db.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2", *action
    )
    await db.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2", *bcl_action
    )
    await db.execute("DELETE FROM qiita.mask_sample WHERE mask_idx = $1", mask_idx)
    await db.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)
    for _bs, _ps, ss_idx in created:
        await db.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
    await db.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await db.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await db.execute("DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", *action)
    await db.execute("DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", *bcl_action)
    for _bs, ps_idx, _ss in created:
        await db.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", ps_idx)
    for bs_idx, _ps, _ss in created:
        await db.execute("DELETE FROM qiita.biosample WHERE idx = $1", bs_idx)


def _url(run_idx, pool_idx):
    return URL_SEQUENCED_POOL_COMPLETION.format(
        sequencing_run_idx=run_idx, sequenced_pool_idx=pool_idx
    )


async def test_get_completion_buckets_samples(ctx, seeded_pool):
    resp = await ctx["wet"].get(_url(seeded_pool["run_idx"], seeded_pool["pool_idx"]))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["sequenced_pool_idx"] == seeded_pool["pool_idx"]
    assert body["sequencing_run_idx"] == seeded_pool["run_idx"]
    assert body["sample_count"] == 2
    assert body["samples_completed"] == 1
    assert body["samples_in_flight"] == 0
    assert body["samples_no_data"] == 0
    assert body["samples_failed"] == 0
    assert body["samples_not_submitted"] == 1
    # Not every sample completed → not complete.
    assert body["complete"] is False
    # No bcl-convert ticket seeded by default → demux not_submitted, not fully done.
    assert body["demux_state"] == "not_submitted"
    assert body["fully_processed"] is False
    # Pool-wide (no reference scope) echoes reference_idx=null.
    assert body["reference_idx"] is None


async def test_get_completion_reports_a_withdrawn_run_as_invalidated(ctx, seeded_pool):
    """The operator surface for the whole fix. `qiita pool-completion` prints
    `fully_processed` as "DONE and clean"; before this, a pool whose only masked
    sample had been withdrawn printed exactly that, and every masked-read pull
    then 409'd. The sample is counted, just not as usable."""
    db = ctx["pool"]
    ps0 = await db.fetchval(
        "SELECT prep_sample_idx FROM qiita.sequenced_sample"
        " WHERE sequenced_pool_idx = $1 AND sequenced_pool_item_id = 'compl-item-0'",
        seeded_pool["pool_idx"],
    )
    await db.execute(
        "UPDATE qiita.mask_sample SET state = 'invalidated', invalidated_at = now(),"
        "  invalidated_by_idx = $3, invalidation_reason = 'withdrawn in test'"
        " WHERE mask_idx = $1 AND prep_sample_idx = $2",
        seeded_pool["mask_idx"],
        ps0,
        seeded_pool["owner"],
    )
    resp = await ctx["wet"].get(_url(seeded_pool["run_idx"], seeded_pool["pool_idx"]))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["samples_invalidated"] == 1
    assert body["samples_completed"] == 0
    assert body["complete"] is False
    assert body["fully_processed"] is False


async def test_get_completion_counts_the_same_sample_as_completed_when_not_withdrawn(
    ctx, seeded_pool
):
    """Control for the case above — the fixture's sample, untouched, is completed
    and `samples_invalidated` is zero. Without it the assertions above could hold
    for a sample that was never counted at all."""
    resp = await ctx["wet"].get(_url(seeded_pool["run_idx"], seeded_pool["pool_idx"]))
    body = resp.json()
    assert body["samples_completed"] == 1
    assert body["samples_invalidated"] == 0


async def test_get_completion_reference_scope_echoes_and_narrows(ctx, seeded_pool):
    """?reference_idx=N echoes the scope and narrows host-masking to masks that
    used that reference. The fixture's mask names no host reference in its
    params, so scoping to any reference excludes it and the sample reads
    not_submitted — masked, but not against N."""
    resp = await ctx["wet"].get(
        _url(seeded_pool["run_idx"], seeded_pool["pool_idx"]) + "?reference_idx=555"
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["reference_idx"] == 555
    assert body["sample_count"] == 2
    assert body["samples_completed"] == 0
    assert body["samples_not_submitted"] == 2


async def _complete_second_sample(db, seeded_pool):
    """Give the not-submitted second sample (compl-item-1) a completed read-mask
    ticket so the host-masking stage is `complete`."""
    ps1 = await db.fetchval(
        "SELECT prep_sample_idx FROM qiita.sequenced_sample"
        " WHERE sequenced_pool_idx = $1 AND sequenced_pool_item_id = 'compl-item-1'",
        seeded_pool["pool_idx"],
    )
    await _add_ticket(
        db,
        action=seeded_pool["action"],
        owner=seeded_pool["owner"],
        prep_sample_idx=ps1,
        state="completed",
        mask_idx=seeded_pool["mask_idx"],
    )


async def test_get_completion_fully_processed_when_demux_done_and_all_masked(ctx, seeded_pool):
    """demux COMPLETED + every sample masked → fully_processed True."""
    db = ctx["pool"]
    await _complete_second_sample(db, seeded_pool)
    await _add_pool_ticket(
        db,
        action=seeded_pool["bcl_action"],
        owner=seeded_pool["owner"],
        sequenced_pool_idx=seeded_pool["pool_idx"],
        state="completed",
    )
    body = (await ctx["wet"].get(_url(seeded_pool["run_idx"], seeded_pool["pool_idx"]))).json()
    assert body["demux_state"] == "completed"
    assert body["complete"] is True
    assert body["fully_processed"] is True


async def test_get_completion_failed_demux_blocks_fully_processed(ctx, seeded_pool):
    """Even with every sample masked (`complete`), a FAILED demux keeps
    fully_processed False — the end-to-end flag requires demux completed."""
    db = ctx["pool"]
    await _complete_second_sample(db, seeded_pool)
    await _add_pool_ticket(
        db,
        action=seeded_pool["bcl_action"],
        owner=seeded_pool["owner"],
        sequenced_pool_idx=seeded_pool["pool_idx"],
        state="failed",
    )
    body = (await ctx["wet"].get(_url(seeded_pool["run_idx"], seeded_pool["pool_idx"]))).json()
    assert body["demux_state"] == "failed"
    assert body["complete"] is True
    assert body["fully_processed"] is False


async def test_get_completion_complete_when_all_done(ctx, seeded_pool):
    """Completing the second sample's ticket flips `complete` to True."""
    db = ctx["pool"]
    # The not-submitted sample is the second one (compl-item-1); give it a
    # completed ticket.
    ps1 = await db.fetchval(
        "SELECT prep_sample_idx FROM qiita.sequenced_sample"
        " WHERE sequenced_pool_idx = $1 AND sequenced_pool_item_id = 'compl-item-1'",
        seeded_pool["pool_idx"],
    )
    await _add_ticket(
        db,
        action=seeded_pool["action"],
        owner=seeded_pool["owner"],
        prep_sample_idx=ps1,
        state="completed",
        mask_idx=seeded_pool["mask_idx"],
    )
    resp = await ctx["wet"].get(_url(seeded_pool["run_idx"], seeded_pool["pool_idx"]))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["samples_completed"] == 2
    assert body["complete"] is True


async def test_get_completion_no_data_makes_pool_complete(ctx, seeded_pool):
    """Giving the not-submitted second sample a NO_DATA (empty-well) ticket flips
    `complete` to True — completed + no_data == sample_count — and surfaces the
    sample in the samples_no_data bucket, NOT samples_failed."""
    db = ctx["pool"]
    ps1 = await db.fetchval(
        "SELECT prep_sample_idx FROM qiita.sequenced_sample"
        " WHERE sequenced_pool_idx = $1 AND sequenced_pool_item_id = 'compl-item-1'",
        seeded_pool["pool_idx"],
    )
    await _add_ticket(
        db,
        action=seeded_pool["action"],
        owner=seeded_pool["owner"],
        prep_sample_idx=ps1,
        state="no_data",
    )
    resp = await ctx["wet"].get(_url(seeded_pool["run_idx"], seeded_pool["pool_idx"]))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["samples_completed"] == 1
    assert body["samples_no_data"] == 1
    assert body["samples_failed"] == 0
    assert body["complete"] is True


async def test_get_completion_empty_pool(ctx, seeded_pool):
    """A pool with no samples reads as all-zero counts and complete=False (not
    vacuously true)."""
    db = ctx["pool"]
    owner = ctx["wet_session"]["principal_idx"]
    bs_idx, ps_idx = await seed_biosample_with_sequenced_prep_sample(db, owner_idx=owner)
    run_idx, pool_idx, ss_idx = await seed_sequenced_sample_subtype(
        db, prep_sample_idx=ps_idx, owner_idx=owner, sequenced_pool_item_id="empty-compl-1"
    )
    # Retire the only sample so the pool has zero active samples.
    await db.execute(
        "UPDATE qiita.prep_sample SET retired = true, retired_by_idx = $2, retired_at = now(),"
        " retire_reason = 'test' WHERE idx = $1",
        ps_idx,
        owner,
    )
    try:
        resp = await ctx["wet"].get(_url(run_idx, pool_idx))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["sample_count"] == 0
        assert body["samples_not_submitted"] == 0
        assert body["complete"] is False
    finally:
        await db.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
        await db.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
        await db.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
        await db.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", ps_idx)
        await db.execute("DELETE FROM qiita.biosample WHERE idx = $1", bs_idx)


async def test_get_completion_unknown_pool_404(ctx, seeded_pool):
    resp = await ctx["wet"].get(_url(seeded_pool["run_idx"], 999_999_999))
    assert resp.status_code == 404


async def test_get_completion_wrong_run_422(ctx, seeded_pool):
    resp = await ctx["wet"].get(_url(seeded_pool["run_idx"] + 10_000, seeded_pool["pool_idx"]))
    assert resp.status_code == 422


async def test_get_completion_anonymous_401(ctx, seeded_pool):
    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.get(_url(seeded_pool["run_idx"], seeded_pool["pool_idx"]))
    assert resp.status_code == 401


async def test_get_completion_missing_scope_403(seeded_pool, no_prep_sample_read_client):
    resp = await no_prep_sample_read_client.get(
        _url(seeded_pool["run_idx"], seeded_pool["pool_idx"])
    )
    assert resp.status_code == 403


async def test_get_completion_regular_user_403(ctx, seeded_pool):
    # Non-owner: the gate is `require_caller_owns_run()`, and this run was created
    # by the wet-admin principal. A plain user is refused for not owning it, not
    # for lacking a role — the twin below is the owner case.
    resp = await ctx["user"].get(_url(seeded_pool["run_idx"], seeded_pool["pool_idx"]))
    assert resp.status_code == 403


async def test_get_completion_run_creator_can_read(ctx, seeded_pool):
    """A plain user who created the RUN reads its pool's completion rollup."""
    await ctx["pool"].execute(
        "UPDATE qiita.sequencing_run SET created_by_idx = $1 WHERE idx = $2",
        ctx["user_session"]["principal_idx"],
        seeded_pool["run_idx"],
    )
    resp = await ctx["user"].get(_url(seeded_pool["run_idx"], seeded_pool["pool_idx"]))
    assert resp.status_code == 200, resp.text
