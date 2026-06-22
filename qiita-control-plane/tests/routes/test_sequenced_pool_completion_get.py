"""Route tests for GET /sequencing-run/{run}/sequenced-pool/{pool}/completion —
the pool's prep-generation completion rollup.

Covers the happy path (per-sample fastq-to-parquet tickets bucketed + the
`complete` flag), the all-completed pool, the empty pool, and the read gate
(404 missing pool, 422 pool-not-in-run, 401 anonymous, 403 missing scope /
regular user). Bucket precedence is exercised at the repo layer in
test_sequenced_pool_completion; here the wiring, response model, and auth.
"""

import json
import uuid

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
    """Insert a fastq-to-parquet action so work_ticket FK resolves; return its
    (action_id, version). The completion query matches on the bare action_id."""
    action_id = "fastq-to-parquet"
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


async def _add_ticket(db, *, action, owner, prep_sample_idx, state):
    action_id, version = action
    # work_ticket_failure_consistent requires the failure columns set together on
    # a FAILED ticket and all-NULL otherwise.
    failed = state == "failed"
    await db.execute(
        "INSERT INTO qiita.work_ticket"
        "  (action_id, action_version, originator_principal_idx,"
        "   scope_target_kind, prep_sample_idx, state,"
        "   failure_type, failure_stage, failure_reason)"
        " VALUES ($1, $2, $3, 'prep_sample'::qiita.scope_target_kind, $4,"
        "         $5::qiita.work_ticket_state,"
        "         $6::qiita.failure_type, $7::qiita.work_ticket_failure_stage, $8)",
        action_id,
        version,
        owner,
        prep_sample_idx,
        state,
        "permanent" if failed else None,
        "finalize" if failed else None,
        "test failure" if failed else None,
    )


@pytest_asyncio.fixture
async def seeded_pool(ctx):
    """Seed a run + pool with two samples: one with a COMPLETED fastq-to-parquet
    ticket, one with none (not-submitted). FK-reverse cleanup."""
    db = ctx["pool"]
    owner = ctx["wet_session"]["principal_idx"]
    action = await _seed_fastq_action(db)
    created = []

    bs0, ps0 = await seed_biosample_with_sequenced_prep_sample(db, owner_idx=owner)
    run_idx, pool_idx, ss0 = await seed_sequenced_sample_subtype(
        db, prep_sample_idx=ps0, owner_idx=owner, sequenced_pool_item_id="compl-item-0"
    )
    await _add_ticket(db, action=action, owner=owner, prep_sample_idx=ps0, state="completed")
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

    yield {"run_idx": run_idx, "pool_idx": pool_idx, "owner": owner, "action": action}

    await db.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2", *action
    )
    for _bs, _ps, ss_idx in created:
        await db.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
    await db.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await db.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await db.execute("DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", *action)
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
    assert body["samples_failed"] == 0
    assert body["samples_not_submitted"] == 1
    # Not every sample completed → not complete.
    assert body["complete"] is False


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
    )
    resp = await ctx["wet"].get(_url(seeded_pool["run_idx"], seeded_pool["pool_idx"]))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["samples_completed"] == 2
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
    resp = await ctx["user"].get(_url(seeded_pool["run_idx"], seeded_pool["pool_idx"]))
    assert resp.status_code == 403
