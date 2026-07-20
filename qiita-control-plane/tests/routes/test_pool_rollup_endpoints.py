"""Route tests for the two new pool rollup endpoints:

- GET .../sequenced-pool/{P}/sequenced-sample/exceptions — the anomalous-sample
  drill-down (no usable reads / missing accession / failed ticket).
- GET .../sequenced-pool/{P}/work-ticket/summary — read-mask ticket coverage +
  per-state ticket counts, reconciled with the completion rollup.

One fixture seeds a run + pool with three samples: a clean one (never an
exception), a zero-reads-and-missing-accession one, and an unprocessed +
no-accessions + failed-ticket one. FK-reverse cleanup.
"""

import json
import uuid

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import (
    URL_SEQUENCED_POOL_WORK_TICKET_SUMMARY,
    URL_SEQUENCED_SAMPLE_EXCEPTIONS,
)

from qiita_control_plane.main import app
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
)

pytestmark = pytest.mark.db


@pytest.fixture
def ctx(role_keyed_clients):
    return role_keyed_clients


async def _seed_read_mask_action(db):
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


async def _add_ticket(db, *, action, owner, prep_sample_idx, state):
    action_id, version = action
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
async def seeded(ctx):
    db = ctx["pool"]
    owner = ctx["wet_session"]["principal_idx"]
    action = await _seed_read_mask_action(db)
    created = []

    # Sample A — clean: reads survive, all four accessions, a COMPLETED ticket.
    bs_a, ps_a = await seed_biosample_with_sequenced_prep_sample(db, owner_idx=owner)
    run_idx, pool_idx, ss_a = await seed_sequenced_sample_subtype(
        db, prep_sample_idx=ps_a, owner_idx=owner, sequenced_pool_item_id="roll-a"
    )
    await db.execute(
        "UPDATE qiita.biosample SET biosample_accession='SAMEA-A', ena_sample_accession='ERS-A'"
        " WHERE idx=$1",
        bs_a,
    )
    await db.execute(
        "UPDATE qiita.sequenced_sample SET raw_read_count_r1r2=1000,"
        " quality_filtered_read_count_r1r2=900, ena_experiment_accession='ERX-A',"
        " ena_run_accession='ERR-A' WHERE idx=$1",
        ss_a,
    )
    await _add_ticket(db, action=action, owner=owner, prep_sample_idx=ps_a, state="completed")
    created.append((bs_a, ps_a, ss_a))

    async def _add_pool_sample(item_id):
        bs, ps = await seed_biosample_with_sequenced_prep_sample(db, owner_idx=owner)
        ss = await db.fetchval(
            "INSERT INTO qiita.sequenced_sample"
            "  (prep_sample_idx, sequenced_pool_idx, sequenced_pool_item_id, created_by_idx)"
            " VALUES ($1, $2, $3, $4) RETURNING idx",
            ps,
            pool_idx,
            item_id,
            owner,
        )
        created.append((bs, ps, ss))
        return bs, ps, ss

    # Sample B — processed but zero survived, and missing ENA run accession
    # (has biosample + ena-sample + ena-experiment). No ticket → not_submitted.
    bs_b, ps_b, ss_b = await _add_pool_sample("roll-b")
    await db.execute(
        "UPDATE qiita.biosample SET biosample_accession='SAMEA-B', ena_sample_accession='ERS-B'"
        " WHERE idx=$1",
        bs_b,
    )
    await db.execute(
        "UPDATE qiita.sequenced_sample SET raw_read_count_r1r2=500,"
        " quality_filtered_read_count_r1r2=0, ena_experiment_accession='ERX-B' WHERE idx=$1",
        ss_b,
    )

    # Sample C — unprocessed, no accessions at all, and a FAILED ticket (no completed).
    bs_c, ps_c, ss_c = await _add_pool_sample("roll-c")
    await _add_ticket(db, action=action, owner=owner, prep_sample_idx=ps_c, state="failed")

    yield {
        "run_idx": run_idx,
        "pool_idx": pool_idx,
        "ss": {"a": ss_a, "b": ss_b, "c": ss_c},
        "ps": {"a": ps_a, "b": ps_b, "c": ps_c},
    }

    await db.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id=$1 AND action_version=$2", *action
    )
    for _bs, _ps, ss_idx in created:
        await db.execute("DELETE FROM qiita.sequenced_sample WHERE idx=$1", ss_idx)
    await db.execute("DELETE FROM qiita.sequenced_pool WHERE idx=$1", pool_idx)
    await db.execute("DELETE FROM qiita.sequencing_run WHERE idx=$1", run_idx)
    await db.execute("DELETE FROM qiita.action WHERE action_id=$1 AND version=$2", *action)
    for _bs, ps_idx, _ss in created:
        await db.execute("DELETE FROM qiita.prep_sample WHERE idx=$1", ps_idx)
    for bs_idx, _ps, _ss in created:
        await db.execute("DELETE FROM qiita.biosample WHERE idx=$1", bs_idx)


def _exc_url(run_idx, pool_idx):
    return URL_SEQUENCED_SAMPLE_EXCEPTIONS.format(
        sequencing_run_idx=run_idx, sequenced_pool_idx=pool_idx
    )


def _wt_url(run_idx, pool_idx):
    return URL_SEQUENCED_POOL_WORK_TICKET_SUMMARY.format(
        sequencing_run_idx=run_idx, sequenced_pool_idx=pool_idx
    )


# --------------------------------------------------------------------------- #
# exceptions drill-down
# --------------------------------------------------------------------------- #


async def test_exceptions_lists_only_anomalous_with_flags(ctx, seeded):
    resp = await ctx["wet"].get(_exc_url(seeded["run_idx"], seeded["pool_idx"]))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["sequenced_pool_idx"] == seeded["pool_idx"]
    # A is clean and excluded; B and C are exceptions.
    assert body["count"] == 2
    by_idx = {s["sequenced_sample_idx"]: s for s in body["samples"]}
    assert seeded["ss"]["a"] not in by_idx
    b = by_idx[seeded["ss"]["b"]]
    assert b["flags"] == ["no_reads", "missing_ena_run_accession"]
    assert b["quality_filtered_read_count_r1r2"] == 0
    c = by_idx[seeded["ss"]["c"]]
    assert c["flags"] == [
        "unprocessed",
        "missing_biosample_accession",
        "missing_ena_sample_accession",
        "missing_ena_experiment_accession",
        "missing_ena_run_accession",
        "failed_ticket",
    ]
    assert c["quality_filtered_read_count_r1r2"] is None


async def test_exceptions_missing_pool_404(ctx, seeded):
    resp = await ctx["wet"].get(_exc_url(seeded["run_idx"], 999_999_999))
    assert resp.status_code == 404


async def test_exceptions_anonymous_401(ctx, seeded):
    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.get(_exc_url(seeded["run_idx"], seeded["pool_idx"]))
    assert resp.status_code == 401


async def test_exceptions_regular_user_403(ctx, seeded):
    resp = await ctx["user"].get(_exc_url(seeded["run_idx"], seeded["pool_idx"]))
    assert resp.status_code == 403


# --------------------------------------------------------------------------- #
# work-ticket summary
# --------------------------------------------------------------------------- #


async def test_work_ticket_summary_coverage_and_state_counts(ctx, seeded):
    resp = await ctx["wet"].get(_wt_url(seeded["run_idx"], seeded["pool_idx"]))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["sequenced_pool_idx"] == seeded["pool_idx"]
    assert body["sample_count"] == 3
    # A (completed) and C (failed) have a read-mask ticket; B has none.
    assert body["read_mask"]["samples_with_read_mask_ticket"] == 2
    assert body["read_mask"]["samples_without_read_mask_ticket"] == 1
    # Coverage reconciles with the completion rollup by construction.
    assert (
        body["read_mask"]["samples_with_read_mask_ticket"]
        + body["read_mask"]["samples_without_read_mask_ticket"]
        == body["sample_count"]
    )
    counts = body["ticket_state_counts"]
    assert counts["completed"] == 1
    assert counts["failed"] == 1
    # Every state is present, zero-filled.
    for state in ("pending", "queued", "processing", "no_data"):
        assert counts[state] == 0


async def test_work_ticket_summary_regular_user_403(ctx, seeded):
    resp = await ctx["user"].get(_wt_url(seeded["run_idx"], seeded["pool_idx"]))
    assert resp.status_code == 403
