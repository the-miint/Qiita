"""DB-tier tests for the server-side block planner (plan_and_submit_blocks).

Seeds one sequencing_run + sequenced_pool + several sequenced prep_samples with
minted sequence_ranges, then plans against a small target_reads so a pool tiles
into several blocks. Asserts the planner persists the block / block_member
cover-map, a PENDING mask_sample gate per sample, one block-scoped work_ticket
per block (with the partition's mask_idx + host/instrument action_context, and
block.work_ticket_idx back-filled), and dispatches each ticket exactly once.

schedule_dispatch is monkeypatched to a recorder so no real orchestrator work is
fired. adapter_set_hash is passed in fixed (no data plane) — the route owns the
one-time adapter DoGet.
"""

import secrets
import types

import pytest
import pytest_asyncio

from qiita_control_plane import block_planner
from qiita_control_plane.repositories.sequence_range import mint_sequence_range
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)

pytestmark = pytest.mark.db

_BLOCK_ACTION_ID = "read-mask-block"
_BLOCK_ACTION_VERSION = "1.0.0"


@pytest_asyncio.fixture
async def planapp(monkeypatch):
    """A minimal app stand-in + a dispatch recorder. plan_and_submit_blocks only
    passes `app` to schedule_dispatch, which we replace with a recorder, so the
    app object never needs real wiring."""
    dispatched: list[int] = []

    def _record(app, work_ticket_idx, **kwargs):
        dispatched.append(work_ticket_idx)

    monkeypatch.setattr(block_planner, "schedule_dispatch", _record)
    app = types.SimpleNamespace(state=types.SimpleNamespace(compute_backend_client=object()))
    return app, dispatched


@pytest_asyncio.fixture
async def pooled(postgres_pool):
    """Seed a run + pool + block action; yield helpers to add samples. FK-reverse
    cleanup keyed on the tracked ids (parallel-safe — no global sweeps)."""
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="plan-test", suffix=suffix)
    run_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequencing_run"
        "  (instrument_run_id, platform, instrument_model, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, 'NovaSeq 6000', $2) RETURNING idx",
        f"plan-run-{suffix}",
        principal_idx,
    )
    pool_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequenced_pool (sequencing_run_idx, created_by_idx)"
        " VALUES ($1, $2) RETURNING idx",
        run_idx,
        principal_idx,
    )
    await postgres_pool.execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status)"
        " VALUES ($1, $2, 'block', '{}'::text[], $3::jsonb, '{}'::jsonb, '[]'::jsonb,"
        "         1, 1, '1 minute', 'active', 'failed')",
        _BLOCK_ACTION_ID,
        _BLOCK_ACTION_VERSION,
        '{"service": false, "human_roles": ["wet_lab_admin", "system_admin"]}',
    )

    prep_samples: list[int] = []
    biosamples: list[int] = []

    async def add_sample(*, reads: int, protocol_name="short_read_metagenomics") -> int:
        """Seed one biosample + sequenced prep_sample in this pool with `reads`
        reads (a minted sequence_range). Returns the prep_sample_idx."""
        bs, ps = await seed_biosample_with_sequenced_prep_sample(
            postgres_pool, owner_idx=principal_idx, protocol_name=protocol_name
        )
        biosamples.append(bs)
        prep_samples.append(ps)
        await postgres_pool.execute(
            "INSERT INTO qiita.sequenced_sample"
            "  (prep_sample_idx, sequenced_pool_idx, sequenced_pool_item_id, created_by_idx)"
            " VALUES ($1, $2, $3, $4)",
            ps,
            pool_idx,
            f"item-{ps}-{secrets.token_hex(3)}",
            principal_idx,
        )
        if reads > 0:
            async with postgres_pool.acquire() as conn, conn.transaction():
                await mint_sequence_range(
                    conn, prep_sample_idx=ps, count=reads, principal_idx=principal_idx
                )
        return ps

    yield {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "run_idx": run_idx,
        "pool_idx": pool_idx,
        "add_sample": add_sample,
        "prep_samples": prep_samples,
    }

    # FK-reverse cleanup keyed on the seeded prep_samples / ids.
    ps_arr = prep_samples
    # Block tickets first (cascades their blocks + members via block.work_ticket_idx).
    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE block_idx IN"
        " (SELECT bm.block_idx FROM qiita.block_member bm"
        "   WHERE bm.prep_sample_idx = ANY($1::bigint[]))",
        ps_arr,
    )
    # Any ticketless blocks left (planner failed before ticket create, etc.).
    await postgres_pool.execute(
        "DELETE FROM qiita.block WHERE block_idx IN"
        " (SELECT block_idx FROM qiita.block_member WHERE prep_sample_idx = ANY($1::bigint[]))",
        ps_arr,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.block_member WHERE prep_sample_idx = ANY($1::bigint[])", ps_arr
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.mask_sample WHERE prep_sample_idx = ANY($1::bigint[])", ps_arr
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.sequence_range WHERE prep_sample_idx = ANY($1::bigint[])", ps_arr
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.sequenced_sample WHERE prep_sample_idx = ANY($1::bigint[])", ps_arr
    )
    await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", ps_arr
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", biosamples
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
        _BLOCK_ACTION_ID,
        _BLOCK_ACTION_VERSION,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.mask_definition WHERE created_by_idx = $1", principal_idx
    )
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


async def _plan(pooled, planapp, **overrides):
    app, dispatched = planapp
    kwargs = dict(
        app=app,
        sequencing_run_idx=pooled["run_idx"],
        sequenced_pool_idx=pooled["pool_idx"],
        host_rype_reference_idx=None,
        host_minimap2_reference_idx=None,
        only_missing=False,
        adapter_set_hash=None,
        originator_principal_idx=pooled["principal_idx"],
        block_action_id=_BLOCK_ACTION_ID,
        block_action_version=_BLOCK_ACTION_VERSION,
        target_reads=100,
    )
    kwargs.update(overrides)
    summary = await block_planner.plan_and_submit_blocks(pooled["pool"], **kwargs)
    return summary, dispatched


# ---------------------------------------------------------------------------
# happy path: blocks + cover-map + gate + tickets
# ---------------------------------------------------------------------------


async def test_plan_creates_blocks_cover_map_gate_and_tickets(pooled, planapp):
    pool = pooled["pool"]
    # Two samples of 150 reads each in one partition (same protocol) → 300 reads,
    # target 100 → 3 blocks (100, 100, 100). Sample 2 straddles a boundary.
    ps1 = await pooled["add_sample"](reads=150)
    ps2 = await pooled["add_sample"](reads=150)

    summary, dispatched = await _plan(pooled, planapp)

    assert summary["blocks_created"] == 3
    assert summary["samples_planned"] == 2
    assert len(summary["partitions"]) == 1
    mask_idx = summary["partitions"][0]["mask_idx"]

    # One block-scoped work_ticket per block, back-filled, mask_idx set.
    tickets = await pool.fetch(
        "SELECT wt.work_ticket_idx, wt.scope_target_kind, wt.block_idx, wt.mask_idx, wt.state,"
        "       b.work_ticket_idx AS block_backfill"
        "  FROM qiita.work_ticket wt JOIN qiita.block b ON b.block_idx = wt.block_idx"
        " WHERE wt.block_idx = ANY($1::bigint[])",
        [b["block_idx"] for b in summary["blocks"]],
    )
    assert len(tickets) == 3
    for t in tickets:
        assert t["scope_target_kind"] == "block"
        assert t["mask_idx"] == mask_idx
        assert t["state"] == "pending"
        assert t["block_backfill"] == t["work_ticket_idx"]  # back-fill closed the cycle

    # block_member cover-map: the union of each sample's sub-ranges covers its
    # full minted range exactly (300 member-reads total across the 3 blocks).
    total_reads = await pool.fetchval(
        "SELECT COALESCE(SUM(max_sequence_idx - min_sequence_idx + 1), 0)"
        "  FROM qiita.block_member WHERE prep_sample_idx = ANY($1::bigint[])",
        [ps1, ps2],
    )
    assert total_reads == 300

    # PENDING mask_sample gate per sample.
    gate = await pool.fetch(
        "SELECT prep_sample_idx, state FROM qiita.mask_sample WHERE mask_idx = $1", mask_idx
    )
    assert {(g["prep_sample_idx"], g["state"]) for g in gate} == {
        (ps1, "pending"),
        (ps2, "pending"),
    }

    # Each block ticket dispatched exactly once.
    assert sorted(dispatched) == sorted(t["work_ticket_idx"] for t in tickets)


async def test_plan_host_filter_context_and_mask(pooled, planapp):
    """With a rype host reference the action_context records host filtering and
    the mask identity differs from a pass-through of the same pool."""
    await pooled["add_sample"](reads=50)
    summary_pass, _ = await _plan(pooled, planapp)
    pass_mask = summary_pass["partitions"][0]["mask_idx"]

    # Re-plan the SAME sample with a host reference → distinct mask identity, and
    # the ticket action_context carries the host ref + enabled flag.
    summary_hf, _ = await _plan(pooled, planapp, host_rype_reference_idx=42, only_missing=False)
    hf_mask = summary_hf["partitions"][0]["mask_idx"]
    assert hf_mask != pass_mask
    assert summary_hf["host_filter_enabled"] is True

    pool = pooled["pool"]
    ctx = await pool.fetchval(
        "SELECT action_context FROM qiita.work_ticket WHERE mask_idx = $1 LIMIT 1", hf_mask
    )
    import json

    ctx = json.loads(ctx)
    assert ctx["host_filter_enabled"] is True
    assert ctx["host_rype_reference_idx"] == 42
    assert ctx["instrument_model"] == "NovaSeq 6000"


# ---------------------------------------------------------------------------
# partition by prep_protocol (mask_idx)
# ---------------------------------------------------------------------------


async def test_plan_partitions_by_prep_protocol(pooled, planapp):
    await pooled["add_sample"](reads=50, protocol_name="short_read_metagenomics")
    await pooled["add_sample"](reads=50, protocol_name="long_read_metagenomics")

    summary, _ = await _plan(pooled, planapp)

    # Two distinct prep_protocols → two mask partitions → two masks, one block each.
    assert len(summary["partitions"]) == 2
    mask_idxs = {p["mask_idx"] for p in summary["partitions"]}
    assert len(mask_idxs) == 2
    assert summary["blocks_created"] == 2


# ---------------------------------------------------------------------------
# only_missing + no-reads reporting
# ---------------------------------------------------------------------------


async def test_plan_only_missing_skips_already_gated(pooled, planapp):
    await pooled["add_sample"](reads=50)
    first, dispatched = await _plan(pooled, planapp)
    assert first["samples_planned"] == 1
    # The recorder is shared across _plan calls (one planapp fixture), so capture
    # the count after the first plan to assert the second dispatches nothing new.
    dispatched_after_first = len(dispatched)

    second, dispatched2 = await _plan(pooled, planapp, only_missing=True)
    assert second["samples_planned"] == 0
    assert second["samples_skipped_existing"] == 1
    assert second["blocks_created"] == 0
    # only_missing planned 0 blocks → no stray dispatch fired.
    assert len(dispatched2) == dispatched_after_first


async def test_plan_disallow_resubmit_over_completed_mask(pooled, planapp):
    """A sample already COMPLETED for its resolved mask cannot be re-planned
    without a DELETE (mirrors the pool COMPLETED-resubmit rule): re-masking would
    double-write its read_mask. only_missing=False raises BlockMaskResubmitError;
    only_missing=True skips it (the resume path)."""
    pool = pooled["pool"]
    ps = await pooled["add_sample"](reads=50)
    first, _ = await _plan(pooled, planapp)
    mask_idx = first["partitions"][0]["mask_idx"]
    # Simulate reconcile marking the sample's gate COMPLETED.
    await pool.execute(
        "UPDATE qiita.mask_sample SET state = 'completed'"
        " WHERE mask_idx = $1 AND prep_sample_idx = $2",
        mask_idx,
        ps,
    )

    # Re-plan (only_missing=False) → refused, naming the completed sample.
    with pytest.raises(block_planner.BlockMaskResubmitError) as ei:
        await _plan(pooled, planapp)
    assert ps in ei.value.conflicting_prep_sample_idxs

    # only_missing=True → the completed sample is skipped, no error, nothing new.
    second, _ = await _plan(pooled, planapp, only_missing=True)
    assert second["samples_planned"] == 0
    assert second["samples_skipped_existing"] == 1
    assert second["blocks_created"] == 0


async def test_plan_disallow_resubmit_over_pending_mask(pooled, planapp):
    """A sample still PENDING for its resolved mask also cannot be re-planned on a
    fresh (only_missing=False) plan: a prior plan's covering block is in-flight or
    failed, and minting a fresh same-footprint block would wedge the sample's
    finalize forever (has_incomplete_covering_block would keep seeing the stale
    block). only_missing=False raises; only_missing=True resumes only the gap."""
    ps = await pooled["add_sample"](reads=50)
    # First plan leaves the sample's mask_sample gate PENDING (no reconcile).
    await _plan(pooled, planapp)

    # Re-plan (only_missing=False) → refused, naming the still-pending sample, so
    # no duplicate covering block is minted.
    with pytest.raises(block_planner.BlockMaskResubmitError) as ei:
        await _plan(pooled, planapp)
    assert ps in ei.value.conflicting_prep_sample_idxs

    # only_missing=True → the pending sample is skipped, no error, nothing new.
    second, _ = await _plan(pooled, planapp, only_missing=True)
    assert second["samples_planned"] == 0
    assert second["samples_skipped_existing"] == 1
    assert second["blocks_created"] == 0


async def test_plan_reports_no_reads_samples(pooled, planapp):
    await pooled["add_sample"](reads=50)  # has reads
    await pooled["add_sample"](reads=0)  # no sequence_range → un-tileable

    summary, _ = await _plan(pooled, planapp)
    assert summary["samples_planned"] == 1
    assert summary["samples_skipped_no_reads"] == 1
    assert summary["blocks_created"] == 1


async def test_plan_excludes_retired_samples(pooled, planapp):
    """A retired prep_sample is not planned at all — no block_member, no gate — and
    is NOT counted as skipped-no-reads (it is excluded entirely, matching the
    per-sample roster + pool-status endpoints, not re-masked)."""
    pool = pooled["pool"]
    await pooled["add_sample"](reads=50)  # active
    retired_ps = await pooled["add_sample"](reads=50)
    await pool.execute(
        "UPDATE qiita.prep_sample SET retired = true, retired_at = now(),"
        "  retired_by_idx = $2, retire_reason = 'test' WHERE idx = $1",
        retired_ps,
        pooled["principal_idx"],
    )

    summary, _ = await _plan(pooled, planapp)
    assert summary["samples_planned"] == 1  # only the active sample
    assert summary["samples_skipped_no_reads"] == 0  # retired ≠ no-reads
    assert summary["blocks_created"] == 1

    no_gate = await pool.fetchval(
        "SELECT count(*) FROM qiita.mask_sample WHERE prep_sample_idx = $1", retired_ps
    )
    assert no_gate == 0
    no_member = await pool.fetchval(
        "SELECT count(*) FROM qiita.block_member WHERE prep_sample_idx = $1", retired_ps
    )
    assert no_member == 0
