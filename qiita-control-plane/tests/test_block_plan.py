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

import json
import secrets
import types

import pytest
import pytest_asyncio
from qiita_common.host_filter_plan import PoolPlanRefusal
from qiita_common.models import (
    HOST_FILTER_INDEX_TYPE_RYPE,
    MISSING_REASON_CONTROL_SAMPLE,
    MISSING_REASON_NOT_APPLICABLE,
)

from qiita_control_plane import block_planner
from qiita_control_plane.repositories._sample_helpers import (
    _get_or_create_globally_linked_study_field,
    insert_entity_to_study,
)
from qiita_control_plane.repositories.biosample_metadata import BIOSAMPLE_METADATA_SPEC
from qiita_control_plane.repositories.sequence_range import mint_sequence_range
from qiita_control_plane.testing.db_seeds import (
    NCBI_TAXONOMY_HUMAN_TERM_ID,
    fetch_missing_value_reason_idx,
    fetch_ncbi_taxonomy_term,
    fetch_seeded_metagenome_term,
    seed_biosample_with_sequenced_prep_sample,
    seed_host_filter_profile,
    seed_host_reference,
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
                    conn,
                    prep_sample_idx=ps,
                    count=reads,
                    principal_idx=principal_idx,
                    work_ticket_idx=None,
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
        # Drop-in for the old `host_rype_reference_idx=None`: a force override that
        # disables host filtering, so tiling/gate/resubmit tests skip resolution.
        # Tests exercising REAL per-sample resolution pass `force_decision=None`.
        force_decision=block_planner.SampleHostFilter(enabled=False),
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


@pytest_asyncio.fixture
async def hf(pooled):
    """`pooled` + the host-filter resolution infra so a plan with
    `force_decision=None` exercises REAL per-sample resolution.

    Seeds a study with a globally-linked `host_taxon_id` field (the trigger-
    maintained link the resolver reads), an ACTIVE + rype-index-built host
    reference with an ILLUMINA host_filter_profile for the human term, plus a
    bare second host reference for a MULTI_HOST case. Exposes helpers to attach
    `host_taxon_id` metadata (a term or a missing-reason) to a sample's biosample.
    Teardown runs BEFORE `pooled`'s (which deletes the biosamples), removing the
    metadata / links / profiles / references it added.
    """
    pool = pooled["pool"]
    principal_idx = pooled["principal_idx"]
    suffix = secrets.token_hex(4)

    study_idx = await pool.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        principal_idx,
        f"blkhf-{suffix}",
    )
    host_gf_idx = await pool.fetchval(
        "SELECT idx FROM qiita.biosample_global_field WHERE internal_name = 'host_taxon_id'"
    )
    assert host_gf_idx is not None, "host_taxon_id global field should be seeded"
    async with pool.acquire() as conn, conn.transaction():
        field_idx, _ = await _get_or_create_globally_linked_study_field(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            study_idx=study_idx,
            global_field_idx=host_gf_idx,
            display_name="host taxon id",
            created_by_idx=principal_idx,
        )

    human_term = await fetch_ncbi_taxonomy_term(pool, NCBI_TAXONOMY_HUMAN_TERM_ID)
    human_term_idx = human_term["idx"]
    metagenome_term = await fetch_seeded_metagenome_term(pool)
    metagenome_term_idx = metagenome_term["idx"]

    async def _seed_ready_host_reference(name: str) -> int:
        """An ACTIVE host reference with a whole-reference rype index built, so
        `_assert_pool_references_ready` accepts it (a bare `seed_host_reference`
        row is `pending` + index-less and would raise `HostReferenceNotReady`)."""
        ref_idx = await seed_host_reference(pool, name=name, created_by_idx=principal_idx)
        await pool.execute(
            "UPDATE qiita.reference SET status = 'active' WHERE reference_idx = $1", ref_idx
        )
        await pool.execute(
            "INSERT INTO qiita.reference_index"
            "  (reference_idx, index_type, fs_path, params, shard_id)"
            " VALUES ($1, $2, $3, '{}'::jsonb, NULL)",
            ref_idx,
            HOST_FILTER_INDEX_TYPE_RYPE,
            f"/derived/references/{ref_idx}/rype.ryxdi",
        )
        return ref_idx

    rype_human = await _seed_ready_host_reference(f"blkhf-rype-h-{suffix}")
    # The second host's reference need not be ready: a MULTI_HOST pool refuses in
    # resolution, before `_assert_pool_references_ready` ever runs.
    rype_meta = await seed_host_reference(
        pool, name=f"blkhf-rype-m-{suffix}", created_by_idx=principal_idx
    )
    await seed_host_filter_profile(
        pool,
        host_term_idx=human_term_idx,
        platform="illumina",
        rype_reference_idx=rype_human,
        created_by_idx=principal_idx,
    )

    references = [rype_human, rype_meta]
    meta_idxs: list[int] = []
    linked: set[int] = set()

    async def _bs_for(ps: int) -> int:
        return await pool.fetchval("SELECT biosample_idx FROM qiita.prep_sample WHERE idx = $1", ps)

    async def _ensure_linked(bs: int) -> None:
        if bs in linked:
            return
        async with pool.acquire() as conn, conn.transaction():
            await insert_entity_to_study(
                conn,
                spec=BIOSAMPLE_METADATA_SPEC,
                entity_idx=bs,
                study_idx=study_idx,
                created_by_idx=principal_idx,
            )
        linked.add(bs)

    async def set_host_term(ps: int, term_idx: int) -> None:
        bs = await _bs_for(ps)
        await _ensure_linked(bs)
        idx = await pool.fetchval(
            "INSERT INTO qiita.biosample_metadata (biosample_idx,"
            " biosample_study_field_idx, value_terminology_term_idx, created_by_idx)"
            " VALUES ($1, $2, $3, $4) RETURNING idx",
            bs,
            field_idx,
            term_idx,
            principal_idx,
        )
        meta_idxs.append(idx)

    async def set_host_missing(ps: int, reason_name: str) -> None:
        bs = await _bs_for(ps)
        await _ensure_linked(bs)
        reason_idx = await fetch_missing_value_reason_idx(pool, reason_name)
        assert reason_idx is not None, f"missing_value_reason {reason_name!r} should be seeded"
        idx = await pool.fetchval(
            "INSERT INTO qiita.biosample_metadata"
            " (biosample_idx, biosample_study_field_idx, value_missing_reason_idx, created_by_idx)"
            " VALUES ($1, $2, $3, $4) RETURNING idx",
            bs,
            field_idx,
            reason_idx,
            principal_idx,
        )
        meta_idxs.append(idx)

    async def seed_second_host_profile() -> None:
        await seed_host_filter_profile(
            pool,
            host_term_idx=metagenome_term_idx,
            platform="illumina",
            rype_reference_idx=rype_meta,
            created_by_idx=principal_idx,
        )

    yield {
        **pooled,
        "study_idx": study_idx,
        "human_term_idx": human_term_idx,
        "metagenome_term_idx": metagenome_term_idx,
        "rype_human": rype_human,
        "rype_meta": rype_meta,
        "set_host_term": set_host_term,
        "set_host_missing": set_host_missing,
        "seed_second_host_profile": seed_second_host_profile,
    }

    # Teardown (runs before `pooled` deletes the biosamples): FK-reverse.
    await pool.execute(
        "DELETE FROM qiita.biosample_metadata WHERE idx = ANY($1::bigint[])", meta_idxs
    )
    await pool.execute("DELETE FROM qiita.biosample_to_study WHERE study_idx = $1", study_idx)
    await pool.execute(
        "DELETE FROM qiita.host_filter_profile WHERE created_by_idx = $1", principal_idx
    )
    await pool.execute("DELETE FROM qiita.biosample_study_field WHERE idx = $1", field_idx)
    await pool.execute(
        "DELETE FROM qiita.reference_index WHERE reference_idx = ANY($1::bigint[])", references
    )
    await pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = ANY($1::bigint[])", references
    )
    await pool.execute("DELETE FROM qiita.study WHERE idx = $1", study_idx)


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


async def test_plan_host_filter_context_and_mask(hf, planapp):
    """With a forced rype host reference the per-partition summary + the ticket
    action_context record host filtering, and the mask identity differs from a
    pass-through of the same pool. host_filter_enabled is now PER PARTITION (the
    removed top-level summary field), and the forced reference must be ACTIVE +
    index-built (`_assert_pool_references_ready` runs even under force)."""
    await hf["add_sample"](reads=50)
    summary_pass, _ = await _plan(hf, planapp)
    pass_mask = summary_pass["partitions"][0]["mask_idx"]

    # Re-plan the SAME sample with a forced host reference → distinct mask identity,
    # and the ticket action_context carries the host ref + enabled flag.
    rype = hf["rype_human"]
    summary_hf, _ = await _plan(
        hf,
        planapp,
        force_decision=block_planner.SampleHostFilter(enabled=True, rype_reference_idx=rype),
        only_missing=False,
    )
    hf_mask = summary_hf["partitions"][0]["mask_idx"]
    assert hf_mask != pass_mask
    assert summary_hf["partitions"][0]["host_filter_enabled"] is True
    assert summary_hf["partitions"][0]["host_rype_reference_idx"] == rype

    pool = hf["pool"]
    ctx = await pool.fetchval(
        "SELECT action_context FROM qiita.work_ticket WHERE mask_idx = $1 LIMIT 1", hf_mask
    )
    ctx = json.loads(ctx)
    assert ctx["host_filter_enabled"] is True
    assert ctx["host_rype_reference_idx"] == rype
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


# ---------------------------------------------------------------------------
# per-sample host-filter resolution (force_decision=None) — the point of this change
# ---------------------------------------------------------------------------


async def test_plan_resolves_single_host_sample_from_metadata(hf, planapp):
    """A single human-host sample with an ILLUMINA profile resolves to FILTER: its
    partition enables host filtering against the profile's rype ref, and the block
    ticket action_context carries that ref."""
    pool = hf["pool"]
    ps = await hf["add_sample"](reads=50)
    await hf["set_host_term"](ps, hf["human_term_idx"])

    summary, _ = await _plan(hf, planapp, force_decision=None)

    assert len(summary["partitions"]) == 1
    part = summary["partitions"][0]
    assert part["host_filter_enabled"] is True
    assert part["host_rype_reference_idx"] == hf["rype_human"]
    assert part["host_minimap2_reference_idx"] is None

    ctx = json.loads(
        await pool.fetchval(
            "SELECT action_context FROM qiita.work_ticket WHERE mask_idx = $1 LIMIT 1",
            part["mask_idx"],
        )
    )
    assert ctx["host_filter_enabled"] is True
    assert ctx["host_rype_reference_idx"] == hf["rype_human"]


async def test_plan_heterogeneous_pool_partitions_by_decision(hf, planapp):
    """A human-host sample (FILTER) + a not_applicable sample (PASS_THROUGH) share
    a prep_protocol but resolve to DIFFERENT decisions → two partitions with
    different mask_idx: the FILTER partition carries host refs, the PASS_THROUGH
    partition has host filtering disabled."""
    ps_human = await hf["add_sample"](reads=50)
    ps_na = await hf["add_sample"](reads=50)
    await hf["set_host_term"](ps_human, hf["human_term_idx"])
    await hf["set_host_missing"](ps_na, MISSING_REASON_NOT_APPLICABLE)

    summary, _ = await _plan(hf, planapp, force_decision=None)

    assert len(summary["partitions"]) == 2
    by_enabled = {p["host_filter_enabled"]: p for p in summary["partitions"]}
    assert set(by_enabled) == {True, False}
    assert by_enabled[True]["mask_idx"] != by_enabled[False]["mask_idx"]
    assert by_enabled[True]["host_rype_reference_idx"] == hf["rype_human"]
    assert by_enabled[False]["host_rype_reference_idx"] is None


async def test_plan_blank_inherits_pool_host(hf, planapp):
    """A blank (control_sample missing-reason) rides in a human-host pool: it
    inherits the pool's sole host, so its decision equals the host sample's and the
    two share a partition / mask (one gate covering both)."""
    pool = hf["pool"]
    ps_human = await hf["add_sample"](reads=50)
    ps_blank = await hf["add_sample"](reads=50)
    await hf["set_host_term"](ps_human, hf["human_term_idx"])
    await hf["set_host_missing"](ps_blank, MISSING_REASON_CONTROL_SAMPLE)

    summary, _ = await _plan(hf, planapp, force_decision=None)

    assert len(summary["partitions"]) == 1
    part = summary["partitions"][0]
    assert part["host_filter_enabled"] is True
    assert part["host_rype_reference_idx"] == hf["rype_human"]
    assert part["sample_count"] == 2
    gate = await pool.fetch(
        "SELECT prep_sample_idx FROM qiita.mask_sample WHERE mask_idx = $1", part["mask_idx"]
    )
    assert {g["prep_sample_idx"] for g in gate} == {ps_human, ps_blank}


async def test_plan_unresolved_sample_refuses(hf, planapp):
    """A sample with no host_taxon_id at all resolves UNRESOLVED, so the whole plan
    is refused with PoolHostFilterRefusal(UNRESOLVED_SAMPLES) before any mask is
    minted (fail-closed: we will not mask a sample against the wrong thing)."""
    ps = await hf["add_sample"](reads=50)  # no host_taxon_id metadata

    with pytest.raises(block_planner.PoolHostFilterRefusal) as ei:
        await _plan(hf, planapp, force_decision=None)
    assert ei.value.refusal is PoolPlanRefusal.UNRESOLVED_SAMPLES
    assert ps in ei.value.offending


async def test_plan_multi_host_refuses(hf, planapp):
    """Two samples with two DIFFERENT host terms (each with its own ILLUMINA
    profile) make the pool span more than one host → the blanks have no single
    answer, so the plan is refused with PoolHostFilterRefusal(MULTI_HOST)."""
    await hf["seed_second_host_profile"]()
    ps_human = await hf["add_sample"](reads=50)
    ps_meta = await hf["add_sample"](reads=50)
    await hf["set_host_term"](ps_human, hf["human_term_idx"])
    await hf["set_host_term"](ps_meta, hf["metagenome_term_idx"])

    with pytest.raises(block_planner.PoolHostFilterRefusal) as ei:
        await _plan(hf, planapp, force_decision=None)
    assert ei.value.refusal is PoolPlanRefusal.MULTI_HOST


async def test_plan_force_bypasses_resolution(hf, planapp):
    """A forced decision applies pool-wide, bypassing resolution: a sample with NO
    host_taxon_id (which would be UNRESOLVED under real resolution) plans cleanly
    into one host-filtered partition when forced against a ready reference."""
    await hf["add_sample"](reads=50)  # no host_taxon_id metadata

    summary, _ = await _plan(
        hf,
        planapp,
        force_decision=block_planner.SampleHostFilter(
            enabled=True, rype_reference_idx=hf["rype_human"]
        ),
    )
    assert len(summary["partitions"]) == 1
    assert summary["partitions"][0]["host_filter_enabled"] is True
    assert summary["partitions"][0]["host_rype_reference_idx"] == hf["rype_human"]


async def test_plan_forced_reference_not_ready_raises(hf, planapp):
    """`_assert_pool_references_ready` runs even under force: a forced decision
    pointing at an ACTIVE but index-less reference raises HostReferenceNotReady
    (one actionable error up front, not N failed blocks)."""
    pool = hf["pool"]
    await hf["add_sample"](reads=50)
    # An ACTIVE reference with no rype index built → ReferenceIndexNotBuilt inside
    # _assert_pool_references_ready → HostReferenceNotReady.
    bare_ref = await seed_host_reference(
        pool, name=f"blkhf-bare-{secrets.token_hex(4)}", created_by_idx=hf["principal_idx"]
    )
    await pool.execute(
        "UPDATE qiita.reference SET status = 'active' WHERE reference_idx = $1", bare_ref
    )
    try:
        with pytest.raises(block_planner.HostReferenceNotReady):
            await _plan(
                hf,
                planapp,
                force_decision=block_planner.SampleHostFilter(
                    enabled=True, rype_reference_idx=bare_ref
                ),
            )
    finally:
        await pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", bare_ref)
