"""DB tests for fetch_sequenced_pool_completion — the compute-on-read pool
prep-generation completion rollup.

The repo function classifies each non-retired sequenced_sample by the state of
its read-mask work tickets (any version) and tallies the five
mutually-exclusive buckets (completed / in-flight / no-data / failed /
not-submitted). Each
test seeds one principal + one run + one pool + a read-mask action, then
attaches samples via `add_sample` and tickets via `add_ticket`; cleanup is
FK-reverse on the shared postgres_pool fixture.
"""

import json
import secrets
import uuid

import pytest
import pytest_asyncio

from qiita_control_plane.repositories.sequencing_run import (
    fetch_sequenced_pool_completion,
    fetch_sequenced_pool_demux_state,
)
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def pool_ctx(postgres_pool):
    """Seed a principal + run + pool + a fastq-to-parquet action; yield a context
    with `add_sample()` (attach a sequenced_sample, optionally retired) and
    `add_ticket(prep_sample_idx, state)` (attach a fastq-to-parquet work ticket).
    FK-reverse cleanup."""
    owner_idx = await seed_user_principal(postgres_pool, prefix="poolcompl", suffix="owner")
    run_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequencing_run (instrument_run_id, platform, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, $2) RETURNING idx",
        f"pc-run-{secrets.token_hex(4)}",
        owner_idx,
    )
    pool_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequenced_pool (sequencing_run_idx, created_by_idx)"
        " VALUES ($1, $2) RETURNING idx",
        run_idx,
        owner_idx,
    )
    # A read-mask action row so the work_ticket (action_id, action_version)
    # FK resolves. The completion query matches on the bare action_id (a sample
    # is "processed" once it has a mask), so the version is arbitrary here.
    action_id = "read-mask"
    action_version = f"v-{uuid.uuid4()}"
    await postgres_pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, target_processing_kinds,"
        "  scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status"
        ") VALUES ($1, $2, 'prep_sample'::qiita.scope_target_kind,"
        "          ARRAY['sequenced']::qiita.processing_kind[], ARRAY['prep_sample:write']::text[],"
        "          $3::jsonb, '{}'::jsonb, '[]'::jsonb, 1, 1, '1 minute', 'active', 'failed')",
        action_id,
        action_version,
        json.dumps({"service": False, "human_roles": ["user"]}),
    )
    # A bcl-convert action row so a pool-scoped demux work_ticket's FK resolves.
    # target_processing_kinds must be empty for a non-prep_sample target_kind
    # (action_processing_kinds_only_for_prep_sample CHECK).
    bcl_action_id = "bcl-convert"
    bcl_action_version = f"v-{uuid.uuid4()}"
    await postgres_pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, target_processing_kinds,"
        "  scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status"
        ") VALUES ($1, $2, 'sequenced_pool'::qiita.scope_target_kind,"
        "          ARRAY[]::qiita.processing_kind[], ARRAY['prep_sample:write']::text[],"
        "          $3::jsonb, '{}'::jsonb, '[]'::jsonb, 1, 1, '1 minute', 'active', 'failed')",
        bcl_action_id,
        bcl_action_version,
        json.dumps({"service": False, "human_roles": ["user"]}),
    )

    samples: list[tuple[int, int, int]] = []  # (biosample, prep_sample, sequenced_sample)
    masks: list[int] = []

    async def mint_mask(*, rype_ref=None, minimap2_ref=None):
        """Insert a mask_definition whose params name host references, so the
        reference-scoped completion can match a ticket's mask against a reference."""
        mask_idx = await postgres_pool.fetchval(
            "INSERT INTO qiita.mask_definition"
            "  (params_hash, filter_workflow, filter_version, params, created_by_idx)"
            " VALUES ($1, 'read-mask', '1.0', $2::jsonb, $3) RETURNING mask_idx",
            secrets.token_bytes(32),
            json.dumps(
                {"host_rype_reference_idx": rype_ref, "host_minimap2_reference_idx": minimap2_ref}
            ),
            owner_idx,
        )
        masks.append(mask_idx)
        return mask_idx

    async def add_sample(*, retired=False):
        bs_idx, ps_idx = await seed_biosample_with_sequenced_prep_sample(
            postgres_pool, owner_idx=owner_idx
        )
        ss_idx = await postgres_pool.fetchval(
            "INSERT INTO qiita.sequenced_sample"
            "  (prep_sample_idx, sequenced_pool_idx, sequenced_pool_item_id, created_by_idx)"
            " VALUES ($1, $2, $3, $4) RETURNING idx",
            ps_idx,
            pool_idx,
            f"item-{secrets.token_hex(4)}",
            owner_idx,
        )
        if retired:
            await postgres_pool.execute(
                "UPDATE qiita.prep_sample SET retired = true, retired_by_idx = $2,"
                " retired_at = now(), retire_reason = 'test' WHERE idx = $1",
                ps_idx,
                owner_idx,
            )
        samples.append((bs_idx, ps_idx, ss_idx))
        return ps_idx

    async def add_ticket(prep_sample_idx, state, mask_idx=None):
        # The work_ticket_failure_consistent CHECK requires the failure columns to
        # be set together on a FAILED ticket and all-NULL otherwise.
        failed = state == "failed"
        await postgres_pool.execute(
            "INSERT INTO qiita.work_ticket"
            "  (action_id, action_version, originator_principal_idx,"
            "   scope_target_kind, prep_sample_idx, mask_idx, state,"
            "   failure_type, failure_stage, failure_reason)"
            " VALUES ($1, $2, $3, 'prep_sample'::qiita.scope_target_kind, $4, $5,"
            "         $6::qiita.work_ticket_state,"
            "         $7::qiita.failure_type, $8::qiita.work_ticket_failure_stage, $9)",
            action_id,
            action_version,
            owner_idx,
            prep_sample_idx,
            mask_idx,
            state,
            "permanent" if failed else None,
            "finalize" if failed else None,
            "test failure" if failed else None,
        )

    async def add_demux_ticket(state):
        # The pool-scoped bcl-convert ticket (sequenced_pool scope target).
        failed = state == "failed"
        await postgres_pool.execute(
            "INSERT INTO qiita.work_ticket"
            "  (action_id, action_version, originator_principal_idx,"
            "   scope_target_kind, sequenced_pool_idx, state,"
            "   failure_type, failure_stage, failure_reason)"
            " VALUES ($1, $2, $3, 'sequenced_pool'::qiita.scope_target_kind, $4,"
            "         $5::qiita.work_ticket_state,"
            "         $6::qiita.failure_type, $7::qiita.work_ticket_failure_stage, $8)",
            bcl_action_id,
            bcl_action_version,
            owner_idx,
            pool_idx,
            state,
            "permanent" if failed else None,
            "finalize" if failed else None,
            "test failure" if failed else None,
        )

    yield {
        "pool": postgres_pool,
        "pool_idx": pool_idx,
        "add_sample": add_sample,
        "add_ticket": add_ticket,
        "add_demux_ticket": add_demux_ticket,
        "mint_mask": mint_mask,
    }

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        action_id,
        action_version,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        bcl_action_id,
        bcl_action_version,
    )
    # Masks after the work_tickets that referenced them (mask_idx is ON DELETE
    # SET NULL, so order isn't strictly required, but keep it FK-reverse).
    if masks:
        await postgres_pool.execute(
            "DELETE FROM qiita.mask_definition WHERE mask_idx = ANY($1::bigint[])", masks
        )
    for _bs, _ps, ss_idx in samples:
        await postgres_pool.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, action_version
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
        bcl_action_id,
        bcl_action_version,
    )
    for _bs, ps_idx, _ss in samples:
        await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", ps_idx)
    for bs_idx, _ps, _ss in samples:
        await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", bs_idx)
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", owner_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", owner_idx)


async def test_empty_pool_is_all_zero(pool_ctx):
    """A pool with no samples: all buckets zero (the aggregate over zero rows is
    one all-zero row)."""
    row = await fetch_sequenced_pool_completion(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert row["sample_count"] == 0
    assert row["samples_completed"] == 0
    assert row["samples_in_flight"] == 0
    assert row["samples_no_data"] == 0
    assert row["samples_failed"] == 0
    assert row["samples_not_submitted"] == 0


async def test_sample_without_ticket_is_not_submitted(pool_ctx):
    await pool_ctx["add_sample"]()
    row = await fetch_sequenced_pool_completion(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert row["sample_count"] == 1
    assert row["samples_not_submitted"] == 1
    assert row["samples_completed"] == 0


async def test_each_terminal_state_buckets(pool_ctx):
    """One sample per bucket: completed, in-flight (processing), no-data, failed,
    and not-submitted — each lands in exactly one count, and the five sum to
    sample_count."""
    ps_done = await pool_ctx["add_sample"]()
    await pool_ctx["add_ticket"](ps_done, "completed")
    ps_run = await pool_ctx["add_sample"]()
    await pool_ctx["add_ticket"](ps_run, "processing")
    ps_empty = await pool_ctx["add_sample"]()
    await pool_ctx["add_ticket"](ps_empty, "no_data")
    ps_fail = await pool_ctx["add_sample"]()
    await pool_ctx["add_ticket"](ps_fail, "failed")
    await pool_ctx["add_sample"]()  # not submitted

    row = await fetch_sequenced_pool_completion(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert row["sample_count"] == 5
    assert row["samples_completed"] == 1
    assert row["samples_in_flight"] == 1
    assert row["samples_no_data"] == 1
    assert row["samples_failed"] == 1
    assert row["samples_not_submitted"] == 1
    bucketed = (
        row["samples_completed"]
        + row["samples_in_flight"]
        + row["samples_no_data"]
        + row["samples_failed"]
        + row["samples_not_submitted"]
    )
    assert bucketed == row["sample_count"]


async def test_no_data_excluded_from_failed_bucket(pool_ctx):
    """A sample whose only ticket is NO_DATA (an empty well) counts as no_data,
    NOT failed — the whole point of the distinct terminal outcome."""
    ps = await pool_ctx["add_sample"]()
    await pool_ctx["add_ticket"](ps, "no_data")
    row = await fetch_sequenced_pool_completion(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert row["sample_count"] == 1
    assert row["samples_no_data"] == 1
    assert row["samples_failed"] == 0


async def test_no_data_wins_over_failed_retry(pool_ctx):
    """A sample with both a stale FAILED ticket and a NO_DATA ticket counts as
    no_data — no_data outranks failed, so an empty well that was retried then
    superseded doesn't get stuck in the failed bucket (and the pool can still
    reach `complete`)."""
    ps = await pool_ctx["add_sample"]()
    await pool_ctx["add_ticket"](ps, "failed")
    await pool_ctx["add_ticket"](ps, "no_data")
    row = await fetch_sequenced_pool_completion(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert row["sample_count"] == 1
    assert row["samples_no_data"] == 1
    assert row["samples_failed"] == 0


async def test_completed_plus_no_data_makes_pool_complete(pool_ctx):
    """A pool of real data with empty wells: completed + no_data == sample_count,
    so the PoolCompletionStatus `complete` flag fires (verified at the model
    layer; here we assert the buckets the flag reads)."""
    ps_done = await pool_ctx["add_sample"]()
    await pool_ctx["add_ticket"](ps_done, "completed")
    ps_empty = await pool_ctx["add_sample"]()
    await pool_ctx["add_ticket"](ps_empty, "no_data")
    row = await fetch_sequenced_pool_completion(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert row["sample_count"] == 2
    assert row["samples_completed"] + row["samples_no_data"] == row["sample_count"]
    assert row["samples_failed"] == 0


async def test_completed_wins_over_failed_retry(pool_ctx):
    """A sample with both a FAILED (first attempt) and a COMPLETED ticket counts
    as completed — completed has top precedence."""
    ps = await pool_ctx["add_sample"]()
    await pool_ctx["add_ticket"](ps, "failed")
    await pool_ctx["add_ticket"](ps, "completed")
    row = await fetch_sequenced_pool_completion(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert row["sample_count"] == 1
    assert row["samples_completed"] == 1
    assert row["samples_failed"] == 0


async def test_in_flight_wins_over_failed(pool_ctx):
    """A sample with a FAILED first attempt and a QUEUED resubmission (no
    COMPLETED) counts as in-flight, not failed — work is ongoing."""
    ps = await pool_ctx["add_sample"]()
    await pool_ctx["add_ticket"](ps, "failed")
    await pool_ctx["add_ticket"](ps, "queued")
    row = await fetch_sequenced_pool_completion(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert row["samples_in_flight"] == 1
    assert row["samples_failed"] == 0


async def test_retired_sample_excluded(pool_ctx):
    """A retired prep_sample contributes to no bucket, even with a COMPLETED
    ticket — it is out of the pool's active set."""
    ps_active = await pool_ctx["add_sample"]()
    await pool_ctx["add_ticket"](ps_active, "completed")
    ps_retired = await pool_ctx["add_sample"](retired=True)
    await pool_ctx["add_ticket"](ps_retired, "completed")
    row = await fetch_sequenced_pool_completion(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert row["sample_count"] == 1
    assert row["samples_completed"] == 1


# ---------------------------------------------------------------------------
# fetch_sequenced_pool_demux_state — the pool-scoped bcl-convert stage
# ---------------------------------------------------------------------------


async def test_demux_state_not_submitted_without_ticket(pool_ctx):
    """A pool with no bcl-convert ticket reads as not_submitted."""
    state = await fetch_sequenced_pool_demux_state(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert state == "not_submitted"


async def test_demux_state_completed(pool_ctx):
    await pool_ctx["add_demux_ticket"]("completed")
    state = await fetch_sequenced_pool_demux_state(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert state == "completed"


async def test_demux_state_failed(pool_ctx):
    await pool_ctx["add_demux_ticket"]("failed")
    state = await fetch_sequenced_pool_demux_state(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert state == "failed"


async def test_demux_state_completed_wins_over_failed(pool_ctx):
    """A force re-submit can leave a stale FAILED bcl-convert beside a COMPLETED
    one; completed outranks failed (precedence), so the pool reads completed."""
    await pool_ctx["add_demux_ticket"]("failed")
    await pool_ctx["add_demux_ticket"]("completed")
    state = await fetch_sequenced_pool_demux_state(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert state == "completed"


# ---------------------------------------------------------------------------
# reference-scoped completion (per-host-reference breakdown)
# ---------------------------------------------------------------------------


async def test_reference_scoped_completion_distinguishes_per_reference(pool_ctx):
    """With a reference_idx, host-masking completion counts only masks that used
    that reference. Sample A is masked against reference 100, sample B against
    reference 200. Scoped to 100: A completed, B not_submitted (masked, but not
    against 100) — which the reference-agnostic form cannot tell apart."""
    ref_a, ref_b = 100, 200
    mask_a = await pool_ctx["mint_mask"](rype_ref=ref_a)
    mask_b = await pool_ctx["mint_mask"](minimap2_ref=ref_b)

    ps_a = await pool_ctx["add_sample"]()
    await pool_ctx["add_ticket"](ps_a, "completed", mask_idx=mask_a)
    ps_b = await pool_ctx["add_sample"]()
    await pool_ctx["add_ticket"](ps_b, "completed", mask_idx=mask_b)

    # Reference-agnostic: both masked → both completed.
    allref = await fetch_sequenced_pool_completion(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert allref["sample_count"] == 2
    assert allref["samples_completed"] == 2
    assert allref["samples_not_submitted"] == 0

    # Scoped to reference 100: only A counts as masked; B is not_submitted.
    scoped = await fetch_sequenced_pool_completion(
        pool_ctx["pool"], pool_ctx["pool_idx"], reference_idx=ref_a
    )
    assert scoped["sample_count"] == 2
    assert scoped["samples_completed"] == 1
    assert scoped["samples_not_submitted"] == 1

    # A reference nobody masked against → every sample not_submitted.
    none_match = await fetch_sequenced_pool_completion(
        pool_ctx["pool"], pool_ctx["pool_idx"], reference_idx=999
    )
    assert none_match["samples_completed"] == 0
    assert none_match["samples_not_submitted"] == 2


async def test_reference_scoped_matches_rype_or_minimap2(pool_ctx):
    """A reference used as EITHER the rype or the minimap2 host reference matches:
    a mask that names reference 300 only as its minimap2 reference still counts
    when scoped to 300."""
    mask = await pool_ctx["mint_mask"](minimap2_ref=300)
    ps = await pool_ctx["add_sample"]()
    await pool_ctx["add_ticket"](ps, "completed", mask_idx=mask)
    scoped = await fetch_sequenced_pool_completion(
        pool_ctx["pool"], pool_ctx["pool_idx"], reference_idx=300
    )
    assert scoped["samples_completed"] == 1
    assert scoped["samples_not_submitted"] == 0


async def test_reference_scope_matches_real_build_mask_params_keys(pool_ctx):
    """Pin the producer<->consumer JSONB-key contract. The reference-scoped
    completion SQL reads md.params->>'host_rype_reference_idx' /
    'host_minimap2_reference_idx'; those keys are produced by
    runner._mask._build_mask_params (the single source of the mask identity
    shape). Mint a mask with its REAL output and assert the scope matches — so a
    future key rename there fails loudly here instead of silently reading every
    sample as not_submitted."""
    from qiita_control_plane.runner._mask import _build_mask_params

    ref = 777
    params = _build_mask_params(
        action_id="read-mask",
        action_version="1.0.0",
        prep_protocol_idx=None,
        instrument_model=None,
        adapter_set_hash=None,
        host_rype_reference_idx=ref,
        host_minimap2_reference_idx=None,
        resolved_lima=None,
        resolved_syndna=None,
    )
    db = pool_ctx["pool"]
    mask_idx = await db.fetchval(
        "INSERT INTO qiita.mask_definition"
        "  (params_hash, filter_workflow, filter_version, params, created_by_idx)"
        " VALUES ($1, $2, $3, $4::jsonb,"
        "  (SELECT created_by_idx FROM qiita.sequenced_pool WHERE idx = $5))"
        " RETURNING mask_idx",
        secrets.token_bytes(32),
        params["filter_workflow"],
        params["filter_version"],
        json.dumps(params),
        pool_ctx["pool_idx"],
    )
    try:
        ps = await pool_ctx["add_sample"]()
        await pool_ctx["add_ticket"](ps, "completed", mask_idx=mask_idx)
        scoped = await fetch_sequenced_pool_completion(db, pool_ctx["pool_idx"], reference_idx=ref)
        assert scoped["samples_completed"] == 1
        assert scoped["samples_not_submitted"] == 0
    finally:
        # mask_idx on work_ticket is ON DELETE SET NULL, so deleting the mask
        # clears the ticket's ref; the ticket itself is cleaned by the fixture.
        await db.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)
