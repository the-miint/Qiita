"""DB tests for fetch_sequenced_pool_completion — the compute-on-read pool
prep-generation completion rollup.

The repo function classifies each non-retired sequenced_sample by the state of
its fastq-to-parquet work tickets (any version) and tallies the four
mutually-exclusive buckets (completed / in-flight / failed / not-submitted). Each
test seeds one principal + one run + one pool + a fastq-to-parquet action, then
attaches samples via `add_sample` and tickets via `add_ticket`; cleanup is
FK-reverse on the shared postgres_pool fixture.
"""

import json
import secrets
import uuid

import pytest
import pytest_asyncio

from qiita_control_plane.repositories.sequencing_run import fetch_sequenced_pool_completion
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
    # A fastq-to-parquet action row so the work_ticket (action_id, action_version)
    # FK resolves. The completion query matches on the bare action_id, so the
    # version is arbitrary here.
    action_id = "fastq-to-parquet"
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

    samples: list[tuple[int, int, int]] = []  # (biosample, prep_sample, sequenced_sample)

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

    async def add_ticket(prep_sample_idx, state):
        # The work_ticket_failure_consistent CHECK requires the failure columns to
        # be set together on a FAILED ticket and all-NULL otherwise.
        failed = state == "failed"
        await postgres_pool.execute(
            "INSERT INTO qiita.work_ticket"
            "  (action_id, action_version, originator_principal_idx,"
            "   scope_target_kind, prep_sample_idx, state,"
            "   failure_type, failure_stage, failure_reason)"
            " VALUES ($1, $2, $3, 'prep_sample'::qiita.scope_target_kind, $4,"
            "         $5::qiita.work_ticket_state,"
            "         $6::qiita.failure_type, $7::qiita.work_ticket_failure_stage, $8)",
            action_id,
            action_version,
            owner_idx,
            prep_sample_idx,
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
    }

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        action_id,
        action_version,
    )
    for _bs, _ps, ss_idx in samples:
        await postgres_pool.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, action_version
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
    assert row["samples_failed"] == 0
    assert row["samples_not_submitted"] == 0


async def test_sample_without_ticket_is_not_submitted(pool_ctx):
    await pool_ctx["add_sample"]()
    row = await fetch_sequenced_pool_completion(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert row["sample_count"] == 1
    assert row["samples_not_submitted"] == 1
    assert row["samples_completed"] == 0


async def test_each_terminal_state_buckets(pool_ctx):
    """One sample per bucket: completed, in-flight (processing), failed, and
    not-submitted — each lands in exactly one count, and the four sum to
    sample_count."""
    ps_done = await pool_ctx["add_sample"]()
    await pool_ctx["add_ticket"](ps_done, "completed")
    ps_run = await pool_ctx["add_sample"]()
    await pool_ctx["add_ticket"](ps_run, "processing")
    ps_fail = await pool_ctx["add_sample"]()
    await pool_ctx["add_ticket"](ps_fail, "failed")
    await pool_ctx["add_sample"]()  # not submitted

    row = await fetch_sequenced_pool_completion(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert row["sample_count"] == 4
    assert row["samples_completed"] == 1
    assert row["samples_in_flight"] == 1
    assert row["samples_failed"] == 1
    assert row["samples_not_submitted"] == 1
    bucketed = (
        row["samples_completed"]
        + row["samples_in_flight"]
        + row["samples_failed"]
        + row["samples_not_submitted"]
    )
    assert bucketed == row["sample_count"]


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
