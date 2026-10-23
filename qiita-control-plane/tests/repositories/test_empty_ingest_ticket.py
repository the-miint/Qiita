"""DB tests for fetch_empty_ingest_tickets — the record that a prep_sample with no
sequence_range was ingested and found empty, rather than never ingested or left to an
ingest that failed or has not finished. Only the newest ingest record counts.

Each test seeds samples on the shared `pool_ctx` pool and attaches ingest tickets
directly: per-sample (fastq-to-parquet, bam-to-parquet) by prep_sample, the pool
ingest (bcl-convert) by sequenced_pool with a `sample_map` roster.
"""

import json
import secrets
import uuid

import pytest
import pytest_asyncio
from qiita_common.actions import (
    BAM_TO_PARQUET_ACTION_ID,
    BCL_CONVERT_ACTION_ID,
    BCL_CONVERT_SAMPLE_MAP_KEY,
    FASTQ_TO_PARQUET_ACTION_ID,
    READ_MASK_ACTION_ID,
)

from qiita_control_plane.repositories.sequenced_sample import fetch_empty_ingest_tickets
from qiita_control_plane.testing.db_seeds import delete_action_if_created, seed_action_if_absent

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def ingest_ctx(pool_ctx):
    """`pool_ctx` plus throwaway-version action rows for the three ingests and
    read-mask, and helpers to attach tickets and a read range to a sample."""
    pg = pool_ctx["pool"]
    owner_idx = pool_ctx["owner_idx"]
    versions: dict[str, str] = {}
    for action_id, target_kind in (
        (BAM_TO_PARQUET_ACTION_ID, "prep_sample"),
        (FASTQ_TO_PARQUET_ACTION_ID, "prep_sample"),
        (READ_MASK_ACTION_ID, "prep_sample"),
        (BCL_CONVERT_ACTION_ID, "sequenced_pool"),
    ):
        version = f"v-{uuid.uuid4()}"
        created = await seed_action_if_absent(
            pg, action_id=action_id, version=version, target_kind=target_kind
        )
        versions[action_id] = (version, created)

    def _failure(state):
        # work_ticket_failure_consistent: the failure columns are set together on a
        # FAILED ticket and all-NULL otherwise.
        failed = state == "failed"
        return (
            "permanent" if failed else None,
            "submission" if failed else None,
            "test failure" if failed else None,
        )

    async def add_sample_ticket(action_id, prep_sample_idx, state):
        return await pg.fetchval(
            "INSERT INTO qiita.work_ticket"
            "  (action_id, action_version, originator_principal_idx,"
            "   scope_target_kind, prep_sample_idx, state,"
            "   failure_type, failure_stage, failure_reason)"
            " VALUES ($1, $2, $3, 'prep_sample'::qiita.scope_target_kind, $4,"
            "         $5::qiita.work_ticket_state, $6::qiita.failure_type,"
            "         $7::qiita.work_ticket_failure_stage, $8)"
            " RETURNING work_ticket_idx",
            action_id,
            versions[action_id][0],
            owner_idx,
            prep_sample_idx,
            state,
            *_failure(state),
        )

    async def add_pool_ingest(state, roster, *, sequenced_pool_idx=None):
        sample_map = [
            {"prep_sample_idx": ps, "pool_item_id": f"item-{i}"} for i, ps in enumerate(roster)
        ]
        return await pg.fetchval(
            "INSERT INTO qiita.work_ticket"
            "  (action_id, action_version, originator_principal_idx,"
            "   scope_target_kind, sequenced_pool_idx, state, action_context,"
            "   failure_type, failure_stage, failure_reason)"
            " VALUES ($1, $2, $3, 'sequenced_pool'::qiita.scope_target_kind, $4,"
            "         $5::qiita.work_ticket_state, $6::jsonb, $7::qiita.failure_type,"
            "         $8::qiita.work_ticket_failure_stage, $9)"
            " RETURNING work_ticket_idx",
            BCL_CONVERT_ACTION_ID,
            versions[BCL_CONVERT_ACTION_ID][0],
            owner_idx,
            sequenced_pool_idx or pool_ctx["pool_idx"],
            state,
            json.dumps({BCL_CONVERT_SAMPLE_MAP_KEY: sample_map}),
            *_failure(state),
        )

    async def add_range(prep_sample_idx):
        # A start far from anything else this worker seeds; ranges must not overlap.
        start = 10**12 + secrets.randbelow(10**9) * 1000
        await pg.execute(
            "INSERT INTO qiita.sequence_range"
            " (prep_sample_idx, sequence_idx_start, sequence_idx_stop, created_by_idx)"
            " VALUES ($1, $2, $3, $4)",
            prep_sample_idx,
            start,
            start + 9,
            owner_idx,
        )

    extra_pools: list[int] = []

    async def other_pool():
        pool_idx = await pg.fetchval(
            "INSERT INTO qiita.sequenced_pool (sequencing_run_idx, created_by_idx)"
            " VALUES ($1, $2) RETURNING idx",
            pool_ctx["run_idx"],
            owner_idx,
        )
        extra_pools.append(pool_idx)
        return pool_idx

    yield {
        **pool_ctx,
        "add_sample_ticket": add_sample_ticket,
        "add_pool_ingest": add_pool_ingest,
        "add_range": add_range,
        "other_pool": other_pool,
    }

    for action_id, (version, created) in versions.items():
        await pg.execute(
            "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
            action_id,
            version,
        )
        await delete_action_if_created(pg, action_id=action_id, version=version, created=created)
    for pool_idx in extra_pools:
        await pg.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)


async def _proof(ctx, prep_sample_idx, current=None):
    found = await fetch_empty_ingest_tickets(
        ctx["pool"], prep_sample_idxs=[prep_sample_idx], current_work_ticket_idx=current
    )
    return found.get(prep_sample_idx)


async def test_no_ingest_record_is_none(ingest_ctx):
    s = await ingest_ctx["add_sample"]()
    assert await _proof(ingest_ctx, s.prep_sample_idx) is None


@pytest.mark.parametrize("action_id", [BAM_TO_PARQUET_ACTION_ID, FASTQ_TO_PARQUET_ACTION_ID])
async def test_per_sample_ingest_no_data_is_proof(ingest_ctx, action_id):
    s = await ingest_ctx["add_sample"]()
    wt = await ingest_ctx["add_sample_ticket"](action_id, s.prep_sample_idx, "no_data")
    assert await _proof(ingest_ctx, s.prep_sample_idx) == wt


@pytest.mark.parametrize("state", ["failed", "completed", "processing"])
async def test_per_sample_ingest_in_another_state_is_not_proof(ingest_ctx, state):
    """Only NO_DATA says the input was read and held nothing; a failed ingest may
    never have read it."""
    s = await ingest_ctx["add_sample"]()
    await ingest_ctx["add_sample_ticket"](BAM_TO_PARQUET_ACTION_ID, s.prep_sample_idx, state)
    assert await _proof(ingest_ctx, s.prep_sample_idx) is None


async def test_the_callers_own_running_ingest_is_proof(ingest_ctx):
    """fastq-to-parquet's run handler asks while its own ticket is still running: the
    ticket that found the input empty is the caller."""
    s = await ingest_ctx["add_sample"]()
    wt = await ingest_ctx["add_sample_ticket"](
        FASTQ_TO_PARQUET_ACTION_ID, s.prep_sample_idx, "processing"
    )
    assert await _proof(ingest_ctx, s.prep_sample_idx, current=wt) == wt
    assert await _proof(ingest_ctx, s.prep_sample_idx) is None


async def test_a_running_read_mask_is_not_its_own_proof(ingest_ctx):
    """read-mask stores no reads, so its own ticket says nothing about ingest."""
    s = await ingest_ctx["add_sample"]()
    wt = await ingest_ctx["add_sample_ticket"](READ_MASK_ACTION_ID, s.prep_sample_idx, "processing")
    assert await _proof(ingest_ctx, s.prep_sample_idx, current=wt) is None


@pytest.mark.parametrize("state", ["completed", "no_data"])
async def test_finished_pool_ingest_listing_the_sample_is_proof(ingest_ctx, state):
    s = await ingest_ctx["add_sample"]()
    wt = await ingest_ctx["add_pool_ingest"](state, [s.prep_sample_idx])
    assert await _proof(ingest_ctx, s.prep_sample_idx) == wt


async def test_failed_pool_ingest_is_not_proof(ingest_ctx):
    s = await ingest_ctx["add_sample"]()
    await ingest_ctx["add_pool_ingest"]("failed", [s.prep_sample_idx])
    assert await _proof(ingest_ctx, s.prep_sample_idx) is None


async def test_pool_ingest_whose_roster_omits_the_sample_is_not_proof(ingest_ctx):
    s = await ingest_ctx["add_sample"]()
    other = await ingest_ctx["add_sample"]()
    await ingest_ctx["add_pool_ingest"]("completed", [other.prep_sample_idx])
    assert await _proof(ingest_ctx, s.prep_sample_idx) is None


async def test_pool_ingest_of_another_pool_is_not_proof(ingest_ctx):
    s = await ingest_ctx["add_sample"]()
    elsewhere = await ingest_ctx["other_pool"]()
    await ingest_ctx["add_pool_ingest"](
        "completed", [s.prep_sample_idx], sequenced_pool_idx=elsewhere
    )
    assert await _proof(ingest_ctx, s.prep_sample_idx) is None


async def test_a_sample_with_reads_is_never_empty(ingest_ctx):
    """A range means reads exist, whatever an ingest ticket says."""
    s = await ingest_ctx["add_sample"]()
    await ingest_ctx["add_sample_ticket"](BAM_TO_PARQUET_ACTION_ID, s.prep_sample_idx, "no_data")
    await ingest_ctx["add_pool_ingest"]("completed", [s.prep_sample_idx])
    await ingest_ctx["add_range"](s.prep_sample_idx)
    assert await _proof(ingest_ctx, s.prep_sample_idx) is None


@pytest.mark.parametrize("state", ["failed", "cancelled", "queued", "processing"])
async def test_a_newer_unfinished_or_failed_ingest_overrides_an_older_no_data(ingest_ctx, state):
    """An empty or wrong file ends NO_DATA; the corrected file's ingest fails, is
    cancelled or has not finished. The newer ingest was given reads the older never
    saw, so the prep_sample is not shown empty."""
    s = await ingest_ctx["add_sample"]()
    add_ticket = ingest_ctx["add_sample_ticket"]
    await add_ticket(BAM_TO_PARQUET_ACTION_ID, s.prep_sample_idx, "no_data")
    await add_ticket(FASTQ_TO_PARQUET_ACTION_ID, s.prep_sample_idx, state)
    assert await _proof(ingest_ctx, s.prep_sample_idx) is None


async def test_a_newer_no_data_ingest_after_a_failed_one_is_proof(ingest_ctx):
    s = await ingest_ctx["add_sample"]()
    add_ticket = ingest_ctx["add_sample_ticket"]
    await add_ticket(BAM_TO_PARQUET_ACTION_ID, s.prep_sample_idx, "failed")
    wt = await add_ticket(BAM_TO_PARQUET_ACTION_ID, s.prep_sample_idx, "no_data")
    assert await _proof(ingest_ctx, s.prep_sample_idx) == wt


async def test_the_newest_record_decides_across_pool_and_per_sample_ingests(ingest_ctx):
    """A per-sample ingest that fails after the pool ingest found the well empty
    leaves the prep_sample unproven; a pool ingest after a failed per-sample one
    shows it empty."""
    after_pool, after_sample = [await ingest_ctx["add_sample"]() for _ in range(2)]
    add_ticket = ingest_ctx["add_sample_ticket"]
    await ingest_ctx["add_pool_ingest"]("completed", [after_pool.prep_sample_idx])
    await add_ticket(BAM_TO_PARQUET_ACTION_ID, after_pool.prep_sample_idx, "failed")
    await add_ticket(BAM_TO_PARQUET_ACTION_ID, after_sample.prep_sample_idx, "failed")
    pool_ingest = await ingest_ctx["add_pool_ingest"]("completed", [after_sample.prep_sample_idx])

    assert await _proof(ingest_ctx, after_pool.prep_sample_idx) is None
    assert await _proof(ingest_ctx, after_sample.prep_sample_idx) == pool_ingest


@pytest.mark.parametrize("state", ["failed", "queued"])
async def test_a_newer_unfinished_or_failed_pool_ingest_overrides_a_per_sample_no_data(
    ingest_ctx, state
):
    s = await ingest_ctx["add_sample"]()
    await ingest_ctx["add_sample_ticket"](BAM_TO_PARQUET_ACTION_ID, s.prep_sample_idx, "no_data")
    await ingest_ctx["add_pool_ingest"](state, [s.prep_sample_idx])
    assert await _proof(ingest_ctx, s.prep_sample_idx) is None


async def test_the_callers_own_ingest_is_not_proof_behind_a_newer_one(ingest_ctx):
    """An ingest that finds its input empty while a newer ingest of the prep_sample
    is queued does not record it empty: the newer one may store reads."""
    s = await ingest_ctx["add_sample"]()
    add_ticket = ingest_ctx["add_sample_ticket"]
    running = await add_ticket(FASTQ_TO_PARQUET_ACTION_ID, s.prep_sample_idx, "processing")
    # Another action: one ticket of an action version is in flight per prep_sample.
    await add_ticket(BAM_TO_PARQUET_ACTION_ID, s.prep_sample_idx, "queued")
    assert await _proof(ingest_ctx, s.prep_sample_idx, current=running) is None


async def test_answers_for_many_prep_samples_at_once(ingest_ctx):
    """One read answers a whole pool: each prep_sample whose newest ingest shows it
    empty maps to that ingest, and one nothing shows is absent."""
    by_bam, by_pool, unproven = [await ingest_ctx["add_sample"]() for _ in range(3)]
    add_ticket = ingest_ctx["add_sample_ticket"]
    await add_ticket(BAM_TO_PARQUET_ACTION_ID, by_bam.prep_sample_idx, "no_data")
    newer = await add_ticket(BAM_TO_PARQUET_ACTION_ID, by_bam.prep_sample_idx, "no_data")
    pool_ingest = await ingest_ctx["add_pool_ingest"]("completed", [by_pool.prep_sample_idx])

    found = await fetch_empty_ingest_tickets(
        ingest_ctx["pool"],
        prep_sample_idxs=[s.prep_sample_idx for s in (by_bam, by_pool, unproven)],
        current_work_ticket_idx=None,
    )

    assert found == {by_bam.prep_sample_idx: newer, by_pool.prep_sample_idx: pool_ingest}
