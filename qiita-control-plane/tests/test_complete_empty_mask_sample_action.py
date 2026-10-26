"""Tests for `complete_empty_mask_sample`, which records a prep_sample that ingest
found empty as masked under a mask with the rows a finished mask leaves behind, and
`clear_empty_mask_completions`, which removes that record when reads arrive, including
when the two run at once."""

import asyncio
import contextlib
import json
import uuid

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.actions import BAM_TO_PARQUET_ACTION_ID
from qiita_common.api_paths import URL_SEQUENCE_RANGE_PREFIX

from qiita_control_plane.actions.library import (
    clear_empty_mask_completions,
    complete_empty_mask_sample,
    complete_empty_mask_sample_in_tx,
)
from qiita_control_plane.repositories import INT4_MASK
from qiita_control_plane.repositories.block import MaskSampleInvalidated
from qiita_control_plane.repositories.sequence_range import PREP_SAMPLE_RANGE_LOCK_CLASS
from qiita_control_plane.routes import sequence_range as sequence_range_route
from qiita_control_plane.testing.db_seeds import (
    cleanup_reference_graph,
    delete_action_if_created,
    seed_action_if_absent,
    seed_bare_feature,
    seed_bare_reference,
    seed_biosample_with_sequenced_prep_sample,
    seed_reference_membership,
    seed_sequenced_sample_subtype,
    seed_user_principal,
)
from qiita_control_plane.testing.db_teardown import (
    delete_idxs,
    delete_principal,
    teardown_entity_graph,
)
from qiita_control_plane.testing.postgres import until_advisory_lock_awaited

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def seeded(postgres_pool):
    """A pooled prep_sample with no reads, a SynDNA reference of three inserts, and
    two masks: one carrying that reference and one without. `ingested_empty()` adds
    the bam-to-parquet ticket that shows ingest found the prep_sample empty."""
    principal_idx = await seed_user_principal(postgres_pool, prefix="empty-mask", suffix="owner")
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    run_idx, pool_idx, _ss_idx = await seed_sequenced_sample_subtype(
        postgres_pool,
        prep_sample_idx=prep_sample_idx,
        owner_idx=principal_idx,
        sequenced_pool_item_id=f"item-{uuid.uuid4().hex[:8]}",
    )
    reference_idx = await seed_bare_reference(postgres_pool, label="empty-mask-syndna")
    inserts = [await seed_bare_feature(postgres_pool) for _ in range(3)]
    for feature_idx in inserts:
        await seed_reference_membership(
            postgres_pool, reference_idx=reference_idx, feature_idx=feature_idx
        )
    masks = {}
    for name, syndna in (("syndna", {"reference_idx": reference_idx}), ("plain", None)):
        masks[name] = await postgres_pool.fetchval(
            "INSERT INTO qiita.mask_definition"
            " (params_hash, filter_workflow, filter_version, params, created_by_idx)"
            " VALUES ($1, 'read-mask', '1.0.0', $2::jsonb, $3) RETURNING mask_idx",
            uuid.uuid4().bytes + uuid.uuid4().bytes,
            json.dumps({"resolved_syndna": syndna}),
            principal_idx,
        )
    version = f"v-{uuid.uuid4()}"
    created = await seed_action_if_absent(
        postgres_pool,
        action_id=BAM_TO_PARQUET_ACTION_ID,
        version=version,
        target_kind="prep_sample",
    )

    async def ingested_empty():
        return await postgres_pool.fetchval(
            "INSERT INTO qiita.work_ticket"
            "  (action_id, action_version, originator_principal_idx,"
            "   scope_target_kind, prep_sample_idx, state)"
            " VALUES ($1, $2, $3, 'prep_sample', $4, 'no_data') RETURNING work_ticket_idx",
            BAM_TO_PARQUET_ACTION_ID,
            version,
            principal_idx,
            prep_sample_idx,
        )

    yield {
        "principal_idx": principal_idx,
        "prep_sample_idx": prep_sample_idx,
        "inserts": inserts,
        "masks": masks,
        "ingested_empty": ingested_empty,
    }
    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        BAM_TO_PARQUET_ACTION_ID,
        version,
    )
    await delete_action_if_created(
        postgres_pool, action_id=BAM_TO_PARQUET_ACTION_ID, version=version, created=created
    )
    await teardown_entity_graph(
        postgres_pool,
        study_idxs=[],
        biosample_idxs=[biosample_idx],
        prep_sample_idxs=[prep_sample_idx],
    )
    await delete_idxs(postgres_pool, "sequenced_pool", pool_idx)
    await delete_idxs(postgres_pool, "sequencing_run", run_idx)
    await postgres_pool.execute(
        "DELETE FROM qiita.mask_definition WHERE mask_idx = ANY($1::bigint[])",
        list(masks.values()),
    )
    await cleanup_reference_graph(postgres_pool, reference_idx=reference_idx, feature_idxs=inserts)
    await delete_principal(postgres_pool, [principal_idx])


async def _gate(pool, mask_idx, prep_sample_idx):
    return await pool.fetchval(
        "SELECT state FROM qiita.mask_sample WHERE mask_idx = $1 AND prep_sample_idx = $2",
        mask_idx,
        prep_sample_idx,
    )


async def _counts(pool, prep_sample_idx):
    row = await pool.fetchrow(
        "SELECT raw_read_count_r1r2, biological_read_count_r1r2,"
        "       quality_filtered_read_count_r1r2, spikein_read_count_r1r2"
        "  FROM qiita.sequenced_sample WHERE prep_sample_idx = $1",
        prep_sample_idx,
    )
    return tuple(row)


async def _syndna(pool, mask_idx, prep_sample_idx):
    rows = await pool.fetch(
        "SELECT feature_idx, read_count FROM qiita.syndna_read_count"
        " WHERE mask_idx = $1 AND prep_sample_idx = $2",
        mask_idx,
        prep_sample_idx,
    )
    return {r["feature_idx"]: r["read_count"] for r in rows}


async def _complete(pool, seeded, mask_name):
    return await complete_empty_mask_sample(
        pool,
        mask_idx=seeded["masks"][mask_name],
        prep_sample_idx=seeded["prep_sample_idx"],
        current_work_ticket_idx=None,
    )


async def test_completes_the_gate_with_zero_counts(postgres_pool, seeded):
    ps, mask_idx = seeded["prep_sample_idx"], seeded["masks"]["plain"]
    proof = await seeded["ingested_empty"]()

    assert await _complete(postgres_pool, seeded, "plain") == proof

    assert await _gate(postgres_pool, mask_idx, ps) == "completed"
    assert await _counts(postgres_pool, ps) == (0, 0, 0, 0)
    assert await _syndna(postgres_pool, mask_idx, ps) == {}


async def test_writes_a_zero_per_syndna_insert(postgres_pool, seeded):
    """The SynDNA export refuses a completed prep_sample with no count rows."""
    ps, mask_idx = seeded["prep_sample_idx"], seeded["masks"]["syndna"]
    await seeded["ingested_empty"]()

    await _complete(postgres_pool, seeded, "syndna")

    assert await _gate(postgres_pool, mask_idx, ps) == "completed"
    assert await _syndna(postgres_pool, mask_idx, ps) == dict.fromkeys(seeded["inserts"], 0)


async def test_is_idempotent(postgres_pool, seeded):
    ps, mask_idx = seeded["prep_sample_idx"], seeded["masks"]["syndna"]
    await seeded["ingested_empty"]()

    for _ in range(2):
        await _complete(postgres_pool, seeded, "syndna")

    assert await _gate(postgres_pool, mask_idx, ps) == "completed"
    assert await _syndna(postgres_pool, mask_idx, ps) == dict.fromkeys(seeded["inserts"], 0)


async def test_writes_nothing_when_no_ingest_shows_the_prep_sample_empty(postgres_pool, seeded):
    """The writer reads the ingest record itself, so no caller can open the gate
    without it."""
    ps, mask_idx = seeded["prep_sample_idx"], seeded["masks"]["syndna"]

    assert await _complete(postgres_pool, seeded, "syndna") is None

    assert await _gate(postgres_pool, mask_idx, ps) is None
    assert await _counts(postgres_pool, ps) == (None, None, None, None)
    assert await _syndna(postgres_pool, mask_idx, ps) == {}


async def test_a_withdrawn_gate_stays_withdrawn_and_nothing_is_written(postgres_pool, seeded):
    ps, mask_idx = seeded["prep_sample_idx"], seeded["masks"]["syndna"]
    await seeded["ingested_empty"]()
    await postgres_pool.execute(
        "INSERT INTO qiita.mask_sample"
        "  (mask_idx, prep_sample_idx, state,"
        "   invalidated_at, invalidated_by_idx, invalidation_reason)"
        " VALUES ($1, $2, 'invalidated', now(), $3, 'test withdrawal')",
        mask_idx,
        ps,
        seeded["principal_idx"],
    )

    with pytest.raises(MaskSampleInvalidated):
        await _complete(postgres_pool, seeded, "syndna")

    assert await _gate(postgres_pool, mask_idx, ps) == "invalidated"
    assert await _counts(postgres_pool, ps) == (None, None, None, None)
    assert await _syndna(postgres_pool, mask_idx, ps) == {}


async def _clear(pool, prep_sample_idx):
    async with pool.acquire() as conn, conn.transaction():
        return await clear_empty_mask_completions(conn, prep_sample_idx=prep_sample_idx)


async def test_clearing_removes_the_record_when_reads_arrive(postgres_pool, seeded):
    """Left in place, the record would keep serving an empty pass-set over the
    prep_sample's new reads."""
    ps = seeded["prep_sample_idx"]
    await seeded["ingested_empty"]()
    for name in ("syndna", "plain"):
        await _complete(postgres_pool, seeded, name)

    assert await _clear(postgres_pool, ps) == 2

    for mask_idx in seeded["masks"].values():
        assert await _gate(postgres_pool, mask_idx, ps) is None
        assert await _syndna(postgres_pool, mask_idx, ps) == {}
    assert await _counts(postgres_pool, ps) == (None, None, None, None)


async def test_clearing_leaves_a_withdrawn_row_and_touches_nothing_else(postgres_pool, seeded):
    ps, mask_idx = seeded["prep_sample_idx"], seeded["masks"]["plain"]
    await postgres_pool.execute(
        "INSERT INTO qiita.mask_sample"
        "  (mask_idx, prep_sample_idx, state,"
        "   invalidated_at, invalidated_by_idx, invalidation_reason)"
        " VALUES ($1, $2, 'invalidated', now(), $3, 'test withdrawal')",
        mask_idx,
        ps,
        seeded["principal_idx"],
    )
    await postgres_pool.execute(
        "UPDATE qiita.sequenced_sample SET raw_read_count_r1r2 = 5,"
        " biological_read_count_r1r2 = 5, quality_filtered_read_count_r1r2 = 5,"
        " spikein_read_count_r1r2 = 0 WHERE prep_sample_idx = $1",
        ps,
    )

    assert await _clear(postgres_pool, ps) == 0

    assert await _gate(postgres_pool, mask_idx, ps) == "invalidated"
    assert await _counts(postgres_pool, ps) == (5, 5, 5, 0)


async def test_read_counts_are_written_all_set_or_all_unset(postgres_pool, seeded):
    """No masking run leaves a partial set of counts, so the writer refuses one."""
    from qiita_control_plane.actions.library import _update_sequenced_sample_read_counts

    with pytest.raises(ValueError, match="all set or all None"):
        await _update_sequenced_sample_read_counts(
            postgres_pool,
            seeded["prep_sample_idx"],
            raw=0,
            biological=None,
            quality_filtered=0,
            spikein=0,
        )
    assert await _counts(postgres_pool, seeded["prep_sample_idx"]) == (None, None, None, None)


@pytest_asyncio.fixture
async def mint_range(postgres_pool, compute_worker_service_account):
    """`mint_range(prep_sample_idx)`: `POST /sequence-range` as the compute service
    account, returning the response."""
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.settings = Settings(
        database_url="unused", flight_signing_key=b"\x00" * 32, data_plane_url="unused"
    )
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {compute_worker_service_account['token']}"},
    ) as client:

        async def mint(prep_sample_idx):
            # The range records its minter; no FK, so any positive idx does.
            return await client.post(
                URL_SEQUENCE_RANGE_PREFIX,
                json={"prep_sample_idx": prep_sample_idx, "count": 3, "work_ticket_idx": 7},
            )

        yield mint


async def _until_range_lock_awaited(pool, prep_sample_idx, task):
    await asyncio.wait_for(
        until_advisory_lock_awaited(
            pool,
            lock_class=PREP_SAMPLE_RANGE_LOCK_CLASS,
            key=prep_sample_idx & INT4_MASK,
            task=task,
        ),
        timeout=10,
    )


async def _cancel_unfinished(tasks):
    """Cancel and reap what a failed test left running, so it cannot write during
    teardown and hide the failure."""
    for task in tasks:
        if not task.done():
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


async def test_a_writer_that_starts_during_a_mint_waits_and_writes_nothing(
    postgres_pool, seeded, mint_range, monkeypatch
):
    """The mint has inserted its range but not committed when the writer starts.
    The mint holds the prep_sample's range lock from before its insert, so the writer
    waits for the commit, then finds the range and records nothing."""
    ps, mask_idx = seeded["prep_sample_idx"], seeded["masks"]["syndna"]
    await seeded["ingested_empty"]()
    inserted, release = asyncio.Event(), asyncio.Event()
    real_clear = sequence_range_route.clear_empty_mask_completions

    async def clear_after_release(conn, *, prep_sample_idx):
        inserted.set()
        await release.wait()
        return await real_clear(conn, prep_sample_idx=prep_sample_idx)

    monkeypatch.setattr(sequence_range_route, "clear_empty_mask_completions", clear_after_release)
    tasks = [mint := asyncio.create_task(mint_range(ps))]
    try:
        try:
            await asyncio.wait_for(inserted.wait(), timeout=10)
            tasks.append(writer := asyncio.create_task(_complete(postgres_pool, seeded, "syndna")))
            await _until_range_lock_awaited(postgres_pool, ps, writer)
        finally:
            release.set()

        resp = await asyncio.wait_for(mint, timeout=10)
        assert resp.status_code == 201, resp.text
        assert await asyncio.wait_for(writer, timeout=10) is None
    finally:
        await _cancel_unfinished(tasks)
    assert await _gate(postgres_pool, mask_idx, ps) is None
    assert await _counts(postgres_pool, ps) == (None, None, None, None)
    assert await _syndna(postgres_pool, mask_idx, ps) == {}


async def test_a_mint_during_an_uncommitted_completion_waits_and_removes_it(
    postgres_pool, seeded, mint_range
):
    """The writer has recorded the prep_sample empty but not committed when the mint
    starts. The writer holds the range lock to its commit, so the mint waits, and its
    clean-up then sees the record and removes it."""
    ps, mask_idx = seeded["prep_sample_idx"], seeded["masks"]["syndna"]
    await seeded["ingested_empty"]()
    tasks = []
    try:
        async with postgres_pool.acquire() as conn, conn.transaction():
            assert (
                await complete_empty_mask_sample_in_tx(
                    conn, mask_idx=mask_idx, prep_sample_idx=ps, current_work_ticket_idx=None
                )
                is not None
            )
            tasks.append(mint := asyncio.create_task(mint_range(ps)))
            await _until_range_lock_awaited(postgres_pool, ps, mint)

        resp = await asyncio.wait_for(mint, timeout=10)
        assert resp.status_code == 201, resp.text
    finally:
        await _cancel_unfinished(tasks)
    assert await _gate(postgres_pool, mask_idx, ps) is None
    assert await _counts(postgres_pool, ps) == (None, None, None, None)
    assert await _syndna(postgres_pool, mask_idx, ps) == {}
