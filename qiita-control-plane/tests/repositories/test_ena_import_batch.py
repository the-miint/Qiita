"""Repository-layer tests for qiita.ena_import_batch / qiita.ena_import_batch_item.

Exercises the SQL moved out of `ena_import.batch` (into
`repositories.ena_import_batch`) directly against Postgres. Orchestration
behaviour (the resolve/register/submit flow, the per-item state machine, the
`fetch_batch_status` rollup) stays covered by `tests/ena_import/test_batch.py`
unchanged -- this file only pins the query shapes these functions issue.
"""

import json
import secrets

import pytest
import pytest_asyncio
from qiita_common.models.ena_import import BatchItemState

from qiita_control_plane.repositories.ena_import_batch import (
    append_ena_import_batch_item_download_ticket,
    ena_import_batch_exists,
    ena_import_created_study,
    fetch_ena_import_batch_items,
    fetch_inflight_ena_import_batch_items,
    fetch_sequenced_pool_download_states,
    fetch_work_ticket_states_for_idxs,
    insert_ena_import_batch,
    insert_ena_import_batch_item,
    update_ena_import_batch_item_registered,
    update_ena_import_batch_item_state,
    update_ena_import_batch_item_study_created,
)
from qiita_control_plane.repositories.study import create_study
from qiita_control_plane.testing.db_seeds import (
    delete_action_if_created,
    seed_action_if_absent,
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
    seed_user_principal,
)
from qiita_control_plane.testing.unique_names import unique_accession

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def eib(postgres_pool):
    """Seed one principal; yield helpers + pool + tracking lists.

    Tests create their own batches (and, sometimes, studies) and register
    them here so teardown can target exactly the rows this test created.
    """
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="eib-test", suffix=suffix)
    created_batches: list[int] = []
    created_studies: list[int] = []

    yield {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "created_batches": created_batches,
        "created_studies": created_studies,
    }

    # FK-reverse: batches first (CASCADE removes their items, which is what
    # RESTRICTs a study delete), then studies, then the principal.
    if created_batches:
        await postgres_pool.execute(
            "DELETE FROM qiita.ena_import_batch WHERE idx = ANY($1::bigint[])", created_batches
        )
    if created_studies:
        await postgres_pool.execute(
            "DELETE FROM qiita.study_access WHERE study_idx = ANY($1::bigint[])", created_studies
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.study WHERE idx = ANY($1::bigint[])", created_studies
        )
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


async def _new_batch_item(eib, *, accession: str | None = None) -> tuple[int, int]:
    """insert_ena_import_batch + insert_ena_import_batch_item in one
    transaction, mirroring `ena_import.batch.create_ena_import_batch`'s own
    loop. Tracked for the `eib` fixture's teardown."""
    accession = accession or unique_accession("PRJNA")
    pool = eib["pool"]
    async with pool.acquire() as conn, conn.transaction():
        batch_idx = await insert_ena_import_batch(
            conn, submitted_by_principal_idx=eib["principal_idx"]
        )
        item_idx = await insert_ena_import_batch_item(
            conn, batch_idx=batch_idx, ena_study_accession=accession
        )
    eib["created_batches"].append(batch_idx)
    return batch_idx, item_idx


# ---------------------------------------------------------------------------
# insert_ena_import_batch / insert_ena_import_batch_item
# ---------------------------------------------------------------------------


async def test_insert_ena_import_batch_and_item_creates_rows(eib):
    accession = unique_accession("PRJNA")
    batch_idx, item_idx = await _new_batch_item(eib, accession=accession)

    batch_row = await eib["pool"].fetchrow(
        "SELECT submitted_by_principal_idx FROM qiita.ena_import_batch WHERE idx = $1", batch_idx
    )
    assert batch_row["submitted_by_principal_idx"] == eib["principal_idx"]

    item_row = await eib["pool"].fetchrow(
        "SELECT batch_idx, ena_study_accession, state, study_created,"
        "       download_work_ticket_idxs, ena_run_outcomes"
        " FROM qiita.ena_import_batch_item WHERE idx = $1",
        item_idx,
    )
    assert item_row["batch_idx"] == batch_idx
    assert item_row["ena_study_accession"] == accession
    assert item_row["state"] == BatchItemState.PENDING.value
    assert item_row["study_created"] is False
    assert list(item_row["download_work_ticket_idxs"]) == []
    assert item_row["ena_run_outcomes"] == "[]"


async def test_insert_ena_import_batch_requires_transaction(eib):
    async with eib["pool"].acquire() as conn:
        with pytest.raises(RuntimeError):
            await insert_ena_import_batch(conn, submitted_by_principal_idx=eib["principal_idx"])


async def test_insert_ena_import_batch_item_requires_transaction(eib):
    batch_idx, _ = await _new_batch_item(eib)
    async with eib["pool"].acquire() as conn:
        with pytest.raises(RuntimeError):
            await insert_ena_import_batch_item(
                conn, batch_idx=batch_idx, ena_study_accession=unique_accession("PRJNA")
            )


# ---------------------------------------------------------------------------
# update_ena_import_batch_item_state
# ---------------------------------------------------------------------------


async def test_update_ena_import_batch_item_state_sets_state_and_failure_reason(eib):
    _, item_idx = await _new_batch_item(eib)
    async with eib["pool"].acquire() as conn:
        await update_ena_import_batch_item_state(
            conn,
            item_idx=item_idx,
            state=BatchItemState.FAILED.value,
            failure_reason="boom",
        )
    row = await eib["pool"].fetchrow(
        "SELECT state, failure_reason FROM qiita.ena_import_batch_item WHERE idx = $1", item_idx
    )
    assert row["state"] == BatchItemState.FAILED.value
    assert row["failure_reason"] == "boom"


async def test_update_ena_import_batch_item_state_clears_failure_reason_by_default(eib):
    _, item_idx = await _new_batch_item(eib)
    async with eib["pool"].acquire() as conn:
        await update_ena_import_batch_item_state(
            conn, item_idx=item_idx, state=BatchItemState.FAILED.value, failure_reason="boom"
        )
        await update_ena_import_batch_item_state(
            conn, item_idx=item_idx, state=BatchItemState.RESOLVING.value
        )
    row = await eib["pool"].fetchrow(
        "SELECT state, failure_reason FROM qiita.ena_import_batch_item WHERE idx = $1", item_idx
    )
    assert row["state"] == BatchItemState.RESOLVING.value
    assert row["failure_reason"] is None


# ---------------------------------------------------------------------------
# update_ena_import_batch_item_registered
# ---------------------------------------------------------------------------


async def _seed_study(eib) -> int:
    async with eib["pool"].acquire() as conn, conn.transaction():
        row = await create_study(
            conn,
            owner_idx=eib["principal_idx"],
            created_by_idx=eib["principal_idx"],
            title="eib repo test study",
            bioproject_accession=unique_accession("PRJNA"),
        )
    eib["created_studies"].append(row["idx"])
    return row["idx"]


async def test_update_ena_import_batch_item_registered_sets_fields(eib):
    _, item_idx = await _new_batch_item(eib)
    study_idx = await _seed_study(eib)
    outcomes = [{"run_accession": "SRR1", "status": "registered", "failure_reason": None}]

    async with eib["pool"].acquire() as conn:
        await update_ena_import_batch_item_registered(
            conn,
            item_idx=item_idx,
            study_idx=study_idx,
            ena_run_outcomes=outcomes,
        )

    row = await eib["pool"].fetchrow(
        "SELECT state, study_idx, failure_reason, ena_run_outcomes"
        " FROM qiita.ena_import_batch_item WHERE idx = $1",
        item_idx,
    )
    assert row["state"] == BatchItemState.REGISTERED.value
    assert row["study_idx"] == study_idx
    assert row["failure_reason"] is None
    # jsonb does not preserve key order, so compare parsed structure.
    assert json.loads(row["ena_run_outcomes"]) == outcomes


async def test_update_ena_import_batch_item_study_created_survives_registration(eib):
    _, item_idx = await _new_batch_item(eib)
    study_idx = await _seed_study(eib)

    async with eib["pool"].acquire() as conn:
        async with conn.transaction():
            await update_ena_import_batch_item_study_created(
                conn, item_idx=item_idx, study_idx=study_idx
            )
        await update_ena_import_batch_item_registered(
            conn, item_idx=item_idx, study_idx=study_idx, ena_run_outcomes=[]
        )

    row = await eib["pool"].fetchrow(
        "SELECT study_idx, study_created FROM qiita.ena_import_batch_item WHERE idx = $1",
        item_idx,
    )
    assert row["study_idx"] == study_idx
    assert row["study_created"] is True


async def test_update_ena_import_batch_item_study_created_requires_transaction(eib):
    _, item_idx = await _new_batch_item(eib)
    study_idx = await _seed_study(eib)
    async with eib["pool"].acquire() as conn:
        with pytest.raises(RuntimeError):
            await update_ena_import_batch_item_study_created(
                conn, item_idx=item_idx, study_idx=study_idx
            )


# ---------------------------------------------------------------------------
# append_ena_import_batch_item_download_ticket
# ---------------------------------------------------------------------------


async def test_append_ena_import_batch_item_download_ticket_is_idempotent(eib):
    _, item_idx = await _new_batch_item(eib)
    async with eib["pool"].acquire() as conn:
        await append_ena_import_batch_item_download_ticket(conn, item_idx=item_idx, ticket_idx=1)
        await append_ena_import_batch_item_download_ticket(conn, item_idx=item_idx, ticket_idx=2)
        # Re-appending 1 must not duplicate it.
        await append_ena_import_batch_item_download_ticket(conn, item_idx=item_idx, ticket_idx=1)

    ticket_idxs = await eib["pool"].fetchval(
        "SELECT download_work_ticket_idxs FROM qiita.ena_import_batch_item WHERE idx = $1",
        item_idx,
    )
    assert list(ticket_idxs) == [1, 2]


# ---------------------------------------------------------------------------
# ena_import_created_study
# ---------------------------------------------------------------------------


async def test_ena_import_created_study_true_and_false(eib):
    _, item_idx = await _new_batch_item(eib)
    created_study_idx = await _seed_study(eib)
    uncreated_study_idx = await _seed_study(eib)

    async with eib["pool"].acquire() as conn, conn.transaction():
        await update_ena_import_batch_item_study_created(
            conn, item_idx=item_idx, study_idx=created_study_idx
        )

    assert await ena_import_created_study(eib["pool"], created_study_idx) is True
    assert await ena_import_created_study(eib["pool"], uncreated_study_idx) is False


# ---------------------------------------------------------------------------
# ena_import_batch_exists / fetch_ena_import_batch_items
# ---------------------------------------------------------------------------


async def test_ena_import_batch_exists(eib):
    batch_idx, _ = await _new_batch_item(eib)
    assert await ena_import_batch_exists(eib["pool"], batch_idx) is True
    assert await ena_import_batch_exists(eib["pool"], -1) is False


async def test_fetch_ena_import_batch_items_orders_by_idx(eib):
    pool = eib["pool"]
    async with pool.acquire() as conn, conn.transaction():
        batch_idx = await insert_ena_import_batch(
            conn, submitted_by_principal_idx=eib["principal_idx"]
        )
        accessions = [unique_accession("PRJNA") for _ in range(3)]
        item_idxs = [
            await insert_ena_import_batch_item(conn, batch_idx=batch_idx, ena_study_accession=a)
            for a in accessions
        ]
    eib["created_batches"].append(batch_idx)

    rows = await fetch_ena_import_batch_items(pool, batch_idx)
    assert [r["idx"] for r in rows] == item_idxs
    assert [r["ena_study_accession"] for r in rows] == accessions


# ---------------------------------------------------------------------------
# fetch_inflight_ena_import_batch_items
# ---------------------------------------------------------------------------


async def test_fetch_inflight_ena_import_batch_items_filters_by_state(eib):
    pool = eib["pool"]
    _, pending_item = await _new_batch_item(eib)
    _, registered_item = await _new_batch_item(eib)
    _, failed_item = await _new_batch_item(eib)

    async with pool.acquire() as conn:
        await update_ena_import_batch_item_state(
            conn, item_idx=registered_item, state=BatchItemState.REGISTERED.value
        )
        await update_ena_import_batch_item_state(
            conn, item_idx=failed_item, state=BatchItemState.FAILED.value
        )

    rows = await fetch_inflight_ena_import_batch_items(pool)
    inflight_idxs = {r["idx"] for r in rows}
    assert pending_item in inflight_idxs
    assert registered_item in inflight_idxs
    assert failed_item not in inflight_idxs

    for row in rows:
        if row["idx"] == pending_item:
            assert row["submitted_by_principal_idx"] == eib["principal_idx"]


# ---------------------------------------------------------------------------
# fetch_sequenced_pool_download_states / fetch_work_ticket_states_for_idxs
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def sequenced_pool_ctx(postgres_pool):
    """Seed a principal + sequenced_pool + a throwaway `sequenced_pool`-scoped
    action, for work_ticket-shaped tests. Fully independent of the real
    download-ena-study action -- the repo functions under test take
    action_id/action_version as plain parameters."""
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="eib-pool", suffix=suffix)
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    run_idx, pool_idx, sequenced_sample_idx = await seed_sequenced_sample_subtype(
        postgres_pool,
        prep_sample_idx=prep_sample_idx,
        owner_idx=principal_idx,
        sequenced_pool_item_id=f"item-{suffix}",
    )
    action_id = f"eib-test-action-{suffix}"
    version = "1.0.0"
    created = await seed_action_if_absent(
        postgres_pool, action_id=action_id, version=version, target_kind="sequenced_pool"
    )

    yield {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "sequenced_pool_idx": pool_idx,
        "sequencing_run_idx": run_idx,
        "action_id": action_id,
        "version": version,
    }

    await delete_action_if_created(
        postgres_pool, action_id=action_id, version=version, created=created
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.sequenced_sample WHERE idx = $1", sequenced_sample_idx
    )
    await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", prep_sample_idx)
    await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx)
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


async def _seed_work_ticket(ctx, *, state: str = "pending") -> int:
    return await ctx["pool"].fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, sequenced_pool_idx, state)"
        " VALUES ($1, $2, $3, 'sequenced_pool'::qiita.scope_target_kind, $4,"
        "         $5::qiita.work_ticket_state)"
        " RETURNING work_ticket_idx",
        ctx["action_id"],
        ctx["version"],
        ctx["principal_idx"],
        ctx["sequenced_pool_idx"],
        state,
    )


async def test_fetch_sequenced_pool_download_states_reports_latest_ticket(sequenced_pool_ctx):
    ctx = sequenced_pool_ctx
    older_idx = await _seed_work_ticket(ctx, state="completed")
    latest_idx = await _seed_work_ticket(ctx, state="cancelled")
    try:
        (row,) = await fetch_sequenced_pool_download_states(
            ctx["pool"],
            sequencing_run_idx=ctx["sequencing_run_idx"],
            action_id=ctx["action_id"],
            action_version=ctx["version"],
        )
        assert row["sequenced_pool_idx"] == ctx["sequenced_pool_idx"]
        assert row["has_sequenced_sample"] is True
        assert row["work_ticket_idx"] == latest_idx
        assert row["work_ticket_state"] == "cancelled"

        # Another action's tickets are not this pool's download tickets.
        (other,) = await fetch_sequenced_pool_download_states(
            ctx["pool"],
            sequencing_run_idx=ctx["sequencing_run_idx"],
            action_id="some-other-action",
            action_version=ctx["version"],
        )
        assert other["work_ticket_idx"] is None
        assert other["work_ticket_state"] is None
    finally:
        await ctx["pool"].execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = ANY($1::bigint[])",
            [older_idx, latest_idx],
        )


async def test_fetch_work_ticket_states_for_idxs(sequenced_pool_ctx):
    ctx = sequenced_pool_ctx
    pending_idx = await _seed_work_ticket(ctx, state="pending")
    async with ctx["pool"].acquire() as conn, conn.transaction():
        failed_idx = await conn.fetchval(
            "INSERT INTO qiita.work_ticket"
            " (action_id, action_version, originator_principal_idx,"
            "  scope_target_kind, sequenced_pool_idx, state,"
            "  failure_type, failure_stage, failure_reason)"
            " VALUES ($1, $2, $3, 'sequenced_pool'::qiita.scope_target_kind, $4,"
            "         'failed'::qiita.work_ticket_state,"
            "         'permanent'::qiita.failure_type,"
            "         'submission'::qiita.work_ticket_failure_stage, 'simulated')"
            " RETURNING work_ticket_idx",
            ctx["action_id"],
            ctx["version"],
            ctx["principal_idx"],
            ctx["sequenced_pool_idx"],
        )
    try:
        rows = await fetch_work_ticket_states_for_idxs(ctx["pool"], [pending_idx, failed_idx, -1])
        states = {r["work_ticket_idx"]: r["state"] for r in rows}
        assert states == {pending_idx: "pending", failed_idx: "failed"}
    finally:
        await ctx["pool"].execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = ANY($1::bigint[])",
            [pending_idx, failed_idx],
        )
