"""DB tests for the alignment lifecycle: deprecating a config, withdrawing and
restoring per-sample runs, abandoning a plan's unfinished blocks, and the refusals
each of those produces downstream.

The data plane is absent at this tier. The abandon route's DuckLake purge is
replaced by a recording stub, so these tests check what the control plane owns: the
Postgres transitions, the purge's arguments, and that a failed purge changes nothing.
"""

import json
import secrets

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.actions import ALIGN_ACTION_ID
from qiita_common.api_paths import (
    URL_ALIGNMENT_COHORT_DOGET,
    URL_ALIGNMENT_DEFINITION_ABANDON_PLAN,
    URL_ALIGNMENT_DEFINITION_SAMPLE_STATUS,
    URL_ALIGNMENT_DEFINITION_STATUS,
    URL_WORK_TICKET_RUN,
)

from qiita_control_plane.align_planner import ALIGN_ACTION_VERSION
from qiita_control_plane.fanout_dispatch import align_block_cohort, top_up_dispatch
from qiita_control_plane.repositories.alignment_definition import (
    AlignmentDefinitionDeprecated,
    mint_alignment_definition,
)
from qiita_control_plane.repositories.block import (
    AlignmentSampleInvalidated,
    add_block_members,
    create_block,
    finalize_alignment_sample,
    has_incomplete_covering_alignment_block,
    set_block_work_ticket,
)
from qiita_control_plane.testing.db_seeds import (
    delete_action_if_created,
    seed_action_if_absent,
    seed_biosample_with_sequenced_prep_sample,
)
from qiita_control_plane.testing.db_teardown import teardown_entity_graph

pytestmark = pytest.mark.db

_REASON = "aligned against a reference build later found to carry contamination"


class _RecordingPurge:
    """Stands in for `delete_alignment_block_data` in the abandon action."""

    def __init__(self, rows_deleted=7, error=None):
        self.rows_deleted = rows_deleted
        self.error = error
        self.calls: list[dict] = []

    async def __call__(self, *, alignment_idx, members, signing_key, data_plane_url):
        self.calls.append({"alignment_idx": alignment_idx, "members": members})
        if self.error is not None:
            raise self.error
        return self.rows_deleted


@pytest.fixture
def purge(monkeypatch):
    stub = _RecordingPurge()
    monkeypatch.setattr(
        "qiita_control_plane.actions.align_abandon.delete_alignment_block_data", stub
    )
    return stub


def _install_settings(app):
    from qiita_control_plane.config import Settings

    app.state.settings = Settings(
        database_url="unused", flight_signing_key=b"\x00" * 32, data_plane_url="unused"
    )


@pytest_asyncio.fixture
async def client(postgres_pool, human_admin_session):
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    saved_settings = getattr(app.state, "settings", None)
    saved_backend = getattr(app.state, "compute_backend_client", None)
    _install_settings(app)
    # /run only needs a backend client to exist; these tests never reach dispatch.
    app.state.compute_backend_client = object()
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={"Authorization": f"Bearer {human_admin_session['token']}"},
        ) as ac:
            yield ac
    finally:
        app.state.settings = saved_settings
        app.state.compute_backend_client = saved_backend


@pytest_asyncio.fixture
async def lc(postgres_pool, human_admin_session):
    """Three sequenced prep_samples, a reference, an active alignment against it with
    a gate per sample, and a `make_block` helper that mints a block plus its align
    ticket in a chosen state."""
    owner = human_admin_session["principal_idx"]
    suffix = secrets.token_hex(4)
    seeded = [
        await seed_biosample_with_sequenced_prep_sample(postgres_pool, owner_idx=owner)
        for _ in range(3)
    ]
    biosamples = [b for b, _p in seeded]
    samples = [p for _b, p in seeded]
    reference_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, created_by_idx)"
        " VALUES ($1, '1', 'sequence_reference', $2) RETURNING reference_idx",
        f"align-lifecycle-{suffix}",
        owner,
    )
    async with postgres_pool.acquire() as conn:
        row = await mint_alignment_definition(
            conn,
            params={
                "reference_idx": reference_idx,
                "aligner": "minimap2",
                "mask_idx": 1,
                "shard_ids": [0],
                "s": suffix,
            },
            principal_idx=owner,
        )
    alignment_idx = row["alignment_idx"]
    created_action = await seed_action_if_absent(
        postgres_pool, action_id=ALIGN_ACTION_ID, version=ALIGN_ACTION_VERSION
    )
    blocks: list[int] = []

    async def make_block(
        members, *, block_state, ticket_state, dispatch_held=False, alignment=None
    ) -> tuple[int, int]:
        async with postgres_pool.acquire() as conn, conn.transaction():
            block_idx = await create_block(conn)
            await add_block_members(conn, block_idx=block_idx, members=members)
        blocks.append(block_idx)
        failed = ticket_state == "failed"
        wt_idx = await postgres_pool.fetchval(
            "INSERT INTO qiita.work_ticket ("
            "  action_id, action_version, originator_principal_idx, scope_target_kind,"
            "  block_idx, alignment_idx, state, dispatch_held,"
            "  failure_type, failure_stage, failure_step_name, failure_reason"
            ") VALUES ($1, $2, $3, 'block', $4, $5, $6::qiita.work_ticket_state, $7,"
            "          $8::qiita.failure_type, $9::qiita.work_ticket_failure_stage, $10, $11)"
            " RETURNING work_ticket_idx",
            ALIGN_ACTION_ID,
            ALIGN_ACTION_VERSION,
            owner,
            block_idx,
            alignment or alignment_idx,
            ticket_state,
            dispatch_held,
            "permanent" if failed else None,
            "step_run" if failed else None,
            "align_sharded" if failed else None,
            "step timed out" if failed else None,
        )
        async with postgres_pool.acquire() as conn, conn.transaction():
            await set_block_work_ticket(conn, block_idx=block_idx, work_ticket_idx=wt_idx)
        await postgres_pool.execute(
            "UPDATE qiita.block SET state = $2 WHERE block_idx = $1", block_idx, block_state
        )
        return block_idx, wt_idx

    async def set_gate(prep_sample_idx, state, alignment=None):
        await postgres_pool.execute(
            "INSERT INTO qiita.alignment_sample (alignment_idx, prep_sample_idx, state)"
            " VALUES ($1, $2, $3)"
            " ON CONFLICT (alignment_idx, prep_sample_idx) DO UPDATE SET state = $3",
            alignment or alignment_idx,
            prep_sample_idx,
            state,
        )

    yield {
        "pool": postgres_pool,
        "owner": owner,
        "samples": samples,
        "reference_idx": reference_idx,
        "alignment_idx": alignment_idx,
        "make_block": make_block,
        "set_gate": set_gate,
    }

    # The entity sweep owns the gate, cover-map and QC rows keyed on these
    # prep_samples; what it does not reach goes after it.
    await teardown_entity_graph(
        postgres_pool, study_idxs=[], biosample_idxs=biosamples, prep_sample_idxs=samples
    )
    if blocks:
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE block_idx = ANY($1::bigint[])", blocks
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.block WHERE block_idx = ANY($1::bigint[])", blocks
        )
    await postgres_pool.execute(
        "DELETE FROM qiita.alignment_definition WHERE (params->>'reference_idx')::bigint = $1",
        reference_idx,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    await delete_action_if_created(
        postgres_pool,
        action_id=ALIGN_ACTION_ID,
        version=ALIGN_ACTION_VERSION,
        created=created_action,
    )


async def _gate(pool, alignment_idx, prep_sample_idx):
    return await pool.fetchval(
        "SELECT state FROM qiita.alignment_sample"
        " WHERE alignment_idx = $1 AND prep_sample_idx = $2",
        alignment_idx,
        prep_sample_idx,
    )


async def _deprecate(client, alignment_idx, **extra):
    return await client.patch(
        URL_ALIGNMENT_DEFINITION_STATUS.format(alignment_idx=alignment_idx),
        json={"status": "deprecated", "reason": _REASON, **extra},
    )


# ---------------------------------------------------------------------------
# Config lifecycle
# ---------------------------------------------------------------------------


async def test_deprecate_records_provenance_and_restore_clears_it(client, lc):
    resp = await _deprecate(client, lc["alignment_idx"])
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "deprecated"
    assert body["deprecation_reason"] == _REASON
    assert body["deprecated_by_idx"] == lc["owner"]
    assert body["deprecated_at"] is not None

    resp = await client.patch(
        URL_ALIGNMENT_DEFINITION_STATUS.format(alignment_idx=lc["alignment_idx"]),
        json={"status": "active"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "active"
    assert body["deprecation_reason"] is None
    assert body["deprecated_by_idx"] is None


async def test_deprecation_waits_for_unfinished_tickets(client, lc):
    """A held ticket is unfinished too: the fan-out would release it and its block
    would write rows under a void config. Deprecation names the tickets, and goes
    through once they are terminal. A terminal ticket never blocks it."""
    a, b, _c = lc["samples"]
    await lc["make_block"]([(a, 1, 100)], block_state="completed", ticket_state="completed")
    _block, held = await lc["make_block"](
        [(b, 1, 100)], block_state="pending", ticket_state="pending", dispatch_held=True
    )
    resp = await _deprecate(client, lc["alignment_idx"])
    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"]["work_ticket_idx"] == [held]
    status = await lc["pool"].fetchval(
        "SELECT status FROM qiita.alignment_definition WHERE alignment_idx = $1",
        lc["alignment_idx"],
    )
    assert status == "active"

    await lc["pool"].execute(
        "UPDATE qiita.work_ticket SET state = 'cancelled' WHERE work_ticket_idx = $1", held
    )
    assert (await _deprecate(client, lc["alignment_idx"])).status_code == 200


async def test_a_plan_cannot_create_tickets_under_a_deprecated_alignment(lc):
    """The ticket writers re-check the status under a share lock in the transaction
    that creates the tickets, so a deprecation landing after the mint still stops
    them."""
    from qiita_control_plane.repositories.alignment_definition import lock_active_alignment

    await lc["pool"].execute(
        "UPDATE qiita.alignment_definition SET status = 'deprecated', deprecated_at = now(),"
        "       deprecated_by_idx = $2, deprecation_reason = 'late'"
        " WHERE alignment_idx = $1",
        lc["alignment_idx"],
        lc["owner"],
    )
    with pytest.raises(AlignmentDefinitionDeprecated, match="late"):
        async with lc["pool"].acquire() as conn, conn.transaction():
            await lock_active_alignment(conn, lc["alignment_idx"])


async def test_deprecate_without_reason_is_422(client, lc):
    resp = await client.patch(
        URL_ALIGNMENT_DEFINITION_STATUS.format(alignment_idx=lc["alignment_idx"]),
        json={"status": "deprecated"},
    )
    assert resp.status_code == 422


async def test_superseded_by_must_name_another_alignment(client, lc):
    resp = await _deprecate(client, lc["alignment_idx"], superseded_by=lc["alignment_idx"])
    assert resp.status_code == 422
    resp = await _deprecate(client, lc["alignment_idx"], superseded_by=2_000_000_000)
    assert resp.status_code == 422
    assert "superseded_by" in resp.json()["detail"]


async def test_status_unknown_alignment_is_404(client):
    from qiita_control_plane.routes._helpers import ALIGNMENT_NOT_FOUND_DETAIL

    resp = await _deprecate(client, 2_000_000_000)
    assert resp.status_code == 404
    assert resp.json()["detail"] == ALIGNMENT_NOT_FOUND_DETAIL


async def test_lifecycle_routes_need_the_lifecycle_scope(postgres_pool, lc, wet_lab_admin_session):
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {wet_lab_admin_session['token']}"},
    ) as ac:
        status = await _deprecate(ac, lc["alignment_idx"])
        sample = await ac.patch(
            URL_ALIGNMENT_DEFINITION_SAMPLE_STATUS.format(alignment_idx=lc["alignment_idx"]),
            json={"prep_sample_idx": [lc["samples"][0]], "state": "invalidated", "reason": "x"},
        )
        abandon = await ac.post(
            URL_ALIGNMENT_DEFINITION_ABANDON_PLAN.format(alignment_idx=lc["alignment_idx"])
        )
    for resp in (status, sample, abandon):
        assert resp.status_code == 403
        assert "alignment_definition:lifecycle" in resp.json()["detail"]


async def test_mint_refuses_a_deprecated_config(client, lc):
    assert (await _deprecate(client, lc["alignment_idx"])).status_code == 200
    params = await lc["pool"].fetchval(
        "SELECT params FROM qiita.alignment_definition WHERE alignment_idx = $1",
        lc["alignment_idx"],
    )
    async with lc["pool"].acquire() as conn:
        with pytest.raises(AlignmentDefinitionDeprecated, match=_REASON):
            await mint_alignment_definition(
                conn, params=json.loads(params), principal_idx=lc["owner"]
            )


# ---------------------------------------------------------------------------
# Per-sample lifecycle
# ---------------------------------------------------------------------------


async def test_sample_status_invalidates_completed_and_skips_pending(client, lc):
    a, b, c = lc["samples"]
    await lc["set_gate"](a, "completed")
    await lc["set_gate"](b, "pending")
    resp = await client.patch(
        URL_ALIGNMENT_DEFINITION_SAMPLE_STATUS.format(alignment_idx=lc["alignment_idx"]),
        json={"prep_sample_idx": [a, b, c], "state": "invalidated", "reason": _REASON},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["updated"] == [a]
    assert body["skipped_pending"] == [b]
    assert body["not_found"] == [c]
    row = await lc["pool"].fetchrow(
        "SELECT state, invalidation_reason, invalidated_by_idx FROM qiita.alignment_sample"
        " WHERE alignment_idx = $1 AND prep_sample_idx = $2",
        lc["alignment_idx"],
        a,
    )
    assert dict(row) == {
        "state": "invalidated",
        "invalidation_reason": _REASON,
        "invalidated_by_idx": lc["owner"],
    }

    resp = await client.patch(
        URL_ALIGNMENT_DEFINITION_SAMPLE_STATUS.format(alignment_idx=lc["alignment_idx"]),
        json={"prep_sample_idx": [a], "state": "completed"},
    )
    assert resp.json()["updated"] == [a]
    assert await _gate(lc["pool"], lc["alignment_idx"], a) == "completed"


async def test_finalize_refuses_an_invalidated_gate(lc):
    a = lc["samples"][0]
    await lc["set_gate"](a, "completed")
    await lc["pool"].execute(
        "UPDATE qiita.alignment_sample SET state = 'invalidated', invalidated_at = now(),"
        "       invalidated_by_idx = $3, invalidation_reason = 'r'"
        " WHERE alignment_idx = $1 AND prep_sample_idx = $2",
        lc["alignment_idx"],
        a,
        lc["owner"],
    )
    async with lc["pool"].acquire() as conn, conn.transaction():
        with pytest.raises(AlignmentSampleInvalidated):
            await finalize_alignment_sample(
                conn, alignment_idx=lc["alignment_idx"], prep_sample_idx=a
            )


# ---------------------------------------------------------------------------
# Abandon plan
# ---------------------------------------------------------------------------


async def test_abandon_clears_pending_gates_and_keeps_completed(client, lc, purge):
    """Sample A completed under one block. Sample B is split: half in a completed
    block, half in a failed one. Sample C sits in a held block that never ran. The
    failed and held blocks are abandoned, the held ticket cancelled, and B's and C's
    pending gates cleared after a purge covering every block that touched them —
    B's completed half included, since it registered rows for a sample now re-planned."""
    pool, alignment_idx = lc["pool"], lc["alignment_idx"]
    a, b, c = lc["samples"]
    done_a, _ = await lc["make_block"](
        [(a, 1, 100)], block_state="completed", ticket_state="completed"
    )
    done_b, _ = await lc["make_block"](
        [(b, 1, 50)], block_state="completed", ticket_state="completed"
    )
    failed_b, failed_wt = await lc["make_block"](
        [(b, 51, 100)], block_state="pending", ticket_state="failed"
    )
    held_c, held_wt = await lc["make_block"](
        [(c, 1, 100)], block_state="pending", ticket_state="pending", dispatch_held=True
    )
    await lc["set_gate"](a, "completed")
    await lc["set_gate"](b, "pending")
    await lc["set_gate"](c, "pending")

    resp = await client.post(
        URL_ALIGNMENT_DEFINITION_ABANDON_PLAN.format(alignment_idx=alignment_idx)
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "alignment_idx": alignment_idx,
        "abandoned_block_idx": sorted([failed_b, held_c]),
        "cancelled_work_ticket_idx": [held_wt],
        "cleared_prep_sample_idx": sorted([b, c]),
        "rows_deleted": 7,
    }
    assert purge.calls == [
        {
            "alignment_idx": alignment_idx,
            "members": sorted(
                [
                    {"prep_sample_idx": b, "sequence_idx_start": 1, "sequence_idx_stop": 50},
                    {"prep_sample_idx": b, "sequence_idx_start": 51, "sequence_idx_stop": 100},
                    {"prep_sample_idx": c, "sequence_idx_start": 1, "sequence_idx_stop": 100},
                ],
                key=lambda m: (m["prep_sample_idx"], m["sequence_idx_start"]),
            ),
        }
    ]
    states = dict(
        await pool.fetch(
            "SELECT block_idx, state FROM qiita.block WHERE block_idx = ANY($1::bigint[])",
            [done_a, done_b, failed_b, held_c],
        )
    )
    assert states == {
        done_a: "completed",
        done_b: "completed",
        failed_b: "abandoned",
        held_c: "abandoned",
    }
    tickets = dict(
        await pool.fetch(
            "SELECT work_ticket_idx, state::text FROM qiita.work_ticket"
            " WHERE work_ticket_idx = ANY($1::bigint[])",
            [failed_wt, held_wt],
        )
    )
    assert tickets == {failed_wt: "failed", held_wt: "cancelled"}
    assert await _gate(pool, alignment_idx, a) == "completed"
    assert await _gate(pool, alignment_idx, b) is None
    assert await _gate(pool, alignment_idx, c) is None

    # Nothing unfinished is left, so a second call changes nothing in Postgres. It
    # purges the same footprint again: the purge set is derived from state so that a
    # failed purge can be retried, and the purge is idempotent.
    again = await client.post(
        URL_ALIGNMENT_DEFINITION_ABANDON_PLAN.format(alignment_idx=alignment_idx)
    )
    assert again.status_code == 200
    assert again.json()["abandoned_block_idx"] == []
    assert again.json()["cleared_prep_sample_idx"] == []
    assert purge.calls == [purge.calls[0], purge.calls[0]]

    # Once a prep_sample is re-planned it has a gate again, and drops out of the purge.
    await lc["set_gate"](c, "pending")
    await client.post(URL_ALIGNMENT_DEFINITION_ABANDON_PLAN.format(alignment_idx=alignment_idx))
    assert {m["prep_sample_idx"] for m in purge.calls[-1]["members"]} == {b}


async def test_abandon_refuses_while_a_released_block_runs(client, lc, purge):
    a = lc["samples"][0]
    block, wt = await lc["make_block"](
        [(a, 1, 100)], block_state="processing", ticket_state="processing"
    )
    await lc["set_gate"](a, "pending")
    resp = await client.post(
        URL_ALIGNMENT_DEFINITION_ABANDON_PLAN.format(alignment_idx=lc["alignment_idx"])
    )
    assert resp.status_code == 409
    assert resp.json()["detail"]["work_ticket_idx"] == [wt]
    assert purge.calls == []
    assert await _gate(lc["pool"], lc["alignment_idx"], a) == "pending"
    assert (
        await lc["pool"].fetchval("SELECT state FROM qiita.block WHERE block_idx = $1", block)
        == "processing"
    )


async def test_a_failed_purge_leaves_the_abandon_committed_and_is_retried(client, lc, purge):
    """The purge runs after the commit, so its failure leaves the plan abandoned and
    the gates cleared (no gate a redrive could complete over purged rows), and a
    second call purges what the first could not."""
    import pyarrow.flight as flight

    purge.error = flight.FlightUnavailableError("data plane down")
    a = lc["samples"][0]
    block, _wt = await lc["make_block"]([(a, 1, 100)], block_state="pending", ticket_state="failed")
    await lc["set_gate"](a, "pending")
    url = URL_ALIGNMENT_DEFINITION_ABANDON_PLAN.format(alignment_idx=lc["alignment_idx"])
    resp = await client.post(url)
    assert resp.status_code == 502
    assert "again" in resp.json()["detail"]
    assert await _gate(lc["pool"], lc["alignment_idx"], a) is None
    assert (
        await lc["pool"].fetchval("SELECT state FROM qiita.block WHERE block_idx = $1", block)
        == "abandoned"
    )

    purge.error = None
    retry = await client.post(url)
    assert retry.status_code == 200, retry.text
    assert retry.json()["rows_deleted"] == 7
    assert purge.calls[-1]["members"] == [
        {"prep_sample_idx": a, "sequence_idx_start": 1, "sequence_idx_stop": 100}
    ]


async def test_abandon_unknown_alignment_is_404(client, purge):
    resp = await client.post(
        URL_ALIGNMENT_DEFINITION_ABANDON_PLAN.format(alignment_idx=2_000_000_000)
    )
    assert resp.status_code == 404


async def test_an_abandoned_block_owes_its_samples_nothing(client, lc, purge):
    """After abandon, a re-planned block for the same sample finalizes on its own:
    the abandoned sibling is not read as an incomplete cover."""
    pool, alignment_idx = lc["pool"], lc["alignment_idx"]
    a = lc["samples"][0]
    await lc["make_block"]([(a, 1, 100)], block_state="pending", ticket_state="failed")
    await lc["set_gate"](a, "pending")
    assert (
        await client.post(URL_ALIGNMENT_DEFINITION_ABANDON_PLAN.format(alignment_idx=alignment_idx))
    ).status_code == 200
    await lc["make_block"]([(a, 1, 100)], block_state="completed", ticket_state="completed")
    async with pool.acquire() as conn:
        assert not await has_incomplete_covering_alignment_block(
            conn, alignment_idx=alignment_idx, prep_sample_idx=a
        )


async def test_an_abandoned_failed_ticket_does_not_fail_stop_the_replan(client, lc, purge):
    pool, alignment_idx = lc["pool"], lc["alignment_idx"]
    a, b, _c = lc["samples"]
    await lc["make_block"]([(a, 1, 100)], block_state="pending", ticket_state="failed")
    await lc["set_gate"](a, "pending")
    assert (
        await client.post(URL_ALIGNMENT_DEFINITION_ABANDON_PLAN.format(alignment_idx=alignment_idx))
    ).status_code == 200
    _block, held = await lc["make_block"](
        [(a, 1, 100)], block_state="pending", ticket_state="pending", dispatch_held=True
    )
    dispatched: list[int] = []
    released = await top_up_dispatch(
        pool, align_block_cohort(alignment_idx), max_inflight=8, dispatch_cb=dispatched.append
    )
    assert released == [held]
    # The control: a failed ticket whose block was NOT abandoned still fail-stops.
    await lc["make_block"]([(b, 1, 100)], block_state="pending", ticket_state="failed")
    await lc["make_block"](
        [(b, 1, 100)], block_state="pending", ticket_state="pending", dispatch_held=True
    )
    assert (
        await top_up_dispatch(
            pool, align_block_cohort(alignment_idx), max_inflight=8, dispatch_cb=dispatched.append
        )
        == []
    )


# ---------------------------------------------------------------------------
# Consumers
# ---------------------------------------------------------------------------


async def test_redrive_refuses_an_abandoned_block_and_a_deprecated_alignment(
    client, lc, purge, monkeypatch
):
    # The shared seeded align action has no steps, which the definition load before
    # the guards rejects; the guards do not read the definition.
    async def _any_definition(*_a, **_k):
        return object()

    monkeypatch.setattr("qiita_control_plane.routes.work_ticket._fetch_action", _any_definition)
    a, b, _c = lc["samples"]
    _block, abandoned_wt = await lc["make_block"](
        [(a, 1, 100)], block_state="pending", ticket_state="failed"
    )
    await lc["set_gate"](a, "pending")
    assert (
        await client.post(
            URL_ALIGNMENT_DEFINITION_ABANDON_PLAN.format(alignment_idx=lc["alignment_idx"])
        )
    ).status_code == 200
    resp = await client.post(URL_WORK_TICKET_RUN.format(work_ticket_idx=abandoned_wt))
    assert resp.status_code == 409
    assert "abandoned" in resp.json()["detail"]

    _block, failed_wt = await lc["make_block"](
        [(b, 1, 100)], block_state="pending", ticket_state="failed"
    )
    assert (await _deprecate(client, lc["alignment_idx"])).status_code == 200
    resp = await client.post(URL_WORK_TICKET_RUN.format(work_ticket_idx=failed_wt))
    assert resp.status_code == 409
    assert "deprecated" in resp.json()["detail"]


async def test_cohort_doget_refuses_a_deprecated_alignment(client, lc):
    a = lc["samples"][0]
    await lc["set_gate"](a, "completed")
    assert (await _deprecate(client, lc["alignment_idx"])).status_code == 200
    resp = await client.post(
        URL_ALIGNMENT_COHORT_DOGET.format(alignment_idx=lc["alignment_idx"]),
        json={"prep_sample_idx": [a], "columns": ["alignment_idx"]},
    )
    assert resp.status_code == 409, resp.text
    assert _REASON in resp.json()["detail"]


async def test_cohort_doget_refuses_an_invalidated_sample(client, lc):
    a = lc["samples"][0]
    await lc["set_gate"](a, "completed")
    resp = await client.patch(
        URL_ALIGNMENT_DEFINITION_SAMPLE_STATUS.format(alignment_idx=lc["alignment_idx"]),
        json={"prep_sample_idx": [a], "state": "invalidated", "reason": _REASON},
    )
    assert resp.status_code == 200
    resp = await client.post(
        URL_ALIGNMENT_COHORT_DOGET.format(alignment_idx=lc["alignment_idx"]),
        json={"prep_sample_idx": [a], "columns": ["alignment_idx"]},
    )
    assert resp.status_code == 422, resp.text


async def test_qc_verdict_ignores_a_deprecated_alignment(client, lc):
    a = lc["samples"][0]
    await lc["set_gate"](a, "completed")
    _block, wt = await lc["make_block"](
        [(a, 1, 100)], block_state="completed", ticket_state="completed"
    )
    await lc["pool"].execute(
        "INSERT INTO qiita.prep_sample_qc_check"
        ' (prep_sample_idx, "check", severity, alignment_idx, reason, work_ticket_idx)'
        " VALUES ($1, 'classifier', 'fail', $2, 'lifecycle_test', $3)",
        a,
        lc["alignment_idx"],
        wt,
    )
    verdict = "SELECT severity FROM qiita.qc_verdict WHERE prep_sample_idx = $1"
    assert await lc["pool"].fetchval(verdict, a) == "fail"
    assert (await _deprecate(client, lc["alignment_idx"])).status_code == 200
    assert await lc["pool"].fetchval(verdict, a) is None
