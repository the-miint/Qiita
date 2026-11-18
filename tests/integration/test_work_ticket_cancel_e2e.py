"""POST /api/v1/work-ticket/cancel across the CP → CO boundary, for a failed ticket.

The control plane's route flips the ticket in Postgres, then reaps through a real
`ComputeBackendClient` calling the orchestrator's `POST /api/v1/step/cancel` (the
orchestrator app reached over httpx `ASGITransport`, backed by `LocalBackend`). The
ticket's scratch workspace does not exist, as after a scratch reclaim.
"""

import uuid

import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.actions import READ_MASK_ACTION_ID
from qiita_common.api_paths import URL_WORK_TICKET_CANCEL
from qiita_common.compute_backend_client import ComputeBackendClient
from qiita_common.models import (
    FailureType,
    ScopeTargetKind,
    WorkTicketFailureStage,
    WorkTicketState,
)
from qiita_compute_orchestrator.backends.local import LocalBackend
from qiita_compute_orchestrator.main import app as orch_app
from qiita_control_plane.testing.db_seeds import (
    delete_action_if_created,
    seed_action_if_absent,
    seed_bare_reference,
)

_CP_TO_CO_TOKEN = "work-ticket-cancel-e2e-token"
_VERSION = "0.0.0-cancel-e2e"


@pytest_asyncio.fixture
async def cancel_world(postgres_pool, human_admin_session, tmp_path):
    """The CP app wired to the orchestrator app through a real client, and a failed
    reference-scoped ticket whose workspace root does not exist."""
    from qiita_control_plane.config import Settings
    from qiita_control_plane.dispatch import build_dispatch_semaphore
    from qiita_control_plane.main import app as cp_app

    orch_app.state.backend = LocalBackend()
    orch_app.state.cp_to_co_token = _CP_TO_CO_TOKEN

    workspace_root = tmp_path / "ticket"
    cp_app.state.pool = postgres_pool
    cp_app.state.settings = Settings(
        database_url="unused",
        flight_signing_key=b"\x00" * 32,
        data_plane_url="unused",
        path_scratch_ticket=workspace_root,
    )
    cp_app.state.running_dispatches = set()
    cp_app.state.dispatch_semaphore = build_dispatch_semaphore()
    saved_client = getattr(cp_app.state, "compute_backend_client", None)

    created = await seed_action_if_absent(
        postgres_pool,
        action_id=READ_MASK_ACTION_ID,
        version=_VERSION,
        target_kind=ScopeTargetKind.REFERENCE.value,
    )
    reference_idx = await seed_bare_reference(postgres_pool, label=f"cancel-e2e-{uuid.uuid4()}")
    ticket = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        "  (action_id, action_version, originator_principal_idx, scope_target_kind,"
        "   reference_idx, state, failure_type, failure_stage, failure_reason)"
        " VALUES ($1, $2, $3, $4::qiita.scope_target_kind, $5,"
        "         $6::qiita.work_ticket_state, $7::qiita.failure_type,"
        "         $8::qiita.work_ticket_failure_stage, $9)"
        " RETURNING work_ticket_idx",
        READ_MASK_ACTION_ID,
        _VERSION,
        human_admin_session["principal_idx"],
        ScopeTargetKind.REFERENCE.value,
        reference_idx,
        WorkTicketState.FAILED.value,
        FailureType.PERMANENT.value,
        WorkTicketFailureStage.FINALIZE.value,
        "failed in a test",
    )

    async with AsyncClient(
        transport=ASGITransport(app=orch_app),
        base_url="http://orch",
        headers={"Authorization": f"Bearer {_CP_TO_CO_TOKEN}"},
    ) as orch_http:
        cp_app.state.compute_backend_client = ComputeBackendClient(
            "http://orch", api_token=_CP_TO_CO_TOKEN, http_client=orch_http
        )
        async with AsyncClient(
            transport=ASGITransport(app=cp_app),
            base_url="http://test",
            headers={"Authorization": f"Bearer {human_admin_session['token']}"},
        ) as cp:
            yield {
                "cp": cp,
                "pool": postgres_pool,
                "ticket": ticket,
                "workspace_root": workspace_root,
            }

    cp_app.state.compute_backend_client = saved_client
    await postgres_pool.execute("DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", ticket)
    await postgres_pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    await delete_action_if_created(
        postgres_pool, action_id=READ_MASK_ACTION_ID, version=_VERSION, created=created
    )


async def test_cancel_a_failed_ticket_flips_it_and_reaps_through_the_orchestrator(cancel_world):
    w = cancel_world
    assert not w["workspace_root"].exists()

    resp = await w["cp"].post(URL_WORK_TICKET_CANCEL, json={"work_ticket_idxs": [w["ticket"]]})

    assert resp.status_code == 200, resp.text
    (r,) = resp.json()["results"]
    assert (r["previous_state"], r["state"], r["cancelled"]) == (
        WorkTicketState.FAILED.value,
        WorkTicketState.CANCELLED.value,
        True,
    )
    # The orchestrator answered the reap: LocalBackend has no job to scancel, and a
    # failed reap would set reap_error instead.
    assert r["reap_error"] is None
    assert r["cancelled_job_ids"] == []
    row = await w["pool"].fetchrow(
        "SELECT state, failure_type, failure_reason FROM qiita.work_ticket"
        " WHERE work_ticket_idx = $1",
        w["ticket"],
    )
    assert dict(row) == {
        "state": WorkTicketState.CANCELLED.value,
        "failure_type": None,
        "failure_reason": None,
    }
