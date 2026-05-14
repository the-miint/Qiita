"""DB-bound integration tests for /api/v1/work-ticket/*.

Covers submission (with the implicit dispatch fired by the route), the
state-aware /run override, and the dispatch-side recovery helper. The
asyncio task lifecycle itself is covered by tests/test_dispatch.py —
here we patch `_run_and_log` to a no-op so the route returns 202
without actually executing a workflow.
"""

from __future__ import annotations

import json
import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_WORK_TICKET_PREFIX, URL_WORK_TICKET_RUN
from qiita_common.auth_constants import Scope, SystemRole
from qiita_common.models import WorkTicketState

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def stub_compute_backend_client():
    """A non-None object stand-in. The route only checks that
    `app.state.compute_backend_client is not None`; the actual HTTP
    work happens inside `_run_and_log` (which we patch to a no-op).
    Anything truthy works."""
    return object()


@pytest.fixture(autouse=True)
async def _patch_run_and_log(monkeypatch):
    """Replace `_run_and_log` with a no-op so route tests don't fan out
    to the orchestrator. Each test that wants to verify dispatch
    happened can re-patch with a recorder."""

    async def _noop(_app, _idx):
        return None

    monkeypatch.setattr("qiita_control_plane.dispatch._run_and_log", _noop)


@pytest.fixture
async def wt_client(postgres_pool, stub_compute_backend_client):
    """App configured for work-ticket route tests."""
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.oidc_verifier = None
    app.state.settings = Settings(
        database_url="unused",
        hmac_secret_key=b"\x00" * 32,
        data_plane_url="unused",
    )
    app.state.compute_backend_client = stub_compute_backend_client
    app.state.running_dispatches = set()

    created_principals: list[int] = []
    created_tickets: list[int] = []
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        ac._created_principals = created_principals  # type: ignore[attr-defined]
        ac._created_tickets = created_tickets  # type: ignore[attr-defined]
        yield ac

    # Cleanup. work_tickets first (FK to action / reference / principal).
    if created_tickets:
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = ANY($1::bigint[])",
            created_tickets,
        )
    if created_principals:
        async with postgres_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "ALTER TABLE qiita.auth_event DISABLE TRIGGER auth_event_no_delete"
                )
                try:
                    for table in ("api_token", "user_identity", "user", "service_account"):
                        await conn.execute(
                            f"DELETE FROM qiita.{table} WHERE principal_idx = ANY($1::bigint[])",
                            created_principals,
                        )
                    await conn.execute(
                        "DELETE FROM qiita.auth_event"
                        " WHERE principal_idx = ANY($1::bigint[])"
                        "    OR actor_principal_idx = ANY($1::bigint[])",
                        created_principals,
                    )
                    await conn.execute(
                        "DELETE FROM qiita.principal WHERE idx = ANY($1::bigint[])",
                        created_principals,
                    )
                finally:
                    await conn.execute(
                        "ALTER TABLE qiita.auth_event ENABLE TRIGGER auth_event_no_delete"
                    )

    # Drain any tasks the routes scheduled (the no-op `_run_and_log`
    # finishes quickly, but make sure none leak across tests).
    import asyncio

    pending = list(app.state.running_dispatches)
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


@pytest.fixture
async def admin_token(postgres_pool, wt_client):
    """A throwaway system_admin with the scopes our test action requires."""
    from qiita_control_plane.auth.token import mint_api_token

    email = f"wt-admin-{uuid.uuid4()}@example.com"
    pidx = await postgres_pool.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, 1) RETURNING idx",
        email,
        SystemRole.SYSTEM_ADMIN,
    )
    await postgres_pool.execute(
        "INSERT INTO qiita.user (principal_idx, email, affiliation, address, phone)"
        " VALUES ($1, $2, 'X', 'Y', 'Z')",
        pidx,
        email,
    )
    wt_client._created_principals.append(pidx)
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=pidx,
        label="wt-admin",
        scopes=[
            Scope.SELF_PROFILE,
            Scope.SELF_TOKEN,
            Scope.REFERENCE_READ,
            Scope.REFERENCE_WRITE,
            Scope.ADMIN_USER,
        ],
    )
    return plaintext, pidx


@pytest.fixture
async def regular_token(postgres_pool, wt_client):
    """A throwaway 'user'-role human; for audience-mismatch tests."""
    from qiita_control_plane.auth.token import mint_api_token

    email = f"wt-user-{uuid.uuid4()}@example.com"
    pidx = await postgres_pool.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, 1) RETURNING idx",
        email,
        SystemRole.USER,
    )
    await postgres_pool.execute(
        "INSERT INTO qiita.user (principal_idx, email, affiliation, address, phone)"
        " VALUES ($1, $2, 'X', 'Y', 'Z')",
        pidx,
        email,
    )
    wt_client._created_principals.append(pidx)
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=pidx,
        label="wt-user",
        scopes=[Scope.SELF_PROFILE, Scope.SELF_TOKEN, Scope.REFERENCE_READ],
    )
    return plaintext, pidx


@pytest.fixture
async def reference_idx(postgres_pool, admin_token):
    """A 'pending' reference owned by the admin principal — the
    scope_target our submission tests aim at. (`pending` here is a
    qiita.reference_status value, distinct from the work_ticket_state
    enum tracked elsewhere in this file.)"""
    _, admin_idx = admin_token
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', 'pending', $2)"
        " RETURNING reference_idx",
        f"wt-test-{uuid.uuid4()}",
        admin_idx,
    )
    yield idx
    # work_ticket has FK RESTRICT on reference_idx — drop dependents first.
    await postgres_pool.execute("DELETE FROM qiita.work_ticket WHERE reference_idx = $1", idx)
    await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", idx)


_TEST_STEPS = [
    {
        "kind": "step",
        "name": "noop",
        "step_type": "singleton",
        "container": "qiita/noop:1.0.0",
        "inputs": [],
        "outputs": [],
        "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    }
]


async def _seed_action(postgres_pool, *, context_schema: dict) -> tuple[str, str]:
    """Insert an enabled, reference-targeting action with the given
    context_schema. Returns (action_id, version). Caller is responsible
    for cleanup — fixtures wrap this with teardown."""
    action_id = "wt-test-action"
    version = f"v-{uuid.uuid4()}"
    await postgres_pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, scopes, audience,"
        "  context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling,"
        "  success_status, failure_status"
        ") VALUES ($1, $2, 'reference', $3::text[], $4::jsonb,"
        "  $5::jsonb, $6::jsonb, 1, 1, '1 minute', $7, $8)",
        action_id,
        version,
        [Scope.REFERENCE_WRITE.value],
        json.dumps({"service": False, "human_roles": [SystemRole.SYSTEM_ADMIN.value]}),
        json.dumps(context_schema),
        json.dumps(_TEST_STEPS),
        "active",
        "failed",
    )
    return action_id, version


async def _drop_action(postgres_pool, action_id: str, version: str) -> None:
    """Delete dependent work_tickets first (FK RESTRICT), then the action."""
    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        action_id,
        version,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
        action_id,
        version,
    )


@pytest.fixture
async def reference_action(postgres_pool):
    """An enabled, reference-targeting action requiring system_admin and
    reference:write, with an empty context_schema (accepts any object)
    — matches the admin_token fixture's grant."""
    action_id, version = await _seed_action(postgres_pool, context_schema={})
    yield action_id, version
    await _drop_action(postgres_pool, action_id, version)


@pytest.fixture
async def reference_action_with_schema(postgres_pool):
    """Same shape as `reference_action` but the context_schema requires a
    `sample_count: integer` — drives the 422-on-invalid-context tests."""
    schema = {
        "type": "object",
        "properties": {"sample_count": {"type": "integer"}},
        "required": ["sample_count"],
    }
    action_id, version = await _seed_action(postgres_pool, context_schema=schema)
    yield action_id, version
    await _drop_action(postgres_pool, action_id, version)


# Helper: the standard request body shape.
def _body(action_id: str, version: str, reference_idx: int, **overrides) -> dict:
    base = {
        "action_id": action_id,
        "action_version": version,
        "scope_target": {"kind": "reference", "reference_idx": reference_idx},
        "action_context": {},
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# POST /api/v1/work-ticket
# ---------------------------------------------------------------------------


async def test_submit_happy_path(
    wt_client, postgres_pool, admin_token, reference_action, reference_idx
):
    """Successful submission returns 202 + ticket_idx; ticket lands in
    PENDING and dispatch task is registered on app.state."""
    token, _ = admin_token
    action_id, version = reference_action
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json=_body(action_id, version, reference_idx),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text
    body = resp.json()
    idx = body["work_ticket_idx"]
    wt_client._created_tickets.append(idx)
    assert body["state"] == WorkTicketState.PENDING.value

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", idx
    )
    assert state == WorkTicketState.PENDING.value


async def test_submit_unknown_action_returns_404(wt_client, admin_token, reference_idx):
    token, _ = admin_token
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json=_body("does-not-exist", "v0", reference_idx),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 404
    assert "not found" in resp.text


async def test_submit_deprecated_action_returns_410(
    wt_client, postgres_pool, admin_token, reference_action, reference_idx
):
    """An action whose row exists but is `enabled=false` returns HTTP
    410 Gone — distinct from the 404 returned for an unknown
    action_id+version. The distinction lets clients that retry on 404
    stop trying a permanently-deprecated version."""
    from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX

    from qiita_control_plane.actions.sync import AUTO_DEPRECATE_REASON

    token, _ = admin_token
    action_id, version = reference_action

    await postgres_pool.execute(
        "UPDATE qiita.action"
        "   SET enabled = false,"
        "       disabled_at = NOW(),"
        "       disabled_reason = $4,"
        "       disabled_by_idx = $3"
        " WHERE action_id = $1 AND version = $2",
        action_id,
        version,
        SYSTEM_PRINCIPAL_IDX,
        AUTO_DEPRECATE_REASON,
    )

    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json=_body(action_id, version, reference_idx),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 410
    assert "deprecated" in resp.text


async def test_submit_wrong_role_returns_403(
    wt_client, regular_token, reference_action, reference_idx
):
    """A 'user'-role principal calling a system_admin-only action gets 403."""
    token, _ = regular_token
    action_id, version = reference_action
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json=_body(action_id, version, reference_idx),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 403
    assert "audience" in resp.json()["detail"]


async def test_submit_scope_target_kind_mismatch_returns_422(
    wt_client, admin_token, reference_action
):
    """Action target_kind=reference, body sends study_prep — 422."""
    token, _ = admin_token
    action_id, version = reference_action
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json={
            "action_id": action_id,
            "action_version": version,
            "scope_target": {"kind": "study_prep", "study_idx": 1, "prep_idx": 1},
            "action_context": {},
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert detail["scope_target_kind"] == "study_prep"
    assert detail["action_target_kind"] == "reference"


async def test_submit_in_flight_blocks_with_409(
    wt_client, postgres_pool, admin_token, reference_action, reference_idx
):
    """Two consecutive submissions for the same (scope_target, action) →
    second returns 409 with the blocking ticket idx."""
    token, _ = admin_token
    action_id, version = reference_action
    body = _body(action_id, version, reference_idx)
    headers = {"Authorization": f"Bearer {token}"}

    first = await wt_client.post(URL_WORK_TICKET_PREFIX, json=body, headers=headers)
    assert first.status_code == 202
    first_idx = first.json()["work_ticket_idx"]
    wt_client._created_tickets.append(first_idx)

    second = await wt_client.post(URL_WORK_TICKET_PREFIX, json=body, headers=headers)
    assert second.status_code == 409
    detail = second.json()["detail"]
    assert detail["blocking_work_ticket_idx"] == first_idx


async def test_submit_unique_index_catches_select_race(
    wt_client, postgres_pool, admin_token, reference_action, reference_idx, monkeypatch
):
    """If two submissions race past the SELECT-LIMIT-1 fast path, the
    unique partial index `work_ticket_one_in_flight_per_reference` is
    the atomic gate. Simulate the race by short-circuiting the SELECT
    check to a no-op so both submissions reach INSERT; the second one
    must hit the constraint and surface as 409."""
    token, _ = admin_token
    action_id, version = reference_action
    body = _body(action_id, version, reference_idx)
    headers = {"Authorization": f"Bearer {token}"}

    first = await wt_client.post(URL_WORK_TICKET_PREFIX, json=body, headers=headers)
    assert first.status_code == 202
    wt_client._created_tickets.append(first.json()["work_ticket_idx"])

    async def _noop(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        "qiita_control_plane.routes.work_ticket._check_disallow_without_delete",
        _noop,
    )

    second = await wt_client.post(URL_WORK_TICKET_PREFIX, json=body, headers=headers)
    assert second.status_code == 409
    assert "in flight" in second.json()["detail"]["reason"]


async def test_submit_valid_context_against_schema(
    wt_client,
    postgres_pool,
    admin_token,
    reference_action_with_schema,
    reference_idx,
):
    """Submission whose action_context satisfies the action's
    context_schema lands in PENDING (no 422)."""
    token, _ = admin_token
    action_id, version = reference_action_with_schema
    body = _body(action_id, version, reference_idx, action_context={"sample_count": 12})
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json=body,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text
    wt_client._created_tickets.append(resp.json()["work_ticket_idx"])


async def test_submit_invalid_context_returns_422_with_errors_list(
    wt_client, admin_token, reference_action_with_schema, reference_idx
):
    """Submission whose action_context fails the schema returns 422
    with a structured `errors` list — every violation reported, not
    just the first."""
    token, _ = admin_token
    action_id, version = reference_action_with_schema
    # `sample_count` is required and must be an integer; provide neither.
    body = _body(action_id, version, reference_idx, action_context={"unrelated": "value"})
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json=body,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 422, resp.text
    detail = resp.json()["detail"]
    assert detail["reason"] == "action_context does not match action.context_schema"
    assert isinstance(detail["errors"], list)
    assert len(detail["errors"]) >= 1
    # Each error has the documented shape.
    err = detail["errors"][0]
    assert "path" in err
    assert "message" in err
    assert "schema_path" in err


async def test_submit_invalid_context_type_returns_422(
    wt_client, admin_token, reference_action_with_schema, reference_idx
):
    """`sample_count: "twelve"` (string instead of integer) → 422 with a
    type-mismatch error pointing at /sample_count."""
    token, _ = admin_token
    action_id, version = reference_action_with_schema
    body = _body(
        action_id,
        version,
        reference_idx,
        action_context={"sample_count": "twelve"},
    )
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json=body,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 422
    errs = resp.json()["detail"]["errors"]
    paths = {e["path"] for e in errs}
    assert "/sample_count" in paths


async def test_submit_503_when_compute_backend_unconfigured(
    wt_client, admin_token, reference_action, reference_idx
):
    """If the orchestrator URL was not set, the route 503s rather than
    silently creating a ticket that nothing will dispatch."""
    from qiita_control_plane.main import app

    saved = app.state.compute_backend_client
    app.state.compute_backend_client = None
    try:
        token, _ = admin_token
        action_id, version = reference_action
        resp = await wt_client.post(
            URL_WORK_TICKET_PREFIX,
            json=_body(action_id, version, reference_idx),
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 503
    finally:
        app.state.compute_backend_client = saved


# ---------------------------------------------------------------------------
# POST /api/v1/work-ticket/{idx}/run
# ---------------------------------------------------------------------------


async def test_run_unknown_idx_returns_404(wt_client, admin_token):
    token, _ = admin_token
    resp = await wt_client.post(
        URL_WORK_TICKET_RUN.format(work_ticket_idx=999999999),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 404


async def test_run_on_completed_returns_409(
    wt_client, postgres_pool, admin_token, reference_action, reference_idx
):
    """COMPLETED is terminal — /run refuses with 409."""
    token, admin_idx = admin_token
    action_id, version = reference_action
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, state)"
        " VALUES ($1, $2, $3, 'reference', $4, $5::qiita.work_ticket_state)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        admin_idx,
        reference_idx,
        WorkTicketState.COMPLETED.value,
    )
    wt_client._created_tickets.append(idx)

    resp = await wt_client.post(
        URL_WORK_TICKET_RUN.format(work_ticket_idx=idx),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 409
    assert resp.json()["detail"]["current_state"] == WorkTicketState.COMPLETED.value


async def test_run_on_failed_resets_to_pending(
    wt_client, postgres_pool, admin_token, reference_action, reference_idx
):
    """FAILED → /run flips state back to PENDING, resets retry_count,
    and clears failure_* (DB CHECK requires failure_* all-NULL when
    state != failed)."""
    token, admin_idx = admin_token
    action_id, version = reference_action
    # Seed a FAILED ticket directly. DB CHECK requires failure_type,
    # failure_stage, and failure_reason all set when state=failed; the
    # step_name/stage coupling forces step_name=NULL for stage=submission.
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, state, retry_count,"
        "  failure_type, failure_stage, failure_reason)"
        " VALUES ($1, $2, $3, 'reference', $4,"
        "  $5::qiita.work_ticket_state, 2,"
        "  $6::qiita.failure_type, $7::qiita.work_ticket_failure_stage, $8)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        admin_idx,
        reference_idx,
        WorkTicketState.FAILED.value,
        "permanent",
        "submission",
        "test seed: simulated failure",
    )
    wt_client._created_tickets.append(idx)

    resp = await wt_client.post(
        URL_WORK_TICKET_RUN.format(work_ticket_idx=idx),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text

    row = await postgres_pool.fetchrow(
        "SELECT state, retry_count,"
        "       failure_type, failure_stage, failure_step_name, failure_reason"
        " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        idx,
    )
    assert row["state"] == WorkTicketState.PENDING.value
    # /run resets retry_count and clears failure_*: an operator-driven
    # restart gets a clean budget and the post-mortem column lineage
    # cleared so a successful retry doesn't carry stale failure data.
    assert row["retry_count"] == 0
    assert row["failure_type"] is None
    assert row["failure_stage"] is None
    assert row["failure_step_name"] is None
    assert row["failure_reason"] is None


async def test_run_on_pending_dispatches_without_state_change(
    wt_client, postgres_pool, admin_token, reference_action, reference_idx
):
    """PENDING → /run fires dispatch (recovery from a lost create-task);
    state stays PENDING because the runner itself transitions it."""
    token, admin_idx = admin_token
    action_id, version = reference_action
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, state)"
        " VALUES ($1, $2, $3, 'reference', $4, $5::qiita.work_ticket_state)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        admin_idx,
        reference_idx,
        WorkTicketState.PENDING.value,
    )
    wt_client._created_tickets.append(idx)

    resp = await wt_client.post(
        URL_WORK_TICKET_RUN.format(work_ticket_idx=idx),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", idx
    )
    assert state == WorkTicketState.PENDING.value


# ---------------------------------------------------------------------------
# Startup recovery
# ---------------------------------------------------------------------------


async def test_recover_orphaned_tickets_marks_non_terminal_failed(
    postgres_pool, admin_token, reference_action
):
    """All non-terminal tickets become FAILED; terminal ones are
    untouched. Each ticket targets its own reference so the
    one-in-flight-per-reference unique index doesn't fire on insert."""
    from qiita_control_plane.dispatch import recover_orphaned_tickets

    _, admin_idx = admin_token
    action_id, version = reference_action
    states_before = [
        WorkTicketState.PENDING.value,
        WorkTicketState.QUEUED.value,
        WorkTicketState.PROCESSING.value,
        WorkTicketState.COMPLETED.value,
    ]

    created_refs: list[int] = []
    created_idxs: list[int] = []
    try:
        for s in states_before:
            ref_idx = await postgres_pool.fetchval(
                "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
                " VALUES ($1, '1.0', 'sequence_reference', 'pending', $2)"
                " RETURNING reference_idx",
                f"wt-recover-{uuid.uuid4()}",
                admin_idx,
            )
            created_refs.append(ref_idx)
            idx = await postgres_pool.fetchval(
                "INSERT INTO qiita.work_ticket"
                " (action_id, action_version, originator_principal_idx,"
                "  scope_target_kind, reference_idx, state)"
                " VALUES ($1, $2, $3, 'reference', $4, $5::qiita.work_ticket_state)"
                " RETURNING work_ticket_idx",
                action_id,
                version,
                admin_idx,
                ref_idx,
                s,
            )
            created_idxs.append(idx)

        recovered_count = await recover_orphaned_tickets(postgres_pool)
        # Three non-terminal tickets we just inserted should be picked up
        # (plus possibly orphans from earlier tests in the same session,
        # so use >= rather than ==).
        assert recovered_count >= 3

        rows_after = []
        for idx in created_idxs:
            row = await postgres_pool.fetchrow(
                "SELECT state, failure_type, failure_stage, failure_step_name, failure_reason"
                " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
                idx,
            )
            rows_after.append(dict(row))
        # pending, queued, processing → failed; completed untouched.
        assert [r["state"] for r in rows_after] == [
            WorkTicketState.FAILED.value,
            WorkTicketState.FAILED.value,
            WorkTicketState.FAILED.value,
            WorkTicketState.COMPLETED.value,
        ]
        # The three recovered tickets carry the orphan-recovery diagnostic
        # populated by recover_orphaned_tickets — failure_type=retriable,
        # stage=submission (no per-step context), step_name=NULL,
        # reason explaining the cp-restart provenance.
        for r in rows_after[:3]:
            assert r["failure_type"] == "retriable"
            assert r["failure_stage"] == "submission"
            assert r["failure_step_name"] is None
            assert "cp restarted" in r["failure_reason"]
        # The COMPLETED ticket was untouched: failure_* all NULL.
        assert rows_after[3]["failure_type"] is None
        assert rows_after[3]["failure_reason"] is None
    finally:
        if created_idxs:
            await postgres_pool.execute(
                "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = ANY($1::bigint[])",
                created_idxs,
            )
        if created_refs:
            await postgres_pool.execute(
                "DELETE FROM qiita.reference WHERE reference_idx = ANY($1::bigint[])",
                created_refs,
            )
