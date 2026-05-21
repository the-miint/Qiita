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
from qiita_common.api_paths import (
    URL_WORK_TICKET_BY_IDX,
    URL_WORK_TICKET_PREFIX,
    URL_WORK_TICKET_RUN,
)
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


@pytest.fixture
async def prep_sample_idx(postgres_pool, admin_token):
    """A minimal qiita.prep_sample row (the supertype introduced by #35)
    owned by the admin principal, with processing_kind='sequenced'.

    Uses the shared db_seeds composer so this fixture stays in sync with
    every other "I need a sequenced prep_sample" site (route tests,
    repository tests, integration smoke). The sequenced_sample 1:1
    subtype row is intentionally NOT created — the work_ticket scope
    target is the supertype, and the tests here exercise scope/action
    plumbing only (not sequencing-specific fields).

    All seeded rows are cleaned up in reverse-FK order after the test."""
    from qiita_control_plane.testing.db_seeds import (
        seed_biosample_with_sequenced_prep_sample,
    )

    _, admin_idx = admin_token
    biosample_idx, idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=admin_idx
    )
    yield idx
    # FK RESTRICT cascade — drop dependents in reverse order. The
    # composer used the seeded `short_read_metagenomics` prep_protocol
    # (system-owned), so we don't delete the protocol here.
    await postgres_pool.execute("DELETE FROM qiita.work_ticket WHERE prep_sample_idx = $1", idx)
    await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", idx)
    await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx)


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


async def _seed_action(
    postgres_pool,
    *,
    context_schema: dict,
    target_kind: str = "reference",
    scopes: list[str] | None = None,
    target_processing_kinds: list[str] | None = None,
    human_roles: list[str] | None = None,
) -> tuple[str, str]:
    """Insert an enabled action with the given context_schema and
    target_kind. Returns (action_id, version). Caller is responsible
    for cleanup — fixtures wrap this with teardown.

    `target_kind` selects the scope kind the action accepts; `scopes`
    overrides the default scope list (REFERENCE_WRITE) for targets that
    don't fit that grant. `target_processing_kinds` only applies when
    target_kind = 'prep_sample'; left as default-empty otherwise (the
    DB CHECK action_processing_kinds_only_for_prep_sample enforces
    that pairing). `human_roles` overrides the default audience
    ([system_admin]) — pass [user, wet_lab_admin, system_admin] to
    exercise the wider audience the fastq-to-parquet YAML declares."""
    action_id = "wt-test-action"
    version = f"v-{uuid.uuid4()}"
    await postgres_pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, target_processing_kinds,"
        "  scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling,"
        "  success_status, failure_status"
        ") VALUES ($1, $2, $3::qiita.scope_target_kind,"
        "          $4::qiita.processing_kind[], $5::text[], $6::jsonb,"
        "          $7::jsonb, $8::jsonb, 1, 1, '1 minute', $9, $10)",
        action_id,
        version,
        target_kind,
        target_processing_kinds or [],
        scopes if scopes is not None else [Scope.REFERENCE_WRITE.value],
        json.dumps(
            {
                "service": False,
                "human_roles": (
                    human_roles if human_roles is not None else [SystemRole.SYSTEM_ADMIN.value]
                ),
            }
        ),
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
async def prep_sample_action(postgres_pool):
    """An enabled, prep_sample-targeting action accepting any context
    object and any processing_kind (target_processing_kinds=[] = "any").
    Reuses the noop step shape so route-side dispatch tests don't fan out
    — `_patch_run_and_log` short-circuits real execution."""
    action_id, version = await _seed_action(
        postgres_pool,
        context_schema={},
        target_kind="prep_sample",
        # No reference / sample-specific scope exists yet; REFERENCE_WRITE
        # is the closest grant the admin_token fixture carries.
        scopes=[Scope.REFERENCE_WRITE.value],
    )
    yield action_id, version
    await _drop_action(postgres_pool, action_id, version)


@pytest.fixture
async def sequenced_only_prep_sample_action(postgres_pool):
    """A prep_sample-targeting action that ONLY accepts
    processing_kind='sequenced' — drives the option-(b) check that
    rejects submissions against a non-sequenced prep_sample. Mirrors
    fastq-to-parquet's YAML declaration."""
    action_id, version = await _seed_action(
        postgres_pool,
        context_schema={},
        target_kind="prep_sample",
        target_processing_kinds=["sequenced"],
        scopes=[Scope.REFERENCE_WRITE.value],
    )
    yield action_id, version
    await _drop_action(postgres_pool, action_id, version)


@pytest.fixture
async def user_audience_prep_sample_action(postgres_pool):
    """An enabled, prep_sample-targeting action whose audience admits
    USER (plus wet_lab_admin / system_admin) — mirrors the audience
    fastq-to-parquet declares in its YAML. Drives the per-resource
    study-access gate the work_ticket POST applies for prep_sample-
    scoped submissions. Scopes left empty (the gate runs independent of
    scope checks); target_processing_kinds left empty so the test
    doesn't have to wrestle with the kind-match arm too."""
    action_id, version = await _seed_action(
        postgres_pool,
        context_schema={},
        target_kind="prep_sample",
        scopes=[],
        human_roles=[
            SystemRole.USER.value,
            SystemRole.WET_LAB_ADMIN.value,
            SystemRole.SYSTEM_ADMIN.value,
        ],
    )
    yield action_id, version
    await _drop_action(postgres_pool, action_id, version)


@pytest.fixture
async def prep_sample_with_study_link(postgres_pool, prep_sample_idx, admin_token):
    """The shared sequenced prep_sample plus a non-retired
    `prep_sample_to_study` link against a freshly-seeded study owned by
    the admin principal. Returns `(prep_sample_idx, study_idx,
    study_owner_idx)`. No `study_access` row is seeded by default — the
    test chooses whether to insert a grant.

    The biosample-side link is required by the
    `prep_sample_to_study_reject_without_biosample_link` trigger: a
    `prep_sample_to_study` insert raises unless a non-retired
    `biosample_to_study` already exists on the same study. Without
    this, the fixture's INSERT fails with a confusing RaiseError instead
    of yielding."""
    _, admin_idx = admin_token
    study_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        admin_idx,
        f"wt-prep-sample-study-{uuid.uuid4()}",
    )
    # The biosample side has no such trigger; we resolve the prep_sample's
    # biosample via the supertype column and link it to the new study.
    biosample_idx = await postgres_pool.fetchval(
        "SELECT biosample_idx FROM qiita.prep_sample WHERE idx = $1",
        prep_sample_idx,
    )
    await postgres_pool.execute(
        "INSERT INTO qiita.biosample_to_study (biosample_idx, study_idx, created_by_idx)"
        " VALUES ($1, $2, $3)",
        biosample_idx,
        study_idx,
        admin_idx,
    )
    await postgres_pool.execute(
        "INSERT INTO qiita.prep_sample_to_study (prep_sample_idx, study_idx, created_by_idx)"
        " VALUES ($1, $2, $3)",
        prep_sample_idx,
        study_idx,
        admin_idx,
    )

    yield prep_sample_idx, study_idx, admin_idx

    # FK-reverse cleanup of just the rows this fixture created.
    await postgres_pool.execute(
        "DELETE FROM qiita.study_access WHERE study_idx = $1",
        study_idx,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample_to_study WHERE prep_sample_idx = $1 AND study_idx = $2",
        prep_sample_idx,
        study_idx,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample_to_study WHERE biosample_idx = $1 AND study_idx = $2",
        biosample_idx,
        study_idx,
    )
    await postgres_pool.execute("DELETE FROM qiita.study WHERE idx = $1", study_idx)


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


async def test_submit_prep_sample_scope_persists_idx(
    wt_client,
    postgres_pool,
    admin_token,
    prep_sample_action,
    prep_sample_idx,
):
    """Submitting a prep_sample-scoped ticket round-trips the scope
    kind through the INSERT path: 202 with PENDING, and the persisted
    row carries scope_target_kind='prep_sample' plus the
    prep_sample_idx the body declared (with all other scope arms NULL,
    per the work_ticket_scope_target_consistent CHECK)."""
    token, _ = admin_token
    action_id, version = prep_sample_action
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json={
            "action_id": action_id,
            "action_version": version,
            "scope_target": {
                "kind": "prep_sample",
                "prep_sample_idx": prep_sample_idx,
            },
            "action_context": {},
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text
    idx = resp.json()["work_ticket_idx"]
    wt_client._created_tickets.append(idx)

    row = await postgres_pool.fetchrow(
        "SELECT scope_target_kind, study_idx, prep_idx, reference_idx,"
        "       prep_sample_idx, state"
        " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        idx,
    )
    assert row["scope_target_kind"] == "prep_sample"
    assert row["prep_sample_idx"] == prep_sample_idx
    assert row["study_idx"] is None
    assert row["prep_idx"] is None
    assert row["reference_idx"] is None
    assert row["state"] == WorkTicketState.PENDING.value


async def test_submit_prep_sample_disallow_without_delete(
    wt_client,
    postgres_pool,
    admin_token,
    prep_sample_action,
    prep_sample_idx,
):
    """A second prep_sample submission against the same (action, sample)
    triple while the first is non-terminal must 409 — same
    disallow-without-delete contract reference and study_prep have,
    now exercised through the work_ticket_one_in_flight_per_prep_sample
    partial unique index."""
    token, _ = admin_token
    action_id, version = prep_sample_action
    body = {
        "action_id": action_id,
        "action_version": version,
        "scope_target": {
            "kind": "prep_sample",
            "prep_sample_idx": prep_sample_idx,
        },
        "action_context": {},
    }
    first = await wt_client.post(
        URL_WORK_TICKET_PREFIX, json=body, headers={"Authorization": f"Bearer {token}"}
    )
    assert first.status_code == 202
    wt_client._created_tickets.append(first.json()["work_ticket_idx"])

    second = await wt_client.post(
        URL_WORK_TICKET_PREFIX, json=body, headers={"Authorization": f"Bearer {token}"}
    )
    assert second.status_code == 409, second.text
    assert "in flight" in second.text.lower()


async def test_submit_prep_sample_kind_match_passes(
    wt_client,
    postgres_pool,
    admin_token,
    sequenced_only_prep_sample_action,
    prep_sample_idx,
):
    """Option (b): a sequenced-only action accepts a sequenced
    prep_sample. The prep_sample fixture seeds processing_kind='sequenced'
    and the action's target_processing_kinds=['sequenced'] — the
    submit-time check fires and passes."""
    token, _ = admin_token
    action_id, version = sequenced_only_prep_sample_action
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json={
            "action_id": action_id,
            "action_version": version,
            "scope_target": {
                "kind": "prep_sample",
                "prep_sample_idx": prep_sample_idx,
            },
            "action_context": {},
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text
    wt_client._created_tickets.append(resp.json()["work_ticket_idx"])


async def test_submit_prep_sample_kind_check_404_on_missing_prep(
    wt_client,
    admin_token,
    sequenced_only_prep_sample_action,
):
    """A prep_sample-scoped submission against an action that declares
    target_processing_kinds=['sequenced'] returns 404 when the prep_sample
    row doesn't exist. The check fires `SELECT processing_kind FROM
    qiita.prep_sample WHERE idx = $1`, gets NULL, and 404s rather than
    leaking a downstream FK error."""
    token, _ = admin_token
    action_id, version = sequenced_only_prep_sample_action
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json={
            "action_id": action_id,
            "action_version": version,
            "scope_target": {
                "kind": "prep_sample",
                # Vanishingly unlikely to collide with a real row in the
                # short-lived test DB; the route resolves this against
                # qiita.prep_sample.idx (BIGINT) and 404s on miss.
                "prep_sample_idx": 99_999_999,
            },
            "action_context": {},
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 404, resp.text
    assert "prep_sample" in resp.text.lower()


# A real kind-mismatch test (action target_processing_kinds=['sequenced']
# against a prep_sample with processing_kind != 'sequenced') will become
# meaningful when a second qiita.processing_kind enum value lands. Today
# the enum only contains 'sequenced'; every seedable prep_sample matches
# every sequenced-only action by construction, so the negative path
# cannot be exercised without amending the ENUM and a new subtype.


# ---------------------------------------------------------------------------
# Per-resource study-access gate for prep_sample-scoped tickets
# ---------------------------------------------------------------------------


async def test_submit_prep_sample_user_without_admin_tier_403(
    wt_client,
    postgres_pool,
    regular_token,
    user_audience_prep_sample_action,
    prep_sample_with_study_link,
):
    """USER caller submitting against a prep_sample whose only non-retired
    linked study they have no ADMIN access to gets 403 from the new
    per-resource gate. Pins both that the gate runs and that the 403
    detail names the offending study (so a USER caller can ask for
    access by study_idx)."""
    token, _ = regular_token
    action_id, version = user_audience_prep_sample_action
    prep_sample_idx, study_idx, _ = prep_sample_with_study_link

    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json={
            "action_id": action_id,
            "action_version": version,
            "scope_target": {"kind": "prep_sample", "prep_sample_idx": prep_sample_idx},
            "action_context": {},
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 403, resp.text
    assert f"study {study_idx}" in resp.json()["detail"]


async def test_submit_prep_sample_user_with_admin_tier_passes(
    wt_client,
    postgres_pool,
    regular_token,
    user_audience_prep_sample_action,
    prep_sample_with_study_link,
):
    """USER caller granted Tier.ADMIN on the prep_sample's linked study
    passes the per-resource gate and the submission lands."""
    token, user_idx = regular_token
    action_id, version = user_audience_prep_sample_action
    prep_sample_idx, study_idx, granted_by_idx = prep_sample_with_study_link

    await postgres_pool.execute(
        "INSERT INTO qiita.study_access (study_idx, principal_idx, access_tier, granted_by_idx)"
        " VALUES ($1, $2, 'admin'::qiita.tier, $3)",
        study_idx,
        user_idx,
        granted_by_idx,
    )

    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json={
            "action_id": action_id,
            "action_version": version,
            "scope_target": {"kind": "prep_sample", "prep_sample_idx": prep_sample_idx},
            "action_context": {},
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text
    wt_client._created_tickets.append(resp.json()["work_ticket_idx"])


async def test_submit_prep_sample_admin_bypasses_study_access(
    wt_client,
    admin_token,
    user_audience_prep_sample_action,
    prep_sample_with_study_link,
):
    """system_admin submits against a prep_sample whose linked study they
    have no `study_access` row on — `require_caller_has_admin_on_all_studies`
    short-circuits on `has_role_at_least(WET_LAB_ADMIN)` before any DB
    lookup runs. Pins the bypass invariant for the wider audience."""
    token, _ = admin_token
    action_id, version = user_audience_prep_sample_action
    prep_sample_idx, _study_idx, _granted_by_idx = prep_sample_with_study_link

    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json={
            "action_id": action_id,
            "action_version": version,
            "scope_target": {"kind": "prep_sample", "prep_sample_idx": prep_sample_idx},
            "action_context": {},
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text
    wt_client._created_tickets.append(resp.json()["work_ticket_idx"])


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
# GET /api/v1/work-ticket/{idx}
# ---------------------------------------------------------------------------


async def _submit_reference_ticket(
    wt_client, *, token: str, action_id: str, version: str, reference_idx: int
) -> int:
    """Submit a reference-scoped ticket and return its work_ticket_idx;
    track for cleanup. Used by the GET tests to land a real row to read
    back."""
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json=_body(action_id, version, reference_idx),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text
    idx = resp.json()["work_ticket_idx"]
    wt_client._created_tickets.append(idx)
    return idx


async def test_get_work_ticket_404_on_missing(wt_client, admin_token):
    token, _ = admin_token
    resp = await wt_client.get(
        URL_WORK_TICKET_BY_IDX.format(work_ticket_idx=99_999_999),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 404


async def test_get_work_ticket_401_on_anonymous(
    wt_client, admin_token, reference_action, reference_idx
):
    # Submit so a row exists, then GET without an Authorization header.
    token, _ = admin_token
    action_id, version = reference_action
    idx = await _submit_reference_ticket(
        wt_client, token=token, action_id=action_id, version=version, reference_idx=reference_idx
    )
    resp = await wt_client.get(URL_WORK_TICKET_BY_IDX.format(work_ticket_idx=idx))
    assert resp.status_code == 401


async def test_get_work_ticket_originator_reads_own(
    wt_client, admin_token, reference_action, reference_idx
):
    """The originator (the admin who submitted) can read the ticket back.
    Returned payload matches the full WorkTicket shape: discriminated
    scope_target, state, action info, retry accounting, null failure
    surface for a fresh ticket."""
    token, admin_idx = admin_token
    action_id, version = reference_action
    idx = await _submit_reference_ticket(
        wt_client, token=token, action_id=action_id, version=version, reference_idx=reference_idx
    )
    resp = await wt_client.get(
        URL_WORK_TICKET_BY_IDX.format(work_ticket_idx=idx),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["work_ticket_idx"] == idx
    assert body["originator_principal_idx"] == admin_idx
    assert body["action_id"] == action_id
    assert body["action_version"] == version
    assert body["state"] in {WorkTicketState.PENDING.value, WorkTicketState.QUEUED.value}
    assert body["scope_target"] == {"kind": "reference", "reference_idx": reference_idx}
    assert body["failure_type"] is None
    assert body["failure_stage"] is None


async def test_get_work_ticket_non_originator_404(
    wt_client, admin_token, regular_token, reference_action, reference_idx
):
    """A different USER (non-originator, no admin role) gets 404 — the
    same response a genuinely missing idx returns — so they cannot probe
    work_ticket_idx values to learn which tickets exist."""
    token, _ = admin_token
    user_token, _ = regular_token
    action_id, version = reference_action
    idx = await _submit_reference_ticket(
        wt_client, token=token, action_id=action_id, version=version, reference_idx=reference_idx
    )
    resp = await wt_client.get(
        URL_WORK_TICKET_BY_IDX.format(work_ticket_idx=idx),
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert resp.status_code == 404
    # Detail is identical to the missing-idx case — no existence signal.
    assert resp.json()["detail"] == f"work_ticket {idx} not found"


async def _seed_wet_lab_admin_token(postgres_pool, wt_client) -> tuple[str, int]:
    """Build a throwaway wet_lab_admin and an authenticated PAT. Mirrors
    the `admin_token` / `regular_token` fixtures inline — only the GET
    tests need a wet_lab_admin caller, so a per-test seed avoids a
    file-wide fixture."""
    from qiita_control_plane.auth.token import mint_api_token

    email = f"wt-wla-{uuid.uuid4()}@example.com"
    pidx = await postgres_pool.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, 1) RETURNING idx",
        email,
        SystemRole.WET_LAB_ADMIN,
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
        label="wt-wla",
        scopes=[Scope.SELF_PROFILE, Scope.SELF_TOKEN, Scope.REFERENCE_READ],
    )
    return plaintext, pidx


async def test_get_work_ticket_wet_lab_admin_bypasses(
    wt_client, postgres_pool, admin_token, reference_action, reference_idx
):
    """A wet_lab_admin who did not originate the ticket still reads it
    via the role bypass — pins the operator-view path."""
    token, _ = admin_token
    action_id, version = reference_action
    idx = await _submit_reference_ticket(
        wt_client, token=token, action_id=action_id, version=version, reference_idx=reference_idx
    )
    wla_token, _ = await _seed_wet_lab_admin_token(postgres_pool, wt_client)
    resp = await wt_client.get(
        URL_WORK_TICKET_BY_IDX.format(work_ticket_idx=idx),
        headers={"Authorization": f"Bearer {wla_token}"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["work_ticket_idx"] == idx


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
