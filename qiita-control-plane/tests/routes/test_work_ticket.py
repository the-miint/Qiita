"""DB-bound integration tests for /api/v1/work-ticket/*.

Covers submission (with the implicit dispatch fired by the route), the
state-aware /run override, and the dispatch-side recovery helper. The
asyncio task lifecycle itself is covered by tests/test_dispatch.py —
here we patch `_run_and_log` to a no-op so the route returns 202
without actually executing a workflow.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import (
    URL_WORK_TICKET_BY_IDX,
    URL_WORK_TICKET_LIST,
    URL_WORK_TICKET_PREFIX,
    URL_WORK_TICKET_RUN,
    URL_WORK_TICKET_STEP_LOGS,
)
from qiita_common.auth_constants import Scope, SystemRole
from qiita_common.models import (
    ComputeTarget,
    ReferenceStatus,
    StepProgressState,
    WorkTicketState,
)

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

    async def _noop(_app, _idx, **_kwargs):
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
    """A minimal qiita.prep_sample row (the supertype)
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


@pytest.fixture
async def prep_sample_with_pool_item(postgres_pool, prep_sample_idx, admin_token):
    """The shared sequenced prep_sample with its 1:1 sequenced_sample
    subtype attached, carrying a known `sequenced_pool_item_id`. Returns
    `(prep_sample_idx, sequenced_pool_item_id)`.

    The bare `prep_sample_idx` fixture deliberately omits the subtype
    row; this fixture adds the run -> pool -> sequenced_sample chain so
    the work_ticket fastq-filename-prefix gate has a pool item id to
    resolve. Teardown drops the subtype chain in reverse-FK order — it
    runs before `prep_sample_idx`'s own teardown removes the supertype,
    so the prep_sample DELETE there does not trip the subtype FK."""
    from qiita_control_plane.testing.db_seeds import seed_sequenced_sample_subtype

    _, admin_idx = admin_token
    pool_item_id = f"wt-item-{uuid.uuid4()}"
    run_idx, pool_idx, ss_idx = await seed_sequenced_sample_subtype(
        postgres_pool,
        prep_sample_idx=prep_sample_idx,
        owner_idx=admin_idx,
        sequenced_pool_item_id=pool_item_id,
    )
    yield prep_sample_idx, pool_item_id
    await postgres_pool.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)


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
    mem_ceiling_gb: int = 1,
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
        f"          $7::jsonb, $8::jsonb, 1, {mem_ceiling_gb}, '1 minute', $9, $10)",
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
async def reference_action_open(postgres_pool):
    """A reference-targeting action with NO scope requirement, an audience
    admitting USER (plus the two admin roles), and a 64 GB mem ceiling. Lets
    the resource_override tests isolate the override's own role gate (a regular
    user clears audience + scope and is stopped only by the override gate) and
    exercise a meaningful below-ceiling override."""
    action_id, version = await _seed_action(
        postgres_pool,
        context_schema={},
        target_kind="reference",
        scopes=[],
        human_roles=[
            SystemRole.USER.value,
            SystemRole.WET_LAB_ADMIN.value,
            SystemRole.SYSTEM_ADMIN.value,
        ],
        mem_ceiling_gb=64,
    )
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
async def sequenced_pool_for_wt(postgres_pool, admin_token):
    """A bare sequencing_run + sequenced_pool owned by the admin
    principal — the scope_target the bcl-convert-shaped submission tests
    aim at. No sequenced_sample subtype is attached; the work_ticket's
    scope target is the pool itself. Returns
    `(sequencing_run_idx, sequenced_pool_idx)`.

    Teardown is FK-reverse: drop dependent work_tickets, then the pool,
    then the run."""
    _, admin_idx = admin_token
    run_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequencing_run (instrument_run_id, platform, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, $2) RETURNING idx",
        f"wt-pool-run-{uuid.uuid4()}",
        admin_idx,
    )
    pool_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequenced_pool (sequencing_run_idx, created_by_idx)"
        " VALUES ($1, $2) RETURNING idx",
        run_idx,
        admin_idx,
    )
    yield run_idx, pool_idx
    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE sequenced_pool_idx = $1", pool_idx
    )
    await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)


@pytest.fixture
async def sequenced_pool_action(postgres_pool):
    """An enabled, sequenced_pool-targeting action — the bcl-convert
    shape. Empty scopes (bcl-convert declares `scopes: []`) and a
    context_schema requiring an absolute `bcl_input_dir`, mirroring
    workflows/bcl-convert/1.0.0.yaml. Default audience ([system_admin])
    matches the admin_token fixture."""
    action_id, version = await _seed_action(
        postgres_pool,
        context_schema={
            "type": "object",
            "required": ["bcl_input_dir"],
            "properties": {"bcl_input_dir": {"type": "string", "pattern": "^/"}},
        },
        target_kind="sequenced_pool",
        scopes=[],
    )
    yield action_id, version
    await _drop_action(postgres_pool, action_id, version)


@pytest.fixture
async def block_action(postgres_pool):
    """An enabled, block-targeting action — the read-mask-block shape. Empty
    scopes and any-object context, default audience ([system_admin]) matching
    the admin_token fixture."""
    action_id, version = await _seed_action(
        postgres_pool,
        context_schema={},
        target_kind="block",
        scopes=[],
    )
    yield action_id, version
    await _drop_action(postgres_pool, action_id, version)


@pytest.fixture
async def block_for_wt(postgres_pool):
    """A bare qiita.block row with no back-filled work_ticket_idx yet — the
    scope target a block-scoped submission aims at. Returns block_idx.

    Teardown is FK-reverse: drop dependent work_tickets (work_ticket.block_idx
    is a deferred NO ACTION, so the tickets must go first), then the block
    (its block_member rows cascade)."""
    block_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.block (state) VALUES ('pending') RETURNING block_idx"
    )
    yield block_idx
    await postgres_pool.execute("DELETE FROM qiita.work_ticket WHERE block_idx = $1", block_idx)
    await postgres_pool.execute("DELETE FROM qiita.block WHERE block_idx = $1", block_idx)


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


async def test_submit_resets_failed_reference_scope_to_pending(
    wt_client, postgres_pool, admin_token, reference_action_open, reference_idx
):
    """A fresh submission bound to a reference a prior ticket left at `failed`
    resets it to `pending` before dispatch, so the run's first status PATCH
    (`pending → hashing`) is legal instead of the illegal `failed → hashing`
    that killed the ticket on a `reference load --reference-idx N` retry."""
    token, _ = admin_token
    action_id, version = reference_action_open
    # A prior failed run leaves the reference at `failed`.
    await postgres_pool.execute(
        "UPDATE qiita.reference SET status = 'failed' WHERE reference_idx = $1",
        reference_idx,
    )

    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json=_body(action_id, version, reference_idx),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text
    wt_client._created_tickets.append(resp.json()["work_ticket_idx"])

    status = await postgres_pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    assert status == "pending"


async def test_submit_leaves_non_failed_reference_untouched(
    wt_client, postgres_pool, admin_token, reference_action_open, reference_idx, caplog
):
    """The dispatch reset only fires from `failed`. A reference in any other
    state (here `active`) is left as-is — the illegal `active → pending` is
    swallowed and logged at WARNING, the submission still 202s, and the status
    is unchanged."""
    token, _ = admin_token
    action_id, version = reference_action_open
    await postgres_pool.execute(
        "UPDATE qiita.reference SET status = 'active' WHERE reference_idx = $1",
        reference_idx,
    )

    with caplog.at_level(logging.WARNING, logger="qiita_control_plane.routes.work_ticket"):
        resp = await wt_client.post(
            URL_WORK_TICKET_PREFIX,
            json=_body(action_id, version, reference_idx),
            headers={"Authorization": f"Bearer {token}"},
        )
    assert resp.status_code == 202, resp.text
    wt_client._created_tickets.append(resp.json()["work_ticket_idx"])

    status = await postgres_pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    assert status == "active"
    # The swallowed IllegalStatusTransition is surfaced, not silent.
    assert any("not 'failed'" in r.getMessage() for r in caplog.records)


async def test_submit_rolls_back_ticket_if_scope_reset_fails(
    wt_client, postgres_pool, admin_token, reference_action_open, reference_idx, monkeypatch
):
    """INSERT + the failed→pending reset are one transaction. An UNEXPECTED
    reset failure (not the swallowed IllegalStatusTransition/ReferenceNotFound)
    must roll the new ticket back — never leave a committed-but-undispatched
    PENDING ticket that `_check_disallow_without_delete` would wedge."""
    token, _ = admin_token
    action_id, version = reference_action_open
    await postgres_pool.execute(
        "UPDATE qiita.reference SET status = 'failed' WHERE reference_idx = $1",
        reference_idx,
    )

    async def _boom(*_args, **_kwargs):
        raise RuntimeError("injected reset failure")

    # Patch the symbol as imported into the route module.
    monkeypatch.setattr("qiita_control_plane.routes.work_ticket.transition_reference_status", _boom)

    with pytest.raises(RuntimeError, match="injected reset failure"):
        await wt_client.post(
            URL_WORK_TICKET_PREFIX,
            json=_body(action_id, version, reference_idx),
            headers={"Authorization": f"Bearer {token}"},
        )

    # The INSERT rolled back with the failed reset — no orphan ticket left.
    count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.work_ticket WHERE reference_idx = $1", reference_idx
    )
    assert count == 0


async def test_submit_resource_override_persisted(
    wt_client, postgres_pool, admin_token, reference_action_open, reference_idx
):
    """A wet_lab_admin+ caller's resource_override (<= ceiling) is accepted
    and persisted verbatim on the work_ticket row."""
    token, _ = admin_token
    action_id, version = reference_action_open
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json=_body(action_id, version, reference_idx, resource_override={"mem_gb": 48}),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text
    idx = resp.json()["work_ticket_idx"]
    wt_client._created_tickets.append(idx)
    stored = await postgres_pool.fetchval(
        "SELECT resource_override FROM qiita.work_ticket WHERE work_ticket_idx = $1", idx
    )
    assert json.loads(stored) == {"mem_gb": 48}


async def test_submit_resource_override_above_ceiling_422(
    wt_client, postgres_pool, admin_token, reference_action_open, reference_idx
):
    """An override above the action's mem ceiling is a clean 422 at
    submission, not a ticket that fails later at dispatch."""
    token, _ = admin_token
    action_id, version = reference_action_open
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json=_body(action_id, version, reference_idx, resource_override={"mem_gb": 128}),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 422, resp.text
    assert "ceiling" in resp.text


async def test_submit_resource_override_requires_admin_403(
    wt_client, postgres_pool, regular_token, reference_action_open, reference_idx
):
    """A regular user who clears the action's audience + (empty) scope is
    still forbidden from setting resource_override — it is gated to
    wet_lab_admin / system_admin regardless of the action's own audience."""
    token, _ = regular_token
    action_id, version = reference_action_open
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json=_body(action_id, version, reference_idx, resource_override={"mem_gb": 16}),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 403, resp.text
    assert "wet_lab_admin" in resp.text


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


def _sequenced_pool_body(action_id, version, pool_idx, run_idx, **overrides):
    base = {
        "action_id": action_id,
        "action_version": version,
        "scope_target": {
            "kind": "sequenced_pool",
            "sequenced_pool_idx": pool_idx,
            "sequencing_run_idx": run_idx,
        },
        "action_context": {"bcl_input_dir": "/data/runs/240101_M00001_0001_000000000-ABCDE"},
    }
    base.update(overrides)
    return base


async def test_submit_sequenced_pool_scope_round_trips_both_idxs(
    wt_client, postgres_pool, admin_token, sequenced_pool_action, sequenced_pool_for_wt
):
    """A sequenced_pool-scoped submission (the bcl-convert shape) persists
    sequenced_pool_idx with every other scope arm NULL, and a GET round-
    trips the full scope_target — including the parent sequencing_run_idx,
    which the table does not store on the work_ticket row but the GET route
    reconstructs via the LEFT JOIN onto sequenced_pool."""
    token, _ = admin_token
    action_id, version = sequenced_pool_action
    run_idx, pool_idx = sequenced_pool_for_wt
    headers = {"Authorization": f"Bearer {token}"}

    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json=_sequenced_pool_body(action_id, version, pool_idx, run_idx),
        headers=headers,
    )
    assert resp.status_code == 202, resp.text
    idx = resp.json()["work_ticket_idx"]
    wt_client._created_tickets.append(idx)

    # Persisted row: sequenced_pool_idx set, every other scope arm NULL
    # (per the work_ticket_scope_target_consistent CHECK).
    row = await postgres_pool.fetchrow(
        "SELECT scope_target_kind, study_idx, prep_idx, reference_idx,"
        "       prep_sample_idx, sequenced_pool_idx, state"
        " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        idx,
    )
    assert row["scope_target_kind"] == "sequenced_pool"
    assert row["sequenced_pool_idx"] == pool_idx
    assert row["study_idx"] is None
    assert row["prep_idx"] is None
    assert row["reference_idx"] is None
    assert row["prep_sample_idx"] is None
    assert row["state"] == WorkTicketState.PENDING.value

    # GET round-trips both idxs — run_idx comes back via the JOIN, not a
    # stored work_ticket column.
    got = await wt_client.get(URL_WORK_TICKET_BY_IDX.format(work_ticket_idx=idx), headers=headers)
    assert got.status_code == 200, got.text
    assert got.json()["scope_target"] == {
        "kind": "sequenced_pool",
        "sequenced_pool_idx": pool_idx,
        "sequencing_run_idx": run_idx,
    }


async def test_submit_sequenced_pool_disallow_without_delete(
    wt_client, admin_token, sequenced_pool_action, sequenced_pool_for_wt
):
    """A second sequenced_pool submission against the same (action, pool)
    while the first is non-terminal must 409 via the SELECT-side
    disallow-without-delete check, naming the blocking ticket idx."""
    token, _ = admin_token
    action_id, version = sequenced_pool_action
    run_idx, pool_idx = sequenced_pool_for_wt
    headers = {"Authorization": f"Bearer {token}"}
    body = _sequenced_pool_body(action_id, version, pool_idx, run_idx)

    first = await wt_client.post(URL_WORK_TICKET_PREFIX, json=body, headers=headers)
    assert first.status_code == 202, first.text
    first_idx = first.json()["work_ticket_idx"]
    wt_client._created_tickets.append(first_idx)

    second = await wt_client.post(URL_WORK_TICKET_PREFIX, json=body, headers=headers)
    assert second.status_code == 409, second.text
    detail = second.json()["detail"]
    assert detail["blocking_work_ticket_idx"] == first_idx


async def test_submit_sequenced_pool_completed_blocks_without_force(
    wt_client, postgres_pool, admin_token, sequenced_pool_action, sequenced_pool_for_wt
):
    """A re-submit over an already-COMPLETED pool ticket is refused (409)
    without force — a re-run would re-register the pool's reads into the lake.
    The 409 names the blocking COMPLETED ticket and points at the override."""
    token, admin_idx = admin_token
    action_id, version = sequenced_pool_action
    run_idx, pool_idx = sequenced_pool_for_wt
    headers = {"Authorization": f"Bearer {token}"}

    completed_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, sequenced_pool_idx, state)"
        " VALUES ($1, $2, $3, 'sequenced_pool', $4, $5::qiita.work_ticket_state)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        admin_idx,
        pool_idx,
        WorkTicketState.COMPLETED.value,
    )
    wt_client._created_tickets.append(completed_idx)

    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json=_sequenced_pool_body(action_id, version, pool_idx, run_idx),
        headers=headers,
    )
    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert detail["blocking_work_ticket_idx"] == completed_idx
    assert "COMPLETED" in detail["reason"]
    assert "force" in detail["reason"]


async def test_submit_sequenced_pool_completed_force_allows(
    wt_client, postgres_pool, admin_token, sequenced_pool_action, sequenced_pool_for_wt
):
    """force=true (here a system_admin) intentionally re-submits over a
    COMPLETED pool ticket: 202 with a fresh PENDING ticket alongside the
    COMPLETED one."""
    token, admin_idx = admin_token
    action_id, version = sequenced_pool_action
    run_idx, pool_idx = sequenced_pool_for_wt
    headers = {"Authorization": f"Bearer {token}"}

    completed_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, sequenced_pool_idx, state)"
        " VALUES ($1, $2, $3, 'sequenced_pool', $4, $5::qiita.work_ticket_state)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        admin_idx,
        pool_idx,
        WorkTicketState.COMPLETED.value,
    )
    wt_client._created_tickets.append(completed_idx)

    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json=_sequenced_pool_body(action_id, version, pool_idx, run_idx, force=True),
        headers=headers,
    )
    assert resp.status_code == 202, resp.text
    new_idx = resp.json()["work_ticket_idx"]
    wt_client._created_tickets.append(new_idx)
    assert new_idx != completed_idx


async def test_submit_force_requires_admin_403(
    wt_client, regular_token, reference_action_open, reference_idx
):
    """force is privileged regardless of the action's audience: a regular user
    who clears audience + (empty) scope is still 403'd for force=true — mirrors
    the resource_override gate."""
    token, _ = regular_token
    action_id, version = reference_action_open
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json=_body(action_id, version, reference_idx, force=True),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 403, resp.text
    assert "wet_lab_admin" in resp.text


async def test_submit_force_noop_on_non_pool_scope(
    wt_client, admin_token, reference_action, reference_idx
):
    """An authorized force=true is a no-op outside the sequenced_pool COMPLETED
    gate: an admin submitting a reference action with force=true still gets a
    clean 202 (the flag changes nothing for non-pool scopes)."""
    token, _ = admin_token
    action_id, version = reference_action
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json=_body(action_id, version, reference_idx, force=True),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text
    wt_client._created_tickets.append(resp.json()["work_ticket_idx"])


async def test_submit_sequenced_pool_unique_index_catches_select_race(
    wt_client, admin_token, sequenced_pool_action, sequenced_pool_for_wt, monkeypatch
):
    """The atomic gate for a sequenced_pool double-submit is the partial
    unique index `work_ticket_one_in_flight_per_sequenced_pool`. Short-
    circuit the SELECT-side check so both submissions reach INSERT; the
    second must trip the constraint and surface as the same 409 the
    SELECT path returns."""
    token, _ = admin_token
    action_id, version = sequenced_pool_action
    run_idx, pool_idx = sequenced_pool_for_wt
    headers = {"Authorization": f"Bearer {token}"}
    body = _sequenced_pool_body(action_id, version, pool_idx, run_idx)

    first = await wt_client.post(URL_WORK_TICKET_PREFIX, json=body, headers=headers)
    assert first.status_code == 202, first.text
    wt_client._created_tickets.append(first.json()["work_ticket_idx"])

    async def _noop(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        "qiita_control_plane.routes.work_ticket._check_disallow_without_delete",
        _noop,
    )

    second = await wt_client.post(URL_WORK_TICKET_PREFIX, json=body, headers=headers)
    assert second.status_code == 409, second.text
    assert "in flight" in second.json()["detail"]["reason"]


# ---------------------------------------------------------------------------
# block scope target (bulk-block read-mask)
# ---------------------------------------------------------------------------


def _block_body(action_id, version, block_idx, **overrides):
    base = {
        "action_id": action_id,
        "action_version": version,
        "scope_target": {"kind": "block", "block_idx": block_idx},
        "action_context": {},
    }
    base.update(overrides)
    return base


async def test_submit_block_scope_round_trips(
    wt_client, postgres_pool, admin_token, block_action, block_for_wt
):
    """A block-scoped submission persists block_idx with every other scope arm
    NULL (per work_ticket_scope_target_consistent), and a GET round-trips the
    block scope_target."""
    token, _ = admin_token
    action_id, version = block_action
    block_idx = block_for_wt
    headers = {"Authorization": f"Bearer {token}"}

    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json=_block_body(action_id, version, block_idx),
        headers=headers,
    )
    assert resp.status_code == 202, resp.text
    idx = resp.json()["work_ticket_idx"]
    wt_client._created_tickets.append(idx)

    row = await postgres_pool.fetchrow(
        "SELECT scope_target_kind, study_idx, prep_idx, reference_idx,"
        "       prep_sample_idx, sequenced_pool_idx, block_idx, state"
        " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        idx,
    )
    assert row["scope_target_kind"] == "block"
    assert row["block_idx"] == block_idx
    assert row["study_idx"] is None
    assert row["prep_idx"] is None
    assert row["reference_idx"] is None
    assert row["prep_sample_idx"] is None
    assert row["sequenced_pool_idx"] is None
    assert row["state"] == WorkTicketState.PENDING.value

    got = await wt_client.get(URL_WORK_TICKET_BY_IDX.format(work_ticket_idx=idx), headers=headers)
    assert got.status_code == 200, got.text
    assert got.json()["scope_target"] == {"kind": "block", "block_idx": block_idx}


async def test_submit_block_disallow_without_delete(
    wt_client, admin_token, block_action, block_for_wt
):
    """A second block submission against the same (action, block) while the
    first is non-terminal must 409 via the SELECT-side disallow-without-delete
    check, naming the blocking ticket idx."""
    token, _ = admin_token
    action_id, version = block_action
    block_idx = block_for_wt
    headers = {"Authorization": f"Bearer {token}"}
    body = _block_body(action_id, version, block_idx)

    first = await wt_client.post(URL_WORK_TICKET_PREFIX, json=body, headers=headers)
    assert first.status_code == 202, first.text
    first_idx = first.json()["work_ticket_idx"]
    wt_client._created_tickets.append(first_idx)

    second = await wt_client.post(URL_WORK_TICKET_PREFIX, json=body, headers=headers)
    assert second.status_code == 409, second.text
    assert second.json()["detail"]["blocking_work_ticket_idx"] == first_idx


async def test_submit_block_distinct_blocks_allowed(
    wt_client, postgres_pool, admin_token, block_action, block_for_wt
):
    """The in-flight gate is per-block: a second, distinct block submits
    cleanly alongside a non-terminal ticket for the first block."""
    token, _ = admin_token
    action_id, version = block_action
    first_block = block_for_wt
    headers = {"Authorization": f"Bearer {token}"}

    first = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json=_block_body(action_id, version, first_block),
        headers=headers,
    )
    assert first.status_code == 202, first.text
    wt_client._created_tickets.append(first.json()["work_ticket_idx"])

    second_block = await postgres_pool.fetchval(
        "INSERT INTO qiita.block (state) VALUES ('pending') RETURNING block_idx"
    )
    try:
        second = await wt_client.post(
            URL_WORK_TICKET_PREFIX,
            json=_block_body(action_id, version, second_block),
            headers=headers,
        )
        assert second.status_code == 202, second.text
        wt_client._created_tickets.append(second.json()["work_ticket_idx"])
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE block_idx = $1", second_block
        )
        await postgres_pool.execute("DELETE FROM qiita.block WHERE block_idx = $1", second_block)


async def test_submit_block_unique_index_catches_select_race(
    wt_client, admin_token, block_action, block_for_wt, monkeypatch
):
    """The atomic gate for a block double-submit is the partial unique index
    work_ticket_one_in_flight_per_block. Short-circuit the SELECT-side check so
    both submissions reach INSERT; the second must trip the constraint and
    surface as the same 409 the SELECT path returns."""
    token, _ = admin_token
    action_id, version = block_action
    block_idx = block_for_wt
    headers = {"Authorization": f"Bearer {token}"}
    body = _block_body(action_id, version, block_idx)

    first = await wt_client.post(URL_WORK_TICKET_PREFIX, json=body, headers=headers)
    assert first.status_code == 202, first.text
    wt_client._created_tickets.append(first.json()["work_ticket_idx"])

    async def _noop(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        "qiita_control_plane.routes.work_ticket._check_disallow_without_delete",
        _noop,
    )

    second = await wt_client.post(URL_WORK_TICKET_PREFIX, json=body, headers=headers)
    assert second.status_code == 409, second.text
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


# ---------------------------------------------------------------------------
# fastq filename-prefix gate: a fastq path in action_context must carry a
# basename prefixed by the prep_sample's sequenced_pool_item_id.
# ---------------------------------------------------------------------------


async def test_submit_fastq_path_prefix_match_passes(
    wt_client, admin_token, prep_sample_action, prep_sample_with_pool_item
):
    """fastq_path and reverse_fastq_path whose basenames both start with
    the prep_sample's sequenced_pool_item_id clear the filename-prefix
    gate → 202."""
    token, _ = admin_token
    action_id, version = prep_sample_action
    prep_sample_idx, pool_item_id = prep_sample_with_pool_item
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json={
            "action_id": action_id,
            "action_version": version,
            "scope_target": {"kind": "prep_sample", "prep_sample_idx": prep_sample_idx},
            "action_context": {
                "fastq_path": f"/scratch/{pool_item_id}_R1.fastq",
                "reverse_fastq_path": f"/scratch/{pool_item_id}_R2.fastq",
            },
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text
    wt_client._created_tickets.append(resp.json()["work_ticket_idx"])


async def test_submit_single_end_fastq_path_prefix_match_passes(
    wt_client, admin_token, prep_sample_action, prep_sample_with_pool_item
):
    """Forward-only (single-end) submission stays valid: a lone fastq_path
    with no reverse_fastq_path, basename prefixed by the
    sequenced_pool_item_id, clears the gate → 202. The prefix rule fires
    on the one read present rather than requiring a pair."""
    token, _ = admin_token
    action_id, version = prep_sample_action
    prep_sample_idx, pool_item_id = prep_sample_with_pool_item
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json={
            "action_id": action_id,
            "action_version": version,
            "scope_target": {"kind": "prep_sample", "prep_sample_idx": prep_sample_idx},
            "action_context": {"fastq_path": f"/scratch/{pool_item_id}.fastq"},
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text
    wt_client._created_tickets.append(resp.json()["work_ticket_idx"])


async def test_submit_fastq_path_prefix_mismatch_returns_422(
    wt_client, admin_token, prep_sample_action, prep_sample_with_pool_item
):
    """A fastq_path whose basename does not start with the prep_sample's
    sequenced_pool_item_id is rejected with 422; the detail names the
    pool item id and the offending path."""
    token, _ = admin_token
    action_id, version = prep_sample_action
    prep_sample_idx, pool_item_id = prep_sample_with_pool_item
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json={
            "action_id": action_id,
            "action_version": version,
            "scope_target": {"kind": "prep_sample", "prep_sample_idx": prep_sample_idx},
            "action_context": {"fastq_path": "/scratch/wrong-prefix_R1.fastq"},
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 422, resp.text
    detail = resp.json()["detail"]
    assert "sequenced_pool_item_id" in detail["reason"]
    assert detail["sequenced_pool_item_id"] == pool_item_id
    assert len(detail["mismatched"]) == 1
    assert detail["mismatched"][0]["context_key"] == "fastq_path"
    assert detail["mismatched"][0]["basename"] == "wrong-prefix_R1.fastq"


async def test_submit_fastq_path_prefix_segment_anchored(
    wt_client, admin_token, prep_sample_action, prep_sample_with_pool_item
):
    """The gate is segment-anchored, not a bare substring match: a
    basename carrying the pool item id followed straight by another
    character — no `_`/`.` separator — is rejected (422). `<id>9_R1.fastq`
    must not pass for pool item id `<id>`."""
    token, _ = admin_token
    action_id, version = prep_sample_action
    prep_sample_idx, pool_item_id = prep_sample_with_pool_item
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json={
            "action_id": action_id,
            "action_version": version,
            "scope_target": {"kind": "prep_sample", "prep_sample_idx": prep_sample_idx},
            "action_context": {"fastq_path": f"/scratch/{pool_item_id}9_R1.fastq"},
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"]["mismatched"][0]["context_key"] == "fastq_path"


async def test_submit_reverse_fastq_path_prefix_checked(
    wt_client, admin_token, prep_sample_action, prep_sample_with_pool_item
):
    """The gate covers reverse_fastq_path too: a matching fastq_path
    paired with a mismatched reverse_fastq_path still 422s, and the
    detail flags only the reverse key."""
    token, _ = admin_token
    action_id, version = prep_sample_action
    prep_sample_idx, pool_item_id = prep_sample_with_pool_item
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json={
            "action_id": action_id,
            "action_version": version,
            "scope_target": {"kind": "prep_sample", "prep_sample_idx": prep_sample_idx},
            "action_context": {
                "fastq_path": f"/scratch/{pool_item_id}_R1.fastq",
                "reverse_fastq_path": "/scratch/other-sample_R2.fastq",
            },
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 422, resp.text
    mismatched = resp.json()["detail"]["mismatched"]
    assert [m["context_key"] for m in mismatched] == ["reverse_fastq_path"]


async def test_submit_fastq_path_prefix_skipped_without_pool_item(
    wt_client, admin_token, prep_sample_action, prep_sample_idx
):
    """When the prep_sample has no sequenced_sample subtype row (hence no
    sequenced_pool_item_id), the filename-prefix gate is vacuous and
    skipped — any fastq_path passes. Uses the bare `prep_sample_idx`
    fixture, which deliberately omits the subtype row."""
    token, _ = admin_token
    action_id, version = prep_sample_action
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json={
            "action_id": action_id,
            "action_version": version,
            "scope_target": {"kind": "prep_sample", "prep_sample_idx": prep_sample_idx},
            "action_context": {"fastq_path": "/scratch/anything-goes_R1.fastq"},
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text
    wt_client._created_tickets.append(resp.json()["work_ticket_idx"])


async def test_submit_non_string_fastq_path_skipped(
    wt_client, admin_token, prep_sample_action, prep_sample_with_pool_item
):
    """A non-string value under fastq_path is not a path: the gate's
    isinstance guard skips it rather than 422-ing. For fastq-to-parquet
    proper, context_schema would 422 a non-string upstream — this pins
    the defense-in-depth branch for a permissive-schema action
    (prep_sample_action carries context_schema={})."""
    token, _ = admin_token
    action_id, version = prep_sample_action
    prep_sample_idx, _ = prep_sample_with_pool_item
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json={
            "action_id": action_id,
            "action_version": version,
            "scope_target": {"kind": "prep_sample", "prep_sample_idx": prep_sample_idx},
            "action_context": {"fastq_path": 123},
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text
    wt_client._created_tickets.append(resp.json()["work_ticket_idx"])


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


async def test_get_work_ticket_surfaces_transient_reason(
    wt_client, postgres_pool, admin_token, reference_action, reference_idx
):
    """The runner records why it is retrying an unreachable orchestrator in
    place on the ticket (transient_reason / transient_since); GET surfaces
    them so a wedged-looking 'processing' ticket is explainable, not silent."""
    token, admin_idx = admin_token
    action_id, version = reference_action
    idx = await _submit_reference_ticket(
        wt_client, token=token, action_id=action_id, version=version, reference_idx=reference_idx
    )
    await postgres_pool.execute(
        "UPDATE qiita.work_ticket"
        " SET state = 'processing', transient_reason = $2, transient_since = now()"
        " WHERE work_ticket_idx = $1",
        idx,
        "submit: orchestrator_unreachable",
    )

    resp = await wt_client.get(
        URL_WORK_TICKET_BY_IDX.format(work_ticket_idx=idx),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["transient_reason"] == "submit: orchestrator_unreachable"
    assert body["transient_since"] is not None


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


async def test_run_on_failed_resets_reference_scope_target_to_pending(
    wt_client, postgres_pool, admin_token, reference_action, reference_idx
):
    """A FAILED reference workflow leaves its reference pinned at the
    'failed' status (the failure_status PATCH put it there). The reference
    FSM's ONLY legal exit from 'failed' is '-> pending', so a redrive must
    reset the scope_target reference too — otherwise the redriven
    workflow's first status PATCH ('failed -> hashing') is illegal and the
    redrive dies on the spot. /run sends the reference back to
    'pending' so the workflow can re-walk pending -> hashing -> ...."""
    token, admin_idx = admin_token
    action_id, version = reference_action
    # The reference the workflow failed against — pin it where a real
    # failure leaves it.
    await postgres_pool.execute(
        "UPDATE qiita.reference SET status = $1 WHERE reference_idx = $2",
        ReferenceStatus.FAILED.value,
        reference_idx,
    )
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, state, retry_count,"
        "  failure_type, failure_stage, failure_reason)"
        " VALUES ($1, $2, $3, 'reference', $4,"
        "  $5::qiita.work_ticket_state, 1,"
        "  'permanent'::qiita.failure_type,"
        "  'submission'::qiita.work_ticket_failure_stage, 'test seed')"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        admin_idx,
        reference_idx,
        WorkTicketState.FAILED.value,
    )
    wt_client._created_tickets.append(idx)

    resp = await wt_client.post(
        URL_WORK_TICKET_RUN.format(work_ticket_idx=idx),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text

    ref_status = await postgres_pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    assert ref_status == ReferenceStatus.PENDING.value


async def test_run_on_failed_drops_dead_step_rows_keeps_completed(
    wt_client, postgres_pool, admin_token, reference_action, reference_idx
):
    """A redrive re-enters every not-yet-completed entry at attempt 0, but a
    prior FAILED run leaves terminal 'failed' work_ticket_step rows behind.
    Re-using that attempt would collide — the step_progress writers reject
    any transition out of 'failed' (and record_failed refuses
    failed->failed), so the redrive would die re-adjudicating the dead row.
    /run drops every non-'completed' step row so the redrive's
    attempt-0 writes land on a clean slate, while KEEPING 'completed' rows
    so the runner still fast-forwards already-finished steps."""
    token, admin_idx = admin_token
    action_id, version = reference_action
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, state,"
        "  failure_type, failure_stage, failure_reason)"
        " VALUES ($1, $2, $3, 'reference', $4,"
        "  $5::qiita.work_ticket_state, 'permanent'::qiita.failure_type,"
        "  'submission'::qiita.work_ticket_failure_stage, 'test seed')"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        admin_idx,
        reference_idx,
        WorkTicketState.FAILED.value,
    )
    wt_client._created_tickets.append(idx)
    # An earlier step that finished (fast-forward depends on this row
    # surviving) and a later step's dead 'failed' attempt (must clear).
    await postgres_pool.execute(
        "INSERT INTO qiita.work_ticket_step"
        " (work_ticket_idx, step_index, attempt, step_name, compute_target, state)"
        " VALUES ($1, 0, 0, 'hash', $2, $3)",
        idx,
        ComputeTarget.LOCAL.value,
        StepProgressState.COMPLETED.value,
    )
    await postgres_pool.execute(
        "INSERT INTO qiita.work_ticket_step"
        " (work_ticket_idx, step_index, attempt, step_name, compute_target,"
        "  state, slurm_job_id, job_name, failure_kind, failure_reason)"
        " VALUES ($1, 1, 0, 'load', $2, $3, 987654, 'qiita-wt-load-a0',"
        "  'contract_violation', 'boom')",
        idx,
        ComputeTarget.SLURM.value,
        StepProgressState.FAILED.value,
    )

    resp = await wt_client.post(
        URL_WORK_TICKET_RUN.format(work_ticket_idx=idx),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text

    rows = await postgres_pool.fetch(
        "SELECT step_index, attempt, state FROM qiita.work_ticket_step"
        " WHERE work_ticket_idx = $1 ORDER BY step_index, attempt",
        idx,
    )
    surviving = [(r["step_index"], r["attempt"], r["state"]) for r in rows]
    assert surviving == [(0, 0, StepProgressState.COMPLETED.value)]


async def test_run_on_failed_with_non_failed_reference_does_not_abort_redrive(
    wt_client, postgres_pool, admin_token, reference_action, reference_idx
):
    """If the reference is NOT at 'failed' on a redrive (e.g. the workflow
    died before any status PATCH, so the reference is still 'pending'), the
    scope-target reset is a no-op — `failed -> pending` is illegal from
    'pending', and that IllegalStatusTransition must be swallowed, not abort
    the redrive. The redrive still succeeds (202), the ticket resets, and
    the reference is left where it was ('pending' redrives fine on its own)."""
    token, admin_idx = admin_token
    action_id, version = reference_action
    # reference_idx is seeded 'pending'; leave it there (the not-'failed' case).
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, state,"
        "  failure_type, failure_stage, failure_reason)"
        " VALUES ($1, $2, $3, 'reference', $4,"
        "  $5::qiita.work_ticket_state, 'permanent'::qiita.failure_type,"
        "  'submission'::qiita.work_ticket_failure_stage, 'test seed')"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        admin_idx,
        reference_idx,
        WorkTicketState.FAILED.value,
    )
    wt_client._created_tickets.append(idx)

    resp = await wt_client.post(
        URL_WORK_TICKET_RUN.format(work_ticket_idx=idx),
        headers={"Authorization": f"Bearer {token}"},
    )
    # The redrive is NOT aborted by the un-resettable reference.
    assert resp.status_code == 202, resp.text

    ticket_state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", idx
    )
    assert ticket_state == WorkTicketState.PENDING.value
    ref_status = await postgres_pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    assert ref_status == ReferenceStatus.PENDING.value


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


async def test_reconcile_schedules_resume_for_non_terminal_only(
    postgres_pool, admin_token, reference_action, monkeypatch
):
    """Startup reconcile schedules a resume dispatch for every non-terminal
    ticket and leaves terminal ones alone — the re-attach replaces the old
    blanket fail-all. Each ticket targets its own reference so the
    one-in-flight-per-reference unique index doesn't fire on insert."""
    from types import SimpleNamespace

    from qiita_control_plane import dispatch

    _, admin_idx = admin_token
    action_id, version = reference_action

    # state → seeded work_ticket_idx, for the three non-terminal states plus
    # the two terminal ones (which must NOT be scheduled).
    seeded: dict[str, int] = {}
    created_refs: list[int] = []
    created_idxs: list[int] = []

    async def _seed(state: str, *, failed: bool = False) -> int:
        ref_idx = await postgres_pool.fetchval(
            "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
            " VALUES ($1, '1.0', 'sequence_reference', 'pending', $2)"
            " RETURNING reference_idx",
            f"wt-reconcile-{uuid.uuid4()}",
            admin_idx,
        )
        created_refs.append(ref_idx)
        if failed:
            idx = await postgres_pool.fetchval(
                "INSERT INTO qiita.work_ticket"
                " (action_id, action_version, originator_principal_idx, scope_target_kind,"
                "  reference_idx, state, failure_type, failure_stage, failure_reason)"
                " VALUES ($1, $2, $3, 'reference', $4, 'failed'::qiita.work_ticket_state,"
                "  'permanent'::qiita.failure_type, 'submission'::qiita.work_ticket_failure_stage,"
                "  'seed')"
                " RETURNING work_ticket_idx",
                action_id,
                version,
                admin_idx,
                ref_idx,
            )
        else:
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
                state,
            )
        created_idxs.append(idx)
        return idx

    try:
        for s in ("pending", "queued", "processing"):
            seeded[s] = await _seed(s)
        completed_idx = await _seed("completed")
        failed_idx = await _seed("failed", failed=True)

        scheduled: list[tuple[int, dict]] = []
        monkeypatch.setattr(
            dispatch,
            "schedule_dispatch",
            lambda app, idx, **kw: scheduled.append((idx, kw)),
        )
        app = SimpleNamespace(
            state=SimpleNamespace(pool=postgres_pool, compute_backend_client=object())
        )
        count = await dispatch.reconcile_inflight_tickets(app)

        scheduled_idxs = {idx for idx, _ in scheduled}
        # All three non-terminal tickets scheduled for resume.
        for s in ("pending", "queued", "processing"):
            assert seeded[s] in scheduled_idxs
        assert all(kw == {"resume": True} for idx, kw in scheduled if idx in seeded.values())
        # Terminal tickets are never scheduled.
        assert completed_idx not in scheduled_idxs
        assert failed_idx not in scheduled_idxs
        # Count covers at least our three (earlier tests may leave orphans).
        assert count >= 3
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


# ---------------------------------------------------------------------------
# GET /api/v1/work-ticket  (list / summary, Phase 6)
# ---------------------------------------------------------------------------


@pytest.fixture
async def ticket_seeder(postgres_pool):
    """Seed work_tickets — each with its own throwaway reference so the
    one-in-flight-per-reference unique index never fires — and optional
    work_ticket_step progress rows, directly via the pool. Returns a
    namespace of `ticket(...)` / `step(...)` coroutines; cleans up every
    seeded ticket (its progress rows cascade) and reference at teardown.

    Seeding through the pool (not the route) lets a test pin an arbitrary
    work_ticket state and an arbitrary current-entry compute shape, which
    the no-op-dispatch route flow can't produce on its own."""
    from types import SimpleNamespace

    refs: list[int] = []
    idxs: list[int] = []

    async def seed_ticket(*, action, originator_idx: int, state: str = "pending") -> int:
        action_id, version = action
        ref_idx = await postgres_pool.fetchval(
            "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
            " VALUES ($1, '1.0', 'sequence_reference', 'pending', $2)"
            " RETURNING reference_idx",
            f"wt-list-{uuid.uuid4()}",
            originator_idx,
        )
        refs.append(ref_idx)
        if state == WorkTicketState.FAILED.value:
            # DB CHECK requires failure_* set when state=failed.
            idx = await postgres_pool.fetchval(
                "INSERT INTO qiita.work_ticket"
                " (action_id, action_version, originator_principal_idx, scope_target_kind,"
                "  reference_idx, state, failure_type, failure_stage, failure_reason)"
                " VALUES ($1, $2, $3, 'reference', $4, 'failed'::qiita.work_ticket_state,"
                "  'permanent'::qiita.failure_type,"
                "  'submission'::qiita.work_ticket_failure_stage, 'seed')"
                " RETURNING work_ticket_idx",
                action_id,
                version,
                originator_idx,
                ref_idx,
            )
        else:
            idx = await postgres_pool.fetchval(
                "INSERT INTO qiita.work_ticket"
                " (action_id, action_version, originator_principal_idx,"
                "  scope_target_kind, reference_idx, state)"
                " VALUES ($1, $2, $3, 'reference', $4, $5::qiita.work_ticket_state)"
                " RETURNING work_ticket_idx",
                action_id,
                version,
                originator_idx,
                ref_idx,
                state,
            )
        idxs.append(idx)
        return idx

    async def seed_step(
        *,
        work_ticket_idx: int,
        compute_target: str,
        state: str,
        slurm_job_id: int | None = None,
        job_name: str | None = None,
        step_index: int = 0,
        attempt: int = 0,
        step_name: str = "step-0",
        failure_kind: str | None = None,
        failure_reason: str | None = None,
    ) -> None:
        await postgres_pool.execute(
            "INSERT INTO qiita.work_ticket_step"
            " (work_ticket_idx, step_index, attempt, step_name, compute_target,"
            "  state, slurm_job_id, job_name, failure_kind, failure_reason)"
            " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            work_ticket_idx,
            step_index,
            attempt,
            step_name,
            compute_target,
            state,
            slurm_job_id,
            job_name,
            failure_kind,
            failure_reason,
        )

    yield SimpleNamespace(ticket=seed_ticket, step=seed_step)

    # work_ticket_step rows cascade on the ticket delete (ON DELETE CASCADE).
    if idxs:
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = ANY($1::bigint[])",
            idxs,
        )
    if refs:
        await postgres_pool.execute(
            "DELETE FROM qiita.reference WHERE reference_idx = ANY($1::bigint[])",
            refs,
        )


def _summary_by_idx(payload: list[dict], idx: int) -> dict | None:
    """Find the summary dict for `idx` in a list response, or None."""
    return next((row for row in payload if row["work_ticket_idx"] == idx), None)


async def test_list_work_ticket_401_on_anonymous(wt_client):
    """No Authorization header → 401, same as the single-ticket GET."""
    resp = await wt_client.get(URL_WORK_TICKET_LIST)
    assert resp.status_code == 401


async def test_list_work_ticket_originator_sees_own_only(
    wt_client, admin_token, regular_token, reference_action, ticket_seeder
):
    """Default (no `all`) scoping is caller-relative: the originating admin
    sees the ticket; an unrelated USER does not (and cannot enumerate it)."""
    _, admin_idx = admin_token
    user_token, _ = regular_token
    admin_tok, _ = admin_token
    idx = await ticket_seeder.ticket(
        action=reference_action, originator_idx=admin_idx, state="processing"
    )

    resp = await wt_client.get(
        URL_WORK_TICKET_LIST, headers={"Authorization": f"Bearer {admin_tok}"}
    )
    assert resp.status_code == 200, resp.text
    assert _summary_by_idx(resp.json(), idx) is not None

    resp = await wt_client.get(
        URL_WORK_TICKET_LIST, headers={"Authorization": f"Bearer {user_token}"}
    )
    assert resp.status_code == 200, resp.text
    assert _summary_by_idx(resp.json(), idx) is None


async def test_list_work_ticket_all_requires_admin_403(wt_client, regular_token):
    """A non-admin requesting the cross-tenant view (`?all=true`) is
    refused — the role gate mirrors the single-ticket wet_lab_admin bypass."""
    user_token, _ = regular_token
    resp = await wt_client.get(
        URL_WORK_TICKET_LIST,
        params={"all": "true"},
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert resp.status_code == 403


async def test_list_work_ticket_admin_all_sees_other_originators(
    wt_client, postgres_pool, admin_token, reference_action, ticket_seeder
):
    """`?all=true` from a wet_lab_admin returns tickets they did not
    originate — the operator-wide view."""
    _, admin_idx = admin_token
    idx = await ticket_seeder.ticket(
        action=reference_action, originator_idx=admin_idx, state="processing"
    )
    wla_token, _ = await _seed_wet_lab_admin_token(postgres_pool, wt_client)

    resp = await wt_client.get(
        URL_WORK_TICKET_LIST,
        params={"all": "true"},
        headers={"Authorization": f"Bearer {wla_token}"},
    )
    assert resp.status_code == 200, resp.text
    assert _summary_by_idx(resp.json(), idx) is not None


async def test_list_work_ticket_reports_current_slurm_entry(
    wt_client, admin_token, reference_action, ticket_seeder
):
    """The current entry = the highest (step_index, attempt) progress row.
    A multi-step ticket whose step 0 (a control_plane action) completed and
    whose step 1 is on its second attempt running a SLURM job reports
    step 1 / slurm / the live job id / running."""
    admin_tok, admin_idx = admin_token
    idx = await ticket_seeder.ticket(
        action=reference_action, originator_idx=admin_idx, state="processing"
    )
    await ticket_seeder.step(
        work_ticket_idx=idx,
        step_index=0,
        attempt=0,
        compute_target=ComputeTarget.CONTROL_PLANE.value,
        state=StepProgressState.COMPLETED.value,
        step_name="prep",
    )
    await ticket_seeder.step(
        work_ticket_idx=idx,
        step_index=1,
        attempt=0,
        compute_target=ComputeTarget.SLURM.value,
        state=StepProgressState.SUBMITTED.value,
        slurm_job_id=4241,
        job_name="qiita-wt-x-a0",
        step_name="align",
    )
    await ticket_seeder.step(
        work_ticket_idx=idx,
        step_index=1,
        attempt=1,
        compute_target=ComputeTarget.SLURM.value,
        state=StepProgressState.RUNNING.value,
        slurm_job_id=4242,
        job_name="qiita-wt-x-a1",
        step_name="align",
    )

    resp = await wt_client.get(
        URL_WORK_TICKET_LIST, headers={"Authorization": f"Bearer {admin_tok}"}
    )
    assert resp.status_code == 200, resp.text
    summary = _summary_by_idx(resp.json(), idx)
    assert summary is not None
    assert summary["current_step_index"] == 1
    assert summary["current_step_name"] == "align"
    assert summary["compute_target"] == ComputeTarget.SLURM.value
    assert summary["slurm_job_id"] == 4242
    assert summary["step_state"] == StepProgressState.RUNNING.value


async def test_list_work_ticket_reports_control_plane_entry_without_job_id(
    wt_client, admin_token, reference_action, ticket_seeder
):
    """An in-process `action:` entry reports control_plane and no job id."""
    admin_tok, admin_idx = admin_token
    idx = await ticket_seeder.ticket(
        action=reference_action, originator_idx=admin_idx, state="processing"
    )
    await ticket_seeder.step(
        work_ticket_idx=idx,
        compute_target=ComputeTarget.CONTROL_PLANE.value,
        state=StepProgressState.RUNNING.value,
        step_name="mint",
    )

    resp = await wt_client.get(
        URL_WORK_TICKET_LIST, headers={"Authorization": f"Bearer {admin_tok}"}
    )
    summary = _summary_by_idx(resp.json(), idx)
    assert summary is not None
    assert summary["compute_target"] == ComputeTarget.CONTROL_PLANE.value
    assert summary["slurm_job_id"] is None
    assert summary["step_state"] == StepProgressState.RUNNING.value


async def test_list_work_ticket_pending_has_no_compute_fields(
    wt_client, admin_token, reference_action, ticket_seeder
):
    """A ticket with no progress rows yet (PENDING before first write-ahead)
    reports all current-entry fields as null."""
    admin_tok, admin_idx = admin_token
    idx = await ticket_seeder.ticket(
        action=reference_action, originator_idx=admin_idx, state="pending"
    )
    resp = await wt_client.get(
        URL_WORK_TICKET_LIST, headers={"Authorization": f"Bearer {admin_tok}"}
    )
    summary = _summary_by_idx(resp.json(), idx)
    assert summary is not None
    assert summary["current_step_index"] is None
    assert summary["current_step_name"] is None
    assert summary["compute_target"] is None
    assert summary["slurm_job_id"] is None
    assert summary["step_state"] is None


async def test_list_work_ticket_state_filter(
    wt_client, admin_token, reference_action, ticket_seeder
):
    """`?state=processing` returns only tickets in that state (scoped to
    the caller's own, so the assertion is exact)."""
    admin_tok, admin_idx = admin_token
    pending_idx = await ticket_seeder.ticket(
        action=reference_action, originator_idx=admin_idx, state="pending"
    )
    processing_idx = await ticket_seeder.ticket(
        action=reference_action, originator_idx=admin_idx, state="processing"
    )
    completed_idx = await ticket_seeder.ticket(
        action=reference_action, originator_idx=admin_idx, state="completed"
    )

    resp = await wt_client.get(
        URL_WORK_TICKET_LIST,
        params={"state": "processing"},
        headers={"Authorization": f"Bearer {admin_tok}"},
    )
    assert resp.status_code == 200, resp.text
    returned = {row["work_ticket_idx"] for row in resp.json()}
    assert processing_idx in returned
    assert pending_idx not in returned
    assert completed_idx not in returned


async def test_list_work_ticket_active_filter(
    wt_client, admin_token, reference_action, ticket_seeder
):
    """`?active=true` returns only non-terminal tickets (excludes COMPLETED
    and FAILED)."""
    admin_tok, admin_idx = admin_token
    pending_idx = await ticket_seeder.ticket(
        action=reference_action, originator_idx=admin_idx, state="pending"
    )
    processing_idx = await ticket_seeder.ticket(
        action=reference_action, originator_idx=admin_idx, state="processing"
    )
    completed_idx = await ticket_seeder.ticket(
        action=reference_action, originator_idx=admin_idx, state="completed"
    )
    failed_idx = await ticket_seeder.ticket(
        action=reference_action, originator_idx=admin_idx, state="failed"
    )

    resp = await wt_client.get(
        URL_WORK_TICKET_LIST,
        params={"active": "true"},
        headers={"Authorization": f"Bearer {admin_tok}"},
    )
    assert resp.status_code == 200, resp.text
    returned = {row["work_ticket_idx"] for row in resp.json()}
    assert {pending_idx, processing_idx} <= returned
    assert completed_idx not in returned
    assert failed_idx not in returned


async def test_list_work_ticket_state_and_active_intersect(
    wt_client, admin_token, reference_action, ticket_seeder
):
    """`state` and `active` AND-compose: `?state=completed&active=true` is a
    valid query that returns the empty intersection (completed is terminal),
    not an error — pins the docstring's contract."""
    admin_tok, admin_idx = admin_token
    completed_idx = await ticket_seeder.ticket(
        action=reference_action, originator_idx=admin_idx, state="completed"
    )
    resp = await wt_client.get(
        URL_WORK_TICKET_LIST,
        params={"state": "completed", "active": "true"},
        headers={"Authorization": f"Bearer {admin_tok}"},
    )
    assert resp.status_code == 200, resp.text
    assert _summary_by_idx(resp.json(), completed_idx) is None


async def test_list_work_ticket_orders_newest_first(
    wt_client, admin_token, reference_action, ticket_seeder
):
    """Results are ordered work_ticket_idx DESC (newest first). Own-scoped,
    so the seeded ids are the only rows and their relative order is exact."""
    admin_tok, admin_idx = admin_token
    seeded = [
        await ticket_seeder.ticket(
            action=reference_action, originator_idx=admin_idx, state="processing"
        )
        for _ in range(3)
    ]
    resp = await wt_client.get(
        URL_WORK_TICKET_LIST, headers={"Authorization": f"Bearer {admin_tok}"}
    )
    assert resp.status_code == 200, resp.text
    returned = [row["work_ticket_idx"] for row in resp.json()]
    assert returned == sorted(seeded, reverse=True)


async def test_list_work_ticket_limit_caps_results(
    wt_client, admin_token, reference_action, ticket_seeder
):
    """`?limit=N` caps the page size (own-scoped, so the count is exact)."""
    admin_tok, admin_idx = admin_token
    for _ in range(3):
        await ticket_seeder.ticket(
            action=reference_action, originator_idx=admin_idx, state="processing"
        )
    resp = await wt_client.get(
        URL_WORK_TICKET_LIST,
        params={"limit": "2"},
        headers={"Authorization": f"Bearer {admin_tok}"},
    )
    assert resp.status_code == 200, resp.text
    assert len(resp.json()) == 2


async def test_list_work_ticket_limit_out_of_range_422(wt_client, admin_token):
    """`limit` is bounded (ge=1, le=max) like the audit-log query param.
    Pin the exact configured boundaries, not arbitrary values, so a change
    to the bound is caught here."""
    from qiita_control_plane.routes.work_ticket import _WORK_TICKET_LIST_MAX_LIMIT

    admin_tok, _ = admin_token
    for bad in ("0", str(_WORK_TICKET_LIST_MAX_LIMIT + 1)):
        resp = await wt_client.get(
            URL_WORK_TICKET_LIST,
            params={"limit": bad},
            headers={"Authorization": f"Bearer {admin_tok}"},
        )
        assert resp.status_code == 422, (bad, resp.text)


# ---------------------------------------------------------------------------
# GET /api/v1/work-ticket/{idx}/step/{step_index}/logs
# ---------------------------------------------------------------------------

_LOGS_STEP_NAME = "stage_local_fasta"


async def _seed_ticket_with_step_logs(
    wt_client,
    postgres_pool,
    token,
    action,
    reference_idx,
    tmp_path,
    *,
    attempts=(0,),
    stderr_by_attempt=None,
    stdout_by_attempt=None,
):
    """Create a real ticket (as `token`'s principal), seed one or more
    `work_ticket_step` rows for step 0, write their on-disk log files under
    `tmp_path`, and point the CP's `path_scratch_ticket` at `tmp_path`. Returns
    the ticket idx. The work_ticket_step rows cascade-delete with the ticket in
    the wt_client teardown."""
    action_id, version = action
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json=_body(action_id, version, reference_idx),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text
    idx = resp.json()["work_ticket_idx"]
    wt_client._created_tickets.append(idx)

    for attempt in attempts:
        # state='failed' requires the (failure_kind, failure_reason) pair per
        # the work_ticket_step_failure_consistent CHECK.
        await postgres_pool.execute(
            "INSERT INTO qiita.work_ticket_step"
            " (work_ticket_idx, step_index, attempt, step_name, compute_target, state,"
            "  failure_kind, failure_reason)"
            " VALUES ($1, 0, $2, $3, 'slurm', 'failed', 'oom_killed', 'seeded for logs test')",
            idx,
            attempt,
            _LOGS_STEP_NAME,
        )
        logs_dir = tmp_path / str(idx) / _LOGS_STEP_NAME / f"attempt-{attempt}" / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        if stderr_by_attempt and attempt in stderr_by_attempt:
            (logs_dir / "stderr").write_text(stderr_by_attempt[attempt])
        if stdout_by_attempt and attempt in stdout_by_attempt:
            (logs_dir / "stdout").write_text(stdout_by_attempt[attempt])

    from qiita_control_plane.main import app

    app.state.settings = dataclasses.replace(app.state.settings, path_scratch_ticket=tmp_path)
    return idx


async def test_get_step_logs_originator_reads_stderr_tail(
    wt_client, postgres_pool, admin_token, reference_action, reference_idx, tmp_path
):
    """The originator gets a step attempt's stdout/stderr tail off shared
    scratch — the no-sudo diagnosis path."""
    token, _ = admin_token
    idx = await _seed_ticket_with_step_logs(
        wt_client,
        postgres_pool,
        token,
        reference_action,
        reference_idx,
        tmp_path,
        stderr_by_attempt={0: "loading reference...\noom_kill event\n"},
        stdout_by_attempt={0: "starting\n"},
    )
    resp = await wt_client.get(
        URL_WORK_TICKET_STEP_LOGS.format(work_ticket_idx=idx, step_index=0),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["work_ticket_idx"] == idx
    assert body["step_index"] == 0
    assert body["attempt"] == 0
    assert body["step_name"] == _LOGS_STEP_NAME
    assert "oom_kill event" in body["stderr"]
    assert "starting" in body["stdout"]
    assert body["stderr_truncated"] is False


async def test_get_step_logs_defaults_to_latest_attempt(
    wt_client, postgres_pool, admin_token, reference_action, reference_idx, tmp_path
):
    """With no `attempt` query param the route returns the highest recorded
    attempt; an explicit `attempt` pins an earlier one."""
    token, _ = admin_token
    idx = await _seed_ticket_with_step_logs(
        wt_client,
        postgres_pool,
        token,
        reference_action,
        reference_idx,
        tmp_path,
        attempts=(0, 1),
        stderr_by_attempt={0: "first attempt\n", 1: "second attempt\n"},
    )
    headers = {"Authorization": f"Bearer {token}"}

    latest = await wt_client.get(
        URL_WORK_TICKET_STEP_LOGS.format(work_ticket_idx=idx, step_index=0), headers=headers
    )
    assert latest.status_code == 200, latest.text
    assert latest.json()["attempt"] == 1
    assert "second attempt" in latest.json()["stderr"]

    pinned = await wt_client.get(
        URL_WORK_TICKET_STEP_LOGS.format(work_ticket_idx=idx, step_index=0),
        params={"attempt": 0},
        headers=headers,
    )
    assert pinned.status_code == 200, pinned.text
    assert pinned.json()["attempt"] == 0
    assert "first attempt" in pinned.json()["stderr"]


async def test_get_step_logs_tail_lines_bounds_and_flags_truncation(
    wt_client, postgres_pool, admin_token, reference_action, reference_idx, tmp_path
):
    """`tail_lines` bounds each stream from the end and sets the truncation
    flag when older lines were dropped."""
    token, _ = admin_token
    body_text = "\n".join(f"line{i}" for i in range(50)) + "\n"
    idx = await _seed_ticket_with_step_logs(
        wt_client,
        postgres_pool,
        token,
        reference_action,
        reference_idx,
        tmp_path,
        stderr_by_attempt={0: body_text},
    )
    resp = await wt_client.get(
        URL_WORK_TICKET_STEP_LOGS.format(work_ticket_idx=idx, step_index=0),
        params={"tail_lines": 3},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["stderr"].splitlines() == ["line47", "line48", "line49"]
    assert body["stderr_truncated"] is True


async def test_get_step_logs_tail_lines_out_of_range_422(
    wt_client, postgres_pool, admin_token, reference_action, reference_idx, tmp_path
):
    """`tail_lines` is bounded (ge=1, le=max); both ends 422, pinned to the
    configured boundary so a change to the bound is caught here."""
    from qiita_control_plane.routes.work_ticket import _STEP_LOGS_MAX_TAIL_LINES

    token, _ = admin_token
    idx = await _seed_ticket_with_step_logs(
        wt_client, postgres_pool, token, reference_action, reference_idx, tmp_path
    )
    headers = {"Authorization": f"Bearer {token}"}
    for bad in ("0", str(_STEP_LOGS_MAX_TAIL_LINES + 1)):
        resp = await wt_client.get(
            URL_WORK_TICKET_STEP_LOGS.format(work_ticket_idx=idx, step_index=0),
            params={"tail_lines": bad},
            headers=headers,
        )
        assert resp.status_code == 422, (bad, resp.text)


async def test_get_step_logs_step_name_with_separator_500(
    wt_client, postgres_pool, admin_token, reference_action, reference_idx, tmp_path
):
    """A step_name carrying a path separator (would escape the ticket root) is a
    contract violation — the route fails loud with 500, never reads the file."""
    token, _ = admin_token
    action_id, version = reference_action
    resp = await wt_client.post(
        URL_WORK_TICKET_PREFIX,
        json=_body(action_id, version, reference_idx),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text
    idx = resp.json()["work_ticket_idx"]
    wt_client._created_tickets.append(idx)
    await postgres_pool.execute(
        "INSERT INTO qiita.work_ticket_step"
        " (work_ticket_idx, step_index, attempt, step_name, compute_target, state,"
        "  failure_kind, failure_reason)"
        " VALUES ($1, 0, 0, $2, 'slurm', 'failed', 'oom_killed', 'seeded')",
        idx,
        "evil/../etc",
    )
    from qiita_control_plane.main import app

    app.state.settings = dataclasses.replace(app.state.settings, path_scratch_ticket=tmp_path)
    got = await wt_client.get(
        URL_WORK_TICKET_STEP_LOGS.format(work_ticket_idx=idx, step_index=0),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert got.status_code == 500, got.text


async def test_get_step_logs_missing_log_file_is_empty_not_error(
    wt_client, postgres_pool, admin_token, reference_action, reference_idx, tmp_path
):
    """A step that never wrote a stream comes back as an empty string, 200 —
    not a 404 / 500."""
    token, _ = admin_token
    idx = await _seed_ticket_with_step_logs(
        wt_client,
        postgres_pool,
        token,
        reference_action,
        reference_idx,
        tmp_path,
        stderr_by_attempt={0: "only stderr here\n"},
        # no stdout file written
    )
    resp = await wt_client.get(
        URL_WORK_TICKET_STEP_LOGS.format(work_ticket_idx=idx, step_index=0),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["stdout"] == ""
    assert resp.json()["stderr"] == "only stderr here"


async def test_get_step_logs_unknown_step_or_attempt_404(
    wt_client, postgres_pool, admin_token, reference_action, reference_idx, tmp_path
):
    """An out-of-range step index and an unknown attempt both 404 (the same
    response a genuinely missing ticket returns)."""
    token, _ = admin_token
    idx = await _seed_ticket_with_step_logs(
        wt_client, postgres_pool, token, reference_action, reference_idx, tmp_path
    )
    headers = {"Authorization": f"Bearer {token}"}

    bad_step = await wt_client.get(
        URL_WORK_TICKET_STEP_LOGS.format(work_ticket_idx=idx, step_index=9), headers=headers
    )
    assert bad_step.status_code == 404, bad_step.text

    bad_attempt = await wt_client.get(
        URL_WORK_TICKET_STEP_LOGS.format(work_ticket_idx=idx, step_index=0),
        params={"attempt": 99},
        headers=headers,
    )
    assert bad_attempt.status_code == 404, bad_attempt.text


async def test_get_step_logs_missing_ticket_404(wt_client, admin_token):
    """An unknown ticket idx → 404."""
    token, _ = admin_token
    resp = await wt_client.get(
        URL_WORK_TICKET_STEP_LOGS.format(work_ticket_idx=99_999_999, step_index=0),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 404, resp.text


async def test_get_step_logs_anonymous_401(
    wt_client, postgres_pool, admin_token, reference_action, reference_idx, tmp_path
):
    """No credentials → 401, before any ownership disclosure."""
    token, _ = admin_token
    idx = await _seed_ticket_with_step_logs(
        wt_client, postgres_pool, token, reference_action, reference_idx, tmp_path
    )
    resp = await wt_client.get(URL_WORK_TICKET_STEP_LOGS.format(work_ticket_idx=idx, step_index=0))
    assert resp.status_code == 401, resp.text


async def test_get_step_logs_non_owner_404(
    wt_client, postgres_pool, admin_token, regular_token, reference_action, reference_idx, tmp_path
):
    """A non-originator without the bypass role gets 404, not 403 — the same
    enumeration-safe response GET /work-ticket/{idx} returns."""
    owner_tok, _ = admin_token
    other_tok, _ = regular_token
    idx = await _seed_ticket_with_step_logs(
        wt_client, postgres_pool, owner_tok, reference_action, reference_idx, tmp_path
    )
    resp = await wt_client.get(
        URL_WORK_TICKET_STEP_LOGS.format(work_ticket_idx=idx, step_index=0),
        headers={"Authorization": f"Bearer {other_tok}"},
    )
    assert resp.status_code == 404, resp.text


async def test_get_step_logs_unconfigured_scratch_500(
    wt_client, postgres_pool, admin_token, reference_action, reference_idx, tmp_path
):
    """A CP with no `path_scratch_ticket` configured is a misconfigured deploy,
    not a client error — fail loud with 500."""
    token, _ = admin_token
    idx = await _seed_ticket_with_step_logs(
        wt_client, postgres_pool, token, reference_action, reference_idx, tmp_path
    )
    from qiita_control_plane.main import app

    app.state.settings = dataclasses.replace(app.state.settings, path_scratch_ticket=None)
    resp = await wt_client.get(
        URL_WORK_TICKET_STEP_LOGS.format(work_ticket_idx=idx, step_index=0),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 500, resp.text
