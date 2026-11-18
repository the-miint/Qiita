"""`POST /work-ticket/{idx}/run` refuses a ticket whose krepp build was removed, and
lets one whose build is retired run (repositories.reference_index)."""

import secrets

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_WORK_TICKET_RUN

pytestmark = pytest.mark.db

_ACTION_ID = "krepp-redrive-test"
_VERSION = "1.0.0"
_WT = "qiita_control_plane.routes.work_ticket"


@pytest_asyncio.fixture
async def client(postgres_pool, human_admin_session):
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    saved_settings = getattr(app.state, "settings", None)
    saved_backend = getattr(app.state, "compute_backend_client", None)
    app.state.settings = Settings(
        database_url="unused", flight_signing_key=b"\x00" * 32, data_plane_url="unused"
    )
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
async def failed_ticket_on_build(postgres_pool, human_admin_session, monkeypatch):
    """A failed reference-scoped ticket fixed to a krepp build; yields a setter for
    the build's state. /run's action load and dispatch are stubbed: this test has no
    workflow and no compute backend."""
    pool = postgres_pool
    owner = human_admin_session["principal_idx"]
    reference_idx = await pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1', 'sequence_reference', 'active', $2) RETURNING reference_idx",
        f"krepp-redrive-{secrets.token_hex(4)}",
        owner,
    )
    await pool.execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status)"
        " VALUES ($1, $2, 'reference', '{}'::text[], $3::jsonb, '{}'::jsonb, '[]'::jsonb,"
        "         1, 1, '1 minute', NULL, NULL)"
        " ON CONFLICT (action_id, version) DO NOTHING",
        _ACTION_ID,
        _VERSION,
        '{"service": false, "human_roles": ["system_admin"]}',
    )
    build = await pool.fetchval(
        "INSERT INTO qiita.reference_index"
        " (reference_idx, index_type, fs_path, params, state, fs_path_root)"
        " VALUES ($1, 'krepp', 'references/x/krepp/a', '{}'::jsonb, 'retired', 'node_local')"
        " RETURNING reference_index_idx",
        reference_idx,
    )
    ticket = await pool.fetchval(
        "INSERT INTO qiita.work_ticket (action_id, action_version, originator_principal_idx,"
        " scope_target_kind, reference_idx, action_context, state, reference_index_idx,"
        " failure_type, failure_stage, failure_reason)"
        " VALUES ($1, $2, $3, 'reference', $4, '{}'::jsonb, 'failed', $5,"
        " 'permanent', 'submission', 'test') RETURNING work_ticket_idx",
        _ACTION_ID,
        _VERSION,
        owner,
        reference_idx,
        build,
    )

    class _Definition:
        steps: list = []

    async def _definition(*_a, **_k):
        return _Definition()

    dispatched: list[int] = []
    monkeypatch.setattr(f"{_WT}._fetch_action", _definition)
    monkeypatch.setattr(f"{_WT}.check_step_progress", lambda *_a, **_k: None)
    monkeypatch.setattr(f"{_WT}.schedule_dispatch", lambda _app, idx: dispatched.append(idx))

    async def set_state(state: str) -> None:
        await pool.execute(
            "UPDATE qiita.reference_index SET state = $2 WHERE reference_index_idx = $1",
            build,
            state,
        )

    yield {"ticket": ticket, "build": build, "set_state": set_state, "dispatched": dispatched}
    await pool.execute("DELETE FROM qiita.work_ticket WHERE reference_idx = $1", reference_idx)
    await pool.execute("DELETE FROM qiita.reference_index WHERE reference_idx = $1", reference_idx)
    await pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx)


async def test_run_refuses_a_ticket_whose_build_was_removed(client, failed_ticket_on_build):
    w = failed_ticket_on_build
    await w["set_state"]("removed")
    resp = await client.post(URL_WORK_TICKET_RUN.format(work_ticket_idx=w["ticket"]))
    assert resp.status_code == 409
    assert f"krepp build {w['build']}, which was removed" in resp.json()["detail"]
    assert w["dispatched"] == []


async def test_run_redrives_a_ticket_whose_build_is_retired(client, failed_ticket_on_build):
    """The control: the same ticket on a retired build is still served."""
    w = failed_ticket_on_build
    resp = await client.post(URL_WORK_TICKET_RUN.format(work_ticket_idx=w["ticket"]))
    assert resp.is_success, resp.text
    assert w["dispatched"] == [w["ticket"]]
