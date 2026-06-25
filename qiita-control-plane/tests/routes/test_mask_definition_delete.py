"""DB tests for DELETE /mask-definition/{mask_idx} — full mask purge.

Exercises the Postgres-teardown + gating half of the delete flow against a
real Postgres. The data-plane DoAction (DuckLake `read_mask` cleanup) is stubbed
— this tier has no data plane — so these tests focus on what the control plane
owns: authorization, the 404, and the Postgres row teardown (lake-first, then
Postgres).
"""

import json
import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_MASK_DEFINITION_BY_IDX

pytestmark = pytest.mark.db


class _RecordingDeleteMaskData:
    """Records each `delete_mask_data` call's `mask_idx` and returns a fixed
    rows-deleted count, so a test can both neutralize the (absent) data plane and
    assert the route dialed the lake delete for the right mask exactly once."""

    def __init__(self, rows_deleted=7):
        self.rows_deleted = rows_deleted
        self.calls: list[int] = []

    async def __call__(self, *, mask_idx, hmac_secret, data_plane_url):
        self.calls.append(mask_idx)
        return self.rows_deleted


@pytest.fixture(autouse=True)
def stub_data_plane(monkeypatch):
    """Neutralize the data-plane DoAction: the DB tier has no Flight server.

    Patches the name bound in the route module so the route's own
    `delete_mask_data(...)` call is a recording stub returning a rows-deleted
    count, leaving the Postgres teardown (the thing under test) intact. Yields
    the stub so a test can assert how the route called it."""

    stub = _RecordingDeleteMaskData()
    monkeypatch.setattr("qiita_control_plane.routes.read_masked.delete_mask_data", stub)
    return stub


def _install_settings(app):
    """Minimal Settings so get_hmac_secret / get_data_plane_url resolve. The
    data-plane URL is never dialed — `delete_mask_data` is stubbed."""
    from qiita_control_plane.config import Settings

    app.state.settings = Settings(
        database_url="unused",
        hmac_secret_key=b"\x00" * 32,
        data_plane_url="unused",
    )


@pytest.fixture
async def client(postgres_pool, human_admin_session):
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    _install_settings(app)
    created_masks: list[int] = []

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {human_admin_session['token']}"},
    ) as ac:
        ac._created_masks = created_masks
        yield ac

    if created_masks:
        await postgres_pool.execute(
            "DELETE FROM qiita.mask_definition WHERE mask_idx = ANY($1::bigint[])",
            created_masks,
        )


async def _seed_mask(pool, created=None):
    """Insert a minimal qiita.mask_definition row, returning its mask_idx."""
    mask_idx = await pool.fetchval(
        "INSERT INTO qiita.mask_definition"
        " (params_hash, filter_workflow, filter_version, params, created_by_idx)"
        " VALUES ($1, 'read-mask', '1.0.0', '{}'::jsonb,"
        "         (SELECT MIN(idx) FROM qiita.principal))"
        " RETURNING mask_idx",
        uuid.uuid4().bytes + uuid.uuid4().bytes,  # 32-byte params_hash
    )
    if created is not None:
        created.append(mask_idx)
    return mask_idx


async def test_delete_mask_happy_path(client, postgres_pool, stub_data_plane):
    mask_idx = await _seed_mask(postgres_pool, client._created_masks)

    resp = await client.delete(URL_MASK_DEFINITION_BY_IDX.format(mask_idx=mask_idx))

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["mask_idx"] == mask_idx
    assert body["rows_deleted"] == 7  # from the stub
    # The route dialed the lake delete exactly once, for the deleted mask.
    assert stub_data_plane.calls == [mask_idx]
    # Gone from Postgres.
    assert (
        await postgres_pool.fetchval(
            "SELECT 1 FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx
        )
        is None
    )


async def test_delete_mask_idempotent_when_lake_already_empty(
    client, postgres_pool, stub_data_plane
):
    """`rows_deleted == 0` (lake rows already gone) still returns 200 and still
    deletes the Postgres mask_definition row — the PG-present / lake-empty path."""
    stub_data_plane.rows_deleted = 0
    mask_idx = await _seed_mask(postgres_pool, client._created_masks)

    resp = await client.delete(URL_MASK_DEFINITION_BY_IDX.format(mask_idx=mask_idx))

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["mask_idx"] == mask_idx
    assert body["rows_deleted"] == 0
    assert stub_data_plane.calls == [mask_idx]
    # Postgres row is still removed even though the lake delete touched 0 rows.
    assert (
        await postgres_pool.fetchval(
            "SELECT 1 FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx
        )
        is None
    )


async def test_delete_mask_not_found(client):
    resp = await client.delete(URL_MASK_DEFINITION_BY_IDX.format(mask_idx=99_999_999))
    assert resp.status_code == 404


async def test_delete_mask_detaches_work_ticket(client, postgres_pool):
    """A work_ticket referencing the mask has its mask_idx nulled (FK ON DELETE
    SET NULL) rather than blocking the delete."""
    mask_idx = await _seed_mask(postgres_pool, client._created_masks)
    # The work_ticket scope-target CHECK requires a consistent target; use a
    # `reference` ticket (single seeded reference row) — the scope_target_kind is
    # orthogonal to the mask FK behaviour under test.
    ref_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', (SELECT MIN(idx) FROM qiita.principal))"
        " RETURNING reference_idx",
        f"mask-detach-{uuid.uuid4()}",
    )
    action_id = "mask-delete-test-action"
    action_version = f"v-{uuid.uuid4()}"
    await postgres_pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, scopes, audience, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling"
        ") VALUES ($1, $2, 'reference', '{}'::text[], $3::jsonb, '[]'::jsonb,"
        "          1, 1, '1 minute')",
        action_id,
        action_version,
        json.dumps({"service": False, "human_roles": ["system_admin"]}),
    )
    wt_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, mask_idx, state)"
        " VALUES ($1, $2, (SELECT MIN(idx) FROM qiita.principal),"
        "         'reference', $3, $4, 'completed'::qiita.work_ticket_state)"
        " RETURNING work_ticket_idx",
        action_id,
        action_version,
        ref_idx,
        mask_idx,
    )
    try:
        resp = await client.delete(URL_MASK_DEFINITION_BY_IDX.format(mask_idx=mask_idx))
        assert resp.status_code == 200, resp.text
        # Ticket survives with mask_idx nulled.
        assert (
            await postgres_pool.fetchval(
                "SELECT mask_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1", wt_idx
            )
            is None
        )
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", wt_idx
        )
        await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", ref_idx)
        await postgres_pool.execute(
            "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
            action_id,
            action_version,
        )


async def test_delete_mask_requires_delete_scope(postgres_pool, wet_lab_admin_session):
    """wet_lab_admin holds neither reference:delete nor mask_definition:delete → 403."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    _install_settings(app)
    mask_idx = await _seed_mask(postgres_pool)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={"Authorization": f"Bearer {wet_lab_admin_session['token']}"},
        ) as ac:
            resp = await ac.delete(URL_MASK_DEFINITION_BY_IDX.format(mask_idx=mask_idx))
        assert resp.status_code == 403
        assert "mask_definition:delete" in resp.json()["detail"]
        # Still present — the 403 fired before any teardown.
        assert (
            await postgres_pool.fetchval(
                "SELECT 1 FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx
            )
            == 1
        )
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx
        )


async def test_delete_mask_data_plane_failure_returns_502(client, postgres_pool, monkeypatch):
    """A data-plane FlightError surfaces as 502 and leaves Postgres untouched
    (the Postgres delete runs only after the DuckLake delete succeeds)."""
    import pyarrow.flight as flight

    mask_idx = await _seed_mask(postgres_pool, client._created_masks)

    async def _boom(*, mask_idx, hmac_secret, data_plane_url):
        raise flight.FlightUnavailableError("data plane down")

    monkeypatch.setattr("qiita_control_plane.routes.read_masked.delete_mask_data", _boom)

    resp = await client.delete(URL_MASK_DEFINITION_BY_IDX.format(mask_idx=mask_idx))
    assert resp.status_code == 502
    # Mask row survives — safe to retry once the data plane is back.
    assert (
        await postgres_pool.fetchval(
            "SELECT 1 FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx
        )
        == 1
    )
