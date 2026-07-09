"""DB tests for DELETE /alignment-definition/{alignment_idx} — full alignment
purge (the disallow-without-delete escape hatch), the alignment analog of
test_mask_definition_delete.py.

Exercises the Postgres-teardown + gating half against a real Postgres. The
data-plane DoAction (DuckLake `alignment` cleanup) is stubbed — this tier has no
data plane — so these tests focus on what the control plane owns: authorization,
the 404, the lake-first ordering, and that deleting the alignment_definition row
CASCADE-deletes its alignment_sample gate + detaches referencing work_tickets.
"""

import json
import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_ALIGNMENT_DEFINITION_BY_IDX

pytestmark = pytest.mark.db


class _RecordingDeleteAlignmentData:
    """Records each `delete_alignment_data` call's `alignment_idx` and returns a
    fixed rows-deleted count, so a test can both neutralize the (absent) data plane
    and assert the route dialed the lake delete for the right alignment once."""

    def __init__(self, rows_deleted=5):
        self.rows_deleted = rows_deleted
        self.calls: list[int] = []

    async def __call__(self, *, alignment_idx, hmac_secret, data_plane_url):
        self.calls.append(alignment_idx)
        return self.rows_deleted


@pytest.fixture(autouse=True)
def stub_data_plane(monkeypatch):
    """Neutralize the data-plane DoAction: the DB tier has no Flight server. Patches
    the name bound in the route module so the route's `delete_alignment_data(...)`
    call is a recording stub, leaving the Postgres teardown intact."""
    stub = _RecordingDeleteAlignmentData()
    monkeypatch.setattr("qiita_control_plane.routes.alignment.delete_alignment_data", stub)
    return stub


def _install_settings(app):
    from qiita_control_plane.config import Settings

    app.state.settings = Settings(
        database_url="unused", hmac_secret_key=b"\x00" * 32, data_plane_url="unused"
    )


@pytest.fixture
async def client(postgres_pool, human_admin_session):
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    _install_settings(app)
    created: list[int] = []

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {human_admin_session['token']}"},
    ) as ac:
        ac._created = created
        yield ac

    if created:
        await postgres_pool.execute(
            "DELETE FROM qiita.alignment_definition WHERE alignment_idx = ANY($1::bigint[])",
            created,
        )


async def _seed_alignment(pool, created=None):
    """Insert a minimal qiita.alignment_definition row, returning its alignment_idx."""
    alignment_idx = await pool.fetchval(
        "INSERT INTO qiita.alignment_definition (params_hash, params, created_by_idx)"
        " VALUES ($1, '{}'::jsonb, (SELECT MIN(idx) FROM qiita.principal))"
        " RETURNING alignment_idx",
        uuid.uuid4().bytes + uuid.uuid4().bytes,  # 32-byte params_hash
    )
    if created is not None:
        created.append(alignment_idx)
    return alignment_idx


async def test_delete_alignment_happy_path(client, postgres_pool, stub_data_plane):
    alignment_idx = await _seed_alignment(postgres_pool, client._created)

    resp = await client.delete(URL_ALIGNMENT_DEFINITION_BY_IDX.format(alignment_idx=alignment_idx))

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["alignment_idx"] == alignment_idx
    assert body["rows_deleted"] == 5  # from the stub
    assert stub_data_plane.calls == [alignment_idx]
    assert (
        await postgres_pool.fetchval(
            "SELECT 1 FROM qiita.alignment_definition WHERE alignment_idx = $1", alignment_idx
        )
        is None
    )


async def test_delete_alignment_cascades_gate(
    client, postgres_pool, stub_data_plane, human_admin_session
):
    """Deleting the alignment_definition row CASCADE-deletes its alignment_sample
    gate rows (so a fresh align plan can re-create them PENDING — the reset the
    disallow-without-delete rule needs)."""
    from qiita_control_plane.testing.db_seeds import seed_biosample_with_sequenced_prep_sample

    owner = human_admin_session["principal_idx"]
    alignment_idx = await _seed_alignment(postgres_pool, client._created)
    _bs, prep = await seed_biosample_with_sequenced_prep_sample(postgres_pool, owner_idx=owner)
    await postgres_pool.execute(
        "INSERT INTO qiita.alignment_sample (alignment_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, 'completed')",
        alignment_idx,
        prep,
    )
    try:
        resp = await client.delete(
            URL_ALIGNMENT_DEFINITION_BY_IDX.format(alignment_idx=alignment_idx)
        )
        assert resp.status_code == 200, resp.text
        # Gate row cascade-deleted with the definition.
        assert (
            await postgres_pool.fetchval(
                "SELECT count(*) FROM qiita.alignment_sample WHERE alignment_idx = $1",
                alignment_idx,
            )
            == 0
        )
    finally:
        await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", prep)
        await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", _bs)


async def test_delete_alignment_detaches_work_ticket(client, postgres_pool):
    """A work_ticket referencing the alignment has its alignment_idx nulled (FK ON
    DELETE SET NULL) rather than blocking the delete."""
    alignment_idx = await _seed_alignment(postgres_pool, client._created)
    ref_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', (SELECT MIN(idx) FROM qiita.principal))"
        " RETURNING reference_idx",
        f"align-detach-{uuid.uuid4()}",
    )
    action_id = "align-delete-test-action"
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
        "  scope_target_kind, reference_idx, alignment_idx, state)"
        " VALUES ($1, $2, (SELECT MIN(idx) FROM qiita.principal),"
        "         'reference', $3, $4, 'completed'::qiita.work_ticket_state)"
        " RETURNING work_ticket_idx",
        action_id,
        action_version,
        ref_idx,
        alignment_idx,
    )
    try:
        resp = await client.delete(
            URL_ALIGNMENT_DEFINITION_BY_IDX.format(alignment_idx=alignment_idx)
        )
        assert resp.status_code == 200, resp.text
        assert (
            await postgres_pool.fetchval(
                "SELECT alignment_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1", wt_idx
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


async def test_delete_alignment_not_found(client):
    resp = await client.delete(URL_ALIGNMENT_DEFINITION_BY_IDX.format(alignment_idx=99_999_999))
    assert resp.status_code == 404


async def test_delete_alignment_requires_delete_scope(postgres_pool, wet_lab_admin_session):
    """wet_lab_admin can submit align runs but does NOT hold
    alignment_definition:delete → 403, and the row survives."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    _install_settings(app)
    alignment_idx = await _seed_alignment(postgres_pool)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={"Authorization": f"Bearer {wet_lab_admin_session['token']}"},
        ) as ac:
            resp = await ac.delete(
                URL_ALIGNMENT_DEFINITION_BY_IDX.format(alignment_idx=alignment_idx)
            )
        assert resp.status_code == 403
        assert "alignment_definition:delete" in resp.json()["detail"]
        assert (
            await postgres_pool.fetchval(
                "SELECT 1 FROM qiita.alignment_definition WHERE alignment_idx = $1", alignment_idx
            )
            == 1
        )
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1", alignment_idx
        )


async def test_delete_alignment_data_plane_failure_returns_502(client, postgres_pool, monkeypatch):
    """A data-plane FlightError surfaces as 502 and leaves Postgres untouched (the
    Postgres delete runs only after the DuckLake delete succeeds)."""
    import pyarrow.flight as flight

    alignment_idx = await _seed_alignment(postgres_pool, client._created)

    async def _boom(*, alignment_idx, hmac_secret, data_plane_url):
        raise flight.FlightUnavailableError("data plane down")

    monkeypatch.setattr("qiita_control_plane.routes.alignment.delete_alignment_data", _boom)

    resp = await client.delete(URL_ALIGNMENT_DEFINITION_BY_IDX.format(alignment_idx=alignment_idx))
    assert resp.status_code == 502
    assert (
        await postgres_pool.fetchval(
            "SELECT 1 FROM qiita.alignment_definition WHERE alignment_idx = $1", alignment_idx
        )
        == 1
    )
