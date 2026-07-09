"""DB tests for DELETE /reference/{idx} — full reference purge.

Exercises the Postgres-teardown + gating half of the delete flow against a
real Postgres. The data-plane DoAction (DuckLake cleanup) is stubbed — this
tier has no data plane — so these tests focus on what the control plane owns:
authorization, work-ticket gating, and orphan-vs-shared feature GC.
"""

import json
import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_REFERENCE_BY_IDX, URL_REFERENCE_PREFIX

pytestmark = pytest.mark.db


@pytest.fixture(autouse=True)
def _stub_data_plane(monkeypatch):
    """Neutralize the data-plane DoAction: the DB tier has no Flight server.

    Patches the name bound in the route module so the route's own
    `delete_reference_data(...)` call is the no-op, leaving the Postgres
    teardown (the thing under test) intact."""

    async def _noop(*, reference_idx, hmac_secret, data_plane_url):
        return {"reference_idx": reference_idx}

    monkeypatch.setattr("qiita_control_plane.routes.reference.delete_reference_data", _noop)


def _install_settings(app):
    """Minimal Settings so get_hmac_secret / get_data_plane_url resolve. The
    data-plane URL is never dialed — `delete_reference_data` is stubbed."""
    from qiita_control_plane.config import Settings

    app.state.settings = Settings(
        database_url="unused",
        flight_signing_key=b"\x00" * 32,
        data_plane_url="unused",
    )


@pytest.fixture
async def client(postgres_pool, human_admin_session):
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    _install_settings(app)
    created_refs: list[int] = []

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {human_admin_session['token']}"},
    ) as ac:
        ac._created_refs = created_refs
        yield ac

    if created_refs:
        for table in ("reference_index", "reference_membership", "work_ticket"):
            await postgres_pool.execute(
                f"DELETE FROM qiita.{table} WHERE reference_idx = ANY($1::bigint[])",
                created_refs,
            )
        await postgres_pool.execute(
            "DELETE FROM qiita.reference WHERE reference_idx = ANY($1::bigint[])",
            created_refs,
        )


async def _create_ref(client, name, version="1.0"):
    resp = await client.post(
        URL_REFERENCE_PREFIX,
        json={"name": name, "version": version, "kind": "sequence_reference"},
    )
    if resp.status_code == 201:
        client._created_refs.append(resp.json()["reference_idx"])
    return resp.json()["reference_idx"]


async def _seed_work_ticket(pool, ref_idx, state):
    """Insert a minimal action + work_ticket in `state` scoped to ref_idx."""
    action_id = "ref-delete-test-action"
    version = f"v-{uuid.uuid4()}"
    await pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, scopes, audience, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling"
        ") VALUES ($1, $2, 'reference', $3::text[], $4::jsonb, $5::jsonb,"
        "          1, 1, '1 minute')",
        action_id,
        version,
        ["reference:write"],
        json.dumps({"service": False, "human_roles": ["system_admin"]}),
        json.dumps([]),
    )
    wt_idx = await pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, state)"
        " VALUES ($1, $2, (SELECT MIN(idx) FROM qiita.principal),"
        "         'reference', $3, $4::qiita.work_ticket_state)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        ref_idx,
        state,
    )
    return wt_idx, action_id, version


async def test_delete_reference_happy_path(client, postgres_pool):
    ref_idx = await _create_ref(client, f"del-happy-{uuid.uuid4()}")

    resp = await client.delete(URL_REFERENCE_BY_IDX.format(reference_idx=ref_idx))

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["reference_idx"] == ref_idx
    assert body["orphan_feature_count"] == 0
    # Gone from Postgres.
    assert (
        await postgres_pool.fetchval(
            "SELECT 1 FROM qiita.reference WHERE reference_idx = $1", ref_idx
        )
        is None
    )
    # A second GET 404s.
    follow = await client.get(URL_REFERENCE_BY_IDX.format(reference_idx=ref_idx))
    assert follow.status_code == 404


async def test_delete_reference_not_found(client):
    resp = await client.delete(URL_REFERENCE_BY_IDX.format(reference_idx=99_999_999))
    assert resp.status_code == 404


async def test_delete_reference_requires_delete_scope(postgres_pool, wet_lab_admin_session):
    """wet_lab_admin holds reference:write but NOT reference:delete → 403."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    _install_settings(app)
    ref_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', (SELECT MIN(idx) FROM qiita.principal))"
        " RETURNING reference_idx",
        f"del-scope-{uuid.uuid4()}",
    )
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={"Authorization": f"Bearer {wet_lab_admin_session['token']}"},
        ) as ac:
            resp = await ac.delete(URL_REFERENCE_BY_IDX.format(reference_idx=ref_idx))
        assert resp.status_code == 403
        assert "reference:delete" in resp.json()["detail"]
        # Still present — the 403 fired before any teardown.
        assert (
            await postgres_pool.fetchval(
                "SELECT 1 FROM qiita.reference WHERE reference_idx = $1", ref_idx
            )
            == 1
        )
    finally:
        await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", ref_idx)


async def test_delete_reference_orphan_vs_shared_features(client, postgres_pool):
    """A feature claimed by another reference survives; a feature claimed only
    by the deleted reference is GC'd from qiita.feature."""
    ref_a = await _create_ref(client, f"del-orphan-a-{uuid.uuid4()}")
    ref_b = await _create_ref(client, f"del-orphan-b-{uuid.uuid4()}")

    shared = await postgres_pool.fetchval(
        "INSERT INTO qiita.feature (sequence_hash) VALUES (gen_random_uuid()) RETURNING feature_idx"
    )
    orphan = await postgres_pool.fetchval(
        "INSERT INTO qiita.feature (sequence_hash) VALUES (gen_random_uuid()) RETURNING feature_idx"
    )
    await postgres_pool.executemany(
        "INSERT INTO qiita.reference_membership (reference_idx, feature_idx) VALUES ($1, $2)",
        [(ref_a, shared), (ref_a, orphan), (ref_b, shared)],
    )

    try:
        resp = await client.delete(URL_REFERENCE_BY_IDX.format(reference_idx=ref_a))
        assert resp.status_code == 200, resp.text
        assert resp.json()["orphan_feature_count"] == 1
        assert resp.json()["membership_deleted"] == 2

        # Orphan feature GC'd; shared feature retained.
        assert (
            await postgres_pool.fetchval(
                "SELECT 1 FROM qiita.feature WHERE feature_idx = $1", orphan
            )
            is None
        )
        assert (
            await postgres_pool.fetchval(
                "SELECT 1 FROM qiita.feature WHERE feature_idx = $1", shared
            )
            == 1
        )
        # ref_b still claims the shared feature.
        assert (
            await postgres_pool.fetchval(
                "SELECT count(*) FROM qiita.reference_membership WHERE reference_idx = $1",
                ref_b,
            )
            == 1
        )
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_membership WHERE feature_idx = ANY($1::bigint[])",
            [shared, orphan],
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])",
            [shared, orphan],
        )


async def test_delete_reference_blocked_by_inflight_ticket(client, postgres_pool):
    """An in-flight work ticket blocks the delete even with force=true."""
    ref_idx = await _create_ref(client, f"del-inflight-{uuid.uuid4()}")
    await _seed_work_ticket(postgres_pool, ref_idx, "processing")

    resp = await client.delete(URL_REFERENCE_BY_IDX.format(reference_idx=ref_idx))
    assert resp.status_code == 409
    assert "in-flight" in resp.json()["detail"]

    forced = await client.delete(
        URL_REFERENCE_BY_IDX.format(reference_idx=ref_idx), params={"force": "true"}
    )
    assert forced.status_code == 409, "in-flight blocks even when forced"
    # Untouched.
    assert (
        await postgres_pool.fetchval(
            "SELECT 1 FROM qiita.reference WHERE reference_idx = $1", ref_idx
        )
        == 1
    )


async def test_delete_reference_terminal_ticket_requires_force(client, postgres_pool):
    """A completed work ticket blocks delete without force, allows it with."""
    ref_idx = await _create_ref(client, f"del-terminal-{uuid.uuid4()}")
    await _seed_work_ticket(postgres_pool, ref_idx, "completed")

    blocked = await client.delete(URL_REFERENCE_BY_IDX.format(reference_idx=ref_idx))
    assert blocked.status_code == 409
    assert "force=true" in blocked.json()["detail"]

    forced = await client.delete(
        URL_REFERENCE_BY_IDX.format(reference_idx=ref_idx), params={"force": "true"}
    )
    assert forced.status_code == 200, forced.text
    assert forced.json()["work_ticket_deleted"] == 1
    assert (
        await postgres_pool.fetchval(
            "SELECT 1 FROM qiita.reference WHERE reference_idx = $1", ref_idx
        )
        is None
    )


async def test_delete_reference_gcs_orphan_genome_keeps_shared(client, postgres_pool):
    """The genome-GC branch: a genome mapped only by orphaned features is
    deleted; a genome still mapped by a surviving reference's feature stays."""
    ref_a = await _create_ref(client, f"del-genome-a-{uuid.uuid4()}")
    ref_b = await _create_ref(client, f"del-genome-b-{uuid.uuid4()}")

    async def _feature():
        return await postgres_pool.fetchval(
            "INSERT INTO qiita.feature (sequence_hash) VALUES (gen_random_uuid())"
            " RETURNING feature_idx"
        )

    async def _genome(suffix):
        return await postgres_pool.fetchval(
            "INSERT INTO qiita.genome (source, source_id) VALUES ('test', $1) RETURNING genome_idx",
            f"{suffix}-{uuid.uuid4()}",
        )

    orphan_feat = await _feature()  # ref_a only → orphaned
    orphan_feat2 = await _feature()  # ref_a only → orphaned
    survivor_feat = await _feature()  # ref_b → survives
    genome_shared = await _genome("shared")  # mapped by orphan_feat AND survivor
    genome_orphan = await _genome("orphan")  # mapped by orphan_feat2 only

    await postgres_pool.executemany(
        "INSERT INTO qiita.reference_membership (reference_idx, feature_idx) VALUES ($1, $2)",
        [(ref_a, orphan_feat), (ref_a, orphan_feat2), (ref_b, survivor_feat)],
    )
    await postgres_pool.executemany(
        "INSERT INTO qiita.feature_genome (feature_idx, genome_idx) VALUES ($1, $2)",
        [
            (orphan_feat, genome_shared),
            (orphan_feat2, genome_orphan),
            (survivor_feat, genome_shared),
        ],
    )

    try:
        resp = await client.delete(URL_REFERENCE_BY_IDX.format(reference_idx=ref_a))
        assert resp.status_code == 200, resp.text
        assert resp.json()["orphan_feature_count"] == 2

        # Orphan genome GC'd; shared genome retained (survivor_feat still maps it).
        assert (
            await postgres_pool.fetchval(
                "SELECT 1 FROM qiita.genome WHERE genome_idx = $1", genome_orphan
            )
            is None
        )
        assert (
            await postgres_pool.fetchval(
                "SELECT 1 FROM qiita.genome WHERE genome_idx = $1", genome_shared
            )
            == 1
        )
        # Survivor feature + its feature_genome mapping intact.
        assert (
            await postgres_pool.fetchval(
                "SELECT 1 FROM qiita.feature_genome WHERE feature_idx = $1", survivor_feat
            )
            == 1
        )
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.feature_genome WHERE feature_idx = $1", survivor_feat
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_membership WHERE feature_idx = $1", survivor_feat
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.feature WHERE feature_idx = $1", survivor_feat
        )
        await postgres_pool.execute("DELETE FROM qiita.genome WHERE genome_idx = $1", genome_shared)


async def test_delete_reference_data_plane_failure_returns_502(client, postgres_pool, monkeypatch):
    """A data-plane FlightError surfaces as 502 and leaves Postgres untouched
    (the teardown runs only after the DuckLake delete succeeds)."""
    import pyarrow.flight as flight

    ref_idx = await _create_ref(client, f"del-502-{uuid.uuid4()}")

    async def _boom(*, reference_idx, hmac_secret, data_plane_url):
        raise flight.FlightUnavailableError("data plane down")

    monkeypatch.setattr("qiita_control_plane.routes.reference.delete_reference_data", _boom)

    resp = await client.delete(URL_REFERENCE_BY_IDX.format(reference_idx=ref_idx))
    assert resp.status_code == 502
    # Reference row survives — safe to retry once the data plane is back.
    assert (
        await postgres_pool.fetchval(
            "SELECT 1 FROM qiita.reference WHERE reference_idx = $1", ref_idx
        )
        == 1
    )
