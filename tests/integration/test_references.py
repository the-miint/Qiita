"""Integration tests for reference routes — exercises POST/GET against real Postgres."""

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture
async def client(postgres_pool, human_admin_session):
    """AsyncClient wired to the control plane app with the integration test pool
    and a session-scoped admin PAT preset on the Authorization header.

    ASGITransport does not trigger FastAPI lifespan, so the pool from conftest
    is injected directly into app.state.
    """
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool

    created_refs: list[int] = []

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {human_admin_session['token']}"},
    ) as ac:
        ac._created_refs = created_refs
        yield ac

    # Cleanup only rows we created, in FK dependency order
    if created_refs:
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_membership WHERE reference_idx = ANY($1::bigint[])",
            created_refs,
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.references WHERE reference_idx = ANY($1::bigint[])",
            created_refs,
        )


async def _create_ref(client, name, version="1.0", kind="sequence_reference"):
    """Helper: create a reference and track its idx for cleanup."""
    resp = await client.post(
        "/api/v1/references",
        json={"name": name, "version": version, "kind": kind},
    )
    if resp.status_code == 201:
        client._created_refs.append(resp.json()["reference_idx"])
    return resp


async def test_create_reference_returns_201(client, human_admin_session):
    """POST /api/v1/references with valid payload returns 201."""
    resp = await _create_ref(client, "test-ref-create")
    assert resp.status_code == 201
    body = resp.json()
    assert "reference_idx" in body
    assert body["reference_idx"] > 0
    assert body["status"] == "pending"
    assert body["name"] == "test-ref-create"
    # Phase H.b dual-write invariant: created_by_idx is the admin's idx,
    # created_by is the deterministic uuid5 derived from it.
    from uuid import NAMESPACE_OID, uuid5

    assert body["created_by_idx"] == human_admin_session["principal_idx"]
    assert body["created_by"] == str(
        uuid5(NAMESPACE_OID, str(human_admin_session["principal_idx"]))
    )


async def test_create_reference_rejects_invalid_kind(client):
    """POST /api/v1/references with invalid kind returns 422."""
    resp = await client.post(
        "/api/v1/references",
        json={"name": "bad", "version": "1.0", "kind": "bogus"},
    )
    assert resp.status_code == 422


async def test_create_reference_rejects_empty_name(client):
    """POST /api/v1/references with empty name returns 422."""
    resp = await client.post(
        "/api/v1/references",
        json={"name": "", "version": "1.0", "kind": "sequence_reference"},
    )
    assert resp.status_code == 422


async def test_get_reference_by_idx(client):
    """GET /api/v1/references/{idx} returns the created reference."""
    create_resp = await _create_ref(client, "test-ref-get")
    idx = create_resp.json()["reference_idx"]

    get_resp = await client.get(f"/api/v1/references/{idx}")
    assert get_resp.status_code == 200
    body = get_resp.json()
    assert body["reference_idx"] == idx
    assert body["name"] == "test-ref-get"


async def test_get_reference_not_found(client, postgres_pool):
    """GET for a reference_idx beyond any existing row returns 404."""
    max_idx = await postgres_pool.fetchval(
        "SELECT COALESCE(MAX(reference_idx), 0) FROM qiita.references"
    )
    resp = await client.get(f"/api/v1/references/{max_idx + 1}")
    assert resp.status_code == 404


async def test_get_reference_rejects_zero(client):
    """GET /api/v1/references/0 returns 422 (gt=0 constraint)."""
    resp = await client.get("/api/v1/references/0")
    assert resp.status_code == 422


async def test_get_reference_rejects_negative(client):
    """GET /api/v1/references/-1 returns 422 (gt=0 constraint)."""
    resp = await client.get("/api/v1/references/-1")
    assert resp.status_code == 422


async def test_create_duplicate_reference_returns_409(client):
    """POST with duplicate (name, version) returns 409."""
    resp1 = await _create_ref(client, "test-ref-dup")
    assert resp1.status_code == 201

    resp2 = await _create_ref(client, "test-ref-dup")
    assert resp2.status_code == 409
