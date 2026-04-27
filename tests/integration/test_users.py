"""Integration tests for /api/v1/users — Phase B (mock auth).

Exercises POST /users (admin creates user), GET /users/me (read self),
PATCH /users/me (update profile, with email/status writes ignored).

The fixture mock_authenticated_principal seeds a system_admin principal +
user that the mock get_current_principal_idx resolves to. Real auth lands
in Phase E; this whole test file's mock-auth assumptions get rewritten in
Phase H.b.
"""

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture
async def client(postgres_pool, mock_authenticated_principal):
    """AsyncClient wired to the control plane app.

    ASGITransport does not trigger FastAPI lifespan, so the pool from
    conftest is injected directly into app.state. The mock_authenticated_principal
    fixture is required to pre-seed the principal the deps mock looks up.
    """
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool

    created_principals: list[int] = []

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        ac._created_principals = created_principals
        yield ac

    # Cleanup: delete user rows (FK to principal), then principals we created.
    if created_principals:
        await postgres_pool.execute(
            "DELETE FROM qiita.user WHERE principal_idx = ANY($1::bigint[])",
            created_principals,
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.principal WHERE idx = ANY($1::bigint[])",
            created_principals,
        )


async def _create_user(client, *, display_name, email, **extra):
    body = {"display_name": display_name, "email": email, **extra}
    resp = await client.post("/api/v1/users", json=body)
    if resp.status_code == 201:
        client._created_principals.append(resp.json()["principal_idx"])
    return resp


# ---------------------------------------------------------------------------
# POST /users
# ---------------------------------------------------------------------------


async def test_post_users_creates_principal_and_user(client, postgres_pool):
    resp = await _create_user(
        client,
        display_name="Alice Adams",
        email="alice.post@example.com",
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["principal_idx"] > 1
    assert body["display_name"] == "Alice Adams"
    assert body["email"] == "alice.post@example.com"
    assert body["profile_complete"] is False  # only email, no profile fields

    # Verify both principal and user rows exist in DB.
    pidx = body["principal_idx"]
    p_row = await postgres_pool.fetchrow(
        "SELECT display_name, system_role, created_by_idx"
        " FROM qiita.principal WHERE idx = $1",
        pidx,
    )
    assert p_row["display_name"] == "Alice Adams"
    assert p_row["system_role"] == "user"
    # created_by_idx should be the mock-admin's idx (the actor).
    actor = await postgres_pool.fetchval(
        "SELECT idx FROM qiita.principal WHERE display_name = 'mock-admin'"
    )
    assert p_row["created_by_idx"] == actor


async def test_post_users_with_full_profile_marks_profile_complete(client):
    resp = await _create_user(
        client,
        display_name="Bob Builder",
        email="bob.full@example.com",
        affiliation="UCSD",
        address="9500 Gilman Dr",
        phone="555-1234",
        orcid="0000-0002-1825-0097",
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["profile_complete"] is True
    assert body["orcid"] == "0000-0002-1825-0097"


async def test_post_users_duplicate_email_409(client):
    r1 = await _create_user(
        client, display_name="First", email="dup@example.com"
    )
    assert r1.status_code == 201
    r2 = await _create_user(
        client, display_name="Second", email="dup@example.com"
    )
    assert r2.status_code == 409


async def test_post_users_duplicate_email_case_insensitive(client):
    """CITEXT means 'Alice@Example.com' and 'alice@example.com' collide."""
    r1 = await _create_user(
        client, display_name="One", email="Casey@Example.COM"
    )
    assert r1.status_code == 201
    r2 = await _create_user(
        client, display_name="Two", email="casey@example.com"
    )
    assert r2.status_code == 409


async def test_post_users_rejects_invalid_orcid(client):
    resp = await _create_user(
        client,
        display_name="C",
        email="c@example.com",
        orcid="bad-orcid",
    )
    assert resp.status_code == 422


async def test_post_users_rejects_invalid_email(client):
    resp = await _create_user(
        client,
        display_name="D",
        email="not-an-email",
    )
    assert resp.status_code == 422


async def test_post_users_rejects_empty_display_name(client):
    resp = await _create_user(
        client,
        display_name="",
        email="e@example.com",
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /users/me
# ---------------------------------------------------------------------------


async def test_get_me_returns_mock_admin_profile(
    client, mock_authenticated_principal
):
    resp = await client.get("/api/v1/users/me")
    assert resp.status_code == 200
    body = resp.json()
    assert body["principal_idx"] == mock_authenticated_principal
    assert body["display_name"] == "mock-admin"
    assert body["email"] == "mock-admin@example.com"


# ---------------------------------------------------------------------------
# PATCH /users/me
# ---------------------------------------------------------------------------


async def test_patch_me_updates_profile_fields(client):
    resp = await client.patch(
        "/api/v1/users/me",
        json={
            "affiliation": "UCSD",
            "address": "9500 Gilman Dr",
            "phone": "555-9999",
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["affiliation"] == "UCSD"
    assert body["address"] == "9500 Gilman Dr"
    assert body["phone"] == "555-9999"
    assert body["profile_complete"] is True


async def test_patch_me_does_not_change_email(client):
    """Even if a client sends email, it's silently dropped (not in UserUpdate)."""
    before = await client.get("/api/v1/users/me")
    original_email = before.json()["email"]

    resp = await client.patch(
        "/api/v1/users/me",
        json={"email": "evil-changer@example.com", "affiliation": "X"},
    )
    assert resp.status_code == 200
    assert resp.json()["email"] == original_email
    assert resp.json()["affiliation"] == "X"


async def test_patch_me_empty_body_is_no_op(client):
    """Empty PATCH body returns the current profile unchanged."""
    resp = await client.patch("/api/v1/users/me", json={})
    assert resp.status_code == 200
    body = resp.json()
    assert body["display_name"] == "mock-admin"


async def test_patch_me_rejects_invalid_orcid(client):
    resp = await client.patch(
        "/api/v1/users/me",
        json={"orcid": "obviously-wrong"},
    )
    assert resp.status_code == 422
