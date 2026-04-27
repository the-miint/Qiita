"""Integration tests for /api/v1/users (Phase H.b — real auth).

Phase B used the mock get_current_principal_idx; Phase H.b flipped POST
/users to system_admin + admin:users and GET/PATCH /users/me to require_human
+ self:profile. Tests now use the session admin PAT for admin-flow calls
and the regular-user PAT for self-management calls.
"""

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture
async def admin_app(postgres_pool, human_admin_session):
    """Client wired with the session admin PAT default-set on Authorization.
    Used for POST /users (admin-creates-a-user) and GET /users/me-as-admin."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool

    created_principals: list[int] = []

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {human_admin_session['token']}"},
    ) as ac:
        ac._created_principals = created_principals
        yield ac

    if created_principals:
        await postgres_pool.execute(
            "DELETE FROM qiita.user WHERE principal_idx = ANY($1::bigint[])",
            created_principals,
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.principal WHERE idx = ANY($1::bigint[])",
            created_principals,
        )


@pytest.fixture
async def regular_user_app(postgres_pool, regular_user_session):
    """Client wired with a non-admin user PAT for self-management endpoints."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {regular_user_session['token']}"},
    ) as ac:
        yield ac


async def _create_user(client, *, display_name, email, **extra):
    body = {"display_name": display_name, "email": email, **extra}
    resp = await client.post("/api/v1/users", json=body)
    if resp.status_code == 201:
        client._created_principals.append(resp.json()["principal_idx"])
    return resp


# ---------------------------------------------------------------------------
# POST /users (admin-only after Phase H.b)
# ---------------------------------------------------------------------------


async def test_post_users_creates_principal_and_user(
    admin_app, postgres_pool, human_admin_session
):
    resp = await _create_user(
        admin_app,
        display_name="Alice Adams",
        email="alice.post@example.com",
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["principal_idx"] > 1
    assert body["display_name"] == "Alice Adams"
    assert body["email"] == "alice.post@example.com"
    assert body["profile_complete"] is False  # only email, no profile fields

    p_row = await postgres_pool.fetchrow(
        "SELECT display_name, system_role, created_by_idx"
        " FROM qiita.principal WHERE idx = $1",
        body["principal_idx"],
    )
    assert p_row["display_name"] == "Alice Adams"
    assert p_row["system_role"] == "user"
    # created_by_idx is the admin's principal_idx (the actor).
    assert p_row["created_by_idx"] == human_admin_session["principal_idx"]


async def test_post_users_with_full_profile_marks_profile_complete(admin_app):
    resp = await _create_user(
        admin_app,
        display_name="Bob Builder",
        email="bob.full@example.com",
        affiliation="UCSD",
        address="9500 Gilman Dr",
        phone="555-1234",
        orcid="0000-0002-1825-0097",
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["profile_complete"] is True
    assert body["orcid"] == "0000-0002-1825-0097"


async def test_post_users_duplicate_email_409(admin_app):
    r1 = await _create_user(admin_app, display_name="First", email="dup@example.com")
    assert r1.status_code == 201
    r2 = await _create_user(admin_app, display_name="Second", email="dup@example.com")
    assert r2.status_code == 409


async def test_post_users_duplicate_email_case_insensitive(admin_app):
    r1 = await _create_user(admin_app, display_name="One", email="Casey@Example.COM")
    assert r1.status_code == 201
    r2 = await _create_user(admin_app, display_name="Two", email="casey@example.com")
    assert r2.status_code == 409


async def test_post_users_rejects_invalid_orcid(admin_app):
    resp = await _create_user(
        admin_app,
        display_name="C",
        email="c@example.com",
        orcid="bad-orcid",
    )
    assert resp.status_code == 422


async def test_post_users_rejects_invalid_email(admin_app):
    resp = await _create_user(
        admin_app,
        display_name="D",
        email="not-an-email",
    )
    assert resp.status_code == 422


async def test_post_users_rejects_empty_display_name(admin_app):
    resp = await _create_user(
        admin_app,
        display_name="",
        email="e@example.com",
    )
    assert resp.status_code == 422


async def test_post_users_non_admin_403(regular_user_app):
    """A regular user (system_role='user') cannot create other users."""
    resp = await regular_user_app.post(
        "/api/v1/users",
        json={"display_name": "Should Fail", "email": "x@x.com"},
    )
    assert resp.status_code == 403


async def test_post_users_anonymous_401(postgres_pool):
    """No Authorization header → 401."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        resp = await ac.post(
            "/api/v1/users",
            json={"display_name": "X", "email": "anon@x.com"},
        )
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# GET /users/me
# ---------------------------------------------------------------------------


async def test_get_me_returns_authenticated_user_profile(
    admin_app, human_admin_session
):
    resp = await admin_app.get("/api/v1/users/me")
    assert resp.status_code == 200
    body = resp.json()
    assert body["principal_idx"] == human_admin_session["principal_idx"]
    assert body["email"] == human_admin_session["email"]
    assert body["display_name"] == human_admin_session["display_name"]


async def test_get_me_anonymous_401(postgres_pool):
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        resp = await ac.get("/api/v1/users/me")
    assert resp.status_code == 401


async def test_get_me_service_account_403(
    postgres_pool, compute_worker_service_account
):
    """A service account is not a human; require_human gives 403."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {compute_worker_service_account['token']}"},
    ) as ac:
        resp = await ac.get("/api/v1/users/me")
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# PATCH /users/me
# ---------------------------------------------------------------------------


async def test_patch_me_updates_profile_fields(regular_user_app):
    resp = await regular_user_app.patch(
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


async def test_patch_me_does_not_change_email(regular_user_app):
    """Even if a client sends email, it's silently dropped (not in UserUpdate)."""
    before = await regular_user_app.get("/api/v1/users/me")
    original_email = before.json()["email"]

    resp = await regular_user_app.patch(
        "/api/v1/users/me",
        json={"email": "evil-changer@example.com", "affiliation": "X"},
    )
    assert resp.status_code == 200
    assert resp.json()["email"] == original_email
    assert resp.json()["affiliation"] == "X"


async def test_patch_me_empty_body_is_no_op(regular_user_app, regular_user_session):
    resp = await regular_user_app.patch("/api/v1/users/me", json={})
    assert resp.status_code == 200
    assert resp.json()["display_name"] == regular_user_session["display_name"]


async def test_patch_me_rejects_invalid_orcid(regular_user_app):
    resp = await regular_user_app.patch(
        "/api/v1/users/me",
        json={"orcid": "obviously-wrong"},
    )
    assert resp.status_code == 422


async def test_patch_me_anonymous_401(postgres_pool):
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        resp = await ac.patch("/api/v1/users/me", json={"affiliation": "X"})
    assert resp.status_code == 401
