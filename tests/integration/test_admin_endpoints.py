"""Integration tests for /api/v1/admin/*."""

import json

import pytest
from httpx import ASGITransport, AsyncClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
async def admin_client(postgres_pool, jwks_harness):
    """App with the resolver wired (no OIDC verifier needed; admin tests use
    PAT-resolved principals)."""
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.oidc_verifier = None
    app.state.settings = Settings(
        database_url="unused",
        hmac_secret_key=b"\x00" * 32,
        data_plane_url="unused",
    )
    created: list[int] = []
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        ac._created_principals = created
        yield ac

    if created:
        async with postgres_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "ALTER TABLE qiita.auth_events DISABLE TRIGGER auth_events_no_delete"
                )
                try:
                    for table in (
                        "api_tokens",
                        "user_identities",
                        "user",
                        "service_account",
                    ):
                        await conn.execute(
                            f"DELETE FROM qiita.{table}"
                            " WHERE principal_idx = ANY($1::bigint[])",
                            created,
                        )
                    await conn.execute(
                        "DELETE FROM qiita.auth_events"
                        " WHERE principal_idx = ANY($1::bigint[])"
                        "    OR actor_principal_idx = ANY($1::bigint[])",
                        created,
                    )
                    await conn.execute(
                        "DELETE FROM qiita.principal WHERE idx = ANY($1::bigint[])",
                        created,
                    )
                finally:
                    await conn.execute(
                        "ALTER TABLE qiita.auth_events ENABLE TRIGGER auth_events_no_delete"
                    )


async def _seed_human(
    postgres_pool,
    *,
    email: str,
    role: str = "user",
) -> int:
    pidx = await postgres_pool.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, 1) RETURNING idx",
        email,
        role,
    )
    await postgres_pool.execute(
        "INSERT INTO qiita.user (principal_idx, email, affiliation, address, phone)"
        " VALUES ($1, $2, 'X', 'Y', 'Z')",
        pidx,
        email,
    )
    return pidx


async def _admin_token(postgres_pool, admin_client) -> str:
    """Seed a fresh system_admin and return a PAT for them with full admin scopes."""
    from qiita_control_plane.auth.tokens import mint_api_token

    admin_idx = await _seed_human(
        postgres_pool,
        email=f"admin-{id(admin_client)}@example.com",
        role="system_admin",
    )
    admin_client._created_principals.append(admin_idx)
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=admin_idx,
        label="admin-test",
        scopes=[
            "self:profile",
            "self:token",
            "reference:read",
            "reference:write",
            "admin:user",
            "admin:service_account",
            "admin:audit_read",
        ],
    )
    return plaintext, admin_idx


def _track(client, pidx):
    client._created_principals.append(pidx)


def _detail(row) -> dict:
    raw = row["detail"]
    return json.loads(raw) if isinstance(raw, str) else (raw or {})


# ---------------------------------------------------------------------------
# POST /admin/service-accounts
# ---------------------------------------------------------------------------


async def test_post_service_accounts_admin_only(admin_client, postgres_pool):
    """A non-admin token gets 403."""
    from qiita_control_plane.auth.tokens import mint_api_token

    pidx = await _seed_human(postgres_pool, email="non-admin@example.com")
    _track(admin_client, pidx)
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=pidx,
        label="non-admin",
        scopes=["self:profile"],
    )
    resp = await admin_client.post(
        "/api/v1/admin/service-accounts",
        headers={"Authorization": f"Bearer {plaintext}"},
        json={"name": "blocked-svc", "scopes": ["feature:mint"]},
    )
    assert resp.status_code == 403


async def test_post_service_accounts_returns_token_once(admin_client, postgres_pool):
    admin_token, _ = await _admin_token(postgres_pool, admin_client)
    resp = await admin_client.post(
        "/api/v1/admin/service-accounts",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={
            "name": "ok-svc",
            "scopes": ["feature:mint", "reference:read"],
        },
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["token"].startswith("qk_")
    assert body["principal_idx"] > 0
    _track(admin_client, body["principal_idx"])


async def test_post_service_accounts_requires_explicit_scopes(
    admin_client, postgres_pool
):
    admin_token, _ = await _admin_token(postgres_pool, admin_client)
    resp = await admin_client.post(
        "/api/v1/admin/service-accounts",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"name": "no-scopes", "scopes": []},
    )
    assert resp.status_code == 422


async def test_post_service_accounts_rejects_scope_outside_service_ceiling(
    admin_client, postgres_pool
):
    admin_token, _ = await _admin_token(postgres_pool, admin_client)
    resp = await admin_client.post(
        "/api/v1/admin/service-accounts",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"name": "evil-svc", "scopes": ["feature:mint", "admin:user"]},
    )
    assert resp.status_code == 422
    # Flat 422 body (matches /auth/pat shape) — top-level rejected_scopes.
    assert "admin:user" in resp.json()["rejected_scopes"]


async def test_post_service_accounts_duplicate_name_409(admin_client, postgres_pool):
    """A second create with the same name returns 409 with a name-specific
    detail. The detail body is asserted (not just the status) because the
    route's UniqueViolation handler dispatches on the constraint name; if
    that dispatch ever falls through (e.g., the constraint gets renamed in
    a migration), the response would become a 500 and this test catches it."""
    admin_token, _ = await _admin_token(postgres_pool, admin_client)
    payload = {"name": "dup-svc", "scopes": ["feature:mint"]}
    r1 = await admin_client.post(
        "/api/v1/admin/service-accounts",
        headers={"Authorization": f"Bearer {admin_token}"},
        json=payload,
    )
    assert r1.status_code == 201
    _track(admin_client, r1.json()["principal_idx"])
    r2 = await admin_client.post(
        "/api/v1/admin/service-accounts",
        headers={"Authorization": f"Bearer {admin_token}"},
        json=payload,
    )
    assert r2.status_code == 409
    assert r2.json()["detail"] == "service account named 'dup-svc' already exists"


# ---------------------------------------------------------------------------
# PATCH /admin/principals/{idx}/disabled
# ---------------------------------------------------------------------------


async def test_patch_principal_disabled_admin_only(admin_client, postgres_pool):
    from qiita_control_plane.auth.tokens import mint_api_token

    pidx = await _seed_human(postgres_pool, email="not-admin@example.com")
    _track(admin_client, pidx)
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=pidx,
        label="x",
        scopes=["self:profile"],
    )
    target = await _seed_human(postgres_pool, email="target@example.com")
    _track(admin_client, target)
    resp = await admin_client.patch(
        f"/api/v1/admin/principals/{target}/disabled",
        headers={"Authorization": f"Bearer {plaintext}"},
        json={"disabled": True, "reason": "test"},
    )
    assert resp.status_code == 403


async def test_patch_principal_disabled_then_enabled_round_trip(
    admin_client, postgres_pool
):
    admin_token, _ = await _admin_token(postgres_pool, admin_client)
    target = await _seed_human(postgres_pool, email="rt-target@example.com")
    _track(admin_client, target)

    r1 = await admin_client.patch(
        f"/api/v1/admin/principals/{target}/disabled",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"disabled": True, "reason": "investigation"},
    )
    assert r1.status_code == 204
    state = await postgres_pool.fetchrow(
        "SELECT disabled, disabled_at, disable_reason FROM qiita.principal"
        " WHERE idx = $1",
        target,
    )
    assert state["disabled"] is True
    assert state["disable_reason"] == "investigation"

    r2 = await admin_client.patch(
        f"/api/v1/admin/principals/{target}/disabled",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"disabled": False},
    )
    assert r2.status_code == 204
    state = await postgres_pool.fetchrow(
        "SELECT disabled, disabled_at, disable_reason FROM qiita.principal"
        " WHERE idx = $1",
        target,
    )
    assert state["disabled"] is False
    assert state["disabled_at"] is None
    assert state["disable_reason"] is None


async def test_patch_principal_disabled_requires_reason(admin_client, postgres_pool):
    admin_token, _ = await _admin_token(postgres_pool, admin_client)
    target = await _seed_human(postgres_pool, email="no-reason@example.com")
    _track(admin_client, target)
    resp = await admin_client.patch(
        f"/api/v1/admin/principals/{target}/disabled",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"disabled": True},
    )
    assert resp.status_code == 422


async def test_cannot_disable_system_principal(admin_client, postgres_pool):
    admin_token, _ = await _admin_token(postgres_pool, admin_client)
    resp = await admin_client.patch(
        "/api/v1/admin/principals/1/disabled",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"disabled": True, "reason": "evil"},
    )
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# PATCH /admin/principals/{idx}/retired
# ---------------------------------------------------------------------------


async def test_patch_principal_retired_revokes_all_tokens(admin_client, postgres_pool):
    from qiita_control_plane.auth.tokens import mint_api_token

    admin_token, _ = await _admin_token(postgres_pool, admin_client)
    target = await _seed_human(postgres_pool, email="will-retire@example.com")
    _track(admin_client, target)
    for i in range(2):
        await mint_api_token(
            postgres_pool,
            principal_idx=target,
            label=f"t{i}",
            scopes=["self:profile"],
        )
    resp = await admin_client.patch(
        f"/api/v1/admin/principals/{target}/retired",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"reason": "left lab"},
    )
    assert resp.status_code == 204
    n_active = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.api_tokens"
        " WHERE principal_idx = $1 AND revoked_at IS NULL",
        target,
    )
    assert n_active == 0


async def test_patch_principal_retired_is_terminal(admin_client, postgres_pool):
    admin_token, _ = await _admin_token(postgres_pool, admin_client)
    target = await _seed_human(postgres_pool, email="term@example.com")
    _track(admin_client, target)
    r1 = await admin_client.patch(
        f"/api/v1/admin/principals/{target}/retired",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"reason": "first"},
    )
    assert r1.status_code == 204
    # Trying to disable a retired principal: DB trigger enforces no
    # transition out of retired (CHECK forbids both-true; the route's
    # WHERE clause filters out retired).
    r2 = await admin_client.patch(
        f"/api/v1/admin/principals/{target}/disabled",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"disabled": True, "reason": "should-fail"},
    )
    assert r2.status_code == 409


async def test_admin_cannot_retire_themselves(admin_client, postgres_pool):
    admin_token, admin_idx = await _admin_token(postgres_pool, admin_client)
    resp = await admin_client.patch(
        f"/api/v1/admin/principals/{admin_idx}/retired",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"reason": "self"},
    )
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# PATCH /admin/principals/{idx}/system-role
# ---------------------------------------------------------------------------


async def test_patch_principal_system_role_writes_audit_event(
    admin_client, postgres_pool
):
    admin_token, admin_idx = await _admin_token(postgres_pool, admin_client)
    target = await _seed_human(postgres_pool, email="role-target@example.com")
    _track(admin_client, target)
    resp = await admin_client.patch(
        f"/api/v1/admin/principals/{target}/system-role",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"system_role": "wet_lab_admin", "reason": "promo"},
    )
    assert resp.status_code == 204

    new_role = await postgres_pool.fetchval(
        "SELECT system_role FROM qiita.principal WHERE idx = $1", target
    )
    assert new_role == "wet_lab_admin"

    rows = await postgres_pool.fetch(
        "SELECT detail, actor_principal_idx FROM qiita.auth_events"
        " WHERE event_type = 'system_role_change' AND principal_idx = $1",
        target,
    )
    assert rows
    detail = _detail(rows[-1])
    assert detail["from"] == "user"
    assert detail["to"] == "wet_lab_admin"
    assert rows[-1]["actor_principal_idx"] == admin_idx


async def test_patch_principal_system_role_admin_only(admin_client, postgres_pool):
    from qiita_control_plane.auth.tokens import mint_api_token

    pidx = await _seed_human(postgres_pool, email="not-admin-role@example.com")
    _track(admin_client, pidx)
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=pidx,
        label="x",
        scopes=["self:profile"],
    )
    target = await _seed_human(postgres_pool, email="rt2@example.com")
    _track(admin_client, target)
    resp = await admin_client.patch(
        f"/api/v1/admin/principals/{target}/system-role",
        headers={"Authorization": f"Bearer {plaintext}"},
        json={"system_role": "system_admin"},
    )
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# GET /admin/audit
# ---------------------------------------------------------------------------


async def test_get_audit_log_admin_only(admin_client, postgres_pool):
    from qiita_control_plane.auth.tokens import mint_api_token

    pidx = await _seed_human(postgres_pool, email="audit-blocked@example.com")
    _track(admin_client, pidx)
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=pidx,
        label="x",
        scopes=["self:profile"],
    )
    resp = await admin_client.get(
        "/api/v1/admin/audit",
        headers={"Authorization": f"Bearer {plaintext}"},
    )
    assert resp.status_code == 403


async def test_get_audit_log_filters(admin_client, postgres_pool):
    admin_token, admin_idx = await _admin_token(postgres_pool, admin_client)
    target = await _seed_human(postgres_pool, email="audit-target@example.com")
    _track(admin_client, target)
    # Generate some events: role change, disable, enable.
    await admin_client.patch(
        f"/api/v1/admin/principals/{target}/system-role",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"system_role": "wet_lab_admin"},
    )
    await admin_client.patch(
        f"/api/v1/admin/principals/{target}/disabled",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"disabled": True, "reason": "x"},
    )
    # Filtered by event_type.
    resp = await admin_client.get(
        "/api/v1/admin/audit",
        headers={"Authorization": f"Bearer {admin_token}"},
        params={"event_type": "system_role_change", "principal_idx": target},
    )
    assert resp.status_code == 200, resp.text
    rows = resp.json()
    assert all(r["event_type"] == "system_role_change" for r in rows)
    assert all(r["principal_idx"] == target for r in rows)


# ---------------------------------------------------------------------------
# POST /admin/principals/{idx}/revoke-all-tokens
# ---------------------------------------------------------------------------


async def test_revoke_all_tokens_admin_only(admin_client, postgres_pool):
    from qiita_control_plane.auth.tokens import mint_api_token

    pidx = await _seed_human(postgres_pool, email="revoke-blocked@example.com")
    _track(admin_client, pidx)
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=pidx,
        label="x",
        scopes=["self:profile"],
    )
    target = await _seed_human(postgres_pool, email="rt-target@example.com")
    _track(admin_client, target)
    resp = await admin_client.post(
        f"/api/v1/admin/principals/{target}/revoke-all-tokens",
        headers={"Authorization": f"Bearer {plaintext}"},
    )
    assert resp.status_code == 403


async def test_revoke_all_tokens_requires_admin_service_accounts_for_service_target(
    admin_client, postgres_pool
):
    """An admin token with admin:users but NOT admin:service_accounts
    cannot bulk-revoke tokens for a service-account-kind target."""
    from qiita_control_plane.auth.tokens import mint_api_token

    # Fresh admin with admin:users but explicitly NOT admin:service_accounts.
    admin_idx = await _seed_human(
        postgres_pool,
        email="narrow-admin@example.com",
        role="system_admin",
    )
    _track(admin_client, admin_idx)
    narrow_token, _ = await mint_api_token(
        postgres_pool,
        principal_idx=admin_idx,
        label="narrow",
        scopes=[
            "self:profile",
            "self:token",
            "reference:read",
            "reference:write",
            "admin:user",
            # admin:service_accounts intentionally absent
            "admin:audit_read",
        ],
    )

    # Service-account target.
    svc_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.principal"
        "  (display_name, system_role, created_by_idx)"
        " VALUES ('narrow-svc-target', 'user', 1) RETURNING idx"
    )
    _track(admin_client, svc_idx)
    await postgres_pool.execute(
        "INSERT INTO qiita.service_account (principal_idx, name)"
        " VALUES ($1, 'narrow-svc-target')",
        svc_idx,
    )

    resp = await admin_client.post(
        f"/api/v1/admin/principals/{svc_idx}/revoke-all-tokens",
        headers={"Authorization": f"Bearer {narrow_token}"},
    )
    assert resp.status_code == 403
    assert "admin:service_account" in resp.json()["detail"]


async def test_revoke_all_tokens_requires_admin_users_for_user_target(
    admin_client, postgres_pool
):
    """Symmetric: admin:service_accounts alone is not enough for a user target."""
    from qiita_control_plane.auth.tokens import mint_api_token

    admin_idx = await _seed_human(
        postgres_pool,
        email="svc-only-admin@example.com",
        role="system_admin",
    )
    _track(admin_client, admin_idx)
    narrow_token, _ = await mint_api_token(
        postgres_pool,
        principal_idx=admin_idx,
        label="svc-only",
        scopes=[
            "self:profile",
            "self:token",
            "reference:read",
            # admin:users intentionally absent
            "admin:service_account",
        ],
    )

    user_target = await _seed_human(
        postgres_pool,
        email="narrow-user-target@example.com",
    )
    _track(admin_client, user_target)

    resp = await admin_client.post(
        f"/api/v1/admin/principals/{user_target}/revoke-all-tokens",
        headers={"Authorization": f"Bearer {narrow_token}"},
    )
    assert resp.status_code == 403
    assert "admin:user" in resp.json()["detail"]


async def test_revoke_all_tokens_revokes_all_active_and_skips_revoked(
    admin_client, postgres_pool
):
    from qiita_control_plane.auth.tokens import mint_api_token

    admin_token, admin_idx = await _admin_token(postgres_pool, admin_client)
    target = await _seed_human(postgres_pool, email="bulk-revoke@example.com")
    _track(admin_client, target)
    # Two active tokens.
    _, t1 = await mint_api_token(
        postgres_pool, principal_idx=target, label="a", scopes=[]
    )
    _, t2 = await mint_api_token(
        postgres_pool, principal_idx=target, label="b", scopes=[]
    )
    # One pre-revoked token.
    _, t3 = await mint_api_token(
        postgres_pool, principal_idx=target, label="pre", scopes=[]
    )
    await postgres_pool.execute(
        "UPDATE qiita.api_tokens SET revoked_at = now() WHERE token_idx = $1",
        t3,
    )

    resp = await admin_client.post(
        f"/api/v1/admin/principals/{target}/revoke-all-tokens",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert set(body["revoked_token_idxs"]) == {t1, t2}
    assert body["already_revoked_count"] == 1

    # All three are now revoked.
    n_active = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.api_tokens"
        " WHERE principal_idx = $1 AND revoked_at IS NULL",
        target,
    )
    assert n_active == 0

    # One token_revoke event per newly-revoked token (not for already-revoked).
    rows = await postgres_pool.fetch(
        "SELECT detail FROM qiita.auth_events"
        " WHERE event_type = 'token_revoke' AND principal_idx = $1"
        "   AND actor_principal_idx = $2",
        target,
        admin_idx,
    )
    revoked_in_audit = [_detail(r)["token_idx"] for r in rows]
    assert set(revoked_in_audit) == {t1, t2}
