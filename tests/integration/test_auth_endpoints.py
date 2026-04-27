"""Integration tests for /api/v1/auth/* (Phase F).

The control-plane app is mounted with a real OIDC verifier (backed by the
JwksHarness fixture from conftest) and the test postgres pool. Each test
seeds whatever principals/users it needs and drives the routes via httpx.
"""

import json
import time
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient


# ---------------------------------------------------------------------------
# Helpers / fixture
# ---------------------------------------------------------------------------


def _claims(jwks_harness, **overrides) -> dict:
    """Default claim set; override per test."""
    now = int(time.time())
    base = {
        "iss": jwks_harness.issuer,
        "aud": "test-audience",
        "sub": f"sub-{int(time.time() * 1000)}",
        "email": f"u-{int(time.time() * 1000)}@example.com",
        "email_verified": True,
        "iat": now,
        "exp": now + 3600,
        "auth_time": now,
    }
    base.update(overrides)
    return base


def _verifier(jwks_harness):
    from qiita_control_plane.auth.oidc import JwtVerifier

    return JwtVerifier(
        jwks_url=jwks_harness.jwks_url,
        issuer=jwks_harness.issuer,
        audience="test-audience",
    )


@pytest.fixture
async def auth_client(postgres_pool, jwks_harness):
    """Mount the production app with the test pool + a real verifier
    pointed at the local JwksHarness. Tracks created principals/tokens
    for cleanup."""
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.oidc_verifier = _verifier(jwks_harness)
    # Provide a Settings instance that the /auth/pat route can read for
    # auth_age and default ttl. We only set the fields the route actually
    # uses; database_url etc. don't matter here.
    app.state.settings = Settings(
        database_url="unused",
        hmac_secret_key=b"\x00" * 32,
        data_plane_url="unused",
        authrocket_issuer=jwks_harness.issuer,
        authrocket_audience="test-audience",
        authrocket_jwks_url=jwks_harness.jwks_url,
        authrocket_jwt_leeway_seconds=30,
        authrocket_pat_max_auth_age_seconds=300,
        token_default_ttl_days=90,
    )

    created: list[int] = []

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        ac._created_principals = created
        yield ac

    # Cleanup with auth_events trigger temporarily disabled.
    if created:
        async with postgres_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "ALTER TABLE qiita.auth_events DISABLE TRIGGER auth_events_no_delete"
                )
                try:
                    for table in ("api_tokens", "user_identities", "user", "service_account"):
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


async def _seed_user(
    postgres_pool,
    *,
    email: str,
    role: str = "user",
    profile_complete: bool = True,
    issuer: str | None = None,
    subject: str | None = None,
) -> int:
    """Seed principal + user (+ optional user_identities). Returns principal_idx."""
    pidx = await postgres_pool.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, 1) RETURNING idx",
        email, role,
    )
    if profile_complete:
        await postgres_pool.execute(
            "INSERT INTO qiita.user (principal_idx, email, affiliation, address, phone)"
            " VALUES ($1, $2, 'UCSD', '9500 Gilman', '555-0001')",
            pidx, email,
        )
    else:
        await postgres_pool.execute(
            "INSERT INTO qiita.user (principal_idx, email)"
            " VALUES ($1, $2)",
            pidx, email,
        )
    if issuer and subject:
        await postgres_pool.execute(
            "INSERT INTO qiita.user_identities (principal_idx, issuer, subject)"
            " VALUES ($1, $2, $3)",
            pidx, issuer, subject,
        )
    return pidx


def _track(client, pidx):
    client._created_principals.append(pidx)


def _detail(row) -> dict:
    raw = row["detail"]
    return json.loads(raw) if isinstance(raw, str) else raw


# ---------------------------------------------------------------------------
# GET /auth/whoami
# ---------------------------------------------------------------------------


async def test_auth_whoami_anonymous_returns_anonymous(auth_client):
    resp = await auth_client.get("/api/v1/auth/whoami")
    assert resp.status_code == 200
    assert resp.json() == {"kind": "anonymous"}


async def test_auth_whoami_human_returns_profile_and_role_and_scopes(
    auth_client, postgres_pool, jwks_harness
):
    pidx = await _seed_user(
        postgres_pool,
        email="whoami-human@example.com",
        role="wet_lab_admin",
        issuer=jwks_harness.issuer,
        subject="whoami-human",
    )
    _track(auth_client, pidx)
    token = jwks_harness.sign(
        _claims(
            jwks_harness, sub="whoami-human", email="whoami-human@example.com"
        )
    )
    resp = await auth_client.get(
        "/api/v1/auth/whoami",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["kind"] == "human"
    assert body["principal_idx"] == pidx
    assert body["email"] == "whoami-human@example.com"
    assert body["system_role"] == "wet_lab_admin"
    assert "references:write" in body["scopes"]  # wet_lab_admin ceiling
    assert body["profile_complete"] is True


async def test_auth_whoami_service_returns_service_summary(
    auth_client, postgres_pool
):
    from qiita_control_plane.auth.tokens import mint_api_token

    pidx = await postgres_pool.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ('whoami-svc', 'user', 1) RETURNING idx"
    )
    _track(auth_client, pidx)
    await postgres_pool.execute(
        "INSERT INTO qiita.service_account (principal_idx, name)"
        " VALUES ($1, 'whoami-svc-name')",
        pidx,
    )
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=pidx,
        label="whoami-svc",
        scopes=["features:mint"],
    )
    resp = await auth_client.get(
        "/api/v1/auth/whoami",
        headers={"Authorization": f"Bearer {plaintext}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "service"
    assert body["principal_idx"] == pidx
    assert body["name"] == "whoami-svc-name"
    assert body["scopes"] == ["features:mint"]


# ---------------------------------------------------------------------------
# POST /auth/pat
# ---------------------------------------------------------------------------


async def test_post_pat_returns_token_once(
    auth_client, postgres_pool, jwks_harness
):
    pidx = await _seed_user(
        postgres_pool,
        email="pat-once@example.com",
        issuer=jwks_harness.issuer,
        subject="pat-once",
    )
    _track(auth_client, pidx)
    jwt = jwks_harness.sign(
        _claims(jwks_harness, sub="pat-once", email="pat-once@example.com")
    )
    resp = await auth_client.post(
        "/api/v1/auth/pat",
        headers={"Authorization": f"Bearer {jwt}"},
        json={"label": "my-laptop"},
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["token"].startswith("qk_")
    assert len(body["token"]) == 46
    assert body["label"] == "my-laptop"
    assert body["token_idx"] > 0


async def test_post_pat_requires_oidc_jwt_not_pat(
    auth_client, postgres_pool, jwks_harness
):
    """A PAT token in the Authorization header is rejected — humans-only via OIDC."""
    from qiita_control_plane.auth.tokens import mint_api_token

    pidx = await _seed_user(
        postgres_pool,
        email="pat-not-pat@example.com",
        issuer=jwks_harness.issuer,
        subject="pat-not-pat",
    )
    _track(auth_client, pidx)
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=pidx,
        label="existing",
        scopes=["self:tokens"],
    )
    resp = await auth_client.post(
        "/api/v1/auth/pat",
        headers={"Authorization": f"Bearer {plaintext}"},
        json={"label": "would-be-new-pat"},
    )
    assert resp.status_code == 401


async def test_post_pat_rejects_jwt_with_stale_auth_time(
    auth_client, postgres_pool, jwks_harness
):
    pidx = await _seed_user(
        postgres_pool,
        email="pat-stale@example.com",
        issuer=jwks_harness.issuer,
        subject="pat-stale",
    )
    _track(auth_client, pidx)
    # auth_time 10 minutes ago > 300s threshold.
    stale = int(time.time()) - 600
    jwt = jwks_harness.sign(
        _claims(
            jwks_harness, sub="pat-stale", email="pat-stale@example.com",
            auth_time=stale,
        )
    )
    resp = await auth_client.post(
        "/api/v1/auth/pat",
        headers={"Authorization": f"Bearer {jwt}"},
        json={"label": "stale-attempt"},
    )
    assert resp.status_code == 401


async def test_post_pat_rejects_jwt_with_missing_auth_time(
    auth_client, postgres_pool, jwks_harness
):
    pidx = await _seed_user(
        postgres_pool,
        email="pat-no-at@example.com",
        issuer=jwks_harness.issuer,
        subject="pat-no-at",
    )
    _track(auth_client, pidx)
    claims = _claims(jwks_harness, sub="pat-no-at", email="pat-no-at@example.com")
    del claims["auth_time"]
    jwt = jwks_harness.sign(claims)
    resp = await auth_client.post(
        "/api/v1/auth/pat",
        headers={"Authorization": f"Bearer {jwt}"},
        json={"label": "no-at"},
    )
    assert resp.status_code == 401


async def test_post_pat_ttl_defaults_to_90_days(
    auth_client, postgres_pool, jwks_harness
):
    pidx = await _seed_user(
        postgres_pool,
        email="pat-ttl@example.com",
        issuer=jwks_harness.issuer,
        subject="pat-ttl",
    )
    _track(auth_client, pidx)
    jwt = jwks_harness.sign(
        _claims(jwks_harness, sub="pat-ttl", email="pat-ttl@example.com")
    )
    before = datetime.now(UTC)
    resp = await auth_client.post(
        "/api/v1/auth/pat",
        headers={"Authorization": f"Bearer {jwt}"},
        json={"label": "default-ttl"},
    )
    assert resp.status_code == 201
    expires = datetime.fromisoformat(resp.json()["expires_at"])
    expected = before + timedelta(days=90)
    delta = abs((expires - expected).total_seconds())
    assert delta < 60, f"expires_at not ~90d from now: {expires} vs {expected}"


async def test_post_pat_ttl_beyond_365_rejected(
    auth_client, postgres_pool, jwks_harness
):
    pidx = await _seed_user(
        postgres_pool,
        email="pat-ttl-too-long@example.com",
        issuer=jwks_harness.issuer,
        subject="pat-ttl-too-long",
    )
    _track(auth_client, pidx)
    jwt = jwks_harness.sign(
        _claims(jwks_harness, sub="pat-ttl-too-long", email="pat-ttl-too-long@example.com")
    )
    resp = await auth_client.post(
        "/api/v1/auth/pat",
        headers={"Authorization": f"Bearer {jwt}"},
        json={"label": "too-long", "ttl_days": 366},
    )
    assert resp.status_code == 422


async def test_post_pat_incomplete_profile_rejects_with_422(
    auth_client, postgres_pool, jwks_harness
):
    pidx = await _seed_user(
        postgres_pool,
        email="pat-incomplete@example.com",
        profile_complete=False,
        issuer=jwks_harness.issuer,
        subject="pat-incomplete",
    )
    _track(auth_client, pidx)
    jwt = jwks_harness.sign(
        _claims(jwks_harness, sub="pat-incomplete", email="pat-incomplete@example.com")
    )
    resp = await auth_client.post(
        "/api/v1/auth/pat",
        headers={"Authorization": f"Bearer {jwt}"},
        json={"label": "incomplete-attempt"},
    )
    assert resp.status_code == 422
    body = resp.json()
    assert body["detail"] == "profile incomplete"
    assert body["reason"] == "profile_incomplete"
    assert set(body["missing_fields"]) == {"affiliation", "address", "phone"}


async def test_post_pat_rejects_unknown_scope(
    auth_client, postgres_pool, jwks_harness
):
    pidx = await _seed_user(
        postgres_pool,
        email="pat-unknown-scope@example.com",
        issuer=jwks_harness.issuer,
        subject="pat-unknown-scope",
    )
    _track(auth_client, pidx)
    jwt = jwks_harness.sign(
        _claims(jwks_harness, sub="pat-unknown-scope", email="pat-unknown-scope@example.com")
    )
    resp = await auth_client.post(
        "/api/v1/auth/pat",
        headers={"Authorization": f"Bearer {jwt}"},
        json={
            "label": "weird-scope",
            "scopes": ["self:profile", "this:is:bogus"],
        },
    )
    assert resp.status_code == 422
    body = resp.json()
    assert "this:is:bogus" in body["rejected_scopes"]


async def test_post_pat_default_scopes_match_role_ceiling(
    auth_client, postgres_pool, jwks_harness
):
    pidx = await _seed_user(
        postgres_pool,
        email="pat-default-scopes@example.com",
        role="user",
        issuer=jwks_harness.issuer,
        subject="pat-default-scopes",
    )
    _track(auth_client, pidx)
    jwt = jwks_harness.sign(
        _claims(jwks_harness, sub="pat-default-scopes", email="pat-default-scopes@example.com")
    )
    resp = await auth_client.post(
        "/api/v1/auth/pat",
        headers={"Authorization": f"Bearer {jwt}"},
        json={"label": "default-scopes"},
    )
    assert resp.status_code == 201
    assert set(resp.json()["scopes"]) == {
        "self:profile", "self:tokens", "references:read",
    }


async def test_post_pat_system_admin_role_ceiling_includes_lower_role_scopes(
    auth_client, postgres_pool, jwks_harness
):
    pidx = await _seed_user(
        postgres_pool,
        email="pat-admin@example.com",
        role="system_admin",
        issuer=jwks_harness.issuer,
        subject="pat-admin",
    )
    _track(auth_client, pidx)
    jwt = jwks_harness.sign(
        _claims(jwks_harness, sub="pat-admin", email="pat-admin@example.com")
    )
    resp = await auth_client.post(
        "/api/v1/auth/pat",
        headers={"Authorization": f"Bearer {jwt}"},
        json={"label": "admin-default"},
    )
    assert resp.status_code == 201
    body = resp.json()
    # Inheriting: system_admin includes lower-tier scopes.
    assert "self:profile" in body["scopes"]
    assert "self:tokens" in body["scopes"]
    assert "references:read" in body["scopes"]
    assert "references:write" in body["scopes"]
    assert "admin:users" in body["scopes"]


async def test_post_pat_rejects_upscoping(
    auth_client, postgres_pool, jwks_harness
):
    pidx = await _seed_user(
        postgres_pool,
        email="pat-upscope@example.com",
        role="user",
        issuer=jwks_harness.issuer,
        subject="pat-upscope",
    )
    _track(auth_client, pidx)
    jwt = jwks_harness.sign(
        _claims(jwks_harness, sub="pat-upscope", email="pat-upscope@example.com")
    )
    resp = await auth_client.post(
        "/api/v1/auth/pat",
        headers={"Authorization": f"Bearer {jwt}"},
        json={"label": "up", "scopes": ["self:profile", "admin:users"]},
    )
    assert resp.status_code == 422
    body = resp.json()
    assert body["detail"] == "scopes not granted by your role"
    assert body["rejected_scopes"] == ["admin:users"]


async def test_post_pat_upscoping_error_does_not_leak_role_ceiling(
    auth_client, postgres_pool, jwks_harness
):
    """The 422 body should NOT include the caller's full role ceiling — that
    would let an attacker probe scope combinations to enumerate the map."""
    pidx = await _seed_user(
        postgres_pool,
        email="pat-noleak@example.com",
        role="user",
        issuer=jwks_harness.issuer,
        subject="pat-noleak",
    )
    _track(auth_client, pidx)
    jwt = jwks_harness.sign(
        _claims(jwks_harness, sub="pat-noleak", email="pat-noleak@example.com")
    )
    resp = await auth_client.post(
        "/api/v1/auth/pat",
        headers={"Authorization": f"Bearer {jwt}"},
        json={"label": "noleak", "scopes": ["admin:users"]},
    )
    assert resp.status_code == 422
    body = resp.json()
    assert "your_role_ceiling" not in body
    assert "ceiling" not in str(body)
    # Only the rejected scopes are echoed.
    assert set(body.keys()) == {"detail", "rejected_scopes"}


async def test_post_pat_writes_audit_event(
    auth_client, postgres_pool, jwks_harness
):
    pidx = await _seed_user(
        postgres_pool,
        email="pat-audit@example.com",
        issuer=jwks_harness.issuer,
        subject="pat-audit",
    )
    _track(auth_client, pidx)
    jwt = jwks_harness.sign(
        _claims(jwks_harness, sub="pat-audit", email="pat-audit@example.com")
    )
    resp = await auth_client.post(
        "/api/v1/auth/pat",
        headers={"Authorization": f"Bearer {jwt}"},
        json={"label": "audited"},
    )
    assert resp.status_code == 201
    token_idx = resp.json()["token_idx"]

    rows = await postgres_pool.fetch(
        "SELECT detail FROM qiita.auth_events"
        " WHERE event_type = 'token_mint' AND principal_idx = $1",
        pidx,
    )
    assert rows
    detail = _detail(rows[-1])
    assert detail["token_idx"] == token_idx
    # Plaintext NOT in audit detail.
    raw = json.dumps(detail)
    assert resp.json()["token"] not in raw


# ---------------------------------------------------------------------------
# GET /auth/tokens
# ---------------------------------------------------------------------------


async def test_get_own_tokens_lists_metadata_only(
    auth_client, postgres_pool, jwks_harness
):
    from qiita_control_plane.auth.tokens import mint_api_token

    pidx = await _seed_user(
        postgres_pool,
        email="list-own@example.com",
        issuer=jwks_harness.issuer,
        subject="list-own",
    )
    _track(auth_client, pidx)
    plaintext, token_idx = await mint_api_token(
        postgres_pool,
        principal_idx=pidx,
        label="for-listing",
        scopes=["self:tokens"],
    )

    resp = await auth_client.get(
        "/api/v1/auth/tokens",
        headers={"Authorization": f"Bearer {plaintext}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert any(t["token_idx"] == token_idx for t in body)
    # Plaintext / hash NEVER returned.
    raw = json.dumps(body)
    assert plaintext not in raw
    assert "token_hash" not in raw
    for t in body:
        assert "token" not in t  # only metadata keys


async def test_get_own_tokens_cannot_see_others(
    auth_client, postgres_pool, jwks_harness
):
    from qiita_control_plane.auth.tokens import mint_api_token

    # Two distinct users, each with their own token.
    pa = await _seed_user(
        postgres_pool, email="own-a@example.com",
        issuer=jwks_harness.issuer, subject="own-a",
    )
    pb = await _seed_user(
        postgres_pool, email="own-b@example.com",
        issuer=jwks_harness.issuer, subject="own-b",
    )
    _track(auth_client, pa)
    _track(auth_client, pb)
    pa_token, pa_idx = await mint_api_token(
        postgres_pool, principal_idx=pa, label="A", scopes=["self:tokens"],
    )
    pb_token, pb_idx = await mint_api_token(
        postgres_pool, principal_idx=pb, label="B", scopes=["self:tokens"],
    )

    resp = await auth_client.get(
        "/api/v1/auth/tokens",
        headers={"Authorization": f"Bearer {pa_token}"},
    )
    assert resp.status_code == 200
    listed_idxs = {t["token_idx"] for t in resp.json()}
    assert pa_idx in listed_idxs
    assert pb_idx not in listed_idxs


async def test_list_tokens_anonymous_401(auth_client):
    resp = await auth_client.get("/api/v1/auth/tokens")
    assert resp.status_code == 401


async def test_list_tokens_403_without_self_tokens_scope(
    auth_client, postgres_pool, jwks_harness
):
    from qiita_control_plane.auth.tokens import mint_api_token

    pidx = await _seed_user(
        postgres_pool, email="list-no-scope@example.com",
        issuer=jwks_harness.issuer, subject="list-no-scope",
    )
    _track(auth_client, pidx)
    plaintext, _ = await mint_api_token(
        postgres_pool, principal_idx=pidx, label="no-tokens-scope",
        scopes=["self:profile"],  # no self:tokens
    )
    resp = await auth_client.get(
        "/api/v1/auth/tokens",
        headers={"Authorization": f"Bearer {plaintext}"},
    )
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# DELETE /auth/tokens/{token_idx}
# ---------------------------------------------------------------------------


async def test_delete_own_token_revokes_and_writes_audit_event(
    auth_client, postgres_pool, jwks_harness
):
    from qiita_control_plane.auth.tokens import mint_api_token

    pidx = await _seed_user(
        postgres_pool, email="delete-own@example.com",
        issuer=jwks_harness.issuer, subject="delete-own",
    )
    _track(auth_client, pidx)
    auth_token, _ = await mint_api_token(
        postgres_pool, principal_idx=pidx, label="auth-token",
        scopes=["self:tokens"],
    )
    _, target_idx = await mint_api_token(
        postgres_pool, principal_idx=pidx, label="will-revoke",
        scopes=[],
    )

    resp = await auth_client.delete(
        f"/api/v1/auth/tokens/{target_idx}",
        headers={"Authorization": f"Bearer {auth_token}"},
    )
    assert resp.status_code == 204

    revoked = await postgres_pool.fetchval(
        "SELECT revoked_at FROM qiita.api_tokens WHERE token_idx = $1",
        target_idx,
    )
    assert revoked is not None

    rows = await postgres_pool.fetch(
        "SELECT detail FROM qiita.auth_events"
        " WHERE event_type = 'token_revoke' AND principal_idx = $1",
        pidx,
    )
    assert rows
    detail = _detail(rows[-1])
    assert detail["token_idx"] == target_idx


async def test_delete_others_token_returns_404_not_403(
    auth_client, postgres_pool, jwks_harness
):
    """Existence-hiding: trying to revoke another user's token returns the
    same 404 as a truly-nonexistent token_idx, so probing doesn't enumerate."""
    from qiita_control_plane.auth.tokens import mint_api_token

    pa = await _seed_user(
        postgres_pool, email="del-attacker@example.com",
        issuer=jwks_harness.issuer, subject="del-attacker",
    )
    pb = await _seed_user(
        postgres_pool, email="del-victim@example.com",
        issuer=jwks_harness.issuer, subject="del-victim",
    )
    _track(auth_client, pa)
    _track(auth_client, pb)
    attacker_token, _ = await mint_api_token(
        postgres_pool, principal_idx=pa, label="attacker",
        scopes=["self:tokens"],
    )
    _, victim_token_idx = await mint_api_token(
        postgres_pool, principal_idx=pb, label="victim", scopes=[],
    )

    resp_other = await auth_client.delete(
        f"/api/v1/auth/tokens/{victim_token_idx}",
        headers={"Authorization": f"Bearer {attacker_token}"},
    )
    resp_missing = await auth_client.delete(
        "/api/v1/auth/tokens/9999999999",
        headers={"Authorization": f"Bearer {attacker_token}"},
    )
    # Identical response shape — attacker can't tell which token_idx values
    # exist.
    assert resp_other.status_code == 404
    assert resp_missing.status_code == 404
    assert resp_other.json() == resp_missing.json()

    # Victim's token still active.
    revoked = await postgres_pool.fetchval(
        "SELECT revoked_at FROM qiita.api_tokens WHERE token_idx = $1",
        victim_token_idx,
    )
    assert revoked is None


async def test_post_pat_rejects_jwt_with_future_auth_time(
    auth_client, postgres_pool, jwks_harness
):
    """auth_time more than `authrocket_jwt_leeway_seconds` in the future
    must be rejected — otherwise an IdP with severe forward clock skew
    would let a forged-future-auth_time JWT bypass the freshness gate."""
    pidx = await _seed_user(
        postgres_pool,
        email="pat-future@example.com",
        issuer=jwks_harness.issuer,
        subject="pat-future",
    )
    _track(auth_client, pidx)
    # auth_time 1 hour in the future — well beyond any plausible leeway.
    future = int(time.time()) + 3600
    jwt = jwks_harness.sign(
        _claims(
            jwks_harness, sub="pat-future", email="pat-future@example.com",
            auth_time=future,
        )
    )
    resp = await auth_client.post(
        "/api/v1/auth/pat",
        headers={"Authorization": f"Bearer {jwt}"},
        json={"label": "future-attempt"},
    )
    assert resp.status_code == 401
