"""Integration tests for `auth.principal.get_current_principal`.

Each test mounts a small ad-hoc FastAPI app with a single /resolve endpoint
that calls the resolver dep and returns a JSON view of the result. Testing
the resolver through a dedicated test endpoint isolates its behaviour from
route logic.
"""

import asyncio
import json
import time

import pytest
from fastapi import Depends, FastAPI
from httpx import ASGITransport, AsyncClient


def _detail(row) -> dict:
    """asyncpg returns JSONB columns as raw JSON strings without an explicit
    codec. Tests that inspect auth_events.detail need to parse it."""
    raw = row["detail"]
    return json.loads(raw) if isinstance(raw, str) else raw


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_resolver_app(postgres_pool, oidc_verifier=None):
    """Build a tiny FastAPI app whose /resolve endpoint returns the resolved
    principal as a JSON shape useful for assertions."""
    from qiita_control_plane.auth.principal import (
        HumanUser,
        Principal,
        ServiceAccount,
        get_current_principal,
    )

    app = FastAPI()
    app.state.pool = postgres_pool
    app.state.oidc_verifier = oidc_verifier

    @app.get("/resolve")
    async def resolve(
        p: Principal = Depends(get_current_principal),
    ):
        if isinstance(p, HumanUser):
            return {
                "kind": "human",
                "principal_idx": p.principal_idx,
                "email": p.email,
                "system_role": p.system_role,
                "scopes": sorted(p.scopes),
                "profile_complete": p.profile_complete,
                "disabled": p.disabled,
                "retired": p.retired,
            }
        if isinstance(p, ServiceAccount):
            return {
                "kind": "service",
                "principal_idx": p.principal_idx,
                "name": p.name,
                "scopes": sorted(p.scopes),
                "disabled": p.disabled,
                "retired": p.retired,
            }
        return {"kind": "anonymous"}

    return app


def _claims(jwks_harness, **overrides) -> dict:
    now = int(time.time())
    base = {
        "iss": jwks_harness.issuer,
        "aud": "test-audience",
        "sub": f"sub-{int(time.time() * 1000)}",
        "email": "user@example.com",
        "email_verified": True,
        "iat": now,
        "exp": now + 3600,
    }
    base.update(overrides)
    return base


def _verifier(jwks_harness):
    from qiita_control_plane.auth.oidc import JwtVerifier

    return JwtVerifier(
        jwks_url=jwks_harness.jwks_url,
        issuer=jwks_harness.issuer,
        audience="test-audience",
        leeway_seconds=30,
    )


@pytest.fixture
async def resolver_client(postgres_pool, jwks_harness):
    """AsyncClient bound to a tiny FastAPI app with the resolver wired in.

    Tracks principals it creates so we can clean them up afterward.
    """
    app = _make_resolver_app(postgres_pool, oidc_verifier=_verifier(jwks_harness))

    created: list[int] = []

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        ac._created_principals = created
        ac._postgres_pool = postgres_pool
        yield ac

    if created:
        # Cleanup must fully reset state so reruns don't see stale email/sub
        # collisions. qiita.auth_events is append-only by design — UPDATE/DELETE
        # are blocked by triggers — so we temporarily disable those triggers
        # for the cleanup transaction. Production never does this; it's
        # test-only infrastructure.
        async with postgres_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "ALTER TABLE qiita.auth_events DISABLE TRIGGER auth_events_no_delete"
                )
                try:
                    await conn.execute(
                        "DELETE FROM qiita.api_tokens"
                        " WHERE principal_idx = ANY($1::bigint[])",
                        created,
                    )
                    await conn.execute(
                        "DELETE FROM qiita.user_identities"
                        " WHERE principal_idx = ANY($1::bigint[])",
                        created,
                    )
                    await conn.execute(
                        "DELETE FROM qiita.user"
                        " WHERE principal_idx = ANY($1::bigint[])",
                        created,
                    )
                    await conn.execute(
                        "DELETE FROM qiita.service_account"
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


def _track(client, principal_idx: int):
    """Mark a principal_idx for cleanup after the test."""
    client._created_principals.append(principal_idx)


# ---------------------------------------------------------------------------
# Dispatch / no-bearer
# ---------------------------------------------------------------------------


async def test_resolver_returns_anonymous_when_no_header(resolver_client):
    resp = await resolver_client.get("/resolve")
    assert resp.status_code == 200
    assert resp.json() == {"kind": "anonymous"}


async def test_resolver_rejects_non_bearer_scheme(resolver_client):
    """Authorization: Basic ... is a malformed auth *attempt*, not absence
    of auth. Surface as 401 rather than silently downgrading to Anonymous,
    so client misconfiguration is visible on public routes too."""
    resp = await resolver_client.get("/resolve", headers={"Authorization": "Basic xyz"})
    assert resp.status_code == 401


async def test_resolver_rejects_empty_bearer(resolver_client):
    """`Authorization: Bearer ` with empty credential is unambiguously
    a malformed attempt — same 401 treatment as a non-Bearer scheme."""
    resp = await resolver_client.get("/resolve", headers={"Authorization": "Bearer "})
    assert resp.status_code == 401


async def test_resolver_rejects_malformed_bearer(resolver_client):
    """Doesn't start with qk_, doesn't have JWT shape — 401."""
    resp = await resolver_client.get(
        "/resolve", headers={"Authorization": "Bearer this-is-just-junk"}
    )
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Token path
# ---------------------------------------------------------------------------


async def test_resolver_dispatches_qk_prefix_to_token_path(
    resolver_client, postgres_pool
):
    """A qk_ token resolves to the owning principal's HumanUser/ServiceAccount."""
    from qiita_control_plane.auth.tokens import mint_api_token

    # Seed: principal + user
    pidx = await postgres_pool.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ('token-resolve', 'user', 1) RETURNING idx"
    )
    _track(resolver_client, pidx)
    await postgres_pool.execute(
        "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
        pidx,
        f"u{pidx}@example.com",
    )

    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=pidx,
        label="resolver-test",
        scopes=["self:profile", "references:read"],
    )
    resp = await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {plaintext}"}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["kind"] == "human"
    assert body["principal_idx"] == pidx
    assert sorted(body["scopes"]) == ["references:read", "self:profile"]


async def test_resolver_token_path_for_service_account(resolver_client, postgres_pool):
    from qiita_control_plane.auth.tokens import mint_api_token

    pidx = await postgres_pool.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ('svc-resolve', 'user', 1) RETURNING idx"
    )
    _track(resolver_client, pidx)
    await postgres_pool.execute(
        "INSERT INTO qiita.service_account (principal_idx, name) VALUES ($1, $2)",
        pidx,
        f"svc-{pidx}",
    )
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=pidx,
        label="svc-resolver",
        scopes=["features:mint"],
    )
    resp = await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {plaintext}"}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "service"
    assert body["principal_idx"] == pidx
    assert body["scopes"] == ["features:mint"]


async def test_resolver_rejects_revoked_token(resolver_client, postgres_pool):
    from qiita_control_plane.auth.tokens import mint_api_token

    pidx = await postgres_pool.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ('rev-resolve', 'user', 1) RETURNING idx"
    )
    _track(resolver_client, pidx)
    await postgres_pool.execute(
        "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
        pidx,
        f"u{pidx}@example.com",
    )
    plaintext, token_idx = await mint_api_token(
        postgres_pool,
        principal_idx=pidx,
        label="will-revoke",
        scopes=[],
    )
    await postgres_pool.execute(
        "UPDATE qiita.api_tokens SET revoked_at = now() WHERE token_idx = $1",
        token_idx,
    )
    resp = await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {plaintext}"}
    )
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# OIDC path — first login
# ---------------------------------------------------------------------------


async def test_resolver_dispatches_jwt_header_shape_to_oidc_path(
    resolver_client, postgres_pool, jwks_harness
):
    """A 3-segment JWT goes through the OIDC path; first login creates rows."""
    claims = _claims(
        jwks_harness, sub="first-login-1", email="first.login.1@example.com"
    )
    token = jwks_harness.sign(claims)
    resp = await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {token}"}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["kind"] == "human"
    assert body["email"] == "first.login.1@example.com"
    pidx = body["principal_idx"]
    _track(resolver_client, pidx)

    # Verify all three rows landed.
    p = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.principal WHERE idx = $1", pidx
    )
    u = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.user WHERE principal_idx = $1", pidx
    )
    ui = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.user_identities WHERE principal_idx = $1",
        pidx,
    )
    assert (p, u, ui) == (1, 1, 1)


async def test_resolver_creates_audit_event_on_first_login(
    resolver_client, postgres_pool, jwks_harness
):
    claims = _claims(jwks_harness, sub="first-login-audit", email="audit@example.com")
    token = jwks_harness.sign(claims)
    resp = await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {token}"}
    )
    assert resp.status_code == 200
    pidx = resp.json()["principal_idx"]
    _track(resolver_client, pidx)

    events = await postgres_pool.fetch(
        "SELECT event_type, detail FROM qiita.auth_events"
        " WHERE principal_idx = $1 ORDER BY event_idx",
        pidx,
    )
    types = [e["event_type"] for e in events]
    assert "oidc_create_principal" in types


async def test_resolver_409s_on_email_collision_with_different_iss_sub(
    resolver_client, postgres_pool, jwks_harness
):
    # First login with a unique email.
    c1 = _claims(jwks_harness, sub="A", email="collide@example.com")
    r1 = await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {jwks_harness.sign(c1)}"}
    )
    assert r1.status_code == 200
    _track(resolver_client, r1.json()["principal_idx"])

    # Second login: same email, different (iss, sub).
    c2 = _claims(jwks_harness, sub="B", email="collide@example.com")
    r2 = await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {jwks_harness.sign(c2)}"}
    )
    assert r2.status_code == 409, r2.text

    # Audit event recorded.
    rows = await postgres_pool.fetch(
        "SELECT detail FROM qiita.auth_events"
        " WHERE event_type = 'oidc_create_principal_email_conflict'"
        "   AND detail->>'issuer' = $1",
        jwks_harness.issuer,
    )
    assert rows
    # Audit detail must NOT contain cleartext email; only its sha256.
    for row in rows:
        d = _detail(row)
        assert "collide@example.com" not in json.dumps(d)
        assert "attempted_email_sha256" in d


async def test_resolver_accepts_jwt_with_email_verified_false(
    resolver_client, jwks_harness
):
    """The verifier no longer strict-checks email_verified — LoginRocket Web
    JWTs omit the claim entirely, and the realm enforces verification at
    signup as policy. See docs/auth.md and the realm runbook.

    The resolver therefore accepts tokens with `email_verified=false` (or
    missing) and creates the principal as usual. If a realm operator
    misconfigures and allows unverified emails through, that's a realm-side
    policy issue, not something the verifier should re-check.
    """
    # Use a unique email so this test doesn't collide with stale state from
    # an earlier run that minted a principal under the same default email.
    claims = _claims(
        jwks_harness,
        email_verified=False,
        sub="ev-false",
        email=f"ev-false-{int(time.time() * 1000)}@example.com",
    )
    token = jwks_harness.sign(claims)
    resp = await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {token}"}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    _track(resolver_client, body["principal_idx"])
    assert body["kind"] == "human"


# ---------------------------------------------------------------------------
# OIDC path — repeat login, drift, race, status
# ---------------------------------------------------------------------------


async def test_resolver_reuses_existing_identity_on_repeat_login(
    resolver_client, jwks_harness
):
    sub = "repeat-A"
    email = "repeat@example.com"
    c = _claims(jwks_harness, sub=sub, email=email)

    r1 = await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {jwks_harness.sign(c)}"}
    )
    assert r1.status_code == 200
    p1 = r1.json()["principal_idx"]
    _track(resolver_client, p1)

    r2 = await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {jwks_harness.sign(c)}"}
    )
    assert r2.status_code == 200
    assert r2.json()["principal_idx"] == p1


async def test_resolver_updates_email_on_drift_when_no_collision(
    resolver_client, postgres_pool, jwks_harness
):
    sub = "drift-A"
    c1 = _claims(jwks_harness, sub=sub, email="drift1@example.com")
    r1 = await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {jwks_harness.sign(c1)}"}
    )
    pidx = r1.json()["principal_idx"]
    _track(resolver_client, pidx)

    c2 = _claims(jwks_harness, sub=sub, email="drift2@example.com")
    r2 = await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {jwks_harness.sign(c2)}"}
    )
    assert r2.status_code == 200
    assert r2.json()["email"] == "drift2@example.com"

    events = await postgres_pool.fetch(
        "SELECT detail FROM qiita.auth_events"
        " WHERE event_type = 'email_drift' AND principal_idx = $1",
        pidx,
    )
    assert events
    assert _detail(events[0])["outcome"] == "updated"


async def test_resolver_noops_and_audits_email_drift_on_collision(
    resolver_client, postgres_pool, jwks_harness
):
    # Two principals, two unique emails.
    c1 = _claims(jwks_harness, sub="dr-A", email="dr-a@example.com")
    c2 = _claims(jwks_harness, sub="dr-B", email="dr-b@example.com")
    r1 = await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {jwks_harness.sign(c1)}"}
    )
    r2 = await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {jwks_harness.sign(c2)}"}
    )
    pa = r1.json()["principal_idx"]
    pb = r2.json()["principal_idx"]
    _track(resolver_client, pa)
    _track(resolver_client, pb)

    # Now A logs in with B's email. Drift attempt → no-op + collision audit.
    c1_drift = _claims(jwks_harness, sub="dr-A", email="dr-b@example.com")
    r3 = await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {jwks_harness.sign(c1_drift)}"}
    )
    assert r3.status_code == 200
    # A's stored email is unchanged.
    assert r3.json()["email"] == "dr-a@example.com"

    events = await postgres_pool.fetch(
        "SELECT detail FROM qiita.auth_events"
        " WHERE event_type = 'email_drift' AND principal_idx = $1",
        pa,
    )
    assert events
    parsed = [_detail(e) for e in events]
    collision = [d for d in parsed if d.get("outcome") == "collision"]
    assert collision
    detail = collision[0]
    # Cleartext attempted email NOT present.
    assert "dr-b@example.com" not in json.dumps(detail)
    assert "attempted_email_sha256" in detail


async def test_resolver_email_drift_uses_sha256_hash_for_collision_attempt(
    resolver_client, postgres_pool, jwks_harness
):
    """The hash in the audit detail should be sha256(jwt_email)."""
    from qiita_control_plane.auth.audit import sha256_hex

    c1 = _claims(jwks_harness, sub="hash-A", email="hash-a@example.com")
    c2 = _claims(jwks_harness, sub="hash-B", email="hash-b@example.com")
    r1 = await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {jwks_harness.sign(c1)}"}
    )
    r2 = await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {jwks_harness.sign(c2)}"}
    )
    pa = r1.json()["principal_idx"]
    pb = r2.json()["principal_idx"]
    _track(resolver_client, pa)
    _track(resolver_client, pb)

    drift = _claims(jwks_harness, sub="hash-A", email="hash-b@example.com")
    await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {jwks_harness.sign(drift)}"}
    )

    rows = await postgres_pool.fetch(
        "SELECT detail FROM qiita.auth_events"
        " WHERE event_type = 'email_drift' AND principal_idx = $1"
        "   AND detail->>'outcome' = 'collision'",
        pa,
    )
    assert rows
    expected_hash = sha256_hex("hash-b@example.com")
    assert _detail(rows[0])["attempted_email_sha256"] == expected_hash


async def test_resolver_refuses_oidc_upsert_for_disabled_principal(
    resolver_client, postgres_pool, jwks_harness
):
    sub = "disabled-A"
    c = _claims(jwks_harness, sub=sub, email="disabled@example.com")
    r1 = await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {jwks_harness.sign(c)}"}
    )
    pidx = r1.json()["principal_idx"]
    _track(resolver_client, pidx)

    # Mark disabled.
    await postgres_pool.execute(
        "UPDATE qiita.principal SET"
        "  disabled = true, disabled_at = now(), disabled_by_idx = 1"
        " WHERE idx = $1",
        pidx,
    )

    # Same identity tries again → 401.
    r2 = await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {jwks_harness.sign(c)}"}
    )
    assert r2.status_code == 401


async def test_resolver_refuses_oidc_upsert_for_retired_principal(
    resolver_client, postgres_pool, jwks_harness
):
    sub = "retired-A"
    c = _claims(jwks_harness, sub=sub, email="retired@example.com")
    r1 = await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {jwks_harness.sign(c)}"}
    )
    pidx = r1.json()["principal_idx"]
    _track(resolver_client, pidx)

    await postgres_pool.execute(
        "UPDATE qiita.principal SET"
        "  retired = true, retired_at = now(), retired_by_idx = 1"
        " WHERE idx = $1",
        pidx,
    )

    r2 = await resolver_client.get(
        "/resolve", headers={"Authorization": f"Bearer {jwks_harness.sign(c)}"}
    )
    assert r2.status_code == 401


async def test_resolver_handles_concurrent_same_iss_sub_race(
    resolver_client, jwks_harness, postgres_pool
):
    """Two parallel first-logins for the same (iss, sub) should produce
    exactly one principal, with the loser re-reading the winner's idx."""
    sub = "race-A"
    c = _claims(jwks_harness, sub=sub, email="race@example.com")
    token = jwks_harness.sign(c)

    async def _login():
        return await resolver_client.get(
            "/resolve", headers={"Authorization": f"Bearer {token}"}
        )

    r1, r2 = await asyncio.gather(_login(), _login())
    assert r1.status_code == 200, r1.text
    assert r2.status_code == 200, r2.text
    p1 = r1.json()["principal_idx"]
    p2 = r2.json()["principal_idx"]
    assert p1 == p2  # same principal even though both raced first-login
    _track(resolver_client, p1)

    # Exactly one user_identities row for this (iss, sub).
    n = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.user_identities WHERE issuer = $1 AND subject = $2",
        jwks_harness.issuer,
        sub,
    )
    assert n == 1


async def test_resolver_oidc_with_no_verifier_returns_503(postgres_pool, jwks_harness):
    """If the app starts without an OIDC verifier configured, JWT-shaped
    bearers get a clear 503 (not a 500 or silent ignore)."""
    app = _make_resolver_app(postgres_pool, oidc_verifier=None)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        token = jwks_harness.sign(_claims(jwks_harness, sub="no-verifier"))
        resp = await ac.get("/resolve", headers={"Authorization": f"Bearer {token}"})
        assert resp.status_code == 503
