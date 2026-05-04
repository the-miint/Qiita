"""Auth boundary matrix for guarded routes.

Every guarded route is exercised against the standard negative-case set:
    (no_auth, 401) — no Authorization header
    (wrong_role / wrong_kind, 403) — authenticated but wrong role/kind
    (wrong_scope, 403) — authenticated but token lacks required scope
    (disabled_principal, 401) — principal disabled at the resolver layer
    (revoked_token, 401) — token revoked after issue
    (expired_token, 401) — token expires_at in the past

GET /api/v1/reference/{id} is anonymous-OK by design and is excluded
from the matrix; its dedicated coverage (anonymous=200, authenticated=200)
lives at the bottom of this file.
"""

import secrets
from datetime import UTC, datetime, timedelta

import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = pytest.mark.db


def _unique_suffix(human_label: str) -> str:
    """Combine a readable label with a random component so reruns of the
    same test don't collide on UNIQUE constraints (user.email,
    service_account.name)."""
    return f"{human_label}-{secrets.token_hex(4)}"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
async def boundary_client(postgres_pool):
    """Bare client — tests pass their own Authorization header per case.

    Settings is initialised because routes/reference.py routes pull
    `get_hmac_secret` (and one pulls `get_data_plane_url`) before the auth
    guard runs; without it those routes 500 instead of 401/403.
    """
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.settings = Settings(
        database_url="unused",
        hmac_secret_key=b"\x00" * 32,
        data_plane_url="grpc://localhost:50051",
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac


async def _seed_human_with_token(
    postgres_pool,
    *,
    system_role: str,
    scopes: list[str],
    profile_complete: bool = True,
    suffix: str | None = None,
    expires_at: datetime | None = None,
    disabled: bool = False,
    revoked: bool = False,
):
    """Seed a fresh human + mint a PAT with the given shape.

    The principal is created per-call with a unique display_name so tests
    don't collide on email uniqueness across the session. Returns the token
    plaintext and the principal_idx.
    """
    from qiita_control_plane.auth.token import mint_api_token

    # Always append a random component so reruns of the same test don't
    # collide on user.email (UNIQUE).
    base = suffix or f"{datetime.now(UTC).timestamp() * 1e6:.0f}"
    display_name = f"boundary-{_unique_suffix(base)}"
    email = f"{display_name}@example.com"
    async with postgres_pool.acquire() as conn:
        async with conn.transaction():
            pidx = await conn.fetchval(
                "INSERT INTO qiita.principal"
                "  (display_name, system_role, created_by_idx)"
                " VALUES ($1, $2, 1) RETURNING idx",
                display_name,
                system_role,
            )
            if profile_complete:
                await conn.execute(
                    "INSERT INTO qiita.user"
                    "  (principal_idx, email, affiliation, address, phone)"
                    " VALUES ($1, $2, 'X', 'Y', 'Z')",
                    pidx,
                    email,
                )
            else:
                await conn.execute(
                    "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
                    pidx,
                    email,
                )
    plaintext, token_idx = await mint_api_token(
        postgres_pool,
        principal_idx=pidx,
        label="boundary-test",
        scopes=scopes,
        expires_at=expires_at,
    )
    if disabled:
        await postgres_pool.execute(
            "UPDATE qiita.principal SET disabled = true,"
            " disabled_at = now(), disabled_by_idx = 1, disable_reason = 'test'"
            " WHERE idx = $1",
            pidx,
        )
    if revoked:
        await postgres_pool.execute(
            "UPDATE qiita.api_token SET revoked_at = now() WHERE token_idx = $1",
            token_idx,
        )
    return plaintext, pidx


async def _seed_service_with_token(
    postgres_pool,
    *,
    scopes: list[str],
    suffix: str | None = None,
):
    from qiita_control_plane.auth.token import mint_api_token

    base = suffix or f"{datetime.now(UTC).timestamp() * 1e6:.0f}"
    # Random component to avoid service_account.name UNIQUE collisions across runs.
    name = f"boundary-svc-{_unique_suffix(base)}"
    async with postgres_pool.acquire() as conn:
        async with conn.transaction():
            pidx = await conn.fetchval(
                "INSERT INTO qiita.principal"
                "  (display_name, system_role, created_by_idx)"
                " VALUES ($1, 'user', 1) RETURNING idx",
                name,
            )
            await conn.execute(
                "INSERT INTO qiita.service_account (principal_idx, name) VALUES ($1, $2)",
                pidx,
                name,
            )
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=pidx,
        label="boundary-svc",
        scopes=scopes,
    )
    return plaintext, pidx


def _h(token: str | None) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"} if token else {}


# ---------------------------------------------------------------------------
# POST /api/v1/reference — require_complete_profile + scope reference:write
# ---------------------------------------------------------------------------


_BODY_REF = {"name": "boundary-ref", "version": "1.0", "kind": "sequence_reference"}


async def test_post_references_no_auth_401(boundary_client):
    resp = await boundary_client.post("/api/v1/reference", json=_BODY_REF)
    assert resp.status_code == 401


async def test_post_references_service_account_403(boundary_client, postgres_pool):
    """Service kind cannot create references — require_human rejects."""
    token, _ = await _seed_service_with_token(
        postgres_pool,
        scopes=[
            "feature:mint",
            "reference:read",
            "reference:register_files",
            "ticket:doget",
        ],
        suffix="ref-svc",
    )
    resp = await boundary_client.post(
        "/api/v1/reference",
        json=_BODY_REF,
        headers=_h(token),
    )
    assert resp.status_code == 403


async def test_post_references_missing_scope_403(boundary_client, postgres_pool):
    """Human + complete profile but no reference:write scope."""
    token, _ = await _seed_human_with_token(
        postgres_pool,
        system_role="user",
        scopes=["self:profile", "reference:read"],
        suffix="ref-no-scope",
    )
    resp = await boundary_client.post(
        "/api/v1/reference",
        json=_BODY_REF,
        headers=_h(token),
    )
    assert resp.status_code == 403


async def test_post_references_incomplete_profile_422(boundary_client, postgres_pool):
    """require_complete_profile gives 422 when profile is incomplete."""
    token, _ = await _seed_human_with_token(
        postgres_pool,
        system_role="wet_lab_admin",
        scopes=["self:profile", "self:token", "reference:read", "reference:write"],
        profile_complete=False,
        suffix="ref-incomplete",
    )
    resp = await boundary_client.post(
        "/api/v1/reference",
        json=_BODY_REF,
        headers=_h(token),
    )
    assert resp.status_code == 422


async def test_post_references_disabled_principal_401(boundary_client, postgres_pool):
    token, _ = await _seed_human_with_token(
        postgres_pool,
        system_role="wet_lab_admin",
        scopes=["self:profile", "self:token", "reference:read", "reference:write"],
        disabled=True,
        suffix="ref-disabled",
    )
    resp = await boundary_client.post(
        "/api/v1/reference",
        json=_BODY_REF,
        headers=_h(token),
    )
    assert resp.status_code == 401


async def test_post_references_revoked_token_401(boundary_client, postgres_pool):
    token, _ = await _seed_human_with_token(
        postgres_pool,
        system_role="wet_lab_admin",
        scopes=["self:profile", "self:token", "reference:read", "reference:write"],
        revoked=True,
        suffix="ref-revoked",
    )
    resp = await boundary_client.post(
        "/api/v1/reference",
        json=_BODY_REF,
        headers=_h(token),
    )
    assert resp.status_code == 401


async def test_post_references_expired_token_401(boundary_client, postgres_pool):
    token, _ = await _seed_human_with_token(
        postgres_pool,
        system_role="wet_lab_admin",
        scopes=["self:profile", "self:token", "reference:read", "reference:write"],
        expires_at=datetime.now(UTC) - timedelta(seconds=1),
        suffix="ref-expired",
    )
    resp = await boundary_client.post(
        "/api/v1/reference",
        json=_BODY_REF,
        headers=_h(token),
    )
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# POST /reference/{id}/feature/mint — require_service + feature:mint
# ---------------------------------------------------------------------------


async def _seed_active_reference(postgres_pool, suffix: str) -> int:
    # Random component prevents (name, version) UNIQUE collisions across runs.
    return await postgres_pool.fetchval(
        "INSERT INTO qiita.reference"
        "  (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', 'hashing', 1)"
        " RETURNING reference_idx",
        f"boundary-mint-{_unique_suffix(suffix)}",
    )


async def test_mint_features_no_auth_401(boundary_client, postgres_pool):
    ref_idx = await _seed_active_reference(postgres_pool, "no-auth")
    resp = await boundary_client.post(
        f"/api/v1/reference/{ref_idx}/feature/mint",
        json={"entries": [{"sequence_hash": "00000000-0000-0000-0000-000000000001"}]},
    )
    assert resp.status_code == 401


async def test_mint_features_human_403(boundary_client, postgres_pool):
    """Human cannot mint — workers only."""
    ref_idx = await _seed_active_reference(postgres_pool, "human-blocked")
    token, _ = await _seed_human_with_token(
        postgres_pool,
        system_role="system_admin",
        scopes=[
            "self:profile",
            "self:token",
            "reference:read",
            "reference:write",
            "admin:user",
            "admin:service_account",
            "admin:audit_read",
        ],
        suffix="mint-human",
    )
    resp = await boundary_client.post(
        f"/api/v1/reference/{ref_idx}/feature/mint",
        json={"entries": [{"sequence_hash": "00000000-0000-0000-0000-000000000002"}]},
        headers=_h(token),
    )
    assert resp.status_code == 403


async def test_mint_features_service_missing_scope_403(boundary_client, postgres_pool):
    """Service token without feature:mint."""
    ref_idx = await _seed_active_reference(postgres_pool, "svc-no-scope")
    token, _ = await _seed_service_with_token(
        postgres_pool,
        scopes=["reference:read"],
        suffix="mint-svc-no-scope",
    )
    resp = await boundary_client.post(
        f"/api/v1/reference/{ref_idx}/feature/mint",
        json={"entries": [{"sequence_hash": "00000000-0000-0000-0000-000000000003"}]},
        headers=_h(token),
    )
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# POST /reference/{id}/register — require_service + reference:register_files
# ---------------------------------------------------------------------------


async def test_register_files_human_403(boundary_client, postgres_pool):
    ref_idx = await _seed_active_reference(postgres_pool, "register-human")
    token, _ = await _seed_human_with_token(
        postgres_pool,
        system_role="system_admin",
        scopes=[
            "self:profile",
            "self:token",
            "reference:read",
            "reference:write",
            "admin:user",
            "admin:service_account",
            "admin:audit_read",
        ],
        suffix="reg-human",
    )
    resp = await boundary_client.post(
        f"/api/v1/reference/{ref_idx}/register",
        json={"staging_dir": "/tmp/x", "files": {}},
        headers=_h(token),
    )
    assert resp.status_code == 403


async def test_register_files_service_missing_scope_403(boundary_client, postgres_pool):
    ref_idx = await _seed_active_reference(postgres_pool, "register-no-scope")
    token, _ = await _seed_service_with_token(
        postgres_pool,
        scopes=["reference:read", "feature:mint"],
        suffix="reg-no-scope",
    )
    resp = await boundary_client.post(
        f"/api/v1/reference/{ref_idx}/register",
        json={"staging_dir": "/tmp/x", "files": {}},
        headers=_h(token),
    )
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# POST /reference/{id}/ticket/doget — scope ticket:doget
# ---------------------------------------------------------------------------


async def test_doget_no_auth_401(boundary_client, postgres_pool):
    ref_idx = await _seed_active_reference(postgres_pool, "doget-no-auth")
    resp = await boundary_client.post(
        f"/api/v1/reference/{ref_idx}/ticket/doget",
        json={"table": "reference_sequences"},
    )
    assert resp.status_code == 401


async def test_doget_missing_scope_403(boundary_client, postgres_pool):
    ref_idx = await _seed_active_reference(postgres_pool, "doget-no-scope")
    token, _ = await _seed_human_with_token(
        postgres_pool,
        system_role="user",
        scopes=["self:profile", "reference:read"],
        suffix="doget-no-scope",
    )
    resp = await boundary_client.post(
        f"/api/v1/reference/{ref_idx}/ticket/doget",
        json={"table": "reference_sequences"},
        headers=_h(token),
    )
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# GET /api/v1/user/me — require_human
# ---------------------------------------------------------------------------


async def test_get_me_anonymous_401(boundary_client):
    resp = await boundary_client.get("/api/v1/user/me")
    assert resp.status_code == 401


async def test_get_me_service_403(boundary_client, postgres_pool):
    token, _ = await _seed_service_with_token(
        postgres_pool,
        scopes=["feature:mint"],
        suffix="me-svc",
    )
    resp = await boundary_client.get("/api/v1/user/me", headers=_h(token))
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# PATCH /api/v1/user/me — require_human + self:profile
# ---------------------------------------------------------------------------


async def test_patch_me_missing_scope_403(boundary_client, postgres_pool):
    """Human, but token doesn't carry self:profile."""
    token, _ = await _seed_human_with_token(
        postgres_pool,
        system_role="user",
        scopes=["reference:read"],  # no self:profile
        suffix="patch-no-scope",
    )
    resp = await boundary_client.patch(
        "/api/v1/user/me",
        json={"affiliation": "X"},
        headers=_h(token),
    )
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# GET /reference/{id} — anonymous-OK by design
# ---------------------------------------------------------------------------


async def test_get_reference_anonymous_returns_200(boundary_client, postgres_pool):
    ref_idx = await _seed_active_reference(postgres_pool, "get-anon")
    resp = await boundary_client.get(f"/api/v1/reference/{ref_idx}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["reference_idx"] == ref_idx
    assert body["created_by_idx"] is not None  # H.b dual-write invariant


async def test_get_reference_authenticated_returns_200(boundary_client, postgres_pool):
    """Authenticated reads also work — same payload as anonymous."""
    ref_idx = await _seed_active_reference(postgres_pool, "get-auth")
    token, _ = await _seed_human_with_token(
        postgres_pool,
        system_role="user",
        scopes=["reference:read"],
        suffix="get-auth",
    )
    resp = await boundary_client.get(f"/api/v1/reference/{ref_idx}", headers=_h(token))
    assert resp.status_code == 200
    assert resp.json()["reference_idx"] == ref_idx
