"""Principal types and the request-time resolver.

A Principal is the typed view of an authenticated request. Three runtime
kinds:
  - HumanUser   — has rows in qiita.user (subtype of principal)
  - ServiceAccount — has rows in qiita.service_account (subtype of principal)
  - Anonymous   — no Authorization header

The resolver dispatches by Authorization-header shape:
  - "Bearer qk_..."         → token path (Phase C verifier)
  - "Bearer eyJ...x.y.z"    → OIDC path (Phase D verifier + first-login UPSERT)
  - "Bearer <other>"        → 401 malformed
  - missing / non-Bearer    → Anonymous

For OIDC, first-login creates principal + user + user_identities atomically.
Email-collision-with-different-(iss,sub) returns 409 + audit event. Concurrent
first-logins for the same (iss, sub) race on the user_identities PK; the
loser catches the unique violation, re-reads, and returns the winner's
principal_idx.

Disabled / retired principals are rejected at the resolver layer for both
paths — verify_api_token already filters by these flags, and the OIDC
upsert refuses to construct a Principal for a (disabled OR retired) row.
"""

from __future__ import annotations

from dataclasses import dataclass

import asyncpg
from fastapi import HTTPException, Request

from .audit import record_event, sha256_hex
from .oidc import InvalidJwt, JwtVerifier
from .tokens import verify_api_token

_ROLE_ORDER = {"user": 0, "wet_lab_admin": 1, "system_admin": 2}


class Principal:
    """Base for HumanUser, ServiceAccount, Anonymous.

    Default capability methods return False so guards can call methods
    without isinstance gymnastics. Empty __slots__ keeps subclasses
    (frozen slotted dataclasses) memory-tight and lets them inherit from
    Principal while still being instances of it for FastAPI typing.
    """

    __slots__ = ()

    def has_role(self, role: str) -> bool:
        return False

    def has_role_at_least(self, role: str) -> bool:
        return False

    def has_scope(self, scope: str) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class HumanUser(Principal):
    principal_idx: int
    email: str
    system_role: str  # one of qiita.system_role enum: user / wet_lab_admin / system_admin
    scopes: frozenset[str]
    profile_complete: bool
    disabled: bool
    retired: bool

    def has_role(self, role: str) -> bool:
        return self.system_role == role

    def has_role_at_least(self, role: str) -> bool:
        return _ROLE_ORDER[self.system_role] >= _ROLE_ORDER[role]

    def has_scope(self, scope: str) -> bool:
        return scope in self.scopes


@dataclass(frozen=True, slots=True)
class ServiceAccount(Principal):
    principal_idx: int
    name: str
    scopes: frozenset[str]
    disabled: bool
    retired: bool

    def has_scope(self, scope: str) -> bool:
        return scope in self.scopes

    # has_role / has_role_at_least always return False — services don't
    # use the system_role hierarchy. Their authz is scope-only.


@dataclass(frozen=True, slots=True)
class Anonymous(Principal):
    pass


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------


_BEARER = "Bearer "


def _looks_like_jwt(s: str) -> bool:
    """Cheap structural check: a JWT has exactly two dots separating
    header.payload.signature segments. We delegate the actual signature
    verification to PyJWT — this is just dispatch."""
    return s.count(".") == 2


async def get_current_principal(request: Request) -> Principal:
    """FastAPI dependency: resolve the current principal from the request.

    Returns:
      - Anonymous() when no Authorization header is present (or it's not Bearer).
      - HumanUser / ServiceAccount on successful auth.

    Raises HTTPException 401 on invalid token / JWT, malformed bearer,
    or disabled/retired principal. Raises 503 if a JWT-shaped bearer
    arrives but the OIDC verifier is not configured.
    """
    auth = request.headers.get("Authorization", "")
    if not auth.startswith(_BEARER):
        return Anonymous()
    bearer = auth[len(_BEARER) :].strip()
    if not bearer:
        return Anonymous()

    pool = _get_pool(request)

    if bearer.startswith("qk_"):
        return await _resolve_token(pool, bearer)
    if _looks_like_jwt(bearer):
        verifier = _get_oidc_verifier(request)
        return await _resolve_oidc(pool, verifier, bearer)

    raise HTTPException(status_code=401, detail="malformed bearer token")


def _get_pool(request: Request) -> asyncpg.Pool:
    pool = getattr(request.app.state, "pool", None)
    if pool is None:
        raise RuntimeError("Database pool not initialised")
    return pool


def _get_oidc_verifier(request: Request) -> JwtVerifier:
    verifier = getattr(request.app.state, "oidc_verifier", None)
    if verifier is None:
        raise HTTPException(
            status_code=503,
            detail="OIDC verifier is not configured (set AUTHROCKET_* env vars)",
        )
    return verifier


# ---------------------------------------------------------------------------
# Token path
# ---------------------------------------------------------------------------


async def _resolve_token(pool: asyncpg.Pool, plaintext: str) -> Principal:
    verified = await verify_api_token(pool, plaintext)
    if verified is None:
        raise HTTPException(status_code=401, detail="invalid or revoked token")

    # verify_api_token already filters disabled/retired. Re-fetch principal
    # info to populate the typed Principal.
    row = await pool.fetchrow(
        "SELECT p.idx, p.system_role, p.disabled, p.retired,"
        "  u.email, u.profile_complete,"
        "  sa.name AS service_name"
        " FROM qiita.principal p"
        " LEFT JOIN qiita.user u ON u.principal_idx = p.idx"
        " LEFT JOIN qiita.service_account sa ON sa.principal_idx = p.idx"
        " WHERE p.idx = $1",
        verified.principal_idx,
    )
    if row is None:
        # Token references missing principal — schema FK would normally
        # prevent this, but fail closed.
        raise HTTPException(status_code=401, detail="principal not found")

    if row["email"] is not None:
        return HumanUser(
            principal_idx=row["idx"],
            email=row["email"],
            system_role=row["system_role"],
            scopes=verified.scopes,
            profile_complete=row["profile_complete"],
            disabled=row["disabled"],
            retired=row["retired"],
        )
    if row["service_name"] is not None:
        return ServiceAccount(
            principal_idx=row["idx"],
            name=row["service_name"],
            scopes=verified.scopes,
            disabled=row["disabled"],
            retired=row["retired"],
        )
    # Bare principal holding a token shouldn't happen (sentinel CHECK +
    # subtype creation is the only path that produces a token-bearing
    # principal in practice). Fail closed.
    raise HTTPException(status_code=401, detail="principal has no auth subtype (bare)")


# ---------------------------------------------------------------------------
# OIDC path
# ---------------------------------------------------------------------------


async def _resolve_oidc(pool: asyncpg.Pool, verifier: JwtVerifier, bearer: str) -> Principal:
    try:
        identity = verifier.verify(bearer)
    except InvalidJwt:
        raise HTTPException(status_code=401, detail="invalid jwt")

    # Look up existing identity link.
    existing = await pool.fetchrow(
        "SELECT ui.principal_idx, p.disabled, p.retired"
        " FROM qiita.user_identities ui"
        " JOIN qiita.principal p ON p.idx = ui.principal_idx"
        " WHERE ui.issuer = $1 AND ui.subject = $2",
        identity.issuer,
        identity.subject,
    )
    if existing is not None:
        if existing["disabled"] or existing["retired"]:
            raise HTTPException(status_code=401, detail="principal disabled or retired")
        await _handle_email_drift(pool, existing["principal_idx"], identity.email)
        return await _build_human_user(pool, existing["principal_idx"], scopes=None)

    return await _create_human_from_oidc(pool, identity)


async def _create_human_from_oidc(pool: asyncpg.Pool, identity) -> Principal:
    """First-login path: create principal + user + user_identities atomically.

    Two known race outcomes:
      - Email collision with another user (different (iss, sub), same email):
        409, audit event with sha256 of attempted email.
      - Concurrent first-login for same (iss, sub): one INSERT wins on the
        user_identities PK, the loser catches the unique violation, re-reads,
        returns the winner's principal_idx.
    """
    async with pool.acquire() as conn:
        try:
            async with conn.transaction():
                principal_idx = await conn.fetchval(
                    "INSERT INTO qiita.principal"
                    "  (display_name, system_role, created_by_idx)"
                    " VALUES ($1, 'user', 1) RETURNING idx",
                    identity.email,
                )
                await conn.execute(
                    "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
                    principal_idx,
                    identity.email,
                )
                await conn.execute(
                    "INSERT INTO qiita.user_identities"
                    "  (principal_idx, issuer, subject)"
                    " VALUES ($1, $2, $3)",
                    principal_idx,
                    identity.issuer,
                    identity.subject,
                )
                await record_event(
                    conn,
                    event_type="oidc_create_principal",
                    principal_idx=principal_idx,
                    detail={"issuer": identity.issuer},
                )
        except asyncpg.UniqueViolationError:
            # Two scenarios produce a UniqueViolationError on this transaction:
            #   a) Concurrent first-login race for the same (iss, sub) — the
            #      first transaction to commit owns the (iss, sub); the loser
            #      may surface either user_identities_pkey OR user_email_key
            #      depending on which constraint trips first under timing.
            #   b) A true email collision: a different (iss, sub) tries to
            #      land an already-claimed email.
            # Distinguishing on constraint name is fragile under (a) because
            # the same race can trip either constraint. Disambiguate by
            # re-reading user_identities for our (iss, sub) instead: if it
            # exists, the winner created our identity and we return them;
            # if not, this is a real email collision.
            row = await pool.fetchrow(
                "SELECT principal_idx FROM qiita.user_identities"
                " WHERE issuer = $1 AND subject = $2",
                identity.issuer,
                identity.subject,
            )
            if row is not None:
                return await _build_human_user(pool, row["principal_idx"], scopes=None)
            await record_event(
                pool,
                event_type="oidc_create_principal_email_conflict",
                detail={
                    "issuer": identity.issuer,
                    "attempted_email_sha256": sha256_hex(identity.email),
                },
            )
            raise HTTPException(
                status_code=409,
                detail="email already linked to a different identity",
            )

    return await _build_human_user(pool, principal_idx, scopes=None)


async def _handle_email_drift(pool: asyncpg.Pool, principal_idx: int, jwt_email: str) -> None:
    """On repeat OIDC login, reconcile a possibly-changed email.

    On mismatch + no collision: UPDATE succeeds; emit email_drift audit
    event with outcome=updated.
    On mismatch + collision with another user: no-op; emit email_drift
    audit event with outcome=collision (sha256 of attempted email,
    cleartext NOT logged).
    """
    current = await pool.fetchval(
        "SELECT email FROM qiita.user WHERE principal_idx = $1", principal_idx
    )
    if current is None or str(current).lower() == jwt_email.lower():
        return  # no drift (CITEXT compares case-insensitively at the DB; we mirror)
    try:
        await pool.execute(
            "UPDATE qiita.user SET email = $1 WHERE principal_idx = $2",
            jwt_email,
            principal_idx,
        )
        await record_event(
            pool,
            event_type="email_drift",
            principal_idx=principal_idx,
            detail={"outcome": "updated", "from": current, "to": jwt_email},
        )
    except asyncpg.UniqueViolationError:
        # Another user has that email. Don't overwrite; log the attempt
        # under a hash so audit-log readers can't trivially harvest the
        # cleartext attempted email by reading their own audit.
        await record_event(
            pool,
            event_type="email_drift",
            principal_idx=principal_idx,
            detail={
                "outcome": "collision",
                "attempted_email_sha256": sha256_hex(jwt_email),
            },
        )


async def _build_human_user(
    pool: asyncpg.Pool, principal_idx: int, *, scopes: frozenset[str] | None
) -> HumanUser:
    """Load a HumanUser from the DB. `scopes` is None for OIDC-resolved users
    (they get the full role-implied ceiling at this layer; Phase F's PAT
    mint validates per-token scopes), or a token's scope set when arriving
    via the token path.
    """
    row = await pool.fetchrow(
        "SELECT p.idx, p.system_role, p.disabled, p.retired,"
        "  u.email, u.profile_complete"
        " FROM qiita.principal p"
        " JOIN qiita.user u ON u.principal_idx = p.idx"
        " WHERE p.idx = $1",
        principal_idx,
    )
    if row is None:
        raise HTTPException(status_code=401, detail="user record not found for principal")
    if row["disabled"] or row["retired"]:
        raise HTTPException(status_code=401, detail="principal disabled or retired")
    # For OIDC-derived sessions we hand back the role's full ceiling. Phase F
    # narrows this when minting PATs; per-request bearers carry their own
    # scope set via the token path.
    if scopes is None:
        from .scopes import role_ceiling

        scopes = role_ceiling(row["system_role"])
    return HumanUser(
        principal_idx=row["idx"],
        email=row["email"],
        system_role=row["system_role"],
        scopes=scopes,
        profile_complete=row["profile_complete"],
        disabled=row["disabled"],
        retired=row["retired"],
    )
