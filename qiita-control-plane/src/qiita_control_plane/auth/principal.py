"""Principal types and the request-time resolver.

A Principal is the typed view of an authenticated request. Three runtime
kinds:
  - HumanUser   — has rows in qiita.user (subtype of principal)
  - ServiceAccount — has rows in qiita.service_account (subtype of principal)
  - Anonymous   — no Authorization header

The resolver dispatches by Authorization-header shape:
  - "Bearer qk_..."         → token path (opaque-token verifier)
  - "Bearer eyJ...x.y.z"    → OIDC path (JWT verifier + first-login UPSERT)
  - "Bearer <other>"        → 401 malformed
  - missing / non-Bearer    → Anonymous

For OIDC, first-login creates principal + user + user_identity atomically.
Email-collision-with-different-(iss,sub) returns 409 + audit event. Concurrent
first-logins for the same (iss, sub) race on the user_identity PK; the
loser catches the unique violation, re-reads, and returns the winner's
principal_idx.

Disabled / retired principals are rejected at the resolver layer for both
paths — verify_api_token already filters by these flags, and the OIDC
upsert refuses to construct a Principal for a (disabled OR retired) row.
"""

from dataclasses import dataclass

import asyncpg
from fastapi import HTTPException, Request
from qiita_common.auth_constants import (
    BEARER_PREFIX,
    MSG_PRINCIPAL_DISABLED_OR_RETIRED,
    MSG_PRINCIPAL_NOT_FOUND,
    SYSTEM_PRINCIPAL_IDX,
    AuthEventType,
    SystemRole,
)

from ..deps import get_db_pool
from . import TOKEN_PREFIX
from .audit import record_event, sha256_hex
from .db import insert_principal
from .oidc import InvalidJwt, JwtVerifier
from .scopes import role_ceiling
from .token import verify_api_token

_ROLE_ORDER = {SystemRole.USER: 0, SystemRole.WET_LAB_ADMIN: 1, SystemRole.SYSTEM_ADMIN: 2}


def _human_user_from_row(row, *, scopes: frozenset[str]) -> HumanUser:
    """Construct a HumanUser from a row exposing
    `idx, email, system_role, profile_complete, disabled, retired`.

    Both call sites (`_resolve_token` and `_build_human_user`) select these
    columns; only the surrounding query and scopes-source differ.
    """
    return HumanUser(
        principal_idx=row["idx"],
        email=row["email"],
        system_role=row["system_role"],
        scopes=scopes,
        profile_complete=row["profile_complete"],
        disabled=row["disabled"],
        retired=row["retired"],
    )


def _service_account_from_row(row, *, scopes: frozenset[str]) -> ServiceAccount:
    """Construct a ServiceAccount from a row exposing
    `idx, service_name, disabled, retired`.

    `service_name` is the LEFT JOIN alias used by `_resolve_token`'s SELECT;
    future loaders reading from `qiita.service_account` directly should
    alias their `name` column the same way.
    """
    return ServiceAccount(
        principal_idx=row["idx"],
        name=row["service_name"],
        scopes=scopes,
        disabled=row["disabled"],
        retired=row["retired"],
    )


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


def _looks_like_jwt(s: str) -> bool:
    """Cheap structural check: a JWT has exactly two dots separating
    header.payload.signature segments. We delegate the actual signature
    verification to PyJWT — this is just dispatch."""
    return s.count(".") == 2


async def get_current_principal(request: Request) -> Principal:
    """FastAPI dependency: resolve the current principal from the request.

    Returns:
      - Anonymous() when no Authorization header is present at all.
      - HumanUser / ServiceAccount on successful auth.

    Raises HTTPException 401 on any malformed authentication attempt:
      - non-Bearer scheme (e.g., `Authorization: Basic xxx`),
      - `Authorization: Bearer ` with empty credential,
      - bearer payload that is neither a `qk_` token nor JWT-shaped,
      - invalid token / JWT, or disabled/retired principal.

    Raises 503 if a JWT-shaped bearer arrives but the OIDC verifier is
    not configured.

    Policy: an `Authorization` header is read as an authentication
    *attempt*. Treating malformed attempts as Anonymous would silently
    hide client misconfiguration on public routes; a 401 surfaces the
    failure so the client can fix the request.
    """
    auth = request.headers.get("Authorization", "")
    if not auth:
        return Anonymous()
    if not auth.startswith(BEARER_PREFIX):
        raise HTTPException(
            status_code=401,
            detail="unsupported authentication scheme; only Bearer is accepted",
        )
    bearer = auth[len(BEARER_PREFIX) :].strip()
    if not bearer:
        raise HTTPException(status_code=401, detail="empty bearer credential")

    pool = get_db_pool(request)

    if bearer.startswith(TOKEN_PREFIX):
        return await _resolve_token(pool, bearer)
    if _looks_like_jwt(bearer):
        verifier = get_oidc_verifier(request)
        return await resolve_oidc(pool, verifier, bearer)

    raise HTTPException(status_code=401, detail="malformed bearer token")


def get_oidc_verifier(request: Request) -> JwtVerifier:
    """Return the configured JWKS-backed JWT verifier from app state.

    503s rather than 500s because a missing verifier reflects a deployment
    config gap (AUTHROCKET_* env vars), not a bug — callers downstream are
    correct, the service is not ready to authenticate JWT bearers.
    """
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
    # info to populate the typed Principal. The CASE expression makes the
    # kind dispatch explicit rather than inferring it from "which LEFT-JOIN
    # column is non-null"; matches the convention used in routes/admin.py.
    row = await pool.fetchrow(
        "SELECT p.idx, p.system_role, p.disabled, p.retired,"
        "  u.email, u.profile_complete,"
        "  sa.name AS service_name,"
        "  CASE"
        "    WHEN u.principal_idx IS NOT NULL THEN 'user'"
        "    WHEN sa.principal_idx IS NOT NULL THEN 'service'"
        "    ELSE 'bare'"
        "  END AS kind"
        " FROM qiita.principal p"
        " LEFT JOIN qiita.user u ON u.principal_idx = p.idx"
        " LEFT JOIN qiita.service_account sa ON sa.principal_idx = p.idx"
        " WHERE p.idx = $1",
        verified.principal_idx,
    )
    if row is None:
        # Token references missing principal — schema FK would normally
        # prevent this, but fail closed.
        raise HTTPException(status_code=401, detail=MSG_PRINCIPAL_NOT_FOUND)

    kind = row["kind"]
    if kind == "user":
        return _human_user_from_row(row, scopes=verified.scopes)
    if kind == "service":
        return _service_account_from_row(row, scopes=verified.scopes)
    # `kind == "bare"` — a bare principal holding a token shouldn't happen
    # (sentinel CHECK + subtype creation is the only path that produces a
    # token-bearing principal in practice). Fail closed.
    raise HTTPException(status_code=401, detail="principal has no auth subtype (bare)")


# ---------------------------------------------------------------------------
# OIDC path
# ---------------------------------------------------------------------------


async def resolve_oidc(pool: asyncpg.Pool, verifier: JwtVerifier, bearer: str) -> Principal:
    try:
        identity = verifier.verify(bearer)
    except InvalidJwt:
        raise HTTPException(status_code=401, detail="invalid jwt")

    # Look up existing identity link.
    existing = await pool.fetchrow(
        "SELECT ui.principal_idx, p.disabled, p.retired"
        " FROM qiita.user_identity ui"
        " JOIN qiita.principal p ON p.idx = ui.principal_idx"
        " WHERE ui.issuer = $1 AND ui.subject = $2",
        identity.issuer,
        identity.subject,
    )
    if existing is not None:
        if existing["disabled"] or existing["retired"]:
            raise HTTPException(status_code=401, detail=MSG_PRINCIPAL_DISABLED_OR_RETIRED)
        await _handle_email_drift(pool, existing["principal_idx"], identity.email)
        return await _build_human_user(pool, existing["principal_idx"])

    return await _create_human_from_oidc(pool, identity)


async def _create_human_from_oidc(pool: asyncpg.Pool, identity) -> Principal:
    """First-login path: create principal + user + user_identity atomically.

    Two known race outcomes:
      - Email collision with another user (different (iss, sub), same email):
        409, audit event with sha256 of attempted email.
      - Concurrent first-login for same (iss, sub): one INSERT wins on the
        user_identity PK, the loser catches the unique violation, re-reads,
        returns the winner's principal_idx.
    """
    async with pool.acquire() as conn:
        try:
            async with conn.transaction():
                principal_idx = await insert_principal(
                    conn,
                    display_name=identity.email,
                    created_by_idx=SYSTEM_PRINCIPAL_IDX,
                )
                await conn.execute(
                    "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
                    principal_idx,
                    identity.email,
                )
                await conn.execute(
                    "INSERT INTO qiita.user_identity"
                    "  (principal_idx, issuer, subject)"
                    " VALUES ($1, $2, $3)",
                    principal_idx,
                    identity.issuer,
                    identity.subject,
                )
                await record_event(
                    conn,
                    event_type=AuthEventType.OIDC_CREATE_PRINCIPAL,
                    principal_idx=principal_idx,
                    detail={"issuer": identity.issuer},
                )
        except asyncpg.UniqueViolationError:
            # Two scenarios produce a UniqueViolationError on this transaction:
            #   a) Concurrent first-login race for the same (iss, sub) — the
            #      first transaction to commit owns the (iss, sub); the loser
            #      may surface either user_identity_pkey OR user_email_key
            #      depending on which constraint trips first under timing.
            #   b) A true email collision: a different (iss, sub) tries to
            #      land an already-claimed email.
            # Distinguishing on constraint name is fragile under (a) because
            # the same race can trip either constraint. Disambiguate by
            # re-reading user_identity for our (iss, sub) instead: if it
            # exists, the winner created our identity and we return them;
            # if not, this is a real email collision.
            row = await pool.fetchrow(
                "SELECT principal_idx FROM qiita.user_identity WHERE issuer = $1 AND subject = $2",
                identity.issuer,
                identity.subject,
            )
            if row is not None:
                return await _build_human_user(pool, row["principal_idx"])
            await record_event(
                pool,
                event_type=AuthEventType.OIDC_CREATE_PRINCIPAL_EMAIL_CONFLICT,
                detail={
                    "issuer": identity.issuer,
                    "attempted_email_sha256": sha256_hex(identity.email),
                },
            )
            raise HTTPException(
                status_code=409,
                detail="email already linked to a different identity",
            )

    return await _build_human_user(pool, principal_idx)


async def _handle_email_drift(pool: asyncpg.Pool, principal_idx: int, jwt_email: str) -> None:
    """On repeat OIDC login, reconcile a possibly-changed email.

    "Email drift" = the email claim on the incoming JWT differs from the
    email stored on qiita.user when the principal first logged in. Users
    can change their email at the IdP at any time, and we want our local
    copy to track theirs — unless the new value collides with another
    user, in which case we don't overwrite and we record the attempt.

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
            event_type=AuthEventType.EMAIL_DRIFT,
            principal_idx=principal_idx,
            detail={"outcome": "updated", "from": current, "to": jwt_email},
        )
    except asyncpg.UniqueViolationError:
        # Another user has that email. Don't overwrite; log the attempt
        # under a hash so audit-log readers can't trivially harvest the
        # cleartext attempted email by reading their own audit.
        await record_event(
            pool,
            event_type=AuthEventType.EMAIL_DRIFT,
            principal_idx=principal_idx,
            detail={
                "outcome": "collision",
                "attempted_email_sha256": sha256_hex(jwt_email),
            },
        )


async def _build_human_user(pool: asyncpg.Pool, principal_idx: int) -> HumanUser:
    """Load a HumanUser from the DB for an OIDC-resolved session.

    Hands back the role's full implied scope ceiling — the PAT mint route
    narrows this when minting PATs, and per-request token bearers carry
    their own scope set via the token path (`_resolve_token`), so this
    function is only used for OIDC-arrived users.
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
        raise HTTPException(status_code=401, detail=MSG_PRINCIPAL_DISABLED_OR_RETIRED)
    return _human_user_from_row(row, scopes=role_ceiling(row["system_role"]))
