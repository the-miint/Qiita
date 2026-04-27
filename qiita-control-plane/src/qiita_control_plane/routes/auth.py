"""Auth endpoints — /auth/whoami, /auth/pat, /auth/tokens.

Routes use the real `get_current_principal` resolver from Phase E; the
mock `get_current_user` is never wired in here. POST /auth/pat is the
single route that requires a *fresh* OIDC JWT (auth_time within
AUTHROCKET_PAT_MAX_AUTH_AGE_SECONDS) and bypasses the resolver because
freshness is a per-route concern.

POST /auth/login (the OIDC code-exchange callback for the CLI flow) is
deferred to Phase G when the qiita-admin CLI drives the requirements.
"""

import time
from datetime import UTC, datetime, timedelta

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from qiita_common.models import (
    ApiTokenMintRequest,
    ApiTokenMintResponse,
    ApiTokenSummary,
)

from ..auth.audit import record_event
from ..auth.oidc import InvalidJwt, JwtVerifier
from ..auth.principal import (
    Anonymous,
    HumanUser,
    Principal,
    ServiceAccount,
    get_current_principal,
)
from ..auth.scopes import (
    VALID_SCOPES,
    reject_scopes_outside_ceiling,
    role_ceiling,
)
from ..auth.tokens import mint_api_token
from ..config import Settings
from ..deps import get_db_pool

router = APIRouter(prefix="/auth", tags=["auth"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_settings(request: Request) -> Settings:
    settings = getattr(request.app.state, "settings", None)
    if settings is None:
        raise RuntimeError("Settings not initialised — lifespan may not have run")
    return settings


def _get_oidc_verifier(request: Request) -> JwtVerifier:
    verifier = getattr(request.app.state, "oidc_verifier", None)
    if verifier is None:
        raise HTTPException(
            status_code=503,
            detail="OIDC verifier is not configured (set AUTHROCKET_* env vars)",
        )
    return verifier


# ---------------------------------------------------------------------------
# GET /auth/whoami
# ---------------------------------------------------------------------------


@router.get("/whoami")
async def whoami(p: Principal = Depends(get_current_principal)):
    """Return a serializable view of the authenticated principal.

    Public route: anonymous callers get `{"kind": "anonymous"}`. Allows clients
    to probe their own auth state without tripping a 401.
    """
    if isinstance(p, HumanUser):
        return {
            "kind": "human",
            "principal_idx": p.principal_idx,
            "email": p.email,
            "system_role": p.system_role,
            "scopes": sorted(p.scopes),
            "profile_complete": p.profile_complete,
        }
    if isinstance(p, ServiceAccount):
        return {
            "kind": "service",
            "principal_idx": p.principal_idx,
            "name": p.name,
            "scopes": sorted(p.scopes),
        }
    if isinstance(p, Anonymous):
        return {"kind": "anonymous"}
    # Defensive: shouldn't happen.
    raise HTTPException(status_code=500, detail="unknown principal kind")


# ---------------------------------------------------------------------------
# POST /auth/pat
# ---------------------------------------------------------------------------


@router.post("/pat", status_code=201, response_model=None)
async def mint_pat(
    body: ApiTokenMintRequest,
    request: Request,
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> ApiTokenMintResponse | JSONResponse:
    """Mint a personal access token. Requires a *fresh* OIDC JWT — the
    auth_time claim must be within AUTHROCKET_PAT_MAX_AUTH_AGE_SECONDS,
    forcing a real interactive login before minting even when a long-lived
    JWT has been hoarded. PAT-mint via PAT (qk_) is rejected outright.

    On profile-incomplete, returns 422 with a flat body listing the
    missing fields — the CLI surfaces this to the user as an actionable
    error instead of silently failing.
    """
    settings = _get_settings(request)
    verifier = _get_oidc_verifier(request)

    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="OIDC JWT required")
    bearer = auth[len("Bearer "):].strip()
    if bearer.startswith("qk_"):
        raise HTTPException(
            status_code=401,
            detail="PAT mint requires an OIDC JWT, not an existing API token",
        )

    try:
        identity = verifier.verify(bearer)
    except InvalidJwt as exc:
        raise HTTPException(status_code=401, detail=f"invalid jwt: {exc}") from exc

    # Freshness check: auth_time must be present AND recent.
    if identity.auth_time is None:
        raise HTTPException(
            status_code=401,
            detail="PAT mint requires JWT with an auth_time claim",
        )
    age = int(time.time()) - identity.auth_time
    if age > settings.authrocket_pat_max_auth_age_seconds:
        raise HTTPException(
            status_code=401,
            detail=(
                f"auth_time is {age}s old; PAT mint requires a fresh login"
                f" (max {settings.authrocket_pat_max_auth_age_seconds}s)"
            ),
        )
    # An IdP with clock skew can issue auth_time slightly in the future.
    # Without a symmetric guard, a future auth_time produces a negative age
    # that trivially passes the > threshold check and bypasses freshness.
    # Allow up to one leeway window of forward skew, reject anything beyond.
    if age < -settings.authrocket_jwt_leeway_seconds:
        raise HTTPException(
            status_code=401,
            detail=(
                f"auth_time is {-age}s in the future; refusing PAT mint"
                f" (max forward skew {settings.authrocket_jwt_leeway_seconds}s)"
            ),
        )

    # Resolve the user from the verified identity.
    user_row = await pool.fetchrow(
        "SELECT u.principal_idx, p.system_role, p.disabled, p.retired,"
        " u.profile_complete, u.affiliation, u.address, u.phone"
        " FROM qiita.user_identities ui"
        " JOIN qiita.user u ON u.principal_idx = ui.principal_idx"
        " JOIN qiita.principal p ON p.idx = u.principal_idx"
        " WHERE ui.issuer = $1 AND ui.subject = $2",
        identity.issuer,
        identity.subject,
    )
    if user_row is None:
        raise HTTPException(
            status_code=401,
            detail="no user matches this OIDC identity",
        )
    if user_row["disabled"] or user_row["retired"]:
        raise HTTPException(
            status_code=401, detail="principal disabled or retired"
        )

    if not user_row["profile_complete"]:
        # Flat 422 body so the CLI can pluck reason / missing_fields without
        # nested-detail unwrapping.
        missing = [
            f
            for f in ("affiliation", "address", "phone")
            if not user_row[f]
        ]
        return JSONResponse(
            status_code=422,
            content={
                "detail": "profile incomplete",
                "reason": "profile_incomplete",
                "missing_fields": missing,
            },
        )

    # Scope validation against the role ceiling.
    role = user_row["system_role"]
    ceiling = role_ceiling(role)
    if body.scopes is None:
        scopes = sorted(ceiling)
    else:
        unknown = [s for s in body.scopes if s not in VALID_SCOPES]
        if unknown:
            return JSONResponse(
                status_code=422,
                content={
                    "detail": "unknown scopes",
                    "rejected_scopes": sorted(unknown),
                },
            )
        rejected = reject_scopes_outside_ceiling(body.scopes, ceiling)
        if rejected:
            # Per the plan's design note, do NOT echo the caller's full
            # ceiling — the caller already knows it via /auth/whoami; echoing
            # it per-request would leak ceiling structure to a probing
            # attacker.
            return JSONResponse(
                status_code=422,
                content={
                    "detail": "scopes not granted by your role",
                    "rejected_scopes": rejected,
                },
            )
        scopes = list(body.scopes)

    # TTL — Pydantic already enforces gt=0 and le=365.
    ttl_days = (
        body.ttl_days
        if body.ttl_days is not None
        else settings.token_default_ttl_days
    )
    expires_at = datetime.now(UTC) + timedelta(days=ttl_days)

    plaintext, token_idx = await mint_api_token(
        pool,
        principal_idx=user_row["principal_idx"],
        label=body.label,
        scopes=scopes,
        expires_at=expires_at,
    )

    await record_event(
        pool,
        event_type="token_mint",
        principal_idx=user_row["principal_idx"],
        actor_principal_idx=user_row["principal_idx"],  # self-mint
        detail={"token_idx": token_idx, "scopes": scopes, "kind": "pat"},
    )

    return ApiTokenMintResponse(
        token=plaintext,
        token_idx=token_idx,
        label=body.label,
        scopes=scopes,
        expires_at=expires_at,
        created_at=datetime.now(UTC),
    )


# ---------------------------------------------------------------------------
# GET /auth/tokens — list own tokens (metadata only)
# ---------------------------------------------------------------------------


@router.get("/tokens")
async def list_own_tokens(
    p: Principal = Depends(get_current_principal),
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> list[ApiTokenSummary]:
    """List the caller's own tokens. Metadata only — no plaintext, no hash.
    Anonymous: 401. Both HumanUser and ServiceAccount can list their own."""
    if isinstance(p, Anonymous):
        raise HTTPException(status_code=401, detail="authentication required")
    if not p.has_scope("self:tokens"):
        raise HTTPException(
            status_code=403, detail="missing required scope 'self:tokens'"
        )

    rows = await pool.fetch(
        "SELECT token_idx, label, scopes, expires_at, revoked_at,"
        "  last_used_at, created_at"
        " FROM qiita.api_tokens WHERE principal_idx = $1"
        " ORDER BY token_idx",
        p.principal_idx,
    )
    return [ApiTokenSummary.model_validate(dict(r)) for r in rows]


# ---------------------------------------------------------------------------
# DELETE /auth/tokens/{token_idx} — revoke own token
# ---------------------------------------------------------------------------


@router.delete("/tokens/{token_idx}", status_code=204)
async def revoke_own_token(
    token_idx: int,
    p: Principal = Depends(get_current_principal),
    pool: asyncpg.Pool = Depends(get_db_pool),
):
    """Revoke a token belonging to the caller. 401 if Anonymous, 403 if the
    caller's bearer lacks `self:tokens`, 404 for **either** "no such token"
    or "exists but owned by someone else" — same response so probing
    token_idx values does not enumerate the table.
    """
    if isinstance(p, Anonymous):
        raise HTTPException(status_code=401, detail="authentication required")
    if not p.has_scope("self:tokens"):
        raise HTTPException(
            status_code=403, detail="missing required scope 'self:tokens'"
        )

    # Atomic UPDATE WHERE — only revokes if owner matches and token is not
    # already revoked. Returns 0 rows if either condition fails.
    result = await pool.execute(
        "UPDATE qiita.api_tokens SET revoked_at = now()"
        " WHERE token_idx = $1 AND principal_idx = $2 AND revoked_at IS NULL",
        token_idx,
        p.principal_idx,
    )
    if result.endswith("0"):
        # Either the token doesn't exist, is owned by another principal, or
        # is owned by us but already revoked. To avoid leaking existence to
        # an attacker walking token_idx values, conflate the first two as
        # 404. The "already revoked" case is idempotent success, so we let
        # it return 204 silently.
        owner_idx = await pool.fetchval(
            "SELECT principal_idx FROM qiita.api_tokens WHERE token_idx = $1",
            token_idx,
        )
        if owner_idx is None or owner_idx != p.principal_idx:
            raise HTTPException(status_code=404, detail="token not found")
        # Owned by us but already revoked — idempotent success, no audit.
        return

    await record_event(
        pool,
        event_type="token_revoke",
        principal_idx=p.principal_idx,
        actor_principal_idx=p.principal_idx,
        detail={"token_idx": token_idx, "reason": "self_revoke"},
    )
