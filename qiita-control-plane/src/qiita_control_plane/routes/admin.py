"""Admin endpoints — service accounts, principal status / role mutations,
audit log read, bulk token revocation. All routes require system_admin role
PLUS the appropriate admin:* scope, so a token-scoped-narrow system_admin
can't exfiltrate audit data without the right scope.
"""

import json
from datetime import UTC, datetime, timedelta

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from qiita_common.models import (
    AuthEventResponse,
    PrincipalDisabledUpdate,
    PrincipalRetiredUpdate,
    PrincipalSystemRoleUpdate,
    RevokeAllTokensResponse,
    ServiceAccountCreate,
    ServiceAccountCreateResponse,
)

from ..auth.audit import record_event
from ..auth.guards import require_human_with_role, require_scope
from ..auth.principal import HumanUser, Principal
from ..auth.scopes import (
    SERVICE_ACCOUNT_SCOPE_CEILING,
    VALID_SCOPES,
    reject_scopes_outside_ceiling,
)
from ..auth.tokens import mint_api_token
from ..deps import get_db_pool

router = APIRouter(prefix="/admin", tags=["admin"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _decode_detail(raw) -> dict:
    """asyncpg returns JSONB as a string by default; decode once for response."""
    return json.loads(raw) if isinstance(raw, str) else (raw or {})


# ---------------------------------------------------------------------------
# POST /admin/service-accounts
# ---------------------------------------------------------------------------


@router.post("/service-accounts", status_code=201, response_model=None)
async def create_service_account(
    body: ServiceAccountCreate,
    pool: asyncpg.Pool = Depends(get_db_pool),
    actor: HumanUser = Depends(require_human_with_role("system_admin")),
    _scope: Principal = Depends(require_scope("admin:service_accounts")),
) -> ServiceAccountCreateResponse | JSONResponse:
    """Create a service-account-kind principal and mint its initial token.

    Scopes are validated against `SERVICE_ACCOUNT_SCOPE_CEILING` — workers
    don't fit the human role hierarchy, so admins must spell out what the
    worker is allowed to do, bounded by the service ceiling. 409 on
    duplicate name; 422 on out-of-ceiling scopes (flat body shape, matches
    /auth/pat).
    """
    # Scope ceiling check
    unknown = [s for s in body.scopes if s not in VALID_SCOPES]
    if unknown:
        return JSONResponse(
            status_code=422,
            content={"detail": "unknown scopes", "rejected_scopes": sorted(unknown)},
        )
    rejected = reject_scopes_outside_ceiling(body.scopes, SERVICE_ACCOUNT_SCOPE_CEILING)
    if rejected:
        return JSONResponse(
            status_code=422,
            content={
                "detail": "scopes not granted to service accounts",
                "rejected_scopes": rejected,
            },
        )

    expires_at = (
        datetime.now(UTC) + timedelta(days=body.ttl_days) if body.ttl_days is not None else None
    )

    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                principal_idx = await conn.fetchval(
                    "INSERT INTO qiita.principal"
                    "  (display_name, system_role, created_by_idx)"
                    " VALUES ($1, 'user', $2) RETURNING idx",
                    body.name,
                    actor.principal_idx,
                )
                await conn.execute(
                    "INSERT INTO qiita.service_account"
                    "  (principal_idx, name, description)"
                    " VALUES ($1, $2, $3)",
                    principal_idx,
                    body.name,
                    body.description,
                )
                # mint inside the same transaction for atomicity
                plaintext, token_idx = await mint_api_token(
                    conn,
                    principal_idx=principal_idx,
                    label=body.label,
                    scopes=body.scopes,
                    expires_at=expires_at,
                )
                await record_event(
                    conn,
                    event_type="token_mint",
                    principal_idx=principal_idx,
                    actor_principal_idx=actor.principal_idx,
                    detail={
                        "token_idx": token_idx,
                        "scopes": body.scopes,
                        "kind": "service_account_initial",
                    },
                )
    except asyncpg.UniqueViolationError:
        # service_account.name UNIQUE
        raise HTTPException(
            status_code=409,
            detail=f"service account named {body.name!r} already exists",
        )

    return ServiceAccountCreateResponse(
        principal_idx=principal_idx,
        name=body.name,
        description=body.description,
        token=plaintext,
        token_idx=token_idx,
        scopes=body.scopes,
        expires_at=expires_at,
        created_at=datetime.now(UTC),
    )


# ---------------------------------------------------------------------------
# PATCH /admin/principals/{idx}/disabled
# ---------------------------------------------------------------------------


@router.patch("/principals/{principal_idx}/disabled", status_code=204)
async def set_principal_disabled(
    principal_idx: int,
    body: PrincipalDisabledUpdate,
    pool: asyncpg.Pool = Depends(get_db_pool),
    actor: HumanUser = Depends(require_human_with_role("system_admin")),
    _scope: Principal = Depends(require_scope("admin:users")),
):
    """Toggle disabled state. `disabled=true` sets the audit columns;
    `disabled=false` clears them (round-trip back to active). The DB CHECK
    `principal_disabled_consistent` ensures atomicity."""
    if principal_idx == 1:
        raise HTTPException(status_code=403, detail="cannot disable system principal")

    if body.disabled:
        if not body.reason:
            raise HTTPException(status_code=422, detail="reason is required when disabling")
        try:
            result = await pool.execute(
                "UPDATE qiita.principal SET"
                "  disabled = true, disabled_at = now(),"
                "  disabled_by_idx = $2, disable_reason = $3"
                " WHERE idx = $1 AND disabled = false AND retired = false",
                principal_idx,
                actor.principal_idx,
                body.reason,
            )
        except asyncpg.CheckViolationError as exc:
            # principal_not_both_disabled_and_retired (race with retire) or
            # principal_system_principal_always_active.
            raise HTTPException(status_code=409, detail=str(exc)) from exc
    else:
        result = await pool.execute(
            "UPDATE qiita.principal SET"
            "  disabled = false, disabled_at = NULL,"
            "  disabled_by_idx = NULL, disable_reason = NULL"
            " WHERE idx = $1 AND disabled = true",
            principal_idx,
        )

    if result.endswith("0"):
        # Either principal doesn't exist, or already in target state, or
        # retired (terminal). Distinguish via a follow-up read.
        row = await pool.fetchrow(
            "SELECT disabled, retired FROM qiita.principal WHERE idx = $1",
            principal_idx,
        )
        if row is None:
            raise HTTPException(status_code=404, detail="principal not found")
        if row["retired"]:
            raise HTTPException(status_code=409, detail="principal is retired (terminal)")
        # Already in target state → idempotent success.
        return

    await record_event(
        pool,
        event_type="principal_disabled" if body.disabled else "principal_enabled",
        principal_idx=principal_idx,
        actor_principal_idx=actor.principal_idx,
        detail={"reason": body.reason} if body.disabled else {},
    )


# ---------------------------------------------------------------------------
# PATCH /admin/principals/{idx}/retired
# ---------------------------------------------------------------------------


@router.patch("/principals/{principal_idx}/retired", status_code=204)
async def retire_principal(
    principal_idx: int,
    body: PrincipalRetiredUpdate,
    pool: asyncpg.Pool = Depends(get_db_pool),
    actor: HumanUser = Depends(require_human_with_role("system_admin")),
    _scope: Principal = Depends(require_scope("admin:users")),
):
    """Retirement is terminal. The DB trigger revokes all the principal's
    active tokens automatically. An admin cannot retire themselves (refuses
    to leave zero active system_admins is enforced by application logic
    here — the DB trigger doesn't know roles)."""
    if principal_idx == 1:
        raise HTTPException(status_code=403, detail="cannot retire system principal")

    if actor.principal_idx == principal_idx:
        raise HTTPException(status_code=403, detail="admin cannot retire themselves")

    try:
        result = await pool.execute(
            "UPDATE qiita.principal SET"
            "  retired = true, retired_at = now(),"
            "  retired_by_idx = $2, retire_reason = $3"
            " WHERE idx = $1 AND retired = false",
            principal_idx,
            actor.principal_idx,
            body.reason,
        )
    except asyncpg.CheckViolationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    if result.endswith("0"):
        row = await pool.fetchrow(
            "SELECT retired FROM qiita.principal WHERE idx = $1", principal_idx
        )
        if row is None:
            raise HTTPException(status_code=404, detail="principal not found")
        # Already retired → idempotent success.
        return

    await record_event(
        pool,
        event_type="principal_retired",
        principal_idx=principal_idx,
        actor_principal_idx=actor.principal_idx,
        detail={"reason": body.reason},
    )


# ---------------------------------------------------------------------------
# PATCH /admin/principals/{idx}/system-role
# ---------------------------------------------------------------------------


@router.patch("/principals/{principal_idx}/system-role", status_code=204)
async def set_principal_system_role(
    principal_idx: int,
    body: PrincipalSystemRoleUpdate,
    pool: asyncpg.Pool = Depends(get_db_pool),
    actor: HumanUser = Depends(require_human_with_role("system_admin")),
    _scope: Principal = Depends(require_scope("admin:users")),
):
    """Set the principal's system_role. The DB enum validates the value;
    Pydantic's Literal narrows it before we hit the DB."""
    if principal_idx == 1:
        raise HTTPException(status_code=403, detail="cannot modify system principal's role")

    old_role = await pool.fetchval(
        "SELECT system_role FROM qiita.principal WHERE idx = $1", principal_idx
    )
    if old_role is None:
        raise HTTPException(status_code=404, detail="principal not found")

    await pool.execute(
        "UPDATE qiita.principal SET system_role = $1 WHERE idx = $2",
        body.system_role,
        principal_idx,
    )

    await record_event(
        pool,
        event_type="system_role_change",
        principal_idx=principal_idx,
        actor_principal_idx=actor.principal_idx,
        detail={"from": old_role, "to": body.system_role, "reason": body.reason},
    )


# ---------------------------------------------------------------------------
# GET /admin/audit
# ---------------------------------------------------------------------------


@router.get("/audit")
async def get_audit_log(
    pool: asyncpg.Pool = Depends(get_db_pool),
    _role: HumanUser = Depends(require_human_with_role("system_admin")),
    _scope: Principal = Depends(require_scope("admin:audit_read")),
    principal_idx: int | None = Query(default=None),
    event_type: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
) -> list[AuthEventResponse]:
    """List audit events, newest first. Optional filters by principal_idx
    and event_type."""
    sql = (
        "SELECT event_idx, event_type, principal_idx, actor_principal_idx,"
        " detail, occurred_at FROM qiita.auth_events WHERE 1=1"
    )
    params: list = []
    if principal_idx is not None:
        params.append(principal_idx)
        sql += f" AND principal_idx = ${len(params)}"
    if event_type is not None:
        params.append(event_type)
        sql += f" AND event_type = ${len(params)}"
    sql += f" ORDER BY event_idx DESC LIMIT ${len(params) + 1}"
    params.append(limit)

    rows = await pool.fetch(sql, *params)
    return [
        AuthEventResponse(
            event_idx=r["event_idx"],
            event_type=r["event_type"],
            principal_idx=r["principal_idx"],
            actor_principal_idx=r["actor_principal_idx"],
            detail=_decode_detail(r["detail"]),
            occurred_at=r["occurred_at"],
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# POST /admin/principals/{idx}/revoke-all-tokens
# ---------------------------------------------------------------------------


@router.post("/principals/{principal_idx}/revoke-all-tokens")
async def revoke_all_principal_tokens(
    principal_idx: int,
    pool: asyncpg.Pool = Depends(get_db_pool),
    actor: HumanUser = Depends(require_human_with_role("system_admin")),
) -> RevokeAllTokensResponse:
    """Bulk-revoke every active token belonging to the target principal.

    Scope policy: targeting a user-kind principal requires `admin:users`;
    targeting a service-kind principal requires `admin:service_accounts`.
    The route resolves the target's kind first and demands the matching
    scope so an admin can be issued a narrower-than-full token (e.g. for
    a workflow that only manages workers) and have the route layer enforce
    the boundary.

    Idempotent on already-revoked tokens (counted separately). Emits one
    token_revoke audit event per newly-revoked token so the audit trail
    records the bulk action atomically per token.
    """
    kind = await pool.fetchval(
        "SELECT CASE"
        " WHEN EXISTS (SELECT 1 FROM qiita.user WHERE principal_idx = $1) THEN 'user'"
        " WHEN EXISTS (SELECT 1 FROM qiita.service_account WHERE principal_idx = $1)"
        "      THEN 'service'"
        " ELSE 'bare' END",
        principal_idx,
    )
    required_scope = "admin:users" if kind == "user" else "admin:service_accounts"
    if kind == "bare":
        # No subtype row, so no tokens either — but still surface 404 instead
        # of silently succeeding so the caller's intent is verified.
        principal_exists = await pool.fetchval(
            "SELECT 1 FROM qiita.principal WHERE idx = $1", principal_idx
        )
        if not principal_exists:
            raise HTTPException(status_code=404, detail="principal not found")
    if not actor.has_scope(required_scope):
        raise HTTPException(
            status_code=403,
            detail=f"missing required scope {required_scope!r} for {kind}-kind principal",
        )

    rows = await pool.fetch(
        "UPDATE qiita.api_tokens SET revoked_at = now()"
        " WHERE principal_idx = $1 AND revoked_at IS NULL"
        " RETURNING token_idx",
        principal_idx,
    )
    revoked_idxs = [r["token_idx"] for r in rows]

    already_revoked = await pool.fetchval(
        "SELECT count(*) FROM qiita.api_tokens"
        " WHERE principal_idx = $1 AND revoked_at IS NOT NULL"
        "   AND token_idx <> ALL($2::bigint[])",
        principal_idx,
        revoked_idxs,
    )

    for tidx in revoked_idxs:
        await record_event(
            pool,
            event_type="token_revoke",
            principal_idx=principal_idx,
            actor_principal_idx=actor.principal_idx,
            detail={"token_idx": tidx, "reason": "admin_bulk"},
        )

    return RevokeAllTokensResponse(
        revoked_token_idxs=revoked_idxs,
        already_revoked_count=already_revoked or 0,
    )
