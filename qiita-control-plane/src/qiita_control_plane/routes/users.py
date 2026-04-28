"""User management routes — operates on the qiita.user subtype.

Phase H.b: every route uses the real Phase E guards.

- POST /users        — admin creates a user (system_admin + admin:users).
                       In production the OIDC resolver creates users on
                       first login; this route remains for admins to
                       onboard PIs imported from external systems.
- GET /users/me      — humans only, no scope gate (you can always read
                       your own profile).
- PATCH /users/me    — humans only + self:profile scope. `email` and
                       status fields are absent from UserUpdate so
                       attempts to set them are silently dropped.
"""

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from qiita_common.auth_constants import Scope, SystemRole
from qiita_common.models import UserCreate, UserResponse, UserUpdate

from ..auth.guards import (
    require_human,
    require_human_with_role,
    require_scope,
)
from ..auth.principal import HumanUser, Principal
from ..deps import get_db_pool

router = APIRouter(prefix="/users", tags=["users"])


_USER_RETURNING_COLS = (
    "email, affiliation, address, phone, orcid,"
    " receive_processing_emails, profile_complete, created_at, updated_at"
)


@router.post("", status_code=201)
async def create_user(
    body: UserCreate,
    pool: asyncpg.Pool = Depends(get_db_pool),
    actor: HumanUser = Depends(require_human_with_role(SystemRole.SYSTEM_ADMIN)),
    _scope: Principal = Depends(require_scope(Scope.ADMIN_USERS)),
) -> UserResponse:
    """Admin creates a new principal + user row in one transaction.

    The new principal's `created_by_idx` points at the requesting admin's
    principal_idx. 409 on email conflict (case-insensitive via CITEXT).
    """
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                principal_idx = await conn.fetchval(
                    "INSERT INTO qiita.principal"
                    "  (display_name, system_role, created_by_idx)"
                    " VALUES ($1, $2, $3) RETURNING idx",
                    body.display_name,
                    SystemRole.USER,
                    actor.principal_idx,
                )
                user_row = await conn.fetchrow(
                    "INSERT INTO qiita.user"
                    "  (principal_idx, email, affiliation, address, phone,"
                    "   orcid, receive_processing_emails)"
                    " VALUES ($1, $2, $3, $4, $5, $6, $7)"
                    f" RETURNING {_USER_RETURNING_COLS}",
                    principal_idx,
                    body.email,
                    body.affiliation,
                    body.address,
                    body.phone,
                    body.orcid,
                    body.receive_processing_emails,
                )
    except asyncpg.UniqueViolationError:
        raise HTTPException(
            status_code=409,
            detail=f"User with email {body.email!r} already exists",
        )
    return UserResponse.model_validate(
        {"principal_idx": principal_idx, "display_name": body.display_name, **dict(user_row)}
    )


@router.get("/me")
async def get_me(
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: HumanUser = Depends(require_human),
) -> UserResponse:
    """Return the authenticated user's profile."""
    row = await pool.fetchrow(
        "SELECT p.idx AS principal_idx, p.display_name,"
        " u.email, u.affiliation, u.address, u.phone, u.orcid,"
        " u.receive_processing_emails, u.profile_complete,"
        " u.created_at, u.updated_at"
        " FROM qiita.principal p"
        " JOIN qiita.user u ON u.principal_idx = p.idx"
        " WHERE p.idx = $1",
        user.principal_idx,
    )
    if row is None:
        raise HTTPException(
            status_code=404,
            detail="Authenticated principal has no user profile",
        )
    return UserResponse.model_validate(dict(row))


@router.patch("/me")
async def patch_me(
    body: UserUpdate,
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.SELF_PROFILE)),
) -> UserResponse:
    """Update the authenticated user's profile.

    Only profile fields are mutable. `email` and status fields are
    intentionally absent from `UserUpdate`; Pydantic drops unknown fields
    so the SQL UPDATE never builds a SET clause for them.
    """
    updates = body.model_dump(exclude_unset=True)
    if updates:
        cols = list(updates.keys())
        set_clause = ", ".join(f"{c} = ${i + 2}" for i, c in enumerate(cols))
        values = [updates[c] for c in cols]
        result = await pool.execute(
            f"UPDATE qiita.user SET {set_clause} WHERE principal_idx = $1",
            user.principal_idx,
            *values,
        )
        if result.endswith("0"):
            raise HTTPException(
                status_code=404,
                detail="Authenticated principal has no user profile",
            )
    return await get_me(pool=pool, user=user)
