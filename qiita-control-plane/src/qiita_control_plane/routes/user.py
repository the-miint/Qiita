"""User management routes — operates on the qiita.user subtype.

- POST /user        — admin creates a user (system_admin + admin:user).
                      In production the OIDC resolver creates users on
                      first login; this route remains for admins to
                      onboard PIs imported from external systems.
- GET /user/me      — humans only, no scope gate (you can always read
                      your own profile).
- PATCH /user/me    — humans only + self:profile scope. `email` and
                      status fields are absent from UserUpdate so
                      attempts to set them are silently dropped.
"""

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from qiita_common.api_paths import PATH_USER_ME, PATH_USER_PREFIX, PATH_USER_ROOT
from qiita_common.auth_constants import Scope, SystemRole
from qiita_common.models import UserCreate, UserResponse, UserUpdate

from ..auth.db import insert_principal, rows_affected
from ..auth.guards import (
    require_human,
    require_human_with_role,
    require_scope,
)
from ..auth.principal import HumanUser, Principal
from ..deps import TxConnFactory, get_db_pool, get_tx_conn_factory

router = APIRouter(prefix=PATH_USER_PREFIX, tags=["user"])


_USER_RETURNING_COLS = (
    "email, affiliation, address, phone, orcid,"
    " receive_processing_emails, profile_complete, created_at, updated_at"
)

_MSG_NO_USER_PROFILE = "Authenticated principal has no user profile"


@router.post(PATH_USER_ROOT, status_code=201)
async def create_user(
    body: UserCreate,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    actor: HumanUser = Depends(require_human_with_role(SystemRole.SYSTEM_ADMIN)),
    _scope: Principal = Depends(require_scope(Scope.ADMIN_USER)),
) -> UserResponse:
    """Admin creates a new principal + user row in one transaction.

    The new principal's `created_by_idx` points at the requesting admin's
    principal_idx. 409 on email conflict (case-insensitive via CITEXT).
    """
    async with tx() as conn:
        try:
            principal_idx = await insert_principal(
                conn,
                display_name=body.display_name,
                created_by_idx=actor.principal_idx,
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


@router.get(PATH_USER_ME)
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
            detail=_MSG_NO_USER_PROFILE,
        )
    return UserResponse.model_validate(dict(row))


@router.patch(PATH_USER_ME)
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
        if rows_affected(result) == 0:
            raise HTTPException(
                status_code=404,
                detail=_MSG_NO_USER_PROFILE,
            )
    return await get_me(pool=pool, user=user)
