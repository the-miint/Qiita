"""User management routes — operates on the qiita.user subtype.

Phase B uses the mock get_current_principal_idx; the real auth flip happens
in Phase H.b. POST /users is the admin-creates-a-user path; in production
the OIDC resolver creates users on first login and POST /users is rarely
exercised — but it remains the way an admin onboards someone before they've
logged in (e.g., a PI imported from another system).
"""

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from qiita_common.models import UserCreate, UserResponse, UserUpdate

from ..deps import get_current_principal_idx, get_db_pool

router = APIRouter(prefix="/users", tags=["users"])


_USER_RETURNING_COLS = (
    "email, affiliation, address, phone, orcid,"
    " receive_processing_emails, profile_complete, created_at, updated_at"
)


@router.post("", status_code=201)
async def create_user(
    body: UserCreate,
    pool: asyncpg.Pool = Depends(get_db_pool),
    actor_principal_idx: int = Depends(get_current_principal_idx),
) -> UserResponse:
    """Create a new principal + user row in one transaction.

    The new principal's `created_by_idx` points at the requesting principal
    (the admin who initiated the action). Conflicts on email return 409.
    """
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                principal_idx = await conn.fetchval(
                    "INSERT INTO qiita.principal"
                    "  (display_name, system_role, created_by_idx)"
                    " VALUES ($1, 'user', $2) RETURNING idx",
                    body.display_name,
                    actor_principal_idx,
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
    principal_idx: int = Depends(get_current_principal_idx),
) -> UserResponse:
    """Return the authenticated principal's user profile."""
    row = await pool.fetchrow(
        "SELECT p.idx AS principal_idx, p.display_name,"
        " u.email, u.affiliation, u.address, u.phone, u.orcid,"
        " u.receive_processing_emails, u.profile_complete,"
        " u.created_at, u.updated_at"
        " FROM qiita.principal p"
        " JOIN qiita.user u ON u.principal_idx = p.idx"
        " WHERE p.idx = $1",
        principal_idx,
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
    principal_idx: int = Depends(get_current_principal_idx),
) -> UserResponse:
    """Update the authenticated principal's profile.

    Only profile fields are mutable. `email` and status fields are
    intentionally excluded from `UserUpdate`; Pydantic drops unknown fields,
    so the SQL UPDATE never builds a SET clause for them.
    """
    updates = body.model_dump(exclude_unset=True)
    if updates:
        cols = list(updates.keys())
        set_clause = ", ".join(f"{c} = ${i + 2}" for i, c in enumerate(cols))
        values = [updates[c] for c in cols]
        result = await pool.execute(
            f"UPDATE qiita.user SET {set_clause} WHERE principal_idx = $1",
            principal_idx,
            *values,
        )
        # asyncpg returns "UPDATE 0" / "UPDATE 1" — verify the row existed.
        if result.endswith("0"):
            raise HTTPException(
                status_code=404,
                detail="Authenticated principal has no user profile",
            )
    # Always return the current state via the same SELECT path as GET /users/me.
    return await get_me(pool=pool, principal_idx=principal_idx)
