"""Reference management routes."""

from typing import Annotated
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from pydantic import Field
from qiita_common.models import ReferenceCreateRequest, ReferenceResponse

from ..deps import get_current_user, get_db_pool

router = APIRouter(prefix="/references", tags=["references"])


@router.post("", status_code=201)
async def create_reference(
    body: ReferenceCreateRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    user_id: UUID = Depends(get_current_user),
) -> ReferenceResponse:
    try:
        row = await pool.fetchrow(
            "INSERT INTO qiita.references (name, version, kind, created_by)"
            " VALUES ($1, $2, $3, $4)"
            " RETURNING reference_idx, name, version, kind, status, created_by, created_at",
            body.name,
            body.version,
            body.kind,
            user_id,
        )
    except asyncpg.UniqueViolationError:
        raise HTTPException(
            status_code=409,
            detail=f"Reference ({body.name!r}, {body.version!r}) already exists",
        )
    except asyncpg.PostgresError as exc:
        raise HTTPException(status_code=500, detail="Database error") from exc
    return ReferenceResponse(**dict(row))


@router.get("/{reference_idx}")
async def get_reference(
    reference_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> ReferenceResponse:
    row = await pool.fetchrow(
        "SELECT reference_idx, name, version, kind, status, created_by, created_at"
        " FROM qiita.references WHERE reference_idx = $1",
        reference_idx,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Reference not found")
    return ReferenceResponse(**dict(row))
