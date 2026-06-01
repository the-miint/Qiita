"""Reference management routes.

POST /reference creates a reference and is the only mutation endpoint
on this router that humans drive. GET /reference/{id} stays anonymous-OK
(`get_current_principal` directly, no guard). PATCH /reference/{id}/status
moves the reference through its lifecycle and is driven by the workflow
runner. POST /reference/{id}/ticket/doget signs a Flight ticket so a
client can pull active reference rows from the data plane.

Feature minting, membership writes, and DuckLake registration used to
live here as per-primitive routes; they're now reached through the
generic POST /api/v1/library/{name} dispatch (routes/library.py) so
workflow runners and HTTP callers share one transport.
"""

import base64
import json
from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from pydantic import Field
from qiita_common.api_paths import (
    PATH_REFERENCE_BY_IDX,
    PATH_REFERENCE_DOGET,
    PATH_REFERENCE_INDEX,
    PATH_REFERENCE_PREFIX,
    PATH_REFERENCE_ROOT,
    PATH_REFERENCE_STATUS,
)
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    DoGetTicketRequest,
    DoGetTicketResponse,
    ReferenceCreateRequest,
    ReferenceIndex,
    ReferenceKind,
    ReferenceResponse,
    ReferenceStatus,
    ReferenceStatusUpdate,
)

from ..actions.reference import (
    REFERENCE_RETURNING,
    IllegalStatusTransition,
    ReferenceNotFound,
    transition_reference_status,
)
from ..auth.guards import (
    require_complete_profile,
    require_scope,
)
from ..auth.principal import (
    HumanUser,
    Principal,
    get_current_principal,
)
from ..auth.tickets import sign_ticket
from ..deps import get_db_pool, get_hmac_secret

router = APIRouter(prefix=PATH_REFERENCE_PREFIX, tags=["reference"])


# Single source of truth lives in actions/reference.py (REFERENCE_RETURNING);
# aliased here so the existing in-file references read unchanged.
_REFERENCE_RETURNING = REFERENCE_RETURNING

_MSG_REFERENCE_NOT_FOUND = "Reference not found"


@router.post(PATH_REFERENCE_ROOT, status_code=201)
async def create_reference(
    body: ReferenceCreateRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.REFERENCE_WRITE)),
) -> ReferenceResponse:
    """Create a reference. Humans only — service-kind principals can only
    mint features and register files into existing references."""
    try:
        row = await pool.fetchrow(
            "INSERT INTO qiita.reference"
            "  (name, version, kind, is_host, created_by_idx)"
            " VALUES ($1, $2, $3, $4, $5)"
            f" RETURNING {_REFERENCE_RETURNING}",
            body.name,
            body.version,
            body.kind,
            body.is_host,
            user.principal_idx,
        )
    except asyncpg.UniqueViolationError:
        raise HTTPException(
            status_code=409,
            detail=f"Reference ({body.name!r}, {body.version!r}) already exists",
        )
    except asyncpg.PostgresError as exc:
        raise HTTPException(status_code=500, detail="Database error") from exc
    return ReferenceResponse(**dict(row))


@router.get(PATH_REFERENCE_ROOT)
async def list_references(
    pool: asyncpg.Pool = Depends(get_db_pool),
    _principal: Principal = Depends(get_current_principal),
    kind: ReferenceKind | None = None,
    is_host: bool | None = None,
    status: ReferenceStatus | None = None,
) -> list[ReferenceResponse]:
    """Anonymous-OK list of references, optionally filtered by `kind`,
    `is_host`, and `status`. Ordered by reference_idx. Row-level visibility
    (e.g. hiding private references) is not yet implemented — same posture as
    the single-reference GET."""
    clauses: list[str] = []
    args: list[object] = []
    if kind is not None:
        args.append(kind)
        clauses.append(f"kind = ${len(args)}")
    if is_host is not None:
        args.append(is_host)
        clauses.append(f"is_host = ${len(args)}")
    if status is not None:
        args.append(str(status))
        clauses.append(f"status = ${len(args)}")
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = await pool.fetch(
        f"SELECT {_REFERENCE_RETURNING} FROM qiita.reference{where} ORDER BY reference_idx",
        *args,
    )
    return [ReferenceResponse(**dict(r)) for r in rows]


@router.get(PATH_REFERENCE_INDEX)
async def get_reference_index(
    reference_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    _scope: Principal = Depends(require_scope(Scope.REFERENCE_READ)),
) -> list[ReferenceIndex]:
    """List built search indexes (e.g. a rype `.ryxdi`) for a reference,
    newest first. 404 when the reference itself doesn't exist; an empty list
    when it exists but has no index built yet — the two are distinct.

    `fs_path` is the on-disk index location a future host-filter compute job
    consumes (the runner injects it directly; this endpoint is for general
    visibility / admin). Scoped to reference:read — unlike the anonymous-OK
    reference metadata GETs — because fs_path exposes internal filesystem
    layout; reference:read is held by every human role and service account."""
    exists = await pool.fetchval(
        "SELECT 1 FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    if exists is None:
        raise HTTPException(status_code=404, detail=_MSG_REFERENCE_NOT_FOUND)
    rows = await pool.fetch(
        "SELECT reference_index_idx, reference_idx, index_type, fs_path, params, created_at"
        " FROM qiita.reference_index WHERE reference_idx = $1"
        " ORDER BY created_at DESC, reference_index_idx DESC",
        reference_idx,
    )
    out: list[ReferenceIndex] = []
    for r in rows:
        d = dict(r)
        # params is JSONB — asyncpg returns it as a JSON string by default.
        if isinstance(d["params"], str):
            d["params"] = json.loads(d["params"])
        out.append(ReferenceIndex(**d))
    return out


@router.get(PATH_REFERENCE_BY_IDX)
async def get_reference(
    reference_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    _principal: Principal = Depends(get_current_principal),
) -> ReferenceResponse:
    """Anonymous-OK. Returns the full ReferenceResponse including
    created_by_idx; row-level visibility (e.g., hiding private references'
    owner) is not yet implemented."""
    row = await pool.fetchrow(
        f"SELECT {_REFERENCE_RETURNING} FROM qiita.reference WHERE reference_idx = $1",
        reference_idx,
    )
    if row is None:
        raise HTTPException(status_code=404, detail=_MSG_REFERENCE_NOT_FOUND)
    return ReferenceResponse(**dict(row))


@router.patch(PATH_REFERENCE_STATUS)
async def update_reference_status(
    reference_idx: Annotated[int, Field(gt=0)],
    body: ReferenceStatusUpdate,
    pool: asyncpg.Pool = Depends(get_db_pool),
    _scope: Principal = Depends(require_scope(Scope.REFERENCE_WRITE)),
) -> ReferenceResponse:
    try:
        return await transition_reference_status(pool, reference_idx, body.status)
    except ReferenceNotFound:
        raise HTTPException(status_code=404, detail=_MSG_REFERENCE_NOT_FOUND)
    except IllegalStatusTransition as exc:
        raise HTTPException(status_code=409, detail=str(exc))


# Tables that can appear in a DoGet ticket. Must match the data plane's
# ALLOWED_TABLES whitelist in flight_service.rs.
_DOGET_ALLOWED_TABLES = frozenset(
    {
        "reference_sequences",
        "reference_sequence_chunks",
        "reference_taxonomy",
        "reference_phylogeny",
        "reference_placements",
    }
)


@router.post(PATH_REFERENCE_DOGET, status_code=201)
async def create_doget_ticket(
    reference_idx: Annotated[int, Field(gt=0)],
    body: DoGetTicketRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    hmac_secret: bytes = Depends(get_hmac_secret),
    _scope: Principal = Depends(require_scope(Scope.TICKET_DOGET)),
) -> DoGetTicketResponse:
    """Sign a DoGet ticket scoped to a reference.

    Reference must be active. The ticket contains only reference_idx — the
    data plane resolves feature membership at query time via the DuckLake
    reference_membership table (JOIN for reference_sequences, direct
    WHERE for taxonomy/phylogeny).

    Authorization is scope-only at this layer: any principal with
    `tickets:doget` can request a ticket. Row-level visibility (private
    references) is not yet implemented.
    """
    if body.table not in _DOGET_ALLOWED_TABLES:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown table {body.table!r}; allowed: {sorted(_DOGET_ALLOWED_TABLES)}",
        )

    # Reference must be active
    status = await pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1",
        reference_idx,
    )
    if status is None:
        raise HTTPException(status_code=404, detail=_MSG_REFERENCE_NOT_FOUND)
    if status != ReferenceStatus.ACTIVE.value:
        raise HTTPException(
            status_code=409,
            detail=f"Reference status is {status!r}, must be {ReferenceStatus.ACTIVE.value!r}",
        )

    ticket_bytes = sign_ticket(
        table=body.table,
        filter={"reference_idx": [reference_idx]},
        secret=hmac_secret,
    )
    return DoGetTicketResponse(ticket=base64.b64encode(ticket_bytes).decode())
