"""Upload domain — generic Arrow-data staging slots.

`POST /api/v1/upload` mints a row in `qiita.upload` and returns a signed
DoPut Flight ticket. The client streams Arrow batches to the data plane
against that ticket; on completion the client posts the data-plane's
PutResult claim back via `POST /api/v1/upload/{idx}/done`.

The domain is content-agnostic by design — no reference_idx, no role
enum, no FASTA-specific fields. Consumer-side authorization (which
workflow consumes the upload_idx, with what gate) lives on the
work_ticket that references the upload, not here.
"""

import base64
from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from pydantic import Field
from qiita_common.api_paths import (
    PATH_UPLOAD_BY_IDX,
    PATH_UPLOAD_DONE,
    PATH_UPLOAD_PREFIX,
    PATH_UPLOAD_ROOT,
)
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    UploadCreateRequest,
    UploadCreateResponse,
    UploadDoneRequest,
    UploadResponse,
    UploadStatus,
)

from ..auth.guards import require_scope
from ..auth.principal import Principal
from ..auth.tickets import sign_doput
from ..deps import get_db_pool, get_flight_signing_key

router = APIRouter(prefix=PATH_UPLOAD_PREFIX, tags=["upload"])


# Single projection used by every read path so a future column add doesn't
# silently shift positional access.
_UPLOAD_RETURNING = (
    "upload_idx, status, description, sha256, row_count, bytes_received, "
    "created_by_idx, created_at, completed_at"
)


def _record_to_response(row: asyncpg.Record) -> UploadResponse:
    return UploadResponse(
        upload_idx=row["upload_idx"],
        status=row["status"],
        description=row["description"],
        sha256=row["sha256"],
        row_count=row["row_count"],
        bytes_received=row["bytes_received"],
        created_by_idx=row["created_by_idx"],
        created_at=row["created_at"],
        completed_at=row["completed_at"],
    )


@router.post(PATH_UPLOAD_ROOT, status_code=201)
async def create_upload(
    body: UploadCreateRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    hmac_secret: bytes = Depends(get_flight_signing_key),
    principal: Principal = Depends(require_scope(Scope.TICKET_DOPUT)),
) -> UploadCreateResponse:
    """Mint an upload slot and return a signed DoPut ticket.

    The row lands at status='pending'; transitions to 'ready' on
    `POST /upload/{idx}/done`. /done and `GET /upload/{idx}` both gate
    on `created_by_idx == principal.principal_idx` so admins only see /
    finalize their own uploads — matching the invariant
    `_resolve_upload_handles` enforces at work-ticket dispatch.

    Returns the narrower `UploadCreateResponse` (upload_idx + signed
    ticket) rather than the full `UploadResponse` shape every other read
    path uses — `_record_to_response` doesn't fit here because the
    create response also carries the DoPut ticket that lives outside
    the row.
    """
    try:
        row = await pool.fetchrow(
            "INSERT INTO qiita.upload (description, created_by_idx)"
            " VALUES ($1, $2)"
            f" RETURNING {_UPLOAD_RETURNING}",
            body.description,
            principal.principal_idx,
        )
    except asyncpg.PostgresError as exc:
        raise HTTPException(status_code=500, detail="database error") from exc

    ticket = sign_doput(upload_idx=row["upload_idx"], secret=hmac_secret)
    return UploadCreateResponse(
        upload_idx=row["upload_idx"],
        doput_ticket=base64.b64encode(ticket).decode(),
    )


@router.post(PATH_UPLOAD_DONE)
async def complete_upload(
    upload_idx: Annotated[int, Field(gt=0)],
    body: UploadDoneRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    principal: Principal = Depends(require_scope(Scope.TICKET_DOPUT)),
) -> UploadResponse:
    """Record the client's completion claim and transition pending → ready.

    Idempotent on a `ready` row when the claim matches the previously
    recorded sha256 / row_count / bytes_received; a conflicting retry
    returns 409. The recorded values are descriptive — the consuming
    workflow re-verifies the staged file's content itself.

    Gated on `created_by_idx == principal.principal_idx`: an admin can
    only /done an upload they themselves created. This matches the same
    invariant `_resolve_upload_handles` enforces at work-ticket dispatch
    time (runner.py); without it, any admin with TICKET_DOPUT scope
    could finalize a different admin's pending row.
    """
    # Atomic transition: only flip if currently pending AND owned by the
    # caller. The UPDATE returns the new row when the transition fired;
    # NULL when status was something else, the row didn't exist, or
    # ownership didn't match. We disambiguate below.
    row = await pool.fetchrow(
        "UPDATE qiita.upload"
        " SET status = $1,"
        "     sha256 = $2,"
        "     row_count = $3,"
        "     bytes_received = $4,"
        "     completed_at = now()"
        " WHERE upload_idx = $5 AND status = $6 AND created_by_idx = $7"
        f" RETURNING {_UPLOAD_RETURNING}",
        UploadStatus.READY.value,
        body.sha256,
        body.row_count,
        body.bytes_received,
        upload_idx,
        UploadStatus.PENDING.value,
        principal.principal_idx,
    )
    if row is not None:
        return _record_to_response(row)

    # The UPDATE didn't fire — either the row doesn't exist, it's in a
    # non-pending state, or ownership mismatched. Disambiguate in one
    # read, accepting the tiniest TOCTOU window (a concurrent transition
    # would have produced the same observable outcome as "already moved
    # on"). 404 is returned both for "no such row" and "row owned by
    # someone else" — leaking existence to a non-owner is unhelpful.
    current = await pool.fetchrow(
        f"SELECT {_UPLOAD_RETURNING} FROM qiita.upload"
        " WHERE upload_idx = $1 AND created_by_idx = $2",
        upload_idx,
        principal.principal_idx,
    )
    if current is None:
        raise HTTPException(status_code=404, detail="upload not found")

    # Idempotency: a `ready` row with the same claim returns 200 with the
    # current state. A `ready` row with a different claim is a contract
    # violation (the staged file is immutable post-DoPut); fail loud.
    if current["status"] == UploadStatus.READY.value:
        same = (
            current["sha256"] == body.sha256
            and current["row_count"] == body.row_count
            and current["bytes_received"] == body.bytes_received
        )
        if same:
            return _record_to_response(current)
        raise HTTPException(
            status_code=409,
            detail="upload already completed with a different claim",
        )

    raise HTTPException(
        status_code=409,
        detail=(f"upload status is {current['status']!r}, expected {UploadStatus.PENDING.value!r}"),
    )


@router.get(PATH_UPLOAD_BY_IDX)
async def get_upload(
    upload_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    principal: Principal = Depends(require_scope(Scope.TICKET_DOPUT)),
) -> UploadResponse:
    # Owner-gated read: an admin only sees their own uploads. Mirrors
    # the /done and `_resolve_upload_handles` invariant — a row created
    # by a different principal returns 404, not 403, so existence isn't
    # leaked across owners.
    row = await pool.fetchrow(
        f"SELECT {_UPLOAD_RETURNING} FROM qiita.upload"
        " WHERE upload_idx = $1 AND created_by_idx = $2",
        upload_idx,
        principal.principal_idx,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="upload not found")
    return _record_to_response(row)
