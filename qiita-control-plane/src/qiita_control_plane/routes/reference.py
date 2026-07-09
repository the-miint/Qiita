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
import httpx
import pyarrow.flight as _flight
from fastapi import APIRouter, Depends, HTTPException, Query, Request
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
    ReferenceDeleteResponse,
    ReferenceIndex,
    ReferenceKind,
    ReferenceResponse,
    ReferenceStatus,
    ReferenceStatusUpdate,
)

from ..actions.library import delete_reference_data
from ..actions.reference import (
    REFERENCE_RETURNING,
    IllegalStatusTransition,
    ReferenceDeleteBlocked,
    ReferenceNotFound,
    assert_reference_deletable,
    delete_reference_cascade,
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
from ..deps import (
    TxConnFactory,
    get_data_plane_url,
    get_db_pool,
    get_flight_signing_key,
    get_tx_conn_factory,
)

router = APIRouter(prefix=PATH_REFERENCE_PREFIX, tags=["reference"])


# Single source of truth lives in actions/reference.py (REFERENCE_RETURNING);
# aliased here so the existing in-file references read unchanged.
_REFERENCE_RETURNING = REFERENCE_RETURNING

# Backstop cap for the anonymous catalog list. The table is small (curated
# reference databases), so this never bites in practice; it bounds the
# worst-case payload and is caller-overridable via ?limit=.
_DEFAULT_LIST_LIMIT = 1000
_MAX_LIST_LIMIT = 5000

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
    limit: int = Query(default=_DEFAULT_LIST_LIMIT, ge=1, le=_MAX_LIST_LIMIT),
) -> list[ReferenceResponse]:
    """Anonymous-OK list of references, optionally filtered by `kind`,
    `is_host`, and `status`. Ordered by reference_idx, bounded by `limit`
    (default 1000) so the anonymous endpoint can't be made to return an
    unbounded payload. Row-level visibility (e.g. hiding private references) is
    not yet implemented — same posture as the single-reference GET."""
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
    args.append(limit)
    rows = await pool.fetch(
        f"SELECT {_REFERENCE_RETURNING} FROM qiita.reference{where}"
        f" ORDER BY reference_idx LIMIT ${len(args)}",
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


@router.delete(PATH_REFERENCE_BY_IDX)
async def delete_reference(
    reference_idx: Annotated[int, Field(gt=0)],
    request: Request,
    force: bool = False,
    pool: asyncpg.Pool = Depends(get_db_pool),
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    hmac_secret: bytes = Depends(get_flight_signing_key),
    data_plane_url: str = Depends(get_data_plane_url),
    _scope: Principal = Depends(require_scope(Scope.REFERENCE_DELETE)),
) -> ReferenceDeleteResponse:
    """Fully purge a reference — Postgres rows, DuckLake data, and on-disk
    indexes. system_admin only (`reference:delete`).

    Gating: work tickets in-flight (pending/queued/processing) block the delete
    unconditionally (409); completed/failed tickets block it unless `force=true`
    is passed. Shared features (claimed by another reference) are never deleted.

    Ordering is data-plane → orchestrator → Postgres, chosen so the operation
    is *retriable*: every step is idempotent and the `qiita.reference` row — the
    thing a retry keys off — is removed last. The data-plane delete is one
    DuckLake transaction (all-or-nothing), so a failure there leaves DuckLake
    membership fully intact and a retry recomputes the same orphan set. If the
    orchestrator or Postgres step fails, the reference row survives and
    re-issuing the DELETE re-runs every idempotent step. The one residual
    degraded state is a Postgres teardown that fails *after* the data is gone:
    the reference is then empty-but-listed until a retry completes the teardown
    (Postgres membership is still intact, so its orphan GC stays correct).
    Reclaiming DuckLake/disk bytes in that window is not yet automated.
    """
    try:
        await assert_reference_deletable(pool, reference_idx, force=force)
    except ReferenceNotFound:
        raise HTTPException(status_code=404, detail=_MSG_REFERENCE_NOT_FOUND)
    except ReferenceDeleteBlocked as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    # DuckLake data (idempotent, atomic delete-by-reference_idx in the data plane).
    try:
        await delete_reference_data(
            reference_idx=reference_idx,
            hmac_secret=hmac_secret,
            data_plane_url=data_plane_url,
        )
    except _flight.FlightError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"data plane reference delete failed; nothing removed yet: {exc}",
        ) from exc

    # On-disk index artifacts (orchestrator-side; only it can reach
    # PATH_DERIVED). Skipped when no orchestrator is configured (CP-only/dev),
    # in which case there are no compute-built indexes to remove anyway. An
    # orchestrator transport/5xx error here surfaces as a 502 (not an unhandled
    # 500): DuckLake data is already gone, but the reference row still exists,
    # so the operator can re-run the idempotent DELETE to finish cleanup.
    artifacts_removed = False
    client = getattr(request.app.state, "compute_backend_client", None)
    if client is not None:
        try:
            purge = await client.purge_reference_artifacts(reference_idx)
        except httpx.HTTPError as exc:
            raise HTTPException(
                status_code=502,
                detail=(
                    "orchestrator on-disk artifact cleanup failed after DuckLake"
                    f" data was removed; re-run the delete to finish: {exc}"
                ),
            ) from exc
        artifacts_removed = purge.removed

    # Re-gate inside the teardown transaction to close the precheck→cascade
    # window: a work ticket that went in-flight since the precheck must abort
    # the teardown (and 409 loudly) rather than be silently deleted by the
    # cascade. force=True here means only a *new in-flight* ticket aborts —
    # terminal tickets are still the cascade's to delete.
    async with tx() as conn:
        try:
            await assert_reference_deletable(conn, reference_idx, force=True)
        except ReferenceNotFound:
            raise HTTPException(status_code=404, detail=_MSG_REFERENCE_NOT_FOUND)
        except ReferenceDeleteBlocked as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        counts = await delete_reference_cascade(conn, reference_idx)

    return ReferenceDeleteResponse(
        reference_idx=reference_idx,
        artifacts_removed=artifacts_removed,
        **counts,
    )


# Tables that can appear in a DoGet ticket, CP-side mirror of the data plane's
# ALLOWED_TABLES whitelist in flight_service.rs. Must stay in sync with it.
# `read_masked` (the masked-read surface) is the one the data plane reaches via
# Flight in addition to the reference_* tables; raw `read`/`read_mask` are
# deliberately absent from both allowlists (privacy by construction).
_DOGET_ALLOWED_TABLES = frozenset(
    {
        "reference_sequences",
        "reference_sequence_chunks",
        "reference_taxonomy",
        "reference_phylogeny",
        "reference_placements",
        "read_masked",
    }
)

# The subset the reference DoGet route below can sign. `read_masked` is reached
# through the dedicated /read-masked/ticket/doget route (routes/read_masked.py),
# whose ticket carries (prep_sample_idx, mask_idx) — not reference_idx — and
# which enforces the mandatory-filter invariant. The reference route restricts
# itself to the reference_* tables whose membership it resolves.
_REFERENCE_DOGET_TABLES = _DOGET_ALLOWED_TABLES - frozenset({"read_masked"})


@router.post(PATH_REFERENCE_DOGET, status_code=201)
async def create_doget_ticket(
    reference_idx: Annotated[int, Field(gt=0)],
    body: DoGetTicketRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    hmac_secret: bytes = Depends(get_flight_signing_key),
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
    if body.table not in _REFERENCE_DOGET_TABLES:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown table {body.table!r}; allowed: {sorted(_REFERENCE_DOGET_TABLES)}",
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
