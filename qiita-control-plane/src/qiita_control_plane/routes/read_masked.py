"""Masked-read routes: mask_idx minting + the masked-read DoGet ticket.

Two routers live here because they are the two halves of one feature:

* ``POST /mask-definition`` mints (idempotently, deduped on a canonical-config
  hash) the ``mask_idx`` that identifies a read-filtering config. Same config →
  same ``mask_idx`` fleet-wide.
* ``POST /read-masked/ticket/doget`` signs an HMAC DoGet ticket scoped to a
  single ``(prep_sample_idx, mask_idx)`` on the data plane's ``read_masked``
  view — the only Flight-reachable read surface (raw ``read``/``read_mask`` are
  out of Flight by construction, so unmasked/human reads are unreachable).

Both are service-account-only, gated on ``Scope.READ_MASKED_DOGET``. Humans
never mint masks or pull masked reads — the masked-read consumer path is
service-driven, and the lake retains privacy-sensitive (human/host) reads that
the ``read_masked`` view excludes only via ``WHERE reason='pass'``.

**Mandatory-filter invariant.** The data plane's ``build_query`` returns an
unfiltered ``SELECT * FROM read_masked`` for an empty filter — i.e. every
sample's pass reads across every mask, fleet-wide. So the DoGet route MUST
inject a non-empty ``prep_sample_idx`` AND a ``mask_idx`` into every signed
ticket and reject anything that would produce an empty filter. Pydantic's
``gt=0`` on both fields makes an empty/zero filter unrepresentable at the
request layer; the route re-asserts non-empty before signing as defence in
depth, so an unfiltered ``read_masked`` ticket can never be signed here.
"""

import base64
import json
from typing import Annotated

import asyncpg
import pyarrow.flight as _flight
from fastapi import APIRouter, Depends, HTTPException
from pydantic import Field
from qiita_common.api_paths import (
    PATH_MASK_DEFINITION_BY_IDX,
    PATH_MASK_DEFINITION_PREFIX,
    PATH_MASK_DEFINITION_ROOT,
    PATH_READ_MASKED_DOGET,
    PATH_READ_MASKED_PREFIX,
)
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    DoGetTicketResponse,
    MaskDefinition,
    MaskDefinitionDeleteResponse,
    MaskDefinitionMintRequest,
    ReadMaskedDoGetTicketRequest,
)

from ..actions.library import delete_mask_data
from ..auth.guards import require_scope, require_service_with_scope
from ..auth.principal import Principal, ServiceAccount
from ..auth.tickets import sign_ticket
from ..deps import (
    TxConnFactory,
    get_data_plane_url,
    get_db_pool,
    get_flight_signing_key,
    get_tx_conn_factory,
)
from ..repositories.mask_definition import mint_mask_definition

_MSG_MASK_NOT_FOUND = "Mask definition not found"

# The masked-read view table this route is allowed to sign tickets for. Must
# match the CP-side _DOGET_ALLOWED_TABLES (routes/reference.py) and the data
# plane's ALLOWED_TABLES, which back the read_masked view the ticket targets.
# A constant rather than a free literal so the read-masked table name has one
# definition the route signs against.
_READ_MASKED_TABLE = "read_masked"

mask_definition_router = APIRouter(prefix=PATH_MASK_DEFINITION_PREFIX, tags=["mask-definition"])
read_masked_router = APIRouter(prefix=PATH_READ_MASKED_PREFIX, tags=["read-masked"])


def _mask_record_to_response(row: asyncpg.Record) -> MaskDefinition:
    """Project a qiita.mask_definition asyncpg.Record onto the response model.

    `params` is JSONB — asyncpg returns it as a JSON string by default, so
    parse it back to a dict for the wire model. Field access is by name so a
    future column add can't silently shift the projection.
    """
    params = row["params"]
    if isinstance(params, str):
        params = json.loads(params)
    return MaskDefinition(
        mask_idx=row["mask_idx"],
        filter_workflow=row["filter_workflow"],
        filter_version=row["filter_version"],
        params=params,
        created_at=row["created_at"],
    )


@mask_definition_router.post(PATH_MASK_DEFINITION_ROOT, status_code=201)
async def mint_mask_definition_route(
    body: MaskDefinitionMintRequest,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    sa: ServiceAccount = Depends(require_service_with_scope(Scope.READ_MASKED_DOGET)),
) -> MaskDefinition:
    """Mint (or return the existing) mask_idx for a read-filtering config.

    Idempotent: the same `params` (canonical-JSON hashed) always returns the
    same mask_idx, so a 201 may carry a pre-existing row. Caller must be a
    ServiceAccount holding `read_masked:doget`.

    Maps repository-layer exceptions to HTTP status:
      - asyncpg.ForeignKeyViolationError → 400 (unknown principal_idx; defence
        in depth — the principal is the authenticated service account).
      - asyncpg.InvalidParameterValueError (SQLSTATE 22023) → 400 (a non-32-byte
        hash; the repository helper always passes a 32-byte digest).
      - Any other asyncpg.PostgresError → 500 with a generic detail.
    """
    async with tx() as conn:
        try:
            row = await mint_mask_definition(
                conn,
                filter_workflow=body.filter_workflow,
                filter_version=body.filter_version,
                params=body.params,
                principal_idx=sa.principal_idx,
            )
        except asyncpg.ForeignKeyViolationError:
            raise HTTPException(status_code=400, detail="invalid principal for mask mint")
        except asyncpg.InvalidParameterValueError:
            raise HTTPException(status_code=400, detail="invalid mask-definition parameters")
        except asyncpg.PostgresError:
            raise HTTPException(status_code=500, detail="database error")

    return _mask_record_to_response(row)


@mask_definition_router.delete(PATH_MASK_DEFINITION_BY_IDX)
async def delete_mask_definition_route(
    mask_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    hmac_secret: bytes = Depends(get_flight_signing_key),
    data_plane_url: str = Depends(get_data_plane_url),
    _scope: Principal = Depends(require_scope(Scope.MASK_DEFINITION_DELETE)),
) -> MaskDefinitionDeleteResponse:
    """Fully purge a mask — its DuckLake `read_mask` rows then its Postgres
    `mask_definition` row. system_admin only (`mask_definition:delete`).

    Order of operations: existence check first (404 if the mask is absent) →
    DuckLake delete (one all-or-nothing transaction; a 502 on failure removes
    nothing yet, since the existence check is a read and the Postgres delete
    hasn't run) → Postgres `mask_definition` delete last. This ordering makes the
    operation *retriable*: both mutating steps are idempotent and the
    `qiita.mask_definition` row — the thing a retry keys off — is removed last. If
    the lake delete succeeds but the Postgres delete fails, the mask row survives
    and re-issuing the DELETE re-runs both idempotent steps (the second lake
    delete removes 0 rows). A crash therefore leaves at worst a recoverable
    orphan-Postgres row, never an unrecoverable orphan-lake.

    Referencing `qiita.work_ticket` rows detach automatically — the
    `work_ticket.mask_idx` FK is `ON DELETE SET NULL` — so no work-ticket touch
    is needed here.

    Intentional divergence from reference-delete: that route gates on in-flight
    work_tickets (409 via `assert_reference_deletable`); this primitive
    deliberately does NOT. It is an admin-only sharp primitive that lets the FK
    detach any referencing ticket. The shared-mask SAFETY guard — don't delete a
    mask still referenced by a non-failed ticket — lives in the bulk
    purge-failed tool that wraps this route, not in the primitive itself. The
    absence of gating here is a conscious decision, not an oversight.
    """
    # Existence check (a read; safe before the lake delete).
    exists = await pool.fetchval(
        "SELECT 1 FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx
    )
    if exists is None:
        raise HTTPException(status_code=404, detail=_MSG_MASK_NOT_FOUND)

    # DuckLake read_mask rows (idempotent, atomic delete-by-mask_idx in the data
    # plane). Lake-first so a crash before the Postgres delete leaves a
    # recoverable orphan-Postgres row, not an unrecoverable orphan-lake.
    try:
        rows_deleted = await delete_mask_data(
            mask_idx=mask_idx,
            hmac_secret=hmac_secret,
            data_plane_url=data_plane_url,
        )
    except _flight.FlightError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"data plane mask delete failed; nothing removed yet: {exc}",
        ) from exc

    # Postgres row last. The work_ticket FK is ON DELETE SET NULL, so referencing
    # tickets detach automatically — no need to touch work_ticket here.
    await pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)

    return MaskDefinitionDeleteResponse(mask_idx=mask_idx, rows_deleted=rows_deleted)


@read_masked_router.post(PATH_READ_MASKED_DOGET, status_code=201)
async def create_read_masked_doget_ticket(
    body: ReadMaskedDoGetTicketRequest,
    hmac_secret: bytes = Depends(get_flight_signing_key),
    sa: ServiceAccount = Depends(require_service_with_scope(Scope.READ_MASKED_DOGET)),
) -> DoGetTicketResponse:
    """Sign a DoGet ticket scoped to (prep_sample_idx, mask_idx) on read_masked.

    Caller must be a ServiceAccount holding `read_masked:doget`. The ticket
    filters the data plane's read_masked view to exactly one sample under
    exactly one mask config; the view's `WHERE reason='pass'` excludes human/host
    reads by construction.

    Mandatory-filter invariant: both identifiers are required and positive
    (Pydantic gt=0), so the signed filter is always non-empty. The route
    re-asserts this before signing — an unfiltered read_masked ticket is never
    signed, which would otherwise dump every sample's pass reads fleet-wide.
    """
    filter_ = {
        "prep_sample_idx": [body.prep_sample_idx],
        "mask_idx": [body.mask_idx],
    }
    # Defence in depth against the mandatory-filter invariant: never sign a
    # read_masked ticket whose filter (or any filter value list) is empty.
    if not filter_ or any(not v for v in filter_.values()):
        raise HTTPException(
            status_code=422,
            detail="read_masked ticket requires a non-empty prep_sample_idx and mask_idx filter",
        )

    ticket_bytes = sign_ticket(
        table=_READ_MASKED_TABLE,
        filter=filter_,
        secret=hmac_secret,
    )
    return DoGetTicketResponse(ticket=base64.b64encode(ticket_bytes).decode())
