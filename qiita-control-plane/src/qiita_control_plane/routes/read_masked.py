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

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from qiita_common.api_paths import (
    PATH_MASK_DEFINITION_PREFIX,
    PATH_MASK_DEFINITION_ROOT,
    PATH_READ_MASKED_DOGET,
    PATH_READ_MASKED_PREFIX,
)
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    DoGetTicketResponse,
    MaskDefinition,
    MaskDefinitionMintRequest,
    ReadMaskedDoGetTicketRequest,
)

from ..auth.guards import require_service_with_scope
from ..auth.principal import ServiceAccount
from ..auth.tickets import sign_ticket
from ..deps import TxConnFactory, get_hmac_secret, get_tx_conn_factory
from ..repositories.mask_definition import mint_mask_definition

# The masked-read view table this route is allowed to sign tickets for. Must
# match the CP-side _DOGET_ALLOWED_TABLES (routes/reference.py) and the data
# plane's ALLOWED_TABLES — the DP side gains this entry (plus the read_masked
# view itself) in the follow-up data-plane PR; until then a ticket signed here
# is rejected by the DP. A constant rather than a free literal so the read-masked
# table name has one definition the route signs against.
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


@read_masked_router.post(PATH_READ_MASKED_DOGET, status_code=201)
async def create_read_masked_doget_ticket(
    body: ReadMaskedDoGetTicketRequest,
    hmac_secret: bytes = Depends(get_hmac_secret),
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
