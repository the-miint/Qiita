"""Block-read DoGet ticket route.

``POST /read/ticket/doget`` signs an Ed25519 DoGet ticket scoped to ONE block's
``(prep_sample_idx, sequence_idx sub-range)`` members, so a block-scoped compute
job (read-mask-block's ``qc`` / ``host_filter``, align's ``align_sharded``)
STREAMS its reads from the data plane instead of reading a Parquet the control
plane materialized onto shared scratch at submit time.

Service-account-only, gated on ``Scope.TICKET_DOGET`` — the same scope the
sibling ``POST /alignment/ticket/doget`` uses, and for the same reason: the job
mints at RUNTIME (tickets are short-TTL and a SLURM queue can outlive a
submit-time ticket). Reusing that scope rather than minting a new one keeps the
service-account grant surface fixed, so this route needs no operator action.

The body carries only ``work_ticket_idx``. Everything that scopes the ticket is
read CP-side:

* the members, from ``qiita.block_member`` — a block can cover hundreds of
  samples, and that list has no business riding a request body; and
* the selector (raw ``read_block`` vs mask-scoped ``read_masked_block``), from
  the ticket's ``action_context`` via the shared
  ``block_read.resolve_block_read_scope``.

**Privacy.** ``read_block`` streams RAW reads, which may contain host/human
sequence. That is the same data the retired ``export_read_block`` DoAction used
to write onto shared scratch, so this is a narrowing rather than a widening —
but it means the scope check here is load-bearing, not decorative. A ticket is
only ever signed for a BLOCK-scoped work ticket with a non-empty member set;
``sign_ticket`` refuses an empty selector, and the data plane refuses one again.
"""

import base64
import json

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from qiita_common.api_paths import PATH_READ_DOGET, PATH_READ_PREFIX
from qiita_common.auth_constants import Scope
from qiita_common.models import DoGetTicketResponse, ReadDoGetTicketRequest, ScopeTargetKind

from ..auth.guards import require_service_with_scope
from ..auth.principal import ServiceAccount
from ..auth.tickets import sign_ticket
from ..block_read import resolve_block_read_scope
from ..deps import get_db_pool, get_flight_signing_key
from ..repositories.block import fetch_block_members

read_router = APIRouter(prefix=PATH_READ_PREFIX, tags=["read"])


@read_router.post(PATH_READ_DOGET, status_code=201)
async def create_read_doget_ticket(
    body: ReadDoGetTicketRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    signing_key: bytes = Depends(get_flight_signing_key),
    _sa: ServiceAccount = Depends(require_service_with_scope(Scope.TICKET_DOGET)),
) -> DoGetTicketResponse:
    """Sign a block-read DoGet ticket for the given block work ticket.

    Fail loud at every step — a bad scope here would stream the wrong reads, and
    for the raw selector those reads are human-containing:

    * unknown ``work_ticket_idx`` → 404;
    * a work ticket that is not BLOCK-scoped → 422 (only a block has members;
      the per-sample read path does not use this route);
    * an ``action_context`` whose alignment intent disagrees with the
      ``work_ticket.alignment_idx`` column → 422 (a mid-flight alignment DELETE;
      see ``resolve_block_read_scope``);
    * a block with no members → 422 (a planning bug — never a licence to read
      unscoped).

    Authorization is scope-only at this layer, matching the reference /
    read_masked / alignment doget routes: any service account holding
    ``ticket:doget`` can request a ticket; row-level visibility is not enforced
    here.
    """
    row = await pool.fetchrow(
        "SELECT scope_target_kind, block_idx, alignment_idx, mask_idx, action_context"
        "  FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        body.work_ticket_idx,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="work ticket not found")

    if row["scope_target_kind"] != ScopeTargetKind.BLOCK.value:
        raise HTTPException(
            status_code=422,
            detail=(
                "a block-read DoGet ticket requires a block-scoped work ticket; "
                f"work ticket {body.work_ticket_idx} is "
                f"{row['scope_target_kind']!r}-scoped"
            ),
        )

    # asyncpg hands JSONB back as str under the default codec (or a dict if one
    # is registered) — accept both, mirroring routes/alignment.py.
    raw = row["action_context"]
    ctx = json.loads(raw) if isinstance(raw, str) else raw
    if ctx is None:
        ctx = {}
    if not isinstance(ctx, dict):
        raise HTTPException(status_code=422, detail="work ticket action_context is not an object")

    try:
        table, filter_ = resolve_block_read_scope(
            action_context=ctx,
            ticket_alignment_idx=row["alignment_idx"],
            ticket_mask_idx=row["mask_idx"],
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    members = [
        {
            "prep_sample_idx": prep_sample_idx,
            "sequence_idx_start": lo,
            "sequence_idx_stop": hi,
        }
        for (prep_sample_idx, lo, hi) in await fetch_block_members(pool, row["block_idx"])
    ]
    if not members:
        raise HTTPException(
            status_code=422,
            detail=(
                f"block {row['block_idx']} has no members — a planning bug; refusing "
                "to sign an unscoped read ticket"
            ),
        )

    ticket_bytes = sign_ticket(table=table, filter=filter_, members=members, secret=signing_key)
    return DoGetTicketResponse(ticket=base64.b64encode(ticket_bytes).decode())
