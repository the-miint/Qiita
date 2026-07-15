"""Control-plane routes for the sharded-alignment config identity
(`qiita.alignment_definition`).

The `alignment_idx` that keys the DuckLake `alignment` rows is minted at plan time
by the align-plan route (`POST .../sequenced-pool/{P}/align-plan`), so there is no
public POST here. This module owns the destructive DELETE — the full purge of an
alignment (its DuckLake rows + the Postgres `alignment_definition` row) — which is
the escape hatch the align planner's disallow-without-delete rule requires: an
operator must DELETE a completed alignment before re-aligning the same config.

Modelled on the mask-definition purge (`routes/read_masked.py`): lake-first,
system_admin-only, idempotent/retriable.
"""

import base64
import json
from typing import Annotated

import asyncpg
import pyarrow.flight as _flight
from fastapi import APIRouter, Depends, HTTPException
from pydantic import Field
from qiita_common.api_paths import (
    PATH_ALIGNMENT_DEFINITION_BY_IDX,
    PATH_ALIGNMENT_DEFINITION_PREFIX,
    PATH_ALIGNMENT_DOGET,
    PATH_ALIGNMENT_PREFIX,
)
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    AlignmentDefinitionDeleteResponse,
    AlignmentDoGetTicketRequest,
    DoGetTicketResponse,
)

from ..actions.library import delete_alignment_data
from ..auth.guards import require_scope, require_service_with_scope
from ..auth.principal import Principal, ServiceAccount
from ..auth.tickets import sign_ticket
from ..deps import get_data_plane_url, get_db_pool, get_flight_signing_key
from ..feature_table import parse_feature_table_scope

_MSG_ALIGNMENT_NOT_FOUND = "Alignment definition not found"

# The DuckLake table this route signs DoGet tickets for. Must stay in sync with
# the CP-side _DOGET_ALLOWED_TABLES (routes/reference.py) and the data plane's
# ALLOWED_TABLES. A constant so the alignment table name has one definition here.
_ALIGNMENT_TABLE = "alignment"

alignment_definition_router = APIRouter(
    prefix=PATH_ALIGNMENT_DEFINITION_PREFIX, tags=["alignment-definition"]
)

# Distinct from alignment_definition_router (the /alignment-definition identity
# CRUD): this serves the /alignment Flight DoGet-ticket surface for the alignment
# sink, mirroring how read_masked.py holds its doget router beside mask-definition.
alignment_router = APIRouter(prefix=PATH_ALIGNMENT_PREFIX, tags=["alignment"])


@alignment_definition_router.delete(PATH_ALIGNMENT_DEFINITION_BY_IDX)
async def delete_alignment_definition_route(
    alignment_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    signing_key: bytes = Depends(get_flight_signing_key),
    data_plane_url: str = Depends(get_data_plane_url),
    _scope: Principal = Depends(require_scope(Scope.ALIGNMENT_DEFINITION_DELETE)),
) -> AlignmentDefinitionDeleteResponse:
    """Fully purge an alignment — its DuckLake `alignment` rows then its Postgres
    `alignment_definition` row. system_admin only (`alignment_definition:delete`).

    This is the disallow-without-delete escape hatch: the align planner refuses to
    re-plan any sample already carrying an `alignment_sample` gate for its resolved
    alignment, so re-aligning a config requires purging it first. Deleting the
    `alignment_definition` row CASCADE-deletes its `alignment_sample` gate rows (so
    a fresh align plan re-creates them PENDING) and detaches any referencing
    `work_ticket` via the `work_ticket.alignment_idx` `ON DELETE SET NULL` FK.

    Order of operations mirrors the mask purge: existence check first (404 if
    absent) → DuckLake delete (one all-or-nothing transaction; a 502 on failure
    removes nothing yet) → Postgres `alignment_definition` delete last. This makes
    it *retriable*: both mutating steps are idempotent and the row a retry keys off
    is removed last, so a crash between them leaves at worst a recoverable
    orphan-Postgres row, never an unrecoverable orphan-lake.

    **Intentional divergence from reference-delete (a conscious decision, not an
    oversight — same as the mask purge in `routes/read_masked.py`):** this route
    deliberately does NOT gate on in-flight align block tickets. It is an
    admin-only sharp primitive; the `work_ticket.alignment_idx` FK
    (`ON DELETE SET NULL`) detaches any referencing ticket. So an operator who
    DELETEs while a covering block is still PROCESSING can strand that block: on
    completion its `register-files` writes rows under the now-deleted
    `alignment_idx` (an orphan) and `reconcile-alignment-block` then RuntimeErrors
    on the cascade-deleted `alignment_sample` gate, FAILing the ticket. The
    disallow-without-delete rule directs re-aligns here, so the operator's
    responsibility is to purge only an alignment whose blocks are all terminal
    (a bulk safety wrapper, like the mask `purge-failed` tool, can enforce that
    later if the sharp edge proves error-prone in practice)."""
    # Existence check (a read; safe before the lake delete).
    exists = await pool.fetchval(
        "SELECT 1 FROM qiita.alignment_definition WHERE alignment_idx = $1", alignment_idx
    )
    if exists is None:
        raise HTTPException(status_code=404, detail=_MSG_ALIGNMENT_NOT_FOUND)

    # DuckLake alignment rows (idempotent, atomic delete-by-alignment_idx in the
    # data plane). Lake-first so a crash before the Postgres delete leaves a
    # recoverable orphan-Postgres row, not an unrecoverable orphan-lake.
    try:
        rows_deleted = await delete_alignment_data(
            alignment_idx=alignment_idx,
            signing_key=signing_key,
            data_plane_url=data_plane_url,
        )
    except _flight.FlightError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"data plane alignment delete failed; nothing removed yet: {exc}",
        ) from exc

    # Postgres row last. The alignment_sample gate CASCADEs and the work_ticket FK
    # is ON DELETE SET NULL, so referencing rows detach automatically.
    await pool.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1", alignment_idx
    )

    return AlignmentDefinitionDeleteResponse(alignment_idx=alignment_idx, rows_deleted=rows_deleted)


@alignment_router.post(PATH_ALIGNMENT_DOGET, status_code=201)
async def create_alignment_doget_ticket(
    body: AlignmentDoGetTicketRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    signing_key: bytes = Depends(get_flight_signing_key),
    _sa: ServiceAccount = Depends(require_service_with_scope(Scope.TICKET_DOGET)),
) -> DoGetTicketResponse:
    """Sign a DoGet ticket scoped to one alignment run + its explicit
    prep_sample_idx cohort on the data plane's ``alignment`` table.

    Service-account-only (``tickets:doget``) — the feature-table (OGU) compute job
    mints this at runtime (short TTL; a SLURM queue can outlive a submit-time
    ticket). The body carries only ``work_ticket_idx``; ``alignment_idx`` and the
    cohort are read from that ticket's ``action_context`` (set at plan time),
    keeping the potentially large sample list CP-side rather than on the wire.

    Fail loud: a missing ticket is 404; an ``action_context`` that is not a
    feature-table request (no positive ``alignment_idx`` / non-empty
    ``prep_sample_idx`` list of positive ints) is 422, so the data plane's
    single-``alignment_idx`` + non-empty-filter guards are never even reached with
    a bad scope — an unscoped alignment ticket is never signed here.

    Cohort completeness is NOT re-checked here. That every ``prep_sample_idx`` is a
    member of this alignment and its ``qiita.alignment_sample`` gate is
    ``'completed'`` is validated at SUBMIT time by the feature-table runner
    resolver (``runner/_feature_table.py``), which fails the submission before any
    compute runs. ``alignment_sample`` state is monotonic (pending→completed), so a
    cohort validated at submit stays valid at this runtime mint call, and the
    resolver runs unconditionally for this workflow before the job calls this
    route — so this route trusts the validated ``action_context`` cohort rather
    than re-querying the gate.

    Authorization is scope-only at this layer (matching the reference/read_masked
    doget routes): any service account holding ``tickets:doget`` can request a
    ticket; row-level visibility is not enforced here.
    """
    raw = await pool.fetchval(
        "SELECT action_context FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        body.work_ticket_idx,
    )
    if raw is None:
        raise HTTPException(status_code=404, detail="work ticket not found")
    # asyncpg hands JSONB back as str under the default codec (or a dict if one is
    # registered) — accept both, mirroring the context read in routes/reference.py.
    ctx = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(ctx, dict):
        raise HTTPException(
            status_code=422, detail="work ticket action_context is not a feature-table request"
        )
    # Same scope contract the runner resolver validates at submit — one shared rule
    # (parse_feature_table_scope), each boundary translating to its own error type.
    try:
        alignment_idx, prep_sample_idx = parse_feature_table_scope(ctx)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    filter_ = {"alignment_idx": [alignment_idx], "prep_sample_idx": prep_sample_idx}
    ticket_bytes = sign_ticket(table=_ALIGNMENT_TABLE, filter=filter_, secret=signing_key)
    return DoGetTicketResponse(ticket=base64.b64encode(ticket_bytes).decode())
