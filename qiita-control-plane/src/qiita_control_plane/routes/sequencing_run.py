"""Sequencing-run routes.

Holds the two POST handlers that load-code uses to land a sequencing
run and its lanes: POST /sequencing-run and POST
/sequencing-run/{idx}/sequenced-pool. Both write handlers gate on caller
scope (Scope.PREP_SAMPLE_WRITE) plus require_complete_profile
(humans-only). The run POST has no system_role gate (any USER may stand
up a run); the pool POST additionally gates on caller-creator semantics
against the path's run via `require_caller_owns_run()` (wet_lab_admin+
bypass). Both delegate their DB work to the repositories.sequencing_run
module. The per-item sequenced-sample import composer, the run-scoped
sequenced_sample bulk-id read, and the single-sequenced-sample
read/PATCH live in the sibling sequenced_sample route module.
"""

from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from pydantic import Field
from qiita_common.api_paths import (
    PATH_SEQUENCING_RUN_PREFIX,
    PATH_SEQUENCING_RUN_ROOT,
    PATH_SEQUENCING_RUN_SEQUENCED_POOL,
)
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    SequencedPoolCreateRequest,
    SequencedPoolCreateResponse,
    SequencingRunCreateRequest,
    SequencingRunCreateResponse,
)

from ..auth.guards import (
    require_caller_owns_run,
    require_complete_profile,
    require_scope,
    require_sequencing_run_exists,
)
from ..auth.principal import HumanUser, Principal
from ..deps import TxConnFactory, get_tx_conn_factory
from ..repositories.sequencing_run import insert_sequenced_pool, insert_sequencing_run
from ._helpers import GENERIC_FK_VIOLATION

router = APIRouter(prefix=PATH_SEQUENCING_RUN_PREFIX, tags=["sequencing-run"])


# Map of constraint names insert_sequencing_run can trip. Unknown names fall
# back to the generic string on the matching exception path.
_UNIQUE_VIOLATION_MESSAGES: dict[str, str] = {
    "sequencing_run_instrument_run_id_unique": "instrument_run_id already in use",
}
_GENERIC_UNIQUE_VIOLATION = "conflicts with an existing sequencing_run"


@router.post(PATH_SEQUENCING_RUN_ROOT, status_code=201)
async def create_sequencing_run(
    body: SequencingRunCreateRequest,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    user: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_WRITE)),
) -> SequencingRunCreateResponse:
    """Create a sequencing_run row.

    The caller must be a HumanUser with profile_complete=True and must
    hold the prep_sample:write scope. No system_role gate: a run is a
    bench-side container with no per-resource ownership constraints to
    inherit, so any USER may stand one up. Downstream pool / sample
    routes gate on caller-creator semantics against this run.
    """
    async with tx() as conn:
        # Single INSERT inside the transaction so the audit / retention
        # trigger surface remains the same as every other write route.
        try:
            sequencing_run_idx = await insert_sequencing_run(
                conn,
                instrument_run_id=body.instrument_run_id,
                platform=body.platform,
                created_by_idx=user.principal_idx,
                instrument_model=body.instrument_model,
                instrument_serial=body.instrument_serial,
                run_performed_at=body.run_performed_at,
                extra_metadata=body.extra_metadata,
            )
        except asyncpg.UniqueViolationError as exc:
            detail = _UNIQUE_VIOLATION_MESSAGES.get(exc.constraint_name, _GENERIC_UNIQUE_VIOLATION)
            raise HTTPException(status_code=409, detail=detail)

    return SequencingRunCreateResponse(sequencing_run_idx=sequencing_run_idx)


@router.post(PATH_SEQUENCING_RUN_SEQUENCED_POOL, status_code=201)
async def create_sequenced_pool(
    sequencing_run_idx: Annotated[int, Field(gt=0)],
    body: SequencedPoolCreateRequest,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    user: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_WRITE)),
    _run_exists: None = Depends(require_sequencing_run_exists),
    _owns_run: None = Depends(require_caller_owns_run()),
) -> SequencedPoolCreateResponse:
    """Create a sequenced_pool attached to the path's sequencing_run.

    `require_sequencing_run_exists` fires the 404 before the transaction
    opens. `require_caller_owns_run()` then gates on the caller being
    the creator of the path's `sequencing_run` (wet_lab_admin or higher
    bypass via the guard's default `bypass_role`). The run preflight is
    optional: when present, the body's run_preflight_blob is base64-decoded
    by Pydantic and the route stores the raw bytes in the BYTEA column;
    the model rejects a half-populated (blob, filename) pair before this
    handler runs.
    """
    async with tx() as conn:
        try:
            sequenced_pool_idx = await insert_sequenced_pool(
                conn,
                sequencing_run_idx=sequencing_run_idx,
                run_preflight_blob=body.run_preflight_blob,
                run_preflight_filename=body.run_preflight_filename,
                created_by_idx=user.principal_idx,
                extra_metadata=body.extra_metadata,
            )
        except asyncpg.ForeignKeyViolationError:
            # The require_sequencing_run_exists guard above rules this out
            # for the sequencing_run_idx column; a TOCTOU race (run deleted
            # between the guard and the INSERT) lands here and surfaces as 422.
            raise HTTPException(status_code=422, detail=GENERIC_FK_VIOLATION)

    return SequencedPoolCreateResponse(sequenced_pool_idx=sequenced_pool_idx)
