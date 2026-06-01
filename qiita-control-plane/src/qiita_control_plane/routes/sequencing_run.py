"""Sequencing-run routes.

Holds three handlers: POST /sequencing-run and POST
/sequencing-run/{idx}/sequenced-pool (the two mint routes the bcl-convert
submission flow chains through), plus the SA-only
GET /sequencing-run/{R}/sequenced-pool/{P}/preflight the bcl-convert
prep step calls to materialize the sample sheet.

The two write handlers gate on caller scope (Scope.PREP_SAMPLE_WRITE)
plus require_complete_profile (humans-only). The run POST has no
system_role gate (any USER may stand up a run); the pool POST
additionally gates on caller-creator semantics against the path's run
via `require_caller_owns_run()` (wet_lab_admin+ bypass). Both
write handlers are find-or-create on their natural keys
(instrument_run_id for the run; (run_idx, run_preflight_filename) for the
pool) — a same-key + same-payload retry returns HTTP 200 with the
existing idx; a same-key + different-payload retry returns 409 with a
structured PayloadMismatch detail — a soft API-contract change downstream
clients should be aware of.

The preflight GET is SA-only via Scope.SEQUENCED_POOL_PREFLIGHT_READ,
matching the existing CO→CP precedent (routes/sequence_range.py).

All three delegate their DB work to the repositories.sequencing_run
module. The per-item sequenced-sample import composer, the run-scoped
sequenced_sample bulk-id read, and the single-sequenced-sample
read/PATCH live in the sibling sequenced_sample route module.
"""

import base64
from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import Field
from qiita_common.api_paths import (
    PATH_SEQUENCED_POOL_PREFLIGHT,
    PATH_SEQUENCING_RUN_PREFIX,
    PATH_SEQUENCING_RUN_ROOT,
    PATH_SEQUENCING_RUN_SEQUENCED_POOL,
)
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    SequencedPoolCreateRequest,
    SequencedPoolCreateResponse,
    SequencedPoolPreflightResponse,
    SequencingRunCreateRequest,
    SequencingRunCreateResponse,
)

from ..auth.guards import (
    require_caller_owns_run,
    require_complete_profile,
    require_scope,
    require_sequenced_pool_in_run,
    require_sequencing_run_exists,
    require_service_with_scope,
)
from ..auth.principal import HumanUser, Principal, ServiceAccount
from ..deps import TxConnFactory, get_db_pool, get_tx_conn_factory
from ..repositories.sequencing_run import (
    PayloadMismatch,
    fetch_sequenced_pool_preflight,
    insert_sequenced_pool,
    insert_sequencing_run,
)
from ._helpers import GENERIC_FK_VIOLATION

router = APIRouter(prefix=PATH_SEQUENCING_RUN_PREFIX, tags=["sequencing-run"])


@router.post(PATH_SEQUENCING_RUN_ROOT, status_code=201)
async def create_sequencing_run(
    body: SequencingRunCreateRequest,
    response: Response,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    user: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_WRITE)),
) -> SequencingRunCreateResponse:
    """Create-or-reuse a sequencing_run row keyed on instrument_run_id.

    The caller must be a HumanUser with profile_complete=True and must
    hold the prep_sample:write scope. No system_role gate: a run is a
    bench-side container with no per-resource ownership constraints to
    inherit, so any USER may stand one up. Downstream pool / sample
    routes gate on caller-creator semantics against this run.

    Returns 201 on create and 200 on reuse (the bundled CLI relies on
    this idempotency to be retry-safe across the three POSTs the
    submit-bcl-convert flow chains together). 409 when an existing row
    matches the instrument_run_id but a supplied non-None field disagrees
    with the stored value — detail names the conflicting field.
    """
    async with tx() as conn:
        try:
            sequencing_run_idx, created = await insert_sequencing_run(
                conn,
                instrument_run_id=body.instrument_run_id,
                platform=body.platform,
                created_by_idx=user.principal_idx,
                instrument_model=body.instrument_model,
                instrument_serial=body.instrument_serial,
                run_performed_at=body.run_performed_at,
                extra_metadata=body.extra_metadata,
            )
        except PayloadMismatch as exc:
            raise HTTPException(
                status_code=409,
                detail={
                    "conflicting_field": exc.field,
                    "existing_value": exc.existing_value,
                    "supplied_value": exc.supplied_value,
                },
            )

    if not created:
        response.status_code = status.HTTP_200_OK
    return SequencingRunCreateResponse(sequencing_run_idx=sequencing_run_idx)


@router.post(PATH_SEQUENCING_RUN_SEQUENCED_POOL, status_code=201)
async def create_sequenced_pool(
    sequencing_run_idx: Annotated[int, Field(gt=0)],
    body: SequencedPoolCreateRequest,
    response: Response,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    user: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_WRITE)),
    _run_exists: None = Depends(require_sequencing_run_exists),
    _owns_run: None = Depends(require_caller_owns_run()),
) -> SequencedPoolCreateResponse:
    """Create-or-reuse a sequenced_pool attached to the path's sequencing_run.

    `require_sequencing_run_exists` fires the 404 before the transaction
    opens. `require_caller_owns_run()` then gates on the caller being
    the creator of the path's `sequencing_run` (wet_lab_admin or higher
    bypass via the guard's default `bypass_role`). The run preflight is
    optional: when present, the body's run_preflight_blob is base64-decoded
    by Pydantic and the route stores the raw bytes in the BYTEA column;
    the model rejects a half-populated (blob, filename) pair before this
    handler runs.

    Idempotency is keyed on ``(sequencing_run_idx, run_preflight_filename)``
    via the ``sequenced_pool_one_per_run_and_filename`` partial unique
    index. Returns 201 on create, 200 on reuse, 409 on a same-key + blob
    bytes mismatch. The no-preflight case (both blob and filename NULL)
    is outside the partial index and always returns 201.
    """
    async with tx() as conn:
        try:
            sequenced_pool_idx, created = await insert_sequenced_pool(
                conn,
                sequencing_run_idx=sequencing_run_idx,
                run_preflight_blob=body.run_preflight_blob,
                run_preflight_filename=body.run_preflight_filename,
                created_by_idx=user.principal_idx,
                extra_metadata=body.extra_metadata,
            )
        except PayloadMismatch as exc:
            raise HTTPException(
                status_code=409,
                detail={
                    "conflicting_field": exc.field,
                    "existing_value": exc.existing_value,
                    "supplied_value": exc.supplied_value,
                },
            )
        except asyncpg.ForeignKeyViolationError:
            # The require_sequencing_run_exists guard above rules this out
            # for the sequencing_run_idx column; a TOCTOU race (run deleted
            # between the guard and the INSERT) lands here and surfaces as 422.
            raise HTTPException(status_code=422, detail=GENERIC_FK_VIOLATION)

    if not created:
        response.status_code = status.HTTP_200_OK
    return SequencedPoolCreateResponse(sequenced_pool_idx=sequenced_pool_idx)


@router.get(PATH_SEQUENCED_POOL_PREFLIGHT)
async def get_sequenced_pool_preflight(
    sequencing_run_idx: Annotated[int, Field(gt=0)],
    sequenced_pool_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    _sa: ServiceAccount = Depends(require_service_with_scope(Scope.SEQUENCED_POOL_PREFLIGHT_READ)),
    _run_exists: None = Depends(require_sequencing_run_exists),
    _pool_in_run: None = Depends(require_sequenced_pool_in_run),
) -> SequencedPoolPreflightResponse:
    """Read the (blob, filename) pair for a sequenced_pool. SA-only.

    Called by the bcl-convert prep step (qiita_compute_orchestrator/
    jobs/bcl_convert_prep.py) to materialize the sample sheet from
    the pool's run_preflight SQLite blob.

    Returns 404 when the pool exists but has no preflight populated
    (separate from 'pool not found' / 'pool not in run', which the
    guards above resolve).
    """
    row = await fetch_sequenced_pool_preflight(
        pool,
        sequencing_run_idx=sequencing_run_idx,
        sequenced_pool_idx=sequenced_pool_idx,
    )
    # The guards have already eliminated the pool-doesn't-exist / pool-
    # in-wrong-run cases; the only remaining None path is "row exists but
    # blob and filename are both NULL". Surface that as a distinct 404.
    if row is None or row["run_preflight_blob"] is None or row["run_preflight_filename"] is None:
        raise HTTPException(
            status_code=404,
            detail=(f"sequenced_pool {sequenced_pool_idx} has no preflight populated"),
        )
    # SequencedPoolPreflightResponse declares `run_preflight_blob` as
    # Pydantic's `Base64Bytes`; the validator treats input bytes as already
    # base64-encoded and decodes them on construction (the matching encoder
    # produces base64 on serialise). The DB column holds the raw blob, so
    # we base64-encode here before construction. Skipping this would either
    # raise (when the blob contains non-base64 bytes) or silently corrupt
    # the value (when the blob happens to look like valid base64).
    return SequencedPoolPreflightResponse(
        run_preflight_blob=base64.b64encode(bytes(row["run_preflight_blob"])),
        run_preflight_filename=row["run_preflight_filename"],
    )
