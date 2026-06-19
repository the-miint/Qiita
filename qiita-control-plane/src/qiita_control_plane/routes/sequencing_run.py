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
import json
from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import Field
from qiita_common.api_paths import (
    PATH_SEQUENCED_POOL_BY_IDX,
    PATH_SEQUENCED_POOL_PREFLIGHT,
    PATH_SEQUENCING_RUN_BY_IDX,
    PATH_SEQUENCING_RUN_PREFIX,
    PATH_SEQUENCING_RUN_ROOT,
    PATH_SEQUENCING_RUN_SEQUENCED_POOL,
)
from qiita_common.auth_constants import Scope, SystemRole
from qiita_common.models import (
    SequencedPoolCreateRequest,
    SequencedPoolCreateResponse,
    SequencedPoolDeleteResponse,
    SequencedPoolPreflightResponse,
    SequencingRunCreateRequest,
    SequencingRunCreateResponse,
    SequencingRunResponse,
)

from ..actions.sequenced_pool import (
    SequencedPoolDeleteBlocked,
    SequencedPoolNotFound,
    assert_sequenced_pool_deletable,
    delete_sequenced_pool_cascade,
)
from ..auth.guards import (
    require_caller_owns_run,
    require_complete_profile,
    require_human,
    require_role_at_least,
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
    fetch_sequencing_run,
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


@router.get(PATH_SEQUENCING_RUN_BY_IDX)
async def get_sequencing_run(
    sequencing_run_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    _user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
    _role: Principal = Depends(require_role_at_least(SystemRole.WET_LAB_ADMIN)),
) -> SequencingRunResponse:
    """Read one sequencing_run's metadata by idx.

    Returns the run's caller-visible columns (see `fetch_sequencing_run`),
    notably `instrument_model` — the field `qiita submit-host-filter-pool` reads
    to forward QC's polyG gate per sample. Same read gate as the pool roster
    route (`list_sequenced_samples_in_pool`): a HumanUser with
    `Scope.PREP_SAMPLE_READ` and system_role at least wet_lab_admin. 404 when the
    run does not exist.
    """
    row = await fetch_sequencing_run(pool, sequencing_run_idx)
    if row is None:
        raise HTTPException(
            status_code=404, detail=f"sequencing_run {sequencing_run_idx} not found"
        )
    data = dict(row)
    data["sequencing_run_idx"] = data.pop("idx")
    # asyncpg returns JSONB as text (no codec registered); decode so the response
    # carries an object, matching the study GET route's handling.
    if isinstance(data["extra_metadata"], str):
        data["extra_metadata"] = json.loads(data["extra_metadata"])
    return SequencingRunResponse.model_validate(data)


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


@router.delete(PATH_SEQUENCED_POOL_BY_IDX)
async def delete_sequenced_pool(
    sequencing_run_idx: Annotated[int, Field(gt=0)],
    sequenced_pool_idx: Annotated[int, Field(gt=0)],
    force: bool = False,
    pool: asyncpg.Pool = Depends(get_db_pool),
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    _scope: Principal = Depends(require_scope(Scope.SEQUENCED_POOL_DELETE)),
    _run_exists: None = Depends(require_sequencing_run_exists),
    _pool_in_run: None = Depends(require_sequenced_pool_in_run),
) -> SequencedPoolDeleteResponse:
    """Fully purge a sequenced_pool — the pool row plus every sequenced_sample /
    prep_sample under it, their metadata, study links, and pool-/sample-scoped
    work tickets. system_admin only (`sequenced_pool:delete`). Mirrors
    DELETE /reference.

    What survives, by design: the parent `sequencing_run` (it may hold other
    pools) and the underlying `biosample` rows (a biosample is a physical
    sample shared across studies, not pool-owned — the cascade stops at
    prep_sample). Because sequenced_sample↔prep_sample is 1:1 and each
    sequenced_sample belongs to one pool, the deleted prep_samples are
    exclusive to this pool. They are removed outright, which severs **every**
    study link they hold — not only the run/pool the operator is thinking of.

    Gating: in-flight work tickets (pending/queued/processing) block the delete
    unconditionally (409). Completed/failed work tickets, prep_samples published
    into a study, and samples carrying an ENA accession each block it unless
    `force=true`.

    The data-plane DuckLake purge is a no-op today (no processing-result tables
    keyed by prep_sample/processing_idx exist yet); when they land, issue the
    DoAction purge here — mirroring `delete_reference_data` — before the
    Postgres teardown.
    """
    # `require_sequenced_pool_in_run` already fronted existence (404) and
    # parent-run consistency (422), so the SequencedPoolNotFound arm here is
    # belt-and-suspenders for the precheck — kept because the action owns its
    # own existence contract and the in-tx re-gate below CAN legitimately hit
    # it (a concurrent delete between the guard and the teardown).
    try:
        await assert_sequenced_pool_deletable(pool, sequenced_pool_idx, force=force)
    except SequencedPoolNotFound:
        raise HTTPException(
            status_code=404, detail=f"sequenced_pool {sequenced_pool_idx} not found"
        )
    except SequencedPoolDeleteBlocked as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    # Re-gate inside the teardown transaction to close the precheck→cascade
    # window: a work ticket that went in-flight since the precheck must abort
    # the teardown (and 409 loudly) rather than be silently deleted. force=True
    # here means only a *new in-flight* ticket aborts — terminal tickets,
    # published links, and ENA samples are the cascade's to delete.
    async with tx() as conn:
        try:
            await assert_sequenced_pool_deletable(conn, sequenced_pool_idx, force=True)
        except SequencedPoolNotFound:
            raise HTTPException(
                status_code=404, detail=f"sequenced_pool {sequenced_pool_idx} not found"
            )
        except SequencedPoolDeleteBlocked as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        counts = await delete_sequenced_pool_cascade(conn, sequenced_pool_idx)

    return SequencedPoolDeleteResponse(sequenced_pool_idx=sequenced_pool_idx, **counts)
