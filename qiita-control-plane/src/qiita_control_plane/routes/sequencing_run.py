"""Sequencing-run routes.

Covers the run + sequenced-pool mint routes the bcl-convert submission
flow chains through (POST /sequencing-run, POST
/sequencing-run/{idx}/sequenced-pool), the run reads (GET
/sequencing-run/{idx} and the bulk instrument_run_id → idx lookup), and
the SA-only GET /sequencing-run/{R}/sequenced-pool/{P}/preflight the
bcl-convert prep step calls to materialize the sample sheet.

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

Every handler delegates its DB work to the repositories.sequencing_run
module. The per-item sequenced-sample import composer, the run-scoped
sequenced_sample bulk-id read, and the single-sequenced-sample
read/PATCH live in the sibling sequenced_sample route module.
"""

import base64
import json
import tempfile
from pathlib import Path
from typing import Annotated, Any

import asyncpg
import pyarrow.flight as _flight
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import Field
from qiita_common.api_paths import (
    PATH_SEQUENCED_POOL_BLOCK_MASK_PLAN,
    PATH_SEQUENCED_POOL_BY_IDX,
    PATH_SEQUENCED_POOL_COMPLETION,
    PATH_SEQUENCED_POOL_PREFLIGHT,
    PATH_SEQUENCED_POOL_PREFLIGHT_UPDATE_LANE,
    PATH_SEQUENCED_POOL_QC_REPORT,
    PATH_SEQUENCING_RUN_BY_IDX,
    PATH_SEQUENCING_RUN_LOOKUP_BY_INSTRUMENT_RUN_ID,
    PATH_SEQUENCING_RUN_PREFIX,
    PATH_SEQUENCING_RUN_ROOT,
    PATH_SEQUENCING_RUN_SEQUENCED_POOL,
)
from qiita_common.auth_constants import Scope, SystemRole
from qiita_common.models import (
    BlockMaskPlanRequest,
    BlockMaskPlanResponse,
    PoolCompletionStatus,
    PoolQCReport,
    PoolReadMetrics,
    SampleQCReport,
    SequencedPoolCreateRequest,
    SequencedPoolCreateResponse,
    SequencedPoolDeleteResponse,
    SequencedPoolPreflightResponse,
    SequencedPoolPreflightUpdateLaneRequest,
    SequencedPoolPreflightUpdateLaneResponse,
    SequencedPoolResponse,
    SequencingRunCreateRequest,
    SequencingRunCreateResponse,
    SequencingRunLookupByInstrumentRunIdRequest,
    SequencingRunLookupByInstrumentRunIdResponse,
    SequencingRunResponse,
    merge_qc_reports,
)

from .. import block_planner
from ..actions.library import delete_pool_reads_data
from ..actions.sequenced_pool import (
    PreflightNotEditable,
    SequencedPoolDeleteBlocked,
    SequencedPoolNotFound,
    assert_pool_preflight_editable,
    assert_sequenced_pool_deletable,
    delete_sequenced_pool_cascade,
    invalidate_completed_steps_for_sequenced_pool,
    reap_staged_reads,
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
from ..deps import (
    TxConnFactory,
    get_data_plane_url,
    get_db_pool,
    get_hmac_secret,
    get_scratch_staging,
    get_tx_conn_factory,
)
from ..repositories.sequencing_run import (
    PayloadMismatch,
    fetch_sequenced_pool_completion,
    fetch_sequenced_pool_demux_state,
    fetch_sequenced_pool_preflight,
    fetch_sequenced_pool_read_metrics,
    fetch_sequenced_pool_sample_qc_reports,
    fetch_sequencing_run,
    fetch_sequencing_run_idxs_by_instrument_run_id,
    insert_sequenced_pool,
    insert_sequencing_run,
    update_sequenced_pool_preflight_blob,
)
from ._helpers import GENERIC_FK_VIOLATION, resolve_idxs_by_natural_key

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

    Idempotency is keyed on the preflight *content* —
    ``(sequencing_run_idx, run_preflight_sha256)`` via the
    ``sequenced_pool_one_per_run_and_hash`` partial unique index — so the same
    preflight bytes re-uploaded under any filename resolve to the same pool.
    Returns 201 on create, 200 on reuse, 409 on a same-content +
    extra_metadata mismatch, and 409 on a different-content upload that reuses
    an existing filename in the run (the filename index is an independent,
    permanent uniqueness rule — distinct pools must differ in both content and
    filename). The no-preflight case (both blob and filename NULL) is outside
    both partial indexes and always returns 201.
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


class _LaneUpdateRejected(Exception):
    """run_preflight.update_lane rejected the request — an unsupported platform, a
    post-update NULL/non-NULL lane mix, or a unique ``(prepped_sample, lane)``
    collision. A client-error condition (the route maps it to 422), kept distinct
    from a ``ValueError`` raised by ``open_db_file`` (a server-side preflight
    schema-version skew), which must surface as 5xx rather than 422."""


def _apply_preflight_lane_update(
    blob: bytes,
    *,
    platform: str,
    from_lane: int | None,
    to_lane: int | None,
    reason: str,
) -> tuple[bytes, int]:
    """Apply ``run_preflight.update_lane`` to a preflight SQLite blob, returning
    the edited bytes and the number of sample rows reassigned.

    The blob is materialized to a private temp file because run_preflight
    operates on a file-backed sqlite3 connection and commits the lane update in
    place; the edited bytes are then read back. The ``run_preflight`` import is
    lazy and local — matching ``jobs/bcl_convert_prep.py`` — so the git-pinned
    dependency only loads on the rare edit path, never at module import.
    ``open_db_file`` also applies any pending preflight-schema patches, which can
    legitimately rewrite bytes even on a zero-row update; that is intended (it
    keeps a stored preflight current).

    Only update_lane's own ``ValueError`` (bad request) is translated to
    ``_LaneUpdateRejected``; a ``ValueError`` from ``open_db_file`` (e.g. a stored
    blob whose schema version exceeds the deployed run_preflight patch set — a
    server/version-skew condition, not a bad request) is deliberately left to
    propagate so the route returns 5xx rather than mislabeling it 422."""
    from run_preflight import open_db_file, update_lane  # noqa: PLC0415

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "preflight.db"
        db_path.write_bytes(blob)
        conn = open_db_file(str(db_path))
        try:
            rows_updated = update_lane(conn, platform, from_lane, to_lane, reason)
        except ValueError as exc:
            raise _LaneUpdateRejected(str(exc)) from exc
        finally:
            conn.close()
        return db_path.read_bytes(), rows_updated


@router.post(PATH_SEQUENCED_POOL_PREFLIGHT_UPDATE_LANE)
async def update_sequenced_pool_preflight_lane(
    sequencing_run_idx: Annotated[int, Field(gt=0)],
    sequenced_pool_idx: Annotated[int, Field(gt=0)],
    body: SequencedPoolPreflightUpdateLaneRequest,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    _user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_WRITE)),
    _role: Principal = Depends(require_role_at_least(SystemRole.WET_LAB_ADMIN)),
    _run_exists: None = Depends(require_sequencing_run_exists),
    _pool_in_run: None = Depends(require_sequenced_pool_in_run),
) -> SequencedPoolPreflightUpdateLaneResponse:
    """Bulk-reassign the lane on a pool's run-preflight SQLite blob. wet_lab_admin+.

    A POST action (not PATCH): the preflight is not human-readable, so there is no
    ETag for an If-Match optimistic-concurrency PATCH; the not-processed gate plus
    the in-transaction re-check below provide the safety instead. Delegates the
    actual SQLite edit to `run_preflight.update_lane` via `_apply_preflight_lane_update`,
    moving every platform-sample row at `from_lane` to `to_lane` and writing one
    SQLite `change_log` row per reassigned sample carrying the caller's `reason`.

    Auth mirrors the run/pool read routes: a HumanUser with `Scope.PREP_SAMPLE_WRITE`
    and system_role at least wet_lab_admin (no per-creator ownership — a wet-lab
    admin may correct any run's preflight). `require_sequencing_run_exists` /
    `require_sequenced_pool_in_run` front 404 (no such run/pool) and 422 (pool not
    under this run) before the body work.

    Everything below runs in one transaction (the delete route's
    re-gate-inside-the-txn contract): `assert_pool_preflight_editable` re-checks
    against committed state and 409s if the run has been processed (any in-flight
    or completed work ticket on the pool or its samples — a failed or unsubmitted
    run stays editable). This re-check closes the precheck→write window and rejects
    anything committed before it runs, but under the project-default READ COMMITTED
    isolation it does NOT, on its own, serialize against a bcl-convert submission
    whose work_ticket commits *after* this SELECT but before our COMMIT — that
    narrow edit-vs-submit race is the same residual one the delete path carries; a
    general fix is tracked separately. 404 when the pool carries no preflight. 422
    when update_lane rejects the request (bad platform / lane-uniformity violation /
    unique-index collision); a run_preflight schema-version error is left to surface
    as 5xx, not 422."""
    async with tx() as conn:
        try:
            await assert_pool_preflight_editable(conn, sequenced_pool_idx)
        except SequencedPoolNotFound:
            raise HTTPException(
                status_code=404, detail=f"sequenced_pool {sequenced_pool_idx} not found"
            )
        except PreflightNotEditable as exc:
            raise HTTPException(status_code=409, detail=str(exc))

        # Run-scoped read: re-confirms membership and pulls the blob to edit.
        row = await fetch_sequenced_pool_preflight(
            conn,
            sequencing_run_idx=sequencing_run_idx,
            sequenced_pool_idx=sequenced_pool_idx,
        )
        if row is None or row["run_preflight_blob"] is None:
            raise HTTPException(
                status_code=404,
                detail=f"sequenced_pool {sequenced_pool_idx} has no preflight populated",
            )

        try:
            new_blob, rows_updated = _apply_preflight_lane_update(
                bytes(row["run_preflight_blob"]),
                platform=body.platform,
                from_lane=body.from_lane,
                to_lane=body.to_lane,
                reason=body.reason,
            )
        except _LaneUpdateRejected as exc:
            raise HTTPException(status_code=422, detail=str(exc))

        await update_sequenced_pool_preflight_blob(
            conn, sequenced_pool_idx=sequenced_pool_idx, new_blob=new_blob
        )

        # The edit makes any samplesheet a prior bcl_convert_prep produced stale,
        # so drop the pool's COMPLETED step rows in the same transaction: a later
        # `ticket run` redrive must re-run prep against the corrected blob instead
        # of fast-forwarding it. The edit gate above guarantees no pool ticket is
        # in-flight/completed here, so this only ever touches failed-ticket rows.
        await invalidate_completed_steps_for_sequenced_pool(
            conn, sequenced_pool_idx=sequenced_pool_idx
        )

    return SequencedPoolPreflightUpdateLaneResponse(
        sequenced_pool_idx=sequenced_pool_idx, rows_updated=rows_updated
    )


@router.get(PATH_SEQUENCED_POOL_BY_IDX)
async def get_sequenced_pool(
    sequencing_run_idx: Annotated[int, Field(gt=0)],
    sequenced_pool_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    _user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
    _role: Principal = Depends(require_role_at_least(SystemRole.WET_LAB_ADMIN)),
    _pool_in_run: None = Depends(require_sequenced_pool_in_run),
) -> SequencedPoolResponse:
    """Read one sequenced_pool's metadata plus its compute-on-read read-metric
    rollup: per-stage read-count SUMS over the pool's non-retired
    sequenced_samples, with the passing fraction recomputed from the sums (never
    a mean of per-sample fractions) and total / with-metrics sample counts.

    Nothing is stored at the pool level — the rollup is aggregated at request
    time, so it never drifts when a sample is re-processed or deleted. Same read
    gate as `get_sequencing_run` / the pool roster: a HumanUser with
    `Scope.PREP_SAMPLE_READ` and system_role at least wet_lab_admin.
    `require_sequenced_pool_in_run` fronts 404 (no such pool) and 422 (pool not
    under this run); the rollup is always present (an unprocessed pool reads as
    NULL sums / 0 counts). The BYTEA `run_preflight_blob` is not surfaced — only
    its filename.
    """
    row = await fetch_sequenced_pool_read_metrics(pool, sequenced_pool_idx)
    # require_sequenced_pool_in_run already 404'd a missing pool, so the fetch
    # returns the now-guaranteed row; guard belt-and-suspenders against a
    # concurrent delete between the dependency and this read.
    if row is None:
        raise HTTPException(
            status_code=404, detail=f"sequenced_pool {sequenced_pool_idx} not found"
        )
    # Shape via model_validate(dict(row)), like get_sequencing_run: rename idx and
    # pop the aggregate columns into the nested read_metrics, leaving exactly the
    # top-level fields. asyncpg returns JSONB as text (no codec); decode it so the
    # response carries an object.
    data = dict(row)
    data["sequenced_pool_idx"] = data.pop("idx")
    if isinstance(data["extra_metadata"], str):
        data["extra_metadata"] = json.loads(data["extra_metadata"])
    data["read_metrics"] = PoolReadMetrics(
        raw_read_count_r1r2=data.pop("raw_read_count_r1r2"),
        biological_read_count_r1r2=data.pop("biological_read_count_r1r2"),
        quality_filtered_read_count_r1r2=data.pop("quality_filtered_read_count_r1r2"),
        sample_count=data.pop("sample_count"),
        samples_with_metrics=data.pop("samples_with_metrics"),
    )
    return SequencedPoolResponse.model_validate(data)


@router.get(PATH_SEQUENCED_POOL_QC_REPORT)
async def get_sequenced_pool_qc_report(
    sequencing_run_idx: Annotated[int, Field(gt=0)],
    sequenced_pool_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    _user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
    _role: Principal = Depends(require_role_at_least(SystemRole.WET_LAB_ADMIN)),
    _pool_in_run: None = Depends(require_sequenced_pool_in_run),
) -> PoolQCReport:
    """Read the pool's merged (multiqc-equivalent) QC report: the read-metric
    rollup (same SUMs as the pool metadata endpoint), every non-retired sample's
    persisted raw/filtered qc_report, and the run-level `merged` aggregate over
    them (per-mate histograms summed, means base/read-weighted).

    Everything is compute-on-read — the merge runs at request time over the
    constituent sequenced_samples, so it never drifts when a sample is
    re-processed or deleted. Same read gate as the pool metadata endpoint: a
    HumanUser with `Scope.PREP_SAMPLE_READ` and system_role at least
    wet_lab_admin. `require_sequenced_pool_in_run` fronts 404 (no such pool) /
    422 (pool not under this run). A pool with no processed samples reads as an
    empty `samples` list and `merged.raw`/`merged.filtered` of None."""
    # Two sequential reads (rollup, then per-sample rows) on separate
    # acquisitions: a concurrent writer between them could momentarily desync the
    # rollup's sample_count from len(samples). Acceptable for a compute-on-read,
    # read-gated human-facing report — a transient count/list mismatch is
    # cosmetic and self-heals on the next read; not worth a serializable txn.
    rollup = await fetch_sequenced_pool_read_metrics(pool, sequenced_pool_idx)
    # require_sequenced_pool_in_run already 404'd a missing pool; guard
    # belt-and-suspenders against a concurrent delete.
    if rollup is None:
        raise HTTPException(
            status_code=404, detail=f"sequenced_pool {sequenced_pool_idx} not found"
        )
    rows = await fetch_sequenced_pool_sample_qc_reports(pool, sequenced_pool_idx)

    def _report(value: Any) -> dict[str, Any] | None:
        # asyncpg returns JSONB as text (no codec); decode to an object.
        return json.loads(value) if isinstance(value, str) else value

    samples = [
        SampleQCReport(
            prep_sample_idx=row["prep_sample_idx"],
            sequenced_pool_item_id=row["sequenced_pool_item_id"],
            raw_qc_report=_report(row["raw_qc_report"]),
            filtered_qc_report=_report(row["filtered_qc_report"]),
        )
        for row in rows
    ]
    return PoolQCReport(
        sequenced_pool_idx=sequenced_pool_idx,
        sequencing_run_idx=rollup["sequencing_run_idx"],
        sample_count=rollup["sample_count"],
        samples_with_qc_report=sum(1 for s in samples if s.raw_qc_report is not None),
        read_metrics=PoolReadMetrics(
            raw_read_count_r1r2=rollup["raw_read_count_r1r2"],
            biological_read_count_r1r2=rollup["biological_read_count_r1r2"],
            quality_filtered_read_count_r1r2=rollup["quality_filtered_read_count_r1r2"],
            sample_count=rollup["sample_count"],
            samples_with_metrics=rollup["samples_with_metrics"],
        ),
        merged=merge_qc_reports(samples),
        samples=samples,
    )


@router.get(PATH_SEQUENCED_POOL_COMPLETION)
async def get_sequenced_pool_completion(
    sequencing_run_idx: Annotated[int, Field(gt=0)],
    sequenced_pool_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    _user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
    _role: Principal = Depends(require_role_at_least(SystemRole.WET_LAB_ADMIN)),
    _pool_in_run: None = Depends(require_sequenced_pool_in_run),
) -> PoolCompletionStatus:
    """Read the pool's end-to-end processing rollup: the demux (bcl-convert)
    stage state plus the host-masking stage. `demux_state` is the pool-scoped
    bcl-convert ticket's state; the per-sample buckets classify each non-retired
    sequenced_sample by the state of its read-mask work tickets (any version) —
    completed / in-flight / no-data / failed / not-submitted — with a pool-level
    `complete` flag for host-masking (every sample COMPLETED or NO_DATA and the
    pool non-empty, so a plate of real data with empty wells still reaches
    `complete=True`) and `fully_processed` = demux completed AND `complete` (the
    single "this pool is done and clean" signal). Surfaced alongside the
    read-metric and QC rollups.

    Everything is compute-on-read over the work_ticket table, so it never drifts
    when a sample is re-processed, re-submitted, or deleted. Same read gate as the
    pool metadata / QC-report endpoints: a HumanUser with `Scope.PREP_SAMPLE_READ`
    and system_role at least wet_lab_admin. `require_sequenced_pool_in_run` fronts
    404 (no such pool) / 422 (pool not under this run); a pool with no non-retired
    samples reads as all-zero counts and `complete=False`."""
    row = await fetch_sequenced_pool_completion(pool, sequenced_pool_idx)
    demux_state = await fetch_sequenced_pool_demux_state(pool, sequenced_pool_idx)
    return PoolCompletionStatus(
        sequenced_pool_idx=sequenced_pool_idx,
        sequencing_run_idx=sequencing_run_idx,
        demux_state=demux_state,
        sample_count=row["sample_count"],
        samples_completed=row["samples_completed"],
        samples_in_flight=row["samples_in_flight"],
        samples_no_data=row["samples_no_data"],
        samples_failed=row["samples_failed"],
        samples_not_submitted=row["samples_not_submitted"],
    )


@router.post(PATH_SEQUENCED_POOL_BLOCK_MASK_PLAN, status_code=status.HTTP_202_ACCEPTED)
async def submit_block_mask_plan(
    sequencing_run_idx: Annotated[int, Field(gt=0)],
    sequenced_pool_idx: Annotated[int, Field(gt=0)],
    body: BlockMaskPlanRequest,
    request: Request,
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_WRITE)),
    _role: Principal = Depends(require_role_at_least(SystemRole.WET_LAB_ADMIN)),
    _run_exists: None = Depends(require_sequencing_run_exists),
    _pool_in_run: None = Depends(require_sequenced_pool_in_run),
    hmac_secret: bytes = Depends(get_hmac_secret),
    data_plane_url: str = Depends(get_data_plane_url),
    staging_root: Path | None = Depends(get_scratch_staging),
) -> BlockMaskPlanResponse:
    """Plan + submit the pool's bulk-block read masking in ONE call — the
    block-compute analog of the per-sample submit-host-filter-pool fan-out.

    Resolves each sample's `mask_idx` (shared identity with per-sample read-mask),
    partitions by mask, tiles each partition into fixed ~10M-read blocks, persists
    the `block`/`block_member` cover-map + a PENDING `mask_sample` gate per sample,
    creates one block work_ticket per block, and dispatches each. Returns the plan
    (blocks + tickets + partition/sample counts) with HTTP 202; a pool with
    nothing to do returns 202 with zero counts.

    Same gate as the pool read/QC endpoints — a HumanUser with
    `Scope.PREP_SAMPLE_WRITE` at system_role ≥ wet_lab_admin (host filtering / QC
    is a privileged lab operation; matches the read-mask audience).
    `require_sequenced_pool_in_run` fronts 404 (no such pool) / 422 (pool not
    under this run). Host-ref coherence (minimap2⇒rype) is validated on the model.
    """
    # Dispatch is fire-and-forget in-process; refuse if the orchestrator hop is
    # unconfigured rather than minting blocks whose tickets can never run.
    if request.app.state.compute_backend_client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="compute orchestrator not configured (COMPUTE_ORCHESTRATOR_URL unset)",
        )

    # The block workflow ships out-of-tree (qiita-admin actions sync). If it is
    # not yet registered the ticket INSERT would FK-violate mid-plan; front it
    # with a clear 503 so the operator syncs actions rather than seeing a 500.
    action_enabled = await pool.fetchval(
        "SELECT enabled FROM qiita.action WHERE action_id = $1 AND version = $2",
        block_planner.BLOCK_MASK_ACTION_ID,
        block_planner.BLOCK_MASK_ACTION_VERSION,
    )
    if action_enabled is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                f"block-mask workflow "
                f"({block_planner.BLOCK_MASK_ACTION_ID}/{block_planner.BLOCK_MASK_ACTION_VERSION})"
                " is not registered; run `qiita-admin actions sync`"
            ),
        )
    if not action_enabled:
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail=(
                f"block-mask workflow "
                f"({block_planner.BLOCK_MASK_ACTION_ID}/{block_planner.BLOCK_MASK_ACTION_VERSION})"
                " is deprecated"
            ),
        )

    # The mask identity folds in a hash of the canonical adapter set to exactly
    # match the per-sample read-mask mint (so the two collapse to one mask_idx).
    # The planner helper decides inclusion the same way the runner does — gated on
    # the read-mask workflow declaring adapter_parquet AND a default reference
    # being configured — and materializes it once (a data-plane hop) only then.
    try:
        adapter_set_hash = await block_planner.resolve_block_mask_adapter_hash(
            pool,
            default_adapter_reference_idx=request.app.state.settings.default_adapter_reference_idx,
            data_plane_url=data_plane_url,
            hmac_secret=hmac_secret,
            staging_root=staging_root,
            sequencing_run_idx=sequencing_run_idx,
            sequenced_pool_idx=sequenced_pool_idx,
        )
    except block_planner.AdapterMaterializationUnavailable as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)
        ) from exc

    try:
        summary = await block_planner.plan_and_submit_blocks(
            pool,
            app=request.app,
            sequencing_run_idx=sequencing_run_idx,
            sequenced_pool_idx=sequenced_pool_idx,
            host_rype_reference_idx=body.host_rype_reference_idx,
            host_minimap2_reference_idx=body.host_minimap2_reference_idx,
            only_missing=body.only_missing,
            adapter_set_hash=adapter_set_hash,
            originator_principal_idx=user.principal_idx,
            block_action_id=block_planner.BLOCK_MASK_ACTION_ID,
            block_action_version=block_planner.BLOCK_MASK_ACTION_VERSION,
        )
    except block_planner.BlockMaskResubmitError as exc:
        # A sample already gated for the resolved mask would be re-masked
        # (completed → read_mask double-write) or wedged (pending → duplicate
        # covering block). Mirror the pool resubmit 409: DELETE the mask or pass
        # only_missing.
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "reason": str(exc),
                "conflicting_prep_sample_idxs": exc.conflicting_prep_sample_idxs,
            },
        ) from exc
    return BlockMaskPlanResponse(**summary)


@router.delete(PATH_SEQUENCED_POOL_BY_IDX)
async def delete_sequenced_pool(
    sequencing_run_idx: Annotated[int, Field(gt=0)],
    sequenced_pool_idx: Annotated[int, Field(gt=0)],
    force: bool = False,
    pool: asyncpg.Pool = Depends(get_db_pool),
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    hmac_secret: bytes = Depends(get_hmac_secret),
    data_plane_url: str = Depends(get_data_plane_url),
    staging_root: Path | None = Depends(get_scratch_staging),
    _scope: Principal = Depends(require_scope(Scope.SEQUENCED_POOL_DELETE)),
    _run_exists: None = Depends(require_sequencing_run_exists),
    _pool_in_run: None = Depends(require_sequenced_pool_in_run),
) -> SequencedPoolDeleteResponse:
    """Fully purge a sequenced_pool — its Postgres subtree, the DuckLake `read`/
    `read_mask` rows its prep_samples produced, and the durable staged read
    copies on disk. system_admin only (`sequenced_pool:delete`). Mirrors
    DELETE /reference.

    The Postgres cascade removes the pool row plus every sequenced_sample /
    prep_sample under it, their metadata, study links, and pool-/sample-scoped
    work tickets. What survives, by design: the parent `sequencing_run` (it may
    hold other pools) and the underlying `biosample` rows (a biosample is a
    physical sample shared across studies, not pool-owned — the cascade stops at
    prep_sample). Because sequenced_sample↔prep_sample is 1:1 and each
    sequenced_sample belongs to one pool, the deleted prep_samples are
    exclusive to this pool. They are removed outright, which severs **every**
    study link they hold — not only the run/pool the operator is thinking of.

    Gating: in-flight work tickets (pending/queued/processing) block the delete
    unconditionally (409). Completed/failed work tickets, prep_samples published
    into a study, and samples carrying an ENA accession each block it unless
    `force=true`.

    The DuckLake purge (the `read`/`read_mask` rows the pool's bcl-convert run
    wrote, keyed by prep_sample_idx) runs first, then the Postgres teardown —
    same data-plane → Postgres-last ordering as DELETE /reference, chosen so the
    op is retriable: the data-plane delete is one DuckLake transaction
    (all-or-nothing, idempotent), and the `sequenced_pool` row a retry keys off
    is removed last. A transport/data-plane failure 502s with nothing removed.

    Two ordering subtleties, both narrow (system_admin-only, sub-second):
      * The DuckLake read purge precedes the in-tx re-gate, so if a ticket goes
        in-flight in the precheck→purge window, its `read` rows are gone before
        the re-gate aborts the Postgres teardown (409). That job would then fail
        for missing reads, and the DELETE keeps 409ing until it drains; a retry
        once it's terminal completes the delete (the purge is idempotent). This
        is the same class of pre-commit-destructive window reference delete
        accepts — closing it fully would need row locking we deliberately avoid
        here.
      * The staged-read reaper, by contrast, runs only *after* the Postgres
        teardown commits — so an aborted teardown never removes the durable
        `read.parquet` inputs out from under a surviving in-flight job. On the
        rare crash between commit and reap, the staged copies leak (storage
        only) and are left for a future maintenance pass.

    Reclaiming the orphaned Parquet bytes the logical DuckLake delete leaves
    behind is likewise not yet automated (same as reference).
    """
    # `require_sequenced_pool_in_run` already fronted existence (404) and
    # parent-run consistency (422), so the SequencedPoolNotFound arm here is
    # belt-and-suspenders for the precheck — kept because the action owns its
    # own existence contract and the in-tx re-gate below CAN legitimately hit
    # it (a concurrent delete between the guard and the teardown). The precheck
    # returns the pool's prep_sample set (exclusive to this pool) — the keys the
    # DuckLake purge and staged-reads reaper below operate on.
    try:
        prep_sample_idxs = await assert_sequenced_pool_deletable(
            pool, sequenced_pool_idx, force=force
        )
    except SequencedPoolNotFound:
        raise HTTPException(
            status_code=404, detail=f"sequenced_pool {sequenced_pool_idx} not found"
        )
    except SequencedPoolDeleteBlocked as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    # DuckLake data first (idempotent, atomic delete-by-prep_sample_idx in the
    # data plane). A FlightError here means nothing has been removed yet — 502
    # so the operator can re-run the idempotent DELETE.
    try:
        purge = await delete_pool_reads_data(
            prep_sample_idxs=prep_sample_idxs,
            hmac_secret=hmac_secret,
            data_plane_url=data_plane_url,
        )
    except _flight.FlightError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"data plane sequenced-pool read purge failed; nothing removed yet: {exc}",
        ) from exc

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

    # Durable per-sample staged read copies — reaped only AFTER the teardown
    # commits, so an aborted teardown (re-gate 409) never strips the staged
    # inputs from a surviving in-flight job. Best-effort; never fails the delete.
    staged_reads_reaped = reap_staged_reads(staging_root, prep_sample_idxs)

    return SequencedPoolDeleteResponse(
        sequenced_pool_idx=sequenced_pool_idx,
        read_rows_deleted=purge.get("read_rows_deleted", 0),
        read_mask_rows_deleted=purge.get("read_mask_rows_deleted", 0),
        staged_reads_reaped=staged_reads_reaped,
        **counts,
    )


@router.post(PATH_SEQUENCING_RUN_LOOKUP_BY_INSTRUMENT_RUN_ID)
async def lookup_run_by_instrument_run_id(
    body: SequencingRunLookupByInstrumentRunIdRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
) -> SequencingRunLookupByInstrumentRunIdResponse:
    """Resolve a list of instrument_run_id values to sequencing_run_idx.

    Auth: HumanUser with Scope.PREP_SAMPLE_READ. The response is only the
    (instrument_run_id, idx) mapping — no run columns — so resolution does
    not itself disclose row contents; reading a row goes through
    GET /sequencing-run/{idx}. `missing` lists input-order-deduped ids that
    did not resolve.
    """
    # user is read only to keep the dependency chain explicit — no
    # per-caller filter runs here (see auth docstring).
    _ = user
    resolved, missing = await resolve_idxs_by_natural_key(
        values=body.instrument_run_ids,
        fetcher=lambda values: fetch_sequencing_run_idxs_by_instrument_run_id(pool, values=values),
    )
    return SequencingRunLookupByInstrumentRunIdResponse(resolved=resolved, missing=missing)
