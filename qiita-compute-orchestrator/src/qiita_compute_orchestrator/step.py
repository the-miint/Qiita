"""POST /api/v1/step/* — the orchestrator's step-execution surface.

The control-plane runner drives the decoupled trio:

  * POST /step/submit  — submit the job, return a handle immediately.
  * POST /step/status  — single live-status read for a submitted handle.
  * POST /step/result  — finalize a terminal step, return verified outputs.
  * POST /step/find-by-name — look up live jobs by deterministic name, so
    the CP can adopt a job it submitted but never recorded the id for
    (the write-ahead idempotency gap).

The orchestrator is stateless across submit/status/result: the
`StepHandle` returned by submit carries everything status/result need
(job id + workspace paths), and the control plane persists those fields
so it can re-attach after a restart.

Auth is a shared bearer token loaded by `config.py` from
`/etc/qiita/cp-to-co.token` (path overridable via `CP_TO_CO_TOKEN_PATH`;
or, for dev/CI only, value via `CP_TO_CO_TOKEN` when
`QIITA_ALLOW_TOKEN_ENV=true`). Private path between two services on the
same network. Constant-time compare.
"""

from __future__ import annotations

import hmac
import logging
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from qiita_common.actions import NATIVE_MODULE_PREFIX
from qiita_common.api_paths import (
    PATH_STEP_FIND_BY_NAME,
    PATH_STEP_PLAN,
    PATH_STEP_PREFIX,
    PATH_STEP_RESULT,
    PATH_STEP_STATUS,
    PATH_STEP_SUBMIT,
)
from qiita_common.backend_failure import (
    BACKEND_FAILURE_HEADER,
    BACKEND_FAILURE_HTTP_STATUS,
    STEP_NO_DATA_HEADER,
    STEP_NO_DATA_HTTP_STATUS,
    BackendFailure,
    BackendFailureBody,
    StepNoData,
    StepNoDataBody,
)
from qiita_common.models import (
    FoundJobWire,
    StepFindByNameRequest,
    StepFindByNameResponse,
    StepHandleWire,
    StepPlanRequest,
    StepPlanResponse,
    StepResultRequest,
    StepResultResponse,
    StepStatusRequest,
    StepStatusWire,
    StepSubmitRequest,
)

from .backend import ComputeBackend, StepHandle, StepStatusInfo
from .jobs import flatten_native_inputs, run_native_job_plan

_log = logging.getLogger(__name__)

router = APIRouter(prefix=PATH_STEP_PREFIX, tags=["step"])

_bearer = HTTPBearer(auto_error=False)


def _require_cp_to_co_token(
    request: Request,
    creds: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> None:
    expected: str = request.app.state.cp_to_co_token
    if creds is None or not hmac.compare_digest(creds.credentials, expected):
        raise HTTPException(status_code=401, detail="invalid CP↔CO token")


def _get_backend(request: Request) -> ComputeBackend:
    return request.app.state.backend


def _reject_non_native_module(module: str | None) -> None:
    """Defense in depth: the CP sync gate refuses to persist an action
    whose module path is outside NATIVE_MODULE_PREFIX, and the wire-level
    request validator already enforces shape. This third checkpoint
    catches any payload that bypassed sync — e.g. a service-account
    directly POSTing a hand-crafted ticket — before the backend tries to
    import the module."""
    if module is not None and not module.startswith(NATIVE_MODULE_PREFIX):
        raise HTTPException(
            status_code=422,
            detail=f"module must start with {NATIVE_MODULE_PREFIX!r}; got {module!r}",
        )


def _backend_failure_response(exc: BackendFailure) -> JSONResponse:
    """Serialize a BackendFailure so the runner can reconstruct the typed
    failure and apply retry classification — without this, transient kinds
    (NODE_FAIL, OOM_KILLED, SLURMRESTD_UNREACHABLE, ...) would surface as a
    generic HTTPStatusError and be misclassified UNKNOWN_PERMANENT."""
    return JSONResponse(
        status_code=BACKEND_FAILURE_HTTP_STATUS,
        content=BackendFailureBody.from_exception(exc).model_dump(mode="json"),
        headers={BACKEND_FAILURE_HEADER: "1"},
    )


def _step_no_data_response(exc: StepNoData) -> JSONResponse:
    """Serialize a StepNoData so the runner reconstructs the typed terminal
    no-data signal and transitions the ticket to NO_DATA — distinct from the
    BackendFailure → FAILED path. Carries its own discriminator header so the
    client never confuses it with a failure."""
    return JSONResponse(
        status_code=STEP_NO_DATA_HTTP_STATUS,
        content=StepNoDataBody.from_exception(exc).model_dump(mode="json"),
        headers={STEP_NO_DATA_HEADER: "1"},
    )


def _handle_to_wire(handle: StepHandle) -> StepHandleWire:
    return StepHandleWire(
        compute_target=handle.compute_target,
        step_name=handle.step_name,
        slurm_job_id=handle.slurm_job_id,
        job_name=handle.job_name,
        output_path=str(handle.output_path) if handle.output_path is not None else None,
        logs_path=str(handle.logs_path) if handle.logs_path is not None else None,
        terminal_outputs=(
            {k: str(v) for k, v in handle.terminal_outputs.items()}
            if handle.terminal_outputs is not None
            else None
        ),
    )


def _handle_from_wire(wire: StepHandleWire) -> StepHandle:
    return StepHandle(
        compute_target=wire.compute_target,
        step_name=wire.step_name,
        slurm_job_id=wire.slurm_job_id,
        job_name=wire.job_name,
        output_path=Path(wire.output_path) if wire.output_path is not None else None,
        logs_path=Path(wire.logs_path) if wire.logs_path is not None else None,
        terminal_outputs=(
            {k: Path(v) for k, v in wire.terminal_outputs.items()}
            if wire.terminal_outputs is not None
            else None
        ),
    )


def _status_from_wire(wire: StepStatusWire) -> StepStatusInfo:
    return StepStatusInfo(
        status=wire.status,
        raw_state=wire.raw_state,
        exit_code=wire.exit_code,
        reason=wire.reason,
    )


def _status_to_wire(info: StepStatusInfo) -> StepStatusWire:
    return StepStatusWire(
        status=info.status,
        raw_state=info.raw_state,
        exit_code=info.exit_code,
        reason=info.reason,
    )


@router.post(PATH_STEP_SUBMIT)
async def submit_step(
    body: StepSubmitRequest,
    backend: Annotated[ComputeBackend, Depends(_get_backend)],
    _: Annotated[None, Depends(_require_cp_to_co_token)],
) -> StepHandleWire:
    """Submit a step and return its handle without blocking on completion."""
    _reject_non_native_module(body.module)
    try:
        handle = await backend.submit_step(
            body.step_name,
            {k: Path(v) for k, v in body.inputs.items()},
            Path(body.workspace),
            scope_target=body.scope_target,
            work_ticket_idx=body.work_ticket_idx,
            attempt=body.attempt,
            container=body.container,
            module=body.module,
            entrypoint=body.entrypoint,
            baseline_resources=body.baseline_resources,
        )
    except StepNoData as exc:
        # LocalBackend runs the native job to completion at submit time, so an
        # empty-well no-data outcome surfaces here. SLURM defers it to result.
        return _step_no_data_response(exc)
    except BackendFailure as exc:
        return _backend_failure_response(exc)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return _handle_to_wire(handle)


@router.post(PATH_STEP_PLAN)
async def plan_step(
    body: StepPlanRequest,
    _: Annotated[None, Depends(_require_cp_to_co_token)],
) -> StepPlanResponse:
    """Return a native step's optional `plan()` resource hint.

    Backend-agnostic: `plan()` is a pure submit-time function of the job's
    inputs, identical under LocalBackend and SlurmBackend (neither runs it on a
    compute node), so this route calls the dispatcher directly rather than
    going through the ComputeBackend — unlike submit/status/result, which
    genuinely diverge by backend.

    ADVISORY: the hint is an optimization, never a correctness input, so ANY
    failure here (a broken `plan()`, a bad module, a flatten/validation error)
    degrades to an EMPTY response and the control plane falls back to the YAML
    baseline. We log the cause at WARNING so a degrade is never silent, but we
    never fail the step over a sizing hint."""
    _reject_non_native_module(body.module)
    # The mapping into StepPlanResponse is INSIDE the try: StepPlanResponse
    # constrains each axis to `> 0`, but JobResourcePlan does not, so a plan()
    # returning a degenerate hint (cpu=0, a negative value, or a sub-second
    # walltime that truncates to 0) would raise a ValidationError here. Keeping
    # it in the try means such a hint degrades to the baseline like every other
    # plan failure, preserving the advisory guarantee (never a 500).
    try:
        raw_inputs = flatten_native_inputs(
            dict(body.inputs),
            step_name=body.step_name,
            scope_target=body.scope_target,
            work_ticket_idx=body.work_ticket_idx,
        )
        # run_native_job_plan does blocking work — importlib.import_module plus a
        # synchronous DuckDB Parquet-footer read — so offload it to a worker
        # thread rather than calling it inline in this async handler. A slow or
        # stalled filesystem would otherwise block CO's entire event loop for the
        # duration, not just this request. (submit/status/result stay responsive
        # because they await the backend.)
        job_plan = await run_in_threadpool(
            run_native_job_plan, body.module, raw_inputs, step_name=body.step_name
        )
        resources = job_plan.resources
        if resources is None:
            return StepPlanResponse()
        return StepPlanResponse(
            cpu=resources.cpu,
            mem_gb=resources.mem_gb,
            walltime_seconds=(
                int(resources.walltime.total_seconds()) if resources.walltime is not None else None
            ),
        )
    except Exception as exc:
        _log.warning(
            "plan() for step %r (module %r) failed; falling back to baseline: %s: %s",
            body.step_name,
            body.module,
            type(exc).__name__,
            exc,
        )
        return StepPlanResponse()


@router.post(PATH_STEP_STATUS)
async def status_step(
    body: StepStatusRequest,
    backend: Annotated[ComputeBackend, Depends(_get_backend)],
    _: Annotated[None, Depends(_require_cp_to_co_token)],
) -> StepStatusWire:
    """Read the live status of a submitted step (single, non-blocking)."""
    try:
        info = await backend.status_step(_handle_from_wire(body.handle))
    except BackendFailure as exc:
        return _backend_failure_response(exc)
    return _status_to_wire(info)


@router.post(PATH_STEP_RESULT)
async def result_step(
    body: StepResultRequest,
    backend: Annotated[ComputeBackend, Depends(_get_backend)],
    _: Annotated[None, Depends(_require_cp_to_co_token)],
) -> StepResultResponse:
    """Finalize a terminal step: verify the output contract and return the
    named outputs, or serialize the classified BackendFailure."""
    try:
        outputs = await backend.result_step(
            _handle_from_wire(body.handle), _status_from_wire(body.status)
        )
    except StepNoData as exc:
        # SLURM defers the no-data outcome to result_step (the job exited and
        # wrote a structured no-data line; SlurmBackend reconstructs it here).
        return _step_no_data_response(exc)
    except BackendFailure as exc:
        return _backend_failure_response(exc)
    return StepResultResponse(outputs={k: str(v) for k, v in outputs.items()})


@router.post(PATH_STEP_FIND_BY_NAME)
async def find_jobs_by_name(
    body: StepFindByNameRequest,
    backend: Annotated[ComputeBackend, Depends(_get_backend)],
    _: Annotated[None, Depends(_require_cp_to_co_token)],
) -> StepFindByNameResponse:
    """Look up live SLURM jobs by their deterministic name. The control plane
    calls this during restart recovery to adopt a job it submitted but whose
    id it never persisted (the write-ahead gap), instead of re-submitting.
    Serializes a classified BackendFailure (e.g. slurmrestd unreachable) so
    the runner retries rather than failing recovery."""
    try:
        found = await backend.find_jobs_by_name(body.job_name)
    except BackendFailure as exc:
        return _backend_failure_response(exc)
    return StepFindByNameResponse(
        jobs=[
            FoundJobWire(
                slurm_job_id=f.slurm_job_id,
                job_name=f.job_name,
                status=_status_to_wire(f.status),
            )
            for f in found
        ]
    )
