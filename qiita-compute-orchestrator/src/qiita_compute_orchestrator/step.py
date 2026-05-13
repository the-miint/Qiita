"""POST /api/v1/step/run — the only client of this service.

Issued by the control-plane runner for every workflow `step:` entry.
Dispatches synchronously to the configured ComputeBackend and returns
the backend's named output paths.

Auth is a shared bearer token loaded by `config.py` from
`/etc/qiita/cp-to-co.token` (path overridable via `CP_TO_CO_TOKEN_PATH`;
or, for dev/CI only, value via `CP_TO_CO_TOKEN` when
`QIITA_ALLOW_TOKEN_ENV=true`). Private path between two services on the
same network. Constant-time compare.
"""

from __future__ import annotations

import hmac
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from qiita_common.actions import NATIVE_MODULE_PREFIX
from qiita_common.api_paths import PATH_STEP_PREFIX, PATH_STEP_RUN
from qiita_common.backend_failure import (
    BACKEND_FAILURE_HEADER,
    BACKEND_FAILURE_HTTP_STATUS,
    BackendFailure,
    BackendFailureBody,
)
from qiita_common.models import StepRunRequest, StepRunResponse

from .backend import ComputeBackend

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


@router.post(PATH_STEP_RUN)
async def run_step(
    body: StepRunRequest,
    backend: Annotated[ComputeBackend, Depends(_get_backend)],
    _: Annotated[None, Depends(_require_cp_to_co_token)],
) -> StepRunResponse:
    # Defense in depth: the CP sync gate refuses to persist an action
    # whose module path is outside NATIVE_MODULE_PREFIX, and the
    # wire-level StepRunRequest validator already enforces shape. This
    # third checkpoint catches any payload that bypassed sync — e.g.
    # a service-account directly POSTing a hand-crafted ticket — and
    # rejects it before the backend tries to import the module.
    if body.module is not None and not body.module.startswith(NATIVE_MODULE_PREFIX):
        raise HTTPException(
            status_code=422,
            detail=f"module must start with {NATIVE_MODULE_PREFIX!r}; got {body.module!r}",
        )
    try:
        outputs = await backend.run_step(
            body.step_name,
            {k: Path(v) for k, v in body.inputs.items()},
            Path(body.workspace),
            reference_idx=body.reference_idx,
            work_ticket_idx=body.work_ticket_idx,
            container=body.container,
            module=body.module,
            entrypoint=body.entrypoint,
            baseline_resources=body.baseline_resources,
        )
    except BackendFailure as exc:
        # Structured workflow-step failure. Serialize so the runner can
        # reconstruct the typed BackendFailure and apply retry
        # classification — without this, transient kinds (NODE_FAIL,
        # OOM_KILLED, SLURMRESTD_UNREACHABLE, ...) would surface as a
        # generic HTTPStatusError and be misclassified UNKNOWN_PERMANENT.
        return JSONResponse(
            status_code=BACKEND_FAILURE_HTTP_STATUS,
            content=BackendFailureBody.from_exception(exc).model_dump(mode="json"),
            headers={BACKEND_FAILURE_HEADER: "1"},
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return StepRunResponse(outputs={k: str(v) for k, v in outputs.items()})
