"""Native-step jobs package.

Every Python module under this package (except `__init__` and `__main__`)
is a native job: it exports an `Inputs` Pydantic model declaring its
input contract and an `async def execute(inputs, workspace) -> dict[str, Path]`
function doing the work. The framework dispatcher `run_native_job`
below imports a module by name, validates raw inputs against the
module's `Inputs` schema, and invokes `execute()`. The dispatcher is
the single source of error classification — both `LocalBackend`
(in-process dispatch) and the shared SLURM launcher (`__main__.py`)
funnel through here so failures map to typed `BackendFailure` values
the same way regardless of runtime.

Location decision: native jobs live nested inside
`qiita-compute-orchestrator` rather than as a top-level `qiita-jobs/`
package because (a) they share the orchestrator's runtime environment,
(b) co-locating with the dispatcher avoids cross-package import
gymnastics, and (c) the set of native jobs is too small today to
justify its own `pyproject.toml`. If `jobs/` grows into a real domain
with its own evolution cadence, extract to a top-level `qiita-jobs/`
package and update the orchestrator to depend on it.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ValidationError
from qiita_common.actions import NATIVE_MODULE_PREFIX
from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models import WorkTicketFailureStage


def _contract_violation(*, module_name: str, reason: str) -> BackendFailure:
    return BackendFailure(
        kind=FailureKind.CONTRACT_VIOLATION,
        stage=WorkTicketFailureStage.STEP_RUN,
        step_name=module_name,
        reason=reason,
    )


async def run_native_job(
    module_name: str,
    raw_inputs: dict[str, Any],
    workspace: Path,
) -> dict[str, Path]:
    """Dispatch a native job. Returns the job's output map.

    Maps known internal failures to typed `BackendFailure`:
    - Module path outside `NATIVE_MODULE_PREFIX`, or module missing the
      `Inputs` / `execute` exports → CONTRACT_VIOLATION (permanent;
      the job tree is broken).
    - `Inputs.model_validate` rejects `raw_inputs`, or `execute` raises
      `FileNotFoundError` / `ValueError` → BAD_INPUT (permanent;
      same inputs would fail the same way on retry).
    - `execute` raises `NotImplementedError` → UNKNOWN_PERMANENT
      (the job module is a skeleton; auto-retry would not help).

    Other exception types from `execute` propagate so they surface in
    the orchestrator's logs with full traceback rather than being
    silently classified.
    """
    if not module_name.startswith(NATIVE_MODULE_PREFIX):
        raise _contract_violation(
            module_name=module_name,
            reason=(
                f"native module path must start with {NATIVE_MODULE_PREFIX!r}; got {module_name!r}"
            ),
        )

    try:
        mod = importlib.import_module(module_name)
    except ImportError as exc:
        raise _contract_violation(
            module_name=module_name,
            reason=f"failed to import native job module: {exc}",
        ) from exc

    Inputs = getattr(mod, "Inputs", None)
    execute = getattr(mod, "execute", None)
    if Inputs is None or execute is None:
        raise _contract_violation(
            module_name=module_name,
            reason=(
                f"native job {module_name!r} must export `Inputs` (BaseModel) and `execute` (async)"
            ),
        )
    if not (isinstance(Inputs, type) and issubclass(Inputs, BaseModel)):
        raise _contract_violation(
            module_name=module_name,
            reason=f"native job {module_name!r}: `Inputs` must be a BaseModel subclass",
        )

    try:
        inputs = Inputs.model_validate(raw_inputs)
    except ValidationError as exc:
        raise BackendFailure(
            kind=FailureKind.BAD_INPUT,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=module_name,
            reason=f"native job input validation failed: {exc}",
        ) from exc

    try:
        return await execute(inputs, workspace)
    except NotImplementedError as exc:
        # Skeleton path: a job module that ships before its execute() is
        # written. Permanent — retry produces the same NotImplementedError.
        raise BackendFailure(
            kind=FailureKind.UNKNOWN_PERMANENT,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=module_name,
            reason=f"native job not implemented: {exc}",
        ) from exc
    except FileNotFoundError as exc:
        raise BackendFailure(
            kind=FailureKind.BAD_INPUT,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=module_name,
            reason=str(exc),
        ) from exc
    except ValueError as exc:
        # Data-quality issues raised from inside execute() (malformed
        # FASTA, unmapped hashes, etc.). Permanent because the same
        # input always fails the same way.
        raise BackendFailure(
            kind=FailureKind.BAD_INPUT,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=module_name,
            reason=str(exc),
        ) from exc
