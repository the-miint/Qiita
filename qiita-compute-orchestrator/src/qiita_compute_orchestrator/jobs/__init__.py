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
import inspect
import pkgutil
import types
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ValidationError
from qiita_common.actions import NATIVE_MODULE_PREFIX
from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models import WorkTicketFailureStage


def _contract_violation(*, step_name: str, reason: str) -> BackendFailure:
    return BackendFailure(
        kind=FailureKind.CONTRACT_VIOLATION,
        stage=WorkTicketFailureStage.STEP_RUN,
        step_name=step_name,
        reason=reason,
    )


# Framework scalars that get merged into raw_inputs before
# `Inputs.model_validate`. A step `inputs:` entry sharing one of these
# names would silently shadow the work-ticket value; `flatten_native_inputs`
# rejects the collision so LocalBackend and the SLURM launcher behave
# the same way.
_RESERVED_KEYS = frozenset({"reference_idx", "work_ticket_idx"})


def flatten_native_inputs(
    inputs: dict[str, Any],
    *,
    step_name: str,
    reference_idx: int,
    work_ticket_idx: int,
) -> dict[str, Any]:
    """Build the `raw_inputs` dict `Inputs.model_validate` consumes.

    Both LocalBackend and the SLURM launcher route through this helper
    so the reserved-key check fires symmetrically. Raises a typed
    BackendFailure(CONTRACT_VIOLATION) on collision — the runner sees
    the same shape regardless of which backend produced the violation.
    `step_name` is the YAML step name (e.g. "fastq"); failures carry it
    on BackendFailure.step_name to match the work_ticket failure-attribution
    contract.
    """
    overlap = sorted(_RESERVED_KEYS & inputs.keys())
    if overlap:
        raise _contract_violation(
            step_name=step_name,
            reason=f"step `inputs:` cannot use framework-reserved names: {overlap}",
        )
    return {**inputs, "reference_idx": reference_idx, "work_ticket_idx": work_ticket_idx}


async def run_native_job(
    module_name: str,
    raw_inputs: dict[str, Any],
    workspace: Path,
    *,
    step_name: str,
) -> dict[str, Path]:
    """Dispatch a native job. Returns the job's output map.

    `step_name` is the YAML step name (e.g. "fastq") — the same value
    `qiita.work_ticket.failure_step_name` records on failure. All
    `BackendFailure` raises below carry it; the `module_name` stays in
    the reason text for operator-side debugging.

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
            step_name=step_name,
            reason=(
                f"native module path must start with {NATIVE_MODULE_PREFIX!r}; got {module_name!r}"
            ),
        )

    try:
        mod = importlib.import_module(module_name)
    except Exception as exc:
        # Anything raised during import (SyntaxError, NameError,
        # Pydantic model-construction failure, ...) is a job-tree
        # contract violation. Catch broadly here; the scope is the
        # import call only so dispatcher bugs below still surface.
        raise _contract_violation(
            step_name=step_name,
            reason=(
                f"failed to import native job module {module_name!r}: {type(exc).__name__}: {exc}"
            ),
        ) from exc

    mod_errors = validate_native_job_module(mod)
    if mod_errors:
        # Delegate to the same validator the boot scan uses so the
        # dispatcher and the lifespan scan disagree on nothing — the
        # operator sees the exact same message no matter which layer
        # surfaces the violation.
        raise _contract_violation(
            step_name=step_name,
            reason=f"native job {module_name!r}: {'; '.join(mod_errors)}",
        )
    # Validator guarantees both exports exist with the right shapes.
    Inputs = mod.Inputs
    execute = mod.execute

    try:
        inputs = Inputs.model_validate(raw_inputs)
    except ValidationError as exc:
        raise BackendFailure(
            kind=FailureKind.BAD_INPUT,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=step_name,
            reason=f"native job {module_name!r} input validation failed: {exc}",
        ) from exc

    try:
        return await execute(inputs, workspace)
    except NotImplementedError as exc:
        # Skeleton path: a job module that ships before its execute() is
        # written. Permanent — retry produces the same NotImplementedError.
        raise BackendFailure(
            kind=FailureKind.UNKNOWN_PERMANENT,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=step_name,
            reason=f"native job {module_name!r} not implemented: {exc}",
        ) from exc
    except FileNotFoundError as exc:
        raise BackendFailure(
            kind=FailureKind.BAD_INPUT,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=step_name,
            reason=str(exc),
        ) from exc
    except ValueError as exc:
        # Data-quality issues raised from inside execute() (malformed
        # FASTA, unmapped hashes, etc.). Permanent because the same
        # input always fails the same way.
        raise BackendFailure(
            kind=FailureKind.BAD_INPUT,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=step_name,
            reason=str(exc),
        ) from exc


# =============================================================================
# Boot-time discovery scan
# =============================================================================


def validate_native_job_module(mod: types.ModuleType) -> list[str]:
    """Return a list of contract violations for one candidate job module.
    Empty list means the module is a valid native job. Pure function:
    no importing, no filesystem access — the scan does that and hands a
    module object here.
    """
    errors: list[str] = []
    Inputs = getattr(mod, "Inputs", None)
    execute = getattr(mod, "execute", None)
    if Inputs is None:
        errors.append("missing `Inputs`")
    if execute is None:
        errors.append("missing `execute`")
    if errors:
        # Don't drill deeper — the missing-export errors are the
        # primary signal; type-checks on a missing attribute would be
        # noise.
        return errors
    if not (isinstance(Inputs, type) and issubclass(Inputs, BaseModel)):
        errors.append("`Inputs` must be a BaseModel subclass")
    if not inspect.iscoroutinefunction(execute):
        errors.append("`execute` must be an async function")
    return errors


def scan_native_jobs(
    *,
    package_path: list[str] | None = None,
    prefix: str = NATIVE_MODULE_PREFIX,
) -> list[str]:
    """Walk the jobs package and validate every non-dunder submodule.
    Returns the list of validated module names.

    `package_path` and `prefix` default to the real jobs package; tests
    override them to scan a synthetic tree without touching the real
    `jobs/` directory.

    Raises RuntimeError on any contract violation, naming each offending
    module and what's wrong. Boot scan is the orchestrator's earliest
    opportunity to catch broken job code; failing fast prevents a job
    that imports cleanly but is malformed from surprising the runner
    at submit time.

    The scan does NOT skip underscore-prefixed modules — every
    non-dunder file under jobs/ must be a valid native job. Shared
    helpers go in a sibling module outside jobs/ (e.g.
    qiita_compute_orchestrator/job_helpers.py).
    """
    if package_path is None:
        package_path = __path__
    validated: list[str] = []
    errors: list[str] = []
    for _finder, modname, _ispkg in pkgutil.walk_packages(package_path, prefix=prefix):
        leaf = modname.rsplit(".", 1)[-1]
        if leaf in ("__init__", "__main__"):
            continue
        try:
            mod = importlib.import_module(modname)
        except Exception as exc:
            # Same widening rationale as run_native_job's import catch:
            # any exception during a job-module import is a contract
            # violation, not just ImportError.
            errors.append(f"  {modname}: failed to import — {type(exc).__name__}: {exc}")
            continue
        mod_errors = validate_native_job_module(mod)
        if mod_errors:
            errors.append(f"  {modname}: {'; '.join(mod_errors)}")
            continue
        validated.append(modname)

    if errors:
        raise RuntimeError(
            "native job tree is malformed; refusing to start orchestrator:\n"
            + "\n".join(errors)
            + "\n\nShared helpers go in a sibling module outside `jobs/` "
            "(e.g. `qiita_compute_orchestrator/job_helpers.py`); every "
            "non-dunder file in `jobs/` must export `Inputs` (BaseModel) "
            "and `execute` (async)."
        )
    return validated
