"""Compute backend abstraction.

A backend executes one workflow step at a time. The runner translates
each ``step:`` entry in an ActionDefinition into a ``run_step`` call;
``action:`` entries do not go through the backend (they're HTTP calls
to the control-plane library dispatch endpoint).

`name` selects the step implementation (e.g. "hash", "load"). For
LocalBackend this drives an internal Python implementation that
ignores `container` / `entrypoint` / `baseline_resources`. For
SlurmBackend the container metadata is required — SLURM submission
needs to know what image to run and how to size the allocation.

`module` is the peer field to `container`: exactly one of the two is
populated per step. Backends that support native dispatch read the
module path and execute the Python entry point in their environment;
backends that don't fail with a typed BackendFailure via
``native_dispatch_not_implemented``.

`inputs` is a name => path map matching the names declared by the YAML
step's `inputs:` list. `workspace` is a per-step scratch directory the
backend may write outputs into. The return value is a name => path map
matching the step's `outputs:` list.
"""

from abc import ABC, abstractmethod
from pathlib import Path

from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models import StepBaselineResources, WorkTicketFailureStage


def native_dispatch_not_implemented(
    *, backend_name: str, step_name: str, module: str
) -> BackendFailure:
    """Standard typed failure for backends that don't implement the
    `module:` runtime. Both LocalBackend and SlurmBackend route through
    here so the reason string can't drift between them.
    """
    return BackendFailure(
        kind=FailureKind.CONTRACT_VIOLATION,
        stage=WorkTicketFailureStage.STEP_RUN,
        step_name=step_name,
        reason=f"{backend_name} does not implement native dispatch (got module={module!r})",
    )


class ComputeBackend(ABC):
    """Abstract base for compute backends (local, SLURM, etc.)."""

    @abstractmethod
    async def run_step(
        self,
        name: str,
        inputs: dict[str, Path],
        workspace: Path,
        *,
        reference_idx: int,
        work_ticket_idx: int,
        container: str | None = None,
        module: str | None = None,
        entrypoint: str | None = None,
        baseline_resources: StepBaselineResources | None = None,
    ) -> dict[str, Path]:
        """Execute the step identified by `name`. Returns a name => path
        map of outputs the runner can plumb into subsequent steps.

        Exactly one of `container` or `module` must be set — the wire
        validator on StepRunRequest enforces this before the route
        hands off to a backend. `container` drives the apptainer-exec
        path; `module` selects the native-step path (Python modules
        under `qiita_compute_orchestrator.jobs.*`). Backends that do
        not implement native dispatch must fail via
        ``native_dispatch_not_implemented`` when `module` is set so the
        runner sees a typed BackendFailure rather than a confusing
        "step not found".

        `entrypoint` overrides a container's default ENTRYPOINT and is
        meaningful only when `container` is set. `baseline_resources`
        is required by SlurmBackend (CPU/mem/walltime) and ignored by
        LocalBackend.

        Raises:
            ValueError: if `name` is not implemented by this backend.
            FileNotFoundError: if a required input path is missing.
        """
