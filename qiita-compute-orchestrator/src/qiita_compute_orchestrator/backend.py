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
populated per step. Native (`module`) and container steps share the
output contract (manifest + 0o440 file mode) but diverge in how the
SBATCH script is built — `apptainer exec` vs `python -m
qiita_compute_orchestrator.jobs --job <name>`. Both backends today
dispatch both forms through the shared `run_native_job` framework
dispatcher.

`inputs` is a name => path map matching the names declared by the YAML
step's `inputs:` list. `workspace` is a per-step scratch directory the
backend may write outputs into. The return value is a name => path map
matching the step's `outputs:` list.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models import (
    ScopeTargetKind,
    StepBaselineResources,
    WorkTicketFailureStage,
)

_CONTAINER_SUPPORTED_SCOPES: frozenset[str] = frozenset(
    {ScopeTargetKind.REFERENCE.value, ScopeTargetKind.SEQUENCED_POOL.value}
)


def assert_container_scope_supported(*, step_name: str, scope_target: dict[str, Any]) -> None:
    """Reject a container step whose work_ticket's scope_target isn't one
    the backends know how to dispatch.

    Container steps today are referenced by two scope_target.kinds:

      * REFERENCE — every reference-add container step (hash, load).
      * SEQUENCED_POOL — the bcl-convert workflow's container step.

    Submitting a container step against any other kind is a
    workflow-authoring error, not a data error — surface it as
    CONTRACT_VIOLATION so the runner doesn't quietly fall through to a
    downstream path that would extract the wrong scalar. Shared between
    LocalBackend and SlurmBackend so the two implementations cannot
    drift in either the predicate or the error wording.

    Raises BackendFailure(CONTRACT_VIOLATION) on an unsupported kind.
    Returns None otherwise; callers consume the kind-appropriate
    scalars from `scope_target` directly after the guard.
    """
    kind = scope_target.get("kind")
    if kind not in _CONTAINER_SUPPORTED_SCOPES:
        raise BackendFailure(
            kind=FailureKind.CONTRACT_VIOLATION,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=step_name,
            reason=(
                f"container step {step_name!r} requires a scope_target with kind in"
                f" {sorted(_CONTAINER_SUPPORTED_SCOPES)};"
                f" got scope_target.kind={kind!r}"
            ),
        )


class ComputeBackend(ABC):
    """Abstract base for compute backends (local, SLURM, etc.)."""

    async def aclose(self) -> None:
        """Release any resources the backend holds (HTTP clients,
        connection pools, etc.). Called by the FastAPI lifespan
        teardown.

        Default implementation is a no-op so backends without
        long-lived resources (LocalBackend) inherit the right
        behavior without writing an empty override. Backends with
        resources (SlurmBackend's httpx client) override this.
        """

    @abstractmethod
    async def run_step(
        self,
        name: str,
        inputs: dict[str, Path],
        workspace: Path,
        *,
        scope_target: dict[str, Any],
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
        under `qiita_compute_orchestrator.jobs.*`).

        `entrypoint` overrides a container's default ENTRYPOINT and is
        meaningful only when `container` is set. `baseline_resources`
        is required by SlurmBackend (CPU/mem/walltime) and ignored by
        LocalBackend.

        Raises:
            ValueError: if `name` is not implemented by this backend.
            FileNotFoundError: if a required input path is missing.
        """
