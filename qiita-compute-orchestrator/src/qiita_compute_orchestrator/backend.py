"""Compute backend abstraction.

A backend executes one workflow step at a time. The control-plane runner
drives each ``step:`` entry through the decoupled ``submit_step`` /
``status_step`` / ``result_step`` trio (so it never holds a connection open
for a SLURM job's full duration); ``action:`` entries do not go through the
backend (they're in-process calls on the control plane).

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
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models import (
    ComputeTarget,
    ScopeTargetKind,
    StepBaselineResources,
    StepStatus,
    WorkTicketFailureStage,
)


@dataclass(frozen=True, slots=True)
class StepHandle:
    """What `submit_step` hands back, threaded into `status_step` /
    `result_step`. Because the orchestrator holds no state between those
    calls, the handle must carry everything they need — the SLURM job id
    and the workspace paths — so a later poll / result can proceed from
    the handle alone. The control plane persists these fields and, after a
    restart, reconstructs an equivalent handle from them to re-attach
    (it does not reuse this object).

    A synchronous backend (LocalBackend runs the module in-process at
    submit time) sets `terminal_outputs`; the caller then skips polling
    and uses the outputs directly. `slurm_job_id` / `job_name` /
    `output_path` / `logs_path` are populated only for the SLURM path.
    """

    compute_target: ComputeTarget
    step_name: str
    slurm_job_id: int | None = None
    job_name: str | None = None
    output_path: Path | None = None
    logs_path: Path | None = None
    terminal_outputs: dict[str, Path] | None = None


@dataclass(frozen=True, slots=True)
class StepStatusInfo:
    """Live status of a submitted step. `status` is the coarse class the
    runner / ticket-summary consume; `raw_state` / `exit_code` / `reason`
    carry the backend-native detail used for display and for
    `result_step`'s terminal classification."""

    status: StepStatus
    raw_state: str | None = None
    exit_code: int | None = None
    reason: str | None = None

    @property
    def is_terminal(self) -> bool:
        return self.status in (StepStatus.COMPLETED, StepStatus.FAILED)


@dataclass(frozen=True, slots=True)
class FoundJob:
    """One live SLURM job matched by `find_jobs_by_name`: its id plus a
    status snapshot. The control-plane recovery path adopts a found job by
    reconstructing a StepHandle from `slurm_job_id` (workspace paths are
    deterministic), closing the write-ahead 'submitting'-without-id gap
    without re-submitting a duplicate."""

    slurm_job_id: int
    job_name: str
    status: StepStatusInfo


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
    async def submit_step(
        self,
        name: str,
        inputs: dict[str, Path],
        workspace: Path,
        *,
        scope_target: dict[str, Any],
        work_ticket_idx: int,
        attempt: int = 0,
        container: str | None = None,
        module: str | None = None,
        entrypoint: str | None = None,
        baseline_resources: StepBaselineResources | None = None,
    ) -> StepHandle:
        """Submit the step and return immediately with a `StepHandle` —
        do NOT block until completion. For SLURM this `sbatch`es the job
        and returns its id; for a synchronous backend (local) it runs the
        module in-process and returns a terminal handle carrying the
        outputs.

        `attempt` is the retry attempt number; it is encoded into the
        deterministic SLURM job name (`qiita-wt{idx}-{step}-a{attempt}`)
        so a job submitted but not yet recorded can be re-found by name.

        Exactly one of `container` or `module` must be set — the wire
        validator on StepSubmitRequest enforces this before the route hands
        off to a backend; `container` drives the apptainer-exec path,
        `module` selects the native-step path. `entrypoint` overrides a
        container's default ENTRYPOINT (container-only); `baseline_resources`
        is required by SlurmBackend and ignored by LocalBackend. Raises
        `BackendFailure` on a submission error (classified retriable /
        permanent)."""

    @abstractmethod
    async def status_step(self, handle: StepHandle) -> StepStatusInfo:
        """Return the live status of a submitted step in a single
        (non-looping) read. The control-plane runner owns the poll loop;
        a backend never blocks here. Stateless: everything needed comes
        from `handle`."""

    @abstractmethod
    async def result_step(self, handle: StepHandle, status: StepStatusInfo) -> dict[str, Path]:
        """Finalize a terminal step and return its name => path outputs.
        On a COMPLETED status, verify the output contract and parse the
        outputs map; on a FAILED status, raise the classified
        `BackendFailure`. Must only be called once `status.is_terminal`."""

    @abstractmethod
    async def find_jobs_by_name(self, job_name: str) -> list[FoundJob]:
        """Return the live jobs whose name equals `job_name` (the
        deterministic `qiita-wt{idx}-{step}-a{attempt}` name). Empty when
        none match — including a job slurmrestd has already purged, or an
        in-process backend that never submits to SLURM.

        The control plane uses this during restart recovery to adopt a job
        it submitted but whose id it never persisted (the write-ahead gap),
        instead of re-submitting a duplicate. Raises a classified
        `BackendFailure` on a backend read error (e.g. SLURMRESTD_UNREACHABLE,
        which the runner's recovery treats as transient)."""
