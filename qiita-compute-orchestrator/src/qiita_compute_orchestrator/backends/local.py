"""Local compute backend — runs native step modules in-process for dev/test.

After the upload-doput refactor, every workflow step in the system is
a native module (`module:` in the YAML), so this backend has just one
dispatch arm: the framework's `run_native_job`. Container-step support
lives on SlurmBackend, where it belongs in production.

LocalBackend is *synchronous*: it runs the module to completion at
submit time. It implements the decoupled submit/status/result interface
honestly — `submit_step` returns a terminal handle carrying the outputs
(compute_target=local, no SLURM job id), `status_step` is immediately
COMPLETED, and `result_step` returns the captured outputs — rather than
fabricating a job id it doesn't have.
"""

from pathlib import Path

from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models import StepStatus, WorkTicketFailureStage

from ..backend import ComputeBackend, FoundJob, LocalStepHandle, StepHandle, StepStatusInfo
from ..jobs import flatten_native_inputs, run_native_job


class LocalBackend(ComputeBackend):
    """Runs native-module steps in-process. Dev/test only.

    The production analogue is `SlurmBackend`; that one still supports
    `container:` steps (apptainer under SLURM), but no current workflow
    needs to exercise a container step in LocalBackend, so the surface
    here is module-only. A request that sets `container:` on a step is
    a contract violation — refuse loudly rather than silently bypassing
    SLURM-side concerns (resource limits, cgroups, image pinning).
    """

    async def submit_step(
        self,
        name: str,
        inputs: dict[str, Path],
        workspace: Path,
        *,
        scope_target: dict,
        work_ticket_idx: int,
        attempt: int = 0,  # noqa: ARG002 — local has no SLURM job to name per-attempt
        container: str | None = None,
        module: str | None = None,
        entrypoint: str | None = None,  # noqa: ARG002 — LocalBackend ignores entrypoint
        baseline_resources=None,  # noqa: ARG002 — accepted for protocol parity
        # Container-only (bind + env into apptainer), and LocalBackend rejects
        # container steps below — so it can only ever arrive empty here.
        derived_inputs: dict[str, str] | None = None,  # noqa: ARG002 — protocol parity
    ) -> StepHandle:
        """Run the native module in-process to completion and return a
        terminal StepHandle (compute_target=local, no SLURM job id, the
        outputs in hand). Translates known internal failures into typed
        `BackendFailure` via the shared `run_native_job` dispatcher (which
        handles FileNotFoundError / ValueError / ValidationError
        mapping). The contract-violation branches here catch wire-shape
        misconfiguration the dispatcher wouldn't see."""
        if (container is None) == (module is None):
            # Symmetric with SlurmBackend's guard: both None (neither
            # runtime declared) and both set (ambiguous runtime) are
            # contract violations. The wire validator on StepSubmitRequest
            # catches this upstream; this guard protects direct callers
            # (tests, programmatic submission) so silently preferring
            # one runtime over the other can't happen.
            raise BackendFailure(
                kind=FailureKind.CONTRACT_VIOLATION,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=name,
                reason="LocalBackend requires exactly one of `container` or `module` on the step",
            )
        if container is not None:
            raise BackendFailure(
                kind=FailureKind.CONTRACT_VIOLATION,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=name,
                reason=(
                    "LocalBackend no longer dispatches container steps — every workflow "
                    "step must declare `module:`. Container execution lives on SlurmBackend."
                ),
            )
        # Native step: delegate to the framework dispatcher. It validates
        # the module prefix, imports the module, validates raw_inputs via
        # `mod.Inputs`, invokes `mod.execute(inputs, workspace)`, and maps
        # known exceptions to typed BackendFailure. `flatten_native_inputs`
        # merges the scope-target idx scalars and rejects reserved-key
        # collisions the same way the SLURM launcher does — a job module
        # sees identical raw_inputs regardless of runtime.
        raw_inputs = flatten_native_inputs(
            {k: str(v) for k, v in inputs.items()},
            step_name=name,
            scope_target=scope_target,
            work_ticket_idx=work_ticket_idx,
        )
        outputs = await run_native_job(module, raw_inputs, workspace, step_name=name)
        return LocalStepHandle(step_name=name, terminal_outputs=outputs)

    async def status_step(self, handle: StepHandle) -> StepStatusInfo:
        """Local steps run to completion at submit time, so status is
        always COMPLETED. Provided for interface parity; the runner skips
        polling when a handle already carries terminal_outputs."""
        del handle
        return StepStatusInfo(status=StepStatus.COMPLETED)

    async def result_step(self, handle: StepHandle, status: StepStatusInfo) -> dict[str, Path]:
        """Return the outputs captured at submit time. A non-COMPLETED
        status can't arise on the normal local path (submit_step either
        runs to completion or raises before returning a handle), so it's a
        caller bug — honor the ABC contract and fail loudly rather than
        return outputs for a step that didn't succeed."""
        if status.status != StepStatus.COMPLETED:
            raise BackendFailure(
                kind=FailureKind.UNKNOWN_PERMANENT,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=handle.step_name,
                reason=f"LocalBackend.result_step called with non-COMPLETED status "
                f"{status.status.value!r}",
            )
        return handle.terminal_outputs

    async def find_jobs_by_name(self, job_name: str) -> list[FoundJob]:
        """LocalBackend runs steps in-process and never submits to SLURM, so
        there is never an orphaned job to find. Always empty — provided for
        interface parity so the CP→CO find-by-name route works against either
        backend without an isinstance check."""
        del job_name
        return []
