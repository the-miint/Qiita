"""Helpers for tests that drive qiita_control_plane.runner.run_workflow.

`LocalComputeBackendClient` shapes a LocalBackend behind the
ComputeBackendClient protocol so the runner can call it without HTTP —
useful for in-process integration tests where spinning up the
orchestrator would just add ceremony.
"""

from __future__ import annotations

from pathlib import Path

from qiita_compute_orchestrator.backend import StepHandle, StepStatusInfo
from qiita_compute_orchestrator.backends.local import LocalBackend

from qiita_common.models import StepHandleWire, StepStatusWire


def _handle_to_wire(handle: StepHandle) -> StepHandleWire:
    """Convert the orchestrator-internal StepHandle (Path-typed) into the
    StepHandleWire the runner threads around (string-typed)."""
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


def _handle_from_wire(handle: StepHandleWire) -> StepHandle:
    return StepHandle(
        compute_target=handle.compute_target,
        step_name=handle.step_name,
        slurm_job_id=handle.slurm_job_id,
        job_name=handle.job_name,
        output_path=Path(handle.output_path) if handle.output_path is not None else None,
        logs_path=Path(handle.logs_path) if handle.logs_path is not None else None,
        terminal_outputs=(
            {k: Path(v) for k, v in handle.terminal_outputs.items()}
            if handle.terminal_outputs is not None
            else None
        ),
    )


class LocalComputeBackendClient:
    """Drop-in for ComputeBackendClient that calls LocalBackend in-process.

    Lets the runner stay backend-agnostic in tests while skipping the HTTP hop
    a real orchestrator would add. The LocalBackend is synchronous — `submit_step`
    runs the module to completion and hands back a terminal handle (with
    `terminal_outputs`), so the runner short-circuits and never polls. The
    `status_step` / `result_step` methods are implemented for protocol
    completeness (they delegate to the backend) but aren't exercised on the
    local terminal path.
    """

    def __init__(self) -> None:
        self._backend = LocalBackend()

    async def submit_step(
        self,
        *,
        step_name: str,
        inputs: dict[str, Path],
        workspace: Path,
        scope_target: dict,
        work_ticket_idx: int,
        attempt: int = 0,
        container: str | None = None,
        module: str | None = None,
        entrypoint: str | None = None,
        baseline_resources=None,
    ) -> StepHandleWire:
        handle = await self._backend.submit_step(
            step_name,
            inputs,
            workspace,
            scope_target=scope_target,
            work_ticket_idx=work_ticket_idx,
            attempt=attempt,
            container=container,
            module=module,
            entrypoint=entrypoint,
            baseline_resources=baseline_resources,
        )
        return _handle_to_wire(handle)

    async def status_step(self, handle: StepHandleWire) -> StepStatusWire:
        info: StepStatusInfo = await self._backend.status_step(_handle_from_wire(handle))
        return StepStatusWire(
            status=info.status,
            raw_state=info.raw_state,
            exit_code=info.exit_code,
            reason=info.reason,
        )

    async def result_step(self, handle: StepHandleWire, status: StepStatusWire) -> dict[str, Path]:
        return await self._backend.result_step(
            _handle_from_wire(handle),
            StepStatusInfo(
                status=status.status,
                raw_state=status.raw_state,
                exit_code=status.exit_code,
                reason=status.reason,
            ),
        )

    async def find_jobs_by_name(self, job_name: str) -> list:
        # LocalBackend runs steps in-process and never submits to SLURM, so
        # there is never an orphaned job to adopt — always empty (protocol
        # parity with ComputeBackendClient for the runner's resume path).
        return []
