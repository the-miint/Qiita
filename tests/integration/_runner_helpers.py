"""Helpers for tests that drive qiita_control_plane.runner.run_workflow.

`LocalComputeBackendClient` shapes a LocalBackend behind the
ComputeBackendClient protocol so the runner can call it without HTTP —
useful for in-process integration tests where spinning up the
orchestrator would just add ceremony.
"""

from __future__ import annotations

from pathlib import Path

from qiita_common.models import StepHandleWire, StepStatusWire

# _handle_from_wire / _handle_to_wire are reused from the orchestrator rather
# than duplicated here: they dispatch on the typed LocalStepHandle /
# SlurmStepHandle subtypes, so this in-process bridge stays correct if that
# mapping changes.
from qiita_compute_orchestrator.backend import StepStatusInfo
from qiita_compute_orchestrator.backends.local import LocalBackend
from qiita_compute_orchestrator.step import _handle_from_wire, _handle_to_wire


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
        derived_inputs: dict[str, str] | None = None,
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
            # Forwarded, not dropped: this shim stands in for the real HTTP hop
            # to a real backend, so swallowing it here would hide a plumbing
            # break that production would hit.
            derived_inputs=derived_inputs,
        )
        return _handle_to_wire(handle)

    async def status_step(self, handle: StepHandleWire) -> StepStatusWire:
        info: StepStatusInfo = await self._backend.status_step(
            _handle_from_wire(handle)
        )
        return StepStatusWire(
            status=info.status,
            raw_state=info.raw_state,
            exit_code=info.exit_code,
            reason=info.reason,
        )

    async def result_step(
        self, handle: StepHandleWire, status: StepStatusWire
    ) -> dict[str, Path]:
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
