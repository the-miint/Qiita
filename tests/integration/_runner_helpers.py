"""Helpers for tests that drive qiita_control_plane.runner.run_workflow.

`LocalComputeBackendClient` shapes a LocalBackend behind the
ComputeBackendClient protocol so the runner can call it without HTTP —
useful for in-process integration tests where spinning up the
orchestrator would just add ceremony.
"""

from __future__ import annotations

from pathlib import Path

from qiita_compute_orchestrator.backends.local import LocalBackend


class LocalComputeBackendClient:
    """Drop-in for ComputeBackendClient that calls LocalBackend in-process.

    Lets the runner stay backend-agnostic in tests while skipping the
    HTTP hop a real orchestrator would add.
    """

    def __init__(self) -> None:
        self._backend = LocalBackend()

    async def run_step(
        self,
        *,
        step_name: str,
        inputs: dict[str, Path],
        workspace: Path,
        scope_target: dict,
        work_ticket_idx: int,
        container: str | None = None,
        module: str | None = None,
        entrypoint: str | None = None,
        baseline_resources=None,
    ) -> dict[str, Path]:
        return await self._backend.run_step(
            step_name,
            inputs,
            workspace,
            scope_target=scope_target,
            work_ticket_idx=work_ticket_idx,
            container=container,
            module=module,
            entrypoint=entrypoint,
            baseline_resources=baseline_resources,
        )
