"""Local compute backend — runs native step modules in-process for dev/test.

After the upload-doput refactor, every workflow step in the system is
a native module (`module:` in the YAML), so this backend has just one
dispatch arm: the framework's `run_native_job`. Container-step support
lives on SlurmBackend, where it belongs in production.
"""

from pathlib import Path

from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models import WorkTicketFailureStage

from ..backend import ComputeBackend
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

    async def run_step(
        self,
        name: str,
        inputs: dict[str, Path],
        workspace: Path,
        *,
        scope_target: dict,
        work_ticket_idx: int,
        container: str | None = None,
        module: str | None = None,
        entrypoint: str | None = None,  # noqa: ARG002 — LocalBackend ignores entrypoint
        baseline_resources=None,  # noqa: ARG002 — accepted for protocol parity
    ) -> dict[str, Path]:
        """Public backend interface. Translates known internal failures
        into typed `BackendFailure` via the shared `run_native_job`
        dispatcher (which handles FileNotFoundError / ValueError /
        ValidationError mapping). The contract-violation branches here
        catch wire-shape misconfiguration that the dispatcher wouldn't
        see (because it never gets reached)."""
        if (container is None) == (module is None):
            # Symmetric with SlurmBackend's guard: both None (neither
            # runtime declared) and both set (ambiguous runtime) are
            # contract violations. The wire validator on StepRunRequest
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
        return await run_native_job(module, raw_inputs, workspace, step_name=name)
