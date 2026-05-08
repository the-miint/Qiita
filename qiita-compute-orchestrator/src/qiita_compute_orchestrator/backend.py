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

`inputs` is a name => path map matching the names declared by the YAML
step's `inputs:` list. `workspace` is a per-step scratch directory the
backend may write outputs into. The return value is a name => path map
matching the step's `outputs:` list.
"""

from abc import ABC, abstractmethod
from pathlib import Path

from qiita_common.models import StepBaselineResources


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
        entrypoint: str | None = None,
        baseline_resources: StepBaselineResources | None = None,
    ) -> dict[str, Path]:
        """Execute the step identified by `name`. Returns a name => path
        map of outputs the runner can plumb into subsequent steps.

        `container`, `entrypoint`, `baseline_resources` are optional on
        the protocol so LocalBackend (which dispatches on `name` and
        uses internal helpers) can be invoked without them. SlurmBackend
        requires them and refuses the call when they're absent.

        Raises:
            ValueError: if `name` is not implemented by this backend.
            FileNotFoundError: if a required input path is missing.
        """
