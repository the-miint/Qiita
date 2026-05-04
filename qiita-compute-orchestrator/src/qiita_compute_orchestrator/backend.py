"""Compute backend abstraction.

A backend executes one workflow step at a time. The runner translates
each ``step:`` entry in an ActionDefinition into a ``run_step`` call;
``action:`` entries do not go through the backend (they're HTTP calls
to the control-plane library dispatch endpoint).

`name` selects the step implementation (e.g. "hash", "load"). For the
local backend this drives an internal Python implementation; for the
SLURM backend it would inform the container's entrypoint or arguments.
`inputs` is a name → path map matching the names declared by the YAML
step's `inputs:` list. `workspace` is a per-step scratch directory the
backend may write outputs into. The return value is a name → path map
matching the step's `outputs:` list.
"""

from abc import ABC, abstractmethod
from pathlib import Path


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
    ) -> dict[str, Path]:
        """Execute the step identified by `name`. Returns a name → path
        map of outputs the runner can plumb into subsequent steps.

        Raises:
            ValueError: if `name` is not implemented by this backend.
            FileNotFoundError: if a required input path is missing.
        """
