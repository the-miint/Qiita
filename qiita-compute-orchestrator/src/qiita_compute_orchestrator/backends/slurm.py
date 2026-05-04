"""SLURM compute backend — submits jobs to slurmrestd. Not yet implemented."""

from pathlib import Path

from ..backend import ComputeBackend


class SlurmBackend(ComputeBackend):
    """Submits compute jobs to SLURM via slurmrestd."""

    async def run_step(
        self,
        name: str,
        inputs: dict[str, Path],
        workspace: Path,
        *,
        reference_idx: int,
    ) -> dict[str, Path]:
        raise NotImplementedError("SlurmBackend is not yet implemented")
