"""SLURM compute backend — submits jobs to slurmrestd. Not yet implemented."""

from pathlib import Path

from ..backend import ComputeBackend


class SlurmBackend(ComputeBackend):
    """Submits compute jobs to SLURM via slurmrestd."""

    async def run_hash_job(self, fasta_path: Path, output_dir: Path, reference_idx: int) -> Path:
        raise NotImplementedError("SlurmBackend is not yet implemented")

    async def run_load_job(
        self,
        manifest_path: Path,
        fasta_path: Path,
        feature_map_path: Path,
        output_dir: Path,
        reference_idx: int,
        *,
        taxonomy_path: Path | None = None,
        tree_path: Path | None = None,
        jplace_path: Path | None = None,
    ) -> Path:
        raise NotImplementedError("SlurmBackend is not yet implemented")
