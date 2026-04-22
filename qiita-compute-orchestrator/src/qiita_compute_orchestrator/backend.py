"""Compute backend abstraction."""

from abc import ABC, abstractmethod
from pathlib import Path


class ComputeBackend(ABC):
    """Abstract base for compute backends (local, SLURM, etc.)."""

    @abstractmethod
    async def run_hash_job(self, fasta_path: Path, output_dir: Path, reference_idx: int) -> Path:
        """Read sequences, compute MD5 hashes, write manifest.

        Returns the path to the manifest JSON file.
        """

    @abstractmethod
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
        """Write reference data to sorted Parquet files.

        manifest_path: JSON manifest from run_hash_job.
        feature_map_path: NDJSON file with {sequence_hash, feature_idx} rows,
            produced by the pipeline coordinator from the mint response.

        Both files are read directly by DuckDB — no Python-side parsing.

        Always produces:
          - reference_sequences.parquet (metadata: hash + length)
          - reference_sequence_chunks.parquet (chunked sequence data)
          - reference_membership.parquet

        Optional (when paths provided):
          - reference_taxonomy.parquet (from Parquet input with feature_id + taxonomy)
          - reference_phylogeny.parquet (from Newick, with feature_idx on tips)
          - reference_placements.parquet (from jplace)

        Returns the path to the output directory.
        """
