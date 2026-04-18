"""Compute backend abstraction."""

from abc import ABC, abstractmethod
from pathlib import Path
from uuid import UUID

# Mapping from sequence_hash (UUID) to feature_idx (int), as returned by
# the control plane's bulk mint endpoint. Used by load jobs to assign
# the correct feature_idx to each sequence.
FeatureMap = dict[UUID, int]


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
        feature_map: FeatureMap,
        output_dir: Path,
        reference_idx: int,
        *,
        taxonomy_path: Path | None = None,
        tree_path: Path | None = None,
        jplace_path: Path | None = None,
    ) -> Path:
        """Write reference data to sorted Parquet files.

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
