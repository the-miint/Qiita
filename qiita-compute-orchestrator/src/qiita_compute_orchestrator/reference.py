"""Reference load pipeline — coordinates load job, registration, and status updates."""

import json
from pathlib import Path

from qiita_common.client import ControlPlaneClient
from qiita_common.models import ReferenceStatus

from .backend import ComputeBackend

# Maps Parquet filenames produced by run_load_job to DuckLake table names.
_REFERENCE_TABLE_MAP = {
    "reference_sequences.parquet": "reference_sequences",
    "reference_sequence_chunks.parquet": "reference_sequence_chunks",
    "reference_membership.parquet": "reference_membership",
    "reference_taxonomy.parquet": "reference_taxonomy",
    "reference_phylogeny.parquet": "reference_phylogeny",
    "reference_placements.parquet": "reference_placements",
}


def write_feature_map_ndjson(mapping: dict[str, int], path: Path) -> None:
    """Write the mint response mapping as NDJSON for DuckDB consumption.

    Each line is {"sequence_hash": "uuid-str", "feature_idx": N}.
    """
    with open(path, "w") as f:
        for seq_hash, feature_idx in mapping.items():
            f.write(json.dumps({"sequence_hash": seq_hash, "feature_idx": feature_idx}))
            f.write("\n")


async def run_reference_load_pipeline(
    *,
    backend: ComputeBackend,
    client: ControlPlaneClient,
    reference_idx: int,
    manifest_path: Path,
    fasta_path: Path,
    feature_map_path: Path,
    staging_dir: Path,
    taxonomy_path: Path | None = None,
    tree_path: Path | None = None,
    jplace_path: Path | None = None,
) -> None:
    """Run the full reference load pipeline.

    Steps:
    1. Transition status to LOADING
    2. Run load job (write sorted Parquet files to staging_dir)
    3. Request file registration via control plane → data plane DoAction
    4. Transition status to ACTIVE
    """
    # 1. Transition to LOADING
    await client.update_reference_status(reference_idx, ReferenceStatus.LOADING)

    # 2. Run load job
    await backend.run_load_job(
        manifest_path=manifest_path,
        fasta_path=fasta_path,
        feature_map_path=feature_map_path,
        output_dir=staging_dir,
        reference_idx=reference_idx,
        taxonomy_path=taxonomy_path,
        tree_path=tree_path,
        jplace_path=jplace_path,
    )

    # 3. Register files via control plane → data plane DoAction.
    await client.register_files(
        reference_idx=reference_idx,
        staging_dir=str(staging_dir),
        files=_REFERENCE_TABLE_MAP,
    )

    # 4. Transition to ACTIVE
    await client.update_reference_status(reference_idx, ReferenceStatus.ACTIVE)
