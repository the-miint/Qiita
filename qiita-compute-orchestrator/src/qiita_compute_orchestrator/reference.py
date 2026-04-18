"""Reference load pipeline — coordinates load job, DuckLake registration, and status updates."""

from pathlib import Path

from qiita_common.client import ControlPlaneClient
from qiita_common.models import ReferenceStatus

from .backend import ComputeBackend, FeatureMap
from .registration import register_staged_parquet

# Maps Parquet filenames produced by run_load_job to DuckLake table names.
_REFERENCE_TABLE_MAP = {
    "reference_sequences.parquet": "reference_sequences",
    "reference_sequence_chunks.parquet": "reference_sequence_chunks",
    "reference_membership.parquet": "reference_membership",
    "reference_taxonomy.parquet": "reference_taxonomy",
    "reference_phylogeny.parquet": "reference_phylogeny",
    "reference_placements.parquet": "reference_placements",
}


async def run_reference_load_pipeline(
    *,
    backend: ComputeBackend,
    client: ControlPlaneClient,
    reference_idx: int,
    manifest_path: Path,
    fasta_path: Path,
    feature_map: FeatureMap,
    staging_dir: Path,
    ducklake_connstr: str,
    ducklake_data_path: str,
    taxonomy_path: Path | None = None,
    tree_path: Path | None = None,
    jplace_path: Path | None = None,
) -> None:
    """Run the full reference load pipeline.

    Steps:
    1. Transition status to LOADING
    2. Run load job (write sorted Parquet files to staging_dir)
    3. Move Parquet to permanent storage and register in DuckLake
    4. Transition status to ACTIVE
    """
    # 1. Transition to LOADING
    await client.update_reference_status(reference_idx, ReferenceStatus.LOADING)

    # 2. Run load job
    await backend.run_load_job(
        manifest_path=manifest_path,
        fasta_path=fasta_path,
        feature_map=feature_map,
        output_dir=staging_dir,
        reference_idx=reference_idx,
        taxonomy_path=taxonomy_path,
        tree_path=tree_path,
        jplace_path=jplace_path,
    )

    # 3. Move to permanent storage and register in DuckLake
    register_staged_parquet(
        staging_dir=staging_dir,
        ducklake_connstr=ducklake_connstr,
        ducklake_data_path=ducklake_data_path,
        table_file_map=_REFERENCE_TABLE_MAP,
    )

    # 4. Transition to ACTIVE
    await client.update_reference_status(reference_idx, ReferenceStatus.ACTIVE)
