"""Reference load pipeline — coordinates load job, DuckLake registration, and status updates."""

import json
from pathlib import Path

from qiita_common.client import ControlPlaneClient
from qiita_common.models import PhylogenyTipEntry, ReferenceStatus

from .backend import ComputeBackend, FeatureMap
from .registration import register_staged_parquet

# Maps Parquet filenames produced by run_load_job to DuckLake table names.
_REFERENCE_TABLE_MAP = {
    "reference_sequences.parquet": "reference_sequences",
    "reference_taxonomy.parquet": "reference_taxonomy",
    "reference_phylogeny.parquet": "reference_phylogeny",
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
    2. Run load job (write Parquet files to staging_dir)
    3. Move Parquet to permanent storage and register in DuckLake
    4. Post phylogeny tip-feature mappings (if tree was loaded)
    5. Transition status to ACTIVE
    """
    # 1. Transition to LOADING
    await client.update_reference_status(reference_idx, ReferenceStatus.LOADING)

    # 2. Run load job — produces Parquet files + tip_features.json in staging_dir
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

    # 3. Move to permanent storage and register in DuckLake (metadata-only)
    register_staged_parquet(
        staging_dir=staging_dir,
        ducklake_connstr=ducklake_connstr,
        ducklake_data_path=ducklake_data_path,
        table_file_map=_REFERENCE_TABLE_MAP,
    )

    # 4. Post tip-feature mappings (tip_features.json stays in staging — it's
    #    consumed here and not registered in DuckLake)
    tip_path = staging_dir / "tip_features.json"
    if tip_path.exists():
        tips = json.loads(tip_path.read_text())
        if tips:
            entries = [PhylogenyTipEntry(**t) for t in tips]
            await client.write_phylogeny_tips(reference_idx, entries)

    # 5. Transition to ACTIVE
    await client.update_reference_status(reference_idx, ReferenceStatus.ACTIVE)
