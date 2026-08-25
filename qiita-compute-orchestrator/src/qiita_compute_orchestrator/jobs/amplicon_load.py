"""join the deblur counts to their feature_idx and write `amplicon_membership`.

the tail of the amplicon workflow, and far lighter than assembly_load: it stores
no sequences and no Postgres membership. the whole job is one join:

  asv_counts (prep_sample_idx, sequence_hash, count)
    JOIN feature_map (sequence_hash, feature_idx)
    -> amplicon_membership.parquet (prep_sample_idx, processing_idx, feature_idx, count)

the output basename is the table name, so register-files loads it by stem. the
table is replace-keyed on (prep_sample_idx, processing_idx), so a re-run replaces
its own rows instead of doubling counts.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
from pydantic import BaseModel
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    PARQUET_OPTS,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_conn,
    resolve_duckdb_memory_gb,
)

YAML_STEP_NAME = "amplicon_load"

# the join is light; DuckDB stays modest.
_DUCKDB_MEMORY_GB = 4
_DUCKDB_THREADS = 4


class Inputs(BaseModel):
    """input contract for amplicon_load.

    asv_counts: deblur's per-sample counts (prep_sample_idx, sequence_hash, count).
    feature_map: mint-features' (sequence_hash, feature_idx) map.
    processing_idx: threaded via the step's `params:`. work_ticket_idx is injected.
        no prep_sample_idx; it comes from asv_counts (the pool is the scope).
    """

    asv_counts: Path
    feature_map: Path
    processing_idx: int
    work_ticket_idx: int


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    for label, path in [("asv_counts", inputs.asv_counts), ("feature_map", inputs.feature_map)]:
        if not path.exists():
            raise FileNotFoundError(f"{label} not found: {path}")

    workspace.mkdir(parents=True, exist_ok=True)
    staging = workspace / "amplicon_staging"
    staging.mkdir(parents=True, exist_ok=True)
    membership_path = staging / "amplicon_membership.parquet"
    membership_out = validate_parquet_path(membership_path)

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS),
                threads=_DUCKDB_THREADS,
            )
            _write_amplicon_membership(
                conn,
                asv_counts_path=inputs.asv_counts,
                feature_map_path=inputs.feature_map,
                processing_idx=inputs.processing_idx,
                out=membership_out,
            )
        success = True
    finally:
        if not success:
            membership_path.unlink(missing_ok=True)

    return {"staging_dir": staging}


def _write_amplicon_membership(
    conn: duckdb.DuckDBPyConnection,
    *,
    asv_counts_path: Path,
    feature_map_path: Path,
    processing_idx: int,
    out: str,
) -> None:
    """write `amplicon_membership`: one row per (prep_sample, processing,
    feature_idx) with its count. joins asv_counts to feature_map on sequence_hash
    and stamps processing_idx, sorted for pruning. the inner join is deliberate;
    a missing feature_idx should fail loud, not drop."""
    conn.execute(
        "COPY ("
        "  SELECT"
        "    c.prep_sample_idx AS prep_sample_idx,"
        f"    CAST({processing_idx} AS BIGINT) AS processing_idx,"
        "    fm.feature_idx AS feature_idx,"
        "    c.count AS count"
        "  FROM read_parquet(?) c"
        "  JOIN read_parquet(?) fm ON c.sequence_hash = fm.sequence_hash"
        "  ORDER BY prep_sample_idx, feature_idx"
        f") TO '{out}' ({PARQUET_OPTS})",
        [str(asv_counts_path), str(feature_map_path)],
    )
