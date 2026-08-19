"""Native job: join the deblur ASV counts to their minted feature_idx and write
the DuckLake `amplicon_membership` staging Parquet register-files hands to the
data plane. The tail of the amplicon workflow, the amplicon analogue of
assembly_load — but far lighter.

Unlike assembly_load it stores NO sequences and NO Postgres membership: an ASV's
bytes live in the shared feature space (mint-features already upserted the
sequence_hash), and `amplicon_membership` is a DuckLake-only surface the derived
feature table aggregates on demand (never itself a stored feature table). So the
job's whole work is one join:

  asv_counts (prep_sample_idx, sequence_hash, count)
    JOIN feature_map (sequence_hash, feature_idx)
    -> amplicon_membership.parquet (prep_sample_idx, processing_idx, feature_idx, count)

The single staging output's basename == the DuckLake table name, so register-files
loads it by stem. `amplicon_membership` is pure-append (not in the data plane's
REPLACE_KEY_TABLES) — a re-run with the same params reuses the same processing_idx,
so disallow-without-delete gates the re-append rather than a key replace.
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

# The join + COPY is light (a hash join over the pool's distinct ASVs); DuckDB
# stays modest. Off-SLURM fallback; under SLURM the limit tracks the cgroup.
_DUCKDB_MEMORY_GB = 4
_DUCKDB_THREADS = 4


class Inputs(BaseModel):
    """Typed input contract for amplicon_load.

    asv_counts: deblur's per-sample counts (prep_sample_idx, sequence_hash, count).
    feature_map: mint-features' (sequence_hash, feature_idx) map.
    processing_idx: threaded via the step's `params:` (so the runner mints the run
        identity before the loop). work_ticket_idx is a framework-injected scope
        scalar. NOTE: no prep_sample_idx here — amplicon is sequenced_pool-scoped
        and the per-sample prep_sample_idx comes from asv_counts itself.
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
    """DuckLake `amplicon_membership`: one row per (prep_sample, processing,
    feature_idx) with its abundance. Joins deblur's `asv_counts` to `feature_map`
    on sequence_hash (the same identity key mint-features keyed on) and stamps the
    run's processing_idx. Sorted (prep_sample_idx, feature_idx) for catalog pruning
    + row-group pushdown. An ASV whose sequence_hash is absent from feature_map is
    an upstream contract break (mint-features saw the same manifest), so an INNER
    join is deliberate — a missing feature_idx must fail loud downstream, not drop."""
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
