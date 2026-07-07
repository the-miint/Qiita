"""Native job: re-key assembly_hash's hash-keyed outputs to feature_idx and write
the four DuckLake-shape staging Parquets register-files hands to the data plane.

Tail of the pacbio-processing workflow, the assembly analogue of reference_load.
It REUSES reference_load's now-generic re-key writers verbatim — the shared
`qiita.feature` space means an assembled contig and a reference sequence with the
same bytes carry the same feature_idx, so the sequence + chunk writers are
identical. The four staging outputs (basename == DuckLake table name):

  - `assembled_sequence.parquet`        (feature_idx, sequence_hash, sequence_length_bp)
  - `assembled_sequence_chunks/part_*.parquet` (feature_idx, chunk_index, chunk_data)
  - `assembly_membership.parquet`       (prep_sample_idx, processing_idx, kind, bin_id, feature_idx)
  - `bin_quality.parquet`               per-MAG CheckM (+ DAS_Tool provenance)

The first two come straight from reference_load's `write_feature_sequences` /
`write_feature_sequence_chunks` (fed by `build_feature_id_map`). The membership
Parquet is the DuckLake copy of the Postgres `qiita.assembly_membership` the
`write-assembly-membership` action already wrote — joined here from `bin_map`
(read_id -> kind, bin_id) x `id_map` (read_id -> feature_idx) plus the run scalars.
`bin_quality` is read from the container's normalized TSVs with DuckDB's CSV reader
(never a Python csv parser).

Empty/partial semantics mirror the old pacbio_ingest: an LCG-only sample (contigs
but no MAG) is a SUCCESS — `bin_quality` is written empty (register-files still
finds all four tables with the right schema). Zero contigs never reaches here
(assembly_hash raised StepNoData upstream).
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
from pydantic import BaseModel
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    PARQUET_OPTS,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)
from .reference_load import (
    build_feature_id_map,
    write_feature_sequence_chunks,
    write_feature_sequences,
)

YAML_STEP_NAME = "assembly_load"

_KIND_MAG = "MAG"

# Normalized-intermediate basenames the container entrypoints write.
_CHECKM_TSV = "checkm_quality.tsv"
_DAS_SCORES_TSV = "das_tool_scores.tsv"

# DuckDB resource caps. Off-SLURM fallback; under SLURM the limit tracks the real
# cgroup via `resolve_duckdb_memory_gb()`. Sized to fit write_feature_sequence_chunks'
# per-batch sort (_CHUNK_BUDGET_PER_BATCH chunks, ~3.2 GB) with headroom.
_DUCKDB_MEMORY_GB = 8
_DUCKDB_THREADS = 4

# The bin_quality projection (DuckLake column order + types are load-bearing:
# ducklake.rs::ensure_assembly_tables). Every column is explicitly CAST so the
# ONE template serves all three write paths — populated (with/without DAS_Tool
# scores) and empty — with byte-identical schema (a bare NULL would otherwise get
# an ambiguous type in the empty Parquet). Each placeholder is either a source
# column reference (`c.completeness`, `d.das_tool_score`) or the literal `NULL`.
_BIN_QUALITY_SELECT = (
    "  CAST({ps} AS BIGINT) AS prep_sample_idx,"
    "  CAST({proc} AS BIGINT) AS processing_idx,"
    "  CAST({kind} AS VARCHAR) AS kind,"
    "  CAST({bin_id} AS VARCHAR) AS bin_id,"
    "  CAST({marker} AS VARCHAR) AS marker_lineage,"
    "  CAST({completeness} AS DOUBLE) AS completeness,"
    "  CAST({contamination} AS DOUBLE) AS contamination,"
    "  CAST({strain} AS DOUBLE) AS strain_heterogeneity,"
    "  CAST({genome_size} AS BIGINT) AS genome_size,"
    "  CAST({n_contigs} AS BIGINT) AS n_contigs,"
    "  CAST({das_score} AS DOUBLE) AS das_tool_score,"
    "  CAST({das_binner} AS VARCHAR) AS source_binner"
)


class Inputs(BaseModel):
    """Typed input contract for assembly_load.

    `manifest` / `feature_map` / `assembly_chunks` / `bin_map` are the upstream
    outputs (assembly_hash + mint-features). `checkm_dir` / `refined_bins_dir` are
    container-step outputs holding the CheckM + DAS_Tool TSVs. `processing_idx` is
    threaded via the step's `params:` (so the runner mints the run identity before
    the loop); `prep_sample_idx` / `work_ticket_idx` are framework-injected scope
    scalars.
    """

    manifest: Path
    feature_map: Path
    assembly_chunks: Path
    bin_map: Path
    checkm_dir: Path
    refined_bins_dir: Path
    processing_idx: int
    prep_sample_idx: int
    work_ticket_idx: int


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    for label, path in [
        ("manifest", inputs.manifest),
        ("feature_map", inputs.feature_map),
        ("assembly_chunks", inputs.assembly_chunks),
        ("bin_map", inputs.bin_map),
    ]:
        if not path.exists():
            raise FileNotFoundError(f"{label} not found: {path}")

    workspace.mkdir(parents=True, exist_ok=True)
    staging = workspace / "assembly_staging"
    staging.mkdir(parents=True, exist_ok=True)
    sequences_path = staging / "assembled_sequence.parquet"
    # assembled_sequence_chunks is a DIRECTORY of part_*.parquet (register-files
    # picks up a top-level subdir as a multi-file DuckLake table).
    chunks_dir = staging / "assembled_sequence_chunks"
    membership_path = staging / "assembly_membership.parquet"
    bin_quality_path = staging / "bin_quality.parquet"

    sequences_out = validate_parquet_path(sequences_path)
    membership_out = validate_parquet_path(membership_path)
    bin_quality_out = validate_parquet_path(bin_quality_path)

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS),
                threads=_DUCKDB_THREADS,
            )

            # feature_map TEMP TABLE + id_map (read_id -> feature_idx via
            # sequence_hash), exactly as reference_load.execute sets up — the reused
            # writers and the membership join both read them.
            conn.execute(
                "CREATE TEMP TABLE feature_map AS SELECT * FROM read_parquet(?)",
                [str(inputs.feature_map)],
            )
            build_feature_id_map(conn, inputs.manifest)

            # Reused verbatim from reference_load — the shared feature space means
            # the sequence + chunk writers are identical to the reference path.
            write_feature_sequences(conn, sequences_out)
            write_feature_sequence_chunks(conn, inputs.assembly_chunks, chunks_dir)

            _write_assembly_membership(
                conn,
                bin_map_path=inputs.bin_map,
                prep_sample_idx=inputs.prep_sample_idx,
                processing_idx=inputs.processing_idx,
                out=membership_out,
            )

            _write_bin_quality(
                conn,
                checkm_tsv=inputs.checkm_dir / _CHECKM_TSV,
                das_tsv=inputs.refined_bins_dir / _DAS_SCORES_TSV,
                prep_sample_idx=inputs.prep_sample_idx,
                processing_idx=inputs.processing_idx,
                out=bin_quality_out,
            )

            conn.execute("DROP TABLE id_map")
            conn.execute("DROP TABLE feature_map")
        success = True
    finally:
        if not success:
            for partial in (sequences_path, membership_path, bin_quality_path):
                partial.unlink(missing_ok=True)
            shutil.rmtree(chunks_dir, ignore_errors=True)

    return {"staging_dir": staging}


def _write_assembly_membership(
    conn: duckdb.DuckDBPyConnection,
    *,
    bin_map_path: Path,
    prep_sample_idx: int,
    processing_idx: int,
    out: str,
) -> None:
    """DuckLake copy of qiita.assembly_membership: one row per
    (prep_sample, processing, kind, bin_id, feature_idx). Joins `bin_map`
    (read_id -> kind, bin_id) against the `id_map` TEMP TABLE (read_id ->
    feature_idx) and stamps the run scalars. DISTINCT so a bin's duplicate
    (identical) contigs collapse to one row — matching the Postgres ON CONFLICT
    write the membership action performed."""
    conn.execute(
        "COPY ("
        "  SELECT DISTINCT"
        f"    CAST({prep_sample_idx} AS BIGINT) AS prep_sample_idx,"
        f"    CAST({processing_idx} AS BIGINT) AS processing_idx,"
        "    bm.kind AS kind, bm.bin_id AS bin_id, im.feature_idx AS feature_idx"
        "  FROM read_parquet(?) bm"
        "  JOIN id_map im ON bm.read_id = im.read_id"
        "  ORDER BY feature_idx"
        f") TO '{out}' ({PARQUET_OPTS})",
        [str(bin_map_path)],
    )


def _write_bin_quality(
    conn: duckdb.DuckDBPyConnection,
    *,
    checkm_tsv: Path,
    das_tsv: Path,
    prep_sample_idx: int,
    processing_idx: int,
    out: str,
) -> None:
    """Per-MAG CheckM quality (+ optional DAS_Tool provenance) -> the DuckLake
    `bin_quality` shape. Read with DuckDB's CSV reader (never a Python csv parser):
    columns by NAME (header=true) so a producer can reorder them. `kind` is 'MAG';
    `bin_id` is the CheckM `genome_local_id` (the MAG FASTA stem). DAS_Tool scores
    are LEFT-joined on genome_local_id when the file is present, else NULL.

    A sample with no CheckM table (LCG-only, or the CheckM DB was absent) writes a
    valid EMPTY Parquet with the right schema so register-files always finds the
    table."""
    if not checkm_tsv.is_file():
        # Empty write — every placeholder NULL, no FROM, WHERE FALSE.
        projection = _BIN_QUALITY_SELECT.format(
            ps="NULL",
            proc="NULL",
            kind="NULL",
            bin_id="NULL",
            marker="NULL",
            completeness="NULL",
            contamination="NULL",
            strain="NULL",
            genome_size="NULL",
            n_contigs="NULL",
            das_score="NULL",
            das_binner="NULL",
        )
        conn.execute(f"COPY (SELECT {projection} WHERE FALSE) TO '{out}' ({PARQUET_OPTS})")
        return

    # Populated write. DAS_Tool scores are optional: LEFT JOIN them on
    # genome_local_id when present, else the das columns are literal NULLs.
    has_das = das_tsv.is_file()
    projection = _BIN_QUALITY_SELECT.format(
        ps=prep_sample_idx,
        proc=processing_idx,
        kind=f"'{_KIND_MAG}'",
        bin_id="c.genome_local_id",
        marker="c.marker_lineage",
        completeness="c.completeness",
        contamination="c.contamination",
        strain="c.strain_heterogeneity",
        genome_size="c.genome_size",
        n_contigs="c.n_contigs",
        das_score="d.das_tool_score" if has_das else "NULL",
        das_binner="d.source_binner" if has_das else "NULL",
    )
    if has_das:
        source = (
            "  FROM read_csv(?, delim='\t', header=true, auto_detect=true) c"
            "  LEFT JOIN read_csv(?, delim='\t', header=true, auto_detect=true) d"
            "    ON c.genome_local_id = d.genome_local_id"
        )
        params = [str(checkm_tsv), str(das_tsv)]
    else:
        source = "  FROM read_csv(?, delim='\t', header=true, auto_detect=true) c"
        params = [str(checkm_tsv)]

    conn.execute(f"COPY (SELECT {projection} {source}) TO '{out}' ({PARQUET_OPTS})", params)
