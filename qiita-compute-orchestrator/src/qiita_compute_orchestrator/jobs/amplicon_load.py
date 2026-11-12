"""join the deblur counts to their feature_idx and write the amplicon tables.

the tail of the amplicon workflow. it writes two DuckLake-shape staging
parquets, both keyed off mint-features' feature_map:

  amplicon_membership (prep_sample_idx, processing_idx, feature_idx, count)
    asv_counts JOIN feature_map on sequence_hash
  amplicon_sequence (feature_idx, sequence_hash, sequence_length_bp, sequence)
    one row per ASV feature, its bytes stored inline — reassembled from deblur's
    hash-keyed asv_chunks and joined to feature_idx. an ASV is short (a single
    64 KB chunk at most), so there is no separate chunk table: the chunking the
    reference tail needs for genomes is pure overhead here.

each output basename is the table name, so register-files loads it by stem.
membership is replace-keyed on (prep_sample_idx, processing_idx); amplicon_sequence
is replace-keyed on feature_idx.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
from pydantic import BaseModel
from qiita_common.amplicon_constants import (
    AMPLICON_MEMBERSHIP_BASENAME,
    AMPLICON_SEQUENCE_BASENAME,
)
from qiita_common.parquet import validate_parquet_path
from qiita_common.step_log import summarize

from ..miint import (
    PARQUET_OPTS_AT_REST,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_conn,
    resolve_duckdb_memory_gb,
)
from ._feature_load import build_feature_id_map

YAML_STEP_NAME = "amplicon_load"

_LOG = logging.getLogger(__name__)

# the join is light; DuckDB stays modest.
_DUCKDB_MEMORY_GB = 4
_DUCKDB_THREADS = 4


class Inputs(BaseModel):
    """input contract for amplicon_load.

    asv_counts: deblur's per-sample counts (prep_sample_idx, sequence_hash, count).
    feature_map: mint-features' (sequence_hash, feature_idx) map.
    manifest: deblur's (read_id, sequence_hash, sequence_length_bp) per ASV.
    asv_chunks: deblur's hash-keyed chunk directory carrying each ASV's bytes.
    processing_idx: threaded via the step's `params:`. work_ticket_idx is injected.
        no prep_sample_idx; it comes from asv_counts (the pool is the scope).
    """

    asv_counts: Path
    feature_map: Path
    manifest: Path
    asv_chunks: Path
    processing_idx: int
    work_ticket_idx: int


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    for label, path in [
        ("asv_counts", inputs.asv_counts),
        ("feature_map", inputs.feature_map),
        ("manifest", inputs.manifest),
        ("asv_chunks", inputs.asv_chunks),
    ]:
        if not path.exists():
            raise FileNotFoundError(f"{label} not found: {path}")

    workspace.mkdir(parents=True, exist_ok=True)
    staging = workspace / "amplicon_staging"
    staging.mkdir(parents=True, exist_ok=True)
    membership_out = validate_parquet_path(staging / AMPLICON_MEMBERSHIP_BASENAME)
    sequences_out = validate_parquet_path(staging / AMPLICON_SEQUENCE_BASENAME)

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS),
                threads=_DUCKDB_THREADS,
            )
            conn.execute(
                "CREATE TEMP TABLE feature_map AS SELECT * FROM read_parquet(?)",
                [str(inputs.feature_map)],
            )
            # gap check: every manifest hash must have a minted feature_idx. runs
            # first so the membership join below can never silently drop a count.
            build_feature_id_map(conn, inputs.manifest)
            membership = _write_amplicon_membership(
                conn,
                asv_counts_path=inputs.asv_counts,
                processing_idx=inputs.processing_idx,
                out=membership_out,
            )
            _write_amplicon_sequences(conn, inputs.asv_chunks, sequences_out)
            samples, features = conn.execute(
                "SELECT count(DISTINCT prep_sample_idx), count(DISTINCT feature_idx)"
                " FROM read_parquet(?)",
                [membership_out],
            ).fetchone()
            sequences = conn.execute(
                "SELECT count(*) FROM read_parquet(?)", [sequences_out]
            ).fetchone()[0]
            _LOG.info(
                "staged %d membership rows (%d samples, %d features), %d ASV sequences",
                membership,
                samples,
                features,
                sequences,
            )
            summarize(membership_rows=membership, samples=samples, sequences=sequences)
        success = True
    finally:
        if not success:
            for path in (Path(membership_out), Path(sequences_out)):
                path.unlink(missing_ok=True)

    return {"staging_dir": staging}


def _write_amplicon_membership(
    conn: duckdb.DuckDBPyConnection,
    *,
    asv_counts_path: Path,
    processing_idx: int,
    out: str,
) -> int:
    """write `amplicon_membership`: one row per (prep_sample, processing,
    feature_idx) with its count. joins asv_counts to the feature_map TEMP TABLE on
    sequence_hash and stamps processing_idx, sorted for pruning. the inner join
    drops nothing: `build_feature_id_map` already failed loud on any manifest hash
    without a feature_idx, and asv_counts carries the same hash set. returns the
    rows written."""
    return conn.execute(
        "COPY ("
        "  SELECT"
        "    c.prep_sample_idx,"
        f"    CAST({processing_idx} AS BIGINT) AS processing_idx,"
        "    fm.feature_idx,"
        "    c.count"
        "  FROM read_parquet(?) c"
        "  JOIN feature_map fm ON c.sequence_hash = fm.sequence_hash"
        "  ORDER BY prep_sample_idx, feature_idx"
        f") TO '{out}' ({PARQUET_OPTS_AT_REST})",
        [str(asv_counts_path)],
    ).fetchone()[0]


def _write_amplicon_sequences(
    conn: duckdb.DuckDBPyConnection,
    asv_chunks_path: Path,
    out: str,
) -> None:
    """write `amplicon_sequence`: one row per ASV feature_idx with its hash, length,
    and the sequence inline. Reassembles each ASV from deblur's hash-keyed `asv_chunks`
    (a directory of `part_*.parquet` of `(sequence_hash, chunk_index, chunk_data)`) and
    joins the feature_map TEMP TABLE for the feature_idx. An ASV is short, so this is a
    plain `string_agg` per hash — none of the bin-packing the genome chunk writer needs.
    The inner join drops nothing: `build_feature_id_map` already failed loud on any
    manifest hash without a feature_idx, and asv_chunks carries the same hash set."""
    conn.execute(
        "COPY ("
        "  WITH seq AS ("
        "    SELECT sequence_hash,"
        "           string_agg(chunk_data, '' ORDER BY chunk_index) AS sequence"
        "    FROM read_parquet(?) GROUP BY sequence_hash"
        "  )"
        "  SELECT fm.feature_idx, seq.sequence_hash,"
        "         length(seq.sequence)::INTEGER AS sequence_length_bp,"
        "         seq.sequence"
        "  FROM seq JOIN feature_map fm ON seq.sequence_hash = fm.sequence_hash"
        "  ORDER BY fm.feature_idx"
        f") TO '{out}' ({PARQUET_OPTS_AT_REST})",
        [str(asv_chunks_path / "part_*.parquet")],
    )
