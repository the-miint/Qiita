"""Native job: alignment + feature windows -> a feature table of mean coverage depth.

The consumer half of the coverage mechanism. Generic by construction — it knows about
alignments and annotated intervals, not about SynDNA:

  * the SynDNA path feeds it `syndna_alignment` (reads aligned to the spike-in PLASMIDS,
    windows = the inserts on them);
  * a genome path feeds it an alignment against a genome reference (windows = that
    reference's annotated genes).

Both produce the same table. The arithmetic and every threshold live in `_coverage`; this
module is the plumbing around it — pull the windows, run it, write the Parquet.

**The windows come over Flight, not off disk.** `reference_annotation` is read from the
data plane with a `reference_idx`-scoped DoGet ticket, the same way the shard builders pull
sequence chunks. The rows are small (one per annotated interval) and DuckDB pulls the
stream lazily, so nothing is staged.

**Output basename is the DuckLake table name.** `coverage.parquet` -> `qiita_lake.coverage`,
via the `register-files` convention (the runner maps a staging dir's `<table>.parquet` to
`<table>`). Renaming it is a cross-component contract break.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel
from qiita_common.parquet import validate_parquet_path

from ..data_plane_client import open_reference_table_stream
from ..miint import (
    PARQUET_OPTS,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)
from ._coverage import (
    DEPTH_MODE_INCLUDE_DELETIONS,
    compute_feature_depth,
)

YAML_STEP_NAME = "coverage_depth"

# The job holds a per-base depth array per (sample, parent) — parent-length, not
# reference-length — so the envelope is set by the biggest annotated PARENT, not by the
# read count. A SynDNA plasmid is 17 kb; a microbial genome ~1e6. 8 GB is generous for
# both. See `_coverage` for why a human chromosome would not fit and is out of scope.
_DUCKDB_MEMORY_GB = 8
_DUCKDB_THREADS = 4

# The measurement thresholds. Settled with the assay owner, and hashed into the CP-minted
# `coverage_idx` — a change here re-mints rather than silently reusing a coverage_idx
# whose stored params describe the old filter.
_MIN_IDENTITY = 0.95
_MIN_ALIGNED_FRACTION = 0.90
_DEPTH_MODE = DEPTH_MODE_INCLUDE_DELETIONS

_ANNOTATION = "coverage_annotation"
_ALIGNMENT = "coverage_alignment"
_SAMPLE = "coverage_sample"
_WINDOW = "coverage_window"
_PARENT_LEN = "coverage_parent_len"
_OUT = "coverage_out"


class Inputs(BaseModel):
    """Typed input contract for coverage_depth.

    `alignment` is the upstream step's alignment Parquet — `(prep_sample_idx,
    sequence_idx, parent_feature_idx, flags, position, stop_position, cigar)`, UNGATED
    (mapped-primary only). The measurement gate is applied here, so it is defined once and
    the reads a mask calls spike-in and the reads counted toward depth cannot disagree.

    `reference_idx` names the reference whose annotated intervals are being quantified —
    the SynDNA spike-in reference for the mask path. It is a `params:` value, not the
    framework-injected scope scalar: a read-mask ticket is scoped to a prep_sample, not a
    reference.

    `coverage_idx` is the CP-minted identity of this measurement (the params-hash pattern
    `mask_idx` / `alignment_idx` use); every emitted row carries it.
    """

    alignment: Path
    reference_idx: int
    coverage_idx: int
    prep_sample_idx: int | None = None
    work_ticket_idx: int


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    if not inputs.alignment.exists():
        raise FileNotFoundError(f"alignment parquet not found: {inputs.alignment}")

    workspace.mkdir(parents=True, exist_ok=True)
    # `register-files` maps <table>.parquet -> the DuckLake table. This basename IS the
    # contract with qiita_lake.coverage.
    staging_dir = workspace / "coverage_staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    coverage_path = staging_dir / "coverage.parquet"
    out_sql = validate_parquet_path(coverage_path)
    alignment_sql = validate_parquet_path(inputs.alignment)

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS),
                threads=_DUCKDB_THREADS,
            )

            conn.execute(
                f"CREATE VIEW {_ALIGNMENT} AS SELECT * FROM read_parquet('{alignment_sql}')"
            )
            # The samples this ticket MEASURED. Taken from the ticket's scope, not from the
            # alignment: a sample with no spike-in reads at all produces no alignment rows,
            # and it must still appear in the feature table with zeros. Deriving the sample
            # set from the alignment would make "measured, and it was zero" indistinguishable
            # from "not measured".
            conn.execute(
                f"CREATE VIEW {_SAMPLE} AS "
                f"SELECT DISTINCT prep_sample_idx FROM {_ALIGNMENT} "
                + (
                    f"UNION SELECT {inputs.prep_sample_idx}::BIGINT"
                    if inputs.prep_sample_idx is not None
                    else ""
                )
            )

            # The feature windows, over Flight. `reference_annotation` carries reference_idx
            # itself, so the ticket scopes it with no membership join.
            async with open_reference_table_stream(
                conn,
                reference_idx=inputs.reference_idx,
                table="reference_annotation",
                relation=_ANNOTATION,
            ) as annotation_rel:
                conn.execute(
                    f"CREATE TABLE {_WINDOW} AS "
                    "SELECT feature_idx, parent_feature_idx, position, stop_position "
                    f"FROM {annotation_rel}"
                )

            n_windows = conn.execute(f"SELECT count(*) FROM {_WINDOW}").fetchone()[0]
            if n_windows == 0:
                # A reference with no annotations cannot be quantified per-interval. Fail
                # rather than write an empty feature table: an empty result is
                # indistinguishable from "every insert had zero coverage", which is a
                # meaningful and very different finding.
                raise ValueError(
                    f"reference {inputs.reference_idx} has no annotated intervals — "
                    "nothing to quantify. A coverage reference must be ingested with a "
                    "GFF3 (`qiita reference load --gff`)."
                )

            # The parents' lengths — `compute_coverage_depth` sizes its per-base array to
            # them. Derived from the alignment's own coordinates rather than fetched: an
            # alignment's `stop_position` is bounded by the parent's length, so max() over
            # the reference is a LOWER bound, and a lower bound would truncate the array and
            # silently lose coverage past it. So fetch the real lengths.
            async with open_reference_table_stream(
                conn,
                reference_idx=inputs.reference_idx,
                table="reference_sequences",
                relation="coverage_sequences",
            ) as sequences_rel:
                conn.execute(
                    f"CREATE TABLE {_PARENT_LEN} AS "
                    "SELECT feature_idx, sequence_length_bp "
                    f"FROM {sequences_rel} "
                    f"WHERE feature_idx IN (SELECT DISTINCT parent_feature_idx FROM {_WINDOW})"
                )

            missing = conn.execute(
                f"SELECT count(DISTINCT w.parent_feature_idx) FROM {_WINDOW} w "
                f"LEFT JOIN {_PARENT_LEN} p ON p.feature_idx = w.parent_feature_idx "
                "WHERE p.feature_idx IS NULL"
            ).fetchone()[0]
            if missing:
                raise ValueError(
                    f"{missing} annotated parent(s) of reference {inputs.reference_idx} have "
                    "no sequence length — the reference's annotations and its sequences "
                    "disagree"
                )

            compute_feature_depth(
                conn,
                alignment_relation=_ALIGNMENT,
                sample_relation=_SAMPLE,
                window_relation=_WINDOW,
                parent_length_relation=_PARENT_LEN,
                min_identity=_MIN_IDENTITY,
                min_aligned_fraction=_MIN_ALIGNED_FRACTION,
                depth_mode=_DEPTH_MODE,
                out_relation=_OUT,
            )

            # Sorted by the identifier prefix the lake reads on, per the project's
            # result-file rule (count/aggregation files sort by feature_idx, without
            # position).
            conn.execute(
                "COPY (SELECT "
                f"        {inputs.coverage_idx}::BIGINT AS coverage_idx, "
                "         prep_sample_idx, feature_idx, covered_bases, feature_length, "
                "         occurrences, mean_depth "
                f"      FROM {_OUT} "
                "      ORDER BY coverage_idx, prep_sample_idx, feature_idx) "
                f"TO '{out_sql}' ({PARQUET_OPTS})"
            )
        success = True
    finally:
        if not success:
            coverage_path.unlink(missing_ok=True)

    return {"coverage": coverage_path, "coverage_staging_dir": staging_dir}
