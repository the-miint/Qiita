"""Native job: re-key assembly_hash's hash-keyed outputs to feature_idx and write
the four DuckLake-shape staging Parquets register-files hands to the data plane.

Tail of the long-read-assembly workflow, the assembly analogue of reference_load.
It REUSES the shared `_feature_load` re-key writers verbatim — the shared
`qiita.feature` space means an assembled contig and a reference sequence with the
same bytes carry the same feature_idx, so the sequence + chunk writers are
identical. The four staging outputs (basename == DuckLake table name):

  - `assembled_sequence.parquet`        (feature_idx, sequence_hash, sequence_length_bp)
  - `assembled_sequence_chunks/part_*.parquet` (feature_idx, chunk_index, chunk_data)
  - `assembly_membership.parquet`       (prep_sample_idx, processing_idx, kind, bin_id, feature_idx)
  - `bin_quality.parquet`               per-genome CheckM (+ DAS_Tool provenance)

The first two come straight from the shared `_feature_load.write_feature_sequences` /
`write_feature_sequence_chunks` (fed by `build_feature_id_map`). The membership
Parquet is the DuckLake copy of the Postgres `qiita.assembly_membership` the
`write-assembly-membership` action already wrote — joined here from `bin_map`
(read_id -> kind, bin_id) x `id_map` (read_id -> feature_idx) plus the run scalars.
`bin_quality` is built here by reading the container steps' RAW tool output with
DuckDB's CSV reader and doing ALL the column-selection/join/rename in SQL — the
containers emit CheckM's / DAS_Tool's tables verbatim (a plain `cp`, no awk/python
normalization), so DuckDB is the ONE csv framework in this path (never a Python
csv parser, never a shell transform on the tool tables).

Empty/partial semantics: a sample with contigs but nothing CheckM scored — no
refined bin and nothing circular — is a SUCCESS; `bin_quality` is written empty
(register-files still finds all four tables with the right schema). Zero contigs
never reaches here (assembly_hash raised StepNoData upstream).

`bin_quality` holds MAG and LCG rows, one per genome CheckM scored, tagged with the
`kind` of the checkm.sh run that produced it. UNBINNED rows are absent: the residue
is stored in `assembly_membership` so it can be queried, but a contig no bin claimed
is a fragment and is not scored against a marker set. So an unbinned membership row
has no `bin_quality` counterpart by construction rather than by filter.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
from pydantic import BaseModel
from qiita_common.assembly_constants import (
    CONTIG_ATTRIBUTE_REPRESENTATIVE_SQL,
    CONTIG_ATTRIBUTES_FILE,
    KIND_LCG,
    KIND_MAG,
    contig_attribute_join,
    contig_attribute_projection,
    register_contig_attribute_table,
)
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    PARQUET_OPTS,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)
from ._feature_load import (
    build_feature_id_map,
    write_feature_sequence_chunks,
    write_feature_sequences,
)

YAML_STEP_NAME = "assembly_load"

# RAW tool-output basenames the container entrypoints emit verbatim (no
# normalization — DuckDB does the column-selection/join/rename below). checkm.sh
# writes CheckM's two `--tab_table` outputs; bin_refine.sh copies DAS_Tool's
# summary. Column names below are pinned to CheckM 1.x (`resultsParser.py`) and
# DAS_Tool 1.1.x (`_DASTool_summary.tsv`).
_CHECKM_LINEAGE_TSV = "lineage.tsv"  # `checkm lineage_wf --tab_table`
_CHECKM_QA_TSV = "qa.tsv"  # `checkm qa -o 2 --tab_table`
# The same two tables from checkm.sh's second run, over the circular genomes. A
# separate pair rather than more rows in the first, because the file a row came from
# is what carries its `kind` — checkm.sh states why.
_CHECKM_LCG_LINEAGE_TSV = "lcg_lineage.tsv"
_CHECKM_LCG_QA_TSV = "lcg_qa.tsv"
_DAS_SUMMARY_TSV = "das_tool_summary.tsv"  # DAS_Tool `*_DASTool_summary.tsv`

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
    container-step outputs holding CheckM's raw `lineage.tsv` + `qa.tsv` and
    DAS_Tool's raw `das_tool_summary.tsv`. `genomes_dir` is the assemble step's
    output, read here only for the per-contig attribute sidecar the entrypoint
    wrote beside the two FASTAs. `processing_idx` is
    threaded via the step's `params:` (so the runner mints the run identity before
    the loop); `prep_sample_idx` / `work_ticket_idx` are framework-injected scope
    scalars.
    """

    manifest: Path
    feature_map: Path
    assembly_chunks: Path
    bin_map: Path
    genomes_dir: Path
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
    # Checked as a directory, unlike the sidecar inside it: an absent sidecar is
    # a run that predates it and stores NULLs, so a mis-bound genomes_dir would
    # otherwise be indistinguishable from that and silently null a whole run.
    if not inputs.genomes_dir.is_dir():
        raise FileNotFoundError(f"genomes_dir not found: {inputs.genomes_dir}")

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
            # sequence_hash), exactly as reference_load.execute sets up — the shared
            # _feature_load writers and the membership join both read them.
            conn.execute(
                "CREATE TEMP TABLE feature_map AS SELECT * FROM read_parquet(?)",
                [str(inputs.feature_map)],
            )
            build_feature_id_map(conn, inputs.manifest)

            register_contig_attribute_table(conn, inputs.genomes_dir / CONTIG_ATTRIBUTES_FILE)

            # Reused verbatim from _feature_load — the shared feature space means
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
                checkm_dir=inputs.checkm_dir,
                das_tsv=inputs.refined_bins_dir / _DAS_SUMMARY_TSV,
                prep_sample_idx=inputs.prep_sample_idx,
                processing_idx=inputs.processing_idx,
                out=bin_quality_out,
            )

            conn.execute("DROP TABLE contig_attribute")
            conn.execute("DROP TABLE id_map")
            conn.execute("DROP TABLE feature_map")
        success = True
    finally:
        if not success:
            for partial in (sequences_path, membership_path, bin_quality_path):
                partial.unlink(missing_ok=True)
            shutil.rmtree(chunks_dir, ignore_errors=True)

    return {"staging_dir": staging}


# DuckDB read_csv over a tab-delimited tool table with verbatim (spaced /
# parenthesized / '#'-prefixed) headers. header=true keeps the raw column names so
# they are addressed by name below; auto_detect infers types (the projection CASTs
# regardless). No Python/awk ever touches these files — DuckDB is the sole parser.
_READ_TSV = "read_csv(?, delim='\t', header=true, auto_detect=true)"


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
    (read_id -> kind, bin_id, contig_id) against the `id_map` TEMP TABLE (read_id ->
    feature_idx), stamps the run scalars, and carries the assembler's per-contig
    attributes.

    GROUP BY, not DISTINCT. The key is unchanged, but attribute columns can differ
    between two rows the key collapses — a bin holding duplicate (identical)
    contigs is exactly that case, and it is why the DISTINCT was here. Adding the
    columns to a DISTINCT would emit one row per attribute variant and so break
    the primary key the Postgres write upserts on. Aggregating to ONE representative
    contig id first, then joining the attributes to it, keeps all four values from
    the SAME contig rather than mixing them across rows, which a per-column
    aggregate would do.

    The attribute join is LEFT: a contig with no row in the sidecar stores NULLs,
    which is the state of every run that predates the sidecar.

    This table is replace-keyed on (prep_sample_idx, processing_idx)
    (flight_service::REPLACE_KEY_TABLES), so a re-run supersedes the run's rows
    wholesale -- unlike the Postgres twin, which upserts and COALESCEs the four
    attributes. A replay with no sidecar therefore keeps them in Postgres and
    clears them here; Postgres is the authority for these columns."""
    conn.execute(
        "COPY ("
        "  WITH member AS ("
        "    SELECT"
        f"      CAST({prep_sample_idx} AS BIGINT) AS prep_sample_idx,"
        f"      CAST({processing_idx} AS BIGINT) AS processing_idx,"
        "      bm.kind AS kind, bm.bin_id AS bin_id, im.feature_idx AS feature_idx,"
        f"      {CONTIG_ATTRIBUTE_REPRESENTATIVE_SQL}"
        "    FROM read_parquet(?) bm"
        "    JOIN id_map im ON bm.read_id = im.read_id"
        "    GROUP BY bm.kind, bm.bin_id, im.feature_idx"
        "  )"
        "  SELECT m.prep_sample_idx, m.processing_idx, m.kind, m.bin_id, m.feature_idx,"
        f"    {contig_attribute_projection('a')}"
        "  FROM member m" + contig_attribute_join("m") + "  ORDER BY m.feature_idx"
        f") TO '{out}' ({PARQUET_OPTS})",
        [str(bin_map_path)],
    )


def _write_bin_quality(
    conn: duckdb.DuckDBPyConnection,
    *,
    checkm_dir: Path,
    das_tsv: Path,
    prep_sample_idx: int,
    processing_idx: int,
    out: str,
) -> None:
    """Per-genome CheckM quality (+ DAS_Tool provenance for MAGs) -> the DuckLake
    `bin_quality` shape, built entirely in DuckDB from the container's RAW tool
    output (never a Python csv parser).

    One table pair per class, so `kind` is which pair a row was in — the constants
    above name the two and checkm.sh states why they are scored apart. Each pair is
    joined on the verbatim `"Bin Id"` column:
    `lineage_wf` carries marker lineage + completeness/contamination/strain
    heterogeneity, `qa -o 2` adds genome size / # contigs. `bin_id` is that same
    `"Bin Id"`, which CheckM sets from the filename stem — a refined-bin FASTA's
    stem for a MAG, a contig id for an LCG — matching what `assembly_hash` wrote
    into `bin_map` for each kind, so both join `assembly_membership` on
    (prep_sample_idx, processing_idx, kind, bin_id).

    DAS_Tool's summary is LEFT-joined on its `bin` column (== the MAG stem) when
    present. It is MAG-only by construction: DAS_Tool consumes the binners' output
    and never sees a circular contig, so an LCG row's provenance columns are NULL
    rather than missing a join.

    UNBINNED gets no row: checkm.sh does not score the residue, so there is nothing
    to read. The module docstring states why.

    Either class may be absent (a sample with no refined bin, or none circular), and
    with both absent — including the case where the CheckM DB was missing — this
    writes a valid EMPTY Parquet with the right schema so register-files always
    finds the table. Column names are pinned to CheckM 1.x / DAS_Tool 1.1.x (see the
    module constants)."""
    # CheckM headers are verbatim (spaces / parens / '#'), so they are double-quoted.
    arms: list[str] = []
    params: list[str] = []
    for kind, lineage_name, qa_name, with_das in (
        (KIND_MAG, _CHECKM_LINEAGE_TSV, _CHECKM_QA_TSV, das_tsv.is_file()),
        (KIND_LCG, _CHECKM_LCG_LINEAGE_TSV, _CHECKM_LCG_QA_TSV, False),
    ):
        lineage_tsv = checkm_dir / lineage_name
        qa_tsv = checkm_dir / qa_name
        if not (lineage_tsv.is_file() and qa_tsv.is_file()):
            continue
        projection = _BIN_QUALITY_SELECT.format(
            ps=prep_sample_idx,
            proc=processing_idx,
            kind=f"'{kind}'",
            bin_id='lin."Bin Id"',
            marker='lin."Marker lineage"',
            completeness='lin."Completeness"',
            contamination='lin."Contamination"',
            strain='lin."Strain heterogeneity"',
            genome_size='qa."Genome size (bp)"',
            n_contigs='qa."# contigs"',
            das_score='das."bin_score"' if with_das else "NULL",
            das_binner='das."bin_set"' if with_das else "NULL",
        )
        source = f'  FROM {_READ_TSV} lin  JOIN {_READ_TSV} qa ON lin."Bin Id" = qa."Bin Id"'
        # `?` binds positionally across the whole statement, so each arm's paths are
        # appended in the order its own read_csv calls appear.
        params.extend([str(lineage_tsv), str(qa_tsv)])
        if with_das:
            source += f'  LEFT JOIN {_READ_TSV} das ON lin."Bin Id" = das."bin"'
            params.append(str(das_tsv))
        arms.append(f"SELECT {projection} {source}")

    if not arms:
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

    # UNION ALL, not UNION: the two arms are disjoint by `kind` and a dedupe here
    # would only cost a sort over every column.
    conn.execute(f"COPY ({' UNION ALL '.join(arms)}) TO '{out}' ({PARQUET_OPTS})", params)
