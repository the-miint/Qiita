"""Native job: re-key assembly_hash's hash-keyed outputs to feature_idx and write
the DuckLake-shape staging Parquets register-files hands to the data plane.

Tail of the long-read-assembly workflow, the assembly analogue of reference_load.
It REUSES the shared `_feature_load` re-key writers verbatim — the shared
`qiita.feature` space means an assembled contig and a reference sequence with the
same bytes carry the same feature_idx, so the sequence + chunk writers are
identical. The staging outputs (basename == DuckLake table name):

  - `assembled_sequence.parquet`        (feature_idx, sequence_hash, sequence_length_bp)
  - `assembled_sequence_chunks/part_*.parquet` (feature_idx, chunk_index, chunk_data)
  - `assembly_membership.parquet`       (prep_sample_idx, processing_idx, kind, bin_id,
                                         feature_idx, the contig attributes, genome_idx)
  - `bin_quality.parquet`               per-genome CheckM (+ DAS_Tool provenance)
  - `gene_call.parquet`, `gene_call_contig.parquet`  the gene_call step's rows, keyed

Each subject's `genome_idx` comes from `subject_genome_map`, which
export-subject-genome-map wrote from the genomes write-assembly-membership minted. The
gene-call rows name contigs and genes by sequence hash and subjects by `kind` and
`bin_id` (`gene_call_constants.GENE_CALL_STEP_IDENTIFIERS`); here they get their
`feature_idx` (contigs from `feature_map`, genes from mint-gene-features'
`gene_feature_map`) and `genome_idx`. A row any of these leaves unresolved fails the step.

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

Empty/partial semantics: a prep_sample with contigs but nothing CheckM scored — no
refined bin, nothing circular, and no residue contig clearing the length cut — is a
SUCCESS; `bin_quality` is written empty (register-files still finds every table
with the right schema). Zero contigs never reaches here (assembly_hash raised
StepNoData upstream).

`bin_quality` holds MAG, LCG and UNBINNED rows, one per subject CheckM scored, tagged
with the `kind` of the checkm.sh run that produced it. The UNBINNED rows are a SUBSET
of the UNBINNED membership rows: `residue_split.py` scores only the contigs above its
length cut, because completeness against a marker set describes nothing for a short
fragment and CheckM's cost is per subject scored. A short unbinned contig therefore has a
membership row and no quality row — read the two with a LEFT join, not an inner one.

A myloasm run's genomes_dir also carries the assemble step's provenance dump
(`<MYLOASM_RAW_DIR>/`). When it is there, this job adds the six `myloasm_*` staging
Parquets: `_myloasm_provenance` re-keys them, with the reads' names taken as their
`sequence_idx` and each primary contig's `feature_idx` taken from this run's own
`bin_map` x `id_map`. A run without the dump (hifiasm_meta) stages none of them.
The directory's presence is a sound test for a myloasm run: assemble.sh writes the
dump for every non-empty myloasm assembly or fails the step, and an empty one ends
the ticket as no-data at assembly_hash, before this step.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
from pydantic import BaseModel
from qiita_common.assembly_constants import (
    BIN_QUALITY_COLUMN_NAMES,
    CONTIG_ATTRIBUTE_REPRESENTATIVE_SQL,
    CONTIG_ATTRIBUTES_FILE,
    KIND_LCG,
    KIND_MAG,
    KIND_UNBINNED,
    bin_quality_projection,
    contig_attribute_join,
    contig_attribute_projection,
    register_contig_attribute_table,
)
from qiita_common.gene_call_constants import (
    GENE_CALL_COLUMNS,
    GENE_CALL_CONTIG_COLUMNS,
    GENE_CALL_CONTIG_TABLE,
    GENE_CALL_RUN_KEY,
    GENE_CALL_STEP_IDENTIFIERS,
    GENE_CALL_TABLE,
    GENE_CALL_TABLES,
)
from qiita_common.myloasm_constants import MYLOASM_RAW_DIR
from qiita_common.parquet import validate_parquet_path
from qiita_common.sql import cast_projection

from ..miint import (
    PARQUET_OPTS_AT_REST,
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
from ._myloasm_provenance import (
    CONTIG_FEATURE_TABLE,
    read_names_from_sequence_idx,
    register_raw_tables,
    staging_paths,
    write_myloasm_staging,
)
from .gene_call import CONTIG_FILE as GENE_CALL_CONTIG_FILE
from .gene_call import GENE_FILE as GENE_CALL_GENE_FILE

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
# And from its third run, over the residue: the unbinned contigs no refined bin
# claims, above the length cut `residue_split.py` applies. A subset of what this same
# job stores as KIND_UNBINNED — the short ones keep their membership row and have no
# quality row, which is what the whole class had before the third run existed.
_CHECKM_UNBINNED_LINEAGE_TSV = "unbinned_lineage.tsv"
_CHECKM_UNBINNED_QA_TSV = "unbinned_qa.tsv"
_DAS_SUMMARY_TSV = "das_tool_summary.tsv"  # DAS_Tool `*_DASTool_summary.tsv`

# DuckDB resource caps. Off-SLURM fallback; under SLURM the limit tracks the real
# cgroup via `resolve_duckdb_memory_gb()`, so the literal below binds only off SLURM
# (local backend and tests) and is dev-box sized, not cluster sized. The per-batch sort
# it has to fit is `CHUNK_BUDGET_PER_BATCH` chunks, ~3.2 GB raw — under SLURM that
# sort is what drove the step's YAML baseline to 32 GiB rather than 16. The
# measurement, and what it does not settle, are at that step's `baseline_resources`
# in `workflows/long-read-assembly/1.0.1.yaml` — the version that carries it; 1.0.0
# predates the re-size and still declares the 16 GiB this OOMed at.
_DUCKDB_MEMORY_GB = 8
_DUCKDB_THREADS = 4


class Inputs(BaseModel):
    """Typed input contract for assembly_load.

    `manifest` / `feature_map` / `assembly_chunks` / `bin_map` are the upstream
    outputs (assembly_hash + mint-features). `checkm_dir` / `refined_bins_dir` are
    container-step outputs holding CheckM's raw `lineage.tsv` + `qa.tsv` and
    DAS_Tool's raw `das_tool_summary.tsv`. `genomes_dir` is the assemble step's
    output, read here only for the per-contig attribute sidecar the entrypoint
    wrote beside the two FASTAs and, for a myloasm run, its provenance dump.
    `gene_calls` is the gene_call step's output directory, `gene_feature_map`
    mint-gene-features' map and `subject_genome_map` export-subject-genome-map's.
    `processing_idx` is
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
    gene_calls: Path
    gene_feature_map: Path
    subject_genome_map: Path
    processing_idx: int
    prep_sample_idx: int
    work_ticket_idx: int


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    for label, path in [
        ("manifest", inputs.manifest),
        ("feature_map", inputs.feature_map),
        ("assembly_chunks", inputs.assembly_chunks),
        ("bin_map", inputs.bin_map),
        ("gene_feature_map", inputs.gene_feature_map),
        ("subject_genome_map", inputs.subject_genome_map),
        ("gene_calls gene file", inputs.gene_calls / GENE_CALL_GENE_FILE),
        ("gene_calls contig file", inputs.gene_calls / GENE_CALL_CONTIG_FILE),
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
    gene_path = staging / f"{GENE_CALL_TABLE}.parquet"
    gene_contig_path = staging / f"{GENE_CALL_CONTIG_TABLE}.parquet"

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
            conn.execute(
                f"CREATE TEMP TABLE {_SUBJECT_GENOME} AS"
                " SELECT kind, bin_id, genome_idx FROM read_parquet(?)",
                [str(inputs.subject_genome_map)],
            )

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
            for out in (membership_out, bin_quality_out):
                _require_genome(conn, out)

            write_gene_call_staging(
                conn,
                gene_calls=inputs.gene_calls,
                gene_feature_map=inputs.gene_feature_map,
                prep_sample_idx=inputs.prep_sample_idx,
                processing_idx=inputs.processing_idx,
                gene_out=gene_path,
                contig_out=gene_contig_path,
            )

            myloasm_dump = inputs.genomes_dir / MYLOASM_RAW_DIR
            if myloasm_dump.is_dir():
                _write_myloasm_provenance(
                    conn,
                    raw_dir=myloasm_dump,
                    bin_map_path=inputs.bin_map,
                    staging=staging,
                    prep_sample_idx=inputs.prep_sample_idx,
                    processing_idx=inputs.processing_idx,
                )

            conn.execute("DROP TABLE contig_attribute")
            conn.execute(f"DROP TABLE {_SUBJECT_GENOME}")
            conn.execute("DROP TABLE id_map")
            conn.execute("DROP TABLE feature_map")
        success = True
    finally:
        if not success:
            for partial in (
                sequences_path,
                membership_path,
                bin_quality_path,
                gene_path,
                gene_contig_path,
                *staging_paths(staging).values(),
            ):
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
    (flight_service::registration::REPLACE_KEY_TABLES), so a re-run supersedes the run's rows
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
        f"    {contig_attribute_projection('a')}, sg.genome_idx"
        "  FROM member m"
        + contig_attribute_join("m")
        + f"  LEFT JOIN {_SUBJECT_GENOME} sg ON sg.kind = m.kind AND sg.bin_id = m.bin_id"
        "  ORDER BY m.feature_idx"
        f") TO '{out}' ({PARQUET_OPTS_AT_REST})",
        [str(bin_map_path)],
    )


def _write_myloasm_provenance(
    conn: duckdb.DuckDBPyConnection,
    *,
    raw_dir: Path,
    bin_map_path: Path,
    staging: Path,
    prep_sample_idx: int,
    processing_idx: int,
) -> None:
    """The six `myloasm_*` staging Parquets from the assemble step's dump.

    The reads were named by `sequence_idx` when the runner wrote the assembler's
    input, so each name is its own `sequence_idx`. A primary contig's `feature_idx`
    is this run's: `bin_map` names every contig assembly_hash hashed by the
    assembler's own id, and `id_map` carries the minted feature_idx for each."""
    register_raw_tables(conn, raw_dir)
    read_names_from_sequence_idx(conn)
    conn.execute(
        f"CREATE TEMP TABLE {CONTIG_FEATURE_TABLE} AS "
        "SELECT bm.contig_id, im.feature_idx "
        "FROM read_parquet(?) bm JOIN id_map im ON bm.read_id = im.read_id",
        [str(bin_map_path)],
    )
    write_myloasm_staging(
        conn, staging_dir=staging, prep_sample_idx=prep_sample_idx, processing_idx=processing_idx
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
    above name the three and checkm.sh states why they are scored apart. Each pair is
    joined on the verbatim `"Bin Id"` column:
    `lineage_wf` carries marker lineage + completeness/contamination/strain
    heterogeneity, `qa -o 2` adds genome size / # contigs. `bin_id` is that same
    `"Bin Id"`, which CheckM sets from the filename stem — a refined-bin FASTA's
    stem for a MAG, a contig id for an LCG or an UNBINNED contig — matching what
    `assembly_hash` wrote into `bin_map` for each kind, so all three join
    `assembly_membership` on
    (prep_sample_idx, processing_idx, kind, bin_id).

    DAS_Tool's summary is LEFT-joined on its `bin` column (== the MAG stem) when
    present. It is MAG-only by construction: DAS_Tool consumes the binners' output
    and never sees a circular contig, so an LCG row's provenance columns are NULL
    rather than missing a join.

    UNBINNED rows cover only the contigs checkm.sh scored — the residue above
    `residue_split.py`'s length cut. A short unbinned contig keeps its
    `assembly_membership` row and simply has no quality row, so `bin_quality` is a
    subset of `assembly_membership` for this kind and a LEFT join is the way to read
    the two together.

    Any class may be absent (a prep_sample with no refined bin, none circular, or no
    residue clearing the cut), and with all absent this writes a valid EMPTY Parquet
    with the right schema so register-files always finds the table. `checkm.sh` states
    what it does when there are genomes to score and no CheckM DB to score them with.

    Column names are pinned to CheckM 1.x / DAS_Tool 1.1.x (see the module
    constants)."""
    # CheckM headers are verbatim (spaces / parens / '#'), so they are double-quoted.
    arms: list[str] = []
    params: list[str] = []
    for kind, lineage_name, qa_name, with_das in (
        (KIND_MAG, _CHECKM_LINEAGE_TSV, _CHECKM_QA_TSV, das_tsv.is_file()),
        (KIND_LCG, _CHECKM_LCG_LINEAGE_TSV, _CHECKM_LCG_QA_TSV, False),
        (KIND_UNBINNED, _CHECKM_UNBINNED_LINEAGE_TSV, _CHECKM_UNBINNED_QA_TSV, False),
    ):
        lineage_tsv = checkm_dir / lineage_name
        qa_tsv = checkm_dir / qa_name
        if not (lineage_tsv.is_file() and qa_tsv.is_file()):
            continue
        projection = bin_quality_projection(
            {
                "prep_sample_idx": str(prep_sample_idx),
                "processing_idx": str(processing_idx),
                "kind": f"'{kind}'",
                "bin_id": 'lin."Bin Id"',
                "marker_lineage": 'lin."Marker lineage"',
                "completeness": 'lin."Completeness"',
                "contamination": 'lin."Contamination"',
                "strain_heterogeneity": 'lin."Strain heterogeneity"',
                "genome_size": 'qa."Genome size (bp)"',
                "n_contigs": 'qa."# contigs"',
                "das_tool_score": 'das."bin_score"' if with_das else "NULL",
                "source_binner": 'das."bin_set"' if with_das else "NULL",
                "genome_idx": "sg.genome_idx",
            }
        )
        source = (
            f'  FROM {_READ_TSV} lin  JOIN {_READ_TSV} qa ON lin."Bin Id" = qa."Bin Id"'
            f"  LEFT JOIN {_SUBJECT_GENOME} sg"
            f"    ON sg.kind = '{kind}' AND sg.bin_id = CAST(lin.\"Bin Id\" AS VARCHAR)"
        )
        # `?` binds positionally across the whole statement, so each arm's paths are
        # appended in the order its own read_csv calls appear.
        params.extend([str(lineage_tsv), str(qa_tsv)])
        if with_das:
            source += f'  LEFT JOIN {_READ_TSV} das ON lin."Bin Id" = das."bin"'
            params.append(str(das_tsv))
        arms.append(f"SELECT {projection} {source}")

    if not arms:
        # Empty write — every source NULL, no FROM, WHERE FALSE. `bin_quality_projection`
        # is what keeps this Parquet's schema identical to a populated one.
        projection = bin_quality_projection(dict.fromkeys(BIN_QUALITY_COLUMN_NAMES, "NULL"))
        conn.execute(f"COPY (SELECT {projection} WHERE FALSE) TO '{out}' ({PARQUET_OPTS_AT_REST})")
        return

    # UNION ALL, not UNION: the arms are disjoint by `kind` and a dedupe here
    # would only cost a sort over every column.
    conn.execute(f"COPY ({' UNION ALL '.join(arms)}) TO '{out}' ({PARQUET_OPTS_AT_REST})", params)


# The TEMP TABLE holding `subject_genome_map`: (kind, bin_id) -> genome_idx.
_SUBJECT_GENOME = "subject_genome"


def _require_genome(conn: duckdb.DuckDBPyConnection, out: str) -> None:
    """Every subject this run wrote has a genome in `subject_genome_map`, which
    export-subject-genome-map built from the same run's Postgres membership; a row
    without one means the two disagree about the run's subjects."""
    missing = conn.execute(
        "SELECT count(*) FROM read_parquet(?) WHERE genome_idx IS NULL", [out]
    ).fetchone()[0]
    if missing:
        raise RuntimeError(f"{out}: {missing} row(s) whose subject has no genome_idx")


def write_gene_call_staging(
    conn: duckdb.DuckDBPyConnection,
    *,
    gene_calls: Path,
    gene_feature_map: Path,
    prep_sample_idx: int,
    processing_idx: int,
    gene_out: Path,
    contig_out: Path,
) -> None:
    """The gene_call step's rows as `gene_call` / `gene_call_contig` staging: the run
    key added, each subject's `genome_idx` from the `subject_genome` table, each
    contig's `feature_idx` from `feature_map`, and each gene's from
    `gene_feature_map`. Inner joins, then a count against the input: a row that does
    not resolve fails the step rather than being dropped. Needs the `feature_map` and
    `subject_genome` TEMP TABLEs."""
    run = dict(zip(GENE_CALL_RUN_KEY, (str(prep_sample_idx), str(processing_idx)), strict=True))

    def sources(table: str, resolved: dict[str, str]) -> dict[str, str]:
        # The step's own columns as read; each one it names by identifiers instead
        # (`GENE_CALL_STEP_IDENTIFIERS`) from `resolved`.
        step = GENE_CALL_STEP_IDENTIFIERS[table]
        if set(resolved) != set(step):
            raise KeyError(f"{table}: resolved {sorted(resolved)}, step names {sorted(step)}")
        own = (n for n, _ in GENE_CALL_TABLES[table] if n not in run and n not in step)
        return run | {n: f"r.{n}" for n in own} | resolved

    gene_sources = sources(
        GENE_CALL_TABLE,
        {
            "genome_idx": "sg.genome_idx",
            "parent_feature_idx": "cf.feature_idx",
            "feature_idx": "gf.feature_idx",
        },
    )
    contig_sources = sources(
        GENE_CALL_CONTIG_TABLE, {"genome_idx": "sg.genome_idx", "feature_idx": "cf.feature_idx"}
    )
    subject_join = f"JOIN {_SUBJECT_GENOME} sg ON sg.kind = r.kind AND sg.bin_id = r.bin_id"
    for src, out, projection, joins, join_params in (
        (
            gene_calls / GENE_CALL_GENE_FILE,
            gene_out,
            cast_projection(GENE_CALL_TABLE, GENE_CALL_COLUMNS, gene_sources),
            f"{subject_join}"
            " JOIN feature_map cf ON cf.sequence_hash = r.parent_sequence_hash"
            " JOIN read_parquet(?) gf ON gf.sequence_hash = r.sequence_hash",
            [str(gene_feature_map)],
        ),
        (
            gene_calls / GENE_CALL_CONTIG_FILE,
            contig_out,
            cast_projection(GENE_CALL_CONTIG_TABLE, GENE_CALL_CONTIG_COLUMNS, contig_sources),
            f"{subject_join} JOIN feature_map cf ON cf.sequence_hash = r.sequence_hash",
            [],
        ),
    ):
        params = [str(src), *join_params]
        target = validate_parquet_path(out)
        conn.execute(
            f"COPY (SELECT {projection} FROM read_parquet(?) r {joins})"
            f" TO '{target}' ({PARQUET_OPTS_AT_REST})",
            params,
        )
        read, written = conn.execute(
            "SELECT (SELECT count(*) FROM read_parquet(?)), (SELECT count(*) FROM read_parquet(?))",
            [str(src), target],
        ).fetchone()
        if read != written:
            raise RuntimeError(
                f"{src.name}: {read} row(s) read, {written} resolved to a genome and features"
            )
