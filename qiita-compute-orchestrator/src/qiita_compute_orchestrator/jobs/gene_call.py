"""Native job: call genes with Prodigal on every subject CheckM scores, once per call
set, through miint's `predict_genes`
(https://the-miint.github.io/duckdb-miint/gene_prediction/).

The subjects are the ones `checkm.sh` scores: each refined bin (MAG), each circular
contig (LCG), and each unbinned contig at or above the residue cut (UNBINNED). The
call sets, the lake columns and why coordinates are moved onto each contig's
canonical strand are `qiita_common.gene_call_constants`.

Outputs, in `gene_calls/`:

  - `gene.parquet`: one row per gene, the `gene_call` columns without the run key,
    the contig named by `parent_sequence_hash` and the gene by its own canonical
    `sequence_hash` and `sequence_length_bp`, for minting.
  - `contig.parquet`: one row per contig of each called subject, the
    `gene_call_contig` columns without the run key, the contig named by
    `parent_sequence_hash`.
  - `checkm/<kind>/<bin_id>/genes.faa` and `genes.gff`: the `prodigal_checkm` genes of
    each subject that has any, as Prodigal's CLI writes them for that subject's FASTA
    (contig names and strand as read, not canonical), laid out as CheckM's
    `bins/<bin_id>/` so it can reuse them instead of calling Prodigal. A subject
    without genes gets no files, and CheckM calls Prodigal on it as it does today.
    This is a file handed to a later step, not streamed: CheckM reads only files.

**The `prodigal_checkm` rules** are CheckM 1.2.5's `ProdigalRunner.run`
(`checkm/prodigal.py`): metagenomic mode for a subject under 100,000 bp, else single
mode with tables 4 and 11; `-m` (regions of N masked) throughout; a single-mode call
that fails or finds no genes is rerun in metagenomic mode; table 4 is kept only when
its coding density exceeds table 11's by more than 0.05 and is above 0.7, density
being the bases inside at least one gene over the subject's total length.
In metagenomic mode `predict_genes` takes table 11 only, and its models choose table
11 or 4 per contig (gene prediction docs, linked above), so a metagenomic call is made
once and stands for both tables.

The inputs are laid out as long-read-assembly's assemble and bin_refine steps write
them.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from pydantic import BaseModel
from qiita_common.assembly_constants import KIND_MAG, KIND_UNBINNED
from qiita_common.chunking import canonical_sequence_hash_expr, normalized_sequence_expr
from qiita_common.gene_call_constants import (
    CALL_SET_CHECKM,
    CALL_SET_META_CLOSED,
    GENE_CALL_STATUSES,
    STATUS_OK,
)
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    PARQUET_OPTS_TRANSFER,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)
from ._assembly import READ_FASTX_MAX_BATCH_BYTES
from .assembly_hash import _file_meta

YAML_STEP_NAME = "gene_call"

# The shortest unbinned contig CheckM scores. `residue_split.py` owns the value and why
# (it runs in the CheckM container and cannot import this package); a test pins the two
# equal, since a contig scored by one and not called by the other is a subject CheckM
# calls Prodigal on itself.
MIN_RESIDUE_LENGTH_BP = 300_000

# CheckM's switch from single to metagenomic mode, on the subject's total length.
_CHECKM_META_BELOW_BP = 100_000

# Starting values, taken from `assembly_hash`; not measured for this job. Prodigal runs
# out of process, beside DuckDB.
_DUCKDB_MEMORY_GB = 6
_DUCKDB_THREADS = 4

OUTPUT_DIR = "gene_calls"
GENE_FILE = "gene.parquet"
CONTIG_FILE = "contig.parquet"
CHECKM_DIR = "checkm"

# Prodigal's GFF attribute text for each `GENE_CALL_ATTRIBUTE_KEYS` entry, formatted as
# Prodigal prints it, so a GFF written back from the lake is Prodigal's.
ATTRIBUTE_EXPR = {
    "rbs_motif": "coalesce(g.rbs_motif, 'None')",
    "rbs_spacer": "coalesce(g.rbs_spacer, 'None')",
    "gc_cont": "printf('%.3f', g.gene_gc_content)",
    "cscore": "printf('%.2f', g.cscore)",
    "sscore": "printf('%.2f', g.sscore)",
    "rscore": "printf('%.2f', g.rscore)",
    "uscore": "printf('%.2f', g.uscore)",
    "tscore": "printf('%.2f', g.tscore)",
}


class Inputs(BaseModel):
    """`genomes_dir` (`circular.fa`, `noLCG.fa`) and `refined_bins_dir` (one FASTA per
    refined bin) are the assemble and bin_refine steps' outputs, as `assembly_hash`
    reads them. Nothing written here is keyed on the run; the load adds the key."""

    genomes_dir: Path
    refined_bins_dir: Path
    prep_sample_idx: int
    work_ticket_idx: int


def _sql_path(path: Path) -> str:
    """A COPY target as a SQL string literal (`validate_parquet_path` refuses what
    could not be one)."""
    return f"'{validate_parquet_path(path)}'"


def _dir_name(bin_id: str) -> str:
    """A bin_id becomes a directory under `checkm/<kind>/`; one that is not a plain
    name would land outside it."""
    if not bin_id or bin_id in (".", "..") or "/" in bin_id or "\0" in bin_id:
        raise ValueError(f"bin_id {bin_id!r} cannot name a directory")
    return bin_id


def _load_subjects(conn, genomes_dir: Path, refined_bins_dir: Path) -> None:
    """`contig`: every contig of every subject, with its subject (`genome_id`), its
    order in the subject's FASTA, its canonical hash, and whether it was read on the
    other strand. `subject`: one row per subject with its total length."""
    conn.execute("CREATE TEMP TABLE file_meta (filepath VARCHAR, kind VARCHAR, bin_id VARCHAR)")
    meta = _file_meta(genomes_dir, refined_bins_dir)
    if meta:
        conn.executemany("INSERT INTO file_meta VALUES (?, ?, ?)", meta)
        source = (
            f"read_fastx(?, max_batch_bytes := '{READ_FASTX_MAX_BATCH_BYTES}', "
            "include_filepath := true)"
        )
        params = [[row[0] for row in meta]]
    else:
        source = """(SELECT NULL::VARCHAR AS filepath, NULL::VARCHAR AS read_id,
                            NULL::VARCHAR AS sequence1, NULL::BIGINT AS sequence_index
                     WHERE false)"""
        params = []
    conn.execute(
        f"""
        CREATE TEMP TABLE contig_all AS
        SELECT *, md5({normalized_sequence_expr("sequence1")})::uuid <> sequence_hash AS flipped
        FROM (
            SELECT fm.kind, COALESCE(fm.bin_id, rf.read_id) AS bin_id,
                   rf.read_id AS contig_id, rf.sequence_index, rf.sequence1,
                   {canonical_sequence_hash_expr("rf.sequence1")} AS sequence_hash
            FROM {source} rf JOIN file_meta fm ON rf.filepath = fm.filepath)
        """,
        params,
    )
    # The unbinned subjects are the residue above the cut: the noLCG contigs no refined
    # bin claims, matched on the canonical hash as `assembly_hash` and `residue_split.py`
    # match them, at or above `MIN_RESIDUE_LENGTH_BP`.
    conn.execute(
        "CREATE TEMP TABLE binned_hash AS "
        "SELECT DISTINCT sequence_hash FROM contig_all WHERE kind = ?",
        [KIND_MAG],
    )
    conn.execute(
        """
        DELETE FROM contig_all
        WHERE kind = ?
          AND (length(sequence1) < ? OR sequence_hash IN (SELECT sequence_hash FROM binned_hash))
        """,
        [KIND_UNBINNED, MIN_RESIDUE_LENGTH_BP],
    )
    conn.execute(
        """
        CREATE TEMP TABLE subject AS
        SELECT kind, bin_id,
               row_number() OVER (ORDER BY kind, bin_id)::BIGINT AS genome_id,
               sum(length(sequence1))::BIGINT AS total_bases
        FROM contig_all GROUP BY kind, bin_id
        """
    )
    conn.execute(
        """
        CREATE TEMP TABLE contig AS
        SELECT s.genome_id, c.*,
               row_number() OVER (PARTITION BY s.genome_id ORDER BY c.sequence_index)::INTEGER
                   AS contig_order
        FROM contig_all c JOIN subject s USING (kind, bin_id)
        """
    )
    conn.execute("DROP TABLE contig_all")


def _predict(conn, table: str, where: str, settings: str) -> None:
    """`table`: `predict_genes` over the contigs of the subjects `where` selects (an
    expression over `subject s`), with `settings`."""
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {table}_in AS
        SELECT c.genome_id, c.contig_id AS read_id, c.sequence1, c.sequence_index
        FROM contig c JOIN subject s USING (genome_id)
        WHERE {where}
        """
    )
    conn.execute(
        f"""
        CREATE TEMP TABLE {table} AS
        SELECT * FROM predict_genes('{table}_in', genome_id := 'genome_id', {settings})
        """
    )


def _call_checkm(conn) -> None:
    """`calls_checkm`: the `prodigal_checkm` rows CheckM keeps for each subject, with
    the mode each was called in (module docstring: the rules)."""
    single = f"s.total_bases >= {_CHECKM_META_BELOW_BP}"
    _predict(conn, "single4", single, "trans_table := 4, mask_regions := true")
    _predict(conn, "single11", single, "trans_table := 11, mask_regions := true")
    _predict(conn, "meta", f"NOT ({single})", "meta_mode := true, mask_regions := true")
    conn.execute(
        """
        CREATE TEMP TABLE candidate AS
        SELECT 4 AS tbl, false AS meta_mode, * FROM single4
        UNION ALL BY NAME SELECT 11 AS tbl, false AS meta_mode, * FROM single11
        UNION ALL BY NAME SELECT 4 AS tbl, true AS meta_mode, * FROM meta
        UNION ALL BY NAME SELECT 11 AS tbl, true AS meta_mode, * FROM meta
        """
    )
    # A single-mode (subject, table) that failed or found no genes is rerun in
    # metagenomic mode, as CheckM reruns a failed call or one with an empty protein file.
    conn.execute(
        """
        CREATE TEMP TABLE failed AS
        SELECT genome_id, tbl FROM candidate WHERE NOT meta_mode
        GROUP BY ALL HAVING NOT bool_or(gene_index IS NOT NULL)
        """
    )
    _predict(
        conn,
        "retry",
        "s.genome_id IN (SELECT genome_id FROM failed)",
        "meta_mode := true, mask_regions := true",
    )
    conn.execute(
        "DELETE FROM candidate WHERE (genome_id, tbl) IN (SELECT genome_id, tbl FROM failed)"
    )
    conn.execute(
        """
        INSERT INTO candidate BY NAME
        SELECT f.tbl, true AS meta_mode, r.* FROM retry r JOIN failed f USING (genome_id)
        """
    )
    # Coding density per (subject, table): the union of the gene intervals on each
    # contig, over the subject's total length. `reach` is the furthest end of the genes
    # before this one on its contig, so only the part past it is new.
    conn.execute(
        """
        CREATE TEMP TABLE density AS
        WITH g AS (
            SELECT genome_id, tbl, position, stop_position,
                   max(stop_position) OVER (
                       PARTITION BY genome_id, tbl, read_id ORDER BY position, stop_position
                       ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS reach
            FROM candidate WHERE gene_index IS NOT NULL),
        covered AS (
            SELECT genome_id, tbl,
                   sum(greatest(0, stop_position - greatest(position, coalesce(reach, 0))))
                       AS bases
            FROM g GROUP BY ALL)
        SELECT s.genome_id, t.tbl, coalesce(c.bases, 0) / s.total_bases AS density
        FROM subject s CROSS JOIN (VALUES (4), (11)) t(tbl)
        LEFT JOIN covered c ON c.genome_id = s.genome_id AND c.tbl = t.tbl
        """
    )
    conn.execute(
        """
        CREATE TEMP TABLE best AS
        SELECT d4.genome_id,
               CASE WHEN d4.density - d11.density > 0.05 AND d4.density > 0.7 THEN 4 ELSE 11 END
                   AS tbl
        FROM density d4 JOIN density d11 USING (genome_id)
        WHERE d4.tbl = 4 AND d11.tbl = 11
        """
    )
    conn.execute(
        """
        CREATE TEMP TABLE calls_checkm AS
        SELECT c.* EXCLUDE (tbl) FROM candidate c JOIN best b USING (genome_id, tbl)
        """
    )


def _call_meta_closed(conn) -> None:
    """`calls_meta_closed`: every subject in metagenomic mode with closed ends."""
    _predict(
        conn,
        "meta_closed",
        "true",
        "meta_mode := true, closed_ends := true, mask_regions := false",
    )
    conn.execute(
        "CREATE TEMP TABLE calls_meta_closed AS SELECT true AS meta_mode, * FROM meta_closed"
    )


def _gene_rows_sql(calls: str, call_set: str) -> str:
    """The genes of one call set, on each contig's canonical strand. A gene `[p, s)` on
    a contig of length L read on the other strand is `[L - s + 2, L - p + 2)` on the
    canonical one, with its strand and its two partial flags swapped."""
    attributes = ", ".join(f"'{key}': {expr}" for key, expr in ATTRIBUTE_EXPR.items())
    gene_bases = "substring(c.sequence1, g.position, g.stop_position - g.position)"
    length = "length(c.sequence1)"
    return f"""
        SELECT c.kind, c.bin_id, '{call_set}' AS call_set,
               c.sequence_hash AS parent_sequence_hash,
               {canonical_sequence_hash_expr(gene_bases)} AS sequence_hash,
               (g.stop_position - g.position)::BIGINT AS sequence_length_bp,
               (CASE WHEN c.flipped THEN {length} - g.stop_position + 2
                     ELSE g.position END)::BIGINT AS position,
               (CASE WHEN c.flipped THEN {length} - g.position + 2
                     ELSE g.stop_position END)::BIGINT AS stop_position,
               CASE WHEN NOT c.flipped THEN g.strand
                    WHEN g.strand = '+' THEN '-' ELSE '+' END AS strand,
               g.score,
               CASE WHEN c.flipped THEN g.partial_right ELSE g.partial_left END AS partial_left,
               CASE WHEN c.flipped THEN g.partial_left ELSE g.partial_right END AS partial_right,
               g.start_type, g.confidence,
               MAP {{{attributes}}}::MAP(VARCHAR, VARCHAR) AS attributes,
               g.protein
        FROM {calls} g JOIN contig c ON c.genome_id = g.genome_id AND c.contig_id = g.read_id
        WHERE g.gene_index IS NOT NULL
    """


def _contig_rows_sql(calls: str, call_set: str, closed_ends: bool, mask_regions: bool) -> str:
    """One row per contig of each subject: its settings, its outcome, and the model
    Prodigal used on it. A subject whose training failed has no per-contig rows from
    `predict_genes`, only its own, and each of its contigs takes that status. The
    `any_value`s read values `predict_genes` repeats on every row of a contig (its
    model) or of a subject (its mode). A status outside `GENE_CALL_STATUSES`, or a
    contig with none, fails the step."""
    known = ", ".join(f"'{s}'" for s in GENE_CALL_STATUSES)
    return f"""
        WITH per_contig AS (
            SELECT genome_id, read_id, any_value(meta_mode) AS meta_mode,
                   bool_or(gene_index IS NOT NULL) AS has_genes, any_value(status) AS status,
                   any_value(translation_table) AS translation_table,
                   any_value(uses_sd) AS uses_sd, any_value(meta_bin) AS meta_bin,
                   any_value(model_desc) AS model_desc,
                   any_value(model_gc_content) AS model_gc_content
            FROM {calls} WHERE read_id IS NOT NULL GROUP BY genome_id, read_id),
        per_subject AS (
            SELECT genome_id, any_value(meta_mode) AS meta_mode, any_value(status) AS status
            FROM {calls} WHERE read_id IS NULL GROUP BY genome_id)
        SELECT c.kind, c.bin_id, '{call_set}' AS call_set,
               c.sequence_hash AS parent_sequence_hash, c.contig_order,
               coalesce(p.meta_mode, s.meta_mode) AS meta_mode,
               {str(closed_ends).lower()} AS closed_ends,
               {str(mask_regions).lower()} AS mask_regions,
               CASE WHEN p.has_genes THEN '{STATUS_OK}'
                    WHEN coalesce(p.status, s.status) IN ({known})
                        THEN coalesce(p.status, s.status)
                    ELSE error('gene_call: contig ' || c.contig_id || ' of ' || c.bin_id
                               || ' has status ' || coalesce(p.status, s.status, 'NULL'))
               END AS status,
               p.translation_table, p.uses_sd, p.meta_bin, p.model_desc, p.model_gc_content
        FROM contig c
        LEFT JOIN per_contig p ON p.genome_id = c.genome_id AND p.read_id = c.contig_id
        LEFT JOIN per_subject s ON s.genome_id = c.genome_id
    """


def _write_checkm_files(conn, out: Path) -> int:
    """`<out>/<kind>/<bin_id>/genes.{faa,gff}` for each subject with `prodigal_checkm`
    genes. Returns how many subjects got them."""
    # In contig order, the order `prodigal_proteins` numbers contigs in.
    conn.execute(
        """
        CREATE TEMP TABLE checkm_rows AS
        SELECT g.* EXCLUDE (meta_mode), c.contig_order
        FROM calls_checkm g JOIN contig c ON c.genome_id = g.genome_id AND c.contig_id = g.read_id
        ORDER BY g.genome_id, c.contig_order, g.gene_index NULLS FIRST
        """
    )
    # `prodigal_proteins` returns its records in no set order; the CLI writes them in
    # contig order, so each record takes its gene's place back from `checkm_rows`.
    conn.execute(
        """
        CREATE TEMP TABLE checkm_faa AS
        SELECT p.*, r.contig_order, r.gene_index
        FROM prodigal_proteins('checkm_rows', per_genome := true) p
        JOIN checkm_rows r
          ON r.genome_id = p.genome_id AND r.read_id || '_' || r.gene_index = p.read_id
        """
    )
    subjects = conn.execute(
        """
        SELECT DISTINCT s.genome_id, s.kind, s.bin_id
        FROM subject s JOIN checkm_rows r USING (genome_id)
        WHERE r.gene_index IS NOT NULL ORDER BY s.genome_id
        """
    ).fetchall()
    for genome_id, kind, bin_id in subjects:
        d = out / kind / _dir_name(bin_id)
        d.mkdir(parents=True)
        rows = f"""(SELECT * FROM checkm_rows WHERE genome_id = {int(genome_id)}
                    ORDER BY contig_order, gene_index NULLS FIRST)"""
        conn.execute(
            f"""COPY (SELECT * EXCLUDE (contig_order) FROM {rows})
                TO {_sql_path(d / "genes.gff")} (FORMAT GFF, DIALECT 'prodigal')"""
        )
        conn.execute(
            f"""COPY (SELECT read_id, comment, sequence1 FROM checkm_faa
                      WHERE genome_id = {int(genome_id)} ORDER BY contig_order, gene_index)
                TO {_sql_path(d / "genes.faa")}
                (FORMAT FASTA, INCLUDE_COMMENT true, LINE_WIDTH 60)"""
        )
    return len(subjects)


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    out = workspace / OUTPUT_DIR
    (out / CHECKM_DIR).mkdir(parents=True)
    try:
        n_subjects, n_checkm = _call_and_write(inputs, workspace, out)
    except BaseException:
        # A partial output is never left where the next step would read it.
        shutil.rmtree(out, ignore_errors=True)
        raise
    print(
        f"gene_call: {n_subjects} subject(s), {n_checkm} with prodigal_checkm genes, "
        f"prep_sample_idx={inputs.prep_sample_idx}",
        flush=True,
    )
    return {"gene_calls": out}


def _call_and_write(inputs: Inputs, workspace: Path, out: Path) -> tuple[int, int]:
    with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
        apply_duckdb_settings(
            conn,
            duckdb_tmp,
            memory_gb=resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS),
            threads=_DUCKDB_THREADS,
        )
        _load_subjects(conn, inputs.genomes_dir, inputs.refined_bins_dir)
        _call_checkm(conn)
        _call_meta_closed(conn)
        conn.execute(
            f"""COPY (({_gene_rows_sql("calls_checkm", CALL_SET_CHECKM)})
                      UNION ALL ({_gene_rows_sql("calls_meta_closed", CALL_SET_META_CLOSED)}))
                TO {_sql_path(out / GENE_FILE)} ({PARQUET_OPTS_TRANSFER})"""
        )
        checkm_contigs = _contig_rows_sql("calls_checkm", CALL_SET_CHECKM, False, True)
        closed_contigs = _contig_rows_sql("calls_meta_closed", CALL_SET_META_CLOSED, True, False)
        conn.execute(
            f"""COPY (({checkm_contigs}) UNION ALL ({closed_contigs}))
                TO {_sql_path(out / CONTIG_FILE)} ({PARQUET_OPTS_TRANSFER})"""
        )
        n_checkm = _write_checkm_files(conn, out / CHECKM_DIR)
        n_subjects = conn.execute("SELECT count(*) FROM subject").fetchone()[0]
    return n_subjects, n_checkm
