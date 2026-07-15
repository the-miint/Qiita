"""Native job: estimate a metagenomic OGU feature table from alignment data.

Compute-on-demand, never persisted: given one alignment run and an explicit
`prep_sample_idx` cohort (both carried on the work ticket's `action_context` and
resolved CP-side into the alignment DoGet ticket), build a genome-keyed OGU
feature table via duckdb-miint `woltka_ogu`, filtered to genomes meeting a
breadth-of-coverage threshold POOLED over the whole cohort. The OGU key is
`genome_idx` (counts and coverage roll `feature_idx -> genome_idx`, so a
multi-contig genome is handled natively even though today's references are 1:1).

Three inputs, three sources:

* the **alignment slice** streams from the data plane over Arrow Flight
  (`open_alignment_stream`, minted by `work_ticket_idx`) — no disk;
* the **per-feature lengths** stream from the data plane's `reference_sequences`
  (`open_reference_sequences_stream`, whole-reference so unaligned contigs are in
  the coverage denominator) — no disk;
* the **feature -> genome map** is the one Postgres-only input, staged as a small
  workspace Parquet by the CP runner resolver (`runner/_feature_table.py`) and
  read here via `read_parquet`.

The alignment stream is MATERIALIZED to a real non-temp TABLE because
`woltka_ogu` resolves its source relation on a SEPARATE connection during
bind/execute, which sees regular table/view names but not registered stream
relations / TEMP tables / CTEs (see docs/duckdb-miint.md, same constraint
`subject.stage_subject` documents for `save_minimap2_index`).

miint signatures (qiita-verified against the mirror build; see docs/duckdb-miint.md):
  genome_coverage(alignments, subject_total_length, subject_genome_id)  -- table macro
    -> (genome_id, covered BIGINT, proportion_covered DOUBLE)
  woltka_ogu(relation, sequence_id_field, [sample_id])                  -- table function
    -> (feature_id, value DOUBLE) or ({sample_id}, feature_id, value) with sample_id
Both take NATIVE-INTEGER id columns (BIGINT reference/sample_id — no `::VARCHAR`
casts); `woltka_ogu`'s name arguments are quoted string literals and `sample_id`
is a named argument. `genome_coverage`'s three arguments are unquoted relation
names resolved in this connection.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field
from qiita_common.parquet import validate_parquet_path

from ..data_plane_client import open_alignment_stream, open_reference_sequences_stream
from ..miint import (
    PARQUET_OPTS,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)

if TYPE_CHECKING:
    import duckdb

YAML_STEP_NAME = "estimate_feature_table"

# The single step output: the coverage-filtered OGU feature table. Emitted as a
# Parquet under the per-attempt workspace (it is NOT registered into DuckLake —
# the table is computed on demand, never persisted).
OGU_TABLE_OUTPUT_KEY = "ogu_table"
OGU_TABLE_FILENAME = "ogu_table.parquet"

# In-connection working tables. `_ALIGNMENT_TABLE` and (transitively) the map/
# lengths tables are non-temp regular TABLEs so woltka_ogu / genome_coverage can
# resolve them (woltka opens a separate connection; see module docstring).
_MAP_TABLE = "contig_to_genome"
_GENOME_LENGTHS_TABLE = "genome_lengths"
_ALIGNMENT_TABLE = "alignment_slice"

# DuckDB resource caps. This job's heavy work is entirely in-DuckDB (the coverage
# interval merge + the woltka aggregation over the streamed alignment slice) — no
# in-process co-consumer, so no `reserve_gb`. `_DUCKDB_MEMORY_GB` is the OFF-SLURM
# fallback (local backend / tests); under SLURM the limit tracks the real cgroup
# via `resolve_duckdb_memory_gb()`, so a `--mem-gb` override reaches DuckDB. The
# cohort/alignment size is not known at submit time, so there is deliberately no
# `plan()` — the workflow YAML baseline governs, and OOM escalation backstops an
# under-estimate.
_DUCKDB_MEMORY_GB = 8
_DUCKDB_THREADS = 4


class Inputs(BaseModel):
    """Typed input contract for estimate_feature_table.

    `reference_idx` and `work_ticket_idx` are framework-injected scope scalars (a
    REFERENCE-scoped ticket). `coverage_threshold` rides the workflow `params:`
    (a proportion in [0, 1] — e.g. 0.01 for 1% breadth). `genome_map_path` is the
    resolver-staged `(feature_idx, genome_idx)` Parquet. There is deliberately no
    `alignment_idx`: the alignment DoGet ticket is minted by `work_ticket_idx`, and
    the CP route derives `alignment_idx` + the cohort from the ticket's
    `action_context`.
    """

    reference_idx: int
    work_ticket_idx: int
    coverage_threshold: float = Field(ge=0.0, le=1.0)
    genome_map_path: Path


def _write_ogu_table(
    conn: duckdb.DuckDBPyConnection,
    *,
    coverage_threshold: float,
    out_path: Path,
) -> None:
    """Run the coverage-filtered OGU recipe over the already-staged working tables
    (`_MAP_TABLE`, `_ALIGNMENT_TABLE`, and — only when a threshold applies —
    `_GENOME_LENGTHS_TABLE`) and COPY the result to `out_path` as Parquet (v2 + zstd).

    Steps mirror the qiita-verified analytic:
      1. survivors — genomes whose POOLED `proportion_covered` meets the threshold,
         via the `genome_coverage` macro over `cov_alignments` (a VIEW of the
         cohort's non-NULL aligned intervals pooled across all samples — breadth is a
         cohort property; NULL coordinates cannot contribute an interval and would
         poison the merge). The denominator is the FULL genome length incl. unaligned
         contigs, from `_GENOME_LENGTHS_TABLE`. `cov_alignments` is a VIEW because it
         is only read by the macro on this connection — materializing it would just
         duplicate the alignment slice in RAM. **When `coverage_threshold == 0` every
         genome with any alignment trivially qualifies, so this whole step is SKIPPED
         (no `genome_coverage`, and the caller does not even stream the lengths).**
      2. `ogu_input(sequence_idx, prep_sample_idx, flags, reference=genome_idx)` —
         the alignment pre-mapped to the genome level so woltka counts at genome
         granularity, and (when a threshold applies) INNER-JOINed to the survivor set
         so non-surviving genomes are removed BEFORE woltka. This ordering is
         load-bearing: `woltka_ogu` fractionally splits a multi-mapped read by its
         number of UNIQUE `reference` values, so a read hitting a surviving + a
         dropped genome must lose the dropped one FIRST to renormalize to 1.0 on the
         survivor — filtering woltka's OUTPUT instead would strand it at 0.5. The map
         INNER JOIN also drops alignments to no-genome features (not OGUs). A real
         non-temp TABLE — `woltka_ogu` resolves its source on a separate connection.
      3. `woltka_ogu(..., sample_id := 'prep_sample_idx')` per-sample, `feature_id`
         (= genome_idx) renamed for the output. No post-woltka survivor join (done in
         step 2) and no ORDER BY — the reader sorts; the file need not.

    Empty `ogu_input` (no alignment maps to a surviving genome — e.g. an all-16S
    cohort, a reference with no genome-tagged features, or every genome dropped by
    the threshold) is a legitimate compute-on-demand result, but `woltka_ogu` rejects
    an all-NULL `sample_id` source, so this short-circuits to a valid 0-row Parquet
    with the output schema instead of calling woltka on nothing.
    """
    out_sql = validate_parquet_path(out_path)

    # ogu_input pre-maps the alignment to genome level (so woltka counts per genome);
    # the map INNER JOIN drops no-genome features. When a breadth threshold applies it
    # is ALSO filtered to survivors HERE — before woltka — so a read on a surviving +
    # a dropped genome renormalizes to the survivor (see docstring, step 2).
    ogu_input_sql = (
        f"SELECT a.sequence_idx, a.prep_sample_idx, a.flags, m.genome_id AS reference "
        f"FROM {_ALIGNMENT_TABLE} a "
        f"JOIN {_MAP_TABLE} m ON a.feature_idx = m.contig_id"
    )
    if coverage_threshold > 0.0:
        conn.execute(
            f"CREATE VIEW cov_alignments AS "
            f"SELECT feature_idx AS reference, position, stop_position "
            f"FROM {_ALIGNMENT_TABLE} "
            f"WHERE position IS NOT NULL AND stop_position IS NOT NULL"
        )
        conn.execute(
            "CREATE TABLE survivor_genome AS SELECT genome_id "
            f"FROM genome_coverage(cov_alignments, {_GENOME_LENGTHS_TABLE}, {_MAP_TABLE}) "
            "WHERE proportion_covered >= ?",
            [coverage_threshold],
        )
        ogu_input_sql += " JOIN survivor_genome s ON m.genome_id = s.genome_id"
    # else coverage_threshold == 0: no coverage calc; every mapped genome qualifies.
    conn.execute(f"CREATE TABLE ogu_input AS {ogu_input_sql}")

    if conn.execute("SELECT count(*) FROM ogu_input").fetchone()[0] == 0:
        conn.execute(
            f"COPY (SELECT CAST(NULL AS BIGINT) AS prep_sample_idx, "
            f"CAST(NULL AS BIGINT) AS genome_idx, CAST(NULL AS DOUBLE) AS value WHERE false) "
            f"TO '{out_sql}' ({PARQUET_OPTS})"
        )
        return
    conn.execute(
        f"COPY ("
        f"SELECT w.prep_sample_idx, w.feature_id AS genome_idx, w.value "
        f"FROM woltka_ogu('ogu_input', 'sequence_idx', sample_id := 'prep_sample_idx') w"
        f") TO '{out_sql}' ({PARQUET_OPTS})"
    )


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    workspace.mkdir(parents=True, exist_ok=True)
    out_path = workspace / OGU_TABLE_FILENAME

    with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
        apply_duckdb_settings(
            conn,
            duckdb_tmp,
            memory_gb=resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS),
            threads=_DUCKDB_THREADS,
        )

        # The feature -> genome map (Postgres-derived, resolver-staged). A real
        # TABLE: reused by both the genome_coverage macro (subject_genome_id) and
        # the ogu_input join. Inner-consistent BIGINT ids (int64 Parquet).
        map_sql = validate_parquet_path(inputs.genome_map_path)
        conn.execute(
            f"CREATE TABLE {_MAP_TABLE} AS "
            f"SELECT feature_idx AS contig_id, genome_idx AS genome_id "
            f"FROM read_parquet('{map_sql}')"
        )

        # Per-feature lengths -> per-genome length denominators. These feed ONLY the
        # genome_coverage calc, so at coverage_threshold == 0 (the calc is skipped)
        # the stream is skipped too — "avoid the coverage calculation entirely".
        # Whole-reference stream (every contig, incl. unaligned) so the denominator is
        # the FULL genome length. Consumed inside the stream `with` (the GROUP BY
        # drains it) so the Flight client closes before the compute.
        if inputs.coverage_threshold > 0.0:
            async with open_reference_sequences_stream(
                conn, reference_idx=inputs.reference_idx
            ) as lengths_rel:
                conn.execute(
                    f"CREATE TABLE {_GENOME_LENGTHS_TABLE} AS "
                    f"SELECT m.genome_id AS genome_id, SUM(l.sequence_length_bp) AS total_length "
                    f"FROM {lengths_rel} l JOIN {_MAP_TABLE} m ON l.feature_idx = m.contig_id "
                    f"GROUP BY m.genome_id"
                )

        # The alignment slice (all cohort samples pooled). Materialized to a real
        # TABLE — woltka_ogu resolves its source on a separate connection, which
        # cannot see a registered stream relation; the CREATE TABLE also drains
        # the stream so the Flight client closes before the compute.
        async with open_alignment_stream(
            conn, work_ticket_idx=inputs.work_ticket_idx
        ) as alignment_rel:
            conn.execute(
                f"CREATE TABLE {_ALIGNMENT_TABLE} AS SELECT "
                "prep_sample_idx, sequence_idx, feature_idx, flags, position, stop_position "
                f"FROM {alignment_rel}"
            )

        _write_ogu_table(conn, coverage_threshold=inputs.coverage_threshold, out_path=out_path)

    return {OGU_TABLE_OUTPUT_KEY: out_path}
