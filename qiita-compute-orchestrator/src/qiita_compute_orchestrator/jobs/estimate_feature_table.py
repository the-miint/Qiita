"""Native job: estimate a metagenomic OGU feature table from alignment data.

Compute-on-demand, never persisted: given one alignment run and an explicit
`prep_sample_idx` cohort (both carried on the work ticket's `action_context` and
resolved CP-side into the alignment DoGet ticket), build a genome-keyed OGU
feature table via duckdb-miint `woltka_ogu`, filtered to genomes meeting a
breadth-of-coverage threshold POOLED over the whole cohort.

**The analytic itself — its SQL, its relation names, and every rule that makes it
correct — is `qiita_common.analytic`**, shared with the client-side feature-table
recipe so the two cannot disagree about it. This module is the server-side half:
where the three inputs come from, and where the result goes.

A COMBINED table (the inverted open reference) estimates over a second alignment
run as well — each cohort sample against its own assembled contigs — and takes the
de novo arm's placement of a read over the reference arm's. It is requested by
`denovo_alignment_idx` on the ticket's `action_context`, and reaches this module as
`denovo_genome_map_path`: absent, everything below is the reference-only job it
always was. `qiita_common.analytic.reconcile` owns every rule about the second arm;
what is here is where its three inputs come from.

Inputs and their sources:

* the **alignment slice** streams from the data plane over Arrow Flight
  (`open_alignment_stream`, minted by `work_ticket_idx`) — no disk;
* the **per-feature lengths** stream from the data plane's `reference_sequences`
  (`open_reference_sequences_stream`) — no disk. **Whole-reference**, for the reason
  `analytic.genome_lengths_table_sql` gives;
* the **feature -> genome map** is staged as a small workspace Parquet by the CP
  runner resolver (`runner/_feature_table.py`) and read here via `read_parquet`.

The de novo arm draws on the same three, differently scoped: the alignment slice is
a second mint on the same work ticket, the map is a second resolver-staged Parquet,
and the lengths come from the assembly read-back — which is scoped to ONE
`(prep_sample_idx, processing_idx)` run, so a cohort is N single-consumption streams
appended into one relation rather than the reference arm's single whole-reference
one.

It adds one the reference arm has no counterpart for: the assembled genomes' CheckM
completeness / contamination, staged as a third Parquet by the same resolver pass
and arriving already keyed by genome. Neither store holds it whole, which is why the
resolver rather than this module does the join (`runner/_feature_table.py`).

Each stream is drained inside its own `with`, by the CREATE that stages it, so the
Flight client closes before the compute starts.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field
from qiita_common import analytic
from qiita_common.parquet import validate_parquet_path

from ..data_plane_client import (
    open_alignment_stream,
    open_assembled_sequence_stream,
    open_reference_sequences_stream,
)
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
    `action_context`. That holds for the de novo arm too — its `alignment_idx` is on
    the same `action_context`, and the mint names the arm rather than the run.
    """

    reference_idx: int
    work_ticket_idx: int
    coverage_threshold: float = Field(ge=0.0, le=1.0)
    genome_map_path: Path
    # Present ⇒ a combined table. The resolver stages this only when the ticket
    # carries `denovo_alignment_idx`, and it is the job's ONLY signal that the de
    # novo arm was asked for: `Inputs` carries no alignment_idx of either arm, for
    # the reason stated above.
    denovo_genome_map_path: Path | None = None
    # The assembly run the de novo arm aligned against. Not a context key: the
    # resolver reads it off `denovo_alignment_idx`'s hashed params, so the two
    # cannot disagree about which assembly a de novo alignment used. Rides
    # `params:` because a scalar cannot ride `inputs:`.
    denovo_processing_idx: int | None = None
    # Per-genome CheckM scores for the de novo arm, staged by the same resolver
    # pass that stages the map above. Nullable scores; the resolver states why.
    denovo_genome_quality_path: Path | None = None


def _write_ogu_table(
    conn: duckdb.DuckDBPyConnection,
    *,
    coverage_threshold: float,
    out_path: Path,
    combined: bool,
) -> None:
    """Run the shared analytic over the already-staged working tables and COPY the
    result to `out_path` as Parquet (v2 + zstd). `qiita_common.analytic` owns every rule
    the SQL encodes AND the order the statements run in; what is left here is the one
    thing the two consumers genuinely differ on — this one COPYs straight out, where the
    client-side recipe materializes the counts so it can relabel them.
    """
    out_sql = validate_parquet_path(out_path)

    # Pooled-only: breadth over the whole cohort is what this job's workflow `params:`
    # describe, and the per-sample scope is the client-side recipe's.
    for sql, parameters in analytic.ogu_input_statements(
        scope=analytic.CoverageScope.POOLED,
        coverage_threshold=coverage_threshold,
        combined=combined,
    ):
        conn.execute(sql, parameters)

    n_rows = conn.execute(analytic.ogu_input_count_sql()).fetchone()[0]
    select_sql = analytic.woltka_ogu_select_sql() if n_rows else analytic.empty_ogu_select_sql()
    conn.execute(f"COPY ({select_sql}) TO '{out_sql}' ({PARQUET_OPTS})")


def _require_denovo_genome_quality_path(inputs: Inputs) -> Path:
    """The de novo arm's per-genome quality Parquet, or a loud failure.

    Bound together with the map and the run by one resolver pass, so a combined
    ticket reaching here without it is a broken binding. Raising rather than skipping
    the relation, for the reason `_require_denovo_processing_idx` gives: skipping
    does not fail either, it leaves every assembled genome looking unscored — which
    is what a run CheckM legitimately scored nothing in also looks like.
    """
    if inputs.denovo_genome_quality_path is None:
        raise ValueError(
            "denovo_genome_map_path was bound without denovo_genome_quality_path, so "
            "the assembled genomes' completeness/contamination are unavailable"
        )
    return inputs.denovo_genome_quality_path


def _require_denovo_processing_idx(inputs: Inputs) -> int:
    """The assembly run the de novo arm aligned against, or a loud failure.

    The two de novo fields are separately optional on the wire but are one fact:
    the resolver binds both or neither. Reaching the lengths staging with a map but
    no run is a broken binding, and the alternative to raising is to skip the de
    novo lengths — which does not fail either, it gives every qiita genome no
    length denominator, drops all of them at the coverage filter, and returns a
    reference-only table under the combined name.
    """
    if inputs.denovo_processing_idx is None:
        raise ValueError(
            "denovo_genome_map_path was bound without denovo_processing_idx, so the "
            "assembly run to read contig lengths from is unknown"
        )
    return inputs.denovo_processing_idx


async def _stage_denovo_lengths(conn: duckdb.DuckDBPyConnection, *, processing_idx: int) -> None:
    """Stage the cohort's assembled-contig lengths and roll them up to per-genome
    denominators beside the reference arm's.

    **N streams, one per cohort sample**, because the assembly read-back ticket is
    scoped to one `(prep_sample_idx, processing_idx)` run where the reference arm's
    is scoped to a whole reference. Each is single-consumption, so all of them are
    appended into one relation before the roll-up reads it.

    **The cohort comes from the de novo map itself**, not from a separate input. The
    map holds exactly the samples that contributed a genome-bearing contig to this
    run — so a sample that assembled nothing is absent from both, and asking the
    data plane for its contigs would 404 on a run that legitimately produced none.
    A second source for the same list is a second thing that can be wrong.
    """
    conn.execute(analytic.denovo_contig_lengths_table_sql())
    cohort = [
        row[0]
        for row in conn.execute(
            f"SELECT DISTINCT prep_sample_idx FROM {analytic.DENOVO_MAP_TABLE} "
            f"ORDER BY prep_sample_idx"
        ).fetchall()
    ]
    for prep_sample_idx in cohort:
        async with open_assembled_sequence_stream(
            conn, prep_sample_idx=prep_sample_idx, processing_idx=processing_idx
        ) as lengths_rel:
            conn.execute(analytic.denovo_contig_lengths_insert_sql(lengths_rel))
    conn.execute(analytic.denovo_genome_lengths_insert_sql())


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    workspace.mkdir(parents=True, exist_ok=True)
    out_path = workspace / OGU_TABLE_FILENAME

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS),
                threads=_DUCKDB_THREADS,
            )

            combined = inputs.denovo_genome_map_path is not None
            # Checked here, not at its one use below: that use sits behind the
            # coverage-threshold branch, so at threshold 0 a map-without-run binding
            # would go unnoticed — and the two fields are one fact, bound together or
            # not at all.
            denovo_processing_idx = _require_denovo_processing_idx(inputs) if combined else None

            # The feature -> genome map, read from the resolver-staged Parquet.
            # Inner-consistent BIGINT ids (int64 Parquet).
            map_sql = validate_parquet_path(inputs.genome_map_path)
            conn.execute(analytic.map_table_sql(f"read_parquet('{map_sql}')"))
            if combined:
                denovo_map_sql = validate_parquet_path(inputs.denovo_genome_map_path)
                conn.execute(analytic.denovo_map_table_sql(f"read_parquet('{denovo_map_sql}')"))
                quality_sql = validate_parquet_path(_require_denovo_genome_quality_path(inputs))
                conn.execute(
                    analytic.denovo_genome_quality_table_sql(f"read_parquet('{quality_sql}')")
                )

            # The lengths feed ONLY the coverage calc, so when that is skipped the
            # stream is skipped too — the point is to avoid the coverage calculation
            # entirely, not just its filter. Same predicate `_write_ogu_table` uses.
            if analytic.coverage_filter_applies(inputs.coverage_threshold):
                async with open_reference_sequences_stream(
                    conn, reference_idx=inputs.reference_idx
                ) as lengths_rel:
                    conn.execute(analytic.genome_lengths_table_sql(lengths_rel))
                if combined:
                    await _stage_denovo_lengths(conn, processing_idx=denovo_processing_idx)

            # The alignment slice, all cohort samples pooled.
            async with open_alignment_stream(
                conn,
                work_ticket_idx=inputs.work_ticket_idx,
                columns=analytic.ALIGNMENT_COLUMNS,
            ) as alignment_rel:
                conn.execute(analytic.alignment_table_sql(alignment_rel))

            if combined:
                # The second arm, and precedence over the first, in one sequence —
                # see `analytic.reconcile.denovo_alignment_statements` for why they
                # are not separable. Only the first statement reads the stream; the
                # rest run inside the `async with` so the sequence cannot be split
                # across it.
                async with open_alignment_stream(
                    conn,
                    work_ticket_idx=inputs.work_ticket_idx,
                    columns=analytic.ALIGNMENT_COLUMNS,
                    relation="denovo_alignment_stream",
                    denovo=True,
                ) as denovo_rel:
                    for sql in analytic.denovo_alignment_statements(denovo_rel):
                        conn.execute(sql)

            _write_ogu_table(
                conn,
                coverage_threshold=inputs.coverage_threshold,
                out_path=out_path,
                combined=combined,
            )
        success = True
    finally:
        # On failure remove a partial COPY output so the SLURM launcher's manifest
        # walker (which runs after execute()) can't promote it as the result.
        if not success:
            out_path.unlink(missing_ok=True)

    return {OGU_TABLE_OUTPUT_KEY: out_path}
