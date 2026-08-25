"""The OGU table flavour: what `woltka_ogu` reads, and what it returns.

Today this is the one table flavour — genome-keyed counts via miint's `woltka_ogu`;
names carrying `ogu` denote that computation specifically, and taxonomy- and
tree-derived tables join it later.

**The OGU key is `genome_idx`.** Counts and coverage both roll
`feature_idx → genome_idx` through the map, so a multi-contig genome is handled
natively, and a feature belonging to several genomes — a plasmid, under the
`feature_genome` many-to-many — rolls up to each of them, leaving woltka to handle
the resulting multi-mappers by design.

miint signature (qiita-verified; see `docs/duckdb-miint.md`):

    woltka_ogu(relation, sequence_id_field [, sample_id])                 -- table function
      -> (feature_id, value DOUBLE), or ({sample_id}, feature_id, value)

It takes NATIVE-INTEGER id columns — no `::VARCHAR` casts. Its relation arguments are
quoted string literals resolved on a separate connection, and `sample_id` is a named
argument.
"""

from __future__ import annotations

from .coverage import (
    CoverageScope,
    coverage_alignments_view_sql,
    coverage_filter_applies,
    survivor_table_name,
    survivor_table_sql,
)
from .relations import (
    ALIGNMENT_TABLE,
    COVERAGE_ALIGNMENTS_VIEW,
    MAP_TABLE,
    OGU_INPUT_TABLE,
    OGU_OUTPUT_TABLE,
)

# The analytic's output schema, name -> SQL type. Both the real path and the 0-row
# short-circuit must produce exactly this, or an empty cohort yields a
# differently-shaped file than a populated one — and the empty path is the one a
# caller exercises least and would notice last. `empty_ogu_select_sql` is
# GENERATED from this so the two cannot drift by hand; the types are what woltka
# returns for a BIGINT `reference` and `sample_id`.
OUTPUT_SCHEMA = {
    "prep_sample_idx": "BIGINT",
    "genome_idx": "BIGINT",
    "value": "DOUBLE",
}
OUTPUT_COLUMNS = tuple(OUTPUT_SCHEMA)


def ogu_input_table_sql(*, survivor_scope: CoverageScope | None) -> str:
    """woltka's input: the alignment pre-mapped to genome level, so woltka counts
    at genome granularity. The map's INNER JOIN also drops alignments to features
    with no genome (a 16S record is not an OGU).

    `survivor_scope` is the scope whose survivor set to join, or **`None` when no
    threshold applies** — at 0 every genome with any alignment qualifies, so there
    is no set to build or join. It must be the same scope `survivor_table_sql` was
    called with — pooled joins on the genome alone, per-sample on `(sample, genome)`,
    which is the entire behavioural difference between the scopes. A mismatch names a
    relation that was never created, so it is a bind error rather than a wrong
    number (see `_SURVIVOR_TABLES`).

    The survivor join happens **here, before woltka**, and the ordering decides the
    numbers: `woltka_ogu` splits a multi-mapped read across its number of
    distinct `reference` values, so a read hitting a surviving and a dropped genome
    must lose the dropped one first to renormalize to 1.0 on the survivor.
    Filtering woltka's OUTPUT instead strands it at 0.5 — a plausible number rather
    than an error.

    A real non-temp TABLE: woltka resolves it on a separate connection.
    """
    sql = (
        f"CREATE TABLE {OGU_INPUT_TABLE} AS "
        f"SELECT a.sequence_idx, a.prep_sample_idx, a.flags, m.genome_id AS reference "
        f"FROM {ALIGNMENT_TABLE} a "
        f"JOIN {MAP_TABLE} m ON a.feature_idx = m.contig_id"
    )
    if survivor_scope is not None:
        sql += f" JOIN {survivor_table_name(survivor_scope)} s ON m.genome_id = s.genome_id"
        if survivor_scope is CoverageScope.PER_SAMPLE:
            sql += " AND a.prep_sample_idx = s.prep_sample_idx"
    return sql


def ogu_input_statements(
    *, scope: CoverageScope, coverage_threshold: float
) -> tuple[tuple[str, list[float]], ...]:
    """Everything between the staged inputs and woltka's input relation, as ordered
    `(sql, parameters)` pairs. Requires `ALIGNMENT_TABLE`, `MAP_TABLE`, and — when
    the threshold filters — `GENOME_LENGTHS_TABLE`.

    **The order and the scope-to-`None` conversion live here rather than in each
    consumer**, for the reason this package exists at all: a consumer that dropped the
    survivor join would not fail, it would publish a table with unfiltered genomes
    in it. A missed CREATE is a bind error; a missed filter is a plausible wrong
    answer. Same device as `GateClearance.statements`.

    The trailing DROPs are not tidiness. The coverage view, the alignment slice, and
    the survivor set are all dead once `OGU_INPUT_TABLE` exists, and they are dead
    for the whole remaining run — woltka, the relabel, and the write.
    """
    statements: list[tuple[str, list[float]]] = []
    filtering = coverage_filter_applies(coverage_threshold)
    if filtering:
        statements.append((coverage_alignments_view_sql(), []))
        statements.append((survivor_table_sql(scope), [coverage_threshold]))
    statements.append((ogu_input_table_sql(survivor_scope=scope if filtering else None), []))
    if filtering:
        # The view first: it reads the slice.
        statements.append((f"DROP VIEW {COVERAGE_ALIGNMENTS_VIEW}", []))
    statements.append((f"DROP TABLE {ALIGNMENT_TABLE}", []))
    if filtering:
        statements.append((f"DROP TABLE {survivor_table_name(scope)}", []))
    return tuple(statements)


def ogu_input_count_sql() -> str:
    """How many rows woltka would see — the caller's cue to run it or short-circuit
    to `empty_ogu_select_sql`, which is a decision both consumers make and neither
    should spell out by hand."""
    return f"SELECT count(*) FROM {OGU_INPUT_TABLE}"


def woltka_ogu_select_sql() -> str:
    """The feature table itself: per-sample OGU counts over `OGU_INPUT_TABLE`.

    A SELECT rather than a whole statement, because the callers write it
    differently (Parquet server-side, Parquet or BIOM client-side) — wrap it in
    the COPY or CREATE the caller needs.

    No survivor join (done in `ogu_input_table_sql`) and no ORDER BY — the reader
    sorts; the file need not.
    """
    return (
        f"SELECT w.prep_sample_idx, w.feature_id AS genome_idx, w.value "
        f"FROM woltka_ogu('{OGU_INPUT_TABLE}', 'sequence_idx', "
        f"sample_id := 'prep_sample_idx') w"
    )


def empty_ogu_select_sql() -> str:
    """A 0-row stand-in carrying `OUTPUT_COLUMNS` with the same types the real path
    produces.

    An empty `OGU_INPUT_TABLE` is a legitimate result — an all-16S cohort, a
    reference with no genome-tagged features, every genome dropped by the
    threshold — but `woltka_ogu` rejects an all-NULL `sample_id` source, so the
    caller short-circuits to this instead of calling woltka on nothing.
    """
    casts = ", ".join(
        f"CAST(NULL AS {sql_type}) AS {name}" for name, sql_type in OUTPUT_SCHEMA.items()
    )
    return f"SELECT {casts} WHERE false"


def ogu_output_table_sql(*, populated: bool) -> str:
    """Materialize the counts into `OGU_OUTPUT_TABLE`.

    `populated` picks the SELECT: the real `woltka_ogu` call, or the 0-row
    short-circuit `empty_ogu_select_sql` exists for. Passing the flag rather than the
    SELECT is what puts BOTH results in the same relation, so an empty cohort travels
    the same relabel and the same writer as a populated one.

    Materialized, unlike the server-side job's straight COPY, because the client
    reads the counts twice — once to check the labels, once to apply them — and
    re-running woltka for the second read would double the cost of the analytic.
    """
    select = woltka_ogu_select_sql() if populated else empty_ogu_select_sql()
    return f"CREATE TABLE {OGU_OUTPUT_TABLE} AS {select}"
