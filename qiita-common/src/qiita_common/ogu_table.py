"""The OGU feature-table analytic, as SQL text.

Two consumers run this same analytic and must not disagree about it: the
compute-orchestrator native job `estimate_feature_table` (server-side, reached
through a work ticket) and the client-side feature-table recipe (a user's machine,
composing the analytic-export routes). They differ in everything *around* the
analytic — where the three inputs come from, how the result is written — and in
nothing about the analytic itself, so the SQL lives here and the streaming and I/O
stay with each caller.

**Plain SQL text; no duckdb import.** `qiita-common` has no duckdb dependency and
must not gain one — it is the contract layer both Python services import. Callers
execute these statements on a connection that has miint loaded. (Same shape as
`chunking.py`'s expression builders and `parquet.py`'s option strings.)

**The OGU key is `genome_idx`.** Counts and coverage both roll
`feature_idx → genome_idx` through the map, so a multi-contig genome is handled
natively, and a feature belonging to several genomes — a plasmid, under the
`feature_genome` many-to-many — rolls up to each of them, leaving woltka to handle
the resulting multi-mappers by design.

**Three inputs, staged by the caller from wherever it gets them:**

| Relation | Columns | Source |
|---|---|---|
| `ALIGNMENT_TABLE` | `ALIGNMENT_COLUMNS` | the alignment DoGet stream |
| `MAP_TABLE` | `(contig_id, genome_id)` | feature→genome map (Postgres-derived) |
| `GENOME_LENGTHS_TABLE` | `(genome_id, total_length)` | the reference_sequences stream |

The map's source must be keyed `(feature_idx, genome_idx)`; `map_table_sql` does
the rename to the column names `genome_coverage` requires.

**The `source` argument of every staging builder is interpolated VERBATIM, and
that is the caller's obligation to make safe.** A FROM-clause relation cannot be a
bound parameter, so there is no version of this that binds instead. Pass only a
relation name you control (a registered stream relation, an internal table) or an
expression built from an already-validated path — `parquet.validate_parquet_path`
is what both current callers use. Never build one from unvalidated input: the
client-side consumer runs on a user's machine with the user's own credentials, so
a `source` assembled from user input executes as that user against their own
catalog. Everything else in this module is either a fixed literal or a bound `?`.

**Relation names here are part of the contract, not a caller's choice.**
`woltka_ogu` takes its source relation as a *quoted string literal* and resolves it
on a SEPARATE connection during bind/execute, so `OGU_INPUT_TABLE` is embedded in
the generated SQL: a caller that staged a differently-named table gets a bind error.
That separate connection also cannot see TEMP tables, registered stream relations,
or CTEs — hence every staging statement here creates a regular non-temp TABLE. The
one exception is `COVERAGE_ALIGNMENTS_VIEW`, read only by the `genome_coverage`
macro on the caller's own connection, so a VIEW there avoids duplicating the
alignment slice in RAM.

miint signatures (qiita-verified; see `docs/duckdb-miint.md`):

    genome_coverage(alignments, subject_total_length, subject_genome_id)  -- table macro
      -> (genome_id, covered BIGINT, proportion_covered DOUBLE)
    woltka_ogu(relation, sequence_id_field [, sample_id])                 -- table function
      -> (feature_id, value DOUBLE), or ({sample_id}, feature_id, value)

Both take NATIVE-INTEGER id columns — no `::VARCHAR` casts. `genome_coverage`'s
three arguments are UNQUOTED relation names resolved on the caller's connection;
`woltka_ogu`'s are quoted string literals, and `sample_id` is a named argument.
"""

from __future__ import annotations

# The alignment columns the analytic binds — and, because they ride the DoGet
# ticket, the only ones the data plane will stream. One list, used for both the
# request and the SELECT, so the projection a caller signs and the columns it
# binds cannot drift.
#
# What is absent matters as much as what is present. `cigar` is the wide column
# the projection exists for, and this analytic never reads it: breadth comes from
# `genome_coverage`, whose `alignments` relation needs only
# `reference (= feature_idx), position, stop_position` — it merges spans per
# contig (unlike `compute_coverage_depth`, which we do not use here). The OGU key
# is derived from `feature_idx` through the map, so the raw `feature_idx`
# suffices. `alignment_idx` is absent because the ticket is scoped to one
# alignment run, so every streamed row shares it and the caller already has it.
ALIGNMENT_COLUMNS = (
    "prep_sample_idx",
    "sequence_idx",
    "feature_idx",
    "flags",
    "position",
    "stop_position",
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

ALIGNMENT_TABLE = "alignment_slice"
MAP_TABLE = "contig_to_genome"
GENOME_LENGTHS_TABLE = "genome_lengths"
COVERAGE_ALIGNMENTS_VIEW = "cov_alignments"
SURVIVOR_TABLE = "survivor_genome"
OGU_INPUT_TABLE = "ogu_input"


def coverage_filter_applies(coverage_threshold: float) -> bool:
    """Whether a breadth-of-coverage threshold filters anything.

    At 0 every genome with any alignment trivially qualifies, so there is no
    survivor set to build or join — and the caller must skip streaming the
    reference lengths too, since the coverage calc is their only consumer. Both
    of those decisions are this one predicate, which is why it is a function
    rather than a comparison repeated at each site: an edit to the semantics that
    reached only one of them would open the lengths stream for a calculation that
    never runs, or worse, skip it for one that does.
    """
    return coverage_threshold > 0.0


def alignment_table_sql(source: str) -> str:
    """Materialize the alignment slice from `source` (the caller's stream relation)
    into `ALIGNMENT_TABLE`.

    A real non-temp TABLE, because woltka's separate connection cannot see a
    registered stream relation. The CREATE also drains the stream, so the caller's
    Flight client can close before the compute starts.
    """
    return f"CREATE TABLE {ALIGNMENT_TABLE} AS SELECT {', '.join(ALIGNMENT_COLUMNS)} FROM {source}"


def map_table_sql(source: str) -> str:
    """Stage the feature→genome map from `source` — a relation keyed
    `(feature_idx, genome_idx)`, however the caller obtained it (a staged Parquet
    via `read_parquet(...)`, a REST read) — into `MAP_TABLE`.

    The rename is to the column names `genome_coverage` requires of its
    `subject_genome_id` argument. A real TABLE: read by the macro, by the
    `ogu_input` join, and by the length roll-up.
    """
    return (
        f"CREATE TABLE {MAP_TABLE} AS "
        f"SELECT feature_idx AS contig_id, genome_idx AS genome_id FROM {source}"
    )


def genome_lengths_table_sql(source: str) -> str:
    """Roll per-feature lengths from `source` (the caller's reference_sequences
    stream, columns `(feature_idx, sequence_length_bp)`) up to per-genome
    denominators in `GENOME_LENGTHS_TABLE`.

    **The denominator is the genome's FULL length, including contigs nothing
    aligned to.** That is why the caller streams the whole reference and why
    nothing here touches the alignment: narrowing this to aligned contigs would
    raise every genome's breadth and let low-coverage genomes survive a threshold
    they should fail — a plausible wrong table, not an error.
    """
    return (
        f"CREATE TABLE {GENOME_LENGTHS_TABLE} AS "
        f"SELECT m.genome_id AS genome_id, SUM(l.sequence_length_bp) AS total_length "
        f"FROM {source} l JOIN {MAP_TABLE} m ON l.feature_idx = m.contig_id "
        f"GROUP BY m.genome_id"
    )


def coverage_alignments_view_sql() -> str:
    """The `alignments` argument for `genome_coverage`: the cohort's aligned
    intervals, pooled across every sample — breadth is a cohort property.

    NULL coordinates are excluded: they cannot contribute an interval and would
    poison the merge. A VIEW, not a table — only the macro reads it, on this same
    connection, so materializing it would duplicate the alignment slice in RAM.
    """
    return (
        f"CREATE VIEW {COVERAGE_ALIGNMENTS_VIEW} AS "
        f"SELECT feature_idx AS reference, position, stop_position "
        f"FROM {ALIGNMENT_TABLE} "
        f"WHERE position IS NOT NULL AND stop_position IS NOT NULL"
    )


def pooled_survivor_table_sql() -> str:
    """Genomes whose breadth of coverage, POOLED over the whole cohort, meets the
    threshold. Requires `COVERAGE_ALIGNMENTS_VIEW` and `GENOME_LENGTHS_TABLE`.

    The threshold is a bound parameter — execute with `[coverage_threshold]`.
    """
    return (
        f"CREATE TABLE {SURVIVOR_TABLE} AS SELECT genome_id "
        f"FROM genome_coverage({COVERAGE_ALIGNMENTS_VIEW}, {GENOME_LENGTHS_TABLE}, {MAP_TABLE}) "
        f"WHERE proportion_covered >= ?"
    )


def ogu_input_table_sql(*, filter_to_survivors: bool) -> str:
    """woltka's input: the alignment pre-mapped to genome level, so woltka counts
    at genome granularity. The map's INNER JOIN also drops alignments to features
    with no genome (a 16S record is not an OGU).

    When a threshold applies, `filter_to_survivors=True` joins the survivor set
    **here, before woltka**. That ordering is load-bearing: `woltka_ogu` splits a
    multi-mapped read across its number of distinct `reference` values, so a read
    hitting a surviving and a dropped genome must lose the dropped one first to
    renormalize to 1.0 on the survivor. Filtering woltka's OUTPUT instead strands
    it at 0.5 — a plausible number rather than an error.

    A real non-temp TABLE: woltka resolves it on a separate connection.
    """
    sql = (
        f"CREATE TABLE {OGU_INPUT_TABLE} AS "
        f"SELECT a.sequence_idx, a.prep_sample_idx, a.flags, m.genome_id AS reference "
        f"FROM {ALIGNMENT_TABLE} a "
        f"JOIN {MAP_TABLE} m ON a.feature_idx = m.contig_id"
    )
    if filter_to_survivors:
        sql += f" JOIN {SURVIVOR_TABLE} s ON m.genome_id = s.genome_id"
    return sql


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
