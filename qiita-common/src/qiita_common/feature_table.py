"""The feature-table analytic, as SQL text.

Two consumers run this same analytic and must not disagree about it: the
compute-orchestrator native job `estimate_feature_table` (server-side, reached
through a work ticket) and the client-side feature-table recipe (a user's machine,
composing the analytic-export routes). They differ in everything *around* the
analytic — where the three inputs come from, how the result is written — and in
nothing about the analytic itself, so the SQL lives here and the streaming and I/O
stay with each caller.

Today the one table flavour is **OGU** (genome-keyed counts via miint's
`woltka_ogu`); names carrying `ogu` denote that computation specifically, and the
module is named for the general surface because taxonomy- and tree-derived tables
join it later.

**Breadth of coverage filters the table, at one of two scopes** — pooled over the
whole cohort, or per `(sample, genome)`. The two are not symmetric; `CoverageScope`
says why.

**Plain SQL text, so this module needs no connection of its own.** Callers execute
these statements on a connection that has miint loaded. (Same shape as `chunking.py`'s
expression builders and `parquet.py`'s option strings.) That is a choice about where
the I/O lives, not a prohibition: miint is core to all of qiita and both services that
import this one already depend on duckdb.

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

**A caller that PUBLISHES the table stages two more** — the label relations — and
relabels the counts through them, ending at `LABELLED_RELATION` (`LABELLED_SCHEMA`):
our `*_idx` keys are gone, and the public handles are VARCHAR, which is what makes
the result writable as BIOM at all. See the relabel section below, and the two
writers after it — `LABELLED_RELATION` is the only relation here they will copy.

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
or CTEs — hence every staging statement here creates a regular non-temp TABLE.
**Two relations are exceptions, both read only on the caller's own connection and
both VIEWs for the same reason** — materializing would duplicate a large relation
in RAM for one reader: `COVERAGE_ALIGNMENTS_VIEW` (read by the `genome_coverage`
macro) and `LABELLED_RELATION` (read by one COPY).

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

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

# `validate_parquet_path` is named for its first caller but checks a COPY *target* —
# a path safe to interpolate into a SQL string literal, which cannot be bound — so it
# is what both writers below use, BIOM included.
from .parquet import PARQUET_OPTS, validate_parquet_path
from .taxonomy import (
    QUOTED_RANK_COLUMNS,
    RANK_COLUMNS,
    genome_representative_taxonomy_select_sql,
    prefixed_rank_columns_sql,
    rank_columns_sql,
)


class CoverageScope(StrEnum):
    """The dimension breadth of coverage is measured over.

    A plain `StrEnum` with no Postgres twin, deliberately: this is a per-request
    analytic parameter chosen by the caller, never stored, so there is no column
    for a database enum to guard. The values are the CLI's spelling.

    * `POOLED` — one breadth per genome, over every sample in the cohort. A genome
      that clears the threshold keeps its rows for **all** samples.
    * `PER_SAMPLE` — one breadth per `(sample, genome)`. Strictly stricter: since
      pooling unions intervals, pooled breadth ≥ any single sample's, so this can
      only ever remove rows relative to `POOLED`, never add them.
    """

    POOLED = "pooled"
    PER_SAMPLE = "per-sample"


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
# The ungated slice, staged only when a gate needs to read `cigar`. It exists
# because the gate's fail-loud checks have to see the rows the gate would DROP, and
# the alignment arrives as a one-shot Flight stream that cannot be scanned twice.
STREAMED_ALIGNMENT_TABLE = "alignment_streamed"
MAP_TABLE = "contig_to_genome"
GENOME_LENGTHS_TABLE = "genome_lengths"
COVERAGE_ALIGNMENTS_VIEW = "cov_alignments"
OGU_INPUT_TABLE = "ogu_input"

# ONE SURVIVOR RELATION PER SCOPE, because the two have different shapes:
# `(genome_id)` for pooled, `(prep_sample_idx, genome_id)` for per-sample. The
# names differ so that building one scope's set and joining the other's is a bind
# error in BOTH directions.
#
# Under a single shared name only one direction fails loudly. The other — a
# per-sample set joined on the genome alone — is valid SQL and silently wrong: an
# alignment row fans out once per sample the genome survived in, inflating every
# count for that genome regardless of which sample the read came from. A caller
# choosing the scope from a runtime flag is exactly the shape that gets this wrong,
# so the relation name carries the scope rather than a docstring asking nicely.
_SURVIVOR_TABLES = {
    CoverageScope.POOLED: "survivor_genome_pooled",
    CoverageScope.PER_SAMPLE: "survivor_genome_per_sample",
}


def survivor_table_name(scope: CoverageScope) -> str:
    """The relation `survivor_table_sql(scope)` creates and `ogu_input_table_sql`
    joins for that same scope."""
    return _SURVIVOR_TABLES[scope]


def coverage_filter_applies(coverage_threshold: float) -> bool:
    """Whether a breadth-of-coverage threshold filters anything.

    At 0 every genome with any alignment trivially qualifies, so there is no
    survivor set to build or join — and the caller must skip streaming the
    reference lengths too, since the coverage calc is their only consumer. Both
    of those decisions are this one predicate, which is why it is a function
    rather than a comparison repeated at each site: an edit to the semantics that
    reached only one of them would open the lengths stream for a calculation that
    never runs, or worse, skip it for one that does.

    Refuses a threshold that is not a proportion, the same way `AlignmentGate`
    refuses its own: each consumer validates at its boundary (a Pydantic field, an
    argparse type), but out of range the two failures here are silent rather than
    loud — a negative threshold reads as "no filter at all", and one above 1 drops
    every genome and returns an empty table that looks like a result.
    """
    if not 0.0 <= coverage_threshold <= 1.0:
        raise ValueError(
            f"coverage_threshold must be a proportion in [0, 1], got {coverage_threshold!r}"
        )
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
    """The aligned intervals both coverage scopes measure, as the `alignments`
    argument `genome_coverage` takes.

    Carries `prep_sample_idx` even though the macro names only
    `(reference, position, stop_position)`: the macro reads
    `query_table(alignments)` and projects the three columns by name, so the extra
    one is tolerated (probed against the mirror build) and per-sample can group by
    it. One view therefore serves both scopes instead of two near-identical ones.

    NULL coordinates are excluded. `compress_intervals` — which both scopes reach,
    the pooled one inside the macro — drops such rows silently rather than
    erroring, so filtering here is what makes the exclusion visible to a reader
    rather than implicit in an aggregate's behaviour.

    A VIEW, not a table: only this connection reads it, so materializing would
    duplicate the alignment slice in RAM.
    """
    return (
        f"CREATE VIEW {COVERAGE_ALIGNMENTS_VIEW} AS "
        f"SELECT prep_sample_idx, feature_idx AS reference, position, stop_position "
        f"FROM {ALIGNMENT_TABLE} "
        f"WHERE position IS NOT NULL AND stop_position IS NOT NULL"
    )


def survivor_table_sql(scope: CoverageScope) -> str:
    """The survivor set for `scope`: what clears the breadth-of-coverage threshold.
    Requires `COVERAGE_ALIGNMENTS_VIEW` and `GENOME_LENGTHS_TABLE`. The threshold is
    a bound parameter — execute with `[coverage_threshold]`.

    Creates `survivor_table_name(scope)`, whose shape differs per scope —
    `(genome_id)` for pooled, `(prep_sample_idx, genome_id)` for per-sample. That is
    why the name carries the scope: see `_SURVIVOR_TABLES`.

    `POOLED` delegates to the `genome_coverage` macro. `PER_SAMPLE` cannot: the
    macro has no sample key. It instead reproduces the macro's own method with one
    more `GROUP BY` key — `compress_intervals` per contig, summed to the genome,
    over the same full-length denominator and the same `CAST(... AS DOUBLE)`
    division, so a single threshold means the same thing under either scope. This
    is what upstream means by the per-sample dimension being "already expressible
    today" (duckdb-miint#217); if `genome_coverage_per_sample` lands
    (duckdb-miint#220, an open PR) this branch collapses to one call.

    The per-contig merge before the genome roll-up is not incidental:
    `compress_intervals` merges within one coordinate space, so grouping straight
    to the genome would merge intervals from DIFFERENT contigs as though they
    shared coordinates and understate every multi-contig genome.
    """
    if scope is CoverageScope.POOLED:
        return (
            f"CREATE TABLE {survivor_table_name(scope)} AS SELECT genome_id "
            f"FROM genome_coverage("
            f"{COVERAGE_ALIGNMENTS_VIEW}, {GENOME_LENGTHS_TABLE}, {MAP_TABLE}) "
            f"WHERE proportion_covered >= ?"
        )
    return (
        f"CREATE TABLE {survivor_table_name(scope)} AS "
        f"WITH per_contig AS ("
        f"SELECT prep_sample_idx, reference, "
        f"UNNEST(compress_intervals(position, stop_position)) AS ci "
        f"FROM {COVERAGE_ALIGNMENTS_VIEW} GROUP BY prep_sample_idx, reference"
        f"), per_contig_genome AS ("
        f"SELECT p.prep_sample_idx, m.genome_id, "
        f"SUM(p.ci.stop - p.ci.start) AS covered_internal "
        f"FROM per_contig p JOIN {MAP_TABLE} m ON p.reference = m.contig_id "
        f"GROUP BY p.prep_sample_idx, m.genome_id, p.reference"
        f"), covered AS ("
        f"SELECT prep_sample_idx, genome_id, SUM(covered_internal) AS covered "
        f"FROM per_contig_genome GROUP BY prep_sample_idx, genome_id"
        f") SELECT c.prep_sample_idx, c.genome_id FROM covered c "
        f"JOIN {GENOME_LENGTHS_TABLE} l USING (genome_id) "
        f"WHERE CAST(c.covered AS DOUBLE) / l.total_length >= ?"
    )


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

    The survivor join happens **here, before woltka**, and that ordering is
    load-bearing: `woltka_ogu` splits a multi-mapped read across its number of
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
    consumer**, for the reason the module exists at all: a consumer that dropped the
    survivor join would not fail, it would publish a table with unfiltered genomes
    in it. A missed CREATE is a bind error; a missed filter is a plausible wrong
    answer. Same device as `GateClearance.statements`.

    The trailing DROPs are not tidiness. The coverage view, the alignment slice, and
    the survivor set are all dead once `OGU_INPUT_TABLE` exists, and they are dead
    for the whole remaining run — woltka, the relabel, and the write. On a client
    machine over a large cohort that is several hundred MB held for nothing, and
    DuckDB's spill directory is wherever the user happened to run the CLI.
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


def drop_ogu_input_table_sql() -> str:
    """Release woltka's input once the counts exist. Dead from that point on, and
    the largest relation still standing — see `ogu_input_statements` for why that
    matters on a client."""
    return f"DROP TABLE {OGU_INPUT_TABLE}"


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


# ---------------------------------------------------------------------------
# The CIGAR identity / query-coverage gate
# ---------------------------------------------------------------------------

# One PLACEMENT of a read, as a GROUP BY / PARTITION BY key over alignment rows.
#
# Pooling by read alone would concatenate a read's distinct placements — different
# features, or different positions on one feature — and score that concatenation,
# which is nobody's intended semantics. Mates store their own and their partner's
# coordinates in SWAPPED order, so LEAST/GREATEST give both mates of one placement
# the same key; an unpaired row has a NULL `mate_position` and both collapse to
# `position`, making it its own single-row partition.
#
# Verified against real aligner output in the orchestrator's `align_sharded`, which
# imports it from here — one definition, because a change to it that reached only
# one copy would silently rescore every paired placement in the other.
PAIRED_PLACEMENT_PARTITION = (
    "sequence_idx, feature_idx, LEAST(position, mate_position), GREATEST(position, mate_position)"
)


@dataclass(frozen=True, kw_only=True)
class AlignmentGate:
    """Thresholds an alignment must clear to be counted, and how to judge a pair.

    Both scorers return a proportion in [0, 1]. The two thresholds are independent
    and either may be omitted — which matters more than it looks, because
    **`cigar_sequence_identity` needs an `=`/`X` (eqx) CIGAR and
    `cigar_query_coverage` does not** (probed: a plain `150M` scores `qcov` 1.0 and
    `identity` NULL). So a coverage-only gate works on alignments an identity gate
    cannot judge at all.

    `paired` pools a placement's two mates and judges them as a unit, so a pair is
    kept or dropped together and a mate is never orphaned.

    **`paired=True` is correct for single-end rows too** — their partition is one row,
    which scores the same as a per-row predicate — so this flag is a performance
    choice, not a correctness one: pooling costs a full blocking sort of the slice.
    Getting it wrong in the cheap direction is a silent correctness loss, so
    `check_gate_diagnostics` refuses an unpaired gate over a slice that contains
    paired reads rather than trusting the caller to have known.
    """

    min_identity: float | None = None
    min_query_coverage: float | None = None
    paired: bool = False

    def __post_init__(self) -> None:
        if self.min_identity is None and self.min_query_coverage is None:
            raise ValueError(
                "AlignmentGate needs at least one threshold (min_identity or "
                "min_query_coverage): a gate with neither filters nothing while still "
                "costing the wide `cigar` column on the wire, so it is a mistake "
                "rather than a no-op. Pass no gate at all instead."
            )
        for name, value in (
            ("min_identity", self.min_identity),
            ("min_query_coverage", self.min_query_coverage),
        ):
            if value is not None and not 0.0 <= value <= 1.0:
                raise ValueError(
                    f"AlignmentGate.{name} must be a proportion in [0, 1], got {value!r}"
                )


@dataclass(frozen=True)
class GateClearance:
    """Evidence that `check_gate_diagnostics` ran and passed for `gate`.

    `gated_alignment_table_sql` takes one of these rather than a bare gate, which is
    what makes "diagnose before you gate" a constraint instead of a docstring: two of
    the gate's failure modes are silent, so a caller who skips the check gets a
    plausible wrong answer, and a clearance is not something a caller can produce by
    doing anything other than the check.
    """

    gate: AlignmentGate

    @property
    def statements(self) -> tuple[tuple[str, list[float]], ...]:
        """The remaining `(sql, parameters)` pairs, in the order they must run: apply
        the gate, then release the streamed copy that holds `cigar`.

        Iterating this is the whole rest of the protocol, so a caller cannot bind the
        wrong parameters to the predicate or forget the cleanup.
        """
        return (
            (gated_alignment_table_sql(clearance=self), gate_parameters(self.gate)),
            (drop_streamed_alignment_table_sql(), []),
        )


def _gate_terms(gate: AlignmentGate, cigar_expr: str) -> list[tuple[str, float]]:
    """The gate's predicates over `cigar_expr`, paired with their bound values, in
    ONE order — so the SQL's `?` placeholders and `gate_parameters` cannot disagree
    about which threshold is which."""
    terms: list[tuple[str, float]] = []
    if gate.min_identity is not None:
        terms.append((f"cigar_sequence_identity({cigar_expr}) >= ?", gate.min_identity))
    if gate.min_query_coverage is not None:
        terms.append((f"cigar_query_coverage({cigar_expr}) >= ?", gate.min_query_coverage))
    return terms


def gate_parameters(gate: AlignmentGate) -> list[float]:
    """The values to bind to `gated_alignment_table_sql`'s placeholders, in order."""
    return [value for _, value in _gate_terms(gate, "cigar")]


def gate_alignment_columns(gate: AlignmentGate | None) -> tuple[str, ...]:
    """The projection to request from the alignment DoGet: `ALIGNMENT_COLUMNS`, plus
    whatever `gate` needs to read.

    `cigar` rides only when something scores it (see `ALIGNMENT_COLUMNS` for what
    that column costs) and `mate_position` only when the gate pools, since its sole
    use is keying the partition. Both are in the ticket's allowlist.
    """
    if gate is None:
        return ALIGNMENT_COLUMNS
    extra = ("cigar",) + (("mate_position",) if gate.paired else ())
    return ALIGNMENT_COLUMNS + extra


def streamed_alignment_table_sql(source: str, *, gate: AlignmentGate) -> str:
    """Materialize the ungated slice, including the gate's extra columns, into
    `STREAMED_ALIGNMENT_TABLE`. The gated path's first step; an ungated caller uses
    `alignment_table_sql` directly and never creates this relation.

    Ungated and materialized because the fail-loud checks below must see the rows the
    gate would drop, and the stream behind `source` is one-shot.
    """
    return (
        f"CREATE TABLE {STREAMED_ALIGNMENT_TABLE} AS "
        f"SELECT {', '.join(gate_alignment_columns(gate))} FROM {source}"
    )


def gated_alignment_table_sql(*, clearance: GateClearance) -> str:
    """Apply the cleared gate to `STREAMED_ALIGNMENT_TABLE`, producing
    `ALIGNMENT_TABLE` — the same relation the ungated path creates, so **everything
    downstream is identical and a gate cannot be half-applied**: there is one
    relation name to read, and it is either gated or not. In particular the gate
    filters the coverage calculation as well as the counts, which is the point: an
    alignment that fails it is not a placement, so it must not contribute covered
    bases either.

    **Takes a `GateClearance`, not a gate**, so it is unreachable without having run
    `check_gate_diagnostics` — two of this predicate's failure modes are silent, and
    a docstring asking the caller to check first is not a constraint. Prefer
    `clearance.statements`, which pairs this with its parameters and the cleanup
    below in the order they must run.

    Projects exactly `ALIGNMENT_COLUMNS`, so `cigar` stops here instead of
    propagating into the coverage view and woltka's input. A real TABLE, not a view,
    so the CIGARs are parsed once rather than on every downstream scan.

    An unpaired gate filters per row with `WHERE`. A paired one pools each
    placement's mates and filters with `QUALIFY`, which applies after the window.
    """
    gate = clearance.gate
    scored, clause = (
        (f"string_agg(cigar, '') OVER (PARTITION BY {PAIRED_PLACEMENT_PARTITION})", "QUALIFY")
        if gate.paired
        else ("cigar", "WHERE")
    )
    predicate = " AND ".join(sql for sql, _ in _gate_terms(gate, scored))
    return (
        f"CREATE TABLE {ALIGNMENT_TABLE} AS "
        f"SELECT {', '.join(ALIGNMENT_COLUMNS)} FROM {STREAMED_ALIGNMENT_TABLE} "
        f"{clause} {predicate}"
    )


def drop_streamed_alignment_table_sql() -> str:
    """Release the streamed copy once the gate has been applied. Nothing reads it
    again, and it holds `cigar` — so on a client machine over a large cohort, keeping
    it roughly doubles the analytic's peak memory for no benefit. `ogu_input_statements`
    releases the rest of the pipeline's dead relations for the same reason."""
    return f"DROP TABLE {STREAMED_ALIGNMENT_TABLE}"


def gate_diagnostics_sql(gate: AlignmentGate) -> str:
    """One row for `check_gate_diagnostics`, over `STREAMED_ALIGNMENT_TABLE`:
    `(total_rows, scorable_rows, unpoolable_partitions, paired_rows)`.

    Each count is computed only when it could matter — parsing every CIGAR, and
    grouping every placement, are both real work over the slice that still holds
    `cigar`.

    **A paired gate takes ONE grouped pass, not a plain pass plus a grouped
    subquery.** Every count here is additive over the placement partitions, so
    aggregating the groups again gives identical numbers for one read of the widest
    relation in the pipeline instead of two — and `paired` is the common case. The
    `coalesce`s are load-bearing rather than defensive: `sum()` over zero groups is
    NULL, which would turn the empty slice's `total_rows` into NULL and silently
    stop `check_gate_diagnostics`' `== 0` early return from firing.
    """
    scorable = "count(cigar_sequence_identity(cigar))" if gate.min_identity is not None else "NULL"
    # miint's own predicate over the SAM flag, not hand-rolled bit math: the
    # `alignment_is_*` family is what `docs/duckdb-miint.md` tells callers to use, and
    # it reads the same `flags` USMALLINT the lake stores. 0x1 is set on both mates
    # whether or not either mapped, so it answers "is this slice paired data?" even
    # when a mate's row never arrived.
    paired_rows = "count(*) FILTER (WHERE alignment_is_paired(flags))"
    if not gate.paired:
        return (
            f"SELECT count(*) AS total_rows, {scorable} AS scorable_rows, "
            f"0 AS unpoolable_partitions, {paired_rows} AS paired_rows "
            f"FROM {STREAMED_ALIGNMENT_TABLE}"
        )
    # A partition that cannot be a single placement's mates, in the three ways it
    # can fail. `count(col)` skips NULLs, which is what makes each detectable:
    #   1. a row is present but carries no CIGAR   -> string_agg scores short;
    #   2. a row claims a mapped mate that is not in the slice at all -> the
    #      partition holds one row and looks complete, so (1) cannot see it;
    #   3. more than two rows share the key -> not one placement, so the
    #      concatenation spans unrelated alignments.
    return (
        f"WITH placement AS (SELECT count(*) AS rows_in_partition, "
        f"count(cigar) AS with_cigar, count(mate_position) AS with_mate, "
        f"{scorable} AS scorable, {paired_rows} AS paired "
        f"FROM {STREAMED_ALIGNMENT_TABLE} GROUP BY {PAIRED_PLACEMENT_PARTITION}) "
        f"SELECT coalesce(sum(rows_in_partition), 0) AS total_rows, "
        f"sum(scorable) AS scorable_rows, "
        f"count(*) FILTER (WHERE rows_in_partition <> with_cigar "
        f"OR (with_mate > 0 AND rows_in_partition = 1) "
        f"OR rows_in_partition > 2) AS unpoolable_partitions, "
        f"coalesce(sum(paired), 0) AS paired_rows FROM placement"
    )


def check_gate_diagnostics(
    gate: AlignmentGate,
    *,
    total_rows: int,
    scorable_rows: int | None,
    unpoolable_partitions: int,
    paired_rows: int,
) -> GateClearance:
    """Refuse every gate failure that would otherwise produce a plausible wrong
    answer instead of an error, and return the `GateClearance` that
    `gated_alignment_table_sql` requires. Takes `gate_diagnostics_sql(gate)`'s row.

    Both consumers must refuse identically, which is why the judgement lives here
    rather than in each of them.

    **Partially-unscorable slices are NOT refused.** A CIGAR that cannot be scored
    fails its own row\'s predicate and affects no other row, so dropping it is the
    gate working; a caller that wants to tell the user how many rows went that way
    can compare `scorable_rows` against `total_rows` itself.
    """
    if total_rows == 0:
        # 0 of 0 unscorable is not evidence of anything, and an empty slice is a
        # legitimate result elsewhere in this analytic.
        return GateClearance(gate)

    if gate.min_identity is not None:
        if scorable_rows is None:
            raise ValueError(
                "check_gate_diagnostics: scorable_rows is NULL while min_identity is "
                "set. The row must come from gate_diagnostics_sql(the SAME gate), "
                "which only emits NULL there when identity is not being gated."
            )
        if scorable_rows == 0:
            raise ValueError(
                f"None of the {total_rows} alignment rows can be scored for sequence "
                f"identity: `cigar_sequence_identity` needs a CIGAR carrying `=`/`X` "
                f"ops (an 'eqx' CIGAR) and returns NULL otherwise, and "
                f"`NULL >= threshold` drops the row — so this gate would silently "
                f"discard EVERY row and return an empty feature table that looks like "
                f"a real result. Either re-align with eqx CIGARs, drop the identity "
                f"threshold, or gate on query coverage instead, which any CIGAR "
                f"supports."
            )

    if not gate.paired and paired_rows:
        raise ValueError(
            f"{paired_rows} of {total_rows} alignment rows are paired (SAM FLAG 0x1), "
            f"but this gate scores each row on its own CIGAR. "
            f"That judges a placement's mates independently and orphans one when they "
            f"disagree, which is the guarantee the pooled form exists to give. Pass "
            f"`paired=True`; it is also correct for single-end rows, whose partition is "
            f"a single row, and costs only a sort."
        )

    if gate.paired and unpoolable_partitions:
        raise ValueError(
            f"{unpoolable_partitions} pooled partitions are not a single paired "
            f"placement: a mate row carries no CIGAR, or claims a mapped mate absent "
            f"from this slice, or more than two rows share the placement key. "
            f"`string_agg` skips NULLs and concatenates whatever it is given, so each "
            f"of these would be scored on part of a placement — or on unrelated "
            f"alignments — and silently kept or dropped on that basis. Restrict the "
            f"cohort to alignments whose mates both mapped, or use the unpaired gate, "
            f"which scores every row on its own CIGAR."
        )

    return GateClearance(gate)


# ---------------------------------------------------------------------------
# The relabel to public identifiers
# ---------------------------------------------------------------------------

# The counts, materialized. Both the populated and the empty path land here, so the
# relabel below — and every writer after it — reads ONE relation whose name and
# shape do not depend on whether the cohort had any alignments. Client-side only:
# the server-side job COPYs `woltka_ogu_select_sql()` straight out to Parquet and
# relabels nothing, because its output stays inside the system.
OGU_OUTPUT_TABLE = "ogu_output"

# The two label relations, each `internal key -> public handle`.
#
# **The row axis is a genome only because this table flavour is OGU.** A feature is not
# always a genome — an amplicon sequence variant, a full-length 16S observed in a sample,
# an assembled contig are all features with no genome to roll up to — so a row-keyed
# flavour is a genuine second shape, not a variant spelling of this one. The identifier
# layer is already ready for it: the mint behind `export_feature_id` publishes a genome
# OR a `(reference, feature)` pair. What is genome-only is the roll-up, which counts what
# `woltka_ogu` counts. This name says `genome` to be honest about that, rather than
# implying generality the computation does not have.
GENOME_LABEL_TABLE = "genome_label"
SAMPLE_LABEL_TABLE = "sample_label"

# The published shape — a VIEW, not a table, and the second exception to the
# non-temp-TABLE rule above (`COVERAGE_ALIGNMENTS_VIEW` is the first, for the same
# reason): only the caller's own connection reads it, and its one reader is a COPY.
# Materializing would hold a second full copy of the output — two VARCHARs and a
# DOUBLE per row, tens of millions of rows at cohort scale — alive at exactly the
# moment the BIOM writer is building its sparse matrix.
LABELLED_RELATION = "feature_table_labelled"

# What a PUBLISHED table carries, name -> SQL type. Neither id is one of ours: both
# come from a mint whose job is to hand out a handle that means something outside this
# system — `export_id` per processed sample, `export_feature_id` per row. An `*_idx`
# means nothing out there and is not a handle we promise to keep.
#
# The VARCHAR types are why the relabel is load-bearing rather than cosmetic: BIOM
# requires both id columns as VARCHAR while woltka hands back native BIGINTs, so
# the order is relabel-then-write and there is no writable table before the join.
LABELLED_SCHEMA = {
    "sample_id": "VARCHAR",
    "feature_id": "VARCHAR",
    "value": "DOUBLE",
}
LABELLED_COLUMNS = tuple(LABELLED_SCHEMA)


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


def genome_label_table_sql(source: str) -> str:
    """`genome_idx -> feature_id` from `source`, the exported-feature mint's response
    as the route returns it (`(genome_idx, export_feature_id, …)`).

    One row per genome already, so no DISTINCT: a duplicate here would be a mint that
    answered twice for one genome, which `check_relabel_diagnostics` refuses rather
    than silently collapsing — the two rows might carry different handles. Same shape
    and same reasoning as `sample_label_table_sql`.

    **The handle is not always the genome's accession**, which is why this reads a
    minted column and not `genome.source_id`: the mint publishes the accession
    wherever one exists and is unique across the published namespace, and a `QF<n>`
    handle where it is not. The uniqueness that makes a published label name one thing
    is a database constraint on that mint, not something a client can assert about a
    map it was handed.
    """
    return (
        f"CREATE TABLE {GENOME_LABEL_TABLE} AS "
        f"SELECT genome_idx, export_feature_id AS feature_id FROM {source}"
    )


def published_membership_sql() -> str:
    """The feature→genome map restricted to the genomes the table PUBLISHED, as a relation
    expression yielding `(feature_idx, genome_idx, feature_id)`.

    **`MAP_TABLE` is the whole reference's map** — one row per `(feature, genome)` pair,
    and `feature_genome` is many-to-many, so a feature two organisms share appears once per
    genome. `GENOME_LABEL_TABLE` holds only the genomes the roll-up emitted. Restricting
    one by the other is load-bearing in two different ways, which is why it is named here
    once instead of spelled at each site:

    * for the taxonomy sidecar it makes the reduction exactly as wide as the table, and
      cheap — a reference's whole taxonomy is reduced only for the genomes that survived;
    * for the tree it is what stops an UNPUBLISHED co-genome from fanning a tip into two
      nodes. Unrestricted, a feature published under one genome and merely *present* under
      another produces two rows for one tip, which reads as an ambiguity the published
      artifact does not have — and coverage filtering drops some but not all of a shared
      feature's genomes routinely, so that is the common case rather than a corner.
    """
    return (
        f"SELECT m.contig_id AS feature_idx, m.genome_id AS genome_idx, l.feature_id "
        f"FROM {MAP_TABLE} m JOIN {GENOME_LABEL_TABLE} l ON l.genome_idx = m.genome_id"
    )


def sample_label_table_sql(source: str) -> str:
    """`prep_sample_idx -> sample_id` from `source`, the exported-identifier map as
    the mint route returns it (`(prep_sample_idx, export_id, …)`).

    One row per sample already, so no DISTINCT: a duplicate here would be a mint
    that answered twice for one sample, which `check_relabel_diagnostics` refuses
    rather than silently collapsing — the two rows might carry different handles.
    """
    return (
        f"CREATE TABLE {SAMPLE_LABEL_TABLE} AS "
        f"SELECT prep_sample_idx, export_id AS sample_id FROM {source}"
    )


def _labelled_select_sql() -> str:
    """The counts with both labels attached — the ONE join definition the
    diagnostics measure and the relabel writes, so what was checked is what lands.

    LEFT, not INNER: a count whose genome or sample has no label must survive the
    join as a NULL id for the diagnostics to see and refuse it. An INNER join would
    drop it instead, shortening the published table by exactly the rows a caller had
    no way to notice were missing.
    """
    return (
        f"SELECT o.prep_sample_idx, o.genome_idx, s.sample_id, g.feature_id, o.value "
        f"FROM {OGU_OUTPUT_TABLE} o "
        f"LEFT JOIN {GENOME_LABEL_TABLE} g ON o.genome_idx = g.genome_idx "
        f"LEFT JOIN {SAMPLE_LABEL_TABLE} s ON o.prep_sample_idx = s.prep_sample_idx"
    )


@dataclass(frozen=True)
class LabelClearance:
    """Evidence that `check_relabel_diagnostics` ran and passed, plus the number of
    rows it cleared — which is the row count of the table about to be written, so a
    caller can report the size without counting it again.

    `labelled_relation_sql` takes one of these for the same reason
    `gated_alignment_table_sql` does: every failure the checks catch produces a
    published table that looks right, so the check cannot be optional.
    """

    rows: int


def relabel_diagnostics_sql() -> str:
    """One row for `check_relabel_diagnostics`, over the labelled join.

    The unjoined count comes from a scalar subquery because it is the one number the
    joined relation cannot report: if the join fanned out, its own `count(*)` is
    already the inflated figure.
    """
    return (
        f"SELECT (SELECT count(*) FROM {OGU_OUTPUT_TABLE}) AS output_rows, "
        f"count(*) AS labelled_rows, "
        f"count(*) FILTER (WHERE feature_id IS NULL) AS unlabelled_genome_rows, "
        f"count(*) FILTER (WHERE sample_id IS NULL) AS unlabelled_sample_rows, "
        f"count(DISTINCT genome_idx) AS genomes, "
        f"count(DISTINCT feature_id) AS feature_ids, "
        f"count(DISTINCT prep_sample_idx) AS samples, "
        f"count(DISTINCT sample_id) AS sample_ids "
        f"FROM ({_labelled_select_sql()})"
    )


def check_relabel_diagnostics(
    *,
    output_rows: int,
    labelled_rows: int,
    unlabelled_genome_rows: int,
    unlabelled_sample_rows: int,
    genomes: int,
    feature_ids: int,
    samples: int,
    sample_ids: int,
) -> LabelClearance:
    """Refuse every way the relabel can produce a WRONG published table instead of
    an error, and return the `LabelClearance` `labelled_relation_sql` requires. Takes
    `relabel_diagnostics_sql()`'s row.

    All three faults are silent in the output: an inflated count, a row named by a
    NULL id, or two organisms merged under one handle. None of them raises anywhere
    downstream, so this is the only place they can be caught.
    """
    if labelled_rows != output_rows:
        # Phrased for either direction, though only one is reachable through the LEFT
        # JOIN above: a label relation with two rows for one key inflates, and nothing
        # drops rows. An INNER JOIN here would make the other direction possible.
        raise ValueError(
            f"the label join changed the table's size: {output_rows} counted rows "
            f"became {labelled_rows}. A label relation holding more than one row for "
            f"one key repeats every count for that key and inflates its value. Both "
            f"label relations are staged from a mint that answers once per entity, so "
            f"check whichever response was staged for a repeated genome_idx or "
            f"prep_sample_idx."
        )

    # Both unlabelled checks run BEFORE the collision checks below, which compare
    # `count(DISTINCT ...)` of an internal key against its label: NULLs are skipped
    # by that aggregate, so unlabelled rows depress the label count and are
    # indistinguishable from a collision. Reversing the order would report the wrong
    # fault for the commoner mistake.
    if unlabelled_genome_rows:
        raise ValueError(
            f"{unlabelled_genome_rows} of {output_rows} rows name a genome with no "
            f"public handle, so the published table would carry a NULL feature_id. "
            f"The exported-feature mint has to cover every genome the counts mention — "
            f"mint it for the genomes the roll-up actually emitted, not for a set "
            f"resolved before the coverage filter ran."
        )
    if unlabelled_sample_rows:
        raise ValueError(
            f"{unlabelled_sample_rows} of {output_rows} rows name a sample with no "
            f"public handle, so the published table would carry a NULL sample_id. The "
            f"exported-identifier map has to cover the whole cohort the alignment "
            f"slice was streamed for — mint it for the same prep_sample list."
        )

    if feature_ids < genomes:
        raise ValueError(
            f"{genomes} genomes in this table share only {feature_ids} distinct "
            f"export_feature_ids, so relabelling merges genomes that are not the same "
            f"organism. The mint's published namespace is UNIQUE across live rows, so "
            f"this cannot happen server-side — but the check costs one comparison and "
            f"the failure it guards is invisible: upstream documents that a BIOM write "
            f"SUMS duplicate (feature_id, sample_id) pairs, so two organisms would "
            f"quietly become one row. A response staged from anywhere other than the "
            f"mint route is the thing to suspect."
        )
    if sample_ids < samples:
        raise ValueError(
            f"{samples} samples in this table share only {sample_ids} distinct "
            f"export_ids, so relabelling merges samples. An export_id is minted per "
            f"processed sample and cannot repeat, so the exported-identifier map this "
            f"was staged from did not come from the mint route."
        )

    return LabelClearance(rows=labelled_rows)


def labelled_relation_sql(*, clearance: LabelClearance) -> str:
    """Relabel the counts into `LABELLED_RELATION`: `LABELLED_COLUMNS` and nothing else.

    The projection is the enforcement. Both `*_idx` columns are joined ON and then
    dropped, so no writer downstream can read one out of this relation even by
    accident — which is the property that keeps our internal identifiers out of a
    file somebody publishes.

    **Takes a `LabelClearance`**, so it cannot be reached by accident without having
    run `check_relabel_diagnostics`; see `LabelClearance`.
    """
    return (
        f"CREATE VIEW {LABELLED_RELATION} AS "
        f"SELECT {', '.join(LABELLED_COLUMNS)} FROM ({_labelled_select_sql()})"
    )


# ---------------------------------------------------------------------------
# Writing the relabelled table out
# ---------------------------------------------------------------------------

# BIOM's `generated-by` attribute. The writer's own default is `miint`, which names
# the library rather than the system that produced the file. The system and no
# version, deliberately: a version here would have to be kept honest against four
# components and a pinned extension build, and the bundle's manifest is where the
# provenance that reproduces a table actually lives — the reference, the cohort, the
# coverage scope and threshold, the gate, and the tool versions including the miint
# build. This attribute only says which system wrote the file.
BIOM_GENERATED_BY = "qiita-miint"


# ---------------------------------------------------------------------------
# The taxonomy sidecar
# ---------------------------------------------------------------------------

# The reference's per-feature taxonomy, as streamed from the data plane's
# exclusion-aware view. A real TABLE: the reduction below reads it twice (once to pick
# each genome's representative, once to take that member's ranks), and a Flight stream
# cannot be scanned twice.
TAXONOMY_TABLE = "reference_taxonomy"

# The published sidecar — a VIEW, for the reason `LABELLED_RELATION` is one: nothing
# reads it except the checks and the COPY, and materializing would hold a second copy of
# the reduction's output for that. It IS evaluated twice, once per reader, which is the
# price of not holding it; the reduction runs over one row per published genome, so that
# trade is the opposite of the tree's, where the relation is the whole reference.
TAXONOMY_SIDECAR_RELATION = "feature_taxonomy"

# What the sidecar carries: the SAME `feature_id` the table's rows are named with, and
# the eight ranks. One file joinable to the other on one column, which is the whole
# reason both are labelled from one mint.
#
# No `genome_idx`, and no lineage string. The string form would be lossy in a way the
# columns are not — see `qiita_common.taxonomy.genome_lineage_select_sql`.
# Names only, unlike `TREE_SCHEMA` and `LABELLED_SCHEMA`: those carry types because a
# builder writes `CAST(NULL AS …)` from them for an empty result. The sidecar has no such
# path — it is as wide as the table, and an empty table's sidecar is an empty COPY of a
# relation that already has the right types.
TAXONOMY_SIDECAR_COLUMNS = ("feature_id", *RANK_COLUMNS)


def taxonomy_table_sql(source: str) -> str:
    """Materialize the streamed per-feature taxonomy into `TAXONOMY_TABLE`.

    `source` is the caller's stream relation. Projected to `feature_idx` plus the eight
    ranks: the stream also carries `reference_idx` and `ncbi_taxon_id` (always NULL
    today), and holding a whole reference's worth of columns nothing reads is the kind
    of cost that only shows up at GG2 scale.
    """
    return (
        f"CREATE TABLE {TAXONOMY_TABLE} AS SELECT feature_idx, {rank_columns_sql()} FROM {source}"
    )


def taxonomy_sidecar_sql() -> str:
    """Define `TAXONOMY_SIDECAR_RELATION`: one row per PUBLISHED row of the table, named
    the same way, carrying its genome's ranks with the source prefixes restored.

    Scoped to `GENOME_LABEL_TABLE` on both sides, which is what makes the sidecar exactly
    as wide as the table — see `published_membership_sql` for the member set and why the
    restriction matters.
    """
    reduction = genome_representative_taxonomy_select_sql(
        member_genome=f"({published_membership_sql()})", taxonomy=TAXONOMY_TABLE
    )
    return (
        f"CREATE VIEW {TAXONOMY_SIDECAR_RELATION} AS "
        f"SELECT g.feature_id, {prefixed_rank_columns_sql(alias='r')} "
        f"FROM ({reduction}) r "
        f"JOIN {GENOME_LABEL_TABLE} g ON g.genome_idx = r.genome_idx"
    )


@dataclass(frozen=True)
class TaxonomyClearance:
    """Evidence that `check_taxonomy_diagnostics` ran and passed, plus the sidecar's row
    count so a caller can report the size without counting it again.

    `taxonomy_copy_sql` takes one for the same reason `labelled_relation_sql` takes a
    `LabelClearance`: a sidecar that does not line up with the table it accompanies is
    a file people will join anyway.
    """

    rows: int


def taxonomy_diagnostics_sql() -> str:
    """One row for `check_taxonomy_diagnostics`, over the sidecar and the label set it
    must match."""
    return (
        f"SELECT (SELECT count(*) FROM {GENOME_LABEL_TABLE}) AS published_rows, "
        f"(SELECT count(*) - count(DISTINCT feature_idx) FROM {TAXONOMY_TABLE}) "
        f"    AS repeated_features, "
        f"count(*) AS taxonomy_rows, "
        f"count(DISTINCT feature_id) AS taxonomy_feature_ids, "
        f"count(*) FILTER (WHERE feature_id IS NULL) AS unnamed_rows "
        f"FROM {TAXONOMY_SIDECAR_RELATION}"
    )


def check_taxonomy_diagnostics(
    *,
    published_rows: int,
    repeated_features: int,
    taxonomy_rows: int,
    taxonomy_feature_ids: int,
    unnamed_rows: int,
) -> TaxonomyClearance:
    """Refuse a sidecar that does not describe the table beside it, and return the
    clearance `taxonomy_copy_sql` requires.

    Every fault here is silent in the file: a short sidecar reads as "those rows are
    unclassified", a duplicated one double-counts under any join, and a NULL name joins
    to nothing. An unclassified genome is NOT one of these — it is present with NULL
    ranks, which is a different statement and a legitimate one.
    """
    if repeated_features:
        # Checked on the STREAMED taxonomy rather than on the sidecar, because the
        # reduction resolves a repeat to one row and so cannot show it downstream — see
        # `genome_representative_taxonomy_select_sql`. Two rows for one feature need not
        # agree, and arbitrating between two lineages is not ours to do quietly.
        raise ValueError(
            f"this reference's taxonomy holds more than one row for {repeated_features} "
            f"feature(s), which ingest writes one-to-one, so the reference is malformed. "
            f"Two rows for one feature can disagree, and choosing between them silently "
            f"would publish an arbitrary lineage."
        )
    if unnamed_rows:
        raise ValueError(
            f"{unnamed_rows} taxonomy rows carry no feature_id, so nothing could join "
            f"them to the table. The sidecar is named from the same label relation the "
            f"table is, so this means that relation gained a NULL handle."
        )
    if taxonomy_rows != published_rows:
        raise ValueError(
            f"the taxonomy sidecar describes {taxonomy_rows} rows but the table "
            f"publishes {published_rows}. A sidecar shorter than its table reads as "
            f"though the missing rows were unclassified — an unclassified genome is "
            f"present here with NULL ranks — and a longer one describes rows nobody "
            f"can find."
        )
    if taxonomy_feature_ids != taxonomy_rows:
        raise ValueError(
            f"{taxonomy_rows} taxonomy rows carry only {taxonomy_feature_ids} distinct "
            f"feature_ids, so joining the sidecar to the table would multiply the rows "
            f"it duplicates. Each published row has exactly one representative member, "
            f"so a repeat means the reference's taxonomy holds more than one row for "
            f"one feature."
        )
    return TaxonomyClearance(rows=taxonomy_rows)


def taxonomy_copy_sql(path: Path, *, clearance: TaxonomyClearance) -> str:
    """COPY the sidecar to Parquet, with the options every qiita Parquet artifact
    shares. Parquet and not TSV: the ranks are eight nullable strings, and a TSV cannot
    tell an empty rank from an absent one without a convention every reader has to be
    told about.

    **Takes a `TaxonomyClearance`**, so it cannot be reached without having run
    `check_taxonomy_diagnostics`; see that dataclass.
    """
    _ = clearance
    # Quoted, because two of the eight ranks are SQL keywords. The Parquet column names
    # are the unquoted ones — `TAXONOMY_SIDECAR_COLUMNS` — since quoting is how the
    # identifier is written, not part of it.
    projection = ", ".join(("feature_id", *QUOTED_RANK_COLUMNS))
    return (
        f"COPY (SELECT {projection} FROM {TAXONOMY_SIDECAR_RELATION}) "
        f"TO '{validate_parquet_path(path)}' ({PARQUET_OPTS})"
    )


# ---------------------------------------------------------------------------
# What the roll-up leaves behind
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RollupCoverage:
    """How much of the streamed alignment the roll-up could carry to genome level.

    Not a refusal. `ogu_input_table_sql`'s INNER JOIN to the map drops alignments to
    features with no genome, and for some references that is most of them — a 16S
    record is not an OGU and there is no genome-rooted row to emit for it. The count is
    reported because the alternative is a table that is quietly a fraction of the data
    the caller streamed, and nothing else in the recipe would ever mention it.
    """

    alignment_rows: int
    unmapped_rows: int
    unmapped_features: int

    @property
    def complete(self) -> bool:
        return self.unmapped_rows == 0


def rollup_coverage_diagnostics_sql() -> str:
    """One row for `RollupCoverage`: how many staged alignment rows have no genome to
    roll up to.

    **Grouped to features before joining the map**, which is the difference between
    touching the largest relation in this recipe once and touching it three times. The
    obvious form — join the slice to the map row by row — reads the slice for its own
    `count(*)`, reads it again for the join, and then needs a `count(DISTINCT feature_idx)`
    on top; here the group-by has one entry per feature (a six-figure hash table against a
    slice that can run to nine figures), the map is deduplicated to the key it is probed
    on, and the distinct feature count falls out as `count(*)`.

    That shape also removes the fan-out the row-wise join has, rather than compensating
    for it: the map holds one row per `(feature, genome)` pair, so a feature belonging to
    several genomes multiplies its rows — which inflated only the denominator, and
    reported a share that was too low.
    """
    return (
        f"WITH per_feature AS ("
        f"SELECT feature_idx, count(*) AS rows FROM {ALIGNMENT_TABLE} GROUP BY feature_idx), "
        f"mapped AS (SELECT DISTINCT contig_id FROM {MAP_TABLE}) "
        f"SELECT coalesce(sum(f.rows), 0) AS alignment_rows, "
        f"coalesce(sum(f.rows) FILTER (WHERE m.contig_id IS NULL), 0) AS unmapped_rows, "
        f"count(*) FILTER (WHERE m.contig_id IS NULL) AS unmapped_features "
        f"FROM per_feature f LEFT JOIN mapped m ON m.contig_id = f.feature_idx"
    )


def rollup_coverage_warning(coverage: RollupCoverage) -> str:
    """The one wording for "your table does not cover all of what you streamed"."""
    share = 100.0 * coverage.unmapped_rows / coverage.alignment_rows
    return (
        f"note: {coverage.unmapped_rows} of {coverage.alignment_rows} alignment rows "
        f"({share:.1f}%) are to {coverage.unmapped_features} features with no genome in "
        f"this reference, so they cannot be rolled up and are not in this table. A "
        f"feature-rooted table is not built yet; until it is, this is the whole of what "
        f"a genome-keyed table can say about this alignment."
    )


# ---------------------------------------------------------------------------
# The sheared tree
# ---------------------------------------------------------------------------

# The reference's phylogeny as streamed from the lake. A real TABLE for two reasons:
# `shear_tree` resolves its arguments on its own connection, which cannot see a
# registered stream; and this is the largest relation in the recipe — GG2's backbone is
# ~660k nodes — so the shear's clearance drops it the moment the shear is done.
#
# Deliberately NOT named for the lake table it comes from, unlike `TAXONOMY_TABLE`: the
# lake's is `reference_phylogeny` exactly, and one name meaning both a caller's ticket
# argument and a local relation is a trap.
PHYLOGENY_TABLE = "phylogeny_nodes"

# The columns of the stream we read. `name` matters for INTERNAL nodes, which keep
# theirs; `feature_idx` is what resolves a tip to the genome whose row the table
# published. `reference_idx` is not read — the ticket is already scoped to one reference,
# so it is a whole tree's worth of a constant.
PHYLOGENY_COLUMNS = (
    "node_index",
    "parent_index",
    "name",
    "branch_length",
    "edge_id",
    "is_tip",
    "feature_idx",
)

# The shear's two arguments, both VIEWs: `shear_tree` takes relation NAMES and resolves
# them on its own connection, where a VIEW is as visible as a TABLE, so materializing
# either would copy the biggest relation here for nothing.
SHEAR_INPUT_RELATION = "phylogeny_published_names"
SHEAR_KEEP_SET_RELATION = "phylogeny_keep_set"

# The reference's curated blocklist, resolved to feature_idx — the exclusion set the
# data plane's own contract names for a tree consumer, `tips WHERE feature_idx NOT IN
# reference_exclusion` (`qiita-data-plane/src/ducklake.rs`, `ensure_exclusion_tables`).
#
# **The tree is the one artifact here that has to apply it itself.** The alignment and
# the taxonomy arrive through exclusion-aware views, so a blocked feature is already gone
# from the table and the sidecar; the phylogeny stream deliberately has no such view,
# because anti-joining a tip's row would orphan its internal parents. So a blocked contig
# reaches this recipe with its tip intact, and nothing else would drop it.
BLOCKED_FEATURE_TABLE = "blocked_feature"

# The sheared tree, materialized — see `sheared_tree_table_sql`.
TREE_TABLE = "sheared_tree"

# What the published tree carries: `shear_tree`'s own output columns, in its order and
# with the types it returns (measured — see `docs/duckdb-miint.md`).
# `node_index`/`parent_index` are the SHEAR's 0-based reindexing rather than anything of
# ours, and they are how a tree expresses its shape; `edge_id` is the reference's own
# jplace edge id, the only handle back to its placements. No `feature_idx` — a tip is
# named with the handle its row in the table carries.
#
# The types are written out only because the empty path below has to produce them without
# the shear; the populated path takes them from `shear_tree`.
TREE_SCHEMA = {
    "node_index": "BIGINT",
    "name": "VARCHAR",
    "branch_length": "DOUBLE",
    "edge_id": "BIGINT",
    "parent_index": "BIGINT",
    "is_tip": "BOOLEAN",
}
TREE_COLUMNS = tuple(TREE_SCHEMA)


def phylogeny_table_sql(source: str) -> str:
    """Materialize the streamed phylogeny into `PHYLOGENY_TABLE`.

    **A tip's own name is dropped on the way in.** Nothing downstream reads it — the
    shear renames every tip from the mint, and an unpublished one is left nameless — and
    on a reference the size of GG2 the tip labels ARE the tree: half its ~660k nodes, and
    the bulk of 407 MB of Newick, held for the length of the run for nothing. It also
    makes the promise that no reference-internal FASTA header can reach a published file
    structural rather than a property of one CASE expression downstream.
    """
    projection = ", ".join(
        "CASE WHEN is_tip THEN NULL ELSE name END AS name" if column == "name" else column
        for column in PHYLOGENY_COLUMNS
    )
    return f"CREATE TABLE {PHYLOGENY_TABLE} AS SELECT {projection} FROM {source}"


def blocked_feature_table_sql(source: str) -> str:
    """Stage the reference's blocked features into `BLOCKED_FEATURE_TABLE` — see that
    constant for why the tree needs them and nothing else here does.

    DISTINCT because this relation is joined to, and a repeat would fan a tip out. The
    route answers one row per blocked feature today; a lookup table that quietly
    multiplies its caller's rows is not a property worth depending on.
    """
    return f"CREATE TABLE {BLOCKED_FEATURE_TABLE} AS SELECT DISTINCT feature_idx FROM {source}"


def shear_input_statements() -> tuple[str, ...]:
    """Define the shear's two arguments: the tree with its tips renamed to the handles
    the table publishes, and the keep-set of those handles.

    **The tree is renamed rather than the shear's output translated afterwards**, which
    is what makes one vocabulary structural: the shear matches by name, so a keep-set of
    published handles and a tree that speaks them cannot disagree about which tip is
    which, and the sheared output needs no second pass to name.

    A tip whose feature has no published genome gets a NULL name — measured: `shear_tree`
    ignores NULL-named tips, so it is sheared away like any tip outside the keep-set.
    Keeping its own name instead would put a reference-internal FASTA header into a
    published file, and could collide with a handle.

    **A tip whose feature is BLOCKED gets a NULL name for the same reason**, which is how
    this recipe honours the exclusion contract the phylogeny defers to — see
    `BLOCKED_FEATURE_TABLE`. Naming it, not dropping its row: the count has to stay equal
    to the tree's, since a difference is what reports a tip belonging to two genomes. It
    also means a genome with a second, unblocked tip publishes THAT one rather than being
    refused as ambiguous; a genome left with none is caught by `check_tree_diagnostics`,
    which is the honest outcome — the tree has no position for it that a curator accepts.

    **The membership it renames from is the PUBLISHED one** (`published_membership_sql`),
    not the whole reference's map: joining the map unrestricted fans a tip out once per
    genome the feature belongs to, published or not, so a tip would be refused as ambiguous
    on the strength of a genome this table never mentions.

    The keep-set is `GENOME_LABEL_TABLE` itself, so the tips asked for are exactly the
    rows the table publishes.
    """
    published_names = (
        f"CREATE VIEW {SHEAR_INPUT_RELATION} AS "
        f"SELECT p.node_index, p.parent_index, p.branch_length, p.edge_id, p.is_tip, "
        f"CASE WHEN NOT p.is_tip THEN p.name "
        f"     WHEN b.feature_idx IS NULL THEN pub.feature_id END AS name "
        f"FROM {PHYLOGENY_TABLE} p "
        f"LEFT JOIN ({published_membership_sql()}) pub ON pub.feature_idx = p.feature_idx "
        f"LEFT JOIN {BLOCKED_FEATURE_TABLE} b ON b.feature_idx = p.feature_idx"
    )
    keep_set = (
        f"CREATE VIEW {SHEAR_KEEP_SET_RELATION} AS "
        f"SELECT feature_id AS name FROM {GENOME_LABEL_TABLE}"
    )
    return (published_names, keep_set)


@dataclass(frozen=True)
class TreeClearance:
    """Evidence that `check_tree_diagnostics` ran and passed, plus the number of tips the
    sheared tree will carry — which is the published row count, so a caller can report
    the size without counting it again.

    Takes the same role `GateClearance` does: every fault the checks catch produces a
    tree somebody would join to the table anyway.
    """

    tips: int

    @property
    def statements(self) -> tuple[str, ...]:
        """The rest of the protocol in order: shear, then release the whole-reference
        tree and the two views over it.

        Iterating this is what keeps the release from being forgotten — the staged tree
        is the largest relation in the recipe and nothing reads it after the shear. The
        views go first because they read the table.
        """
        return (sheared_tree_table_sql(clearance=self), *drop_phylogeny_statements())


def sheared_tree_table_sql(*, clearance: TreeClearance) -> str:
    """Shear the tree down to the published keep-set, into `TREE_TABLE`.

    `collapse := true` is miint's default and is passed explicitly because it is
    load-bearing: it removes the single-child ancestors pruning leaves behind and **sums
    their branch lengths onto the surviving edge**, so a tip-to-tip distance in the
    sheared tree is that distance in the whole one.

    `ignore_missing := false` is a backstop, not the guard — `check_tree_diagnostics` has
    already refused a published row the tree has no tip for, and with a count where the
    shear's own error lists every missing name. Left false so a disagreement between what
    was checked and what is sheared fails loudly instead of quietly shearing to a subset.

    **A cleared keep-set of zero tips short-circuits**, the way `ogu_output_table_sql`
    does for an empty analytic and for the same reason: publishing no rows is a legitimate
    result here — every genome dropped by the coverage threshold — and the table written
    beside this is a real, empty file rather than an error. `shear_tree` cannot express it
    (a tree cannot be sheared to nothing, so it raises), so the empty tree is built
    directly. One relation name either way, so the writer cannot tell the difference.

    Materialized, so the whole-reference tree can be dropped before the write. Takes a
    `TreeClearance` for the reason `gated_alignment_table_sql` takes a `GateClearance`.
    """
    if not clearance.tips:
        casts = ", ".join(
            f"CAST(NULL AS {sql_type}) AS {name}" for name, sql_type in TREE_SCHEMA.items()
        )
        return f"CREATE TABLE {TREE_TABLE} AS SELECT {casts} WHERE false"
    return (
        f"CREATE TABLE {TREE_TABLE} AS SELECT {', '.join(TREE_COLUMNS)} "
        f"FROM shear_tree('{SHEAR_INPUT_RELATION}', '{SHEAR_KEEP_SET_RELATION}', "
        f"collapse := true, ignore_missing := false)"
    )


def drop_phylogeny_statements() -> tuple[str, ...]:
    """Release the whole-reference tree and the two views over it, once the shear has
    materialized its result. Same reason `drop_streamed_alignment_table_sql` exists: on a
    client machine this is the peak, and holding a reference's whole tree through the
    write buys nothing."""
    return (
        f"DROP VIEW {SHEAR_INPUT_RELATION}",
        f"DROP VIEW {SHEAR_KEEP_SET_RELATION}",
        f"DROP TABLE {PHYLOGENY_TABLE}",
        f"DROP TABLE {BLOCKED_FEATURE_TABLE}",
    )


def tree_diagnostics_sql() -> str:
    """One row for `check_tree_diagnostics`, measured over the two relations the shear
    itself reads rather than over a second copy of their join — so what was checked is
    what gets sheared."""
    return (
        f"WITH tip AS ("
        f"SELECT node_index, name FROM {SHEAR_INPUT_RELATION} WHERE is_tip), "
        # The handles whose tip the rename dropped as blocked. Read off the STAGED tree
        # rather than the view, because the view is where the name was taken away — by
        # the time a row reaches `tip` there is nothing left to say it was ever blocked.
        f"blocked_handle AS ("
        f"SELECT DISTINCT pub.feature_id AS name FROM {PHYLOGENY_TABLE} p "
        f"JOIN ({published_membership_sql()}) pub ON pub.feature_idx = p.feature_idx "
        f"JOIN {BLOCKED_FEATURE_TABLE} b ON b.feature_idx = p.feature_idx "
        f"WHERE p.is_tip), "
        f"per_handle AS ("
        f"SELECT k.name, count(t.node_index) AS tips, "
        f"k.name IN (SELECT name FROM blocked_handle) AS had_blocked_tip "
        f"FROM {SHEAR_KEEP_SET_RELATION} k LEFT JOIN tip t ON t.name = k.name "
        f"GROUP BY k.name) "
        f"SELECT (SELECT count(*) FROM {PHYLOGENY_TABLE}) AS tree_nodes, "
        f"(SELECT count(*) FROM {SHEAR_INPUT_RELATION}) AS shear_nodes, "
        f"count(*) AS published_rows, "
        f"count(*) FILTER (WHERE tips = 0) AS rows_with_no_tip, "
        f"count(*) FILTER (WHERE tips > 1) AS rows_with_many_tips, "
        f"count(*) FILTER (WHERE tips = 0 AND had_blocked_tip) AS rows_with_blocked_tip, "
        f"min(name) FILTER (WHERE tips = 0) AS untreed_example, "
        f"min(name) FILTER (WHERE tips > 1) AS multi_tip_example, "
        f"min(name) FILTER (WHERE tips = 0 AND had_blocked_tip) AS blocked_tip_example "
        f"FROM per_handle"
    )


def check_tree_diagnostics(
    *,
    tree_nodes: int,
    shear_nodes: int,
    published_rows: int,
    rows_with_no_tip: int,
    rows_with_many_tips: int,
    rows_with_blocked_tip: int,
    untreed_example: str | None,
    multi_tip_example: str | None,
    blocked_tip_example: str | None,
) -> TreeClearance:
    """Refuse a tree that cannot honestly be published beside this table, and return the
    clearance `sheared_tree_table_sql` requires.

    The shear catches two of these itself, and it is the *message* that differs: its
    errors name our staged relation and, for missing tips, list every name — unusable at
    the size of a real published set. The third it does not catch at all.
    """
    if not tree_nodes:
        raise ValueError(
            "this reference has no phylogeny, so there is no tree to shear. The table "
            "and the taxonomy sidecar do not depend on one — re-run without --tree."
        )
    if shear_nodes != tree_nodes:
        # Renaming tips by genome multiplies a node whose feature belongs to more than
        # one genome, which `feature_genome` allows on purpose (identical bytes share one
        # feature_idx, so a plasmid two organisms carry resolves to one feature under
        # both). The shear rejects the result — `Duplicate node_id: N` — so this refusal
        # buys the reason rather than the failure.
        raise ValueError(
            f"the reference's tree has {tree_nodes} nodes but naming its tips by genome "
            f"produces {shear_nodes}, so {shear_nodes - tree_nodes} tip(s) belong to more "
            f"than one genome — a feature two genomes share cannot be one genome-named "
            f"tip. A genome-keyed tree is not publishable for this reference; the table "
            f"and the taxonomy sidecar are unaffected, so re-run without --tree."
        )
    if rows_with_many_tips:
        raise ValueError(
            f"{rows_with_many_tips} published row(s) own more than one tip in this "
            f"reference's tree, {multi_tip_example} among them, so the tree is not "
            f"genome-level for this reference. The shear would keep BOTH tips under the "
            f"one handle, giving a tree with duplicate tip names."
        )
    if rows_with_blocked_tip:
        # Before the untreed refusal below, which counts these rows too: both describe
        # the same genome, and only this one says why the tip went away.
        raise ValueError(
            f"{rows_with_blocked_tip} published row(s) have no tip in this reference's "
            f"tree that a curator accepts, {blocked_tip_example} among them: every tip "
            f"they own is on a blocked feature. The genome still publishes because "
            f"another of its contigs aligned, but its only position in the tree comes "
            f"from sequence the blocklist rejects, so the tree cannot honestly carry it. "
            f"The table and the taxonomy sidecar are unaffected — re-run without --tree."
        )
    if rows_with_no_tip:
        raise ValueError(
            f"{rows_with_no_tip} of {published_rows} published row(s) have no tip in this "
            f"reference's tree, {untreed_example} among them. A tree missing tips the "
            f"table publishes reads as though those rows were left out of the analysis."
        )
    return TreeClearance(tips=published_rows)


def tree_copy_sql(path: Path, *, clearance: TreeClearance) -> str:
    """COPY the sheared tree to Parquet, with the options every qiita Parquet artifact
    shares.

    **Parquet, not Newick.** We ship the node table and let a consumer that wants Newick
    convert it — which also keeps `COPY … (FORMAT NEWICK)`'s edge-id default from being
    ours to dodge (it annotates every branch whenever an `edge_id` column is present; see
    `docs/duckdb-miint.md`). `edge_id` is worth carrying: the shear preserves the
    surviving edge's original id, which is the handle back to the reference's placements.

    **A consumer joining this to the table must filter `is_tip`.** Only tips are named from
    the mint; a surviving internal node keeps the reference's own Newick label, and nothing
    makes those labels disjoint from published handles — so an unfiltered
    `name = feature_id` join can match an internal node. (The opposite direction is closed:
    an unpublished tip is nameless, see `shear_input_statements`.)

    **Takes a `TreeClearance`**, so it cannot be reached without the check; see that
    dataclass.
    """
    _ = clearance
    return f"COPY {TREE_TABLE} TO '{validate_parquet_path(path)}' ({PARQUET_OPTS})"


def parquet_copy_sql(path: Path) -> str:
    """COPY the relabelled table to Parquet, with the canonical options every qiita
    Parquet artifact shares.

    `PARQUET_OPTS`' `ROW_GROUP_SIZE_BYTES` requires `SET preserve_insertion_order =
    false` on the writing connection — DuckDB errors at bind time otherwise — which is
    the caller's to set (see `parquet.py`).
    """
    return f"COPY {LABELLED_RELATION} TO '{validate_parquet_path(path)}' ({PARQUET_OPTS})"


def biom_copy_sql(path: Path) -> str:
    """COPY the relabelled table to a BIOM 2.1 (HDF5) file.

    The writer requires exactly `LABELLED_SCHEMA` — `feature_id`/`sample_id` VARCHAR
    and `value` DOUBLE, looked up BY NAME — and **silently ignores any other column**,
    so it is `labelled_relation_sql`'s projection, not this writer, that keeps our
    identifiers out of the file. Behaviours it does enforce, and two it applies
    without asking, are recorded in `docs/duckdb-miint.md` and pinned by the
    control-plane's BIOM contract test; the one that shapes callers most is that it
    **refuses to overwrite an existing file**, unlike the Parquet COPY.

    `COMPRESSION` is passed explicitly even though gzip is also the writer's default,
    so a published artifact's encoding does not change under us if that default does.
    `ID` is left alone: the only distinctive handle for this table today is an
    internal identifier, which must not ride a published file.
    """
    return (
        f"COPY {LABELLED_RELATION} TO '{validate_parquet_path(path)}' "
        f"(FORMAT BIOM, COMPRESSION 'gzip', GENERATED_BY '{BIOM_GENERATED_BY}')"
    )
