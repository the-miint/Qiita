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

**Breadth of coverage filters the table, at one of two scopes** (`CoverageScope`):
pooled over the whole cohort, or per `(sample, genome)`. Pooled breadth is always
≥ the best single sample's, since pooling unions intervals — so per-sample is
strictly the stricter filter, and a genome can survive pooled while every one of
its samples fails.

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

**A caller that PUBLISHES the table stages two more** — the label relations — and
relabels the counts through them, ending at `LABELLED_TABLE` (`LABELLED_SCHEMA`):
our `*_idx` keys are gone, and the public handles are VARCHAR, which is what makes
the result writable as BIOM at all. See the relabel section below, and the two
writers after it — `LABELLED_TABLE` is the only relation here they will copy.

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

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

# `validate_parquet_path` is named for its first caller but checks a COPY *target* —
# a path safe to interpolate into a SQL string literal, which cannot be bound — so it
# is what both writers below use, BIOM included.
from .parquet import PARQUET_OPTS, validate_parquet_path


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
    today" (duckdb-miint#217); when `genome_coverage_per_sample` lands (#220) this
    branch collapses to one call.

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

# SAM FLAG 0x1: "template having multiple segments in sequencing" — i.e. the read is
# part of a pair. Set on both mates regardless of whether either mapped, which is
# what makes it a reliable answer to "is this slice paired data?" even when a mate's
# row never arrived.
SAM_FLAG_PAIRED = 0x1


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
    plausible wrong answer, and the only way to obtain a clearance is to have checked.
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

    `cigar` rides only when something scores it — it is the wide column the signed
    projection exists to leave out — and `mate_position` only when the gate pools,
    since its sole use is keying the partition. Both are in the ticket's allowlist.
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
    terms = _gate_terms(gate, "cigar")
    clause = "WHERE"
    if gate.paired:
        pooled = f"string_agg(cigar, '') OVER (PARTITION BY {PAIRED_PLACEMENT_PARTITION})"
        terms = _gate_terms(gate, pooled)
        clause = "QUALIFY"
    predicate = " AND ".join(sql for sql, _ in terms)
    return (
        f"CREATE TABLE {ALIGNMENT_TABLE} AS "
        f"SELECT {', '.join(ALIGNMENT_COLUMNS)} FROM {STREAMED_ALIGNMENT_TABLE} "
        f"{clause} {predicate}"
    )


def drop_streamed_alignment_table_sql() -> str:
    """Release the streamed copy once the gate has been applied. It holds `cigar`,
    the wide column (see `docs/architecture.md` for what that costs), and nothing
    reads it again — on a client machine over a large cohort, keeping it roughly
    doubles the analytic's peak memory for no benefit."""
    return f"DROP TABLE {STREAMED_ALIGNMENT_TABLE}"


def gate_diagnostics_sql(gate: AlignmentGate) -> str:
    """One row for `check_gate_diagnostics`, over `STREAMED_ALIGNMENT_TABLE`:
    `(total_rows, scorable_rows, unpoolable_partitions, paired_rows)`.

    Each count is computed only when it could matter — parsing every CIGAR, and
    grouping every placement, are both real work over the whole slice.
    """
    scorable = "count(cigar_sequence_identity(cigar))" if gate.min_identity is not None else "NULL"
    unpoolable = "0"
    if gate.paired:
        # A partition that cannot be a single placement's mates, in the three ways it
        # can fail. `count(col)` skips NULLs, which is what makes each detectable:
        #   1. a row is present but carries no CIGAR   -> string_agg scores short;
        #   2. a row claims a mapped mate that is not in the slice at all -> the
        #      partition holds one row and looks complete, so (1) cannot see it;
        #   3. more than two rows share the key -> not one placement, so the
        #      concatenation spans unrelated alignments.
        unpoolable = (
            f"(SELECT count(*) FROM (SELECT 1 FROM {STREAMED_ALIGNMENT_TABLE} "
            f"GROUP BY {PAIRED_PLACEMENT_PARTITION} HAVING "
            f"count(*) <> count(cigar) "
            f"OR (count(mate_position) > 0 AND count(*) = 1) "
            f"OR count(*) > 2))"
        )
    return (
        f"SELECT count(*) AS total_rows, {scorable} AS scorable_rows, "
        f"{unpoolable} AS unpoolable_partitions, "
        f"count(*) FILTER (WHERE flags & {SAM_FLAG_PAIRED} <> 0) AS paired_rows "
        f"FROM {STREAMED_ALIGNMENT_TABLE}"
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
            f"{paired_rows} of {total_rows} alignment rows are paired (SAM FLAG "
            f"0x{SAM_FLAG_PAIRED:x}), but this gate scores each row on its own CIGAR. "
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
GENOME_LABEL_TABLE = "genome_label"
SAMPLE_LABEL_TABLE = "sample_label"
LABELLED_TABLE = "feature_table_labelled"

# What a PUBLISHED table carries, name -> SQL type. Neither id is one of ours:
# `sample_id` is the minted `export_id` and `feature_id` the genome's `source_id`,
# both of which mean something to somebody outside this system — an `*_idx` does
# not, and is not a handle we promise to keep.
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
    short-circuit for an empty `OGU_INPUT_TABLE` (woltka rejects an all-NULL
    `sample_id` source, so the caller cannot simply run it on nothing). Passing the
    flag rather than the SELECT is what puts BOTH results in the same relation, so
    an empty cohort travels the same relabel and the same writer as a populated one.

    Materialized, unlike the server-side job's straight COPY, because the client
    reads the counts twice — once to check the labels, once to apply them — and
    re-running woltka for the second read would double the cost of the analytic.
    """
    select = woltka_ogu_select_sql() if populated else empty_ogu_select_sql()
    return f"CREATE TABLE {OGU_OUTPUT_TABLE} AS {select}"


def genome_label_table_sql(source: str) -> str:
    """`genome_idx -> feature_id` from `source`, the genome map as the route serves
    it (`(feature_idx, genome_idx, source, source_id)`).

    **DISTINCT is load-bearing.** The map carries one row per (feature, genome)
    PAIR, so a genome with N contigs appears N times; joining it un-deduplicated
    multiplies that genome's every count by N. The result is a plausible table, not
    an error, which is why the collapse happens here rather than being left to
    whoever stages the map.

    `source` rides along in the map but not in this relation: it exists so a
    consumer can tell two same-`source_id` genomes apart, and the collision check
    below is what acts on that — a published table names the genome by `source_id`
    alone.
    """
    return (
        f"CREATE TABLE {GENOME_LABEL_TABLE} AS "
        f"SELECT DISTINCT genome_idx, source_id AS feature_id FROM {source}"
    )


def genome_map_relations_sql(source: str) -> tuple[str, ...]:
    """Both relations the genome map feeds, from ONE source, in creation order: the
    roll-up key (`MAP_TABLE`) and the public label (`GENOME_LABEL_TABLE`).

    The route serves them in a single response and they must not disagree about which
    genomes exist — a label set narrower than the roll-up set leaves counts with no
    public handle, which `check_relabel_diagnostics` then refuses. Staging both here
    makes them agree by construction instead. The two builders stay separately
    callable for the server-side job, which rolls up but never relabels.
    """
    return (map_table_sql(source), genome_label_table_sql(source))


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

    `labelled_table_sql` takes one of these for the same reason
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
    an error, and return the `LabelClearance` `labelled_table_sql` requires. Takes
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
            f"one key repeats every count for that key and inflates its value. "
            f"`{GENOME_LABEL_TABLE}` is DISTINCT over the genome map's per-contig "
            f"fan-out, so check the exported-identifier map for a repeated "
            f"prep_sample, or the genome map for a genome carrying two source_ids."
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
            f"The genome map has to cover every genome the counts mention — the usual "
            f"cause is a map fetched for a different reference than the alignment was "
            f"run against."
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
            f"source_ids, so relabelling merges genomes that are not the same "
            f"organism. `qiita.genome` is unique on the COMPOSITE (source, "
            f"source_id), so two sources can each use one source_id — and upstream "
            f"documents that a BIOM write SUMS duplicate (feature_id, sample_id) "
            f"pairs, so the merge would never surface. Restrict the reference to one "
            f"source, or relabel from a handle that is unique across the emitted "
            f"genomes."
        )
    if sample_ids < samples:
        raise ValueError(
            f"{samples} samples in this table share only {sample_ids} distinct "
            f"export_ids, so relabelling merges samples. An export_id is minted per "
            f"processed sample and cannot repeat, so the exported-identifier map this "
            f"was staged from did not come from the mint route."
        )

    return LabelClearance(rows=labelled_rows)


def labelled_table_sql(*, clearance: LabelClearance) -> str:
    """Relabel the counts into `LABELLED_TABLE`: `LABELLED_COLUMNS` and nothing else.

    The projection is the enforcement. Both `*_idx` columns are joined ON and then
    dropped, so no writer downstream can read one out of this relation even by
    accident — which is the property that keeps our internal identifiers out of a
    file somebody publishes.

    **Takes a `LabelClearance`**, so it is unreachable without having run
    `check_relabel_diagnostics`; see `LabelClearance`.
    """
    return (
        f"CREATE TABLE {LABELLED_TABLE} AS "
        f"SELECT {', '.join(LABELLED_COLUMNS)} FROM ({_labelled_select_sql()})"
    )


# ---------------------------------------------------------------------------
# Writing the relabelled table out
# ---------------------------------------------------------------------------

# BIOM's `generated-by` attribute. The writer's own default is `miint`, which names
# the library rather than the system that produced the file. The system and no
# version, deliberately: a version here would have to be kept honest against four
# components and a pinned extension build, and nothing reads it. **So the file
# carries no provenance beyond this name today** — reproducing a table needs the
# reference, the cohort, the coverage scope and threshold, and any gate, none of
# which the bundle records yet.
BIOM_GENERATED_BY = "qiita"


def parquet_copy_sql(path: Path) -> str:
    """COPY the relabelled table to Parquet, with the canonical options every qiita
    Parquet artifact shares.

    `PARQUET_OPTS`' `ROW_GROUP_SIZE_BYTES` requires `SET preserve_insertion_order =
    false` on the writing connection — DuckDB errors at bind time otherwise — which is
    the caller's to set (see `parquet.py`).
    """
    return f"COPY {LABELLED_TABLE} TO '{validate_parquet_path(path)}' ({PARQUET_OPTS})"


def biom_copy_sql(path: Path) -> str:
    """COPY the relabelled table to a BIOM 2.1 (HDF5) file.

    The writer requires exactly `LABELLED_SCHEMA` — `feature_id`/`sample_id` VARCHAR
    and `value` DOUBLE, looked up BY NAME — and **silently ignores any other column**,
    so it is `labelled_table_sql`'s projection, not this writer, that keeps our
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
        f"COPY {LABELLED_TABLE} TO '{validate_parquet_path(path)}' "
        f"(FORMAT BIOM, COMPRESSION 'gzip', GENERATED_BY '{BIOM_GENERATED_BY}')"
    )
