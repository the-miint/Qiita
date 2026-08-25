"""The alignment gate: what an alignment must clear to be counted.

An alignment row that fails the gate is not a placement, so the gate is applied to
the staged slice itself — before the coverage calculation and before woltka — rather
than to any one consumer of it.

A gate scores on one of two axes: the CIGAR of a record (or of a placement's two mates
pooled), or a whole read against one reference through miint's
[`circular_query_coverage`](https://the-miint.github.io/duckdb-miint/alignment_analysis/#circular-query-coverage).
`AlignmentGate` carries both axes' thresholds and `__post_init__` is where the choice
between them is enforced.
"""

from __future__ import annotations

from dataclasses import dataclass

from .relations import (
    ALIGNMENT_TABLE,
    CIRCULAR_ALIGNMENTS_VIEW,
    FEATURE_LENGTHS_TABLE,
    FEATURE_TOPOLOGY_VIEW,
    STREAMED_ALIGNMENT_TABLE,
    drop_circular_inputs_statements,
    drop_streamed_alignment_table_sql,
)
from .stage import ALIGNMENT_COLUMNS

# The circular gate's thresholds, as defaults. Named here so the CLI's flag defaults and
# the dataclass's are one value, and so a caller reading the SQL finds no literal in it:
# both ride as bound parameters.
CIRCULAR_MIN_COVERAGE = 0.90
CIRCULAR_MIN_IDENTITY = 0.95

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

# One READ against one reference, as a GROUP BY key over alignment rows — the grouping
# `circular_query_coverage` reports on, in our column names. The diagnostics group by it
# so what they measure is what the gate will judge; `_circular_gated_sql` joins on the
# same three expressions.
CIRCULAR_READ_PARTITION = "sequence_idx, alignment_is_read1(flags), feature_idx"


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

    `circular` pools every record one read was split into against one reference and
    judges the read there, keeping every row of a group that clears
    `circular_min_coverage`, `circular_min_identity` and same-strandedness. A read
    crossing the origin of a circular reference held as a linearised contig arrives as
    two records covering half the read each, so a per-row coverage floor discards what
    pooled coverage scores 1.0. It replaces both CIGAR thresholds and the `paired`
    pooling rather than combining with them — `__post_init__` says why.

    The circular thresholds carry defaults where the CIGAR ones do not: a CIGAR gate with
    neither threshold filters nothing and is refused, while `circular` names a grouping,
    so it takes `CIRCULAR_MIN_COVERAGE` / `CIRCULAR_MIN_IDENTITY` and a caller moves
    either. `circular_min_identity` is optional because a legacy-`M` alignment has no
    poolable identity at all; `check_gate_diagnostics` refuses that slice and its message
    is the one copy of why.
    """

    min_identity: float | None = None
    min_query_coverage: float | None = None
    paired: bool = False
    circular: bool = False
    circular_min_coverage: float = CIRCULAR_MIN_COVERAGE
    circular_min_identity: float | None = CIRCULAR_MIN_IDENTITY

    def __post_init__(self) -> None:
        if self.circular:
            if self.min_identity is not None or self.min_query_coverage is not None:
                raise ValueError(
                    "AlignmentGate cannot score both axes: min_identity / "
                    "min_query_coverage judge one record's own CIGAR, and a "
                    "query-coverage floor discards exactly the split records a circular "
                    "gate pools — the halves of an origin-spanning read cover half of it "
                    "each. Use circular_min_coverage / circular_min_identity instead."
                )
            if self.paired:
                raise ValueError(
                    "AlignmentGate cannot pool on both axes: `paired` groups a "
                    "placement's two mates, `circular` groups one read's fragments "
                    "against one reference, and `circular_query_coverage` keeps mates "
                    "apart itself. Pass paired=False."
                )
        elif self.min_identity is None and self.min_query_coverage is None:
            raise ValueError(
                "AlignmentGate needs at least one threshold (min_identity or "
                "min_query_coverage): a gate with neither filters nothing while still "
                "costing the wide `cigar` column on the wire, so it is a mistake "
                "rather than a no-op. Pass no gate at all instead."
            )
        for name, value in (
            ("min_identity", self.min_identity),
            ("min_query_coverage", self.min_query_coverage),
            ("circular_min_coverage", self.circular_min_coverage),
            ("circular_min_identity", self.circular_min_identity),
        ):
            if value is not None and not 0.0 <= value <= 1.0:
                raise ValueError(
                    f"AlignmentGate.{name} must be a proportion in [0, 1], got {value!r}"
                )

    @property
    def scores_identity(self) -> bool:
        """Whether this gate reads a sequence identity at all — the CIGAR one per record
        or the pooled one per read. What decides whether the diagnostics count scorable
        rows, and whether an all-legacy-`M` slice is refused."""
        return (
            self.circular_min_identity is not None
            if self.circular
            else self.min_identity is not None
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
        """The remaining `(sql, parameters)` pairs, in the order they must run: define
        whatever the gate reads besides the slice, apply the gate, then release what the
        gated path put up — the streamed copy that holds `cigar`, and for a circular gate
        the two rename views and the per-feature lengths staged for them.

        Iterating this is the whole rest of the protocol, so a caller cannot bind the
        wrong parameters to the predicate or forget the cleanup. The circular gate's two
        views live here rather than with the caller for the same reason: the first reads
        the streamed slice, so it cannot be defined before the stream is drained, and
        defining it is not a decision the caller has to make.
        """
        setup: tuple[tuple[str, list[float]], ...] = (
            ((circular_alignments_view_sql(), []), (feature_topology_view_sql(), []))
            if self.gate.circular
            else ()
        )
        gated = (gated_alignment_table_sql(clearance=self), gate_parameters(self.gate))
        circular_drops = (
            tuple((sql, []) for sql in drop_circular_inputs_statements())
            if self.gate.circular
            else ()
        )
        return (*setup, gated, *circular_drops, (drop_streamed_alignment_table_sql(), []))


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


def _circular_terms(gate: AlignmentGate) -> list[tuple[str, float]]:
    """The circular gate's predicates over `circular_query_coverage`'s output columns,
    with their bound values, in ONE order — same device as `_gate_terms`.

    `mixed_strand` is not among them and is not a knob: fragments on opposite strands are
    an inverted repeat, a chimera or a misassembly rather than one molecule
    linearisation cut, so admitting them changes what the coverage number counts instead
    of relaxing a cutoff. It is spelled in the SQL alongside these terms.

    `max_ref_gap` is reported by the macro and not read here: it is what would separate a
    wrap from a chimera that reaches the same coverage, and on a reference small enough
    for a read to wrap more than once it cannot (duckdb-miint#240).
    """
    terms: list[tuple[str, float]] = [("coverage >= ?", gate.circular_min_coverage)]
    if gate.circular_min_identity is not None:
        terms.append(("identity >= ?", gate.circular_min_identity))
    return terms


def gate_parameters(gate: AlignmentGate) -> list[float]:
    """The values to bind to `gated_alignment_table_sql`'s placeholders, in order."""
    terms = _circular_terms(gate) if gate.circular else _gate_terms(gate, "cigar")
    return [value for _, value in terms]


def gate_alignment_columns(gate: AlignmentGate | None) -> tuple[str, ...]:
    """The projection to request from the alignment DoGet: `ALIGNMENT_COLUMNS`, plus
    whatever `gate` needs to read.

    `cigar` rides only when something scores it (see `ALIGNMENT_COLUMNS` for what
    that column costs) and `mate_position` only when the gate pools mates, since its
    sole use is keying `PAIRED_PLACEMENT_PARTITION`. Both are in the ticket's allowlist.
    """
    if gate is None:
        return ALIGNMENT_COLUMNS
    extra = ("cigar",) + (("mate_position",) if gate.paired else ())
    return ALIGNMENT_COLUMNS + extra


def circular_alignments_view_sql() -> str:
    """Define `CIRCULAR_ALIGNMENTS_VIEW`: the streamed slice under the column names
    `circular_query_coverage` reads (`read_id`, `reference`).

    Our `sequence_idx` is the read and our `feature_idx` is the reference it aligned to,
    so the rename is the whole of it — the same shape `map_table_sql` does for
    `genome_coverage`. A VIEW: the macro resolves relation names on this connection,
    where a view is as visible as a table, so materializing would copy the widest
    relation in the pipeline for one reader.
    """
    return (
        f"CREATE VIEW {CIRCULAR_ALIGNMENTS_VIEW} AS "
        f"SELECT sequence_idx AS read_id, feature_idx AS reference, "
        f"flags, position, stop_position, cigar "
        f"FROM {STREAMED_ALIGNMENT_TABLE}"
    )


def feature_topology_view_sql() -> str:
    """Define `FEATURE_TOPOLOGY_VIEW`: the `reference_lengths` argument
    `circular_query_coverage` requires, over `FEATURE_LENGTHS_TABLE`.

    Every feature is declared circular, and that claim does not reach the result. The
    macro rejects a NULL `is_circular` because only the caller knows a reference's
    topology, and it moves exactly one output column — `max_ref_gap`, read modulo the
    reference length on a circular reference and as a plain distance on a linear one.
    `coverage`, `identity` and `mixed_strand`, the three this gate reads, are computed
    identically either way. So the claim is the one under which the gap column would
    mean "wrap distance", and nothing else; qiita records real circularity only for
    assembled contigs (`assembly_membership.kind`), never for a reference's features.
    """
    return (
        f"CREATE VIEW {FEATURE_TOPOLOGY_VIEW} AS "
        f"SELECT feature_idx AS reference, sequence_length_bp AS length, "
        f"TRUE AS is_circular FROM {FEATURE_LENGTHS_TABLE}"
    )


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
    `clearance.statements`, which pairs this with its parameters and the release of
    the streamed copy in the order they must run.

    Projects exactly `ALIGNMENT_COLUMNS`, so `cigar` stops here instead of
    propagating into the coverage view and woltka's input. A real TABLE, not a view,
    so the CIGARs are parsed once rather than on every downstream scan.

    An unpaired gate filters per row with `WHERE`. A paired one pools each
    placement's mates and filters with `QUALIFY`, which applies after the window. A
    circular one judges the read against the reference and keeps the whole group, so it
    filters with a SEMI JOIN onto what cleared.
    """
    gate = clearance.gate
    if gate.circular:
        return _circular_gated_sql(clearance)
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


def circular_predicate_sql(*, clearance: GateClearance) -> str:
    """What a `(read, reference)` group must clear, over `circular_query_coverage`'s
    own output columns. Bind `gate_parameters(clearance.gate)` to its placeholders.

    Public because the gated path is not the only consumer: a job that PRODUCES
    alignments applies the same predicate to the same macro output, and then reports the
    groups that cleared with more of the macro's columns than a filter needs. One
    definition, so a threshold that reached only the consumer would not admit rows the
    producer had already dropped.

    Takes a `GateClearance` for the reason `gated_alignment_table_sql` does: two of
    this predicate's failure modes are silent, and `check_gate_diagnostics` is what
    refuses them.
    """
    predicate = " AND ".join(sql for sql, _ in _circular_terms(clearance.gate))
    return f"{predicate} AND NOT mixed_strand"


def circular_cleared_join(alignments: str, cleared: str) -> str:
    """The ON clause matching one alignment row to its `(read, reference)` group in
    `circular_query_coverage`'s output. `alignments` and `cleared` are table aliases.

    `CIRCULAR_READ_PARTITION`'s three expressions, against the macro's column names.
    Shared with the producer for the reason `circular_predicate_sql` is: a group key
    that matched on two of the three would pool R1 and R2 together on one side of the
    join and not the other.
    """
    return (
        f"{alignments}.sequence_idx = {cleared}.read_id "
        f"AND {alignments}.feature_idx = {cleared}.reference "
        f"AND alignment_is_read1({alignments}.flags) = {cleared}.is_read1"
    )


def _circular_gated_sql(clearance: GateClearance) -> str:
    """The circular arm of `gated_alignment_table_sql`: keep every record of each
    `(read, reference)` group that cleared the pooled predicate.

    The macro answers per group, not per record, so the filter is a SEMI JOIN back onto
    the slice: a fragment is kept because the read it belongs to cleared against that
    reference. The join key is `CIRCULAR_READ_PARTITION`'s three expressions.

    A CTE rather than a subquery in the join: the macro resolves its relation arguments
    through `query_table`, which takes only literals, so it cannot sit in a lateral
    position — a CTE is the form upstream names for this.
    """
    return (
        f"CREATE TABLE {ALIGNMENT_TABLE} AS "
        f"WITH cleared AS ("
        f"SELECT read_id, is_read1, reference FROM circular_query_coverage("
        f"{CIRCULAR_ALIGNMENTS_VIEW}, {FEATURE_TOPOLOGY_VIEW}) "
        f"WHERE {circular_predicate_sql(clearance=clearance)}"
        f") SELECT {', '.join(f'a.{column}' for column in ALIGNMENT_COLUMNS)} "
        f"FROM {STREAMED_ALIGNMENT_TABLE} a SEMI JOIN cleared c "
        f"ON {circular_cleared_join('a', 'c')}"
    )


def gate_diagnostics_sql(gate: AlignmentGate) -> str:
    """One row for `check_gate_diagnostics`, over `STREAMED_ALIGNMENT_TABLE`:
    `(total_rows, scorable_rows, unpoolable_partitions, unpoolable_rows,
    unscorable_groups, paired_rows)`.

    Each count is computed only when it could matter — parsing every CIGAR, and
    grouping every placement, are both real work over the slice that still holds
    `cigar`.

    **Each axis counts unscorable identity with the function its own gate applies.**
    The CIGAR axis scores a record, so it counts scorable RECORDS with
    `cigar_sequence_identity`; the circular axis scores a read, so it counts unscorable
    GROUPS with `cigar_pooled_identity` over the macro's own grouping key. Asking the
    per-record question about a pooled gate reports rows as scorable whose read the gate
    then drops.

    A paired gate takes ONE grouped pass, not a plain pass plus a grouped
    subquery: every count is additive over the placement partitions, so aggregating the
    groups again gives identical numbers for one read of the widest relation in the
    pipeline instead of two. The `coalesce`s hold the empty slice together —
    `sum()` over zero groups is NULL, which would turn `total_rows` into NULL and stop
    `check_gate_diagnostics`' `== 0` early return from firing.
    """
    scorable = "count(cigar_sequence_identity(cigar))" if gate.scores_identity else "NULL"
    # miint's own predicate over the SAM flag, not hand-rolled bit math: the
    # `alignment_is_*` family is what `docs/duckdb-miint.md` tells callers to use, and
    # it reads the same `flags` USMALLINT the lake stores. 0x1 is set on both mates
    # whether or not either mapped, so it answers "is this slice paired data?" even
    # when a mate's row never arrived.
    paired_rows = "count(*) FILTER (WHERE alignment_is_paired(flags))"
    if gate.circular:
        # The rows `circular_query_coverage` will not see. It excludes secondary and
        # unmapped records and records missing a coordinate, so each one would leave the
        # gated slice unless a sibling record of the same read carried its group — a
        # drop no threshold asked for. Counted here so the check can refuse instead.
        unpoolable_rows = (
            "count(*) FILTER (WHERE alignment_is_secondary(flags) "
            "OR alignment_is_unmapped(flags) "
            "OR position IS NULL OR stop_position IS NULL)"
        )
        if not gate.scores_identity:
            return (
                f"SELECT count(*) AS total_rows, NULL AS scorable_rows, "
                f"0 AS unpoolable_partitions, {unpoolable_rows} AS unpoolable_rows, "
                f"0 AS unscorable_groups, {paired_rows} AS paired_rows "
                f"FROM {STREAMED_ALIGNMENT_TABLE}"
            )
        # Grouped by what the macro groups by, so a group here is a read the gate will
        # judge as one. The per-row counts are additive over those groups, so the pass
        # that answers the identity question answers the rest too.
        return (
            f"WITH pooled AS (SELECT count(*) AS rows_in_group, "
            f"{unpoolable_rows} AS unpoolable, {paired_rows} AS paired, "
            f"cigar_pooled_identity(cigar) AS identity "
            f"FROM {STREAMED_ALIGNMENT_TABLE} "
            f"GROUP BY {CIRCULAR_READ_PARTITION}) "
            f"SELECT coalesce(sum(rows_in_group), 0) AS total_rows, "
            f"NULL AS scorable_rows, 0 AS unpoolable_partitions, "
            f"coalesce(sum(unpoolable), 0) AS unpoolable_rows, "
            f"count(*) FILTER (WHERE identity IS NULL) AS unscorable_groups, "
            f"coalesce(sum(paired), 0) AS paired_rows FROM pooled"
        )
    if not gate.paired:
        return (
            f"SELECT count(*) AS total_rows, {scorable} AS scorable_rows, "
            f"0 AS unpoolable_partitions, 0 AS unpoolable_rows, "
            f"0 AS unscorable_groups, {paired_rows} AS paired_rows "
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
        f"0 AS unpoolable_rows, 0 AS unscorable_groups, "
        f"coalesce(sum(paired), 0) AS paired_rows FROM placement"
    )


def check_gate_diagnostics(
    gate: AlignmentGate,
    *,
    total_rows: int,
    scorable_rows: int | None,
    unpoolable_partitions: int,
    unpoolable_rows: int,
    unscorable_groups: int,
    paired_rows: int,
) -> GateClearance:
    """Refuse every gate failure that would otherwise produce a plausible wrong
    answer instead of an error, and return the `GateClearance` that
    `gated_alignment_table_sql` requires. Takes `gate_diagnostics_sql(gate)`'s row.

    Both consumers must refuse identically, which is why the judgement lives here
    rather than in each of them.

    **What a partly-unscorable slice costs differs by axis, so the two treat it
    differently.** On the CIGAR axis an unscorable record fails its own predicate and
    affects no other record, so it is dropped rather than refused. On the circular axis
    one unscorable record makes its whole READ unscorable — pooled identity is NULL when
    a group mixes `M` with `=`/`X` — so it would take its scorable siblings with it, and
    a single such group is refused.
    """
    if total_rows == 0:
        # 0 of 0 unscorable is not evidence of anything, and an empty slice is a
        # legitimate result elsewhere in this analytic.
        return GateClearance(gate)

    if gate.circular:
        if unscorable_groups:
            raise ValueError(
                f"{unscorable_groups} read(s) in this slice have no poolable sequence "
                f"identity: `cigar_pooled_identity` needs CIGARs carrying `=`/`X` ops "
                f"(an 'eqx' CIGAR) and is NULL when a read's records have none, or mix "
                f"`M` with `=`/`X` across records. `NULL >= threshold` drops the row, so "
                f"this gate would discard each of those reads WHOLE — including the "
                f"records that did score — and return a table that looks like a real "
                f"result. Either re-align with eqx CIGARs, or drop the identity "
                f"threshold and gate on coverage and strand, which any CIGAR supports."
            )
        if unpoolable_rows:
            raise ValueError(
                f"{unpoolable_rows} of {total_rows} alignment rows cannot be pooled by "
                f"read: `circular_query_coverage` excludes secondary (FLAG 0x100) and "
                f"unmapped (0x4) records, and records missing a coordinate. Each one "
                f"would leave the gated slice without failing any threshold — and a "
                f"secondary record is how a read says it also placed elsewhere, so "
                f"dropping those changes every multi-mapped read's count. Gate on the "
                f"CIGAR instead (min_identity / min_query_coverage), which scores every "
                f"row it is given."
            )
        if paired_rows:
            raise ValueError(
                f"{paired_rows} of {total_rows} alignment rows are paired (SAM FLAG "
                f"0x1), but a circular gate pools a read's FRAGMENTS and keeps mates "
                f"apart — R1 and R2 are different molecules — so it judges a "
                f"placement's mates independently and orphans one when they disagree. "
                f"Pass `paired=True` without `circular` for paired data; the circular "
                f"axis is for the single-end long reads a reference's origin splits."
            )
        return GateClearance(gate)

    if gate.scores_identity:
        if scorable_rows is None:
            raise ValueError(
                "check_gate_diagnostics: scorable_rows is NULL while an identity "
                "threshold is set. The row must come from gate_diagnostics_sql(the same "
                "gate), which only emits NULL there when identity is not being gated."
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
