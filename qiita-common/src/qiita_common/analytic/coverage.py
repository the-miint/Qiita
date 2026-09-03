"""Breadth of coverage: the scope it is measured over, the survivor set it yields,
and what the roll-up to genome level leaves behind.

**Breadth of coverage filters the table, at one of two scopes** — pooled over the
whole cohort, or per `(sample, genome)`. The two are not symmetric; `CoverageScope`
says why.

miint signature (qiita-verified; see `docs/duckdb-miint.md`):

    genome_coverage(alignments, subject_total_length, subject_genome_id)  -- table macro
      -> (genome_id, covered BIGINT, proportion_covered DOUBLE)

It takes NATIVE-INTEGER id columns — no `::VARCHAR` casts — and its three arguments
are UNQUOTED relation names resolved on the caller's connection.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from .reconcile import denovo_map_join
from .relations import (
    ALIGNMENT_TABLE,
    COVERAGE_ALIGNMENTS_VIEW,
    DENOVO_COVERAGE_ALIGNMENTS_VIEW,
    DENOVO_MAP_TABLE,
    GENOME_LENGTHS_TABLE,
    MAP_TABLE,
)


class CoverageScope(StrEnum):
    """The dimension breadth of coverage is measured over.

    A plain `StrEnum` with no Postgres twin: this is a per-request
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


# ONE SURVIVOR RELATION PER SCOPE, because the two have different shapes:
# `(genome_id)` for pooled, `(prep_sample_idx, genome_id)` for per-sample. The
# names differ so that building one scope's set and joining the other's is a bind
# error in both directions.
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


def _hand_rolled_ctes(
    *, prefix: str, alignments: str, map_relation: str, map_join: str, by_sample: bool
) -> str:
    """`genome_coverage`'s own method, spelled out: `compress_intervals` per contig,
    summed to the genome, over the full-length denominator the final SELECT divides
    by. Returns the CTE definitions; `_hand_rolled_select` reads the last of them.

    One copy, reached from three places — the reference arm's per-sample scope and
    the de novo arm's two. What each caller varies is where the intervals come from
    (`alignments`), how the contig resolves to its genome (`map_relation` +
    `map_join`), and whether the sample is a key of the result (`by_sample`).
    `prefix` keeps two instantiations in one statement from colliding.

    **`per_contig` groups by the sample in every case, `by_sample` or not.**
    `compress_intervals` merges within one coordinate space; grouping without the
    sample would merge two samples' intervals on a contig they share as though they
    were one sample's, overstating coverage. `by_sample` decides only whether the
    sample survives the roll-up to the genome — and for the de novo arm it makes no
    difference to the numbers either way, a qiita genome belonging to exactly one
    prep_sample.

    The per-contig merge precedes the genome roll-up for the same reason: grouping
    straight to the genome would merge intervals from DIFFERENT contigs as though
    they shared coordinates and understate every multi-contig genome.
    """
    sample_col = "p.prep_sample_idx, " if by_sample else ""
    sample_key = "prep_sample_idx, " if by_sample else ""
    return (
        f"{prefix}per_contig AS ("
        f"SELECT prep_sample_idx, reference, "
        f"UNNEST(compress_intervals(position, stop_position)) AS ci "
        f"FROM {alignments} GROUP BY prep_sample_idx, reference"
        f"), {prefix}per_contig_genome AS ("
        f"SELECT {sample_col}m.genome_id, "
        f"SUM(p.ci.stop - p.ci.start) AS covered_internal "
        f"FROM {prefix}per_contig p JOIN {map_relation} m "
        f"ON p.reference = m.contig_id{map_join} "
        f"GROUP BY {sample_col}m.genome_id, p.reference"
        f"), {prefix}covered AS ("
        f"SELECT {sample_key}genome_id, SUM(covered_internal) AS covered "
        f"FROM {prefix}per_contig_genome GROUP BY {sample_key}genome_id"
        f")"
    )


def _hand_rolled_select(*, prefix: str, by_sample: bool) -> str:
    """The threshold test over `_hand_rolled_ctes`' last CTE, shaped to the scope.

    Divides by the same `GENOME_LENGTHS_TABLE` full length and takes the same
    `CAST(... AS DOUBLE)` the macro does internally — otherwise one threshold would
    mean two different things depending on which branch produced a genome.
    """
    sample_col = "c.prep_sample_idx, " if by_sample else ""
    return (
        f"SELECT {sample_col}c.genome_id FROM {prefix}covered c "
        f"JOIN {GENOME_LENGTHS_TABLE} l USING (genome_id) "
        f"WHERE CAST(c.covered AS DOUBLE) / l.total_length >= ?"
    )


def _denovo_survivor_parts(scope: CoverageScope) -> tuple[str, str]:
    """The de novo arm's `(ctes, select)` for `scope`.

    Hand-rolled at BOTH scopes, where the reference arm hand-rolls only per-sample,
    and not collapsible into `genome_coverage_per_sample` the way that one is: both
    macros reduce their map to `SELECT DISTINCT contig_id, genome_id` and join on the
    contig alone, so a map keyed on the sample as well cannot be expressed through
    either. That is what the de novo arm needs — a content-addressed contig belongs
    to a different genome in each sample that assembled it — so the grouping is not
    the blocker here, the map's shape is.

    Lives in `coverage` rather than `reconcile`, which otherwise owns the de novo
    arm, because it needs `_hand_rolled_ctes` and `coverage` already imports
    `reconcile` — moving it would close the cycle.
    """
    return (
        _hand_rolled_ctes(
            prefix="dn_",
            alignments=DENOVO_COVERAGE_ALIGNMENTS_VIEW,
            map_relation=DENOVO_MAP_TABLE,
            map_join=denovo_map_join("p"),
            by_sample=scope is CoverageScope.PER_SAMPLE,
        ),
        _hand_rolled_select(prefix="dn_", by_sample=scope is CoverageScope.PER_SAMPLE),
    )


def survivor_table_sql(scope: CoverageScope, *, combined: bool = False) -> str:
    """The survivor set for `scope`: what clears the breadth-of-coverage threshold.
    Requires `COVERAGE_ALIGNMENTS_VIEW` and `GENOME_LENGTHS_TABLE`, plus the de novo
    arm's own two when `combined`. The threshold is a bound parameter — execute with
    `survivor_parameters(...)`, which knows how many the statement takes.

    Creates `survivor_table_name(scope)`, whose shape differs per scope —
    `(genome_id)` for pooled, `(prep_sample_idx, genome_id)` for per-sample. That is
    why the name carries the scope: see `_SURVIVOR_TABLES`.

    `POOLED` delegates to the `genome_coverage` macro. `PER_SAMPLE` cannot: the
    macro has no sample key. It instead reproduces the macro's own method with one
    more `GROUP BY` key (`_hand_rolled_ctes`), so a single threshold means the same
    thing under either scope. This is what upstream means by the per-sample
    dimension being "already expressible today" (duckdb-miint#217). Upstream's
    `genome_coverage_per_sample` now exists and would collapse THIS branch to one
    call; removal is tracked at the-miint/Qiita#454.

    It would not collapse the de novo branches, which hand-roll for a different
    reason — see `_denovo_survivor_parts`.

    **`combined` adds the de novo arm as a UNION, one survivor set covering both.**
    Not two sets: `ogu_input_table_sql` joins the survivors once per arm, and two
    relations would let a genome survive in one join and not the other.

    `UNION`, not `UNION ALL`, and the difference from `ogu_input_table_sql`'s choice
    is what this relation is FOR: it is joined, so a genome appearing twice fans out
    every alignment row that matches it and doubles that genome's counts. The arms do
    contribute disjoint genomes today — a reference genome and a qiita genome are
    different `qiita.genome` rows — so the distinct removes nothing; it is the
    cheap guard on a set whose duplicates would be silent.
    """
    if scope is CoverageScope.POOLED:
        reference = (
            f"SELECT genome_id "
            f"FROM genome_coverage("
            f"{COVERAGE_ALIGNMENTS_VIEW}, {GENOME_LENGTHS_TABLE}, {MAP_TABLE}) "
            f"WHERE proportion_covered >= ?"
        )
        reference_ctes = ""
    else:
        reference = _hand_rolled_select(prefix="", by_sample=True)
        reference_ctes = _hand_rolled_ctes(
            prefix="",
            alignments=COVERAGE_ALIGNMENTS_VIEW,
            map_relation=MAP_TABLE,
            map_join="",
            by_sample=True,
        )

    if not combined:
        head = f"WITH {reference_ctes} " if reference_ctes else ""
        return f"CREATE TABLE {survivor_table_name(scope)} AS {head}{reference}"

    # One WITH at the head of the statement, covering both UNION arms: SQL admits no
    # second WITH after a set operator, and the pooled branch has no reference CTEs
    # of its own to attach one to.
    denovo_ctes, denovo = _denovo_survivor_parts(scope)
    ctes = f"{reference_ctes}, {denovo_ctes}" if reference_ctes else denovo_ctes
    return f"CREATE TABLE {survivor_table_name(scope)} AS WITH {ctes} {reference} UNION {denovo}"


def survivor_parameters(coverage_threshold: float, *, combined: bool = False) -> list[float]:
    """The bound parameters `survivor_table_sql(..., combined=...)` takes.

    The threshold appears once per arm, because each arm tests its own quotient.
    Paired with `survivor_table_sql` so the count has one home; passing the wrong
    one is a bind error raised after the arms are already written.
    """
    return [coverage_threshold, coverage_threshold] if combined else [coverage_threshold]


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

    **Reads `ALIGNMENT_TABLE` and `MAP_TABLE` only**, so for a combined table this
    counts the reference arm, and counts it AFTER precedence has taken the reads the
    de novo arm won. The de novo arm's own unmappable rows are not in this number.
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
