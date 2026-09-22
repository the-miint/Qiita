"""Breadth of coverage: the scope it is measured over, the survivor set it yields,
and what the roll-up to genome level leaves behind.

**Breadth of coverage filters the table, at one of two scopes** — pooled over the
whole cohort, or per `(sample, genome)`. The two are not symmetric; `CoverageScope`
says why.

miint signatures (see `docs/duckdb-miint.md`, which links upstream's contracts), both
table macros:

    genome_coverage(alignments, subject_total_length, subject_genome_id)
      -> (genome_id, covered BIGINT, proportion_covered DOUBLE)
    genome_coverage_per_sample(alignments, subject_total_length, subject_genome_id)
      -> (sample_id, genome_id, covered BIGINT, proportion_covered DOUBLE)

They take NATIVE-INTEGER id columns — no `::VARCHAR` casts — and their three
arguments are UNQUOTED relation names resolved on the caller's connection.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from .relations import (
    ALIGNMENT_TABLE,
    COVERAGE_ALIGNMENTS_VIEW,
    DENOVO_ALIGNMENT_TABLE,
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


def _alignments_view_sql(view: str, source: str) -> str:
    """One arm's aligned intervals, as the `alignments` argument the coverage macros
    take. `coverage_alignments_view_sql` and `denovo_coverage_alignments_view_sql` are
    this with each arm's relations.

    Renamed to the macros' column names: `reference` for the feature, and
    `sample_id` for the prep_sample, which `genome_coverage_per_sample` groups on.
    `genome_coverage` names only `(reference, position, stop_position)` and projects
    them out of `query_table(alignments)` by name, so the extra column is tolerated
    (probed against the mirror build). One view per arm therefore serves both scopes.

    NULL coordinates are excluded. `compress_intervals`, which both macros run
    internally, drops such rows silently rather than erroring, so filtering here is
    what makes the exclusion visible to a reader rather than implicit in an
    aggregate's behaviour.

    A VIEW, not a table: only this connection reads it, so materializing would
    duplicate the alignment slice in RAM.
    """
    return (
        f"CREATE VIEW {view} AS "
        f"SELECT prep_sample_idx AS sample_id, feature_idx AS reference, "
        f"position, stop_position "
        f"FROM {source} "
        f"WHERE position IS NOT NULL AND stop_position IS NOT NULL"
    )


def coverage_alignments_view_sql() -> str:
    """The reference arm's aligned intervals; see `_alignments_view_sql`."""
    return _alignments_view_sql(COVERAGE_ALIGNMENTS_VIEW, ALIGNMENT_TABLE)


def denovo_coverage_alignments_view_sql() -> str:
    """The de novo arm's aligned intervals; see `_alignments_view_sql`.

    A separate view rather than a `UNION ALL` with the reference one, because the two
    reach their genome through different maps; the union happens in the survivor set,
    after each arm has been rolled up through its own.
    """
    return _alignments_view_sql(DENOVO_COVERAGE_ALIGNMENTS_VIEW, DENOVO_ALIGNMENT_TABLE)


def _survivor_select(scope: CoverageScope, *, alignments: str, genome_map: str) -> str:
    """One arm's threshold test for `scope`: that scope's macro over the arm's
    intervals and contig->genome map, divided by `GENOME_LENGTHS_TABLE`, which holds
    both arms' denominators.

    Per-sample renames the macro's `sample_id` back to `prep_sample_idx`, the key
    `ogu_input_table_sql` joins the per-sample set on.
    """
    if scope is CoverageScope.POOLED:
        columns, macro = "genome_id", "genome_coverage"
    else:
        columns, macro = "sample_id AS prep_sample_idx, genome_id", "genome_coverage_per_sample"
    return (
        f"SELECT {columns} "
        f"FROM {macro}({alignments}, {GENOME_LENGTHS_TABLE}, {genome_map}) "
        f"WHERE proportion_covered >= ?"
    )


def survivor_table_sql(scope: CoverageScope, *, combined: bool = False) -> str:
    """The survivor set for `scope`: what clears the breadth-of-coverage threshold.
    Requires `COVERAGE_ALIGNMENTS_VIEW` and `GENOME_LENGTHS_TABLE`, plus
    `DENOVO_COVERAGE_ALIGNMENTS_VIEW` and `DENOVO_MAP_TABLE` when `combined`. The
    threshold is a bound parameter — execute with `survivor_parameters(...)`, which
    knows how many the statement takes.

    Creates `survivor_table_name(scope)`, whose shape differs per scope —
    `(genome_id)` for pooled, `(prep_sample_idx, genome_id)` for per-sample. That is
    why the name carries the scope: see `_SURVIVOR_TABLES`.

    **The de novo arm passes its map to the macro without the prep_sample term that
    `denovo_map_join` adds to the read-level joins against it.** The macros join on
    the contig alone, and a contig mapped to two genomes counts toward both:
    <https://the-miint.github.io/duckdb-miint/alignment_analysis/#genome-coverage>.
    A contig two cohort prep_samples assembled — one content-addressed `feature_idx`
    under each prep_sample's genome — is therefore credited to both genomes:

    * pooled, that is the scope's definition: breadth over every prep_sample's
      intervals, so every prep_sample's reads on the contig count toward each genome
      holding it, as they do for a reference genome sharing a feature. With the
      prep_sample term, a de novo genome would see only the reads of the prep_sample
      that assembled it, and pooled breadth would equal per-sample breadth. `align_denovo`
      aligns each prep_sample against only the contigs it assembled, so the other
      prep_samples' reads a de novo genome gains are those on contigs they also
      assembled. That includes a read whose own prep_sample has no genome for the
      contig in the gated map: precedence leaves it on the reference arm for counting,
      and its interval still adds to the de novo breadth here. The intervals merged
      are positions on whichever copy of the contig the lake held when each
      prep_sample's `align_denovo` ran. A contig and its reverse complement share one
      `feature_idx`, and a later assembly run's copy replaces the stored one
      (`flight_service::REPLACE_KEY_TABLES` in the data plane). A prep_sample aligned
      before a run that stored the reverse complement keeps positions on the opposite
      axis, and nothing re-aligns it, so pooled breadth on that contig can count one
      stretch twice or merge two distinct stretches into one;
    * per-sample, it adds `(prep_sample, genome)` pairs for the other prep_sample's
      genome, and those never reach the table: `denovo_ogu_input_select_sql` maps each
      read through the prep_sample term, so no read of that prep_sample is on that
      genome.

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
    arms = [_survivor_select(scope, alignments=COVERAGE_ALIGNMENTS_VIEW, genome_map=MAP_TABLE)]
    if combined:
        arms.append(
            _survivor_select(
                scope, alignments=DENOVO_COVERAGE_ALIGNMENTS_VIEW, genome_map=DENOVO_MAP_TABLE
            )
        )
    return f"CREATE TABLE {survivor_table_name(scope)} AS " + " UNION ".join(arms)


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
