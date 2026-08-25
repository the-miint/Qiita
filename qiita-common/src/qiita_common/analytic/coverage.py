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

from .relations import (
    ALIGNMENT_TABLE,
    COVERAGE_ALIGNMENTS_VIEW,
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
