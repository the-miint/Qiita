"""The three input streams, staged into the relations the rest of the analytic reads.

| Relation | Columns | Source |
|---|---|---|
| `ALIGNMENT_TABLE` | `ALIGNMENT_COLUMNS` | the alignment DoGet stream |
| `MAP_TABLE` | `(contig_id, genome_id)` | feature→genome map (Postgres-derived) |
| `GENOME_LENGTHS_TABLE` | `(genome_id, total_length)` | the reference_sequences stream |

The map's source must be keyed `(feature_idx, genome_idx)`; `map_table_sql` does
the rename to the column names `genome_coverage` requires.

Every `source` here is interpolated verbatim — see the package docstring for what
that obliges of a caller.
"""

from __future__ import annotations

from .relations import (
    ALIGNMENT_TABLE,
    FEATURE_LENGTHS_TABLE,
    GENOME_LENGTHS_TABLE,
    MAP_TABLE,
)

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


def feature_lengths_table_sql(source: str) -> str:
    """Stage the per-feature lengths from `source` (the caller's reference_sequences
    stream) into `FEATURE_LENGTHS_TABLE`, unrolled.

    The circular gate needs a length per aligned feature, and the stream is one-shot —
    so when that gate runs, this is what the stream is drained into and
    `genome_lengths_table_sql` reads THIS relation rather than the stream. Every other
    path stages the roll-up directly and never creates this one.

    **Whole-reference, with no narrowing to the features the cohort aligned to**, unlike
    the roll-up which aggregates on arrival. There is nothing to narrow against yet: the
    lengths stream is opened BEFORE the alignment so a failure in it does not come after
    a cohort's rows have crossed the wire, so the aligned feature set is not known here.
    `circular_query_coverage` ignores references its input never mentions, so the extra
    rows cost two BIGINTs each and nothing else, and the gate's clearance releases them.
    """
    return (
        f"CREATE TABLE {FEATURE_LENGTHS_TABLE} AS "
        f"SELECT feature_idx, sequence_length_bp FROM {source}"
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
