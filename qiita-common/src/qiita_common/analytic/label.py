"""The relabel to public identifiers.

**A caller that PUBLISHES the table stages two more relations** — the label
relations — and relabels the counts through them, ending at `LABELLED_RELATION`
(`LABELLED_SCHEMA`): our `*_idx` keys are gone, and the public handles are VARCHAR,
which is what makes the result writable as BIOM at all. `LABELLED_RELATION` is the
only relation in this package the writers will copy.
"""

from __future__ import annotations

from dataclasses import dataclass

from .relations import (
    GENOME_LABEL_TABLE,
    LABELLED_RELATION,
    MAP_TABLE,
    OGU_OUTPUT_TABLE,
    SAMPLE_LABEL_TABLE,
)

# What a PUBLISHED table carries, name -> SQL type. Neither id is one of ours: both
# come from a mint whose job is to hand out a handle that means something outside this
# system — `export_id` per processed sample, `export_feature_id` per row. An `*_idx`
# means nothing out there and is not a handle we promise to keep.
#
# The VARCHAR types are why the relabel cannot be skipped as cosmetic: BIOM
# requires both id columns as VARCHAR while woltka hands back native BIGINTs, so
# the order is relabel-then-write and there is no writable table before the join.
LABELLED_SCHEMA = {
    "sample_id": "VARCHAR",
    "feature_id": "VARCHAR",
    "value": "DOUBLE",
}
LABELLED_COLUMNS = tuple(LABELLED_SCHEMA)


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
    one by the other does two different jobs, which is why it is named here once instead
    of spelled at each site:

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
    """The counts with both labels attached — the one join definition the
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
