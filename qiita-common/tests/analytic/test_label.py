"""String-level tests for `qiita_common.analytic.label` — the relabel to public
handles, and the refusals that keep a wrong published table from looking right."""

from __future__ import annotations

import pytest

from qiita_common import analytic as ft
from qiita_common.analytic import label


def _relabel_check(
    *,
    output_rows=100,
    labeled_rows=100,
    unlabeled_genome_rows=0,
    unlabeled_sample_rows=0,
    genomes=10,
    feature_ids=10,
    samples=5,
    sample_ids=5,
) -> ft.LabelClearance:
    """`check_relabel_diagnostics` over a clean row, with one field overridable per
    test — the defaults describe 100 rows of 10 genomes across 5 samples, all
    labeled and all labels distinct."""
    return ft.check_relabel_diagnostics(
        output_rows=output_rows,
        labeled_rows=labeled_rows,
        unlabeled_genome_rows=unlabeled_genome_rows,
        unlabeled_sample_rows=unlabeled_sample_rows,
        genomes=genomes,
        feature_ids=feature_ids,
        samples=samples,
        sample_ids=sample_ids,
    )


def test_the_public_schema_carries_no_internal_identifier():
    """What the relabel is for. `prep_sample_idx` and `genome_idx` are ours —
    they mean nothing outside this system and are not handles we promise to keep — so
    a published table names its columns with the minted `export_id` and its rows with
    the minted `export_feature_id`. Asserted on the schema rather than on one builder's text
    because this is the property every writer downstream inherits.
    """
    assert set(ft.LABELED_SCHEMA) & set(ft.OUTPUT_SCHEMA) == {"value"}
    assert not [name for name in ft.LABELED_SCHEMA if name.endswith("_idx")]


def test_both_public_identifiers_are_varchar():
    """BIOM requires `feature_id`/`sample_id` as VARCHAR while woltka hands back
    native BIGINTs, so the relabel is what makes the table writable at all — not a
    cosmetic rename. A relabel that carried an integer id through would fail at the
    writer, one phase later, on a table that looks otherwise complete.
    """
    assert ft.LABELED_SCHEMA["sample_id"] == "VARCHAR"
    assert ft.LABELED_SCHEMA["feature_id"] == "VARCHAR"


def test_the_labeled_table_projects_exactly_the_public_columns():
    """The projection is the enforcement: both `_idx` columns are joined ON and then
    dropped, so nothing downstream can read one out of this relation even by mistake.
    """
    sql = ft.labeled_relation_sql(clearance=_relabel_check())
    projection = sql.split(" FROM ", 1)[0]
    assert projection.endswith(", ".join(ft.LABELED_COLUMNS))
    for internal in ("prep_sample_idx", "genome_idx"):
        assert internal not in projection, internal


def test_the_genome_label_table_renames_the_minted_handle():
    """From the mint's response, not from the genome map's `source_id`. The map cannot
    answer what a row is NAMED: whether a genome's accession is unique across
    everything already published is a database fact, and the mint is what holds it.

    No DISTINCT, unlike the map-fed version this replaced — the mint answers once per
    genome, so a duplicate is a fault to refuse rather than a fan-out to collapse.
    """
    sql = ft.genome_label_table_sql("mint_src")
    assert "DISTINCT" not in sql
    assert "export_feature_id AS feature_id" in sql
    assert "FROM mint_src" in sql


def test_the_sample_label_table_renames_the_minted_handle():
    sql = ft.sample_label_table_sql("mint_src")
    assert "export_id AS sample_id" in sql
    assert "FROM mint_src" in sql


def test_the_diagnostics_and_the_relabel_read_THE_SAME_join():
    """One join definition, used to measure and then to write, so the rows checked are
    the rows written. Two copies could drift into a check that clears a join the
    relabel does not perform.
    """
    joined = label._labeled_select_sql()
    assert joined in ft.relabel_diagnostics_sql()
    assert joined in ft.labeled_relation_sql(clearance=_relabel_check())


def test_the_labels_are_left_joined_so_a_missing_one_is_visible():
    """An INNER join would DROP a count whose genome or sample has no label, quietly
    shortening the table; LEFT keeps the row with a NULL id, which is what the
    diagnostics detect and refuse.
    """
    joined = label._labeled_select_sql()
    assert joined.count("LEFT JOIN") == 2
    assert "JOIN" not in joined.replace("LEFT JOIN", "")


def test_the_diagnostics_measure_the_unlabeled_rows_of_both_axes():
    sql = ft.relabel_diagnostics_sql()
    assert "FILTER (WHERE feature_id IS NULL)" in sql
    assert "FILTER (WHERE sample_id IS NULL)" in sql
    # The fan-out check needs the unjoined row count, which the joined relation
    # cannot supply — hence the scalar subquery over the counts table.
    assert f"(SELECT count(*) FROM {ft.OGU_OUTPUT_TABLE})" in sql


def test_check_refuses_a_label_join_that_fanned_out():
    """A label relation with two rows for one key multiplies that key's counts. Every
    shape of that mistake — a duplicated row, a genome under two `source_id`s, a
    sample under two `export_id`s — shows up as more joined rows than counts, so one
    comparison catches all of them.
    """
    with pytest.raises(ValueError, match="changed the table's size"):
        _relabel_check(output_rows=100, labeled_rows=140)


def test_check_refuses_an_unlabeled_genome():
    """Matched on the column that would go NULL, so the two unlabeled messages cannot
    stand in for each other."""
    with pytest.raises(ValueError, match="NULL feature_id"):
        _relabel_check(unlabeled_genome_rows=3)


def test_check_refuses_an_unlabeled_sample():
    with pytest.raises(ValueError, match="NULL sample_id"):
        _relabel_check(unlabeled_sample_rows=3)


def test_check_refuses_a_feature_id_collision():
    """The mint's published namespace is UNIQUE across live rows, so this cannot happen
    server-side — the same posture as the `export_id` case below. The check stays
    because it costs one comparison and the failure is invisible: BIOM SUMS duplicate
    `(feature_id, sample_id)` pairs, so two organisms would quietly become one row.
    """
    with pytest.raises(ValueError, match="export_feature_id"):
        _relabel_check(genomes=10, feature_ids=9)


def test_check_refuses_an_export_id_collision():
    """The mirror of the `source_id` case on the sample axis. `export_id` is minted by
    Postgres from a unique idx so this cannot happen server-side today — but the check
    costs one comparison, and the failure it guards against (two samples summed into
    one column) is invisible in the output.
    """
    with pytest.raises(ValueError, match="export_id"):
        _relabel_check(samples=5, sample_ids=4)


@pytest.mark.parametrize(
    ("unlabeled", "distinct"),
    [
        ({"unlabeled_genome_rows": 1}, {"genomes": 10, "feature_ids": 9}),
        ({"unlabeled_sample_rows": 1}, {"samples": 5, "sample_ids": 4}),
    ],
)
def test_check_reports_unlabeled_rows_before_testing_for_a_collision(unlabeled, distinct):
    """`count(DISTINCT x)` skips NULLs, so an unlabeled row depresses the distinct-label
    count and looks EXACTLY like a collision — both conditions are true here. The
    unlabeled checks therefore run first, and this pins that order on both axes.

    Matched on `no public handle`, which only the unlabeled messages carry: the word
    "genome" appears in the collision message too, so a looser match would be satisfied
    by the very swap this test exists to catch.
    """
    with pytest.raises(ValueError, match="no public handle"):
        _relabel_check(**unlabeled, **distinct)


def test_check_passes_a_clean_join_and_returns_the_row_count():
    """The clearance carries what it cleared, so a caller can report the size of the
    table it is about to write without recounting it."""
    clearance = _relabel_check(output_rows=100, labeled_rows=100)
    assert isinstance(clearance, ft.LabelClearance)
    assert clearance.rows == 100


def test_check_passes_an_empty_table():
    """An empty cohort is a legitimate result — every genome dropped by the threshold,
    an all-16S reference — and has nothing to collide or go unlabeled."""
    assert (
        _relabel_check(
            output_rows=0, labeled_rows=0, genomes=0, feature_ids=0, samples=0, sample_ids=0
        ).rows
        == 0
    )


def test_the_relabel_sql_is_unreachable_without_a_clearance():
    """Same constraint as the gate's, for the same reason: the failures the checks
    catch are silent, so "check first" has to be a type error rather than a docstring.
    """
    with pytest.raises(TypeError):
        ft.labeled_relation_sql()
