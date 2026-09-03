"""String-level tests for `qiita_common.analytic.coverage` — the scope, the survivor
set it builds, and what the roll-up leaves behind."""

from __future__ import annotations

import pytest

from qiita_common import analytic as ft


def test_survivor_table_shape_matches_its_scope():
    """Pooled emits genomes; per-sample emits (sample, genome) pairs."""
    pooled = ft.survivor_table_sql(ft.CoverageScope.POOLED)
    per_sample = ft.survivor_table_sql(ft.CoverageScope.PER_SAMPLE)
    assert "prep_sample_idx" not in pooled
    assert "prep_sample_idx" in per_sample


def test_each_scope_builds_and_joins_a_DIFFERENTLY_NAMED_survivor_relation():
    """The scopes' survivor sets have different shapes, so the name carries the scope
    — that is what makes a build/join mismatch a bind error instead of a wrong
    number.

    The dangerous direction is a per-sample set joined on the genome alone: valid
    SQL that fans each alignment row out once per sample the genome survived in.
    Distinct names remove it, so this asserts the names differ AND that each
    statement only ever mentions its own.
    """
    pooled_name = ft.survivor_table_name(ft.CoverageScope.POOLED)
    per_sample_name = ft.survivor_table_name(ft.CoverageScope.PER_SAMPLE)
    assert pooled_name != per_sample_name

    for scope, own, other in (
        (ft.CoverageScope.POOLED, pooled_name, per_sample_name),
        (ft.CoverageScope.PER_SAMPLE, per_sample_name, pooled_name),
    ):
        build = ft.survivor_table_sql(scope)
        join = ft.ogu_input_table_sql(survivor_scope=scope)
        assert own in build and own in join, scope
        assert other not in build and other not in join, scope


def test_pooled_scope_uses_the_macro_and_per_sample_hand_rolls_it():
    """Pooled delegates to `genome_coverage`. Per-sample cannot — the macro has no
    sample key — so it hand-rolls the macro's own method (`compress_intervals` per
    contig, summed to the genome) with one more GROUP BY key. Upstream confirms
    this is expressible today; see duckdb-miint#217.
    """
    pooled = ft.survivor_table_sql(ft.CoverageScope.POOLED)
    per_sample = ft.survivor_table_sql(ft.CoverageScope.PER_SAMPLE)
    assert "genome_coverage(" in pooled
    assert "compress_intervals(" not in pooled
    assert "compress_intervals(" in per_sample
    assert "genome_coverage(" not in per_sample


def test_per_sample_divides_by_the_same_full_length_denominator():
    """Both scopes must divide by the genome's FULL length — the macro does it
    internally, so the hand-rolled form has to reach for the same lengths table and
    the same DOUBLE cast, or the two scopes' proportions are not comparable and a
    single threshold means two different things.
    """
    sql = ft.survivor_table_sql(ft.CoverageScope.PER_SAMPLE)
    assert ft.GENOME_LENGTHS_TABLE in sql
    assert "total_length" in sql
    assert "CAST(" in sql and "AS DOUBLE)" in sql


def test_per_sample_merges_intervals_per_contig_before_rolling_to_the_genome():
    """`compress_intervals` merges within one contig; a genome's covered bases are
    the SUM over its contigs. Grouping straight to the genome would merge intervals
    from DIFFERENT contigs as if they shared a coordinate space, understating
    coverage for every multi-contig genome.
    """
    sql = ft.survivor_table_sql(ft.CoverageScope.PER_SAMPLE)
    merge_at = sql.index("compress_intervals(")
    rollup_at = sql.index(ft.MAP_TABLE)
    assert merge_at < rollup_at, "intervals must be merged before the genome roll-up"


def test_coverage_input_excludes_null_coordinates():
    """A NULL coordinate cannot contribute an interval. `compress_intervals` drops
    such rows silently rather than erroring, so filtering here is what makes the
    exclusion visible to a reader instead of implicit in an aggregate's behaviour.
    """
    sql = ft.coverage_alignments_view_sql()
    assert "position IS NOT NULL" in sql
    assert "stop_position IS NOT NULL" in sql


def test_coverage_input_carries_the_sample_key_for_both_scopes():
    """One view feeds both scopes: per-sample needs `prep_sample_idx` to group by,
    and `genome_coverage` tolerates the extra column because it projects the three
    it names out of `query_table(alignments)` (probed against the mirror build).
    A pooled-only view would force a second, near-identical view.
    """
    assert "prep_sample_idx" in ft.coverage_alignments_view_sql()


def test_threshold_is_a_bound_parameter_in_both_scopes():
    """The threshold is bound, never interpolated — no builder takes a float. Both
    scopes compare with `>=`, so a genome sitting exactly on the threshold is kept
    (pinned behaviourally in the control-plane tier)."""
    for scope in ft.CoverageScope:
        sql = ft.survivor_table_sql(scope)
        assert "?" in sql, scope
        assert ">= ?" in sql, scope


def test_coverage_filter_applies_only_above_zero():
    """One predicate decides both the SQL branch and whether the caller streams the
    reference lengths at all; a threshold of exactly 0 must take the skip path."""
    assert not ft.coverage_filter_applies(0.0)
    assert ft.coverage_filter_applies(1e-9)
    assert ft.coverage_filter_applies(1.0)


@pytest.mark.parametrize("bad", [-0.001, 1.001, -1.0, 2.0])
def test_a_threshold_outside_zero_to_one_is_refused(bad):
    """Both directions are silent rather than loud if unchecked: below 0 reads as "no
    filter at all", above 1 drops every genome and yields an empty table that looks
    like a result. Each consumer validates at its own boundary; this is the shared
    backstop for the next one."""
    with pytest.raises(ValueError, match="proportion"):
        ft.coverage_filter_applies(bad)


def test_the_rollup_coverage_report_refuses_nothing():
    """A feature with no genome is not an error: a 16S record is not an OGU, and there
    is no genome-rooted row to emit for it. It is REPORTED because the alternative is a
    table that is quietly a fraction of what the caller streamed."""
    coverage = ft.RollupCoverage(alignment_rows=100, unmapped_rows=40, unmapped_features=7)
    assert not coverage.complete
    assert ft.RollupCoverage(alignment_rows=100, unmapped_rows=0, unmapped_features=0).complete


def test_the_rollup_warning_names_the_share_and_what_is_missing():
    warning = ft.rollup_coverage_warning(
        ft.RollupCoverage(alignment_rows=100, unmapped_rows=40, unmapped_features=7)
    )
    assert "40 of 100" in warning
    assert "40.0%" in warning
    assert "7 features" in warning


@pytest.mark.parametrize("scope", list(ft.CoverageScope))
@pytest.mark.parametrize("combined", [False, True])
def test_survivor_parameters_matches_the_placeholders_its_statement_carries(scope, combined):
    """The threshold appears once per ARM, so the combined statement binds it twice.

    Pinned the way `gated_alignment_parameters` is: a caller that passed the wrong
    count gets a bind error, but only after the arms had been written — and the two
    are built by separate functions, so nothing but this holds them together.
    """
    sql = ft.survivor_table_sql(scope, combined=combined)
    parameters = ft.survivor_parameters(0.01, combined=combined)
    assert sql.count("?") == len(parameters)
    assert parameters == [0.01] * (2 if combined else 1)


@pytest.mark.parametrize("scope", list(ft.CoverageScope))
def test_the_combined_survivor_set_unions_both_arms_into_one_relation(scope):
    """One relation covering both arms, not two. `ogu_input_table_sql` joins the
    survivors once per arm, and two relations would let a genome survive in one join
    and not the other — a table whose filter and whose counts disagree about what it
    contains.
    """
    combined = ft.survivor_table_sql(scope, combined=True)
    assert " UNION " in combined
    assert ft.DENOVO_MAP_TABLE in combined
    assert combined.count(f"CREATE TABLE {ft.survivor_table_name(scope)}") == 1
    # And the uncombined form mentions the de novo arm nowhere at all.
    assert ft.DENOVO_MAP_TABLE not in ft.survivor_table_sql(scope)
