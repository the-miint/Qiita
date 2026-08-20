"""String-level tests for `qiita_common.analytic.ogu` — woltka's input and output,
and the statement SEQUENCE that gets from the staged inputs to it.

The sequence tests are the part that is not text but order: a consumer that dropped
a step would not fail, it would publish a table with unfiltered genomes in it.
"""

from __future__ import annotations

import pytest

from qiita_common import analytic as ft


def test_survivor_join_is_in_the_ogu_input_statement_not_after_woltka():
    """Non-surviving genomes must leave woltka's INPUT, not its output.

    `woltka_ogu` splits a multi-mapped read across its distinct `reference`
    values, so a read hitting one surviving and one dropped genome must lose the
    dropped genome BEFORE woltka to renormalize to 1.0 on the survivor. Filtering
    woltka's OUTPUT strands it at 0.5 — a plausible number, not an error, which
    is why this is asserted structurally here as well as behaviourally in the
    consumers' real-miint suites.
    """
    for scope in ft.CoverageScope:
        assert ft.survivor_table_name(scope) in ft.ogu_input_table_sql(survivor_scope=scope), scope
    for scope in ft.CoverageScope:
        assert ft.survivor_table_name(scope) not in ft.woltka_ogu_select_sql(), scope


def test_ogu_input_omits_the_survivor_join_when_unfiltered():
    """At threshold 0 there is no survivor set to join — every mapped genome
    qualifies, and the coverage calc is skipped entirely. `None` is how that is
    said, so the unfiltered case cannot be confused with a scope."""
    sql = ft.ogu_input_table_sql(survivor_scope=None)
    for scope in ft.CoverageScope:
        assert ft.survivor_table_name(scope) not in sql, scope
    assert ft.MAP_TABLE in sql  # the map join is unconditional


def test_ogu_input_inner_joins_the_map():
    """An alignment to a feature with no genome is not an OGU. The map join drops
    it, so it must stay INNER — an outer join would emit NULL-genome rows."""
    sql = ft.ogu_input_table_sql(survivor_scope=ft.CoverageScope.POOLED)
    assert "JOIN" in sql
    assert "LEFT" not in sql.upper()
    assert "OUTER" not in sql.upper()


def test_only_the_per_sample_scope_keys_the_survivor_join_on_the_sample():
    """The whole difference between the scopes, at the join: pooled asks "did this
    genome clear the threshold across the cohort", per-sample asks "did it clear it
    in THIS sample". Getting the join key wrong silently applies one scope's
    survivor set with the other's semantics.
    """
    pooled = ft.ogu_input_table_sql(survivor_scope=ft.CoverageScope.POOLED)
    per_sample = ft.ogu_input_table_sql(survivor_scope=ft.CoverageScope.PER_SAMPLE)
    assert "s.prep_sample_idx" not in pooled
    assert "a.prep_sample_idx = s.prep_sample_idx" in per_sample
    # Both still key on the genome — per-sample ADDS the sample, it does not replace.
    assert "m.genome_id = s.genome_id" in pooled
    assert "m.genome_id = s.genome_id" in per_sample


def test_woltka_reads_its_source_as_a_quoted_relation_name():
    """`woltka_ogu` takes its source as a quoted string literal and resolves it on
    a SEPARATE connection, so the name is part of the contract — not a caller's
    choice — and the relation must exist by that name on the database."""
    assert f"'{ft.OGU_INPUT_TABLE}'" in ft.woltka_ogu_select_sql()


def test_woltka_output_is_per_sample():
    """`sample_id` is a NAMED argument; without it woltka returns
    `(feature_id, value)` and the whole per-sample dimension is lost."""
    assert "sample_id :=" in ft.woltka_ogu_select_sql()


def test_empty_result_schema_matches_the_woltka_projection():
    """The 0-row short-circuit and the real path must produce the same schema.
    They are separate SQL statements, so nothing but this stops them drifting —
    and a consumer reading the Parquet back would see the drift as a missing
    column on empty cohorts only.
    """
    empty = ft.empty_ogu_select_sql()
    real = ft.woltka_ogu_select_sql()
    for column in ft.OUTPUT_COLUMNS:
        assert column in empty, column
        assert column in real, column


def test_empty_result_casts_every_column_to_its_declared_type():
    """The empty path is GENERATED from `OUTPUT_SCHEMA`, so the declared type is the
    only place a type is written down. A hand-edited cast that disagreed with the
    real path would surface as a physical type mismatch between an empty-cohort
    export and a populated one — the empty path being the one exercised least.
    """
    sql = ft.empty_ogu_select_sql()
    for name, sql_type in ft.OUTPUT_SCHEMA.items():
        assert f"CAST(NULL AS {sql_type}) AS {name}" in sql


def test_empty_result_selects_no_rows():
    assert "WHERE false" in ft.empty_ogu_select_sql()


def test_both_populated_and_empty_counts_land_in_one_relation():
    """The empty path is the one a caller exercises least and would notice last, so it
    must produce the same relation name — and hence go through the same relabel and
    the same writer — as a populated cohort.
    """
    for populated in (True, False):
        assert ft.ogu_output_table_sql(populated=populated).startswith(
            f"CREATE TABLE {ft.OGU_OUTPUT_TABLE} AS "
        )
    assert ft.woltka_ogu_select_sql() in ft.ogu_output_table_sql(populated=True)
    assert ft.empty_ogu_select_sql() in ft.ogu_output_table_sql(populated=False)


def _sequence(scope, threshold) -> list[str]:
    """`ogu_input_statements` as a list of first-words-plus-relation, for asserting on
    order without pinning whole SQL strings."""
    return [sql for sql, _ in ft.ogu_input_statements(scope=scope, coverage_threshold=threshold)]


@pytest.mark.parametrize("scope", list(ft.CoverageScope))
def test_the_sequence_builds_the_filter_before_woltkas_input(scope):
    """The order the module owns rather than each consumer: the survivor set must exist
    before `ogu_input` joins it, and the coverage view before the survivor set reads it.
    A consumer that got this order wrong would fail loudly; one that dropped a step
    would publish unfiltered genomes, which is why the order is not the caller's."""
    sequence = _sequence(scope, 0.01)
    positions = {
        "view": next(i for i, s in enumerate(sequence) if s.startswith("CREATE VIEW")),
        "survivor": next(
            i
            for i, s in enumerate(sequence)
            if ft.survivor_table_name(scope) in s and "CREATE" in s
        ),
        "input": next(
            i for i, s in enumerate(sequence) if ft.OGU_INPUT_TABLE in s and "CREATE" in s
        ),
    }
    assert positions["view"] < positions["survivor"] < positions["input"]


def test_an_unfiltered_sequence_has_no_coverage_step_at_all():
    """At threshold 0 there is nothing to filter, so neither the view nor the survivor
    set is built — and the caller must not have to know that, which is why the
    scope-to-`None` conversion lives here."""
    sequence = _sequence(ft.CoverageScope.PER_SAMPLE, 0.0)
    joined = " ".join(sequence)
    assert ft.COVERAGE_ALIGNMENTS_VIEW not in joined
    for scope in ft.CoverageScope:
        assert ft.survivor_table_name(scope) not in joined, scope
    assert any(ft.OGU_INPUT_TABLE in s and "CREATE" in s for s in sequence)


@pytest.mark.parametrize("threshold", [0.0, 0.01])
def test_the_sequence_releases_every_relation_it_finishes_with(threshold):
    """The alignment slice, the coverage view, and the survivor set are all dead once
    woltka's input exists — and dead for the whole rest of the run, through woltka, the
    relabel, and the write. On a client that is several hundred MB held for nothing, and
    DuckDB spills into whatever directory the user ran the CLI from."""
    scope = ft.CoverageScope.POOLED
    sequence = _sequence(scope, threshold)
    dropped = {s.split()[-1] for s in sequence if s.startswith("DROP")}
    expected = {ft.ALIGNMENT_TABLE}
    if threshold:
        expected |= {ft.COVERAGE_ALIGNMENTS_VIEW, ft.survivor_table_name(scope)}
    assert dropped == expected
    # Every DROP comes after the CREATE that made its contents redundant.
    last_create = max(i for i, s in enumerate(sequence) if s.startswith("CREATE"))
    assert all(i > last_create for i, s in enumerate(sequence) if s.startswith("DROP"))


def test_the_view_is_dropped_before_the_table_it_reads():
    """`cov_alignments` selects from the alignment slice. Dropping the slice first
    leaves a dangling view — tolerated by DuckDB today, but not something to rely on."""
    sequence = _sequence(ft.CoverageScope.POOLED, 0.01)
    drops = [s for s in sequence if s.startswith("DROP")]
    assert drops[0] == f"DROP VIEW {ft.COVERAGE_ALIGNMENTS_VIEW}"
    assert f"DROP TABLE {ft.ALIGNMENT_TABLE}" in drops[1:]


def test_woltkas_input_has_its_own_release():
    """Separate from the sequence because the caller must read the row count between
    the two — that count is what decides whether woltka runs at all."""
    assert ft.drop_ogu_input_table_sql() == f"DROP TABLE {ft.OGU_INPUT_TABLE}"
    assert ft.OGU_INPUT_TABLE in ft.ogu_input_count_sql()
