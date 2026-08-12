"""Tests for `qiita_common.ogu_table` — the shared SQL of the OGU feature-table
analytic.

These are **string-level** tests, deliberately. `qiita-common` has no duckdb
dependency (the module returns SQL text and never opens a connection), so the
analytic's *behaviour* is pinned where it runs, against real miint:
`qiita-compute-orchestrator/tests/jobs/test_estimate_feature_table.py` for the
server-side job, and the control-plane tier for the client-side recipe.

What is worth pinning here is what a reader of the SQL cannot see and a
behavioural test can only catch indirectly: which relation each step must be
(TABLE vs VIEW, never TEMP), which JOIN happens *before* woltka, and what the
coverage denominator is allowed to depend on. Each of those is a silent-wrong-
answer rule rather than a crash, so it gets an assertion that names it.
"""

from __future__ import annotations

from qiita_common import ogu_table as ot

# Every builder that emits a CREATE, with a representative call. Used by the
# sweeps below so a newly-added builder is covered by default rather than by
# somebody remembering to extend a list.
_CREATE_BUILDERS = {
    "alignment_table_sql": lambda: ot.alignment_table_sql("some_stream"),
    "map_table_sql": lambda: ot.map_table_sql("read_parquet('m.parquet')"),
    "genome_lengths_table_sql": lambda: ot.genome_lengths_table_sql("lengths_stream"),
    "coverage_alignments_view_sql": ot.coverage_alignments_view_sql,
    "pooled_survivor_table_sql": ot.pooled_survivor_table_sql,
    "ogu_input_table_sql": lambda: ot.ogu_input_table_sql(filter_to_survivors=True),
}


def test_survivor_join_is_in_the_ogu_input_statement_not_after_woltka():
    """The load-bearing ordering: non-surviving genomes leave woltka's INPUT.

    `woltka_ogu` splits a multi-mapped read across its distinct `reference`
    values, so a read hitting one surviving and one dropped genome must lose the
    dropped genome BEFORE woltka to renormalize to 1.0 on the survivor. Filtering
    woltka's OUTPUT strands it at 0.5 — a plausible number, not an error, which
    is why this is asserted structurally here as well as behaviourally in the
    consumers' real-miint suites.
    """
    assert ot.SURVIVOR_TABLE in ot.ogu_input_table_sql(filter_to_survivors=True)
    assert ot.SURVIVOR_TABLE not in ot.woltka_ogu_select_sql()


def test_ogu_input_omits_the_survivor_join_when_unfiltered():
    """At threshold 0 there is no survivor set to join — every mapped genome
    qualifies, and the coverage calc is skipped entirely."""
    sql = ot.ogu_input_table_sql(filter_to_survivors=False)
    assert ot.SURVIVOR_TABLE not in sql
    assert ot.MAP_TABLE in sql  # the map join is unconditional


def test_ogu_input_inner_joins_the_map():
    """An alignment to a feature with no genome is not an OGU. The map join drops
    it, so it must stay INNER — an outer join would emit NULL-genome rows."""
    sql = ot.ogu_input_table_sql(filter_to_survivors=True)
    assert "JOIN" in sql
    assert "LEFT" not in sql.upper()
    assert "OUTER" not in sql.upper()


def test_woltka_reads_its_source_as_a_quoted_relation_name():
    """`woltka_ogu` takes its source as a quoted string literal and resolves it on
    a SEPARATE connection, so the name is part of the contract — not a caller's
    choice — and the relation must exist by that name on the database."""
    assert f"'{ot.OGU_INPUT_TABLE}'" in ot.woltka_ogu_select_sql()


def test_woltka_output_is_per_sample():
    """`sample_id` is a NAMED argument; without it woltka returns
    `(feature_id, value)` and the whole per-sample dimension is lost."""
    assert "sample_id :=" in ot.woltka_ogu_select_sql()


def test_no_builder_creates_a_temp_relation():
    """`woltka_ogu` resolves its source on a separate connection, which sees
    regular tables and views but NOT TEMP tables, registered stream relations, or
    CTEs. A TEMP anywhere in this chain fails at bind time on the real thing."""
    for name, build in _CREATE_BUILDERS.items():
        sql = build().upper()
        assert "TEMP" not in sql, name
        assert "TEMPORARY" not in sql, name


def test_coverage_input_is_a_view_and_woltka_input_is_a_table():
    """Two different rules, one per relation. `cov_alignments` is read only by the
    `genome_coverage` macro on the caller's own connection, so materializing it
    would just duplicate the alignment slice in RAM; `ogu_input` is read by
    woltka's separate connection, so it must be a real table."""
    assert "CREATE VIEW" in ot.coverage_alignments_view_sql()
    assert "CREATE TABLE" in ot.ogu_input_table_sql(filter_to_survivors=True)
    assert "CREATE TABLE" in ot.alignment_table_sql("s")


def test_coverage_input_excludes_null_coordinates():
    """A NULL coordinate cannot contribute an interval and poisons the merge."""
    sql = ot.coverage_alignments_view_sql()
    assert "position IS NOT NULL" in sql
    assert "stop_position IS NOT NULL" in sql


def test_coverage_denominator_does_not_depend_on_the_alignment():
    """The denominator is the genome's FULL length, including contigs nothing
    aligned to. Joining the alignment slice in here would silently narrow it to
    aligned-length-only — every genome's breadth would rise and low-coverage
    genomes would survive a threshold they should fail.
    """
    sql = ot.genome_lengths_table_sql("lengths_stream")
    assert ot.ALIGNMENT_TABLE not in sql
    assert ot.MAP_TABLE in sql  # rolls contig lengths up to the genome
    assert "SUM(" in sql.upper()
    assert "GROUP BY" in sql.upper()


def test_threshold_is_a_bound_parameter():
    """The threshold is bound, never interpolated — the builders take no float."""
    sql = ot.pooled_survivor_table_sql()
    assert "?" in sql
    assert "proportion_covered >=" in sql


def test_empty_result_schema_matches_the_woltka_projection():
    """The 0-row short-circuit and the real path must produce the SAME schema.
    They are separate SQL statements, so nothing but this stops them drifting —
    and a consumer reading the Parquet back would see the drift as a missing
    column on empty cohorts only.
    """
    empty = ot.empty_ogu_select_sql()
    real = ot.woltka_ogu_select_sql()
    for column in ot.OUTPUT_COLUMNS:
        assert column in empty, column
        assert column in real, column


def test_empty_result_casts_every_column_to_its_declared_type():
    """The empty path is GENERATED from `OUTPUT_SCHEMA`, so the declared type is the
    only place a type is written down. A hand-edited cast that disagreed with the
    real path would surface as a physical type mismatch between an empty-cohort
    export and a populated one — the empty path being the one exercised least.
    """
    sql = ot.empty_ogu_select_sql()
    for name, sql_type in ot.OUTPUT_SCHEMA.items():
        assert f"CAST(NULL AS {sql_type}) AS {name}" in sql


def test_empty_result_selects_no_rows():
    assert "WHERE false" in ot.empty_ogu_select_sql()


def test_coverage_filter_applies_only_above_zero():
    """One predicate decides both the SQL branch and whether the caller streams the
    reference lengths at all; a threshold of exactly 0 must take the skip path."""
    assert not ot.coverage_filter_applies(0.0)
    assert ot.coverage_filter_applies(1e-9)
    assert ot.coverage_filter_applies(1.0)


def test_alignment_columns_exclude_cigar():
    """`cigar` is the wide column the signed projection exists to leave out — this
    analytic derives breadth from `position`/`stop_position` and never reads it.
    Its absence is most of what the projection buys.
    """
    assert "cigar" not in ot.ALIGNMENT_COLUMNS


def test_alignment_table_binds_exactly_the_declared_columns():
    """One list, used for both the DoGet request and the SELECT, so the projection
    a caller signs and the columns it binds cannot drift."""
    sql = ot.alignment_table_sql("some_stream")
    for column in ot.ALIGNMENT_COLUMNS:
        assert column in sql, column
    assert "some_stream" in sql


def test_staging_builders_read_from_the_source_they_are_given():
    """Each staging builder reads from the caller's relation — a registered stream, a
    `read_parquet(...)` expression — because where the input comes from differs per
    consumer while the projection does not. Asserted as `FROM <source>` so the source
    has to land in the FROM clause rather than merely appear somewhere.
    """
    assert "FROM my_relation" in ot.alignment_table_sql("my_relation")
    assert "FROM read_parquet('m.parquet')" in ot.map_table_sql("read_parquet('m.parquet')")
    assert "FROM my_lengths l" in ot.genome_lengths_table_sql("my_lengths")


def test_map_table_renames_each_column_to_the_right_one():
    """`genome_coverage`'s `subject_genome_id` relation is `(contig_id, genome_id)`.
    Both consumers stage the map from a source keyed `(feature_idx, genome_idx)`, so
    the rename lives here rather than in each of them.

    Asserted as whole `X AS Y` fragments, not as four independent substrings: a
    SWAPPED rename (`feature_idx AS genome_id, genome_idx AS contig_id`) contains
    all four names and would satisfy a looser test, while silently corrupting every
    join keyed on them — the length roll-up, the survivor join, and the OGU key.
    """
    sql = ot.map_table_sql("src")
    assert "feature_idx AS contig_id" in sql
    assert "genome_idx AS genome_id" in sql
