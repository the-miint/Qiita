"""Tests for `qiita_common.feature_table` — the shared SQL of the feature-table
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

import pytest

from qiita_common import feature_table as ft

# Every builder that emits a CREATE, with a representative call. Used by the
# sweeps below so a newly-added builder is covered by default rather than by
# somebody remembering to extend a list.
_CREATE_BUILDERS = {
    "alignment_table_sql": lambda: ft.alignment_table_sql("some_stream"),
    "map_table_sql": lambda: ft.map_table_sql("read_parquet('m.parquet')"),
    "genome_lengths_table_sql": lambda: ft.genome_lengths_table_sql("lengths_stream"),
    "coverage_alignments_view_sql": ft.coverage_alignments_view_sql,
    "survivor_table_sql(pooled)": lambda: ft.survivor_table_sql(ft.CoverageScope.POOLED),
    "survivor_table_sql(per-sample)": lambda: ft.survivor_table_sql(ft.CoverageScope.PER_SAMPLE),
    "ogu_input_table_sql": lambda: ft.ogu_input_table_sql(survivor_scope=ft.CoverageScope.POOLED),
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


def test_woltka_reads_its_source_as_a_quoted_relation_name():
    """`woltka_ogu` takes its source as a quoted string literal and resolves it on
    a SEPARATE connection, so the name is part of the contract — not a caller's
    choice — and the relation must exist by that name on the database."""
    assert f"'{ft.OGU_INPUT_TABLE}'" in ft.woltka_ogu_select_sql()


def test_woltka_output_is_per_sample():
    """`sample_id` is a NAMED argument; without it woltka returns
    `(feature_id, value)` and the whole per-sample dimension is lost."""
    assert "sample_id :=" in ft.woltka_ogu_select_sql()


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
    assert "CREATE VIEW" in ft.coverage_alignments_view_sql()
    assert "CREATE TABLE" in ft.ogu_input_table_sql(survivor_scope=ft.CoverageScope.POOLED)
    assert "CREATE TABLE" in ft.alignment_table_sql("s")


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


def test_coverage_denominator_does_not_depend_on_the_alignment():
    """The denominator is the genome's FULL length, including contigs nothing
    aligned to. Joining the alignment slice in here would silently narrow it to
    aligned-length-only — every genome's breadth would rise and low-coverage
    genomes would survive a threshold they should fail.
    """
    sql = ft.genome_lengths_table_sql("lengths_stream")
    assert ft.ALIGNMENT_TABLE not in sql
    assert ft.MAP_TABLE in sql  # rolls contig lengths up to the genome
    assert "SUM(" in sql.upper()
    assert "GROUP BY" in sql.upper()


def test_threshold_is_a_bound_parameter_in_both_scopes():
    """The threshold is bound, never interpolated — no builder takes a float. Both
    scopes compare with `>=`, so a genome sitting exactly on the threshold is kept
    (pinned behaviourally in the control-plane tier)."""
    for scope in ft.CoverageScope:
        sql = ft.survivor_table_sql(scope)
        assert "?" in sql, scope
        assert ">= ?" in sql, scope


def test_empty_result_schema_matches_the_woltka_projection():
    """The 0-row short-circuit and the real path must produce the SAME schema.
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


def test_coverage_filter_applies_only_above_zero():
    """One predicate decides both the SQL branch and whether the caller streams the
    reference lengths at all; a threshold of exactly 0 must take the skip path."""
    assert not ft.coverage_filter_applies(0.0)
    assert ft.coverage_filter_applies(1e-9)
    assert ft.coverage_filter_applies(1.0)


def test_alignment_columns_exclude_cigar():
    """`cigar` is the wide column the signed projection exists to leave out — this
    analytic derives breadth from `position`/`stop_position` and never reads it.
    Its absence is most of what the projection buys.
    """
    assert "cigar" not in ft.ALIGNMENT_COLUMNS


def test_alignment_table_binds_exactly_the_declared_columns():
    """One list, used for both the DoGet request and the SELECT, so the projection
    a caller signs and the columns it binds cannot drift."""
    sql = ft.alignment_table_sql("some_stream")
    for column in ft.ALIGNMENT_COLUMNS:
        assert column in sql, column
    assert "some_stream" in sql


def test_staging_builders_read_from_the_source_they_are_given():
    """Each staging builder reads from the caller's relation — a registered stream, a
    `read_parquet(...)` expression — because where the input comes from differs per
    consumer while the projection does not. Asserted as `FROM <source>` so the source
    has to land in the FROM clause rather than merely appear somewhere.
    """
    assert "FROM my_relation" in ft.alignment_table_sql("my_relation")
    assert "FROM read_parquet('m.parquet')" in ft.map_table_sql("read_parquet('m.parquet')")
    assert "FROM my_lengths l" in ft.genome_lengths_table_sql("my_lengths")


def test_map_table_renames_each_column_to_the_right_one():
    """`genome_coverage`'s `subject_genome_id` relation is `(contig_id, genome_id)`.
    Both consumers stage the map from a source keyed `(feature_idx, genome_idx)`, so
    the rename lives here rather than in each of them.

    Asserted as whole `X AS Y` fragments, not as four independent substrings: a
    SWAPPED rename (`feature_idx AS genome_id, genome_idx AS contig_id`) contains
    all four names and would satisfy a looser test, while silently corrupting every
    join keyed on them — the length roll-up, the survivor join, and the OGU key.
    """
    sql = ft.map_table_sql("src")
    assert "feature_idx AS contig_id" in sql
    assert "genome_idx AS genome_id" in sql


# ---------------------------------------------------------------------------
# The CIGAR identity / query-coverage gate
# ---------------------------------------------------------------------------


def test_gate_requires_at_least_one_threshold():
    """A gate with neither threshold filters nothing while still costing the `cigar`
    column on the wire — so it is a mistake, not a no-op, and is refused."""
    with pytest.raises(ValueError, match="threshold"):
        ft.AlignmentGate()


@pytest.mark.parametrize("bad", [-0.1, 1.1])
def test_gate_rejects_a_threshold_outside_zero_to_one(bad):
    """Both scorers return a proportion, so a threshold outside [0, 1] can only be a
    units mistake (a percentage, say) — and one above 1 would drop every row."""
    with pytest.raises(ValueError):
        ft.AlignmentGate(min_identity=bad)
    with pytest.raises(ValueError):
        ft.AlignmentGate(min_query_coverage=bad)


def test_only_a_gate_adds_cigar_to_the_projection():
    """`cigar` is the wide column the signed projection exists to leave out, so it
    rides ONLY when something reads it. The base list stays cigar-free — a permanent
    addition there would defeat the projection for every ungated caller."""
    assert "cigar" not in ft.ALIGNMENT_COLUMNS
    assert ft.gate_alignment_columns(None) == ft.ALIGNMENT_COLUMNS
    assert "cigar" in ft.gate_alignment_columns(ft.AlignmentGate(min_identity=0.9))


def test_only_a_paired_gate_asks_for_mate_position():
    """`mate_position` exists only to key the pooled partition, so an unpaired gate
    must not pay for it."""
    unpaired = ft.gate_alignment_columns(ft.AlignmentGate(min_identity=0.9))
    paired = ft.gate_alignment_columns(ft.AlignmentGate(min_identity=0.9, paired=True))
    assert "mate_position" not in unpaired
    assert "mate_position" in paired


def test_the_gated_relation_drops_cigar_again():
    """`cigar` is read by the gate and by nothing downstream, so the gated relation
    projects exactly `ALIGNMENT_COLUMNS` — the wide column stops there instead of
    propagating into the coverage view and woltka's input."""
    sql = ft.gated_alignment_table_sql(gate=ft.AlignmentGate(min_identity=0.9))
    projection = sql.split(" FROM ")[0]
    assert "cigar" not in projection
    for column in ft.ALIGNMENT_COLUMNS:
        assert column in projection, column


def test_an_unpaired_gate_filters_per_row_and_a_paired_gate_pools():
    """A single-end record has no mate to pool with, so it is judged on its own CIGAR
    by a plain WHERE — the pooled window over a one-row partition returns the same
    answer at the cost of a full blocking sort of every alignment."""
    unpaired = ft.gated_alignment_table_sql(gate=ft.AlignmentGate(min_identity=0.9))
    paired = ft.gated_alignment_table_sql(gate=ft.AlignmentGate(min_identity=0.9, paired=True))
    assert "WHERE" in unpaired and "QUALIFY" not in unpaired
    assert "string_agg" not in unpaired
    assert "QUALIFY" in paired and "string_agg(cigar, '')" in paired


def test_the_pooled_partition_keys_on_the_placement_not_just_the_read():
    """Pooling by read alone would merge a read's DISTINCT placements — two different
    features, or two positions on one feature — into a single CIGAR and score that
    concatenation, which is nobody's intended semantics.

    The key is the placement: the read, the feature, and the mate-symmetric
    coordinate pair. Mates store their own and their partner's coordinates in
    SWAPPED order, so LEAST/GREATEST give both mates the same key. An unpaired row
    has a NULL `mate_position` and both collapse to `position`, making it its own
    single-row partition.
    """
    sql = ft.gated_alignment_table_sql(gate=ft.AlignmentGate(min_identity=0.9, paired=True))
    partition = sql.split("PARTITION BY")[1]
    assert "sequence_idx" in partition
    assert "feature_idx" in partition
    assert "LEAST(position, mate_position)" in partition
    assert "GREATEST(position, mate_position)" in partition


def test_the_pooled_aggregate_has_no_order_by():
    """Deliberate: both CIGAR scorers are permutation-invariant, so which mate
    concatenates first cannot matter, and an ORDER BY would buy a sort inside every
    partition for nothing. If a build ever made a scorer position-dependent this gate
    would go nondeterministic — the same pair kept on one run and dropped the next —
    so the invariance is pinned in the orchestrator's suite, not assumed here.
    """
    sql = ft.gated_alignment_table_sql(gate=ft.AlignmentGate(min_identity=0.9, paired=True))
    pooled = sql[sql.index("string_agg") : sql.index("PARTITION BY")]
    assert "ORDER BY" not in pooled


def test_gate_thresholds_are_bound_parameters_in_predicate_order():
    """Two thresholds means two `?`, and a caller binding them in the wrong order
    would swap identity for coverage silently. The parameter list is built from the
    same ordered terms as the SQL, so it cannot disagree."""
    gate = ft.AlignmentGate(min_identity=0.95, min_query_coverage=0.8)
    sql = ft.gated_alignment_table_sql(gate=gate)
    assert sql.count("?") == 2
    assert sql.index("cigar_sequence_identity") < sql.index("cigar_query_coverage")
    assert ft.gate_parameters(gate) == [0.95, 0.8]


def test_a_query_coverage_only_gate_never_mentions_identity():
    """The two thresholds are independent. Asking only about coverage must not drag
    in an identity predicate — which matters because identity needs an eqx CIGAR and
    coverage does not."""
    sql = ft.gated_alignment_table_sql(gate=ft.AlignmentGate(min_query_coverage=0.9))
    assert "cigar_query_coverage" in sql
    assert "cigar_sequence_identity" not in sql
    assert ft.gate_parameters(ft.AlignmentGate(min_query_coverage=0.9)) == [0.9]


def test_diagnostics_count_scorable_rows_only_when_identity_is_gated():
    """Counting scorable rows means parsing every CIGAR, which is worth paying for
    only when an unscorable one would actually drop the row."""
    with_identity = ft.gate_diagnostics_sql(ft.AlignmentGate(min_identity=0.9))
    without = ft.gate_diagnostics_sql(ft.AlignmentGate(min_query_coverage=0.9))
    assert "cigar_sequence_identity" in with_identity
    assert "cigar_sequence_identity" not in without


def test_diagnostics_count_split_partitions_only_for_a_paired_gate():
    """A partition with a missing mate CIGAR is only a hazard when the gate pools."""
    paired = ft.gate_diagnostics_sql(ft.AlignmentGate(min_identity=0.9, paired=True))
    unpaired = ft.gate_diagnostics_sql(ft.AlignmentGate(min_identity=0.9))
    assert "GROUP BY" in paired
    assert "GROUP BY" not in unpaired


def test_check_refuses_a_slice_whose_every_cigar_is_unscorable():
    """`cigar_sequence_identity` is NULL on a CIGAR without `=`/`X` ops, and
    `NULL >= threshold` is NULL, so the gate would silently drop EVERY row and
    hand back an empty feature table that looks like a legitimate result.
    """
    with pytest.raises(ValueError, match="eqx|identity"):
        ft.check_gate_diagnostics(
            ft.AlignmentGate(min_identity=0.9),
            total_rows=1000,
            scorable_rows=0,
            degraded_partitions=0,
        )


def test_check_ignores_unscorable_rows_when_identity_is_not_gated():
    """Coverage is derivable from any CIGAR, so an unscorable-identity slice is fine
    for a coverage-only gate — refusing it would block a legitimate request."""
    ft.check_gate_diagnostics(
        ft.AlignmentGate(min_query_coverage=0.9),
        total_rows=1000,
        scorable_rows=0,
        degraded_partitions=0,
    )


def test_check_refuses_a_pooled_partition_missing_a_mate_cigar():
    """`string_agg` SKIPS NULLs, so a pair with one unmapped mate is scored on the
    other mate alone and silently judged as if it were a whole placement."""
    with pytest.raises(ValueError, match="mate"):
        ft.check_gate_diagnostics(
            ft.AlignmentGate(min_identity=0.9, paired=True),
            total_rows=1000,
            scorable_rows=1000,
            degraded_partitions=3,
        )


def test_check_ignores_split_partitions_for_an_unpaired_gate():
    """An unpaired gate never pools, so a NULL CIGAR just fails the predicate for its
    own row — no other row is affected."""
    ft.check_gate_diagnostics(
        ft.AlignmentGate(min_identity=0.9),
        total_rows=1000,
        scorable_rows=900,
        degraded_partitions=7,
    )


def test_check_passes_a_clean_slice():
    ft.check_gate_diagnostics(
        ft.AlignmentGate(min_identity=0.9, min_query_coverage=0.8, paired=True),
        total_rows=1000,
        scorable_rows=1000,
        degraded_partitions=0,
    )


def test_check_passes_an_empty_slice():
    """An empty slice is a legitimate result elsewhere in this analytic, so the
    all-unscorable check must not fire on 0 rows (0 of 0 is not evidence)."""
    ft.check_gate_diagnostics(
        ft.AlignmentGate(min_identity=0.9, paired=True),
        total_rows=0,
        scorable_rows=0,
        degraded_partitions=0,
    )
