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
from qiita_common.parquet import PARQUET_OPTS
from qiita_common.taxonomy import RANK_COLUMNS

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
    "ogu_output_table_sql": lambda: ft.ogu_output_table_sql(populated=True),
    "genome_label_table_sql": lambda: ft.genome_label_table_sql("mint_src"),
    "sample_label_table_sql": lambda: ft.sample_label_table_sql("mint_src"),
    "labelled_relation_sql": lambda: ft.labelled_relation_sql(clearance=ft.LabelClearance(rows=10)),
    "taxonomy_table_sql": lambda: ft.taxonomy_table_sql("taxonomy_stream"),
    "taxonomy_sidecar_sql": ft.taxonomy_sidecar_sql,
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


def _clear(gate: ft.AlignmentGate) -> ft.GateClearance:
    """A clearance for a clean slice, so a test that is about the SQL need not
    restate the diagnostics protocol."""
    return ft.check_gate_diagnostics(
        gate,
        total_rows=10,
        scorable_rows=10,
        unpoolable_partitions=0,
        paired_rows=10 if gate.paired else 0,
    )


def _gated_sql(gate: ft.AlignmentGate) -> str:
    return ft.gated_alignment_table_sql(clearance=_clear(gate))


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
    sql = _gated_sql(ft.AlignmentGate(min_identity=0.9))
    projection = sql.split(" FROM ")[0]
    assert "cigar" not in projection
    for column in ft.ALIGNMENT_COLUMNS:
        assert column in projection, column


def test_an_unpaired_gate_filters_per_row_and_a_paired_gate_pools():
    """A single-end record has no mate to pool with, so it is judged on its own CIGAR
    by a plain WHERE — the pooled window over a one-row partition returns the same
    answer at the cost of a full blocking sort of every alignment."""
    unpaired = _gated_sql(ft.AlignmentGate(min_identity=0.9))
    paired = _gated_sql(ft.AlignmentGate(min_identity=0.9, paired=True))
    assert "WHERE" in unpaired and "QUALIFY" not in unpaired
    assert "string_agg" not in unpaired
    assert "QUALIFY" in paired and "string_agg(cigar, '')" in paired


def test_the_pooled_partition_keys_on_the_placement_not_just_the_read():
    """The window must pool a PLACEMENT, not a read — see `PAIRED_PLACEMENT_PARTITION`
    for why each part of the key is there. Asserted here because the alternative
    (pooling by read) is valid SQL that scores a concatenation of unrelated
    placements; the behavioural half is in the control-plane suite.
    """
    sql = _gated_sql(ft.AlignmentGate(min_identity=0.9, paired=True))
    partition = sql.split("PARTITION BY")[1]
    assert "sequence_idx" in partition
    assert "feature_idx" in partition
    assert "LEAST(position, mate_position)" in partition
    assert "GREATEST(position, mate_position)" in partition


def test_the_pooled_aggregate_has_no_order_by():
    """Deliberate, and safe only because both CIGAR scorers are permutation-invariant
    — a property `docs/duckdb-miint.md` records as upstream-documented and
    qiita-verified, and which the orchestrator's suite pins. An ORDER BY would buy a
    sort inside every partition for nothing.
    """
    sql = _gated_sql(ft.AlignmentGate(min_identity=0.9, paired=True))
    pooled = sql[sql.index("string_agg") : sql.index("PARTITION BY")]
    assert "ORDER BY" not in pooled


def test_gate_thresholds_are_bound_parameters_in_predicate_order():
    """Two thresholds means two `?`, and a caller binding them in the wrong order
    would swap identity for coverage silently. The parameter list is built from the
    same ordered terms as the SQL, so it cannot disagree."""
    gate = ft.AlignmentGate(min_identity=0.95, min_query_coverage=0.8)
    sql = _gated_sql(gate)
    assert sql.count("?") == 2
    assert sql.index("cigar_sequence_identity") < sql.index("cigar_query_coverage")
    assert ft.gate_parameters(gate) == [0.95, 0.8]


def test_a_query_coverage_only_gate_never_mentions_identity():
    """The two thresholds are independent. Asking only about coverage must not drag
    in an identity predicate — which matters because identity needs an eqx CIGAR and
    coverage does not."""
    sql = _gated_sql(ft.AlignmentGate(min_query_coverage=0.9))
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


def _check(gate, *, total=1000, scorable=1000, unpoolable=0, paired_rows=None):
    """`check_gate_diagnostics` with the clean-slice defaults, so each test states only
    the count it is about. `paired_rows` defaults to what `gate.paired` implies."""
    return ft.check_gate_diagnostics(
        gate,
        total_rows=total,
        scorable_rows=scorable,
        unpoolable_partitions=unpoolable,
        paired_rows=(total if gate.paired else 0) if paired_rows is None else paired_rows,
    )


def test_check_refuses_a_slice_whose_every_cigar_is_unscorable():
    """`cigar_sequence_identity` is NULL on a CIGAR without `=`/`X` ops, and
    `NULL >= threshold` is NULL, so the gate would silently drop EVERY row and
    hand back an empty feature table that looks like a legitimate result.
    """
    with pytest.raises(ValueError, match="eqx|identity"):
        _check(ft.AlignmentGate(min_identity=0.9), scorable=0)


def test_check_ignores_unscorable_rows_when_identity_is_not_gated():
    """Coverage is derivable from any CIGAR, so an unscorable-identity slice is fine
    for a coverage-only gate — refusing it would block a legitimate request."""
    _check(ft.AlignmentGate(min_query_coverage=0.9), scorable=0)


def test_check_refuses_a_null_scorable_count_when_identity_is_gated():
    """The diagnostics SQL only emits NULL there when identity is NOT gated, so this
    combination means the row came from a different gate than the one being checked —
    a caller-side mix-up that would otherwise pass silently, since `None == 0` is
    False and the eqx check would simply not fire."""
    with pytest.raises(ValueError, match="NULL"):
        _check(ft.AlignmentGate(min_identity=0.9), scorable=None)


def test_check_refuses_a_pooled_partition_that_is_not_one_placement():
    """`string_agg` skips NULLs and concatenates whatever it is given, so a partition
    that is not exactly one placement's mates gets scored on part of a placement — or
    on unrelated alignments — and silently kept or dropped on that basis."""
    with pytest.raises(ValueError, match="placement"):
        _check(ft.AlignmentGate(min_identity=0.9, paired=True), unpoolable=3)


def test_check_ignores_unpoolable_partitions_for_an_unpaired_gate():
    """An unpaired gate never pools, so a NULL CIGAR just fails the predicate for its
    own row — no other row is affected."""
    _check(ft.AlignmentGate(min_identity=0.9), unpoolable=7)


def test_check_refuses_an_unpaired_gate_over_paired_data():
    """The hole a `paired` flag leaves open if nothing checks it: scoring each mate on
    its own CIGAR judges a placement's halves independently and orphans one when they
    disagree — silently, and precisely contrary to the guarantee pooling exists to
    give. SAM FLAG 0x1 is set on both mates whether or not either mapped, so it
    answers "is this paired data?" even when a mate's row never arrived.
    """
    with pytest.raises(ValueError, match="paired"):
        _check(ft.AlignmentGate(min_identity=0.9), paired_rows=400)


def test_check_allows_a_paired_gate_over_single_end_data():
    """The reverse is not an error: pooling single-end rows is correct — each is its
    own one-row partition — merely slower. That asymmetry is what makes
    `paired=True` the safe default for a caller that cannot tell."""
    _check(ft.AlignmentGate(min_identity=0.9, paired=True), paired_rows=0)


def test_check_passes_a_clean_slice_and_returns_a_clearance():
    """The clearance is the point: it is the only way to reach
    `gated_alignment_table_sql`, so passing the checks is what unlocks the gate."""
    gate = ft.AlignmentGate(min_identity=0.9, min_query_coverage=0.8, paired=True)
    clearance = _check(gate)
    assert isinstance(clearance, ft.GateClearance)
    assert clearance.gate is gate


def test_check_passes_an_empty_slice():
    """An empty slice is a legitimate result elsewhere in this analytic, so the
    all-unscorable check must not fire on 0 rows (0 of 0 is not evidence)."""
    _check(ft.AlignmentGate(min_identity=0.9, paired=True), total=0, scorable=0)


def test_the_clearance_carries_the_gate_and_the_cleanup_in_order():
    """Iterating `statements` is the whole rest of the protocol, so a caller cannot
    bind the wrong parameters to the predicate or forget to release the streamed copy
    that holds `cigar`."""
    gate = ft.AlignmentGate(min_identity=0.95, min_query_coverage=0.8)
    statements = _check(gate).statements
    assert len(statements) == 2
    assert statements[0][0].startswith(f"CREATE TABLE {ft.ALIGNMENT_TABLE}")
    assert statements[0][1] == [0.95, 0.8]
    assert statements[1][0] == f"DROP TABLE {ft.STREAMED_ALIGNMENT_TABLE}"
    assert statements[1][1] == []


def test_the_gate_sql_is_unreachable_without_a_clearance():
    """A gate alone will not do — the two silent failure modes make "check first" a
    constraint rather than a docstring."""
    with pytest.raises(TypeError):
        ft.gated_alignment_table_sql(gate=ft.AlignmentGate(min_identity=0.9))


@pytest.mark.parametrize("threshold", [0.0, 1.0])
def test_a_zero_or_one_threshold_is_honoured_not_treated_as_absent(threshold):
    """0.0 is falsy but not absent: it keeps only rows the scorer can score, which is
    a real filter (it drops NULL-identity rows). Every check in this module tests
    `is not None` for exactly this reason, and a "simplification" to truthiness would
    silently disable the gate."""
    gate = ft.AlignmentGate(min_identity=threshold)
    assert ft.gate_parameters(gate) == [threshold]
    assert "cigar_sequence_identity" in _gated_sql(gate)


def test_paired_and_both_thresholds_together_score_the_pooled_cigar():
    """The minimap2-shaped configuration: pooled, with identity AND coverage. Both
    predicates must score the POOLED CIGAR, not one pooled and one per-row — the
    latter would judge a pair on one mate's coverage."""
    gate = ft.AlignmentGate(min_identity=0.9, min_query_coverage=0.8, paired=True)
    sql = _gated_sql(gate)
    assert sql.count("string_agg(cigar, '')") == 2
    assert "QUALIFY" in sql
    assert ft.gate_parameters(gate) == [0.9, 0.8]


# ---------------------------------------------------------------------------
# The relabel to public identifiers
# ---------------------------------------------------------------------------


def _relabel_check(
    *,
    output_rows=100,
    labelled_rows=100,
    unlabelled_genome_rows=0,
    unlabelled_sample_rows=0,
    genomes=10,
    feature_ids=10,
    samples=5,
    sample_ids=5,
) -> ft.LabelClearance:
    """`check_relabel_diagnostics` over a clean row, with one field overridable per
    test — the defaults describe 100 rows of 10 genomes across 5 samples, all
    labelled and all labels distinct."""
    return ft.check_relabel_diagnostics(
        output_rows=output_rows,
        labelled_rows=labelled_rows,
        unlabelled_genome_rows=unlabelled_genome_rows,
        unlabelled_sample_rows=unlabelled_sample_rows,
        genomes=genomes,
        feature_ids=feature_ids,
        samples=samples,
        sample_ids=sample_ids,
    )


def test_the_public_schema_carries_no_internal_identifier():
    """The whole point of the relabel. `prep_sample_idx` and `genome_idx` are ours —
    they mean nothing outside this system and are not handles we promise to keep — so
    a published table names its columns with the minted `export_id` and its rows with
    the minted `export_feature_id`. Asserted on the schema rather than on one builder's text
    because this is the property every writer downstream inherits.
    """
    assert set(ft.LABELLED_SCHEMA) & set(ft.OUTPUT_SCHEMA) == {"value"}
    assert not [name for name in ft.LABELLED_SCHEMA if name.endswith("_idx")]


def test_both_public_identifiers_are_varchar():
    """BIOM requires `feature_id`/`sample_id` as VARCHAR while woltka hands back
    native BIGINTs, so the relabel is what makes the table writable at all — not a
    cosmetic rename. A relabel that carried an integer id through would fail at the
    writer, one phase later, on a table that looks otherwise complete.
    """
    assert ft.LABELLED_SCHEMA["sample_id"] == "VARCHAR"
    assert ft.LABELLED_SCHEMA["feature_id"] == "VARCHAR"


def test_the_labelled_table_projects_exactly_the_public_columns():
    """The projection is the enforcement: both `_idx` columns are joined ON and then
    dropped, so nothing downstream can read one out of this relation even by mistake.
    """
    sql = ft.labelled_relation_sql(clearance=_relabel_check())
    projection = sql.split(" FROM ", 1)[0]
    assert projection.endswith(", ".join(ft.LABELLED_COLUMNS))
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
    joined = ft._labelled_select_sql()
    assert joined in ft.relabel_diagnostics_sql()
    assert joined in ft.labelled_relation_sql(clearance=_relabel_check())


def test_the_labels_are_left_joined_so_a_missing_one_is_visible():
    """An INNER join would DROP a count whose genome or sample has no label, quietly
    shortening the table; LEFT keeps the row with a NULL id, which is what the
    diagnostics detect and refuse.
    """
    joined = ft._labelled_select_sql()
    assert joined.count("LEFT JOIN") == 2
    assert "JOIN" not in joined.replace("LEFT JOIN", "")


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


def test_the_diagnostics_measure_the_unlabelled_rows_of_both_axes():
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
        _relabel_check(output_rows=100, labelled_rows=140)


def test_check_refuses_an_unlabelled_genome():
    """Matched on the column that would go NULL, so the two unlabelled messages cannot
    stand in for each other."""
    with pytest.raises(ValueError, match="NULL feature_id"):
        _relabel_check(unlabelled_genome_rows=3)


def test_check_refuses_an_unlabelled_sample():
    with pytest.raises(ValueError, match="NULL sample_id"):
        _relabel_check(unlabelled_sample_rows=3)


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
    ("unlabelled", "distinct"),
    [
        ({"unlabelled_genome_rows": 1}, {"genomes": 10, "feature_ids": 9}),
        ({"unlabelled_sample_rows": 1}, {"samples": 5, "sample_ids": 4}),
    ],
)
def test_check_reports_unlabelled_rows_before_testing_for_a_collision(unlabelled, distinct):
    """`count(DISTINCT x)` skips NULLs, so an unlabelled row depresses the distinct-label
    count and looks EXACTLY like a collision — both conditions are true here. The
    unlabelled checks therefore run first, and this pins that order on both axes.

    Matched on `no public handle`, which only the unlabelled messages carry: the word
    "genome" appears in the collision message too, so a looser match would be satisfied
    by the very swap this test exists to catch.
    """
    with pytest.raises(ValueError, match="no public handle"):
        _relabel_check(**unlabelled, **distinct)


def test_check_passes_a_clean_join_and_returns_the_row_count():
    """The clearance carries what it cleared, so a caller can report the size of the
    table it is about to write without recounting it."""
    clearance = _relabel_check(output_rows=100, labelled_rows=100)
    assert isinstance(clearance, ft.LabelClearance)
    assert clearance.rows == 100


def test_check_passes_an_empty_table():
    """An empty cohort is a legitimate result — every genome dropped by the threshold,
    an all-16S reference — and has nothing to collide or go unlabelled."""
    assert (
        _relabel_check(
            output_rows=0, labelled_rows=0, genomes=0, feature_ids=0, samples=0, sample_ids=0
        ).rows
        == 0
    )


def test_the_relabel_sql_is_unreachable_without_a_clearance():
    """Same constraint as the gate's, for the same reason: the failures the checks
    catch are silent, so "check first" has to be a type error rather than a docstring.
    """
    with pytest.raises(TypeError):
        ft.labelled_relation_sql()


# ---------------------------------------------------------------------------
# Writing the table out
# ---------------------------------------------------------------------------


def test_the_public_schema_is_exactly_the_biom_writers_required_set():
    """miint's BIOM writer looks its three columns up BY NAME and **ignores every
    other column** (probed), so it would happily write a relation still carrying
    `genome_idx` — it is our projection, not the writer, that keeps our identifiers
    out of the file. It does insist on the types: `value` must be DOUBLE exactly, not
    FLOAT, DECIMAL or BIGINT.
    """
    assert ft.LABELLED_SCHEMA == {
        "sample_id": "VARCHAR",
        "feature_id": "VARCHAR",
        "value": "DOUBLE",
    }


@pytest.mark.parametrize("build", [ft.parquet_copy_sql, ft.biom_copy_sql])
def test_both_writers_copy_the_RELABELLED_relation(build, tmp_path):
    """Not `OGU_OUTPUT_TABLE`, which is the same numbers keyed by our own identifiers
    — the whole point of the relabel is that only one of these two relations may be
    written to a file a user publishes."""
    sql = build(tmp_path / "out")
    assert f"COPY {ft.LABELLED_RELATION} TO " in sql
    assert ft.OGU_OUTPUT_TABLE not in sql


def test_the_parquet_writer_uses_the_canonical_options(tmp_path):
    """One definition of the Parquet shape for every artifact qiita writes, so a
    version or compression bump moves in one place."""
    assert PARQUET_OPTS in ft.parquet_copy_sql(tmp_path / "t.parquet")


def test_the_biom_writer_names_qiita_as_the_generator(tmp_path):
    """The writer's own default is `miint`, which names the library rather than the
    system that produced the file. Compression is passed explicitly even though gzip
    is also the default — a published artifact's encoding should not change under us
    if an upstream default does.
    """
    sql = ft.biom_copy_sql(tmp_path / "t.biom")
    assert "FORMAT BIOM" in sql
    assert "COMPRESSION 'gzip'" in sql
    assert f"GENERATED_BY '{ft.BIOM_GENERATED_BY}'" in sql
    assert ft.BIOM_GENERATED_BY == "qiita"


def test_the_biom_writer_sets_no_table_id(tmp_path):
    """`ID` is left at the writer's default deliberately: the only distinctive handle
    for this table today is an internal identifier, which must not ride a published
    file, and a constant would identify nothing."""
    assert "ID '" not in ft.biom_copy_sql(tmp_path / "t.biom")


@pytest.mark.parametrize("build", [ft.parquet_copy_sql, ft.biom_copy_sql])
def test_a_copy_target_that_cannot_be_interpolated_safely_is_refused(build, tmp_path):
    """The COPY target is a SQL string literal, so it cannot be bound. Both writers
    validate rather than escape — and they do it themselves, since they are the site
    that interpolates."""
    with pytest.raises(ValueError):
        build(tmp_path / "the user's table.biom")


# ---------------------------------------------------------------------------
# The statement SEQUENCE — the part that is not text but order
# ---------------------------------------------------------------------------


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
    set is built — and the caller must not have to know that, which is the whole point
    of the scope-to-`None` conversion living here."""
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


@pytest.mark.parametrize("bad", [-0.001, 1.001, -1.0, 2.0])
def test_a_threshold_outside_zero_to_one_is_refused(bad):
    """Both directions are silent rather than loud if unchecked: below 0 reads as "no
    filter at all", above 1 drops every genome and yields an empty table that looks
    like a result. Each consumer validates at its own boundary; this is the shared
    backstop for the next one."""
    with pytest.raises(ValueError, match="proportion"):
        ft.coverage_filter_applies(bad)


def test_the_labelled_relation_is_a_view():
    """Its one reader is a COPY on the caller's own connection, so materializing would
    hold a second full copy of the output alive exactly while the writer builds its
    own — the same reason `cov_alignments` is a view."""
    sql = ft.labelled_relation_sql(clearance=ft.LabelClearance(rows=3))
    assert sql.startswith(f"CREATE VIEW {ft.LABELLED_RELATION} ")


def test_a_paired_gates_diagnostics_read_the_slice_once():
    """The slice still holds `cigar`, so a second scan of it is the most expensive
    avoidable thing in the gated path. Every count is additive over the placement
    partitions, so one grouped pass answers all four."""
    sql = ft.gate_diagnostics_sql(ft.AlignmentGate(min_identity=0.9, paired=True))
    assert sql.count(ft.STREAMED_ALIGNMENT_TABLE) == 1
    assert sql.count(f"GROUP BY {ft.PAIRED_PLACEMENT_PARTITION}") == 1


def test_the_paired_diagnostics_coalesce_their_sums():
    """`sum()` over zero groups is NULL, not 0 — so without these an empty slice would
    report `total_rows` as NULL and silently stop `check_gate_diagnostics`' zero-row
    early return from firing."""
    sql = ft.gate_diagnostics_sql(ft.AlignmentGate(min_query_coverage=0.5, paired=True))
    assert "coalesce(sum(rows_in_partition), 0) AS total_rows" in sql
    assert "coalesce(sum(paired), 0) AS paired_rows" in sql


# ---------------------------------------------------------------------------
# The taxonomy sidecar
# ---------------------------------------------------------------------------


def test_the_sidecar_carries_the_tables_own_join_key_and_no_lineage_string():
    """One published vocabulary across both files, so they join on one column — and
    eight rank columns rather than a joined string, which `concat_ws` would make lossy
    at exactly the rank a reader cares about."""
    assert ft.TAXONOMY_SIDECAR_COLUMNS[0] == "feature_id"
    assert set(ft.TAXONOMY_SIDECAR_COLUMNS[1:]) == set(RANK_COLUMNS)
    assert "lineage" not in ft.TAXONOMY_SIDECAR_COLUMNS
    assert not [name for name in ft.TAXONOMY_SIDECAR_COLUMNS if name.endswith("_idx")]


def test_the_sidecar_is_scoped_to_the_published_genomes_on_both_sides():
    """Both the row set and the cost: reducing a whole reference's taxonomy when only
    the survivors are published would be wrong AND expensive."""
    sql = ft.taxonomy_sidecar_sql()
    assert sql.count(ft.GENOME_LABEL_TABLE) == 2
    assert ft.MAP_TABLE in sql
    assert ft.TAXONOMY_TABLE in sql


def test_the_sidecar_restores_the_prefixes():
    sql = ft.taxonomy_sidecar_sql()
    assert "'d__' ||" in sql
    assert "'t__' ||" in sql


def test_the_sidecar_projection_quotes_the_keyword_ranks(tmp_path):
    """`class` and `order` are SQL keywords, and the sidecar names its columns for the
    ranks — so the COPY has to quote them while the Parquet column names stay bare."""
    sql = ft.taxonomy_copy_sql(tmp_path / "t.parquet", clearance=ft.TaxonomyClearance(rows=3))
    assert '"class"' in sql
    assert '"order"' in sql


def _taxonomy_check(**overrides):
    row = {
        "published_rows": 10,
        "taxonomy_rows": 10,
        "taxonomy_feature_ids": 10,
        "unnamed_rows": 0,
    } | overrides
    return ft.check_taxonomy_diagnostics(**row)


def test_a_clean_sidecar_clears_with_its_row_count():
    assert _taxonomy_check().rows == 10


def test_check_refuses_a_sidecar_shorter_than_its_table():
    """Which reads as though the missing rows were unclassified — and an unclassified
    genome is present here with NULL ranks, so the two would be indistinguishable."""
    with pytest.raises(ValueError, match="publishes 10"):
        _taxonomy_check(taxonomy_rows=9)


def test_check_refuses_a_duplicated_sidecar_row():
    with pytest.raises(ValueError, match="distinct"):
        _taxonomy_check(taxonomy_feature_ids=9)


def test_check_refuses_an_unnamed_sidecar_row():
    with pytest.raises(ValueError, match="no feature_id"):
        _taxonomy_check(unnamed_rows=1)


# ---------------------------------------------------------------------------
# What the roll-up leaves behind
# ---------------------------------------------------------------------------


def test_the_rollup_coverage_report_refuses_nothing():
    """A feature with no genome is not an error: a 16S record is not an OGU, and there
    is no genome-rooted row to emit for it. It is REPORTED because the alternative is a
    table that is quietly a fraction of what the caller streamed."""
    coverage = ft.check_rollup_coverage(alignment_rows=100, unmapped_rows=40, unmapped_features=7)
    assert not coverage.complete
    assert ft.check_rollup_coverage(
        alignment_rows=100, unmapped_rows=0, unmapped_features=0
    ).complete


def test_the_rollup_warning_names_the_share_and_what_is_missing():
    warning = ft.rollup_coverage_warning(
        ft.check_rollup_coverage(alignment_rows=100, unmapped_rows=40, unmapped_features=7)
    )
    assert "40 of 100" in warning
    assert "40.0%" in warning
    assert "7 features" in warning
