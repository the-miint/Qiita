"""String-level tests for `qiita_common.analytic.gate` — the CIGAR identity /
query-coverage predicate, and the refusals that make a silent mis-gate impossible."""

from __future__ import annotations

import pytest

from qiita_common import analytic as ft


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
    """Safe only because both CIGAR scorers are permutation-invariant
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


def test_the_paired_count_asks_miint_rather_than_masking_the_flag():
    """`docs/duckdb-miint.md` says to use the `alignment_is_*` family and not to
    hand-roll bit math on `flags`. Pinned because both forms produce the same numbers,
    so nothing else in the suite would notice a regression to `flags & 1 <> 0`."""
    sql = ft.gate_diagnostics_sql(ft.AlignmentGate(min_identity=0.9))
    assert "alignment_is_paired(flags)" in sql
    assert "&" not in sql


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
