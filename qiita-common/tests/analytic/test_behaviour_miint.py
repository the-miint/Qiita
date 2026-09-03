"""Behavioural tests for `qiita_common.analytic` against REAL duckdb-miint.

The builders return SQL text and open no connection, so the sibling modules here
assert on strings; the analytic's behaviour is pinned in this one and in the
orchestrator's `test_estimate_feature_table.py`. The division: this module owns what
the analytic COMPUTES — including the per-sample coverage scope, which no server-side
caller uses, and the combined table's reconciliation; that one owns how its driver
feeds it.

Every test drives the shared builders end to end — the staging renames, the
lengths roll-up, the survivor set, the pre-woltka join, and `woltka_ogu` itself —
so a change to any link in that chain surfaces here rather than in a caller.

The scopes are not symmetric, and one of the two directions is impossible: pooling
unions every sample's intervals, so pooled breadth is always >= the best single
sample's. A genome can therefore survive pooled while every one of its samples
fails, but never the reverse — which is why the discriminating fixture below is
one-directional.
"""

from __future__ import annotations

import contextlib

import duckdb
import pytest

from qiita_common import analytic as ft
from qiita_common.duckdb_miint import miint_connect_config, miint_load_sql


def _miint_conn() -> duckdb.DuckDBPyConnection:
    """An in-memory connection with miint LOADed from the extension directory the
    conftest stages. LOAD-only, like every service-side connect: the INSTALL happens
    once per session there, not once per test."""
    conn = duckdb.connect(":memory:", config=miint_connect_config())
    conn.execute(miint_load_sql())
    return conn


# ---------------------------------------------------------------------------
# Fixture vocabulary. Contigs are `feature_idx`, genomes `genome_idx`.
#
#   G100: c10 (1000 bp)                      -> survives BOTH scopes (50%)
#   G200: c20 (10000 bp)                     -> the DISCRIMINATOR: two samples
#         cover extending halves, 0.6% each, 1.2% pooled
#   G300: c30 (1000) + c31 (2000, unaligned) -> 20/3000 = 0.67%, drops on FULL
#         length; would survive on aligned-length-only (20/1000 = 2%)
#   G400: c40 (1000) + c41 (1000)            -> one sample covers 300 bp of each,
#         600/2000 = 30% -> multi-contig summing
# ---------------------------------------------------------------------------

_MAP = [(10, 100), (20, 200), (30, 300), (31, 300), (40, 400), (41, 400)]
_LENGTHS = [(10, 1000), (20, 10000), (30, 1000), (31, 2000), (40, 1000), (41, 1000)]
_ALIGNMENT = [
    # (prep_sample_idx, sequence_idx, feature_idx, flags, position, stop_position)
    (1, 1, 10, 0, 0, 500),  # G100, sample 1: 50%
    (1, 2, 20, 0, 0, 60),  # G200, sample 1: 0.6%
    (2, 3, 20, 0, 60, 120),  # G200, sample 2: 0.6% (extends -> 1.2% pooled)
    (1, 4, 30, 0, 0, 20),  # G300, sample 1: 20 bp of 3000
    (1, 5, 40, 0, 0, 300),  # G400, sample 1: 300 of c40
    (1, 6, 41, 0, 0, 300),  # G400, sample 1: 300 of c41 -> 600/2000
]


def _values(rows: list[tuple], columns: str, casts: str) -> str:
    """A VALUES relation with explicit casts — the id columns must reach miint as
    native BIGINT (and `flags` as USMALLINT), never widened by inference.

    An empty `rows` still yields a correctly-TYPED 0-row relation: `VALUES` with no
    tuples is a parse error, so the empty case selects one casted row and filters it
    away. An empty input is a legitimate result in this analytic, so the scaffolding has
    to be able to express it.
    """
    if not rows:
        nulls = ", ".join(cast.replace("?", "NULL") for cast in casts.split(", "))
        return f"SELECT * FROM (SELECT {nulls}) AS t({columns}) WHERE false"
    tuples = ", ".join(f"({casts})" for _ in rows)
    return f"SELECT * FROM (VALUES {tuples}) AS t({columns})"


def _stage(conn, *, alignment, mapping, lengths, threshold, gate=None):
    """Stage the three inputs exactly as a consumer does, through the shared
    builders, from raw VALUES source relations.

    Alignment rows are 6-tuples, or 8-tuples carrying `(cigar, mate_position)` when a
    gate reads them; short rows are padded with NULLs.

    The alignment source is then **narrowed to `gate_alignment_columns(gate)`**, as
    the real DoGet does — a source wide enough to satisfy a SELECT the analytic never
    asked for would hide exactly the projection drift the signed column list exists
    to prevent, so an over-reaching builder fails to bind here instead.
    """
    padded = [tuple(r) + (None,) * (8 - len(r)) for r in alignment]
    conn.execute(
        "CREATE TABLE _align_src AS "
        + _values(
            padded,
            'prep_sample_idx, sequence_idx, feature_idx, flags, "position", stop_position,'
            " cigar, mate_position",
            "?::BIGINT, ?::BIGINT, ?::BIGINT, ?::USMALLINT, ?::BIGINT, ?::BIGINT,"
            " ?::VARCHAR, ?::BIGINT",
        ),
        [x for r in padded for x in r],
    )
    conn.execute(
        f"CREATE VIEW _align_projected AS "
        f"SELECT {', '.join(ft.gate_alignment_columns(gate))} FROM _align_src"
    )
    conn.execute(
        "CREATE TABLE _map_src AS "
        + _values(mapping, "feature_idx, genome_idx", "?::BIGINT, ?::BIGINT"),
        [x for r in mapping for x in r],
    )
    conn.execute(
        "CREATE TABLE _len_src AS "
        + _values(lengths, "feature_idx, sequence_length_bp", "?::BIGINT, ?::BIGINT"),
        [x for r in lengths for x in r],
    )
    if gate is None:
        conn.execute(ft.alignment_table_sql("_align_projected"))
    else:
        if gate.circular:
            # A circular gate reads a length per feature, so the lengths stream is
            # drained into the per-feature table and the roll-up below reads that —
            # exactly what the client-side recipe does when both are wanted.
            conn.execute(ft.feature_lengths_table_sql("_len_src"))
        conn.execute(ft.streamed_alignment_table_sql("_align_projected", gate=gate))
        cursor = conn.execute(ft.gate_diagnostics_sql(gate))
        names = [column[0] for column in cursor.description]
        # By NAME, as both consumers do — a renamed count is a TypeError here rather
        # than one number silently arriving as another's argument.
        clearance = ft.check_gate_diagnostics(
            gate, **dict(zip(names, cursor.fetchone(), strict=True))
        )
        # The clearance carries the rest of the protocol in order — the gate and the
        # release of the streamed copy holding `cigar`.
        for sql, params in clearance.statements:
            conn.execute(sql, params)
    conn.execute(ft.map_table_sql("_map_src"))
    if ft.coverage_filter_applies(threshold):
        # `feature_lengths` is gone by now — the clearance releases it — so the roll-up
        # reads the stream, as it does on every path that has no circular gate.
        conn.execute(ft.genome_lengths_table_sql("_len_src"))


def _ogu_input(
    conn, scope, threshold, *, alignment=_ALIGNMENT, mapping=_MAP, lengths=_LENGTHS, gate=None
) -> bool:
    """Stage the inputs and build woltka's input for `scope` at `threshold`, driving the
    SHARED statement sequence — the same one both consumers run, rather than a third
    hand-written copy of it. Returns whether the input has any rows, the caller's cue to
    run woltka or short-circuit."""
    _stage(
        conn, alignment=alignment, mapping=mapping, lengths=lengths, threshold=threshold, gate=gate
    )
    for sql, parameters in ft.ogu_input_statements(scope=scope, coverage_threshold=threshold):
        conn.execute(sql, parameters)
    return conn.execute(ft.ogu_input_count_sql()).fetchone()[0] > 0


def _table(
    scope, threshold, *, alignment=_ALIGNMENT, mapping=_MAP, lengths=_LENGTHS, gate=None
) -> list[tuple]:
    """Run the whole analytic for `scope` at `threshold` and return the feature
    table, sorted — keyed by our own identifiers, as the server-side consumer leaves
    it."""
    with _miint_conn() as conn:
        populated = _ogu_input(
            conn, scope, threshold, alignment=alignment, mapping=mapping, lengths=lengths, gate=gate
        )
        select = ft.woltka_ogu_select_sql() if populated else ft.empty_ogu_select_sql()
        return conn.execute(f"SELECT * FROM ({select}) ORDER BY 1, 2").fetchall()


def test_pooled_keeps_the_discriminating_genome_for_every_sample():
    """G200 clears 1% only when the two samples' extending intervals are POOLED
    (0.6% + 0.6% -> 1.2%), and pooled scope then keeps it for BOTH samples — even
    though neither sample covers 1% of it alone."""
    assert _table(ft.CoverageScope.POOLED, 0.01) == [
        (1, 100, 1.0),  # 50%, survives either way
        (1, 200, 1.0),  # kept for sample 1 on the COHORT's breadth
        (1, 400, 2.0),  # 30% multi-contig; TWO distinct reads, one per contig
        (2, 200, 1.0),  # kept for sample 2 likewise
    ]


def test_per_sample_drops_the_pair_no_single_sample_covers():
    """The same fixture under per-sample scope: G200 vanishes entirely, because
    0.6% < 1% in each sample separately. This is the one-directional discriminator
    — the rows a stricter scope removes — and it is what the scope is for."""
    assert _table(ft.CoverageScope.PER_SAMPLE, 0.01) == [
        (1, 100, 1.0),
        (1, 400, 2.0),
        # absent: (1, 200) and (2, 200) — each sample covers only 0.6% of G200
    ]


@pytest.mark.parametrize("threshold", [0.005, 0.01, 0.10, 0.30])
def test_the_two_scopes_agree_exactly_on_a_single_sample_cohort(threshold):
    """**The property the whole scope axis rests on.** With one sample in the cohort
    there is nothing to pool, so "breadth over the cohort" and "breadth in this
    sample" are the same question and the two scopes must return byte-identical
    tables — at every threshold.

    Pooled computes this inside miint's `genome_coverage`; per-sample reimplements
    the macro's method in our own SQL. Anything that makes the two disagree — a
    different denominator, a lost INNER JOIN, a missing DOUBLE cast, a different
    per-contig grouping — shows up here as a divergence, on a fixture that exercises
    a plain genome (G100), an unaligned-contig denominator (G300) and a multi-contig
    sum (G400) at once. Several narrower tests would each catch *some* of that; only
    this one states the general rule.

    Swept across thresholds because a divergence may only be visible where one scope
    lands on the far side of the cut from the other.
    """
    single_sample = [row for row in _ALIGNMENT if row[0] == 1]
    pooled = _table(ft.CoverageScope.POOLED, threshold, alignment=single_sample)
    per_sample = _table(ft.CoverageScope.PER_SAMPLE, threshold, alignment=single_sample)
    assert pooled == per_sample
    assert pooled, "fixture should not be empty at this threshold"


def test_joining_the_wrong_scopes_survivor_set_is_a_bind_error():
    """The scopes' survivor relations are named per scope, so building one and
    joining the other names a relation that does not exist.

    Without that, one direction is silently wrong rather than loud: a per-sample set
    joined on the genome alone fans every alignment row out once per sample the
    genome survived in, inflating its counts. This asserts the mismatch cannot get
    that far.
    """
    with _miint_conn() as conn:
        _stage(conn, alignment=_ALIGNMENT, mapping=_MAP, lengths=_LENGTHS, threshold=0.01)
        conn.execute(ft.coverage_alignments_view_sql())
        conn.execute(ft.survivor_table_sql(ft.CoverageScope.PER_SAMPLE), [0.01])

        with pytest.raises(duckdb.Error, match=ft.survivor_table_name(ft.CoverageScope.POOLED)):
            conn.execute(ft.ogu_input_table_sql(survivor_scope=ft.CoverageScope.POOLED))


@pytest.mark.parametrize("scope", list(ft.CoverageScope))
def test_the_denominator_is_the_full_genome_length_in_both_scopes(scope):
    """G300 is 20 bp over c30 (1000) + c31 (2000, nothing aligned) = 0.67% < 1%, so
    it must be ABSENT. On an aligned-contigs-only denominator it would be 2% and
    survive — its absence is what proves the unaligned contig counts, and the
    per-sample scope must not quietly use a different denominator than pooled.
    """
    assert not [r for r in _table(scope, 0.01) if r[1] == 300]


def test_per_sample_merges_intervals_within_a_contig_not_across_them():
    """`compress_intervals` merges within one coordinate space, so a genome's covered
    bases are the sum over its contigs — and the threshold here is chosen to make
    that discriminating rather than incidental.

    Sample 1 covers [0, 300) on each of two 1000 bp contigs: 600/2000 = **30%**,
    which clears a 20% threshold. Had the per-sample form grouped straight to the
    genome, the two identical [0, 300) spans would have merged as though they shared
    coordinates, giving 300/2000 = **15%** — below the threshold, so the genome
    would vanish. Its presence is the assertion.
    """
    rows = _table(
        ft.CoverageScope.PER_SAMPLE,
        0.20,
        alignment=[(1, 1, 40, 0, 0, 300), (1, 2, 41, 0, 0, 300)],
        mapping=[(40, 400), (41, 400)],
        lengths=[(40, 1000), (41, 1000)],
    )
    # Two DISTINCT reads, each wholly within G400, so woltka counts 2.0.
    assert rows == [(1, 400, 2.0)]


@pytest.mark.parametrize("scope", list(ft.CoverageScope))
def test_a_genome_exactly_at_the_threshold_survives(scope):
    """`>=`, not `>`. A genome sitting precisely on the threshold is KEPT — pinned
    because both scopes write the comparison independently (the pooled one inside
    miint's macro, the per-sample one in our SQL) and nothing else would catch the
    two disagreeing at the boundary.
    """
    # 10 bp covered of a 1000 bp genome = exactly 0.01.
    rows = _table(
        scope,
        0.01,
        alignment=[(1, 1, 10, 0, 0, 10)],
        mapping=[(10, 100)],
        lengths=[(10, 1000)],
    )
    assert rows == [(1, 100, 1.0)]


@pytest.mark.parametrize("scope", list(ft.CoverageScope))
def test_threshold_zero_admits_everything_regardless_of_scope(scope):
    """At 0 there is no survivor set, so the scope cannot matter — every genome with
    any alignment is admitted. G200 and G300, dropped at 1% under both scopes, are
    present here."""
    genomes = {r[1] for r in _table(scope, 0.0)}
    assert genomes == {100, 200, 300, 400}


def test_per_sample_renormalizes_a_multi_mapped_read_onto_that_samples_survivor():
    """The pre-woltka join ordering, under the per-sample scope.

    One read hits a genome that survives for this sample and one that does not.
    Because the non-surviving PAIR leaves woltka's input, the read has a single
    distinct reference left and counts 1.0 — not the 0.5 it would be stranded at if
    the survivor filter ran on woltka's output instead.
    """
    rows = _table(
        ft.CoverageScope.PER_SAMPLE,
        0.01,
        # read 1 -> c10 (G100, 600/1000 = 60%, survives) and c20 (G200, 30/10000 =
        # 0.3%, drops for this sample).
        alignment=[(1, 1, 10, 0, 0, 600), (1, 1, 20, 0, 0, 30)],
        mapping=[(10, 100), (20, 200)],
        lengths=[(10, 1000), (20, 10000)],
    )
    assert rows == [(1, 100, 1.0)]


def test_empty_and_populated_paths_agree_on_column_types():
    """The 0-row short-circuit must match the real path in TYPE, not just in column
    name — a consumer reconciling an empty-cohort export against a populated one
    would otherwise hit a physical type mismatch. `OUTPUT_SCHEMA` declares the
    types; this is what checks the declaration against what woltka actually
    returns.
    """
    with _miint_conn() as conn:
        _stage(conn, alignment=_ALIGNMENT, mapping=_MAP, lengths=_LENGTHS, threshold=0.0)
        conn.execute(ft.ogu_input_table_sql(survivor_scope=None))

        def described(select: str) -> list[tuple[str, str]]:
            return [(r[0], r[1]) for r in conn.execute(f"DESCRIBE {select}").fetchall()]

        real = described(ft.woltka_ogu_select_sql())
        empty = described(ft.empty_ogu_select_sql())

    assert real == empty
    assert real == [(name, sql_type) for name, sql_type in ft.OUTPUT_SCHEMA.items()]


# ---------------------------------------------------------------------------
# The CIGAR identity / query-coverage gate, against real miint
#
# CIGAR vocabulary used below (probed on the mirror build):
#   '150='      identity 1.00, qcov 1.00   — a clean end-to-end hit
#   '75=75X'    identity 0.50, qcov 1.00   — half the columns mismatch
#   '150M'      identity NULL, qcov 1.00   — non-eqx: identity is not derivable
#   '10S140M'   identity NULL, qcov 0.93   — non-eqx AND soft-clipped
# ---------------------------------------------------------------------------

_GATE = ft.AlignmentGate(min_identity=0.9)


def test_the_gate_drops_a_low_identity_alignment():
    """Two reads on one genome, one clean and one at 50% identity. Only the clean read
    is counted — the gate removes the other before woltka ever sees it."""
    rows = _table(
        ft.CoverageScope.POOLED,
        0.0,
        alignment=[
            (1, 1, 10, 0, 0, 150, "150=", None),
            (1, 2, 10, 0, 150, 300, "75=75X", None),
        ],
        mapping=[(10, 100)],
        lengths=[(10, 1000)],
        gate=_GATE,
    )
    assert rows == [(1, 100, 1.0)]


def test_the_gate_filters_coverage_as_well_as_counts():
    """A gated-out alignment must not contribute covered bases either.

    G200's only alignment is a 50%-identity hit spanning 60% of the genome. Ungated
    it clears a 10% breadth threshold; gated there is nothing left to cover with, so
    the genome disappears. If the gate only filtered woltka's input and not the
    coverage view, G200 would survive the threshold and still be counted.
    """
    fixture = dict(
        alignment=[
            (1, 1, 10, 0, 0, 500, "150=", None),  # G100, clean
            (1, 2, 20, 0, 0, 600, "75=75X", None),  # G200, fails the gate
        ],
        mapping=[(10, 100), (20, 200)],
        lengths=[(10, 1000), (20, 1000)],
    )
    ungated = _table(ft.CoverageScope.POOLED, 0.10, **fixture)
    gated = _table(ft.CoverageScope.POOLED, 0.10, **fixture, gate=_GATE)
    assert {r[1] for r in ungated} == {100, 200}
    assert {r[1] for r in gated} == {100}


def test_a_paired_placement_is_kept_or_dropped_as_a_unit():
    """Pooling judges a placement, not a mate. One mate at 1.00 and one at 0.50 pool
    to 0.75 — below a 0.9 gate — so BOTH rows go. Scored per row instead, the clean
    mate would survive and the read would be counted on a half-placement.
    """
    paired = ft.AlignmentGate(min_identity=0.9, paired=True)
    rows = _table(
        ft.CoverageScope.POOLED,
        0.0,
        # One placement of read 1 on feature 10: mates at 100 and 200, coordinates
        # stored in swapped order as a real aligner emits them.
        alignment=[
            (1, 1, 10, 99, 100, 250, "150=", 200),
            (1, 1, 10, 147, 200, 350, "75=75X", 100),
        ],
        mapping=[(10, 100)],
        lengths=[(10, 1000)],
        gate=paired,
    )
    assert rows == []


def test_a_paired_placement_that_pools_above_the_floor_is_kept_whole():
    """The other side of the same rule: two mates that pool to 1.0 both survive, and
    the read is counted once."""
    paired = ft.AlignmentGate(min_identity=0.9, paired=True)
    rows = _table(
        ft.CoverageScope.POOLED,
        0.0,
        alignment=[
            (1, 1, 10, 99, 100, 250, "150=", 200),
            (1, 1, 10, 147, 200, 350, "150=", 100),
        ],
        mapping=[(10, 100)],
        lengths=[(10, 1000)],
        gate=paired,
    )
    # 2.0, not 1.0: woltka counts per SEGMENT, and these flags mark the two rows as
    # first/second in the template. See the `flags` note in docs/duckdb-miint.md.
    assert rows == [(1, 100, 2.0)]


def test_a_reads_distinct_placements_are_judged_separately():
    """`feature_idx` is in the partition key, so a read placed on two features is two
    placements — one may pass and the other fail.

    Pooling by read alone would concatenate all four CIGARs into one score and decide
    both placements together, which is the bug the key exists to prevent: here the
    pooled-by-read identity would be 0.75 and BOTH genomes would vanish.
    """
    paired = ft.AlignmentGate(min_identity=0.9, paired=True)
    rows = _table(
        ft.CoverageScope.POOLED,
        0.0,
        alignment=[
            (1, 1, 10, 99, 100, 250, "150=", 200),  # placement A on feature 10: clean
            (1, 1, 10, 147, 200, 350, "150=", 100),
            (1, 1, 20, 99, 100, 250, "75=75X", 200),  # placement B on feature 20: fails
            (1, 1, 20, 147, 200, 350, "75=75X", 100),
        ],
        mapping=[(10, 100), (20, 200)],
        lengths=[(10, 1000), (20, 1000)],
        gate=paired,
    )
    # Placement A survives alone, so its two segments land wholly on G100 — not
    # split 0.5/0.5 across G100 and G200.
    assert rows == [(1, 100, 2.0)]


def test_an_all_non_eqx_slice_is_refused_rather_than_silently_emptied():
    """Every CIGAR is `150M`, so `cigar_sequence_identity` is NULL throughout and the
    gate would drop every row, returning an empty table that looks like a real
    result. The diagnostics catch it first and say what to do."""
    with pytest.raises(ValueError, match="eqx"):
        _table(
            ft.CoverageScope.POOLED,
            0.0,
            alignment=[(1, 1, 10, 0, 0, 150, "150M", None)],
            mapping=[(10, 100)],
            lengths=[(10, 1000)],
            gate=_GATE,
        )


def test_a_query_coverage_gate_works_on_a_non_eqx_cigar():
    """The refinement that makes the refusal above survivable: coverage is derivable
    from ANY CIGAR. The same `150M` slice an identity gate cannot judge passes a
    coverage gate, and a soft-clipped `10S140M` at 0.93 fails a 0.95 floor.
    """
    rows = _table(
        ft.CoverageScope.POOLED,
        0.0,
        alignment=[
            (1, 1, 10, 0, 0, 150, "150M", None),  # qcov 1.00 -> kept
            (1, 2, 10, 0, 150, 300, "10S140M", None),  # qcov 0.93 -> dropped
        ],
        mapping=[(10, 100)],
        lengths=[(10, 1000)],
        gate=ft.AlignmentGate(min_query_coverage=0.95),
    )
    assert rows == [(1, 100, 1.0)]


def test_a_pair_missing_a_mate_cigar_is_refused_under_the_paired_gate():
    """`string_agg` skips NULLs, so this placement would be scored on its surviving
    mate alone — 1.0 from a half-placement — and silently kept. Refused instead, with
    the unpaired gate offered as the way through."""
    paired = ft.AlignmentGate(min_identity=0.9, paired=True)
    with pytest.raises(ValueError, match="mate"):
        _table(
            ft.CoverageScope.POOLED,
            0.0,
            alignment=[
                (1, 1, 10, 99, 100, 250, "150=", 200),
                (1, 1, 10, 147, 200, 350, None, 100),  # mate row present, no CIGAR
            ],
            mapping=[(10, 100)],
            lengths=[(10, 1000)],
            gate=paired,
        )


def test_cigar_does_not_reach_the_gated_relation():
    """The wide column stops at the gate. Downstream reads `ALIGNMENT_TABLE`, which
    carries exactly `ALIGNMENT_COLUMNS` — so the coverage view and woltka's input
    never pay for it, and the streamed copy that held it is dropped."""
    with _miint_conn() as conn:
        _stage(
            conn,
            alignment=[(1, 1, 10, 0, 0, 150, "150=", None)],
            mapping=[(10, 100)],
            lengths=[(10, 1000)],
            threshold=0.0,
            gate=_GATE,
        )
        columns = [r[0] for r in conn.execute(f"DESCRIBE {ft.ALIGNMENT_TABLE}").fetchall()]
        assert columns == list(ft.ALIGNMENT_COLUMNS)
        assert "cigar" not in columns
        staged = conn.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE table_name = ?",
            [ft.STREAMED_ALIGNMENT_TABLE],
        ).fetchone()[0]
        assert staged == 0, "the streamed copy holding cigar should be dropped"


def test_a_placement_whose_mate_row_is_absent_is_refused():
    """The half of the missing-mate hazard that a NULL-CIGAR check cannot see.

    Here the second mate's row is not in the slice AT ALL — the surviving row still
    claims a mapped mate via `mate_position`, but its partition holds one row, so
    `count(*) = count(cigar)` and the partition looks complete. `string_agg` would
    score that single mate as though it were the whole placement.

    A client streaming arbitrary alignment rows has no protection here: the
    orchestrator's aligner runs under `no_discordant`/`no_mixed`, so it never emits a
    lone mate, but an externally-produced alignment can.
    """
    paired = ft.AlignmentGate(min_identity=0.9, paired=True)
    with pytest.raises(ValueError, match="placement"):
        _table(
            ft.CoverageScope.POOLED,
            0.0,
            alignment=[(1, 1, 10, 99, 100, 250, "150=", 200)],  # mate at 200 never arrives
            mapping=[(10, 100)],
            lengths=[(10, 1000)],
            gate=paired,
        )


def test_an_unpaired_gate_over_paired_data_is_refused():
    """Scoring each mate on its own CIGAR judges a placement's halves independently
    and orphans one when they disagree. The refusal is what makes `paired` safe to get
    wrong: it cannot be silently wrong in the cheap direction."""
    with pytest.raises(ValueError, match="paired"):
        _table(
            ft.CoverageScope.POOLED,
            0.0,
            alignment=[
                (1, 1, 10, 99, 100, 250, "150=", 200),
                (1, 1, 10, 147, 200, 350, "150=", 100),
            ],
            mapping=[(10, 100)],
            lengths=[(10, 1000)],
            gate=ft.AlignmentGate(min_identity=0.9),  # NOT paired
        )


def test_a_paired_gate_over_single_end_data_is_allowed():
    """The asymmetry that makes `paired=True` the safe choice when a caller cannot
    tell: pooling single-end rows is correct, each being its own one-row partition."""
    rows = _table(
        ft.CoverageScope.POOLED,
        0.0,
        alignment=[(1, 1, 10, 0, 0, 150, "150=", None)],  # flags 0: not paired
        mapping=[(10, 100)],
        lengths=[(10, 1000)],
        gate=ft.AlignmentGate(min_identity=0.9, paired=True),
    )
    assert rows == [(1, 100, 1.0)]


def test_a_gated_EMPTY_slice_clears_instead_of_erroring():
    """The paired diagnostics aggregate over placement partitions, and `sum()` over zero
    groups is NULL rather than 0 — so an empty slice would report a NULL `total_rows` and
    silently skip the zero-row early return, reaching the checks with nothing to judge.
    An empty slice is a legitimate result here (a cohort whose reads all failed
    upstream), so it has to clear the gate and produce an empty table.
    """
    assert (
        _table(
            ft.CoverageScope.POOLED,
            0.0,
            alignment=[],
            mapping=[(10, 100)],
            lengths=[(10, 1000)],
            gate=ft.AlignmentGate(min_identity=0.9, paired=True),
        )
        == []
    )


def test_the_analytic_releases_the_relations_it_finishes_with():
    """The memory claim, checked rather than asserted in prose: after woltka's input is
    built, the alignment slice and the coverage machinery are gone — and after the counts
    are materialized, so is woltka's input. On a large cohort these are the biggest
    relations in the pipeline, and they would otherwise live until the connection closes.
    """
    scope = ft.CoverageScope.POOLED
    with _miint_conn() as conn:
        populated = _ogu_input(conn, scope, 0.01)
        surviving = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        assert ft.OGU_INPUT_TABLE in surviving
        assert not surviving & {
            ft.ALIGNMENT_TABLE,
            ft.COVERAGE_ALIGNMENTS_VIEW,
            ft.survivor_table_name(scope),
        }

        conn.execute(ft.ogu_output_table_sql(populated=populated))
        conn.execute(ft.drop_ogu_input_table_sql())
        after = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    assert ft.OGU_OUTPUT_TABLE in after
    assert ft.OGU_INPUT_TABLE not in after


def test_a_paired_gate_with_both_thresholds_scores_the_pooled_cigar():
    """The minimap2-shaped configuration — pooled, identity AND coverage — which no
    other test exercises. The mates pool to identity 1.0 and coverage 0.93, so a 0.95
    coverage floor drops the whole placement even though its identity is perfect.
    """
    rows = _table(
        ft.CoverageScope.POOLED,
        0.0,
        alignment=[
            (1, 1, 10, 99, 100, 250, "150=", 200),
            (1, 1, 10, 147, 200, 350, "10S140M", 100),
        ],
        mapping=[(10, 100)],
        lengths=[(10, 1000)],
        gate=ft.AlignmentGate(min_identity=0.5, min_query_coverage=0.95, paired=True),
    )
    assert rows == []


# ---------------------------------------------------------------------------
# The circular gate, against real miint
#
# The fixture is one 30 kb contig (feature 10, genome 100) and a 6 kb read the
# linearisation cut in two: `3000=3000S` ending where the contig does, `3000H3000=`
# starting at its origin. Each record explains half the read; together they explain all
# of it. `_INTERIOR_READ` is the same read placed away from the origin, in one record.
# ---------------------------------------------------------------------------

_CIRCULAR_LENGTHS = [(10, 30_000)]
_CIRCULAR_MAP = [(10, 100)]
_JUNCTION_READ = [
    (1, 1, 10, 0, 27_000, 30_000, "3000=3000S", None),
    (1, 1, 10, 2048, 0, 3_000, "3000H3000=", None),
]
_INTERIOR_READ = [(1, 2, 10, 0, 10_000, 16_000, "6000=", None)]


def _gated_reads(alignment, *, gate) -> list[tuple]:
    """The `(sequence_idx, position)` the gate left in `ALIGNMENT_TABLE`."""
    with _miint_conn() as conn:
        _stage(
            conn,
            alignment=alignment,
            mapping=_CIRCULAR_MAP,
            lengths=_CIRCULAR_LENGTHS,
            threshold=0.0,
            gate=gate,
        )
        return conn.execute(
            f"SELECT sequence_idx, position FROM {ft.ALIGNMENT_TABLE} ORDER BY 1, 2"
        ).fetchall()


def test_the_circular_gate_keeps_the_split_read_a_per_record_gate_drops():
    """The whole point of the mode, on one fixture: at the same 0.90 coverage floor, the
    per-record gate discards both halves of the junction read — each explains 0.5 of it —
    while the circular gate keeps both, because the read is fully explained by the
    reference it came from. The interior read survives either way."""
    per_record = ft.AlignmentGate(min_query_coverage=0.90, paired=True)
    circular = ft.AlignmentGate(circular=True, circular_min_identity=None)
    reads = _JUNCTION_READ + _INTERIOR_READ

    assert _gated_reads(reads, gate=per_record) == [(2, 10_000)]
    assert _gated_reads(reads, gate=circular) == [(1, 0), (1, 27_000), (2, 10_000)]


def test_the_circular_gate_keeps_or_drops_a_read_whole():
    """It judges the read, so every record of a cleared group survives and none of a
    failing one does. Half a junction read is not a placement, and leaving one fragment
    behind would count the read on evidence the gate rejected."""
    half = [_JUNCTION_READ[0]]  # one 3 kb record of the 6 kb read: coverage 0.5
    assert (
        _gated_reads(half, gate=ft.AlignmentGate(circular=True, circular_min_identity=None)) == []
    )


def test_the_circular_gate_scores_identity_pooled_over_the_records():
    """One mismatch in 6000 aligned columns clears 0.95 and even 0.999; a record scored
    on its own would put the mismatch in a 3000-column denominator. The threshold is a
    parameter, so this asserts the number that is actually compared, not just a side."""
    with_mismatch = [
        _JUNCTION_READ[0],
        (1, 1, 10, 2048, 0, 3_000, "3000H2999=1X", None),
    ]
    kept = ft.AlignmentGate(circular=True, circular_min_identity=0.999)
    dropped = ft.AlignmentGate(circular=True, circular_min_identity=0.9999)
    assert _gated_reads(with_mismatch, gate=kept) == [(1, 0), (1, 27_000)]
    assert _gated_reads(with_mismatch, gate=dropped) == []


def test_the_circular_gate_drops_fragments_on_opposite_strands():
    """Same coverage, same identity, not a wrap: an inverted repeat or a chimera the
    aligner split. `NOT mixed_strand` is what separates them, and it is not a threshold
    a caller can lower."""
    inverted = [_JUNCTION_READ[0], (1, 1, 10, 2064, 0, 3_000, "3000=3000S", None)]
    assert _gated_reads(inverted, gate=ft.AlignmentGate(circular=True)) == []


def test_a_read_whose_records_disagree_about_the_cigar_encoding_is_refused():
    """The diagnostic has to ask the question the gate asks. Pooled identity is NULL when
    a read's records mix `M` with `=`/`X`, so that read is dropped WHOLE — including the
    record that did score. Counted per record, three of these four rows are scorable and
    nothing refuses; counted per read, one group has no identity and this raises.
    """
    mixed = [
        (1, 1, 10, 0, 27_000, 30_000, "3000=3000S", None),
        (1, 1, 10, 2048, 0, 3_000, "3000H3000=", None),
        (1, 2, 10, 0, 20_000, 23_000, "3000=3000S", None),  # scorable on its own
        (1, 2, 10, 2048, 10_000, 13_000, "3000H3000M", None),  # legacy M poisons the read
    ]
    with pytest.raises(ValueError, match="poolable sequence identity"):
        _gated_reads(mixed, gate=ft.AlignmentGate(circular=True))

    # Without the identity term there is nothing to be unscorable about, and both reads
    # are judged on coverage and strand alone.
    no_identity = ft.AlignmentGate(circular=True, circular_min_identity=None)
    assert _gated_reads(mixed, gate=no_identity) == [
        (1, 0),
        (1, 27_000),
        (2, 10_000),
        (2, 20_000),
    ]


def test_the_circular_diagnostics_count_reads_not_records():
    """The count the refusal above reads. One read scorable, one not — as READS; the
    per-record count would say 3 of 4 rows can be scored and clear the slice."""
    mixed = [
        (1, 1, 10, 0, 27_000, 30_000, "3000=3000S", None),
        (1, 1, 10, 2048, 0, 3_000, "3000H3000=", None),
        (1, 2, 10, 0, 20_000, 23_000, "3000=3000S", None),
        (1, 2, 10, 2048, 10_000, 13_000, "3000H3000M", None),
    ]
    gate = ft.AlignmentGate(circular=True)
    with _miint_conn() as conn:
        conn.execute(
            "CREATE TABLE _src AS "
            + _values(
                mixed,
                'prep_sample_idx, sequence_idx, feature_idx, flags, "position",'
                " stop_position, cigar, mate_position",
                "?::BIGINT, ?::BIGINT, ?::BIGINT, ?::USMALLINT, ?::BIGINT, ?::BIGINT,"
                " ?::VARCHAR, ?::BIGINT",
            ),
            [x for r in mixed for x in r],
        )
        conn.execute(ft.streamed_alignment_table_sql("_src", gate=gate))
        cursor = conn.execute(ft.gate_diagnostics_sql(gate))
        row = dict(zip([c[0] for c in cursor.description], cursor.fetchone(), strict=True))
    assert row["total_rows"] == 4
    assert row["unscorable_groups"] == 1
    # The per-record figure is not what this axis asks, so it is not reported at all.
    assert row["scorable_rows"] is None


def test_a_circular_gate_scores_a_secondary_on_its_own_cigar():
    """`circular_query_coverage` never sees a secondary record, so the circular gate
    judges it on the CIGAR axis instead of refusing the slice. A secondary is how a read
    says it also placed elsewhere, which is what woltka splits a count across.

    Both directions, on one fixture: the full-length secondary clears the same
    thresholds on its own span and is kept; the clipped one explains a third of its read
    and is dropped. Neither outcome is reachable through the pooled arm — the macro
    emits no group for either.

    Sequences 3 and 4 are secondary-ONLY groups, which is what makes this test cover the
    diagnostics' `poolable > 0` filter as well: without it their pooled identity is NULL,
    they count as unscorable groups, and the slice is refused before any of the above."""
    full_length = (1, 3, 10, 256, 20_000, 26_000, "6000=", None)
    clipped = (1, 4, 10, 256, 40_000, 42_000, "2000=4000S", None)
    assert _gated_reads(
        _INTERIOR_READ + [full_length, clipped], gate=ft.AlignmentGate(circular=True)
    ) == [(2, 10_000), (3, 20_000)]


def test_a_clearing_secondary_on_its_primarys_contig_is_kept_exactly_once():
    """The case both arms could claim: a secondary sharing `(read, is_read1, reference)`
    with a cleared primary, which also clears on its own CIGAR. Arm 1 would take it for
    its primary's clearance and arm 2 for its own score, so the arms have to partition
    the slice rather than merely both be correct. List equality, so a duplicate fails —
    a read counted twice reaches coverage and woltka as two placements."""
    same_key_and_clears = (1, 2, 10, 256, 20_000, 26_000, "6000=", None)
    assert _gated_reads(
        _INTERIOR_READ + [same_key_and_clears], gate=ft.AlignmentGate(circular=True)
    ) == [(2, 10_000), (2, 20_000)]


def test_a_secondary_that_is_also_unmapped_is_refused_not_scored():
    """`SCORABLE_SECONDARY_ROW` promises a row the CIGAR axis can judge, and an unmapped
    record is not one whatever its secondary bit says — there is no aligned span. The
    fatal class wins, so the slice is refused rather than the row silently dropped."""
    secondary_and_unmapped = (1, 3, 10, 0x104, None, None, "6000=", None)
    with pytest.raises(ValueError, match="neither axis"):
        _gated_reads(
            _INTERIOR_READ + [secondary_and_unmapped], gate=ft.AlignmentGate(circular=True)
        )


def test_a_secondary_does_not_ride_in_on_its_primarys_clearance():
    """The pooled arm keys on `(read, is_read1, reference)`, which a secondary placed
    elsewhere on the SAME contig shares with its primary — a tandem repeat, a collapsed
    element. Without an explicit exclusion it would be kept because its primary cleared,
    never having been scored at all. Here the primary clears and the secondary's own
    CIGAR explains a third of the read, so only the primary survives."""
    same_read_same_contig = (1, 2, 10, 256, 30_000, 32_000, "2000=4000S", None)
    assert _gated_reads(
        _INTERIOR_READ + [same_read_same_contig], gate=ft.AlignmentGate(circular=True)
    ) == [(2, 10_000)]


def test_a_circular_gate_over_paired_data_is_refused():
    """Mates are different molecules and the macro keeps them apart, so a circular gate
    judges a placement's halves independently — the same orphaning the paired gate
    exists to prevent. The refusal names the axis to use instead."""
    with pytest.raises(ValueError, match="paired"):
        _gated_reads(
            [
                (1, 1, 10, 99, 100, 250, "150=", 200),
                (1, 1, 10, 147, 200, 350, "150=", 100),
            ],
            gate=ft.AlignmentGate(circular=True),
        )


def test_an_all_non_eqx_slice_is_refused_under_the_circular_gate_too():
    """Pooled identity is NULL on a legacy-`M` CIGAR and `NULL >= threshold` drops the
    row, so an identity term over such a slice would empty the table and look like a
    result. Dropping the term is the documented way to gate that data on coverage."""
    legacy = [
        (1, 1, 10, 0, 27_000, 30_000, "3000M3000S", None),
        (1, 1, 10, 2048, 0, 3_000, "3000H3000M", None),
    ]
    with pytest.raises(ValueError, match="eqx|identity"):
        _gated_reads(legacy, gate=ft.AlignmentGate(circular=True))
    assert _gated_reads(
        legacy, gate=ft.AlignmentGate(circular=True, circular_min_identity=None)
    ) == [
        (1, 0),
        (1, 27_000),
    ]


def test_the_circular_gate_releases_every_relation_it_staged():
    """The two rename views and the per-feature lengths are the circular gate's own, and
    the clearance is what lets go of them — on a client the lengths are a whole
    reference's worth of rows held for one gate."""
    with _miint_conn() as conn:
        _stage(
            conn,
            alignment=_JUNCTION_READ,
            mapping=_CIRCULAR_MAP,
            lengths=_CIRCULAR_LENGTHS,
            threshold=0.0,
            gate=ft.AlignmentGate(circular=True),
        )
        live = {
            name
            for (name,) in conn.execute(
                "SELECT table_name FROM duckdb_tables() "
                "UNION ALL SELECT view_name FROM duckdb_views()"
            ).fetchall()
        }
        for relation in (
            ft.CIRCULAR_ALIGNMENTS_VIEW,
            ft.FEATURE_TOPOLOGY_VIEW,
            ft.FEATURE_LENGTHS_TABLE,
            ft.STREAMED_ALIGNMENT_TABLE,
        ):
            assert relation not in live, relation


# ---------------------------------------------------------------------------
# The relabel to public identifiers, against real miint
#
# BOTH label relations come from a mint: one handle per published sample, one per
# published genome. Neither is derived from the genome map — the map is the roll-up
# key, and what a row is NAMED is a question only the mint can answer.
# ---------------------------------------------------------------------------

# The exported-feature mint's answer per genome. Accessions here because that is the
# usual case — the mint publishes a genome's real accession wherever it is unique
# across the published namespace, and a `QF<n>` handle only where it is not.
_FEATURE_HANDLES = {
    100: "GCF_000000100",
    200: "GCF_000000200",
    300: "GCF_000000300",
    400: "GCF_000000400",
    500: "GCF_000000500",
}
# The exported-identifier mint's answer for the two samples the alignment fixture uses.
_HANDLES = [(1, "QM1"), (2, "QM2")]


def _feature_handles(mapping, handles=None) -> list[tuple]:
    """The exported-feature mint's response for the genomes in `mapping`, one row per
    genome. A genome absent from `handles` is dropped, which is how a test stages a
    mint that does not cover every genome the counts mention."""
    resolved = _FEATURE_HANDLES if handles is None else handles
    genomes = sorted({g for _, g in mapping} & set(resolved))
    return [(g, resolved[g]) for g in genomes]


def _stage_labels(conn, *, feature_handles, handles) -> None:
    """Stage both label relations from the two mint responses a client holds."""
    conn.execute(
        "CREATE TABLE _exported_feature_src AS "
        + _values(feature_handles, "genome_idx, export_feature_id", "?::BIGINT, ?::VARCHAR"),
        [x for r in feature_handles for x in r],
    )
    conn.execute(
        "CREATE TABLE _mint_src AS "
        + _values(handles, "prep_sample_idx, export_id", "?::BIGINT, ?::VARCHAR"),
        [x for r in handles for x in r],
    )
    conn.execute(ft.genome_label_table_sql("_exported_feature_src"))
    conn.execute(ft.sample_label_table_sql("_mint_src"))


def _relabel(conn) -> ft.LabelClearance:
    """Drive the relabel protocol: diagnose, check, then write the public table.

    The diagnostics row is passed BY NAME — the query's column names are the check's
    parameter names, so a rename on either side fails loudly instead of silently
    shifting one count into another's argument.
    """
    cursor = conn.execute(ft.relabel_diagnostics_sql())
    names = [d[0] for d in cursor.description]
    clearance = ft.check_relabel_diagnostics(**dict(zip(names, cursor.fetchone(), strict=True)))
    conn.execute(ft.labelled_relation_sql(clearance=clearance))
    return clearance


def _public_table(
    scope,
    threshold,
    *,
    alignment=_ALIGNMENT,
    mapping=_MAP,
    lengths=_LENGTHS,
    gate=None,
    feature_handles=None,
    handles=_HANDLES,
) -> list[tuple]:
    """Run the analytic AND the relabel, returning the PUBLIC table, sorted. Mirrors
    the client-side consumer's call order precisely.

    `feature_handles` defaults to a mint response covering every genome in `mapping`,
    which is what the client achieves by minting FROM the roll-up's own output; a test
    that wants the two to DISAGREE passes it explicitly.
    """
    with _miint_conn() as conn:
        populated = _ogu_input(
            conn, scope, threshold, alignment=alignment, mapping=mapping, lengths=lengths, gate=gate
        )
        conn.execute(ft.ogu_output_table_sql(populated=populated))
        _stage_labels(
            conn,
            feature_handles=(
                _feature_handles(mapping) if feature_handles is None else feature_handles
            ),
            handles=handles,
        )
        _relabel(conn)
        return conn.execute(f"SELECT * FROM {ft.LABELLED_RELATION} ORDER BY 1, 2").fetchall()


def test_the_public_table_carries_handles_instead_of_our_identifiers():
    """Every row the analytic emits pooled at 1% (G300 alone fails the threshold),
    named the way a published artifact must name them."""
    assert _public_table(ft.CoverageScope.POOLED, 0.01) == [
        ("QM1", "GCF_000000100", 1.0),
        ("QM1", "GCF_000000200", 1.0),
        ("QM1", "GCF_000000400", 2.0),
        ("QM2", "GCF_000000200", 1.0),
    ]


def test_no_internal_identifier_survives_into_the_public_table():
    """Asserted on the relation's own schema, not on the SQL: this is the property
    that keeps `prep_sample_idx` and `genome_idx` out of a file somebody publishes,
    and the writers downstream inherit it from this table alone.
    """
    with _miint_conn() as conn:
        populated = _ogu_input(conn, ft.CoverageScope.POOLED, 0.01)
        conn.execute(ft.ogu_output_table_sql(populated=populated))
        _stage_labels(conn, feature_handles=_feature_handles(_MAP), handles=_HANDLES)
        _relabel(conn)
        described = conn.execute(f"DESCRIBE {ft.LABELLED_RELATION}").fetchall()

    assert [(r[0], r[1]) for r in described] == list(ft.LABELLED_SCHEMA.items())
    assert not [name for name, *_ in described if "idx" in name]


def test_a_multi_contig_genome_is_not_multiplied_by_the_relabel():
    """G400 has two contigs, so it appears twice in the roll-up KEY. The label relation
    is per-genome, so nothing can repeat its counts — this pins that the two shapes
    still meet correctly. Its value is 2.0 from two reads; the failure mode is two rows
    of 2.0, or a 4.0, both of which read as a real number.
    """
    rows = _public_table(ft.CoverageScope.POOLED, 0.2)
    assert rows == [
        ("QM1", "GCF_000000100", 1.0),
        ("QM1", "GCF_000000400", 2.0),
    ]


def test_an_emitted_handle_collision_is_refused():
    """Two genomes sharing one handle relabel to one row of the published table, and a
    BIOM write SUMS the pair without comment.

    The mint's published namespace is UNIQUE across live rows, so this is no longer a
    state the server can produce — a genome whose accession is taken gets a `QF<n>`
    instead. The check stays as a backstop against a label relation staged from
    anywhere else, because the failure it catches is invisible in the output.
    """
    colliding = dict(_FEATURE_HANDLES) | {400: _FEATURE_HANDLES[100]}
    with pytest.raises(ValueError, match="export_feature_id"):
        _public_table(
            ft.CoverageScope.POOLED, 0.2, feature_handles=_feature_handles(_MAP, colliding)
        )


def test_a_collision_between_genomes_the_threshold_DROPPED_is_not_refused():
    """The assertion is over the genomes this table actually emits, not over the whole
    reference. G300 fails the 1% threshold, so its `source_id` colliding with G100's
    cannot merge anything — refusing it would fail a build whose output is correct.
    """
    colliding = dict(_FEATURE_HANDLES) | {300: _FEATURE_HANDLES[100]}
    assert _public_table(
        ft.CoverageScope.POOLED, 0.01, feature_handles=_feature_handles(_MAP, colliding)
    ) == [
        ("QM1", "GCF_000000100", 1.0),
        ("QM1", "GCF_000000200", 1.0),
        ("QM1", "GCF_000000400", 2.0),
        ("QM2", "GCF_000000200", 1.0),
    ]


def test_a_genome_with_no_public_handle_is_refused():
    """Staged from a mint response that omits G400.

    The client mints FROM the roll-up's own output, so this should be unreachable
    there; it is the backstop for a mint resolved against some other genome set — the
    alternative being a published row whose feature_id is NULL.
    """
    partial = {k: v for k, v in _FEATURE_HANDLES.items() if k != 400}
    with pytest.raises(ValueError, match="genome"):
        _public_table(ft.CoverageScope.POOLED, 0.2, feature_handles=_feature_handles(_MAP, partial))


def test_a_sample_with_no_public_handle_is_refused():
    """The reachable half of the pair: the mint is a separate route taking its own
    cohort, so a caller can mint for fewer samples than they streamed."""
    with pytest.raises(ValueError, match="sample"):
        _public_table(ft.CoverageScope.POOLED, 0.01, handles=[(1, "QM1")])


def test_a_repeated_handle_is_refused_rather_than_inflating_a_count():
    """Two rows for one sample multiply that sample's every count. The mint cannot
    answer twice for a sample, so this catches a map that did not come from it.
    """
    with pytest.raises(ValueError, match="changed the table's size"):
        _public_table(ft.CoverageScope.POOLED, 0.01, handles=[(1, "QM1"), (1, "QM1"), (2, "QM2")])


def test_an_empty_cohort_relabels_to_the_same_columns_and_types():
    """Nothing survives a 100% threshold, so the counts come from the 0-row
    short-circuit — and must still land in the public table with the same schema a
    populated cohort produces. This is the path a caller exercises least.
    """
    with _miint_conn() as conn:
        populated = _ogu_input(conn, ft.CoverageScope.POOLED, 1.0)
        assert not populated
        conn.execute(ft.ogu_output_table_sql(populated=populated))
        _stage_labels(conn, feature_handles=_feature_handles(_MAP), handles=_HANDLES)
        clearance = _relabel(conn)
        described = [
            (r[0], r[1]) for r in conn.execute(f"DESCRIBE {ft.LABELLED_RELATION}").fetchall()
        ]
        rows = conn.execute(f"SELECT count(*) FROM {ft.LABELLED_RELATION}").fetchone()[0]

    assert clearance.rows == 0
    assert rows == 0
    assert described == list(ft.LABELLED_SCHEMA.items())


# ---------------------------------------------------------------------------
# The sheared tree, against real miint
#
# The tree fixture is genome-level, as a publishable one must be: one tip per
# published genome, with G300's contig present but unpublished (it fails the 1%
# threshold) so the NULL-name path is exercised, and `inner1` collapsible so a
# surviving branch length is a SUM rather than a copy.
#
#   (((c10:0.2, c30:0.5)inner1:0.1, c20:0.3)inner2:0.05, c40:0.4);
#
# Shearing to {G100, G200, G400} drops c30, leaving inner1 with one child — so
# c10's branch becomes 0.2 + 0.1.
# ---------------------------------------------------------------------------

_TREE = "(((c10:0.2,c30:0.5)inner1:0.1,c20:0.3)inner2:0.05,c40:0.4);"
_TIP_FEATURES = [("c10", 10), ("c20", 20), ("c30", 30), ("c40", 40)]


def _published_handles(conn) -> list[tuple]:
    """The exported-feature mint's response for the genomes the roll-up EMITTED, which is
    what the client mints from — not every genome in the map. G300 fails the 1% threshold
    and so is not published, and must not be in the keep-set either."""
    rows = conn.execute(
        f"SELECT DISTINCT genome_idx FROM {ft.OGU_OUTPUT_TABLE} ORDER BY genome_idx"
    ).fetchall()
    return [(genome_idx, _FEATURE_HANDLES[genome_idx]) for (genome_idx,) in rows]


def _stage_phylogeny(
    conn, tmp_path, *, newick=_TREE, tip_features=_TIP_FEATURES, blocked=()
) -> None:
    """Stage a tree in the lake's shape: `read_newick` plus `is_tip`, with `feature_idx`
    resolved from the tip's name by a LEFT JOIN — which is how ingest writes it, so an
    unmatched tip keeps its row with a NULL `feature_idx`.

    `blocked` is the reference's curated blocklist, staged alongside because the shear
    reads both — usually empty, which is the state a reference with no exclusions is in.
    """
    conn.execute("CREATE TABLE _exclusion (feature_idx BIGINT)")
    for feature_idx in blocked:
        conn.execute("INSERT INTO _exclusion VALUES (?)", [feature_idx])
    conn.execute(ft.blocked_feature_table_sql("_exclusion"))
    path = tmp_path / "reference.nwk"
    path.write_text(newick + "\n")
    conn.execute(
        "CREATE TABLE _tip_feature AS "
        + _values(tip_features, "name, feature_idx", "?::VARCHAR, ?::BIGINT"),
        [x for r in tip_features for x in r],
    )
    conn.execute(
        f"CREATE TABLE _phylo_src AS "
        f"SELECT n.node_index, n.parent_index, n.name, n.branch_length, n.edge_id, "
        f"n.is_tip, f.feature_idx "
        f"FROM read_newick('{path}') n LEFT JOIN _tip_feature f ON f.name = n.name"
    )
    conn.execute(ft.phylogeny_table_sql("_phylo_src"))


def _shear(conn) -> ft.TreeClearance:
    """Drive the shear protocol as the client does: define the two arguments, diagnose,
    check, then run what the clearance carries."""
    for sql in ft.shear_input_statements():
        conn.execute(sql)
    cursor = conn.execute(ft.tree_diagnostics_sql())
    names = [d[0] for d in cursor.description]
    clearance = ft.check_tree_diagnostics(**dict(zip(names, cursor.fetchone(), strict=True)))
    for sql in clearance.statements:
        conn.execute(sql)
    return clearance


@contextlib.contextmanager
def _labelled(*, mapping=_MAP, lengths=_LENGTHS, threshold=0.01):
    """A connection carrying everything the shear needs: the roll-up done, the labels
    minted from its own output, and the relabel run — the client's order exactly."""
    with _miint_conn() as conn:
        populated = _ogu_input(
            conn, ft.CoverageScope.POOLED, threshold, mapping=mapping, lengths=lengths
        )
        conn.execute(ft.ogu_output_table_sql(populated=populated))
        _stage_labels(conn, feature_handles=_published_handles(conn), handles=_HANDLES)
        _relabel(conn)
        yield conn


def _sheared(conn) -> list[tuple]:
    return conn.execute(f"SELECT * FROM {ft.TREE_TABLE} ORDER BY node_index").fetchall()


def test_the_sheared_tree_sums_the_branch_length_of_a_collapsed_ancestor(tmp_path):
    """The property that makes a sheared tree usable: `inner1` loses c30 and so has one
    child, collapse removes it, and c10's surviving branch is 0.2 + 0.1 — the whole tree's
    distance, not the pruned one's. A float sum, hence approximately."""
    with _labelled() as conn:
        _stage_phylogeny(conn, tmp_path)
        clearance = _shear(conn)
        rows = _sheared(conn)

    assert clearance.tips == 3
    by_name = {name: branch for _, name, branch, *_ in rows}
    assert by_name["GCF_000000100"] == pytest.approx(0.3)
    assert by_name["GCF_000000200"] == pytest.approx(0.3)
    assert by_name["GCF_000000400"] == pytest.approx(0.4)
    # inner2 survives (it keeps two children) and the root is renamed to nothing by the
    # shear; neither is a tip.
    assert sum(1 for row in rows if row[5]) == 3


def test_an_unpublished_tip_is_sheared_away_rather_than_published_by_its_own_name(tmp_path):
    """c30's genome fails the coverage threshold, so it has no handle. Its tip must
    vanish, and its reference-internal name `c30` must not reach the artifact."""
    with _labelled() as conn:
        _stage_phylogeny(conn, tmp_path)
        _shear(conn)
        rows = _sheared(conn)

    names = {name for _, name, *_ in rows}
    assert "c30" not in names
    assert {name for _, name, _, _, _, is_tip in rows if is_tip} == {
        "GCF_000000100",
        "GCF_000000200",
        "GCF_000000400",
    }


def test_the_shear_releases_the_whole_reference_tree(tmp_path):
    """It is the largest relation in the recipe, and the write is still to come."""
    with _labelled() as conn:
        _stage_phylogeny(conn, tmp_path)
        _shear(conn)
        live = {
            name for (name,) in conn.execute("SELECT table_name FROM duckdb_tables()").fetchall()
        }
        views = {
            name for (name,) in conn.execute("SELECT view_name FROM duckdb_views()").fetchall()
        }

    assert ft.TREE_TABLE in live
    assert ft.PHYLOGENY_TABLE not in live
    assert not {ft.SHEAR_INPUT_RELATION, ft.SHEAR_KEEP_SET_RELATION} & views


def test_a_genome_owning_two_tips_is_refused_by_its_handle(tmp_path):
    """A contig-level tree for a multi-contig genome. The shear would accept it and keep
    both tips under one handle — a tree with duplicate tip names."""
    tree = "((c40:0.2,c41:0.3)G400:0.1,(c10:0.4,c20:0.5)inner:0.2);"
    tips = [("c10", 10), ("c20", 20), ("c40", 40), ("c41", 41)]
    with _labelled() as conn:
        _stage_phylogeny(conn, tmp_path, newick=tree, tip_features=tips)
        with pytest.raises(ValueError, match="GCF_000000400"):
            _shear(conn)


def test_a_genome_whose_ONLY_tip_is_blocked_is_refused_by_its_handle(tmp_path):
    """The curator blocked c40, the one tip G400 has. G400 still publishes — c41 aligned
    and nothing blocked it — so the table is fine and only the tree is not: its single
    position for that organism comes from sequence the blocklist rejects."""
    with _labelled() as conn:
        _stage_phylogeny(conn, tmp_path, blocked=[40])
        with pytest.raises(ValueError, match="GCF_000000400") as excinfo:
            _shear(conn)
    assert "blocked" in str(excinfo.value)


def test_a_genome_keeps_its_UNBLOCKED_tip_when_a_sibling_contig_is_blocked(tmp_path):
    """The case the blocklist actually exists for, and the reason a blocked tip is unnamed
    rather than merely counted: G400 has a tip per contig, and blocking c40 leaves exactly
    one — so the tree publishes c41's position instead of being refused as ambiguous."""
    tree = "((c40:0.2,c41:0.3)G400:0.1,(c10:0.4,c20:0.5)inner:0.2);"
    tips = [("c10", 10), ("c20", 20), ("c40", 40), ("c41", 41)]
    with _labelled() as conn:
        _stage_phylogeny(conn, tmp_path, newick=tree, tip_features=tips, blocked=[40])
        clearance = _shear(conn)
        rows = _sheared(conn)

    assert clearance.tips == 3
    # c41's own branch, not c40's and not a sum: G400's surviving tip is a sibling of the
    # blocked one, so nothing collapsed onto it.
    by_name = {name: branch for _, name, branch, *_ in rows if name}
    assert by_name["GCF_000000400"] == pytest.approx(0.3 + 0.1)


def test_a_published_row_with_no_tip_in_the_tree_is_refused(tmp_path):
    """A tree that covers only some of what the table publishes reads as though the rest
    were left out of the analysis."""
    tree = "((c10:0.2,c30:0.5)inner1:0.1,c40:0.4);"
    with _labelled() as conn:
        _stage_phylogeny(conn, tmp_path, newick=tree)
        with pytest.raises(ValueError, match="GCF_000000200"):
            _shear(conn)


def test_a_tip_shared_by_two_published_genomes_is_refused(tmp_path):
    """`feature_genome` is many-to-many — identical bytes are one
    `feature_idx`, so a plasmid two organisms carry belongs to both — and renaming such a
    tip by genome duplicates the node. The shear's own error names a node id; this names
    the reason."""
    shared = [*_MAP, (10, 200)]
    with _labelled(mapping=shared) as conn:
        _stage_phylogeny(conn, tmp_path)
        with pytest.raises(ValueError, match="more than one genome"):
            _shear(conn)


def test_a_reference_with_no_phylogeny_is_refused(tmp_path):
    """An absent tree is an EMPTY stream, not an error, so the recipe has to notice. The
    shear's own message would name our staged relation instead."""
    with _labelled() as conn:
        _stage_phylogeny(conn, tmp_path, tip_features=[])
        conn.execute(f"DELETE FROM {ft.PHYLOGENY_TABLE}")
        with pytest.raises(ValueError, match="no phylogeny"):
            _shear(conn)


def test_the_written_tree_carries_only_published_names_and_no_identifier_of_ours(tmp_path):
    """Asserted on the FILE, which is what somebody publishes: every tip label is one of
    the table's own `feature_id`s, and no column is one of ours."""
    with _labelled() as conn:
        _stage_phylogeny(conn, tmp_path)
        clearance = _shear(conn)
        conn.execute("SET preserve_insertion_order=false")
        conn.execute(ft.tree_copy_sql(tmp_path / "t.tree.parquet", clearance=clearance))
        published = {
            name
            for (name,) in conn.execute(
                f"SELECT feature_id FROM {ft.GENOME_LABEL_TABLE}"
            ).fetchall()
        }
        written = duckdb.connect(":memory:")
        columns = [
            row[0]
            for row in written.execute(
                f"DESCRIBE SELECT * FROM '{tmp_path}/t.tree.parquet'"
            ).fetchall()
        ]
        tips = {
            name
            for (name,) in written.execute(
                f"SELECT name FROM '{tmp_path}/t.tree.parquet' WHERE is_tip"
            ).fetchall()
        }
        written.close()

    assert columns == list(ft.TREE_COLUMNS)
    assert not [name for name in columns if "idx" in name]
    assert tips == published


def test_the_rollup_report_counts_a_shared_features_alignment_row_once():
    """The map holds one row per `(feature, genome)` pair and a feature may belong to
    several genomes, so the diagnostics' join fans a shared feature's alignment row out
    once per genome. Counting the joined rows inflates only the denominator — the share
    the caller is shown then reads lower than the truth."""
    shared_map = [*_MAP, (10, 500)]
    unmapped_row = (1, 7, 77, 0, 0, 100)
    alignment = [*_ALIGNMENT, unmapped_row]
    with _miint_conn() as conn:
        _stage(conn, alignment=alignment, mapping=shared_map, lengths=_LENGTHS, threshold=0.01)
        cursor = conn.execute(ft.rollup_coverage_diagnostics_sql())
        names = [d[0] for d in cursor.description]
        coverage = ft.RollupCoverage(**dict(zip(names, cursor.fetchone(), strict=True)))

    assert coverage.alignment_rows == len(alignment)
    assert coverage.unmapped_rows == 1
    assert coverage.unmapped_features == 1
    assert "1 of 7" in ft.rollup_coverage_warning(coverage)


def test_a_tip_shared_with_an_UNPUBLISHED_genome_still_shears_cleanly(tmp_path):
    """The other half of the shared-feature case, and the one that reads as ambiguous
    without being so. Contig 40 belongs to G400 (published) and to G500, which also owns a
    1 Mb contig nothing aligned to and so fails the coverage threshold. The tip must simply
    be named G400's handle: the map is the whole reference's, but only PUBLISHED membership
    may rename a tip.

    Coverage filtering dropping some but not all of a shared feature's genomes is the
    ordinary case, so this is the axis that matters — and it is the one the fan-out bug
    broke, refusing the build over a genome the table never mentions.
    """
    shared_with_unpublished = [*_MAP, (40, 500), (99, 500)]
    lengths = [*_LENGTHS, (99, 1_000_000)]
    with _labelled(mapping=shared_with_unpublished, lengths=lengths) as conn:
        published = {
            name
            for (name,) in conn.execute(
                f"SELECT feature_id FROM {ft.GENOME_LABEL_TABLE}"
            ).fetchall()
        }
        _stage_phylogeny(conn, tmp_path)
        clearance = _shear(conn)
        rows = _sheared(conn)

    assert "GCF_000000500" not in published, "G500 must be unpublished for this to test anything"
    assert clearance.tips == 3
    assert {name for _, name, _, _, _, is_tip in rows if is_tip} == published


# ---------------------------------------------------------------------------
# The combined (inverted open reference) table: two arms, one woltka pass.
#
# The fixture is built so that every rule the reconciliation depends on has a
# fixture element only IT explains, and c50 carries three of them at once — it is
# a reference sequence AND a contig both samples assembled, which is the whole
# reason `feature_idx` being content-addressed is a hazard here.
#
#   reference   R100: c10          R200: c20          R300: c50
#   de novo     Q900 (sample 1): c50, c51             Q901 (sample 2): c50, c52
#
# All five contigs are 1000 bp, so a genome's denominator is a count of contigs
# and every proportion below is readable without arithmetic.
# ---------------------------------------------------------------------------

_R_MAP = [(10, 100), (20, 200), (50, 300)]
_R_LENGTHS = [(10, 1000), (20, 1000), (50, 1000)]

# (prep_sample_idx, feature_idx, genome_idx) — scoped to ONE assembly run.
_D_MAP = [(1, 50, 900), (1, 51, 900), (2, 50, 901), (2, 52, 901)]
# One stream per cohort sample, as the assembly read-back is scoped. c50 is in both.
_D_LENGTHS = {1: [(50, 1000), (51, 1000)], 2: [(50, 1000), (52, 1000)]}

_R_ALIGNMENT = [
    # (prep_sample_idx, sequence_idx, feature_idx, flags, position, stop_position)
    (1, 1, 10, 0, 0, 500),  # R100, only the reference arm places it
    (1, 2, 20, 0, 0, 500),  # R200, the remainder case
    (1, 3, 50, 0, 0, 500),  # R300 — superseded: read 3 is placed de novo too
    (2, 4, 10, 0, 0, 500),  # R100 again, from the other sample
]
_D_ALIGNMENT = [
    (1, 3, 50, 0, 0, 500),  # Q900, and the read that supersedes its reference row
    (1, 5, 51, 0, 0, 500),  # Q900's second contig
    (2, 6, 50, 0, 0, 500),  # Q901 — the SAME contig as read 3, in the other sample
]


def _stage_combined(
    conn, *, threshold, denovo_map=_D_MAP, denovo_lengths=None, denovo_alignment=None
):
    """Stage both arms through the shared builders, in the order both drivers use.

    Deliberately not a branch inside `_stage`: the reference-only path is what that
    helper pins, and threading a second arm through it would let a change to the
    combined path alter what every reference-only test above exercises.
    """
    denovo_lengths = _D_LENGTHS if denovo_lengths is None else denovo_lengths
    conn.execute(
        "CREATE TABLE _r_map AS "
        + _values(_R_MAP, "feature_idx, genome_idx", "?::BIGINT, ?::BIGINT"),
        [x for r in _R_MAP for x in r],
    )
    conn.execute(
        "CREATE TABLE _d_map AS "
        + _values(
            denovo_map,
            "prep_sample_idx, feature_idx, genome_idx",
            "?::BIGINT, ?::BIGINT, ?::BIGINT",
        ),
        [x for r in denovo_map for x in r],
    )
    conn.execute(
        "CREATE TABLE _r_len AS "
        + _values(_R_LENGTHS, "feature_idx, sequence_length_bp", "?::BIGINT, ?::BIGINT"),
        [x for r in _R_LENGTHS for x in r],
    )
    conn.execute(ft.map_table_sql("_r_map"))
    conn.execute(ft.denovo_map_table_sql("_d_map"))

    if ft.coverage_filter_applies(threshold):
        conn.execute(ft.genome_lengths_table_sql("_r_len"))
        conn.execute(ft.denovo_contig_lengths_table_sql())
        # One INSERT per cohort sample, which is what the per-run assembly DoGet
        # forces and what the dedupe in the roll-up exists to survive.
        for sample, rows in sorted(denovo_lengths.items()):
            conn.execute(
                f"CREATE OR REPLACE TABLE _d_len_{sample} AS "
                + _values(rows, "feature_idx, sequence_length_bp", "?::BIGINT, ?::BIGINT"),
                [x for r in rows for x in r],
            )
            conn.execute(ft.denovo_contig_lengths_insert_sql(f"_d_len_{sample}"))
        conn.execute(ft.denovo_genome_lengths_insert_sql())

    denovo_rows = _D_ALIGNMENT if denovo_alignment is None else denovo_alignment
    for name, rows in (("_r_align", _R_ALIGNMENT), ("_d_align", denovo_rows)):
        conn.execute(
            f"CREATE TABLE {name} AS "
            + _values(
                rows,
                'prep_sample_idx, sequence_idx, feature_idx, flags, "position", stop_position',
                "?::BIGINT, ?::BIGINT, ?::BIGINT, ?::USMALLINT, ?::BIGINT, ?::BIGINT",
            ),
            [x for r in rows for x in r],
        )
    conn.execute(ft.alignment_table_sql("_r_align"))
    for sql in ft.denovo_alignment_statements("_d_align"):
        conn.execute(sql)


def _combined_table(
    scope=ft.CoverageScope.POOLED,
    threshold=0.01,
    *,
    denovo_map=_D_MAP,
    denovo_lengths=None,
    denovo_alignment=None,
) -> list[tuple]:
    """The whole combined analytic, sorted. Same shape as `_table`, one arm wider."""
    with _miint_conn() as conn:
        _stage_combined(
            conn,
            threshold=threshold,
            denovo_map=denovo_map,
            denovo_lengths=denovo_lengths,
            denovo_alignment=denovo_alignment,
        )
        for sql, parameters in ft.ogu_input_statements(
            scope=scope, coverage_threshold=threshold, combined=True
        ):
            conn.execute(sql, parameters)
        populated = conn.execute(ft.ogu_input_count_sql()).fetchone()[0] > 0
        select = ft.woltka_ogu_select_sql() if populated else ft.empty_ogu_select_sql()
        return conn.execute(f"SELECT * FROM ({select}) ORDER BY 1, 2").fetchall()


def _reference_only_table(threshold=0.01) -> list[tuple]:
    """The same reference arm with no de novo arm at all — the control every
    assertion about the combined table is read against."""
    return _table(
        ft.CoverageScope.POOLED,
        threshold,
        alignment=_R_ALIGNMENT,
        mapping=_R_MAP,
        lengths=_R_LENGTHS,
    )


def test_combined_table_places_each_read_on_exactly_one_arm():
    """The whole reconciliation in one assertion, and every row of it is a rule:

    * read 3 is placed by BOTH arms and lands only on the de novo side (Q900),
      contributing 1.0 there and nothing to R300 — precedence;
    * read 2 is placed only by the reference arm and stays there — the remainder;
    * reads 3 and 6 are on the SAME contig c50 in different samples, and each is
      whole against its own sample's genome rather than split across both;
    * R300 is gone: c50 was its only aligned contig and precedence took its only
      read, so a genome that survives the reference-only control drops out here.
    """
    assert _combined_table() == [
        (1, 100, 1.0),  # read 1
        (1, 200, 1.0),  # read 2 — the remainder
        (1, 900, 2.0),  # reads 3 and 5, both whole
        (2, 100, 1.0),  # read 4
        (2, 901, 1.0),  # read 6, whole against ITS sample's genome
    ]


def test_the_reference_only_control_keeps_the_genome_the_combined_table_drops():
    """R300 clears the threshold on the same cohort when there is no de novo arm to
    take its read. Without this the row missing above is not evidence of precedence
    — it is indistinguishable from a fixture that never covered R300."""
    assert _reference_only_table() == [
        (1, 100, 1.0),
        (1, 200, 1.0),
        (1, 300, 1.0),  # read 3, which the combined table gives to Q900 instead
        (2, 100, 1.0),
    ]


@pytest.mark.parametrize("denovo_map", [_D_MAP, [row for row in _D_MAP if row[:2] != (1, 50)]])
def test_no_read_is_lost_by_the_reconciliation(denovo_map):
    """Conservation: every read that reached either arm is still counted, so the
    table's values sum to the number of distinct reads staged.

    Only the losing direction — a read counted twice cannot show up here, because
    woltka normalizes a read across the genomes it sees and 0.5 + 0.5 is also 1.0.
    Precedence's failure to the OTHER side is what
    `test_combined_table_places_each_read_on_exactly_one_arm` reads.

    The second parameter is the case that makes this discriminate at all: with no
    genome for c50 in sample 1, a DELETE that superseded on the raw de novo slice
    rather than through the map would take read 3's reference placement away without
    giving it a de novo one, and the sum would come back 5.
    """
    staged = {row[1] for row in _R_ALIGNMENT} | {row[1] for row in _D_ALIGNMENT}
    table = _combined_table(denovo_map=denovo_map)
    assert sum(value for _, _, value in table) == pytest.approx(len(staged))


def test_a_contig_two_samples_assembled_is_not_credited_across_them():
    """c50 is one content-addressed `feature_idx` under Q900 and Q901, so the de novo
    map holds two rows for it. Joined on the contig alone both match every read on
    c50, and woltka splits each across the two genomes — 0.5 to the sample that did
    not produce the read. The join carries `prep_sample_idx` for exactly this.

    Asserted as whole numbers rather than a shape, because the failure is a plausible
    half rather than an error.
    """
    values = {(sample, genome): value for sample, genome, value in _combined_table()}
    assert values[(1, 900)] == 2.0
    assert values[(2, 901)] == 1.0
    assert (1, 901) not in values and (2, 900) not in values


def test_a_contig_two_samples_assembled_is_counted_once_in_each_denominator():
    """c50 arrives on both samples' length streams, so the roll-up sees it twice.
    Deduplicated, Q901 is 2000 bp and its one 500 bp read is 25% of it; summed raw it
    is 3000 bp and the same read is 16.7%, so a threshold between the two is what
    tells the fix from a coincidence.

    Q900 is the control: at 33% undeduplicated it clears 20% either way, so a failure
    here is specifically the denominator and not the threshold.
    """
    assert _combined_table(threshold=0.20) == [
        (1, 100, 1.0),
        (1, 200, 1.0),
        (1, 900, 2.0),
        (2, 100, 1.0),
        (2, 901, 1.0),  # 500/2000 = 25% — present only if c50 was counted once
    ]


def test_a_de_novo_placement_with_no_genome_leaves_the_read_to_the_reference_arm():
    """Precedence is over rollable placements: the DELETE reads the de novo slice
    THROUGH the map, so a run whose membership carries no genome for a contig falls
    back to that read's reference placement instead of dropping it from both arms.

    Dropping Q900's c50 row from the map removes read 3 from the de novo arm; it must
    reappear on R300, which the reference-only control shows is where it would have
    been all along.
    """
    without_c50 = [row for row in _D_MAP if row[:2] != (1, 50)]
    values = {(s, g): v for s, g, v in _combined_table(denovo_map=without_c50)}
    assert values[(1, 300)] == 1.0, "read 3 falls back to its reference placement"
    assert values[(1, 900)] == 1.0, "only read 5 is left on Q900"


def test_per_sample_scope_reaches_both_arms():
    """The per-sample survivor set is built from two hand-rolled branches in one
    statement, where pooled has the macro on one side; this is the shape that would
    fail to parse or bind rather than answer wrongly. Every genome here clears 1% in
    the sample that carries it, so the table is the pooled one.
    """
    assert _combined_table(scope=ft.CoverageScope.PER_SAMPLE) == _combined_table()


def test_an_unfiltered_combined_table_still_reconciles():
    """At threshold 0 there is no survivor set, no coverage view and no lengths at
    all — so precedence is the only thing left standing between the two arms, and it
    still has to hold. R300 keeps its zero rows; Q900 keeps read 3.
    """
    values = {(s, g): v for s, g, v in _combined_table(threshold=0.0)}
    assert (1, 300) not in values
    assert values[(1, 900)] == 2.0


def test_a_read_the_denovo_arm_won_can_fall_out_of_the_table_entirely():
    """The third consequence of precedence, and the one most easily misread as a
    result: precedence runs at staging, the breadth filter runs after it, and both
    arms inner-join the survivor set. So a read the de novo arm won, on a qiita
    genome that then fails `coverage_threshold`, is gone from the de novo arm by the
    filter and from the reference arm by precedence.

    Q900's contigs are inflated to 100 kb here so 1000 covered bases is 1%, under the
    2% threshold. Read 3 was ALSO placed on R300 by the reference arm, and R300
    survives on sample 2's own read — so the read's reference home is still in the
    table and it still is not counted there.
    """
    fat = {1: [(50, 50_000), (51, 50_000)], 2: [(50, 1000), (52, 1000)]}
    extra_reference = [*_R_ALIGNMENT, (2, 7, 50, 0, 0, 900)]
    with _miint_conn() as conn:
        _stage_combined(conn, threshold=0.02, denovo_lengths=fat)
        # The reference arm keeps a second read on c50 so R300 clears the threshold
        # on its own; without it R300's absence would be ambiguous.
        conn.execute(
            "INSERT INTO alignment_slice "
            + _values(
                [(2, 7, 50, 0, 0, 900)],
                'prep_sample_idx, sequence_idx, feature_idx, flags, "position", stop_position',
                "?::BIGINT, ?::BIGINT, ?::BIGINT, ?::USMALLINT, ?::BIGINT, ?::BIGINT",
            ),
            [2, 7, 50, 0, 0, 900],
        )
        for sql, parameters in ft.ogu_input_statements(
            scope=ft.CoverageScope.POOLED, coverage_threshold=0.02, combined=True
        ):
            conn.execute(sql, parameters)
        populated = conn.execute(ft.ogu_input_count_sql()).fetchone()[0] > 0
        select = ft.woltka_ogu_select_sql() if populated else ft.empty_ogu_select_sql()
        rows = conn.execute(f"SELECT * FROM ({select}) ORDER BY 1, 2").fetchall()

    values = {(s, g): v for s, g, v in rows}
    assert (1, 900) not in values, "Q900 failed the breadth filter"
    assert (1, 300) not in values, "and precedence had already taken read 3 off R300"
    assert (2, 300) in values, (
        "R300 survives on the read the de novo arm never touched — without this the "
        "row above is just a genome nothing covered"
    )
    staged = {row[1] for row in extra_reference} | {row[1] for row in _D_ALIGNMENT}
    assert sum(values.values()) < len(staged), (
        "so the table counts fewer reads than were staged — the documented consequence"
    )


def test_several_rows_of_one_read_within_an_arm_still_count_once():
    """`reconcile`'s module docstring makes "what is counted is unchanged by any of
    this" load-bearing for the whole precedence design, and nothing exercised the
    de novo arm's half of it.

    Read 5 gets a secondary placement on the same contig — two rows, one genome. It
    must contribute what one read contributes, not two: `woltka_ogu` splits across
    DISTINCT `reference` values, and both rows share one. (This says nothing about
    the arms' UNION ALL: within-arm multiplicity flows through one side of it either
    way.)
    """
    with_secondary = [*_D_ALIGNMENT, (1, 5, 51, 256, 100, 600)]
    values = {(s, g): v for s, g, v in _combined_table(denovo_alignment=with_secondary)}
    assert values[(1, 900)] == 2.0, "reads 3 and 5, the secondary adding nothing"
