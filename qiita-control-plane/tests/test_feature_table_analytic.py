"""Behavioural tests for `qiita_common.feature_table` against REAL duckdb-miint.

The builders return SQL text and open no connection, so `qiita-common`'s own tests
assert on strings; the analytic's behaviour is pinned here and in the orchestrator's
`test_estimate_feature_table.py`. This module is the client-side half's home and
carries the properties that suite cannot: **the per-sample coverage scope**, which
no server-side caller uses.

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
from qiita_common import feature_table as ft

from qiita_control_plane.miint import connect_with_miint_staged

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
        conn.execute(ft.streamed_alignment_table_sql("_align_projected", gate=gate))
        total, scorable, unpoolable, paired_rows = conn.execute(
            ft.gate_diagnostics_sql(gate)
        ).fetchone()
        clearance = ft.check_gate_diagnostics(
            gate,
            total_rows=total,
            scorable_rows=scorable,
            unpoolable_partitions=unpoolable,
            paired_rows=paired_rows,
        )
        # The clearance carries the rest of the protocol in order — the gate and the
        # release of the streamed copy holding `cigar`.
        for sql, params in clearance.statements:
            conn.execute(sql, params)
    conn.execute(ft.map_table_sql("_map_src"))
    if ft.coverage_filter_applies(threshold):
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
    with connect_with_miint_staged() as conn:
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
    with connect_with_miint_staged() as conn:
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
    with connect_with_miint_staged() as conn:
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
    with connect_with_miint_staged() as conn:
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
    with connect_with_miint_staged() as conn:
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
    with connect_with_miint_staged() as conn:
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
    with connect_with_miint_staged() as conn:
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
    with connect_with_miint_staged() as conn:
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
    with connect_with_miint_staged() as conn:
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
    with connect_with_miint_staged() as conn:
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
