"""Behavioural tests for `qiita_common.feature_table` against REAL duckdb-miint.

`qiita-common` has no duckdb dependency, so its own tests can only assert on SQL
text; the analytic's behaviour is pinned here and in the orchestrator's
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
    native BIGINT (and `flags` as USMALLINT), never widened by inference."""
    tuples = ", ".join(f"({casts})" for _ in rows)
    return f"SELECT * FROM (VALUES {tuples}) AS t({columns})"


def _stage(conn, *, alignment, mapping, lengths, threshold):
    """Stage the three inputs exactly as a consumer does, through the shared
    builders, from raw VALUES source relations."""
    conn.execute(
        "CREATE TABLE _align_src AS "
        + _values(
            alignment,
            'prep_sample_idx, sequence_idx, feature_idx, flags, "position", stop_position',
            "?::BIGINT, ?::BIGINT, ?::BIGINT, ?::USMALLINT, ?::BIGINT, ?::BIGINT",
        ),
        [x for r in alignment for x in r],
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
    conn.execute(ft.alignment_table_sql("_align_src"))
    conn.execute(ft.map_table_sql("_map_src"))
    if ft.coverage_filter_applies(threshold):
        conn.execute(ft.genome_lengths_table_sql("_len_src"))


def _table(scope, threshold, *, alignment=None, mapping=None, lengths=None) -> list[tuple]:
    """Run the whole analytic for `scope` at `threshold` and return the feature
    table, sorted. Mirrors a consumer's call order precisely."""
    with connect_with_miint_staged() as conn:
        _stage(
            conn,
            alignment=_ALIGNMENT if alignment is None else alignment,
            mapping=_MAP if mapping is None else mapping,
            lengths=_LENGTHS if lengths is None else lengths,
            threshold=threshold,
        )
        survivor_scope = scope if ft.coverage_filter_applies(threshold) else None
        if survivor_scope is not None:
            conn.execute(ft.coverage_alignments_view_sql())
            conn.execute(ft.survivor_table_sql(survivor_scope), [threshold])
        conn.execute(ft.ogu_input_table_sql(survivor_scope=survivor_scope))

        empty = conn.execute(f"SELECT count(*) FROM {ft.OGU_INPUT_TABLE}").fetchone()[0] == 0
        select = ft.empty_ogu_select_sql() if empty else ft.woltka_ogu_select_sql()
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
    """The same fixture under per-sample scope: G200 vanishes ENTIRELY, because
    0.6% < 1% in each sample separately. This is the one-directional discriminator
    — the rows a stricter scope removes — and it is the whole point of the scope."""
    assert _table(ft.CoverageScope.PER_SAMPLE, 0.01) == [
        (1, 100, 1.0),
        (1, 400, 2.0),
        # absent: (1, 200) and (2, 200) — each sample covers only 0.6% of G200
    ]


def test_a_genome_every_sample_covers_is_identical_under_both_scopes():
    """A scope switch must not disturb genomes each sample covers well on its own —
    otherwise the discriminator above could be passing for the wrong reason (e.g. a
    scope that drops everything)."""
    pooled = [r for r in _table(ft.CoverageScope.POOLED, 0.01) if r[1] in (100, 400)]
    per_sample = [r for r in _table(ft.CoverageScope.PER_SAMPLE, 0.01) if r[1] in (100, 400)]
    assert pooled == per_sample == [(1, 100, 1.0), (1, 400, 2.0)]


@pytest.mark.parametrize("scope", list(ft.CoverageScope))
def test_the_denominator_is_the_full_genome_length_in_both_scopes(scope):
    """G300 is 20 bp over c30 (1000) + c31 (2000, nothing aligned) = 0.67% < 1%, so
    it must be ABSENT. On an aligned-contigs-only denominator it would be 2% and
    survive — its absence is what proves the unaligned contig counts, and the
    per-sample scope must not quietly use a different denominator than pooled.
    """
    assert not [r for r in _table(scope, 0.01) if r[1] == 300]


def test_per_sample_merges_intervals_within_a_contig_not_across_them():
    """`compress_intervals` merges within ONE coordinate space, so a genome's covered
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
