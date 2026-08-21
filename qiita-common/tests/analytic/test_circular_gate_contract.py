"""Real-miint contract pins for the three functions the circular gate rests on:
`circular_query_coverage`, and the two it reports through — `cigar_pooled_identity`
and `cigar_query_intervals`.

These run against the team-mirror build (staged by the session-autouse
`_stage_miint_extension` fixture in `tests/conftest.py`). What they pin is what
`analytic.gate`'s circular arm reads and nothing else: the macro takes our native
BIGINT ids under the column names the two rename views produce, the three columns the
predicate reads mean what the gate assumes, and identity is NULL on a legacy-`M` CIGAR
— which is the case `check_gate_diagnostics` refuses a whole slice for.

The fixture is upstream's own worked example (duckdb-miint#227): a 30 kb contig, a 6 kb
read taken across the origin, and a 6 kb interior read as the control. It is the shape
the gate exists for, so a build that stopped answering it would fail here rather than in
a user's feature table.

Upstream reference:
https://the-miint.github.io/duckdb-miint/alignment_analysis/#circular-query-coverage
"""

from __future__ import annotations

import duckdb
import pytest

from qiita_common.duckdb_miint import miint_connect_config, miint_load_sql

_REFERENCE_LENGTH = 30_000

# One read the linearised contig cut in two, and one it did not. Columns are the ones
# `circular_alignments_view_sql` renames ours into.
#   (read_id, flags, reference, position, stop_position, cigar)
_JUNCTION = [
    (1, 0, 7, 27_001, 30_001, "3000=3000S"),  # primary, the read's 5' half
    (1, 2048, 7, 1, 3_001, "3000H3000="),  # supplementary, its 3' half
]
_INTERIOR = [(2, 0, 7, 10_001, 16_001, "6000=")]


@pytest.fixture
def conn():
    con = duckdb.connect(":memory:", config=miint_connect_config())
    con.execute(miint_load_sql())
    try:
        yield con
    finally:
        con.close()


def _stage(conn, rows: list[tuple], *, length: int = _REFERENCE_LENGTH) -> None:
    """The macro's two arguments as relations, with the id columns native BIGINT — the
    types our `sequence_idx` / `feature_idx` arrive as."""
    values = "(?::BIGINT, ?::USMALLINT, ?::BIGINT, ?::BIGINT, ?::BIGINT, ?::VARCHAR)"
    conn.execute(
        "CREATE OR REPLACE TABLE alignments AS SELECT * FROM (VALUES "
        + ", ".join([values] * len(rows))
        + ") AS t(read_id, flags, reference, position, stop_position, cigar)",
        [x for row in rows for x in row],
    )
    conn.execute(
        "CREATE OR REPLACE TABLE reference_lengths AS "
        "SELECT * FROM (VALUES (7::BIGINT, ?::BIGINT, true)) AS t(reference, length, is_circular)",
        [length],
    )


def _pooled(conn, rows: list[tuple]) -> dict[int, tuple]:
    """`circular_query_coverage` over `rows`, keyed by read, as the gate reads it."""
    _stage(conn, rows)
    return {
        read_id: rest
        for read_id, *rest in conn.execute(
            "SELECT read_id, coverage, identity, mixed_strand, n_fragments "
            "FROM circular_query_coverage(alignments, reference_lengths) ORDER BY read_id"
        ).fetchall()
    }


def test_the_three_functions_are_in_the_catalog(conn):
    """The catalog, not a probe, is what answers "does this build have it" — an absent
    function surfaces as a bare `Catalog Error` naming it, which is what the deploy
    check in DEPLOY_CHECKLIST.md exists to catch earlier than a user does."""
    names = {
        name
        for (name,) in conn.execute(
            "SELECT function_name FROM duckdb_functions() WHERE function_name IN "
            "('circular_query_coverage', 'cigar_pooled_identity', 'cigar_query_intervals')"
        ).fetchall()
    }
    assert names == {"circular_query_coverage", "cigar_pooled_identity", "cigar_query_intervals"}


def test_a_split_read_pools_to_the_whole_read_while_each_record_reports_half(conn):
    """The property the mode exists for. Each record of the junction read explains half
    of it, so a per-record coverage floor of 0.90 — which is what the CIGAR gate applies
    — drops both; pooled, the read is fully explained by the reference it came from."""
    per_record = conn.execute(
        "SELECT cigar_query_coverage(?), cigar_query_coverage(?)",
        [_JUNCTION[0][5], _JUNCTION[1][5]],
    ).fetchone()
    assert per_record == (0.5, 0.5)

    pooled = _pooled(conn, _JUNCTION + _INTERIOR)
    assert pooled[1][0] == 1.0
    assert pooled[1][3] == 2  # both records pooled into the one read


def test_a_single_record_read_scores_the_same_either_way(conn):
    """So a caller can gate on the pooled figure unconditionally rather than branching on
    whether a reference is circular — which is what lets one gate serve a whole slice."""
    pooled = _pooled(conn, _JUNCTION + _INTERIOR)
    per_record = conn.execute(
        "SELECT cigar_query_coverage(?), cigar_sequence_identity(?)",
        [_INTERIOR[0][5], _INTERIOR[0][5]],
    ).fetchone()
    assert (pooled[2][0], pooled[2][1]) == per_record


def test_coverage_is_the_union_of_the_records_not_their_sum(conn):
    """Bounded by 1.0, which is what makes a coverage threshold mean the same thing on a
    split read as on a whole one. Overlapping fragments — a couple of bases deleted at
    the junction — would sum past 1.0."""
    overlapping = [
        (1, 0, 7, 27_001, 30_001, "3000=3000S"),
        (1, 2048, 7, 1, 3_101, "2900H3100="),
    ]
    assert _pooled(conn, overlapping)[1][0] == 1.0


def test_identity_is_pooled_over_the_records_not_averaged(conn):
    """One mismatch in the second fragment: pooling weights each record by the columns it
    aligned, so the read scores 5999/6000. Averaging the two records' own figures would
    say 0.99983 — the gate reads this column, so which one it is matters."""
    with_mismatch = [
        (1, 0, 7, 27_001, 30_001, "3000=3000S"),
        (1, 2048, 7, 1, 3_001, "3000H2999=1X"),
    ]
    identity = _pooled(conn, with_mismatch)[1][1]
    assert identity == pytest.approx(5999 / 6000)


def test_identity_is_NULL_on_a_legacy_M_cigar(conn):
    """`M` records that a base aligned, not whether it matched, so no identity is
    recoverable — and `NULL >= threshold` is NULL, so the gate's identity term would
    silently reject every such read. That is the case `check_gate_diagnostics` refuses a
    slice for; it counts on this being NULL rather than 1.0."""
    legacy = [(1, 0, 7, 27_001, 30_001, "3000M3000S"), (1, 2048, 7, 1, 3_001, "3000H3000M")]
    pooled = _pooled(conn, legacy)[1]
    assert pooled[1] is None  # identity
    assert pooled[0] == 1.0  # coverage still answers
    assert conn.execute("SELECT NULL >= 0.95").fetchone()[0] is None


def test_fragments_on_opposite_strands_are_reported_as_mixed_strand(conn):
    """Not a wrap: a read whose fragments lie on opposite strands is an inverted repeat,
    a chimera or a misassembly, and pooling manufactures coverage it does not have. The
    gate's `NOT mixed_strand` conjunct is what excludes it, and it can only do that
    because the macro reports the column."""
    inverted = [
        (1, 0, 7, 27_001, 30_001, "3000=3000S"),
        (1, 2064, 7, 1, 3_001, "3000=3000S"),  # 0x800 supplementary + 0x10 reverse
    ]
    assert _pooled(conn, inverted)[1][2] is True
    assert _pooled(conn, _JUNCTION)[1][2] is False


def test_the_macro_takes_relation_names_and_native_bigint_ids(conn):
    """Both fixtures above are staged with BIGINT `read_id` / `reference` — our
    `sequence_idx` and `feature_idx` — and passed as unquoted relation names, which is
    the calling convention the two rename views exist to satisfy. A VIEW binds as well as
    a TABLE, which is what lets the gate rename rather than copy the slice."""
    _stage(conn, _JUNCTION)
    conn.execute("CREATE VIEW aln_view AS SELECT * FROM alignments")
    conn.execute("CREATE VIEW len_view AS SELECT * FROM reference_lengths")
    rows = conn.execute(
        "SELECT read_id, coverage FROM circular_query_coverage(aln_view, len_view)"
    ).fetchall()
    assert rows == [(1, 1.0)]


def test_cigar_query_intervals_places_a_clip_on_the_reads_own_axis(conn):
    """What the pooling is built on: intervals are mirrored onto the read for a
    reverse-strand record, so the two records of one read are directly comparable. Pinned
    because a union over reference-oriented intervals would be wrong for half of all
    fragments, and wrong quietly."""
    forward, reverse = conn.execute(
        "SELECT cigar_query_intervals('3000=3000S', 0), cigar_query_intervals('3000=3000S', 16)"
    ).fetchone()
    assert forward == [{"start": 0, "stop": 3000}]
    assert reverse == [{"start": 3000, "stop": 6000}]


def test_cigar_pooled_identity_weights_each_record_by_what_it_aligned(conn):
    """The aggregate `circular_query_coverage` reports as its `identity` column. A short
    record with a mismatch and a long clean one: the mean of the two records overstates
    the error, the pooled figure is one mismatch in 1100 aligned columns."""
    conn.execute("CREATE TABLE frag AS SELECT * FROM (VALUES ('1000='), ('99=1X')) AS t(cigar)")
    pooled = conn.execute("SELECT cigar_pooled_identity(cigar) FROM frag").fetchone()[0]
    assert pooled == pytest.approx(1099 / 1100)
