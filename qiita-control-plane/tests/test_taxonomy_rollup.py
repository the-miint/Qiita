"""What the shared per-genome taxonomy reduction actually computes, over a real
DuckDB.

`qiita_common.taxonomy` is SQL text, so its own tests can only assert shape. These
run it. They are separate from `test_plan_shards.py`, which pins the
same reduction through the shard planner's *string* projection: that file is the
regression net for the hoist (its assertions must keep passing untouched), while
these are the column-form contract the published taxonomy sidecar rests on — a
genome present with NULL ranks rather than absent, and one curation decision not
relocating a whole organism.
"""

import duckdb
import pytest
from qiita_common.taxonomy import (
    RANK_COLUMNS,
    genome_representative_taxonomy_select_sql,
    prefixed_rank_columns_sql,
)

_TAXONOMY_DDL = (
    "CREATE TABLE taxonomy ("
    " feature_idx BIGINT, domain VARCHAR, phylum VARCHAR, class VARCHAR,"
    ' "order" VARCHAR, family VARCHAR, genus VARCHAR, species VARCHAR, strain VARCHAR)'
)


@pytest.fixture
def con():
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE member_genome (feature_idx BIGINT, genome_idx BIGINT)")
    connection.execute(_TAXONOMY_DDL)
    yield connection
    connection.close()


def _seed(con, member_genome_rows, taxonomy_rows):
    if member_genome_rows:
        con.executemany("INSERT INTO member_genome VALUES (?, ?)", member_genome_rows)
    if taxonomy_rows:
        con.executemany("INSERT INTO taxonomy VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", taxonomy_rows)


def _reduce(con, *, prefixed=False):
    inner = genome_representative_taxonomy_select_sql(
        member_genome="member_genome", taxonomy="taxonomy"
    )
    columns = prefixed_rank_columns_sql(alias="r") if prefixed else "*"
    if prefixed:
        sql = f"SELECT r.genome_idx, {columns} FROM ({inner}) r ORDER BY r.genome_idx"
    else:
        sql = f"SELECT * FROM ({inner}) ORDER BY genome_idx"
    return con.execute(sql).fetchall()


def test_a_genome_takes_the_ranks_of_its_lowest_classified_member(con):
    """Not the lexicographically smallest lineage, and not whatever the scan reached
    first. Feature 3 is lowest but unclassified; 20 is the lowest classified one and
    carries a lex-LARGER value than 99, so a `min(rank)` refactor would fail here."""
    _seed(
        con,
        [(3, 1), (20, 1), (99, 1)],
        [
            (20, "Zeta", "Zp", None, None, None, None, None, None),
            (99, "Alpha", "Ap", None, None, None, None, None, None),
        ],
    )
    assert _reduce(con) == [(1, "Zeta", "Zp", None, None, None, None, None, None)]


def test_a_genome_with_no_classified_member_is_present_with_null_ranks(con):
    """Present, not absent. A genome missing from the taxonomy artifact is
    indistinguishable from a genome missing from the table it accompanies, so an
    unclassified organism has to appear and say nothing."""
    _seed(con, [(11, 1), (99, 1)], [])
    assert _reduce(con) == [(1, None, None, None, None, None, None, None, None)]

    con.execute("DELETE FROM member_genome")
    _seed(con, [(11, 2)], [(11, None, None, None, None, None, None, None, None)])
    assert _reduce(con) == [(2, None, None, None, None, None, None, None, None)]


def test_blocking_the_lowest_contig_does_not_unclassify_its_siblings(con):
    """The exclusion case, which is why the FILTER exists. A genome is one organism
    whose contigs share one lineage; an excluded contig loses its taxonomy row, and
    letting that speak for the genome would relocate the whole organism on the
    strength of one curation decision."""
    _seed(con, [(11, 1), (99, 1)], [(99, "Zeta", None, None, None, None, None, None, None)])
    assert _reduce(con) == [(1, "Zeta", None, None, None, None, None, None, None)]


def test_each_genome_reduces_to_exactly_one_row(con):
    """The property the sidecar rests on: one row per published feature. Two rows for
    one genome would silently double a genome's taxonomy in a joined artifact."""
    _seed(
        con,
        [(1, 1), (2, 1), (3, 2), (4, 3)],
        [
            (1, "A", None, None, None, None, None, None, None),
            (3, "B", None, None, None, None, None, None, None),
        ],
    )
    rows = _reduce(con)
    assert [row[0] for row in rows] == [1, 2, 3]


def test_the_prefixes_come_back_and_an_empty_rank_keeps_its_own(con):
    """`d__Bacteria`, and a rank the source reported blank comes back as the bare
    `p__` — which is what it was before ingest stripped three characters. NULL stays
    NULL, because "not reported" is not the same claim."""
    _seed(
        con,
        [(1, 1)],
        [(1, "Bacteria", "", "Bacilli", None, None, None, None, None)],
    )
    assert _reduce(con, prefixed=True) == [
        (1, "d__Bacteria", "p__", "c__Bacilli", None, None, None, None, None)
    ]


def test_a_rank_that_is_present_below_a_missing_one_is_not_promoted(con):
    """The failure the column form exists to avoid. `concat_ws` skips NULLs, so the
    lineage STRING for this genome is `d__Bacteria;f__Listeriaceae` — which reads as
    though the phylum were Listeriaceae. The columns keep the gap where it is."""
    _seed(
        con,
        [(1, 1)],
        [(1, "Bacteria", None, None, None, "Listeriaceae", None, None, None)],
    )
    (row,) = _reduce(con, prefixed=True)
    by_rank = dict(zip(RANK_COLUMNS, row[1:], strict=True))
    assert by_rank["domain"] == "d__Bacteria"
    assert by_rank["phylum"] is None
    assert by_rank["family"] == "f__Listeriaceae"


def test_a_duplicated_member_row_still_reduces_to_exactly_one_row(con):
    """The reduction picks a representative member and joins that member's row back, so
    a duplicate at the winning position matches twice. One row per genome has to be a
    property of the SQL: the sidecar's row-count check would catch a second row, but the
    shard planner — the other consumer — has none, and two items sharing one id tile a
    genome into two shards.
    """
    _seed(con, [(1, 1), (1, 1)], [(1, "Zeta", "Zp", None, None, None, None, None, None)])
    assert _reduce(con) == [(1, "Zeta", "Zp", None, None, None, None, None, None)]

    con.execute("DELETE FROM member_genome")
    con.execute("DELETE FROM taxonomy")
    duplicated = (7, "Alpha", None, None, None, None, None, None, None)
    _seed(con, [(7, 2)], [duplicated, duplicated])
    assert _reduce(con) == [(2, "Alpha", None, None, None, None, None, None, None)]


def test_the_representatives_ranks_all_come_from_the_one_member(con):
    """Feature 1 is the lowest classified member and reports no phylum; feature 5 does.
    The genome's phylum must be NULL — an eight-`arg_min` reduction returns Firmicutes
    here, because `arg_min` ignores rows whose argument is NULL, and that is a rank
    promoted across a gap.
    """
    _seed(
        con,
        [(1, 1), (5, 1)],
        [
            (1, "Bacteria", None, "Bacilli", None, None, None, None, None),
            (5, "Bacteria", "Firmicutes", None, None, None, None, None, None),
        ],
    )
    (row,) = _reduce(con)
    by_rank = dict(zip(RANK_COLUMNS, row[1:], strict=True))
    assert by_rank["domain"] == "Bacteria"
    assert by_rank["phylum"] is None
    assert by_rank["class"] == "Bacilli"
