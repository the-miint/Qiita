import duckdb
import pytest

from qiita_common.duckdb_relation import registered


def _source(con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    """Something registrable without pyarrow, which qiita-common does not depend on."""
    return con.sql("SELECT range AS x FROM range(1, 4)")


def _visible(con: duckdb.DuckDBPyConnection, relation: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {relation} LIMIT 0").fetchall()
    except duckdb.CatalogException:
        return False
    return True


def test_the_relation_lives_for_the_block():
    con = duckdb.connect()
    with registered(con, "src", _source(con)) as relation:
        assert relation == "src"
        assert con.execute(f"SELECT sum(x) FROM {relation}").fetchall() == [(6,)]
    assert not _visible(con, "src")


def test_a_part_read_result_does_not_fail_the_release():
    """`fetchone()` on a one-row result leaves it open, which DuckDB 2.0 would refuse to
    unregister under."""
    con = duckdb.connect()
    with registered(con, "src", _source(con)) as relation:
        (total,) = con.execute(f"SELECT sum(x) FROM {relation}").fetchone()
    assert total == 6
    assert not _visible(con, "src")


def test_the_block_s_own_exception_is_the_one_raised():
    con = duckdb.connect()
    with pytest.raises(RuntimeError, match="the block's own"):
        with registered(con, "src", _source(con)) as relation:
            con.execute(f"SELECT x FROM {relation}").fetchone()
            raise RuntimeError("the block's own")
    assert not _visible(con, "src")
