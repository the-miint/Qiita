"""`arrow_batches`, and the warning the test suites turn into an error so a copying
`execute(...).to_arrow_reader()` cannot come back unnoticed."""

from __future__ import annotations

import duckdb
import pytest

from qiita_control_plane.miint import arrow_batches


def test_batches_hold_the_rows_per_batch_asked_for():
    con = duckdb.connect()
    reader = arrow_batches(con, "SELECT range AS x FROM range(?)", [10], 4)
    assert [batch.num_rows for batch in reader] == [4, 4, 2]


def test_an_exhausted_reader_frees_the_connection():
    con = duckdb.connect()
    list(arrow_batches(con, "SELECT range AS x FROM range(?)", [10], 4))
    assert con.sql("SELECT 42").fetchall() == [(42,)]


def test_arrow_from_execute_still_warns():
    # The `filterwarnings` entry in each pyproject matches this message. If DuckDB
    # drops or rewords it, that entry silently stops guarding anything.
    con = duckdb.connect()
    with pytest.warns(UserWarning, match="Arrow output on a DB-API result"):
        con.execute("SELECT 1").to_arrow_reader(1)
