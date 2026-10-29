"""`arrow_batches`, and the `filterwarnings` entries that keep a copying
`execute(...).to_arrow_reader()` from coming back unnoticed."""

from __future__ import annotations

import re
import tomllib
import warnings
from pathlib import Path

import duckdb
import pytest

from qiita_control_plane.miint import arrow_batches

_REPO = Path(__file__).resolve().parents[2]

# A query whose rows past the first 900,000 raise: a reader that copies the whole
# result first raises at the call, one that streams hands over its first batch.
_LATE_ERROR = (
    "SELECT CASE WHEN range < 900000 THEN range ELSE error('late') END AS x FROM range(1000000)"
)


def test_batches_hold_the_rows_per_batch_asked_for():
    con = duckdb.connect()
    reader = arrow_batches(con, "SELECT range AS x FROM range(?)", [25_000], 10_000)
    assert [batch.num_rows for batch in reader] == [10_000, 10_000, 5_000]


def test_the_reader_streams():
    con = duckdb.connect()
    con.execute("SET threads = 1")
    assert arrow_batches(con, _LATE_ERROR, [], 2_048).read_next_batch().num_rows == 2_048

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with pytest.raises(duckdb.InvalidInputException, match="late"):
            con.execute(_LATE_ERROR).to_arrow_reader(2_048)


def test_a_part_read_reader_holds_the_connection_until_exhausted():
    con = duckdb.connect()
    reader = arrow_batches(con, "SELECT range AS x FROM range(?)", [25_000], 10_000)
    reader.read_next_batch()
    with pytest.raises(duckdb.ResourceInUseException):
        con.sql("SELECT 42").fetchall()
    for _ in reader:
        pass
    assert con.sql("SELECT 42").fetchall() == [(42,)]


@pytest.mark.parametrize(
    "project", ["qiita-control-plane", "qiita-compute-orchestrator", "tests/integration"]
)
def test_each_project_errors_on_arrow_from_execute(project):
    # Matched as pytest applies the entry (`warnings.filterwarnings`: from the start
    # of the message, ignoring case), against the message DuckDB emits now.
    pyproject = tomllib.loads((_REPO / project / "pyproject.toml").read_text())
    entries = pyproject["tool"]["pytest"]["ini_options"]["filterwarnings"]
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        duckdb.connect().execute("SELECT 1").to_arrow_reader(1)
    [emitted] = [str(w.message) for w in caught if w.category is UserWarning]
    assert any(
        action == "error"
        and category == "UserWarning"
        and re.match(message, emitted, re.IGNORECASE)
        for action, message, category in (entry.split(":") for entry in entries)
    ), f"{project}: no filterwarnings entry errors on {emitted!r}"
