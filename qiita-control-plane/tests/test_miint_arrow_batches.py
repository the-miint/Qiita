"""`arrow_batches`, and the `filterwarnings` entries that keep a copying
`execute(...).to_arrow_reader()` from coming back unnoticed."""

from __future__ import annotations

import re
import tomllib
import warnings
from pathlib import Path

import duckdb
import pytest

from qiita_control_plane.actions.library import _CHUNK_SIZE
from qiita_control_plane.cli.reference_load import _CHUNK_ROWS_PER_BATCH
from qiita_control_plane.miint import arrow_batches, converted_batches

_REPO = Path(__file__).resolve().parents[2]

# A query whose rows past the first 900,000 raise: a reader that copies the whole
# result first raises at the call, one that streams hands over its first batch.
_LATE_ERROR = (
    "SELECT CASE WHEN range < 900000 THEN range ELSE error('late') END AS x FROM range(1000000)"
)


@pytest.mark.parametrize("rows_per_batch", [_CHUNK_SIZE, _CHUNK_ROWS_PER_BATCH])
def test_batches_hold_the_rows_per_batch_asked_for(rows_per_batch):
    # The sizes the control plane's callers ask for.
    con = duckdb.connect()
    total = 2 * rows_per_batch + rows_per_batch // 2
    reader = arrow_batches(con, "SELECT range AS x FROM range(?)", [total], rows_per_batch)
    assert [batch.num_rows for batch in reader] == [
        rows_per_batch,
        rows_per_batch,
        rows_per_batch // 2,
    ]


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


async def test_converted_batches_convert_each_non_empty_batch_off_the_event_loop():
    """`convert` sees every row once, never an empty batch, and runs on a thread other
    than the event loop's; so does the read, which runs beside it."""
    import threading

    loop_thread = threading.get_ident()
    convert_threads: list[int] = []

    def convert(batch):
        assert batch.num_rows
        convert_threads.append(threading.get_ident())
        return batch.column("x").to_pylist()

    con = duckdb.connect()
    got = [
        chunk
        async for chunk in converted_batches(
            con, "SELECT range AS x FROM range(?) WHERE range % 2 = 0", [10], 2, convert
        )
    ]
    assert [x for chunk in got for x in chunk] == [0, 2, 4, 6, 8]
    assert convert_threads and loop_thread not in convert_threads


async def test_converted_batches_of_an_empty_result_yield_nothing():
    con = duckdb.connect()
    got = [
        chunk
        async for chunk in converted_batches(
            con, "SELECT range AS x FROM range(0)", [], 2, lambda b: b.num_rows
        )
    ]
    assert got == []
