"""Tests for reading a tab-separated table whose first line names its columns."""

import os
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

from qiita_control_plane.delimited_text import read_headed_tsv
from qiita_control_plane.miint import duckdb_connect

_SOURCE_NAME = "probe table"


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "table.tsv"
    path.write_bytes(text.encode("utf-8"))
    return path


def test_read_headed_tsv(tmp_path):
    """Tests the case where the header carries a byte-order mark and padded
    names, and the rows carry an empty cell, a quoted cell holding a tab, and
    leading zeros: keys come back stripped, cells as text."""
    path = _write(
        tmp_path,
        '﻿sample_name\t matrix_tube_id \tnote\nS1\t0012345678\t"a\tb"\nS2\t\tplain\n',
    )
    expected = [
        {"sample_name": "S1", "matrix_tube_id": "0012345678", "note": "a\tb"},
        {"sample_name": "S2", "matrix_tube_id": "", "note": "plain"},
    ]
    assert read_headed_tsv(path, source_name=_SOURCE_NAME) == expected


def test_read_headed_tsv_header_only_returns_no_rows(tmp_path):
    """Tests the case where the file holds a header and no rows: an empty
    table, not an error."""
    path = _write(tmp_path, "sample_name\tnote\n")
    assert read_headed_tsv(path, source_name=_SOURCE_NAME) == []


@pytest.mark.parametrize(
    ("text", "match"),
    [
        ("", "is empty"),
        ("a\t\tb\nx\ty\tz\n", "blank column name"),
        ("a\t  \tb\nx\ty\tz\n", "blank column name"),
        ("a\ta\nx\ty\n", "more than once"),
        ("a\t a \nx\ty\n", "more than once"),
    ],
    ids=["empty-file", "blank-name", "whitespace-name", "duplicate-name", "duplicate-on-strip"],
)
def test_read_headed_tsv_refuses_bad_header(tmp_path, text, match):
    """Tests the case where the file is empty, or a header name is blank or
    repeats another once stripped: the read is refused by the guard for that
    fault."""
    path = _write(tmp_path, text)
    with pytest.raises(ValueError, match=match):
        read_headed_tsv(path, source_name=_SOURCE_NAME)


@pytest.mark.parametrize(
    "make_path", [lambda d: d, lambda d: d / "absent.tsv"], ids=["dir", "missing"]
)
def test_read_headed_tsv_refuses_path_that_is_not_a_file(tmp_path, make_path):
    """Tests the case where `path` is a directory or does not exist: the read is
    refused as not a file, before anything is opened."""
    with pytest.raises(ValueError, match="is not a file"):
        read_headed_tsv(make_path(tmp_path), source_name=_SOURCE_NAME)


@pytest.mark.parametrize("name", ["a'b", "a\\b", "a}b", "a:b", 'a"b', "a,b", "a{b"])
def test_read_headed_tsv_round_trips_punctuated_names(tmp_path, name):
    """Tests the case where a header name carries punctuation that the column
    spec escapes or embeds: the row comes back keyed on exactly that name."""
    path = _write(tmp_path, f"{name}\tz\n1\t2\n")
    assert read_headed_tsv(path, source_name=_SOURCE_NAME) == [{name: "1", "z": "2"}]


@pytest.mark.parametrize(
    "text",
    ["a\tb\tc\nx\ty\n", "a\tb\tc\nx\ty\tz\tw\n"],
    ids=["short-row", "long-row"],
)
def test_read_headed_tsv_refuses_ragged_row(tmp_path, text):
    """Tests the case where a row holds fewer or more cells than the header
    names: the read is refused."""
    path = _write(tmp_path, text)
    with pytest.raises(duckdb.Error):
        read_headed_tsv(path, source_name=_SOURCE_NAME)


def test_read_headed_tsv_unquotes_quoted_names(tmp_path):
    """Tests the case where a header name is quoted, as a spreadsheet quotes a
    cell holding a tab or a quote mark: the row comes back keyed on the
    unquoted name."""
    path = _write(tmp_path, '"a\tb"\t"say ""hi"""\nx\ty\n')
    assert read_headed_tsv(path, source_name=_SOURCE_NAME) == [{"a\tb": "x", 'say "hi"': "y"}]


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads through any permission bit")
def test_read_headed_tsv_raises_permission_error_for_unreadable_parent(tmp_path):
    """Tests the case where the file's parent directory cannot be traversed:
    the read raises the permission error rather than calling the file
    missing."""
    locked_dir = tmp_path / "locked"
    locked_dir.mkdir()
    path = _write(locked_dir, "a\tb\nx\ty\n")
    locked_dir.chmod(0)
    try:
        with pytest.raises(PermissionError):
            read_headed_tsv(path, source_name=_SOURCE_NAME)
    finally:
        locked_dir.chmod(0o755)


@pytest.mark.parametrize(
    "raw",
    [b"\xffa\tb\nx\ty\n", b"a\tb\n\xff\tq\n"],
    ids=["bad-byte-in-header", "bad-byte-in-row"],
)
def test_read_headed_tsv_refuses_text_that_is_not_utf8(tmp_path, raw):
    """Tests the case where the file holds a byte that is not UTF-8 within the
    first block Python decodes: the read is refused naming the source."""
    path = tmp_path / "table.tsv"
    path.write_bytes(raw)
    with pytest.raises(ValueError, match=f"{_SOURCE_NAME} at .* is not UTF-8 text"):
        read_headed_tsv(path, source_name=_SOURCE_NAME)


def test_read_headed_tsv_refuses_bad_byte_past_header_block(tmp_path):
    """Tests the case where the only byte that is not UTF-8 lies beyond the block
    Python decodes for the header: DuckDB's read refuses it."""
    path = tmp_path / "table.tsv"
    path.write_bytes(b"a\tb\n" + b"x\ty\n" * 5000 + b"z\t\xff\n")
    with pytest.raises(duckdb.Error, match="not utf-8 encoded"):
        read_headed_tsv(path, source_name=_SOURCE_NAME)


def test_duckdb_read_csv_takes_wider_later_row_as_header(tmp_path):
    """Tests the case where a later row is wider than the header line and DuckDB
    detects the header itself: it names the columns from that later row without
    an error, which is why the header line is read in Python."""
    path = _write(tmp_path, "a\tb\n1\t2\n3\t4\t5\n")
    conn = duckdb_connect()
    try:
        cursor = conn.execute("SELECT * FROM read_csv(?, delim='\t', header=true)", [str(path)])
        column_names = [column[0] for column in cursor.description]
    finally:
        conn.close()
    assert column_names == ["3", "4", "5"]


def test_duckdb_read_csv_reports_no_file_for_directory(tmp_path):
    """Tests the case where DuckDB is handed a directory holding a table: it
    reports only that no file matches, which is why the path is checked first."""
    (tmp_path / "table.tsv").write_text("a\n1\n", encoding="utf-8")
    conn = duckdb_connect()
    try:
        with pytest.raises(duckdb.IOException, match="No files found that match the pattern"):
            conn.execute("SELECT * FROM read_csv(?, delim='\t')", [str(tmp_path)])
    finally:
        conn.close()


def test_duckdb_renames_column_differing_only_in_case():
    """Tests the case where a registered table holds two column names that differ
    only in ASCII case: DuckDB renames the later one, which is why the writer
    refuses such names."""
    table = pa.table({"Biosample_idx": ["a"], "biosample_idx": ["1"]})
    conn = duckdb_connect()
    try:
        conn.register("case_repeat", table)
        cursor = conn.execute("SELECT * FROM case_repeat")
        column_names = [column[0] for column in cursor.description]
    finally:
        conn.close()
    assert column_names == ["Biosample_idx", "biosample_idx_1"]
