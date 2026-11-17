"""Tests for reading a tab-separated table whose first line names its columns."""

import os
from pathlib import Path

import duckdb
import pytest

from qiita_control_plane.delimited_text import read_headed_tsv

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
    """Tests the case where the file holds a byte that is not UTF-8, in the
    header or in a row: the read is refused naming the source."""
    path = tmp_path / "table.tsv"
    path.write_bytes(raw)
    with pytest.raises(ValueError, match=f"{_SOURCE_NAME} at .* is not UTF-8 text"):
        read_headed_tsv(path, source_name=_SOURCE_NAME)
