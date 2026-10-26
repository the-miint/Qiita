"""Unit tests for `qiita_common.sql`."""

from pathlib import Path

import pytest

from qiita_common.sql import sql_string_literal


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("plain", "'plain'"),
        ("", "''"),
        ("O'Brien", "'O''Brien'"),
        ("'leading and trailing'", "'''leading and trailing'''"),
        ("a'b'c'd", "'a''b''c''d'"),
        ("/tmp/no quotes here/part.parquet", "'/tmp/no quotes here/part.parquet'"),
    ],
)
def test_sql_string_literal_str(value: str, expected: str):
    """Tests the case where the value is a string, with and without embedded
    single quotes; every embedded quote is doubled and the result carries its
    own surrounding quotes."""
    assert sql_string_literal(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Path("/scratch/ticket/42/read.parquet"), "'/scratch/ticket/42/read.parquet'"),
        (Path("/scratch/o'brien/read.parquet"), "'/scratch/o''brien/read.parquet'"),
    ],
)
def test_sql_string_literal_path(value: Path, expected: str):
    """Tests the case where the value is a Path, which renders from its string
    form and escapes the same way a str does."""
    assert sql_string_literal(value) == expected
