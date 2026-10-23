"""`miint.parquet_bytes_per_row`: the row width a sorted at-rest writer sizes its row
groups by, read from the source's footers."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from qiita_compute_orchestrator.miint import parquet_bytes_per_row


def test_reads_the_footers_of_a_glob(tmp_path: Path):
    with duckdb.connect() as conn:
        for i in range(2):
            conn.execute(
                "COPY (SELECT repeat('A', 1000) || range AS s FROM range(500)) "
                f"TO '{tmp_path / f'part_{i}.parquet'}' (FORMAT PARQUET, COMPRESSION 'snappy')"
            )
        # Decompressed but still encoded: the 1,000-odd characters plus a length prefix.
        assert 1000 < parquet_bytes_per_row(conn, tmp_path / "part_*.parquet") < 1100


def test_refuses_a_file_with_no_rows(tmp_path: Path):
    empty = tmp_path / "empty.parquet"
    with duckdb.connect() as conn:
        conn.execute(f"COPY (SELECT 'x' AS s WHERE false) TO '{empty}' (FORMAT PARQUET)")
        with pytest.raises(ValueError, match="no rows or no size"):
            parquet_bytes_per_row(conn, empty)
