"""The Parquet helpers `miint` adds to `qiita_common.parquet`'s option sets.

`parquet_bytes_per_row` is the row width a sorted at-rest writer sizes its row groups by,
read from the source's footers. The chunked option set only adds the chunk row-group
count, so it must still carry no compression level. There is no at-rest chunked set: every
registered chunk file comes from a sorted COPY, which takes its row count from the chunks'
width instead (`qiita_common.parquet.sorted_row_group_size`).
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from qiita_common.chunking import CHUNK_ROW_GROUP_SIZE

from qiita_compute_orchestrator import miint
from qiita_compute_orchestrator.miint import PARQUET_OPTS_CHUNKED_TRANSFER, parquet_bytes_per_row


def test_bytes_per_row_reads_the_footers_of_a_glob(tmp_path: Path):
    with duckdb.connect() as conn:
        for i in range(2):
            conn.execute(
                "COPY (SELECT repeat('A', 1000) || range AS s FROM range(500)) "
                f"TO '{tmp_path / f'part_{i}.parquet'}' (FORMAT PARQUET, COMPRESSION 'snappy')"
            )
        # Decompressed but still encoded: the 1,000-odd characters plus a length prefix.
        assert 1000 < parquet_bytes_per_row(conn, tmp_path / "part_*.parquet") < 1100


def test_bytes_per_row_refuses_a_file_with_no_rows(tmp_path: Path):
    empty = tmp_path / "empty.parquet"
    with duckdb.connect() as conn:
        conn.execute(f"COPY (SELECT 'x' AS s WHERE false) TO '{empty}' (FORMAT PARQUET)")
        with pytest.raises(ValueError, match="no rows or no size"):
            parquet_bytes_per_row(conn, empty)


def test_chunked_transfer_adds_the_chunk_row_group_count():
    assert PARQUET_OPTS_CHUNKED_TRANSFER.endswith(f", ROW_GROUP_SIZE {CHUNK_ROW_GROUP_SIZE}")


def test_chunked_transfer_sets_no_level():
    assert "COMPRESSION_LEVEL" not in PARQUET_OPTS_CHUNKED_TRANSFER


def test_there_is_no_fixed_count_at_rest_chunked_set():
    assert not hasattr(miint, "PARQUET_OPTS_CHUNKED_AT_REST")
