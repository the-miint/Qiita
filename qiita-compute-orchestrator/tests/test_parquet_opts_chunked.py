"""The chunked option set keeps its base set's purpose.

The chunked set only adds the chunk row-group count, so it must still carry no compression
level. There is no at-rest chunked set: every registered chunk file comes from a sorted
COPY, which takes its row count from the chunks' width instead
(`qiita_common.parquet.sorted_row_group_size`).
"""

from __future__ import annotations

from qiita_common.chunking import CHUNK_ROW_GROUP_SIZE

from qiita_compute_orchestrator import miint
from qiita_compute_orchestrator.miint import PARQUET_OPTS_CHUNKED_TRANSFER


def test_chunked_transfer_adds_the_chunk_row_group_count():
    assert PARQUET_OPTS_CHUNKED_TRANSFER.endswith(f", ROW_GROUP_SIZE {CHUNK_ROW_GROUP_SIZE}")


def test_chunked_transfer_sets_no_level():
    assert "COMPRESSION_LEVEL" not in PARQUET_OPTS_CHUNKED_TRANSFER


def test_there_is_no_fixed_count_at_rest_chunked_set():
    assert not hasattr(miint, "PARQUET_OPTS_CHUNKED_AT_REST")
