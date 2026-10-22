"""The chunked option sets keep their base set's purpose.

A chunked set only adds the chunk row-group count, so the at-rest one must still carry the
at-rest level and the transfer one must still carry none.
"""

from __future__ import annotations

from qiita_common.chunking import CHUNK_ROW_GROUP_SIZE
from qiita_common.parquet import PARQUET_COMPRESSION_LEVEL_AT_REST

from qiita_compute_orchestrator.miint import (
    PARQUET_OPTS_CHUNKED_AT_REST,
    PARQUET_OPTS_CHUNKED_TRANSFER,
)


def test_chunked_sets_add_the_chunk_row_group_count():
    for opts in (PARQUET_OPTS_CHUNKED_AT_REST, PARQUET_OPTS_CHUNKED_TRANSFER):
        assert opts.endswith(f", ROW_GROUP_SIZE {CHUNK_ROW_GROUP_SIZE}")


def test_chunked_at_rest_keeps_the_at_rest_level():
    assert f"COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL_AT_REST}" in PARQUET_OPTS_CHUNKED_AT_REST


def test_chunked_transfer_sets_no_level():
    assert "COMPRESSION_LEVEL" not in PARQUET_OPTS_CHUNKED_TRANSFER
