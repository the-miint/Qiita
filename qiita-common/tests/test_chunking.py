"""Unit tests for the shared sequence-chunking SQL expression builders.

Plain SQL-text builders (no duckdb here) — the real split↔reassemble round
trip runs in the orchestrator job tests that execute them on a miint-loaded
connection. These pin the emitted SQL and the chunk contract (the
`chunk_data` / `chunk_index` column names and concatenation order) that the
split and reassemble sides share.
"""

from __future__ import annotations

from qiita_common.chunking import (
    CHUNK_SIZE,
    canonical_sequence_hash_expr,
    normalized_sequence_expr,
    reassemble_chunks_expr,
    sequence_split_expr,
)


def test_sequence_split_expr_bakes_in_chunk_size():
    # The `upper(...)` literal is pinned once, by the normalization test below.
    norm = normalized_sequence_expr("sequence1")
    assert sequence_split_expr("sequence1") == f"sequence_split({norm}, {CHUNK_SIZE})"


def test_reassemble_chunks_expr_default_columns():
    assert reassemble_chunks_expr() == "string_agg(chunk_data, '' ORDER BY chunk_index)"


def test_reassemble_chunks_expr_qualifies_with_prefix():
    # A table alias (e.g. `c.`) is threaded onto both columns so the expression
    # works inside a join that aliases the chunk relation.
    assert reassemble_chunks_expr("c.") == "string_agg(c.chunk_data, '' ORDER BY c.chunk_index)"


def test_normalized_sequence_expr_upper_cases():
    assert normalized_sequence_expr("sequence1") == "upper(sequence1)"


def test_split_and_canonical_hash_normalize_through_the_same_expression():
    # The stored bytes and the hash that keys them must not disagree about case:
    # a soft-masked record and its uppercase twin share one feature_idx, so if
    # only one side normalized, two loads would write different bytes under one
    # key and the lake's replace-by-key would settle it by load order.
    norm = normalized_sequence_expr("s")
    assert norm in sequence_split_expr("s")
    # Both strands of the hash, so neither fold arm can drift from the split.
    assert canonical_sequence_hash_expr("s").count(norm) == 2
