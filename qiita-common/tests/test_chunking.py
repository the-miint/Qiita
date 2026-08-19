"""Unit tests for the shared sequence-chunking SQL expression builders.

Plain SQL-text builders (no duckdb here) — the real split↔reassemble round
trip runs in the orchestrator job tests that execute them on a miint-loaded
connection. These pin the emitted SQL and the chunk contract (the
`chunk_data` / `chunk_index` column names and concatenation order) that the
split and reassemble sides share.
"""

from __future__ import annotations

from qiita_common.chunking import CHUNK_SIZE, reassemble_chunks_expr, sequence_split_expr


def test_sequence_split_expr_bakes_in_chunk_size():
    assert sequence_split_expr("sequence1") == f"sequence_split(sequence1, {CHUNK_SIZE})"


def test_reassemble_chunks_expr_default_columns():
    assert reassemble_chunks_expr() == "string_agg(chunk_data, '' ORDER BY chunk_index)"


def test_reassemble_chunks_expr_qualifies_with_prefix():
    # A table alias (e.g. `c.`) is threaded onto both columns so the expression
    # works inside a join that aliases the chunk relation.
    assert reassemble_chunks_expr("c.") == "string_agg(c.chunk_data, '' ORDER BY c.chunk_index)"
