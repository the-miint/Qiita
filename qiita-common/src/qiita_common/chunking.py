"""Shared sequence-chunking constants.

Chunking itself is **not** done in Python — both the orchestrator
(`stage_local_fasta`) and the CLI (`reference load`) parse FASTA with miint's
`read_fastx` and split sequences into 64 KB pieces with a DuckDB
`list_transform`/`UNNEST` macro. These constants single-source the chunk width
and the ~1 GB row-group size shared by the chunked-Parquet write path and the
CLI's DoPut batches, so a tuning change lands in one place.

(This module replaced the old `fasta_chunker.py` Python parser, which was
removed once both call sites moved to `read_fastx` — see the project memory
`fasta-parsing-uses-read-fastx`.)
"""

CHUNK_SIZE = 65_536  # bytes per chunk_data cell (64 KB)
CHUNK_ROW_GROUP_SIZE = 16_384  # rows per Parquet row group (~1 GB at 64 KB chunks)

# DuckDB scalar macro that splits one sequence into 64 KB chunks: returns a LIST
# of `{chunk_index, chunk_data}` structs. UNNEST it to get chunk rows. This is
# the single definition of qiita's chunking — both the orchestrator's
# `stage_local_fasta` (chunking a staged reads table) and the CLI's read_fastx
# upload stream `CREATE OR REPLACE` it on their connection, then select
# `UNNEST(chunk_list(<seq_column>))`. CHUNK_SIZE is baked in so the chunk width
# is single-sourced. It is plain SQL text (no duckdb import here); the caller
# executes it. See the `sequence-chunking-strategy` project memory.
CHUNK_LIST_MACRO_SQL = (
    "CREATE OR REPLACE TEMP MACRO chunk_list(seq) AS "
    f"list_transform(range(0, CEIL(length(seq) / {CHUNK_SIZE}.0)::INTEGER), "
    f"i -> {{'chunk_index': i::INTEGER, "
    f"'chunk_data': substring(seq, i * {CHUNK_SIZE} + 1, {CHUNK_SIZE})}})"
)
