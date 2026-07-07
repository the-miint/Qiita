"""Shared sequence-chunking constants and the chunking SQL expression.

Chunking is **not** done in Python — both the orchestrator (`stage_local_fasta`)
and the CLI (`reference load`) parse FASTA with miint's `read_fastx` and split
each sequence into 64 KB pieces with miint's native `sequence_split` scalar
(`sequence_split(seq, chunk_size) -> LIST(STRUCT(chunk_index INTEGER, chunk_data
VARCHAR))`); `UNNEST` it to get one row per chunk. Both call sites build the
chunking expression from `sequence_split_expr` so the chunk width is
single-sourced.

`sequence_split` replaced a pure-SQL `list_transform`/`substring` macro that was
**O(L²)** on large single records (host reference genomes, hundreds of MB to
multi-GB): inside a lambda a captured column loses the statistics that select
`substring`'s O(1) ASCII fast path, so `substring` falls back to the Unicode
path and rescans from byte 0 on every chunk — total work quadratic in the record
length (DuckDB #23229; see `duckdb-lambda-captured-column-quadratic-bug.md`). The
native function is a single linear pass (duckdb-miint #121; see
`duckdb-miint-sequence-chunking-feature-request.md`) — ~480× faster on a 256 MB
record. If #23229 is fixed upstream the macro becomes linear again and this can
revert to pure SQL.

The constants single-source the chunk width and the ~1 GB row-group size shared
by the chunked-Parquet write path and the CLI's DoPut batches, so a tuning change
lands in one place.

(This module replaced the old `fasta_chunker.py` Python parser, which was
removed once both call sites moved to `read_fastx` — see the project memory
`fasta-parsing-uses-read-fastx`.)
"""

CHUNK_SIZE = 65_536  # bytes per chunk_data cell (64 KB)
CHUNK_ROW_GROUP_SIZE = 16_384  # rows per Parquet row group (~1 GB at 64 KB chunks)


def sequence_split_expr(seq: str) -> str:
    """SQL expression splitting the sequence column/expression `seq` into
    `CHUNK_SIZE`-byte chunks via miint's native `sequence_split`: a LIST of
    `{chunk_index INTEGER, chunk_data VARCHAR}` structs. `UNNEST` it for chunk
    rows, e.g. ``UNNEST(sequence_split_expr("sequence")) AS c`` then
    ``c.chunk_index`` / ``c.chunk_data``.

    `CHUNK_SIZE` is baked in so the chunk width is single-sourced across both
    call sites (orchestrator `stage_local_fasta`, CLI `reference load`). Plain
    SQL text (no duckdb import here); the caller executes it on a connection that
    has miint loaded.
    """
    return f"sequence_split({seq}, {CHUNK_SIZE})"


def reassemble_chunks_expr(prefix: str = "") -> str:
    """SQL aggregate that reassembles a sequence from its chunk rows — the
    inverse of `sequence_split_expr`. Concatenates `chunk_data` in `chunk_index`
    order; use it under a GROUP BY on the chunk table's key column, e.g.
    ``SELECT feature_idx, {reassemble_chunks_expr()} AS sequence FROM ... GROUP BY
    feature_idx``.

    `prefix` qualifies the columns with a table alias when the query needs it
    (e.g. ``"c."`` → ``string_agg(c.chunk_data, '' ORDER BY c.chunk_index)``).
    Single-sourcing this next to `sequence_split_expr` pins the chunk contract —
    the `chunk_data` / `chunk_index` column names and the concatenation order —
    to one place for both directions. Plain SQL text; the caller executes it on a
    connection that has miint loaded.
    """
    return f"string_agg({prefix}chunk_data, '' ORDER BY {prefix}chunk_index)"
