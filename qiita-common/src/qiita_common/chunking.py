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


def canonical_sequence_hash_expr(seq: str) -> str:
    """SQL expression computing the canonical content hash of the sequence
    column/expression `seq` as a 16-byte DuckDB UUID — the SINGLE source of truth
    for how a sequence maps to its `sequence_hash` (and thus its shared
    `feature_idx`).

    A sequence and its reverse complement are the same molecular entity, so we
    md5 BOTH strands (upper-cased) and keep the lex-smaller:

        LEAST(md5(upper(seq)), md5(revcomp(upper(seq))))::uuid

    Every producer that mints/dedups features against `qiita.feature` MUST use
    this exact expression — reference ingest (`hash_sequences`) and assembly
    ingest alike — or identical bytes would split into two feature_idx. Never
    write the VARCHAR md5 hexstring (project hash-storage rule); the `::uuid` cast
    keeps it 16 bytes to match the wire-side `sequence_hash`/`feature_idx` types.
    Plain SQL text; the caller executes it on a miint-loaded connection
    (`sequence_dna_reverse_complement` is a miint scalar honoring IUPAC).
    """
    return (
        f"LEAST(md5(upper({seq}))::uuid, md5(sequence_dna_reverse_complement(upper({seq})))::uuid)"
    )
