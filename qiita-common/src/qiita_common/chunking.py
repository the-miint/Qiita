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
lands in one place. `normalized_sequence_expr` does the same for case: the split
and the canonical hash both route through it, so stored bytes and the hash that
keyed them agree.

(This module replaced the old `fasta_chunker.py` Python parser, which was
removed once both call sites moved to `read_fastx` — see the project memory
`fasta-parsing-uses-read-fastx`.)
"""

CHUNK_SIZE = 65_536  # bytes per chunk_data cell (64 KB)
CHUNK_ROW_GROUP_SIZE = 16_384  # rows per Parquet row group (~1 GB at 64 KB chunks)


def normalized_sequence_expr(seq: str) -> str:
    """SQL expression normalizing the sequence column/expression `seq` to the form
    Qiita stores and hashes: upper case. `sequence_split_expr` (what gets stored)
    and `canonical_sequence_hash_expr` (what mints the `feature_idx`) both route
    through here, so the bytes in `*_sequence_chunks` and the hash that keyed them
    cannot disagree about case. The canonical hash folds case, so two records
    differing only in case share one `feature_idx`; a split that preserved case
    wrote different bytes under that one key, leaving the survivor to the lake's
    replace-by-key and so to load order.

    Case is discarded at load and is not recoverable from the lake, which costs
    the repeat-masking of a submitted FASTA. Measured 2026-08-24 against the
    team-mirror miint build (`rype 1.0.0-rc.2-48-g345aee9`, `minimap2 0477498`) on
    a reference differing only in a 20 kb soft-masked repeat: every index built
    from it is byte-identical to its uppercase twin's — `rype_index_create`
    (k=64, w=20), `save_minimap2_index` at presets sr / map-hifi / map-ont / asm5,
    and `save_bowtie2_index` — while a one-base edit to the same reference changes
    all of them. That covers the four index builders that read `chunk_data`.

    Strand is deliberately NOT normalized, and the asymmetry with case is the
    point rather than an omission. The case argument above is that every consumer
    of `chunk_data` is case-blind, measured. No such argument exists for strand:
    stored bytes are COORDINATE-BEARING. `qiita.reference_annotation` records
    `position` / `stop_position` as 1-based half-open offsets into the parent
    feature's sequence, on the same axis as `alignment_slice` / `read_alignments`
    / `qiita_lake.alignment`, alongside a `strand` column. Reverse-complementing
    what is stored would relocate every interval on that sequence and invert every
    strand value, so a GFF-bearing reference would keep loading, keep indexing,
    and start answering wrong. Normalizing here is therefore not a trade against
    export fidelity — it is unsound.

    The consequence kept instead: the canonical hash folds strands, so a sequence
    and its reverse complement share one `feature_idx` while their stored bytes
    differ, and which orientation survives follows load order. `REPLACE_KEY_TABLES`
    in `qiita-data-plane/src/flight_service.rs` carries why the newest load's
    strand wins and why keeping the older one is not expressible; the export-side
    consequence is on the control plane's `_write_genome_fasta`.
    """
    return f"upper({seq})"


# What a front-end tells the submitter when their FASTA is soft-masked. One sentence
# for both chunking front-ends (`reference load`, `stage_local_fasta`) so the two
# cannot describe the same discarded masking differently. `%s` names the file(s).
SOFT_MASK_WARNING = (
    "soft-masked (lower case) bases in %s. Chunks are stored upper case, so the"
    " masking is discarded at load and cannot be recovered from the lake — an export"
    " of this reference will return upper case. Every index builder discards case as"
    " well, so alignment and classification are unaffected."
)


def soft_masked_expr(seq: str) -> str:
    """SQL predicate, true for a record carrying soft-masked (lower case) bases —
    i.e. one whose stored form will differ from what was submitted. Built from
    `normalized_sequence_expr`, the same expression `sequence_split_expr` routes
    through, so the predicate cannot disagree with what the split discards.

    Pairs with `SOFT_MASK_WARNING`, which is what to say when it matches.
    """
    return f"{seq} <> {normalized_sequence_expr(seq)}"


def sequence_split_expr(seq: str) -> str:
    """SQL expression splitting the sequence column/expression `seq` into
    `CHUNK_SIZE`-byte chunks via miint's native `sequence_split`: a LIST of
    `{chunk_index INTEGER, chunk_data VARCHAR}` structs. `UNNEST` it for chunk
    rows, e.g. ``UNNEST(sequence_split_expr("sequence")) AS c`` then
    ``c.chunk_index`` / ``c.chunk_data``.

    The sequence is normalized through `normalized_sequence_expr` before it is
    split, so the stored `chunk_data` is upper case regardless of the submitted
    casing; that function's docstring is the one home for why.

    `CHUNK_SIZE` is baked in so the chunk width is single-sourced across every call
    site. Plain SQL text (no duckdb import here); the caller executes it on a
    connection that has miint loaded.
    """
    return f"sequence_split({normalized_sequence_expr(seq)}, {CHUNK_SIZE})"


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


def canonical_sequence_hash_expr(seq: str) -> str:
    """SQL expression computing the canonical content hash of the sequence
    column/expression `seq` as a 16-byte DuckDB UUID — the SINGLE source of truth
    for how a sequence maps to its `sequence_hash` (and thus its shared
    `feature_idx`).

    A sequence and its reverse complement are the same molecular entity, so we
    md5 BOTH strands — each `normalized_sequence_expr`-normalized, the same way
    the stored chunks are — and keep the lex-smaller:

        LEAST(md5(upper(seq)), md5(revcomp(upper(seq))))::uuid

    Every producer that mints/dedups features against `qiita.feature` MUST use
    this exact expression — reference ingest (`hash_sequences`) and assembly
    ingest alike — or identical bytes would split into two feature_idx. Never
    write the VARCHAR md5 hexstring (project hash-storage rule); the `::uuid` cast
    keeps it 16 bytes to match the wire-side `sequence_hash`/`feature_idx` types.
    Plain SQL text; the caller executes it on a miint-loaded connection
    (`sequence_dna_reverse_complement` is a miint scalar honoring IUPAC).
    """
    norm = normalized_sequence_expr(seq)
    return f"LEAST(md5({norm})::uuid, md5(sequence_dna_reverse_complement({norm}))::uuid)"
