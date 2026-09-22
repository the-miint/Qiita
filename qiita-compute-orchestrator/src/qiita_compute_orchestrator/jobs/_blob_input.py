"""Resolve a companion-file input that arrives in one of two shapes.

Reference ingest has two front-ends, and they hand a companion file (Newick,
jplace, GFF3) to the same step in different forms:

  * remote (`reference-add`)       — the CLI DoPuts the file, so the step receives
                                     a chunked-BLOB upload Parquet
                                     `(chunk_index INTEGER, chunk_data BLOB)`.
  * local  (`local-reference-add`) — "no bytes cross the wire": the step receives
                                     the RAW absolute path to the file itself.

miint's readers (`read_newick`, `read_jplace`, `read_gff`) all parse an on-disk
text file, so the upload shape has to be stitched back into one. Doing that
unconditionally is wrong on the local path — `read_parquet()` on a raw `.nwk`
raises. This helper sniffs which shape it was handed and does the right thing.

Read ingest (`fastq-to-parquet`, `bam-to-parquet`) takes the same two front-ends
and adds one constraint: miint detects gzip from the `.gz` extension
(<https://the-miint.github.io/duckdb-miint/reading/>), and the uploaded bytes
keep whatever compression the client's file had — a FASTQ is not worth sending
decompressed. `resolve_reads_blob_input` therefore names the stitched file
from its own leading bytes rather than from anything the client claimed.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# The exact column set the data plane's DoPut writer lays down for a chunked
# BLOB upload. Sniffing on this (rather than on the file extension, or on
# "does read_parquet succeed") is what distinguishes an upload envelope from a
# companion file that happens to BE a Parquet — e.g. taxonomy, which is passed
# through as Parquet in both modes and must never be unwrapped.
_BLOB_UPLOAD_COLUMNS = frozenset({"chunk_index", "chunk_data"})


def _is_parquet(path: Path) -> bool:
    """True iff `path` carries Parquet's `PAR1` magic.

    A POSITIVE test, deliberately. The obvious alternative — try to read it as
    Parquet and treat any error as "raw file" — passes by DEFAULT: a permissions
    error, a truncated upload, or a quote in the filename would all be reclassified
    as "this is a raw companion file", and those bytes would then be handed to
    `read_gff` / `read_newick` as if they were text. Checking the magic means an
    unrelated failure raises from the reader, loudly, instead of silently taking the
    other branch.
    """
    try:
        with path.open("rb") as f:
            return f.read(4) == b"PAR1"
    except OSError:
        return False


def _is_chunked_blob_upload(conn: duckdb.DuckDBPyConnection, path: Path) -> bool:
    """True iff `path` is a chunked-BLOB upload Parquet, not a raw companion file."""
    if not _is_parquet(path):
        # A raw Newick / jplace / GFF3 on the local path.
        return False
    # DESCRIBE, not parquet_schema(): a Parquet leaf column reports `num_children`
    # as NULL (not 0), so the obvious `WHERE num_children = 0` filter matches
    # nothing and every upload would look like a raw file.
    columns = {
        row[0]
        for row in conn.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
    }
    # EXACT match, not a subset. The FASTA upload is
    # `(read_id, chunk_index, chunk_data)` — a strict SUPERSET of the blob envelope —
    # so a subset test would answer "yes, that's a blob upload" for it and silently
    # stitch a FASTA's chunks into a file. No caller does that today; an exact match
    # means none can start to by accident.
    return columns == _BLOB_UPLOAD_COLUMNS


def resolve_blob_input(
    conn: duckdb.DuckDBPyConnection,
    *,
    path: Path,
    out_path: Path,
) -> Path:
    """Return an on-disk file miint's readers can parse.

    A raw companion file is returned unchanged. A chunked-BLOB upload Parquet is
    stitched into `out_path` in `chunk_index` order, fetched in batches so a
    multi-GB jplace never materialises in memory.
    """
    if not _is_chunked_blob_upload(conn, path):
        return path

    out_path.parent.mkdir(parents=True, exist_ok=True)
    cursor = conn.execute(
        "SELECT chunk_data FROM read_parquet(?) ORDER BY chunk_index", [str(path)]
    )
    with out_path.open("wb") as f:
        while True:
            rows = cursor.fetchmany(1024)
            if not rows:
                break
            for (chunk_data,) in rows:
                if chunk_data is None:
                    raise ValueError(f"{path} contains a NULL chunk_data")
                f.write(bytes(chunk_data))
    if out_path.stat().st_size == 0:
        raise ValueError(f"{path} produced an empty file — upload was malformed")
    return out_path


# gzip's magic number. It separates the two shapes each read format arrives in:
# a compressed FASTQ from a plaintext one, and a binary BAM (BGZF is gzip) from
# a text SAM.
_GZIP_MAGIC = b"\x1f\x8b"


def resolve_reads_blob_input(
    conn: duckdb.DuckDBPyConnection,
    *,
    path: Path,
    out_dir: Path,
    stem: str,
    gzipped_suffix: str,
    plain_suffix: str,
) -> Path:
    """`resolve_blob_input` for a reads file, choosing the stitched file's name.

    A raw host path is returned unchanged, as before. A chunked-BLOB upload is
    stitched into `out_dir/{stem}{suffix}`, where the suffix is chosen by
    whether the uploaded bytes start with gzip's magic number.

    The name matters because miint detects a file's compression, and its
    format, from the extension
    (<https://the-miint.github.io/duckdb-miint/reading/>) — while the upload is
    byte-exact, since the CLI does not inflate a client's `.gz` and a FASTQ is
    the one input where the inflated bytes would actually cost something. So
    the two have to be reconciled here, from the bytes rather than from
    anything the client claimed: `upload.source_filename` is a label the
    submitter chose and can be wrong about.

    Each caller says what the two answers mean for its format:

      * FASTQ — `.fastq.gz` / `.fastq`. A stitched `.fastq` holding gzip bytes
        parses as garbage; a `.fastq.gz` holding plaintext fails to inflate.
      * alignments — `.bam` / `.sam`. BGZF is gzip, so a compressed payload is
        the binary container and a plaintext one is text SAM.
    """
    if not _is_chunked_blob_upload(conn, path):
        return path
    head = conn.execute(
        "SELECT chunk_data FROM read_parquet(?) ORDER BY chunk_index LIMIT 1", [str(path)]
    ).fetchone()
    if head is None or head[0] is None:
        raise ValueError(f"{path} carries no chunks — upload was malformed")
    suffix = gzipped_suffix if bytes(head[0][:2]) == _GZIP_MAGIC else plain_suffix
    return resolve_blob_input(conn, path=path, out_path=out_dir / f"{stem}{suffix}")
