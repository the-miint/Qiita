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
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# The exact column set the data plane's DoPut writer lays down for a chunked
# BLOB upload. Sniffing on this (rather than on the file extension, or on
# "does read_parquet succeed") is what distinguishes an upload envelope from a
# companion file that happens to BE a Parquet — e.g. taxonomy, which is passed
# through as Parquet in both modes and must never be unwrapped.
_BLOB_UPLOAD_COLUMNS = {"chunk_index", "chunk_data"}


def is_chunked_blob_upload(conn: duckdb.DuckDBPyConnection, path: Path) -> bool:
    """True iff `path` is a chunked-BLOB upload Parquet, not a raw companion file."""
    try:
        # DESCRIBE, not parquet_schema(): a Parquet leaf column reports
        # `num_children` as NULL (not 0), so the obvious `WHERE num_children = 0`
        # filter matches nothing and every upload looks like a raw file.
        columns = {
            row[0]
            for row in conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
        }
    except duckdb.Error:
        # Not a Parquet at all — a raw Newick / jplace / GFF3 on the local path.
        return False
    return _BLOB_UPLOAD_COLUMNS.issubset(columns)


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
    if not is_chunked_blob_upload(conn, path):
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
