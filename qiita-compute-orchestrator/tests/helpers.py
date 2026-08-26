"""Shared test constants and builders for qiita-compute-orchestrator."""

from pathlib import Path

import duckdb

# Canonical test sequences shared across hash and load job tests.
TEST_SEQUENCES = {
    "seq1": "ATCGATCGATCG",
    "seq2": "GCTAGCTAGCTA",
    "seq3": "AAATTTTCCCGGG",
    "seq4": "TTTTAAAACCCC",
    "seq5": "GGGGCCCCAAAA",
}


def write_chunked_blob_upload(dest: Path, payload: bytes) -> Path:
    """Write the `(chunk_index INTEGER, chunk_data BLOB)` Parquet the data
    plane's DoPut writer produces for a chunked-BLOB upload.

    Two chunks, inserted out of order, so a reassembly that ignores
    `chunk_index` and takes row order produces the wrong bytes rather than
    passing by luck. Every step that resolves a `*_upload_idx` handle meets
    this shape, so it is built here rather than per test module — the envelope
    is the data plane's, and a change to it should break one writer.
    """
    half = max(1, len(payload) // 2)
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TABLE up (chunk_index INTEGER, chunk_data BLOB)")
        conn.execute("INSERT INTO up VALUES (1, ?), (0, ?)", [payload[half:], payload[:half]])
        conn.execute(f"COPY up TO '{dest}' (FORMAT PARQUET)")
    return dest
