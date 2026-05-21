"""Isolated unit tests for `hash_sequences.execute`.

Calls `execute()` directly (not through LocalBackend / run_native_job)
so failures point at the canonical-hash / chunk logic, not framework
wiring. The full-stack happy path against a real staged upload Parquet
lives in the integration suite; this file covers the branches that
path won't exercise:

  - Reverse-complement collapse: a read and its revcomp share one
    canonical hash; manifest preserves both rows; chunks survive for
    only one of them (the lex-smallest read_id).
  - Stored chunks are the ORIGINAL upload bytes — not the canonical
    (lex-smaller) strand.
  - Output schema shape (column names + DuckDB types) — locked here
    because mint-features and reference_load read these files.
  - Empty upload (zero rows in, two empty parquets out, schema
    preserved).

The upload-side Parquet shape — `(read_id VARCHAR, chunk_index INTEGER,
chunk_data VARCHAR)` — is the contract the CLI's DoPut writes; we
synthesize it directly with DuckDB COPY so tests don't depend on the
Rust data plane or pyarrow.flight.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import duckdb
import pytest


def _run(inputs, workspace) -> dict:
    from qiita_compute_orchestrator.jobs.hash_sequences import execute

    return asyncio.run(execute(inputs, workspace))


_CHUNK_SIZE = 65_536


def _write_chunked_upload(path: Path, reads: list[tuple[str, str]]) -> Path:
    """Synthesize an `upload.parquet` with the chunked CLI-side shape:
    `(read_id VARCHAR, chunk_index INTEGER, chunk_data VARCHAR)`. Each
    record gets split into 64 KB chunks; a zero-read input builds via
    an empty SELECT so the file still carries the typed schema."""
    path.parent.mkdir(parents=True, exist_ok=True)
    rows: list[tuple[str, int, str]] = []
    for read_id, seq in reads:
        if not seq:
            rows.append((read_id, 0, ""))
            continue
        for i in range(0, len(seq), _CHUNK_SIZE):
            chunk = seq[i : i + _CHUNK_SIZE]
            rows.append((read_id, i // _CHUNK_SIZE, chunk))

    with duckdb.connect(":memory:") as conn:
        if rows:
            values_sql = ", ".join(
                "(CAST(? AS VARCHAR), CAST(? AS INTEGER), CAST(? AS VARCHAR))" for _ in rows
            )
            params: list = []
            for rid, idx, data in rows:
                params.extend([rid, idx, data])
            conn.execute(
                f"COPY (SELECT * FROM (VALUES {values_sql}) "
                "AS t(read_id, chunk_index, chunk_data)) "
                f"TO '{path}' (FORMAT PARQUET)",
                params,
            )
        else:
            conn.execute(
                f"COPY (SELECT CAST(NULL AS VARCHAR) AS read_id, "
                f"CAST(NULL AS INTEGER) AS chunk_index, "
                f"CAST(NULL AS VARCHAR) AS chunk_data WHERE FALSE) "
                f"TO '{path}' (FORMAT PARQUET)"
            )
    return path


def _read_manifest(path: Path) -> list[tuple[str, str, int]]:
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            "SELECT read_id, CAST(sequence_hash AS VARCHAR), sequence_length_bp "
            f"FROM read_parquet('{path}') ORDER BY read_id"
        ).fetchall()


def _read_chunks(path: Path) -> list[tuple[str, int, str]]:
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            "SELECT CAST(sequence_hash AS VARCHAR), chunk_index, chunk_data "
            f"FROM read_parquet('{path}') ORDER BY sequence_hash, chunk_index"
        ).fetchall()


def _inputs(*, upload_path: Path):
    from qiita_compute_orchestrator.jobs.hash_sequences import Inputs

    return Inputs(fasta_path=upload_path, reference_idx=1, work_ticket_idx=42)


def test_canonical_hashing_collapses_revcomp_duplicates(tmp_path):
    """A read and its reverse complement must share one canonical
    sequence_hash. Manifest preserves both source reads (both bound to
    the shared hash); chunks survive for only one (the lex-smallest
    read_id, deterministically)."""
    upload = _write_chunked_upload(
        tmp_path / "upload" / "upload.parquet",
        [
            ("r_forward", "ATCG"),
            ("r_revcomp", "CGAT"),  # reverse complement of ATCG
            ("r_other", "AAAA"),
        ],
    )

    outputs = _run(_inputs(upload_path=upload), tmp_path / "ws")

    manifest = _read_manifest(outputs["manifest"])

    # Manifest preserves every upload read.
    assert len(manifest) == 3
    by_read = {row[0]: row for row in manifest}
    assert by_read["r_forward"][1] == by_read["r_revcomp"][1], (
        "ATCG and its reverse complement CGAT must hash to the same canonical UUID"
    )
    assert by_read["r_forward"][1] != by_read["r_other"][1]
    # Length tracks the source read.
    assert by_read["r_forward"][2] == 4
    assert by_read["r_revcomp"][2] == 4
    assert by_read["r_other"][2] == 4

    # Chunks: two unique canonical hashes survive (revcomp pair collapses).
    chunks = _read_chunks(outputs["reference_sequence_chunks"])
    unique_hashes = {row[0] for row in chunks}
    assert len(unique_hashes) == 2


def test_stored_chunks_preserve_source_bytes(tmp_path):
    """The chunks file stores the bytes the client uploaded — NOT the
    reverse complement. Even when the source sorts lexicographically
    greater than its revcomp (so the canonical hash comes from the
    revcomp side), the chunk_data column carries the original strand."""
    n = 200
    source = "T" * n  # revcomp = "A"*n; canonical hash comes from the A side
    upload = _write_chunked_upload(
        tmp_path / "upload" / "upload.parquet",
        [("greater_than_revcomp", source)],
    )

    outputs = _run(_inputs(upload_path=upload), tmp_path / "ws")

    chunks = _read_chunks(outputs["reference_sequence_chunks"])
    reassembled = "".join(row[2] for row in chunks)
    assert reassembled == source, (
        "chunks must store the original upload bytes, not the canonical strand"
    )


def test_revcomp_pair_keeps_lex_smallest_read_id_chunks(tmp_path):
    """When two reads collapse to the same canonical hash, the chunks
    file keeps ONE of them — the lex-smallest read_id. This is the
    deterministic dedup rule the SELECT DISTINCT ON enforces."""
    upload = _write_chunked_upload(
        tmp_path / "upload" / "upload.parquet",
        [
            ("zz_revcomp", "CGAT"),
            ("aa_forward", "ATCG"),  # lex-smaller read_id
        ],
    )

    outputs = _run(_inputs(upload_path=upload), tmp_path / "ws")

    chunks = _read_chunks(outputs["reference_sequence_chunks"])
    # Exactly one chunk row survives (4-byte sequence fits in one chunk).
    assert len(chunks) == 1
    # The bytes belong to the lex-smaller read_id's source — "ATCG".
    assert chunks[0][2] == "ATCG"


def test_chunks_at_64kb(tmp_path):
    """A 200 KB sequence produces 4 chunks (65_536 + 65_536 + 65_536 +
    3_392). chunk_index runs 0..3 contiguously and reassembly round-trips."""
    long_seq = "A" * 200_000
    upload = _write_chunked_upload(
        tmp_path / "upload" / "upload.parquet",
        [("long_read", long_seq)],
    )

    outputs = _run(_inputs(upload_path=upload), tmp_path / "ws")

    chunks = _read_chunks(outputs["reference_sequence_chunks"])
    assert len(chunks) == 4
    assert [row[1] for row in chunks] == [0, 1, 2, 3]
    assert [len(row[2]) for row in chunks] == [65_536, 65_536, 65_536, 200_000 - 3 * 65_536]
    assert "".join(row[2] for row in chunks) == long_seq


def test_outputs_schema_shape(tmp_path):
    """Lock the column names + types on the two output Parquets —
    downstream consumers (mint-features, write-membership, reference_load)
    bind to these names. Schema drift here without a coordinated update
    is a contract break."""
    upload = _write_chunked_upload(
        tmp_path / "upload" / "upload.parquet",
        [("r1", "ATCG"), ("r2", "TTTT")],
    )

    outputs = _run(_inputs(upload_path=upload), tmp_path / "ws")

    with duckdb.connect(":memory:") as conn:
        # manifest.parquet: (read_id VARCHAR, sequence_hash UUID, sequence_length_bp BIGINT)
        cols = conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{outputs['manifest']}')"
        ).fetchall()
        by_name = {c[0]: c[1] for c in cols}
        assert by_name == {
            "read_id": "VARCHAR",
            "sequence_hash": "UUID",
            "sequence_length_bp": "BIGINT",
        }

        # reference_sequence_chunks.parquet:
        # (sequence_hash UUID, chunk_index INTEGER, chunk_data VARCHAR)
        cols = conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{outputs['reference_sequence_chunks']}')"
        ).fetchall()
        by_name = {c[0]: c[1] for c in cols}
        assert by_name == {
            "sequence_hash": "UUID",
            "chunk_index": "INTEGER",
            "chunk_data": "VARCHAR",
        }


def test_empty_upload_produces_empty_parquets(tmp_path):
    """Empty upload (zero reads) is legal: both output Parquets exist
    with zero rows and the same schema as a non-empty run."""
    upload = _write_chunked_upload(tmp_path / "upload" / "upload.parquet", [])

    outputs = _run(_inputs(upload_path=upload), tmp_path / "ws")

    for key in ("manifest", "reference_sequence_chunks"):
        assert outputs[key].exists(), f"{key} parquet not written"

    assert _read_manifest(outputs["manifest"]) == []
    assert _read_chunks(outputs["reference_sequence_chunks"]) == []


def test_missing_upload_raises_file_not_found(tmp_path):
    """`execute()` raises FileNotFoundError when the upload path is
    missing. The framework dispatcher (run_native_job) maps that to
    BackendFailure(BAD_INPUT) — assert here that the bare exception is
    correctly raised so the mapping holds."""
    with pytest.raises(FileNotFoundError):
        _run(_inputs(upload_path=tmp_path / "does-not-exist.parquet"), tmp_path / "ws")
