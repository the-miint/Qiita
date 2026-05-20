"""Isolated unit tests for `hash_sequences.execute`.

Calls `execute()` directly (not through LocalBackend / run_native_job)
so failures point at the canonical-hash / dedup / chunk logic, not
framework wiring. The full-stack happy path against a real staged
upload Parquet lives in the integration suite added by Cycle 5; this
file covers the branches that path won't exercise:

  - Reverse-complement collapse: a read and its revcomp share one
    canonical hash; manifest preserves both rows.
  - 64 KB chunking matches the Linux runbook constants
    (`_CHUNK_SIZE = 65536`, `ROW_GROUP_SIZE 16384`).
  - Output schema shape (column names + DuckDB types) — locked here
    because mint-features and write-membership read these files.
  - Empty upload (zero rows in, three empty parquets out, schema
    preserved).

The upload-side Parquet shape — `(read_id VARCHAR, sequence VARCHAR)` —
is the contract the CLI's DoPut writes; we synthesize it directly with
DuckDB COPY so tests don't depend on the Rust data plane.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import duckdb
import pytest


def _run(inputs, workspace) -> dict:
    from qiita_compute_orchestrator.jobs.hash_sequences import execute

    return asyncio.run(execute(inputs, workspace))


def _write_upload_parquet(path: Path, reads: list[tuple[str, str]]) -> Path:
    """Synthesize an `upload.parquet` with the same shape the CLI's DoPut
    writes: `(read_id VARCHAR, sequence VARCHAR)`. Zero-row inputs build
    via an empty SELECT so the file still carries the typed schema."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(":memory:") as conn:
        if reads:
            values_sql = ", ".join("(CAST(? AS VARCHAR), CAST(? AS VARCHAR))" for _ in reads)
            params: list[str] = []
            for rid, seq in reads:
                params.extend([rid, seq])
            conn.execute(
                f"COPY (SELECT * FROM (VALUES {values_sql}) AS t(read_id, sequence)) "
                f"TO '{path}' (FORMAT PARQUET)",
                params,
            )
        else:
            conn.execute(
                f"COPY (SELECT CAST(NULL AS VARCHAR) AS read_id, "
                f"CAST(NULL AS VARCHAR) AS sequence WHERE FALSE) "
                f"TO '{path}' (FORMAT PARQUET)"
            )
    return path


def _read_manifest(path: Path) -> list[tuple[str, str, int]]:
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            "SELECT read_id, CAST(sequence_hash AS VARCHAR), sequence_length_bp "
            f"FROM read_parquet('{path}') ORDER BY read_id"
        ).fetchall()


def _read_reference_sequence(path: Path) -> list[tuple[str, int]]:
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            "SELECT CAST(sequence_hash AS VARCHAR), sequence_length_bp "
            f"FROM read_parquet('{path}') ORDER BY sequence_hash"
        ).fetchall()


def _read_chunks(path: Path) -> list[tuple[str, int, str]]:
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            "SELECT CAST(sequence_hash AS VARCHAR), chunk_index, chunk_data "
            f"FROM read_parquet('{path}') ORDER BY sequence_hash, chunk_index"
        ).fetchall()


def _inputs(*, upload_path: Path):
    from qiita_compute_orchestrator.jobs.hash_sequences import Inputs

    return Inputs(upload_path=upload_path, reference_idx=1, work_ticket_idx=42)


def test_canonical_hashing_collapses_revcomp_duplicates(tmp_path):
    """A read and its reverse complement must share one canonical
    sequence_hash. `reference_sequence.parquet` carries one row for the
    pair; `manifest.parquet` keeps both source reads, both bound to the
    shared hash."""
    upload = _write_upload_parquet(
        tmp_path / "upload" / "upload.parquet",
        [
            ("r_forward", "ATCG"),
            ("r_revcomp", "CGAT"),  # reverse complement of ATCG
            ("r_other", "AAAA"),  # canonical is AAAA (LEAST vs TTTT)
        ],
    )

    outputs = _run(_inputs(upload_path=upload), tmp_path / "ws")

    manifest = _read_manifest(outputs["manifest"])
    ref_seq = _read_reference_sequence(outputs["reference_sequence"])

    # Manifest preserves every upload read.
    assert len(manifest) == 3
    by_read = {row[0]: row for row in manifest}
    assert by_read["r_forward"][1] == by_read["r_revcomp"][1], (
        "ATCG and its reverse complement CGAT must hash to the same canonical UUID"
    )
    assert by_read["r_forward"][1] != by_read["r_other"][1]
    # Length tracks the *source* read, not the canonical form.
    assert by_read["r_forward"][2] == 4
    assert by_read["r_revcomp"][2] == 4
    assert by_read["r_other"][2] == 4

    # reference_sequence.parquet has one row per unique canonical hash.
    assert len(ref_seq) == 2
    seq_hashes = {row[0] for row in ref_seq}
    assert by_read["r_forward"][1] in seq_hashes
    assert by_read["r_other"][1] in seq_hashes


def test_chunks_at_64kb(tmp_path):
    """A 200 KB sequence produces 4 chunks (65_536 + 65_536 + 65_536 +
    3_392). chunk_index runs 0..3 contiguously."""
    long_seq = "A" * 200_000
    upload = _write_upload_parquet(
        tmp_path / "upload" / "upload.parquet",
        [("long_read", long_seq)],
    )

    outputs = _run(_inputs(upload_path=upload), tmp_path / "ws")

    chunks = _read_chunks(outputs["reference_sequence_chunks"])
    assert len(chunks) == 4
    indices = [row[1] for row in chunks]
    assert indices == [0, 1, 2, 3]

    sizes = [len(row[2]) for row in chunks]
    assert sizes == [65_536, 65_536, 65_536, 200_000 - 3 * 65_536]

    # Re-assembly round-trips to the canonical form (upper-cased input).
    reassembled = "".join(row[2] for row in chunks)
    assert reassembled == long_seq


def test_chunks_use_canonical_form_not_source_bytes(tmp_path):
    """When the upload's source bytes sort *greater* than their reverse
    complement, the chunks must come from the canonical (smaller) form,
    not the raw upload. The all-`A` happy path in test_chunks_at_64kb
    is ambiguous because poly-A is already canonical — this test catches
    the bug where chunking accidentally reads the source `sequence` column
    instead of `canonical_sequence`.

    Construct an upload whose canonical form is the reverse complement:
    a long run of `T` (revcomp = `A` < `T`, so canonical is the
    all-`A` form). The reassembled chunks must equal the canonical
    A-string, not the original T-string."""
    n = 200
    source = "T" * n
    upload = _write_upload_parquet(
        tmp_path / "upload" / "upload.parquet",
        [("greater_than_revcomp", source)],
    )

    outputs = _run(_inputs(upload_path=upload), tmp_path / "ws")

    chunks = _read_chunks(outputs["reference_sequence_chunks"])
    reassembled = "".join(row[2] for row in chunks)
    assert reassembled == "A" * n, (
        "chunks must come from the canonical sequence (reverse-complemented "
        "to the lexicographically smaller strand), not the source upload bytes"
    )


def test_outputs_schema_shape(tmp_path):
    """Lock the column names + types on the three output Parquets —
    downstream consumers (mint-features, write-membership, register-files)
    bind to these names. Schema drift here without a coordinated update
    is a contract break."""
    upload = _write_upload_parquet(
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

        # reference_sequence.parquet: (sequence_hash UUID, sequence_length_bp BIGINT)
        cols = conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{outputs['reference_sequence']}')"
        ).fetchall()
        by_name = {c[0]: c[1] for c in cols}
        assert by_name == {
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
    """Empty upload (zero reads) is legal: three output Parquets exist,
    each with zero rows and the same schema as a non-empty run.
    reference-add tolerates a zero-sequence upload (e.g., loading just
    taxonomy + tree against an existing reference's sequences)."""
    upload = _write_upload_parquet(tmp_path / "upload" / "upload.parquet", [])

    outputs = _run(_inputs(upload_path=upload), tmp_path / "ws")

    for key in ("manifest", "reference_sequence", "reference_sequence_chunks"):
        assert outputs[key].exists(), f"{key} parquet not written"

    assert _read_manifest(outputs["manifest"]) == []
    assert _read_reference_sequence(outputs["reference_sequence"]) == []
    assert _read_chunks(outputs["reference_sequence_chunks"]) == []


def test_missing_upload_raises_file_not_found(tmp_path):
    """`execute()` raises FileNotFoundError when the upload path is
    missing. The framework dispatcher (run_native_job) maps that to
    BackendFailure(BAD_INPUT) — assert here that the bare exception is
    correctly raised so the mapping holds."""
    with pytest.raises(FileNotFoundError):
        _run(_inputs(upload_path=tmp_path / "does-not-exist.parquet"), tmp_path / "ws")
