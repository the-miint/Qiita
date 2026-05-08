"""Tests for LocalBackend hash step (Parquet manifest output)."""

import hashlib
import uuid

import duckdb
import pytest


def _read_manifest(manifest_path) -> list[dict]:
    """Materialize manifest.parquet into a list of {read_id, sequence_hash,
    length} dicts. Tests use small fixtures so a full read is fine."""
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            "SELECT read_id, CAST(sequence_hash AS VARCHAR) AS sequence_hash, length "
            "FROM read_parquet(?) ORDER BY read_id",
            [str(manifest_path)],
        ).fetchall()
    return [{"read_id": r[0], "sequence_hash": r[1], "length": r[2]} for r in rows]


async def test_hash_job_produces_manifest(fasta_file, tmp_path):
    """Hash step writes manifest.parquet with one row per sequence."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    fasta_path, _ = fasta_file
    output_dir = tmp_path / "output"

    backend = LocalBackend()
    result = await backend.run_step(
        "hash",
        {"fasta_path": fasta_path},
        output_dir,
        reference_idx=1,
        work_ticket_idx=1,
    )
    manifest_path = result["manifest"]

    assert manifest_path.exists()
    assert manifest_path.name == "manifest.parquet"
    entries = _read_manifest(manifest_path)
    assert len(entries) == 5


async def test_hash_job_md5_matches_python(fasta_file, tmp_path):
    """DuckDB md5() output (cast to UUID in the manifest) must match
    Python hashlib.md5()."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    fasta_path, seqs = fasta_file
    output_dir = tmp_path / "output"

    backend = LocalBackend()
    result = await backend.run_step(
        "hash",
        {"fasta_path": fasta_path},
        output_dir,
        reference_idx=1,
        work_ticket_idx=1,
    )
    manifest_path = result["manifest"]

    for entry in _read_manifest(manifest_path):
        seq = seqs[entry["read_id"]]
        expected_uuid = str(uuid.UUID(hashlib.md5(seq.encode()).hexdigest()))
        assert entry["sequence_hash"] == expected_uuid, (
            f"MD5 mismatch for {entry['read_id']}: "
            f"DuckDB={entry['sequence_hash']}, Python={expected_uuid}"
        )


async def test_hash_job_manifest_has_required_columns(fasta_file, tmp_path):
    """Every row carries read_id (string), sequence_hash (UUID-cast), and a
    positive integer length."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    fasta_path, _ = fasta_file
    output_dir = tmp_path / "output"

    backend = LocalBackend()
    result = await backend.run_step(
        "hash",
        {"fasta_path": fasta_path},
        output_dir,
        reference_idx=1,
        work_ticket_idx=1,
    )
    manifest_path = result["manifest"]

    for entry in _read_manifest(manifest_path):
        assert isinstance(entry["read_id"], str) and entry["read_id"]
        assert isinstance(entry["sequence_hash"], str) and len(entry["sequence_hash"]) == 36
        assert isinstance(entry["length"], int)
        assert entry["length"] > 0


async def test_hash_job_rejects_missing_fasta(tmp_path):
    """Hash step over a missing FASTA must surface as
    BackendFailure(BAD_INPUT) at the run_step boundary — typed +
    permanent so the runner won't retry."""
    from qiita_common.backend_failure import BackendFailure, FailureKind

    from qiita_compute_orchestrator.backends.local import LocalBackend

    backend = LocalBackend()
    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "hash",
            {"fasta_path": tmp_path / "nonexistent.fasta"},
            tmp_path / "output",
            reference_idx=1,
            work_ticket_idx=1,
        )
    assert ei.value.kind == FailureKind.BAD_INPUT
    assert ei.value.step_name == "hash"
    assert not ei.value.transient


async def test_hash_job_rejects_duplicate_read_ids(tmp_path):
    """Duplicate read_ids in the FASTA surface as BackendFailure(BAD_INPUT)."""
    from qiita_common.backend_failure import BackendFailure, FailureKind

    from qiita_compute_orchestrator.backends.local import LocalBackend

    fasta_path = tmp_path / "dup.fasta"
    fasta_path.write_text(">seq1\nATCG\n>seq1\nGCTA\n")

    backend = LocalBackend()
    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "hash",
            {"fasta_path": fasta_path},
            tmp_path / "output",
            reference_idx=1,
            work_ticket_idx=1,
        )
    assert ei.value.kind == FailureKind.BAD_INPUT
    assert "duplicate read_id" in ei.value.reason


async def test_hash_job_empty_fasta(tmp_path):
    """Hash step on an empty FASTA produces an empty manifest.parquet
    (zero rows, schema preserved)."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    fasta_path = tmp_path / "empty.fasta"
    fasta_path.write_text("")

    backend = LocalBackend()
    result = await backend.run_step(
        "hash",
        {"fasta_path": fasta_path},
        tmp_path / "output",
        reference_idx=1,
        work_ticket_idx=1,
    )
    manifest_path = result["manifest"]

    assert manifest_path.exists()
    assert _read_manifest(manifest_path) == []
