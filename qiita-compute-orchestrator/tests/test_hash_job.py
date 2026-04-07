"""Tests for LocalBackend hash job."""

import hashlib
import json
import uuid

import pytest


@pytest.fixture
def fasta_file(tmp_path):
    """Create a 5-sequence FASTA file."""
    seqs = {
        "seq1": "ATCGATCGATCG",
        "seq2": "GCTAGCTAGCTA",
        "seq3": "AAATTTTCCCGGG",
        "seq4": "TTTTAAAACCCC",
        "seq5": "GGGGCCCCAAAA",
    }
    path = tmp_path / "test.fasta"
    with open(path, "w") as f:
        for name, seq in seqs.items():
            f.write(f">{name}\n{seq}\n")
    return path, seqs


async def test_hash_job_produces_manifest(fasta_file, tmp_path):
    """Hash job must produce a manifest with one entry per sequence."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    fasta_path, seqs = fasta_file
    output_dir = tmp_path / "output"

    backend = LocalBackend()
    manifest_path = await backend.run_hash_job(
        fasta_path=fasta_path,
        output_dir=output_dir,
        reference_idx=1,
    )

    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text())
    assert len(manifest["entries"]) == 5


async def test_hash_job_md5_matches_python(fasta_file, tmp_path):
    """DuckDB md5() output must match Python hashlib.md5()."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    fasta_path, seqs = fasta_file
    output_dir = tmp_path / "output"

    backend = LocalBackend()
    manifest_path = await backend.run_hash_job(
        fasta_path=fasta_path,
        output_dir=output_dir,
        reference_idx=1,
    )

    manifest = json.loads(manifest_path.read_text())
    for entry in manifest["entries"]:
        seq = seqs[entry["read_id"]]
        expected_hash = hashlib.md5(seq.encode()).hexdigest()
        expected_uuid = str(uuid.UUID(expected_hash))
        assert entry["sequence_hash"] == expected_uuid, (
            f"MD5 mismatch for {entry['read_id']}: "
            f"DuckDB={entry['sequence_hash']}, Python={expected_uuid}"
        )


async def test_hash_job_manifest_has_required_fields(fasta_file, tmp_path):
    """Each manifest entry must have read_id, sequence_hash, and length."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    fasta_path, _ = fasta_file
    output_dir = tmp_path / "output"

    backend = LocalBackend()
    manifest_path = await backend.run_hash_job(
        fasta_path=fasta_path,
        output_dir=output_dir,
        reference_idx=1,
    )

    manifest = json.loads(manifest_path.read_text())
    for entry in manifest["entries"]:
        assert "read_id" in entry
        assert "sequence_hash" in entry
        assert "length" in entry
        assert isinstance(entry["length"], int)
        assert entry["length"] > 0


async def test_hash_job_manifest_includes_reference_idx(fasta_file, tmp_path):
    """Manifest must include the reference_idx it was created for."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    fasta_path, _ = fasta_file
    output_dir = tmp_path / "output"

    backend = LocalBackend()
    manifest_path = await backend.run_hash_job(
        fasta_path=fasta_path,
        output_dir=output_dir,
        reference_idx=42,
    )

    manifest = json.loads(manifest_path.read_text())
    assert manifest["reference_idx"] == 42


async def test_hash_job_rejects_missing_fasta(tmp_path):
    """Hash job must raise if FASTA file does not exist."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    backend = LocalBackend()
    with pytest.raises(FileNotFoundError):
        await backend.run_hash_job(
            fasta_path=tmp_path / "nonexistent.fasta",
            output_dir=tmp_path / "output",
            reference_idx=1,
        )


async def test_hash_job_rejects_duplicate_read_ids(tmp_path):
    """Hash job must raise ValueError on duplicate read_ids in FASTA."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    fasta_path = tmp_path / "dup.fasta"
    fasta_path.write_text(">seq1\nATCG\n>seq1\nGCTA\n")

    backend = LocalBackend()
    with pytest.raises(ValueError, match="duplicate read_id"):
        await backend.run_hash_job(
            fasta_path=fasta_path,
            output_dir=tmp_path / "output",
            reference_idx=1,
        )


async def test_hash_job_empty_fasta(tmp_path):
    """Hash job on an empty FASTA must produce a manifest with zero entries."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    fasta_path = tmp_path / "empty.fasta"
    fasta_path.write_text("")

    backend = LocalBackend()
    manifest_path = await backend.run_hash_job(
        fasta_path=fasta_path,
        output_dir=tmp_path / "output",
        reference_idx=1,
    )

    manifest = json.loads(manifest_path.read_text())
    assert manifest["entries"] == []
