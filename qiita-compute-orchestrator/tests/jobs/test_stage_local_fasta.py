"""Isolated unit tests for `stage_local_fasta.execute`.

`stage_local_fasta` is the local-ingest front-end: it reads a manifest of
absolute FASTA paths (one per line), parses every file with miint's `read_fastx`
and chunks in DuckDB, and writes ONE combined chunked Parquet —
`(read_id VARCHAR, chunk_index INTEGER, chunk_data VARCHAR)`, exactly the shape
`hash_sequences` already consumes — so the rest of the reference-add pipeline
runs unchanged.

These tests call `execute()` directly (not through LocalBackend /
run_native_job) so failures point at the manifest-parse / chunk / dup-check
logic, not framework wiring. The combined output Parquet is synthesized and
read back with DuckDB so tests don't depend on the Rust data plane or pyarrow.

Coverage:
  - Happy path: 3-file manifest → one `fasta.parquet`, correct schema, every
    read_id present, chunks reassemble.
  - Dup read_id (the global genome_map join key) across files AND within a
    single file → ValueError (run_native_job maps to BAD_INPUT).
  - Empty file in the manifest is skipped (contributes no rows, no error).
  - Zero FASTA paths in the manifest → ValueError.
  - gzipped (`.fa.gz`) entries ingest transparently.
  - Manifest blank lines and `#` comments are ignored.
  - Missing FASTA path, relative manifest entry, missing/relative manifest
    → ValueError (fail fast, mirroring bcl_convert_prep's guards).
"""

from __future__ import annotations

import gzip
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError
from qiita_common.chunking import CHUNK_SIZE

from qiita_compute_orchestrator.jobs import scan_native_jobs

_MODULE = "qiita_compute_orchestrator.jobs.stage_local_fasta"


def _write_fasta(path: Path, records: list[tuple[str, str]]) -> Path:
    """Write a plain FASTA with the given (read_id, sequence) records."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for read_id, seq in records:
            f.write(f">{read_id}\n{seq}\n")
    return path


def _write_fasta_gz(path: Path, records: list[tuple[str, str]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt") as f:
        for read_id, seq in records:
            f.write(f">{read_id}\n{seq}\n")
    return path


def _write_manifest(path: Path, lines: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")
    return path


def _read_combined(path: Path) -> list[tuple[str, int, str]]:
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            "SELECT read_id, chunk_index, chunk_data "
            f"FROM read_parquet('{path}') ORDER BY read_id, chunk_index"
        ).fetchall()


def _inputs(*, manifest_path: Path):
    from qiita_compute_orchestrator.jobs.stage_local_fasta import Inputs

    return Inputs(fasta_manifest_path=manifest_path, reference_idx=1, work_ticket_idx=42)


async def _run(manifest_path: Path, workspace: Path) -> dict:
    from qiita_compute_orchestrator.jobs.stage_local_fasta import execute

    return await execute(_inputs(manifest_path=manifest_path), workspace)


# --------------------------------------------------------------------------
# Discovery + Inputs contract
# --------------------------------------------------------------------------


def test_module_passes_native_job_scan():
    """The shipped module imports cleanly and satisfies the Inputs+execute
    contract, so it is auto-registered by the boot-time scan."""
    assert _MODULE in scan_native_jobs()


def test_inputs_accepts_framework_injected_shape():
    """fasta_manifest_path (YAML-declared) + reference_idx + work_ticket_idx
    (framework-injected REFERENCE scope scalars) validate."""
    from qiita_compute_orchestrator.jobs.stage_local_fasta import Inputs

    inputs = Inputs(
        fasta_manifest_path="/data/refs/manifest.txt",
        reference_idx=7,
        work_ticket_idx=99,
    )
    assert inputs.fasta_manifest_path == Path("/data/refs/manifest.txt")
    assert inputs.reference_idx == 7
    assert inputs.work_ticket_idx == 99


def test_inputs_rejects_missing_scope_scalar():
    from qiita_compute_orchestrator.jobs.stage_local_fasta import Inputs

    with pytest.raises(ValidationError):
        Inputs(fasta_manifest_path="/data/refs/manifest.txt", work_ticket_idx=99)


# --------------------------------------------------------------------------
# Happy path
# --------------------------------------------------------------------------


async def test_three_files_combined_into_one_parquet(tmp_path):
    """A manifest of three small FASTA files produces ONE combined parquet
    keyed `fasta_path`, with every read_id present and reassembling."""
    fa1 = _write_fasta(tmp_path / "a.fa", [("g1", "ACGT"), ("g2", "TTTT")])
    fa2 = _write_fasta(tmp_path / "b.fa", [("g3", "GGGGCCCC")])
    fa3 = _write_fasta(tmp_path / "c.fa", [("g4", "AATTCCGG")])
    manifest = _write_manifest(tmp_path / "m.txt", [str(fa1), str(fa2), str(fa3)])

    outputs = await _run(manifest, tmp_path / "ws")

    assert set(outputs) == {"fasta_path"}
    assert outputs["fasta_path"].exists()
    assert outputs["fasta_path"].suffix == ".parquet"

    rows = _read_combined(outputs["fasta_path"])
    by_read = {rid: data for rid, _idx, data in rows}
    assert set(by_read) == {"g1", "g2", "g3", "g4"}
    assert by_read["g3"] == "GGGGCCCC"
    assert by_read["g4"] == "AATTCCGG"


async def test_output_schema_shape(tmp_path):
    """Lock the combined parquet schema — hash_sequences binds these names
    and types (it reads c.read_id / c.chunk_index / c.chunk_data)."""
    fa = _write_fasta(tmp_path / "a.fa", [("g1", "ACGT")])
    manifest = _write_manifest(tmp_path / "m.txt", [str(fa)])

    outputs = await _run(manifest, tmp_path / "ws")

    with duckdb.connect(":memory:") as conn:
        cols = conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{outputs['fasta_path']}')"
        ).fetchall()
    assert {c[0]: c[1] for c in cols} == {
        "read_id": "VARCHAR",
        "chunk_index": "INTEGER",
        "chunk_data": "VARCHAR",
    }


async def test_long_sequence_chunks_at_default_size(tmp_path):
    """A sequence longer than CHUNK_SIZE is split into contiguous
    chunks that reassemble to the original."""
    seq = "A" * (CHUNK_SIZE * 2 + 17)
    fa = _write_fasta(tmp_path / "a.fa", [("big", seq)])
    manifest = _write_manifest(tmp_path / "m.txt", [str(fa)])

    outputs = await _run(manifest, tmp_path / "ws")

    rows = _read_combined(outputs["fasta_path"])
    assert [idx for _rid, idx, _data in rows] == [0, 1, 2]
    assert "".join(data for _rid, _idx, data in rows) == seq


async def test_exact_multiple_chunk_boundary(tmp_path):
    """A sequence whose length is an exact multiple of CHUNK_SIZE splits into
    exactly that many full chunks — no empty trailing chunk. Guards the
    chunk-boundary behavior of miint's `sequence_split` (last chunk = remainder,
    so an exact multiple yields no empty trailing chunk)."""
    seq = "A" * (CHUNK_SIZE * 2)
    fa = _write_fasta(tmp_path / "a.fa", [("exact", seq)])
    manifest = _write_manifest(tmp_path / "m.txt", [str(fa)])

    outputs = await _run(manifest, tmp_path / "ws")

    rows = _read_combined(outputs["fasta_path"])
    assert [idx for _rid, idx, _data in rows] == [0, 1]
    assert [len(data) for _rid, _idx, data in rows] == [CHUNK_SIZE, CHUNK_SIZE]
    assert "".join(data for _rid, _idx, data in rows) == seq


async def test_gzip_entry_ingests(tmp_path):
    """A `.fa.gz` manifest entry is read transparently and contributes its
    reads to the combined output."""
    plain = _write_fasta(tmp_path / "a.fa", [("g1", "ACGT")])
    gz = _write_fasta_gz(tmp_path / "b.fa.gz", [("g2", "TTTT")])
    manifest = _write_manifest(tmp_path / "m.txt", [str(plain), str(gz)])

    outputs = await _run(manifest, tmp_path / "ws")

    rows = _read_combined(outputs["fasta_path"])
    assert {rid for rid, _idx, _data in rows} == {"g1", "g2"}


async def test_manifest_blanks_and_comments_ignored(tmp_path):
    """Blank lines and `#` comments in the manifest are skipped; only the
    real paths are ingested."""
    fa1 = _write_fasta(tmp_path / "a.fa", [("g1", "ACGT")])
    fa2 = _write_fasta(tmp_path / "b.fa", [("g2", "TTTT")])
    manifest = _write_manifest(
        tmp_path / "m.txt",
        ["# a header comment", "", str(fa1), "   ", "# another", str(fa2), ""],
    )

    outputs = await _run(manifest, tmp_path / "ws")

    rows = _read_combined(outputs["fasta_path"])
    assert {rid for rid, _idx, _data in rows} == {"g1", "g2"}


async def test_empty_file_skipped(tmp_path):
    """A manifest-listed file with zero records contributes nothing but does
    not error; sibling files still ingest."""
    fa1 = _write_fasta(tmp_path / "a.fa", [("g1", "ACGT")])
    empty = tmp_path / "empty.fa"
    empty.write_text("")
    fa2 = _write_fasta(tmp_path / "c.fa", [("g2", "TTTT")])
    manifest = _write_manifest(tmp_path / "m.txt", [str(fa1), str(empty), str(fa2)])

    outputs = await _run(manifest, tmp_path / "ws")

    rows = _read_combined(outputs["fasta_path"])
    assert {rid for rid, _idx, _data in rows} == {"g1", "g2"}


# --------------------------------------------------------------------------
# Dup read_id (global join key) → fail fast
# --------------------------------------------------------------------------


async def test_duplicate_read_id_across_files_raises(tmp_path):
    """read_id is the global genome_map join key — the same read_id in two
    files must fail fast, never be silently namespaced."""
    fa1 = _write_fasta(tmp_path / "a.fa", [("dup", "ACGT"), ("g1", "TTTT")])
    fa2 = _write_fasta(tmp_path / "b.fa", [("dup", "GGGG")])
    manifest = _write_manifest(tmp_path / "m.txt", [str(fa1), str(fa2)])

    with pytest.raises(ValueError, match="dup"):
        await _run(manifest, tmp_path / "ws")


async def test_duplicate_read_id_within_file_raises(tmp_path):
    """A read_id repeated within a single file is equally a contract
    violation."""
    fa = _write_fasta(tmp_path / "a.fa", [("dup", "ACGT"), ("dup", "TTTT")])
    manifest = _write_manifest(tmp_path / "m.txt", [str(fa)])

    with pytest.raises(ValueError, match="dup"):
        await _run(manifest, tmp_path / "ws")


async def test_empty_body_record_raises(tmp_path):
    """A header with no sequence body is bad data — a named read with no
    bytes. read_fastx surfaces it as a length-0 row; the job fails fast."""
    fa = tmp_path / "a.fa"
    fa.write_text(">empty\n>g2\nACGT\n")  # `empty` has a header but no body
    manifest = _write_manifest(tmp_path / "m.txt", [str(fa)])

    with pytest.raises(ValueError, match="empty"):
        await _run(manifest, tmp_path / "ws")


# --------------------------------------------------------------------------
# Fail-fast validation
# --------------------------------------------------------------------------


async def test_zero_fasta_files_raises(tmp_path):
    """A manifest with no real path lines (only blanks/comments) → error."""
    manifest = _write_manifest(tmp_path / "m.txt", ["# nothing here", "", "   "])
    with pytest.raises(ValueError, match="zero|no FASTA"):
        await _run(manifest, tmp_path / "ws")


async def test_missing_fasta_path_raises(tmp_path):
    """An absolute manifest entry that doesn't exist → error before COPY."""
    fa = _write_fasta(tmp_path / "a.fa", [("g1", "ACGT")])
    missing = tmp_path / "gone.fa"
    manifest = _write_manifest(tmp_path / "m.txt", [str(fa), str(missing)])
    with pytest.raises(ValueError, match="not found"):
        await _run(manifest, tmp_path / "ws")


async def test_relative_manifest_entry_raises(tmp_path):
    """A CWD-relative manifest entry is rejected — under SLURM only absolute
    shared-FS paths are visible from the compute node."""
    manifest = _write_manifest(tmp_path / "m.txt", ["relative/path.fa"])
    with pytest.raises(ValueError, match="absolute"):
        await _run(manifest, tmp_path / "ws")


async def test_relative_manifest_path_raises(tmp_path):
    """The manifest path itself must be absolute (mirrors bcl_convert_prep)."""
    from qiita_compute_orchestrator.jobs.stage_local_fasta import Inputs, execute

    inputs = Inputs(
        fasta_manifest_path="relative/manifest.txt", reference_idx=1, work_ticket_idx=42
    )
    with pytest.raises(ValueError, match="absolute"):
        await execute(inputs, tmp_path / "ws")


async def test_missing_manifest_raises(tmp_path):
    """An absolute manifest path that doesn't exist → error."""
    inputs = _inputs(manifest_path=tmp_path / "does-not-exist.txt")
    from qiita_compute_orchestrator.jobs.stage_local_fasta import execute

    with pytest.raises(ValueError, match="not found"):
        await execute(inputs, tmp_path / "ws")
