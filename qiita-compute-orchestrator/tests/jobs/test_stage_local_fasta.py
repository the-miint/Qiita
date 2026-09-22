"""Isolated unit tests for `stage_local_fasta.execute`.

`stage_local_fasta` is the local-ingest front-end: it reads a manifest of
absolute FASTA paths (one per line), parses every file with miint's `read_fastx`
and chunks in DuckDB, and writes a DIRECTORY of chunked Parquet parts —
`(read_id VARCHAR, chunk_index INTEGER, chunk_data VARCHAR)`, exactly the shape
`hash_sequences` already consumes (it reads the directory with `read_parquet`)
— so the rest of the reference-add pipeline runs unchanged.

These tests call `execute()` directly (not through LocalBackend /
run_native_job) so failures point at the manifest-parse / chunk / dup-check
logic, not framework wiring. The output parts are synthesized and read back with
DuckDB so tests don't depend on the Rust data plane or pyarrow.

Coverage:
  - Happy path: 3-file manifest → a `fasta_chunks/` directory, correct schema,
    every read_id present, chunks reassemble.
  - A manifest longer than `_MANIFEST_BATCH_FILES` writes one part per batch and
    loses no reads, and BOTH passes hand `read_fastx` no more than one batch at a
    time — the manifest is batched because a single call holds every path open
    (duckdb-miint#260).
  - A pass-2 failure after earlier parts are on disk removes the whole parts
    directory, so a partial set is never promoted as the step's output.
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
import logging
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
    """Read the step's whole output. `path` is the parts DIRECTORY — the same
    value hash_sequences hands to `read_parquet`, which reads a directory of
    Parquet files as one relation."""
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            "SELECT read_id, chunk_index, chunk_data "
            f"FROM read_parquet('{path}') ORDER BY read_id, chunk_index"
        ).fetchall()


def _parts(path: Path) -> list[Path]:
    return sorted(path.glob("part_*.parquet"))


class _SpyConn:
    """Records every `execute` the job issues, and can fail a chosen `COPY`.

    The batching is invisible in the output of pass 1 — an unbatched pass 1
    builds the identical `read_sanity` table — so the only way to pin it is to
    watch the path lists actually handed to `read_fastx`. `fail_on_copy` drives
    the partial-parts cleanup, which needs a failure AFTER a part is on disk.
    """

    def __init__(self, inner, fail_on_copy: int | None = None):
        self._inner = inner
        self._fail_on_copy = fail_on_copy
        self._copies = 0
        self.read_fastx_batch_sizes: list[int] = []

    def __enter__(self):
        self._inner.__enter__()
        return self

    def __exit__(self, *exc):
        return self._inner.__exit__(*exc)

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def execute(self, sql, parameters=None):
        if "read_fastx" in sql and parameters:
            self.read_fastx_batch_sizes.append(len(parameters[0]))
        if sql.lstrip().startswith("COPY"):
            self._copies += 1
            if self._fail_on_copy is not None and self._copies > self._fail_on_copy:
                raise RuntimeError("probe: COPY failed mid-run")
        return self._inner.execute(sql, parameters) if parameters else self._inner.execute(sql)


def _spy_conn(monkeypatch, *, fail_on_copy: int | None = None) -> list[_SpyConn]:
    """Install a `_SpyConn` around `open_miint_conn` and return the list it
    appends each opened connection to."""
    from qiita_compute_orchestrator.jobs import stage_local_fasta

    opened: list[_SpyConn] = []
    real = stage_local_fasta.open_miint_conn

    def _open():
        spy = _SpyConn(real(), fail_on_copy=fail_on_copy)
        opened.append(spy)
        return spy

    monkeypatch.setattr(stage_local_fasta, "open_miint_conn", _open)
    return opened


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


async def test_three_files_combined_into_parts_directory(tmp_path):
    """A manifest of three small FASTA files produces a parts DIRECTORY keyed
    `fasta_path`, with every read_id present and reassembling."""
    fa1 = _write_fasta(tmp_path / "a.fa", [("g1", "ACGT"), ("g2", "TTTT")])
    fa2 = _write_fasta(tmp_path / "b.fa", [("g3", "GGGGCCCC")])
    fa3 = _write_fasta(tmp_path / "c.fa", [("g4", "AATTCCGG")])
    manifest = _write_manifest(tmp_path / "m.txt", [str(fa1), str(fa2), str(fa3)])

    outputs = await _run(manifest, tmp_path / "ws")

    assert set(outputs) == {"fasta_path"}
    assert outputs["fasta_path"].is_dir()
    # One batch, so one part — the batching is exercised in its own test below.
    assert [p.name for p in _parts(outputs["fasta_path"])] == ["part_00000.parquet"]

    rows = _read_combined(outputs["fasta_path"])
    by_read = {rid: data for rid, _idx, data in rows}
    assert set(by_read) == {"g1", "g2", "g3", "g4"}
    assert by_read["g3"] == "GGGGCCCC"
    assert by_read["g4"] == "AATTCCGG"


async def test_manifest_longer_than_batch_writes_a_part_per_batch(tmp_path, monkeypatch):
    """The manifest is fed to `read_fastx` in `_MANIFEST_BATCH_FILES`-sized
    slices because one call holds every listed path open for its whole life
    (duckdb-miint#260). Pass 2 writes a part per batch; pass 1 accumulates into
    one temp table, so the cross-file dup check still spans every batch."""
    from qiita_compute_orchestrator.jobs import stage_local_fasta

    monkeypatch.setattr(stage_local_fasta, "_MANIFEST_BATCH_FILES", 2)
    files = [_write_fasta(tmp_path / f"g{i}.fa", [(f"g{i}", "ACGT" * (i + 1))]) for i in range(5)]
    manifest = _write_manifest(tmp_path / "m.txt", [str(f) for f in files])

    outputs = await _run(manifest, tmp_path / "ws")

    # 5 paths at 2 per batch -> 3 parts, the last one short.
    assert [p.name for p in _parts(outputs["fasta_path"])] == [
        "part_00000.parquet",
        "part_00001.parquet",
        "part_00002.parquet",
    ]
    by_read = {rid: data for rid, _idx, data in _read_combined(outputs["fasta_path"])}
    assert by_read == {f"g{i}": "ACGT" * (i + 1) for i in range(5)}


async def test_both_passes_hand_read_fastx_one_batch_at_a_time(tmp_path, monkeypatch):
    """Neither pass may pass the whole manifest to `read_fastx` — that is the
    defect, and the part count alone does not catch it: an unbatched pass 1
    builds the identical `read_sanity` table and would still emit one part per
    pass-2 batch. Watch the path lists instead."""
    from qiita_compute_orchestrator.jobs import stage_local_fasta

    monkeypatch.setattr(stage_local_fasta, "_MANIFEST_BATCH_FILES", 2)
    opened = _spy_conn(monkeypatch)
    files = [_write_fasta(tmp_path / f"g{i}.fa", [(f"g{i}", "ACGT")]) for i in range(5)]
    manifest = _write_manifest(tmp_path / "m.txt", [str(f) for f in files])

    await _run(manifest, tmp_path / "ws")

    # 5 paths, 2 per batch, two passes -> 3 + 3 calls, none over the batch size.
    sizes = opened[0].read_fastx_batch_sizes
    assert sizes == [2, 2, 1, 2, 2, 1]
    assert max(sizes) <= stage_local_fasta._MANIFEST_BATCH_FILES


async def test_pass_two_failure_removes_every_part_written_so_far(tmp_path, monkeypatch):
    """A batched pass 2 can fail with earlier parts already on disk. The launcher's
    manifest walker runs after execute() and lists whatever it finds, so a partial
    set must not survive the failure."""
    from qiita_compute_orchestrator.jobs import stage_local_fasta

    monkeypatch.setattr(stage_local_fasta, "_MANIFEST_BATCH_FILES", 1)
    _spy_conn(monkeypatch, fail_on_copy=2)
    files = [_write_fasta(tmp_path / f"g{i}.fa", [(f"g{i}", "ACGT")]) for i in range(4)]
    manifest = _write_manifest(tmp_path / "m.txt", [str(f) for f in files])

    workspace = tmp_path / "ws"
    with pytest.raises(RuntimeError, match="COPY failed"):
        await _run(manifest, workspace)
    # Parts 0 and 1 were written before the third COPY raised.
    assert not (workspace / "fasta_chunks").exists()


async def test_duplicate_read_id_in_a_later_batch_still_raises(tmp_path, monkeypatch):
    """The dup check reads the accumulated temp table, not one batch, so a
    read_id repeated across a batch boundary is still caught — and the failure
    leaves no parts directory for the launcher's manifest walker to promote."""
    from qiita_compute_orchestrator.jobs import stage_local_fasta

    monkeypatch.setattr(stage_local_fasta, "_MANIFEST_BATCH_FILES", 1)
    fa1 = _write_fasta(tmp_path / "a.fa", [("dup", "ACGT")])
    fa2 = _write_fasta(tmp_path / "b.fa", [("g1", "TTTT")])
    fa3 = _write_fasta(tmp_path / "c.fa", [("dup", "GGGG")])
    manifest = _write_manifest(tmp_path / "m.txt", [str(fa1), str(fa2), str(fa3)])

    workspace = tmp_path / "ws"
    with pytest.raises(ValueError, match="dup"):
        await _run(manifest, workspace)
    assert not (workspace / "fasta_chunks").exists()


async def test_soft_masked_input_warns_and_stores_upper(tmp_path, caplog):
    """A soft-masked record warns and is stored upper case; an all-uppercase
    manifest warns not at all. `--local` reaches the same split as the remote
    DoPut path, so it discards case the same way and must say so too."""
    fa = _write_fasta(tmp_path / "masked.fa", [("g1", "ACGTacgtTTGA")])
    manifest = _write_manifest(tmp_path / "m.txt", [str(fa)])
    logger = "qiita_compute_orchestrator.jobs.stage_local_fasta"

    with caplog.at_level(logging.WARNING, logger=logger):
        outputs = await _run(manifest, tmp_path / "ws")

    assert "soft-masked" in caplog.text
    assert str(fa) in caplog.text
    by_read = {rid: data for rid, _idx, data in _read_combined(outputs["fasta_path"])}
    assert by_read["g1"] == "ACGTACGTTTGA"

    caplog.clear()
    plain = _write_fasta(tmp_path / "plain.fa", [("g2", "ACGTACGTTTGA")])
    manifest2 = _write_manifest(tmp_path / "m2.txt", [str(plain)])
    with caplog.at_level(logging.WARNING, logger=logger):
        await _run(manifest2, tmp_path / "ws2")
    assert "soft-masked" not in caplog.text


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
