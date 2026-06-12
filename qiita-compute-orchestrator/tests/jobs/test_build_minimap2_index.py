"""Isolated unit tests for `build_minimap2_index.execute`.

Calls `execute()` directly. The real minimap2 index build
(`save_minimap2_index`) needs the miint extension and real sequence bytes, so
the heavy path is exercised by a single real-minimap2 smoke at the bottom;
everywhere else the build seam (`_run_save_minimap2_index`) is stubbed and we
assert the orchestration around it:

  - LOCAL mode reads the minimap2-tagged raw FASTA via `read_fastx` and stages a
    `(read_id, sequence1)` subject VIEW (no double materialisation);
  - UPLOAD mode reassembles the chunked `upload.parquet`
    `(read_id, chunk_index, chunk_data)` into `(read_id, sequence1)` via
    `string_agg(... ORDER BY chunk_index)`;
  - the persistent index path is `{path_derived}/references/{idx}/minimap2/index.mmi`,
    a FILE (not a directory) cleared with `unlink` on a rerun;
  - the meta JSON records index_type / fs_path / params (preset, source_files);
  - a non-success build raises RuntimeError;
  - exactly one of the two input sources must be supplied.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError


def _write_fasta(path: Path, records: list[tuple[str, str]]) -> Path:
    with open(path, "w") as f:
        for read_id, seq in records:
            f.write(f">{read_id}\n{seq}\n")
    return path


def _write_manifest(path: Path, fasta_paths: list[Path]) -> Path:
    path.write_text("\n".join(str(p) for p in fasta_paths) + "\n")
    return path


def _write_chunks_parquet(path: Path, rows: list[tuple[str, int, str]]) -> Path:
    """Write the chunked `(read_id, chunk_index, chunk_data)` upload shape."""
    with duckdb.connect(":memory:") as conn:
        values_sql = ", ".join(
            "(CAST(? AS VARCHAR), CAST(? AS INTEGER), CAST(? AS VARCHAR))" for _ in rows
        )
        params: list = []
        for read_id, cidx, data in rows:
            params.extend([read_id, cidx, data])
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values_sql}) "
            "AS t(read_id, chunk_index, chunk_data)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )
    return path


def test_build_minimap2_index_local_mode(tmp_path, monkeypatch):
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    shared_root = tmp_path / "shared"
    monkeypatch.setenv("PATH_DERIVED", str(shared_root))

    reference_idx = 7
    fa1 = _write_fasta(
        tmp_path / "a.fasta", [("chr1", "ACGTACGTACGTACGT"), ("chr2", "TTTTGGGGCCCCAAAA")]
    )
    fa2 = _write_fasta(tmp_path / "b.fasta", [("chr3", "GGGGCCCCAAAATTTT")])
    manifest = _write_manifest(tmp_path / "mm2.manifest", [fa1, fa2])

    captured: dict = {}

    def fake_save(conn, subject_table, output_path, *, preset):
        cols = [d[0] for d in conn.execute(f"SELECT * FROM {subject_table} LIMIT 0").description]
        rows = conn.execute(
            f"SELECT read_id, sequence1 FROM {subject_table} ORDER BY read_id"
        ).fetchall()
        captured.update(
            subject_table=subject_table,
            output_path=output_path,
            preset=preset,
            cols=cols,
            rows=rows,
        )
        Path(output_path).write_bytes(b"MMI")  # minimap2 writes a FILE
        return True

    monkeypatch.setattr(build_minimap2_index, "_run_save_minimap2_index", fake_save)

    inputs = build_minimap2_index.Inputs(
        minimap2_fasta_manifest=manifest,
        reference_idx=reference_idx,
        work_ticket_idx=42,
    )
    out = asyncio.run(build_minimap2_index.execute(inputs, tmp_path / "ws"))

    # Subject staged as (read_id, sequence1) over every tagged FASTA record.
    assert captured["cols"] == ["read_id", "sequence1"]
    assert captured["rows"] == [
        ("chr1", "ACGTACGTACGTACGT"),
        ("chr2", "TTTTGGGGCCCCAAAA"),
        ("chr3", "GGGGCCCCAAAATTTT"),
    ]
    assert captured["preset"] == "sr"

    expected = shared_root / "references" / str(reference_idx) / "minimap2" / "index.mmi"
    assert out["minimap2_index_path"] == expected
    assert captured["output_path"] == str(expected)
    assert expected.is_file()

    meta = json.loads(Path(out["minimap2_index_meta"]).read_text())
    assert meta["index_type"] == "minimap2"
    assert meta["fs_path"] == str(expected)
    assert meta["params"] == {"preset": "sr", "source_files": [str(fa1), str(fa2)]}


def test_build_minimap2_index_upload_mode_reassembles(tmp_path, monkeypatch):
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))

    # g1 spans two chunks written out-of-order to prove ORDER BY chunk_index.
    parquet = _write_chunks_parquet(
        tmp_path / "upload.parquet",
        [("g1", 1, "GGGG"), ("g1", 0, "ACGT"), ("g2", 0, "TTTTCCCC")],
    )

    captured: dict = {}

    def fake_save(conn, subject_table, output_path, *, preset):
        captured["cols"] = [
            d[0] for d in conn.execute(f"SELECT * FROM {subject_table} LIMIT 0").description
        ]
        captured["rows"] = conn.execute(
            f"SELECT read_id, sequence1 FROM {subject_table} ORDER BY read_id"
        ).fetchall()
        Path(output_path).write_bytes(b"MMI")
        return True

    monkeypatch.setattr(build_minimap2_index, "_run_save_minimap2_index", fake_save)

    inputs = build_minimap2_index.Inputs(fasta_path=parquet, reference_idx=3, work_ticket_idx=1)
    out = asyncio.run(build_minimap2_index.execute(inputs, tmp_path / "ws"))

    assert captured["cols"] == ["read_id", "sequence1"]
    assert captured["rows"] == [("g1", "ACGTGGGG"), ("g2", "TTTTCCCC")]

    meta = json.loads(Path(out["minimap2_index_meta"]).read_text())
    assert meta["params"] == {"preset": "sr", "source_files": [str(parquet)]}


def test_build_minimap2_index_clears_stale_index_on_rerun(tmp_path, monkeypatch):
    """A retry re-runs against the same persistent path; any prior `.mmi` is
    cleared first (it's a FILE, so `unlink`, not `rmtree`)."""
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    shared_root = tmp_path / "shared"
    monkeypatch.setenv("PATH_DERIVED", str(shared_root))
    fa = _write_fasta(tmp_path / "a.fasta", [("chr1", "ACGTACGTACGTACGT")])
    manifest = _write_manifest(tmp_path / "m", [fa])

    index_path = shared_root / "references" / "9" / "minimap2" / "index.mmi"
    index_path.parent.mkdir(parents=True)
    index_path.write_bytes(b"STALE")

    def fake_save(conn, subject_table, output_path, *, preset):
        assert not Path(output_path).exists(), "stale .mmi not cleared before rebuild"
        Path(output_path).write_bytes(b"NEW")
        return True

    monkeypatch.setattr(build_minimap2_index, "_run_save_minimap2_index", fake_save)

    inputs = build_minimap2_index.Inputs(
        minimap2_fasta_manifest=manifest, reference_idx=9, work_ticket_idx=1
    )
    asyncio.run(build_minimap2_index.execute(inputs, tmp_path / "ws"))
    assert index_path.read_bytes() == b"NEW"


def test_build_minimap2_index_raises_on_failure(tmp_path, monkeypatch):
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    fa = _write_fasta(tmp_path / "a.fasta", [("chr1", "ACGTACGTACGT")])
    manifest = _write_manifest(tmp_path / "m", [fa])

    def fake_save(conn, subject_table, output_path, *, preset):
        return False

    monkeypatch.setattr(build_minimap2_index, "_run_save_minimap2_index", fake_save)

    inputs = build_minimap2_index.Inputs(
        minimap2_fasta_manifest=manifest, reference_idx=1, work_ticket_idx=1
    )
    with pytest.raises(RuntimeError, match="save_minimap2_index"):
        asyncio.run(build_minimap2_index.execute(inputs, tmp_path / "ws"))


def test_build_minimap2_index_requires_exactly_one_source(tmp_path):
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    # Neither source.
    with pytest.raises(ValidationError):
        build_minimap2_index.Inputs(reference_idx=1, work_ticket_idx=1)
    # Both sources.
    with pytest.raises(ValidationError):
        build_minimap2_index.Inputs(
            minimap2_fasta_manifest=tmp_path / "m",
            fasta_path=tmp_path / "p",
            reference_idx=1,
            work_ticket_idx=1,
        )


def test_build_minimap2_index_missing_manifest_raises(tmp_path, monkeypatch):
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    inputs = build_minimap2_index.Inputs(
        minimap2_fasta_manifest=tmp_path / "nope.manifest",
        reference_idx=1,
        work_ticket_idx=1,
    )
    with pytest.raises((FileNotFoundError, ValueError)):
        asyncio.run(build_minimap2_index.execute(inputs, tmp_path / "ws"))


def test_build_minimap2_index_all_empty_files_raises(tmp_path, monkeypatch):
    """Every tagged FASTA being empty leaves nothing to index — fail loudly
    rather than emit a degenerate/zero-subject index."""
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    empty = tmp_path / "empty.fasta"
    empty.write_text("")
    manifest = _write_manifest(tmp_path / "m", [empty])

    inputs = build_minimap2_index.Inputs(
        minimap2_fasta_manifest=manifest, reference_idx=1, work_ticket_idx=1
    )
    with pytest.raises(ValueError):
        asyncio.run(build_minimap2_index.execute(inputs, tmp_path / "ws"))


def test_build_minimap2_index_empty_manifest_raises_actionable(tmp_path, monkeypatch):
    """A zero-line minimap2 manifest is the `local-host-reference-add`-with-no-tags
    case: stage_local_fasta emits an empty subset manifest when no FASTA carries a
    `\\tminimap2` flag. build_minimap2_index must fail with an actionable message
    pointing at the tag, not an opaque 'malformed file' error."""
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    manifest = tmp_path / "minimap2_fasta_manifest.txt"
    manifest.write_text("")  # what stage_local_fasta emits with zero tags

    inputs = build_minimap2_index.Inputs(
        minimap2_fasta_manifest=manifest, reference_idx=1, work_ticket_idx=1
    )
    with pytest.raises(ValueError, match="minimap2"):
        asyncio.run(build_minimap2_index.execute(inputs, tmp_path / "ws"))


# Synthetic but STRUCTURED contigs: each is a distinct motif tiled many times so
# minimap2 sees real (non-random, reproducible) k-mer content. ~3.6 kb each,
# comfortably indexable under the 'sr' preset. Deterministic — no RNG.
_SMOKE_CONTIGS: dict[str, str] = {
    "chr_a": "ACGTACGTGGCCTTAAACGTTGCA" * 150,
    "chr_b": "TTGGCCAATTGGCCAAGTGTGTGT" * 150,
}


def test_build_minimap2_index_real_smoke(tmp_path, monkeypatch):
    """Smoke the REAL `save_minimap2_index` (seam NOT stubbed): assert it
    returns success and writes a non-empty `.mmi` on structured synthetic data.

    Runs against the team-mirror miint build (conftest sets MIINT_EXTENSION_REPO),
    which carries save_minimap2_index. Verifies the seam's call shape — two
    positional args (subject_table, output_path) plus `preset` — against the
    real function.
    """
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    shared_root = tmp_path / "shared"
    monkeypatch.setenv("PATH_DERIVED", str(shared_root))

    reference_idx = 4242
    fa = _write_fasta(tmp_path / "host.fasta", list(_SMOKE_CONTIGS.items()))
    manifest = _write_manifest(tmp_path / "m", [fa])

    inputs = build_minimap2_index.Inputs(
        minimap2_fasta_manifest=manifest, reference_idx=reference_idx, work_ticket_idx=1
    )
    out = asyncio.run(build_minimap2_index.execute(inputs, tmp_path / "ws"))

    mmi = Path(out["minimap2_index_path"])
    assert mmi.is_file(), "save_minimap2_index did not write the .mmi file"
    assert mmi.stat().st_size > 0, "minimap2 index is empty (failed/partial build)"

    meta = json.loads(Path(out["minimap2_index_meta"]).read_text())
    assert meta["params"] == {"preset": "sr", "source_files": [str(fa)]}


def test_build_minimap2_index_upload_real_smoke(tmp_path, monkeypatch):
    """Smoke the REAL `save_minimap2_index` on the UPLOAD path: chunked parquet
    reassembled into a materialised subject TABLE, then a real minimap2 build.
    Exercises the TABLE-staging branch end-to-end (the local smoke covers the
    VIEW branch). One contig is split across two chunks to drive the
    `string_agg ... ORDER BY chunk_index` reassembly."""
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    shared_root = tmp_path / "shared"
    monkeypatch.setenv("PATH_DERIVED", str(shared_root))

    seq_a = _SMOKE_CONTIGS["chr_a"]
    mid = len(seq_a) // 2
    parquet = _write_chunks_parquet(
        tmp_path / "upload.parquet",
        [
            ("chr_a", 0, seq_a[:mid]),
            ("chr_a", 1, seq_a[mid:]),
            ("chr_b", 0, _SMOKE_CONTIGS["chr_b"]),
        ],
    )

    inputs = build_minimap2_index.Inputs(fasta_path=parquet, reference_idx=4343, work_ticket_idx=1)
    out = asyncio.run(build_minimap2_index.execute(inputs, tmp_path / "ws"))

    mmi = Path(out["minimap2_index_path"])
    assert mmi.is_file(), "save_minimap2_index did not write the .mmi file"
    assert mmi.stat().st_size > 0, "minimap2 index is empty (failed/partial build)"
