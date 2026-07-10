"""Isolated unit tests for `build_bowtie2_index.execute`.

Mirrors test_build_minimap2_index.py. The real bowtie2 build
(`save_bowtie2_index`) needs the miint extension and real sequence bytes, so the
heavy path is exercised by a single real-bowtie2 host smoke at the bottom (the
authoritative miint verification — it also confirms NO GPL boundary is required);
everywhere else the build seam (`_run_save_bowtie2_index`) is stubbed.

Key differences from the minimap2 builder:
  - bowtie2 writes a MULTI-FILE `.bt2` set under a shared PREFIX, so `fs_path` is
    a prefix and a rerun rmtrees the whole `bowtie2/` dir (not a single unlink);
  - there is NO `preset` (the bowtie2 index is preset-independent).
"""

from __future__ import annotations

import asyncio
import json
import math
from contextlib import asynccontextmanager
from pathlib import Path

import duckdb
import pytest

_BT2_SUFFIXES = ("1.bt2", "2.bt2", "3.bt2", "4.bt2", "rev.1.bt2", "rev.2.bt2")


def _write_chunks_parquet(path: Path, rows: list[tuple[int, int, str]]) -> Path:
    """Write the feature-keyed `(feature_idx, chunk_index, chunk_data)` chunk
    shape reference_load emits and the builders consume."""
    with duckdb.connect(":memory:") as conn:
        values_sql = ", ".join(
            "(CAST(? AS BIGINT), CAST(? AS INTEGER), CAST(? AS VARCHAR))" for _ in rows
        )
        params: list = []
        for feature_idx, cidx, data in rows:
            params.extend([feature_idx, cidx, data])
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values_sql}) "
            "AS t(feature_idx, chunk_index, chunk_data)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )
    return path


def _write_roster(path: Path, rows: list[tuple[int, int]]) -> Path:
    """Write a shard feature roster Parquet `(feature_idx BIGINT,
    sequence_length_bp BIGINT)`."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(":memory:") as conn:
        if not rows:
            # 0-row but correctly-typed Parquet (an empty shard roster).
            conn.execute(
                "COPY (SELECT CAST(NULL AS BIGINT) AS feature_idx, "
                "CAST(NULL AS BIGINT) AS sequence_length_bp WHERE false) "
                f"TO '{path}' (FORMAT PARQUET)"
            )
            return path
        values_sql = ", ".join("(CAST(? AS BIGINT), CAST(? AS BIGINT))" for _ in rows)
        params: list = []
        for fidx, bp in rows:
            params.extend([fidx, bp])
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values_sql}) AS t(feature_idx, sequence_length_bp)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )
    return path


def _stub_capture_save(build_bowtie2_index, monkeypatch) -> dict:
    """Stub `_run_save_bowtie2_index` to capture the staged subject + output prefix
    and write the six placeholder `.bt2` files bowtie2 would emit."""
    captured: dict = {}

    def fake_save(conn, subject_table, output_path):
        captured["cols"] = [
            d[0] for d in conn.execute(f"SELECT * FROM {subject_table} LIMIT 0").description
        ]
        captured["rows"] = conn.execute(
            f"SELECT read_id, sequence1 FROM {subject_table} ORDER BY read_id"
        ).fetchall()
        captured["subject_table"] = subject_table
        captured["output_path"] = output_path
        prefix = Path(output_path)
        for suffix in _BT2_SUFFIXES:
            (prefix.parent / f"{prefix.name}.{suffix}").write_bytes(b"BT2")
        return True

    monkeypatch.setattr(build_bowtie2_index, "_run_save_bowtie2_index", fake_save)
    return captured


def _fake_stream_from_parquet(parquet: Path, captured: dict):
    @asynccontextmanager
    async def fake_stream(conn, *, reference_idx, feature_idx, relation="reference_chunks"):
        captured["reference_idx"] = reference_idx
        captured["feature_idx"] = feature_idx
        conn.execute(
            f"CREATE OR REPLACE VIEW {relation} AS SELECT * FROM read_parquet('{parquet}')"
        )
        try:
            yield relation
        finally:
            conn.execute(f"DROP VIEW IF EXISTS {relation}")

    return fake_stream


def test_build_bowtie2_index_reassembles_chunks_host(tmp_path, monkeypatch):
    """Host mode reassembles the chunked Parquet into a (read_id, sequence1)
    subject and writes the `.bt2` set under `.../bowtie2/index` (a prefix); the
    meta records index_type/fs_path/params with NO preset."""
    from qiita_compute_orchestrator.jobs import build_bowtie2_index

    shared_root = tmp_path / "shared"
    monkeypatch.setenv("PATH_DERIVED", str(shared_root))

    reference_idx = 7
    parquet = _write_chunks_parquet(
        tmp_path / "chunks.parquet",
        [(1, 1, "GGGGCCCC"), (1, 0, "ACGTACGT"), (2, 0, "TTTTAAAA")],
    )
    captured = _stub_capture_save(build_bowtie2_index, monkeypatch)

    inputs = build_bowtie2_index.Inputs(
        reference_sequence_chunks=parquet, reference_idx=reference_idx, work_ticket_idx=42
    )
    out = asyncio.run(build_bowtie2_index.execute(inputs, tmp_path / "ws"))

    assert captured["cols"] == ["read_id", "sequence1"]
    assert captured["rows"] == [(1, "ACGTACGTGGGGCCCC"), (2, "TTTTAAAA")]

    expected_prefix = shared_root / "references" / str(reference_idx) / "bowtie2" / "index"
    assert captured["output_path"] == str(expected_prefix)
    # The multi-file artifact is NOT a step output; register-index reads fs_path.
    assert "bowtie2_index_path" not in out
    for suffix in _BT2_SUFFIXES:
        assert (expected_prefix.parent / f"index.{suffix}").is_file()

    meta = json.loads(Path(out["bowtie2_index_meta"]).read_text())
    assert meta["index_type"] == "bowtie2"
    assert meta["fs_path"] == str(expected_prefix)
    assert meta["params"] == {
        "source_chunks": str(parquet),
        "num_subjects": 2,
    }
    assert "preset" not in meta["params"]
    assert "shard_id" not in meta


def test_build_bowtie2_index_accepts_part_directory(tmp_path, monkeypatch):
    from qiita_compute_orchestrator.jobs import build_bowtie2_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    chunk_dir = tmp_path / "chunks"
    chunk_dir.mkdir()
    _write_chunks_parquet(chunk_dir / "part_0.parquet", [(1, 0, "ACGTACGT")])
    _write_chunks_parquet(chunk_dir / "part_1.parquet", [(2, 0, "TTTTAAAA")])

    captured = _stub_capture_save(build_bowtie2_index, monkeypatch)
    inputs = build_bowtie2_index.Inputs(
        reference_sequence_chunks=chunk_dir, reference_idx=3, work_ticket_idx=1
    )
    asyncio.run(build_bowtie2_index.execute(inputs, tmp_path / "ws"))
    assert captured["rows"] == [(1, "ACGTACGT"), (2, "TTTTAAAA")]


def test_build_bowtie2_index_clears_stale_dir_on_rerun(tmp_path, monkeypatch):
    """A retry rmtrees the whole `bowtie2/` dir first, so a stale `.bt2` shard
    from a partial prior build cannot survive into the new index."""
    from qiita_compute_orchestrator.jobs import build_bowtie2_index

    shared_root = tmp_path / "shared"
    monkeypatch.setenv("PATH_DERIVED", str(shared_root))
    parquet = _write_chunks_parquet(tmp_path / "chunks.parquet", [(1, 0, "ACGTACGTACGT")])

    bowtie2_dir = shared_root / "references" / "9" / "bowtie2"
    bowtie2_dir.mkdir(parents=True)
    (bowtie2_dir / "index.99.bt2").write_bytes(b"STALE")  # a stray shard from a prior run

    def fake_save(conn, subject_table, output_path):
        prefix = Path(output_path)
        assert not (prefix.parent / "index.99.bt2").exists(), "stale .bt2 not cleared"
        for suffix in _BT2_SUFFIXES:
            (prefix.parent / f"{prefix.name}.{suffix}").write_bytes(b"NEW")
        return True

    monkeypatch.setattr(build_bowtie2_index, "_run_save_bowtie2_index", fake_save)
    inputs = build_bowtie2_index.Inputs(
        reference_sequence_chunks=parquet, reference_idx=9, work_ticket_idx=1
    )
    asyncio.run(build_bowtie2_index.execute(inputs, tmp_path / "ws"))
    assert not (bowtie2_dir / "index.99.bt2").exists()
    assert (bowtie2_dir / "index.1.bt2").read_bytes() == b"NEW"


def test_build_bowtie2_index_raises_on_failure(tmp_path, monkeypatch):
    from qiita_compute_orchestrator.jobs import build_bowtie2_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    parquet = _write_chunks_parquet(tmp_path / "chunks.parquet", [(1, 0, "ACGTACGT")])
    monkeypatch.setattr(build_bowtie2_index, "_run_save_bowtie2_index", lambda *a, **k: False)

    inputs = build_bowtie2_index.Inputs(
        reference_sequence_chunks=parquet, reference_idx=1, work_ticket_idx=1
    )
    with pytest.raises(RuntimeError, match="save_bowtie2_index"):
        asyncio.run(build_bowtie2_index.execute(inputs, tmp_path / "ws"))


def test_build_bowtie2_index_raises_when_no_bt2_written(tmp_path, monkeypatch):
    """A `success=true` with no `.bt2` files on disk is a silent contract break —
    the post-build self-check catches it."""
    from qiita_compute_orchestrator.jobs import build_bowtie2_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    parquet = _write_chunks_parquet(tmp_path / "chunks.parquet", [(1, 0, "ACGTACGT")])
    # Reports success but writes nothing.
    monkeypatch.setattr(build_bowtie2_index, "_run_save_bowtie2_index", lambda *a, **k: True)

    inputs = build_bowtie2_index.Inputs(
        reference_sequence_chunks=parquet, reference_idx=1, work_ticket_idx=1
    )
    with pytest.raises(RuntimeError, match="no bowtie2 index files"):
        asyncio.run(build_bowtie2_index.execute(inputs, tmp_path / "ws"))


def test_build_bowtie2_index_missing_input_raises(tmp_path, monkeypatch):
    from qiita_compute_orchestrator.jobs import build_bowtie2_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    inputs = build_bowtie2_index.Inputs(
        reference_sequence_chunks=tmp_path / "nope.parquet", reference_idx=1, work_ticket_idx=1
    )
    with pytest.raises(FileNotFoundError):
        asyncio.run(build_bowtie2_index.execute(inputs, tmp_path / "ws"))


def test_build_bowtie2_index_empty_reference_raises(tmp_path, monkeypatch):
    from qiita_compute_orchestrator.jobs import build_bowtie2_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    empty = _write_chunks_parquet(tmp_path / "empty.parquet", [(1, 0, "")])
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT * FROM read_parquet('{empty}') WHERE false) "
            f"TO '{empty}' (FORMAT PARQUET)"
        )
    inputs = build_bowtie2_index.Inputs(
        reference_sequence_chunks=empty, reference_idx=1, work_ticket_idx=1
    )
    with pytest.raises(ValueError, match="no sequence chunks"):
        asyncio.run(build_bowtie2_index.execute(inputs, tmp_path / "ws"))


# ---------------------------------------------------------------------------
# Shard mode (streaming via B6s) + plan()
# ---------------------------------------------------------------------------


def test_build_bowtie2_index_shard_mode(tmp_path, monkeypatch):
    """Shard mode streams the roster's chunks, writes to
    `.../bowtie2-shards/{shard_id}/index` (the `{shard_directory}/{shard_name}/index.*`
    shape `align_bowtie2_sharded` binds), and records shard_id + a stream-source
    params block (no preset)."""
    from qiita_compute_orchestrator.jobs import build_bowtie2_index

    shared_root = tmp_path / "shared"
    monkeypatch.setenv("PATH_DERIVED", str(shared_root))

    reference_idx = 7
    shard_id = 3
    stream_parquet = _write_chunks_parquet(
        tmp_path / "stream.parquet",
        [(100, 1, "GGGG"), (100, 0, "ACGT"), (300, 0, "TTTT")],
    )
    roster = _write_roster(tmp_path / "roster.parquet", [(100, 8), (300, 4)])

    stream_capture: dict = {}
    monkeypatch.setattr(
        build_bowtie2_index,
        "open_reference_chunk_stream",
        _fake_stream_from_parquet(stream_parquet, stream_capture),
    )
    save_capture = _stub_capture_save(build_bowtie2_index, monkeypatch)

    inputs = build_bowtie2_index.Inputs(
        reference_idx=reference_idx,
        work_ticket_idx=42,
        shard_id=shard_id,
        shard_features=roster,
    )
    out = asyncio.run(build_bowtie2_index.execute(inputs, tmp_path / "ws"))

    assert stream_capture["reference_idx"] == reference_idx
    assert sorted(stream_capture["feature_idx"]) == [100, 300]
    assert save_capture["rows"] == [(100, "ACGTGGGG"), (300, "TTTT")]

    expected_prefix = (
        shared_root / "references" / str(reference_idx) / "bowtie2-shards" / str(shard_id) / "index"
    )
    assert save_capture["output_path"] == str(expected_prefix)
    for suffix in _BT2_SUFFIXES:
        assert (expected_prefix.parent / f"index.{suffix}").is_file()

    meta = json.loads(Path(out["bowtie2_index_meta"]).read_text())
    assert meta["fs_path"] == str(expected_prefix)
    assert meta["shard_id"] == shard_id
    assert meta["params"] == {"source": "stream", "feature_count": 2, "num_subjects": 2}


def test_build_bowtie2_index_shard_empty_roster_raises(tmp_path, monkeypatch):
    from qiita_compute_orchestrator.jobs import build_bowtie2_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    roster = _write_roster(tmp_path / "roster.parquet", [])

    def _boom(*a, **k):
        raise AssertionError("stream opened for an empty roster")

    monkeypatch.setattr(build_bowtie2_index, "open_reference_chunk_stream", _boom)
    inputs = build_bowtie2_index.Inputs(
        reference_idx=1, work_ticket_idx=1, shard_id=0, shard_features=roster
    )
    with pytest.raises(ValueError, match="roster"):
        asyncio.run(build_bowtie2_index.execute(inputs, tmp_path / "ws"))


def test_build_bowtie2_index_shard_inputs_both_or_neither(tmp_path):
    from qiita_compute_orchestrator.jobs import build_bowtie2_index

    with pytest.raises(ValueError):
        build_bowtie2_index.Inputs(reference_idx=1, work_ticket_idx=1, shard_id=0)
    with pytest.raises(ValueError):
        build_bowtie2_index.Inputs(
            reference_idx=1, work_ticket_idx=1, shard_features=tmp_path / "r.parquet"
        )


def test_build_bowtie2_index_host_mode_requires_chunks():
    from qiita_compute_orchestrator.jobs import build_bowtie2_index

    with pytest.raises(ValueError, match="reference_sequence_chunks"):
        build_bowtie2_index.Inputs(reference_idx=1, work_ticket_idx=1)


def test_build_bowtie2_index_plan_host_mode_no_opinion(tmp_path):
    from qiita_compute_orchestrator.jobs import build_bowtie2_index

    inputs = build_bowtie2_index.Inputs(
        reference_sequence_chunks=tmp_path / "c", reference_idx=1, work_ticket_idx=1
    )
    assert build_bowtie2_index.plan(inputs).resources is None


def test_build_bowtie2_index_plan_shard_mode_sizes_mem(tmp_path):
    from qiita_compute_orchestrator.jobs import build_bowtie2_index

    roster = _write_roster(tmp_path / "roster.parquet", [(100, 500_000_000), (300, 500_000_000)])
    inputs = build_bowtie2_index.Inputs(
        reference_idx=1, work_ticket_idx=1, shard_id=0, shard_features=roster
    )
    plan = build_bowtie2_index.plan(inputs)
    total_bp = 1_000_000_000
    expected = build_bowtie2_index._SHARD_PLAN_FLOOR_GB + math.ceil(
        total_bp / build_bowtie2_index._SHARD_PLAN_BP_PER_GB
    )
    assert plan.resources is not None
    assert plan.resources.mem_gb == expected


def test_build_bowtie2_index_plan_shard_mem_scales_with_bp(tmp_path):
    from qiita_compute_orchestrator.jobs import build_bowtie2_index

    small = _write_roster(tmp_path / "small.parquet", [(1, 100_000_000)])
    big = _write_roster(tmp_path / "big.parquet", [(1, 40_000_000_000)])

    def _mem(roster):
        inputs = build_bowtie2_index.Inputs(
            reference_idx=1, work_ticket_idx=1, shard_id=0, shard_features=roster
        )
        return build_bowtie2_index.plan(inputs).resources.mem_gb

    assert _mem(big) > _mem(small)


# Synthetic but STRUCTURED contigs — distinct motifs tiled many times so bowtie2
# sees real (non-random, reproducible) content. ~3.6 kb each. Deterministic.
_SMOKE_CONTIGS: dict[int, str] = {
    1: "ACGTACGTGGCCTTAAACGTTGCA" * 150,
    2: "TTGGCCAATTGGCCAAGTGTGTGT" * 150,
}


def test_build_bowtie2_index_real_smoke(tmp_path, monkeypatch):
    """Smoke the REAL `save_bowtie2_index` (seam NOT stubbed) in HOST mode: it
    returns success and writes a `.bt2` set from chunked Parquet, with one contig
    split across two chunks to drive the reassembly.

    This is the AUTHORITATIVE miint verification for the bowtie2 builder:
      - confirms save_bowtie2_index exists on the team-mirror build and its
        two-positional call shape (subject_table, output_path), no preset;
      - confirms the seam's `install_gpl_boundary()` call satisfies the mirror
        build's GPL-boundary requirement so the save runs (the binary downloads
        into miint's cache dir; this exercises real network on first run).

    Runs against the team-mirror miint build (conftest sets MIINT_EXTENSION_REPO).
    """
    from qiita_compute_orchestrator.jobs import build_bowtie2_index

    shared_root = tmp_path / "shared"
    monkeypatch.setenv("PATH_DERIVED", str(shared_root))

    reference_idx = 4242
    seq_a = _SMOKE_CONTIGS[1]
    mid = len(seq_a) // 2
    parquet = _write_chunks_parquet(
        tmp_path / "chunks.parquet",
        [(1, 0, seq_a[:mid]), (1, 1, seq_a[mid:]), (2, 0, _SMOKE_CONTIGS[2])],
    )

    inputs = build_bowtie2_index.Inputs(
        reference_sequence_chunks=parquet, reference_idx=reference_idx, work_ticket_idx=1
    )
    out = asyncio.run(build_bowtie2_index.execute(inputs, tmp_path / "ws"))

    meta = json.loads(Path(out["bowtie2_index_meta"]).read_text())
    prefix = Path(meta["fs_path"])
    bt2_files = list(prefix.parent.glob(f"{prefix.name}*.bt2"))
    assert bt2_files, "save_bowtie2_index wrote no .bt2 files"
    assert all(f.stat().st_size > 0 for f in bt2_files), "a .bt2 file is empty (partial build)"
    assert meta["index_type"] == "bowtie2"
    assert meta["params"]["num_subjects"] == 2
    assert "preset" not in meta["params"]
