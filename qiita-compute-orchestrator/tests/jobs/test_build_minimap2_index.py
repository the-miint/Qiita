"""Isolated unit tests for `build_minimap2_index.execute`.

Calls `execute()` directly. The real minimap2 index build
(`save_minimap2_index`) needs the miint extension and real sequence bytes, so
the heavy path is exercised by a single real-minimap2 smoke at the bottom;
everywhere else the build seam (`_run_save_minimap2_index`) is stubbed and we
assert the orchestration around it:

  - the SAME feature-keyed chunked Parquet `build_rype_index` consumes
    (`feature_idx, chunk_index, chunk_data`) is reassembled into a
    `(read_id, sequence1)` subject TABLE via `string_agg(... ORDER BY chunk_index)`
    — `read_id` is the `feature_idx`;
  - a directory of `part_*.parquet` is accepted as well as a single file;
  - the persistent index path is `{path_derived}/references/{idx}/minimap2/index.mmi`,
    a FILE (not a directory) cleared with `unlink` on a rerun;
  - the meta JSON records index_type / fs_path / params (preset, source_chunks);
  - a non-success build raises RuntimeError;
  - an empty reference (no chunk rows) raises ValueError.
"""

from __future__ import annotations

import asyncio
import json
import math
from contextlib import asynccontextmanager
from pathlib import Path

import duckdb
import pytest


def _write_chunks_parquet(path: Path, rows: list[tuple[int, int, str]]) -> Path:
    """Write the feature-keyed `(feature_idx, chunk_index, chunk_data)` chunk
    shape `reference_load` emits and `build_rype_index` / `build_minimap2_index`
    consume."""
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


def _stub_capture_save(build_minimap2_index, monkeypatch) -> dict:
    """Stub `_run_save_minimap2_index` to capture the staged subject (cols/rows)
    + call args and write a placeholder `.mmi`. Returns the capture dict."""
    captured: dict = {}

    def fake_save(conn, subject_table, output_path, *, preset):
        captured["cols"] = [
            d[0] for d in conn.execute(f"SELECT * FROM {subject_table} LIMIT 0").description
        ]
        captured["rows"] = conn.execute(
            f"SELECT read_id, sequence1 FROM {subject_table} ORDER BY read_id"
        ).fetchall()
        captured["subject_table"] = subject_table
        captured["output_path"] = output_path
        captured["preset"] = preset
        Path(output_path).write_bytes(b"MMI")  # minimap2 writes a FILE
        return True

    monkeypatch.setattr(build_minimap2_index, "_run_save_minimap2_index", fake_save)
    return captured


def test_build_minimap2_index_reassembles_chunks(tmp_path, monkeypatch):
    """The chunked feature-keyed Parquet is reassembled into a (read_id,
    sequence1) subject — read_id is feature_idx, ORDER BY chunk_index drives the
    reassembly (chunks written out-of-order to prove it)."""
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    shared_root = tmp_path / "shared"
    monkeypatch.setenv("PATH_DERIVED", str(shared_root))

    reference_idx = 7
    # feature 1 spans two chunks written out-of-order; feature 2 a single chunk.
    parquet = _write_chunks_parquet(
        tmp_path / "chunks.parquet",
        [(1, 1, "GGGGCCCC"), (1, 0, "ACGTACGT"), (2, 0, "TTTTAAAA")],
    )

    captured = _stub_capture_save(build_minimap2_index, monkeypatch)

    inputs = build_minimap2_index.Inputs(
        reference_sequence_chunks=parquet,
        reference_idx=reference_idx,
        work_ticket_idx=42,
    )
    out = asyncio.run(build_minimap2_index.execute(inputs, tmp_path / "ws"))

    assert captured["cols"] == ["read_id", "sequence1"]
    # read_id is feature_idx (BIGINT); feature 1 reassembled in chunk_index order.
    assert captured["rows"] == [(1, "ACGTACGTGGGGCCCC"), (2, "TTTTAAAA")]
    assert captured["preset"] == "sr"

    expected = shared_root / "references" / str(reference_idx) / "minimap2" / "index.mmi"
    # The persistent .mmi is NOT a step output (it lives outside the workspace);
    # its location reaches register-index via the meta `fs_path`.
    assert "minimap2_index_path" not in out
    assert captured["output_path"] == str(expected)
    assert expected.is_file()

    meta = json.loads(Path(out["minimap2_index_meta"]).read_text())
    assert meta["index_type"] == "minimap2"
    assert meta["fs_path"] == str(expected)
    assert meta["params"] == {
        "preset": "sr",
        "source_chunks": str(parquet),
        "num_subjects": 2,
    }


def test_build_minimap2_index_accepts_part_directory(tmp_path, monkeypatch):
    """A directory of `part_*.parquet` (the shape reference_load emits) is read
    via the `part_*.parquet` glob, same as build_rype_index."""
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    chunk_dir = tmp_path / "chunks"
    chunk_dir.mkdir()
    _write_chunks_parquet(chunk_dir / "part_0.parquet", [(1, 0, "ACGTACGT")])
    _write_chunks_parquet(chunk_dir / "part_1.parquet", [(2, 0, "TTTTAAAA")])

    captured = _stub_capture_save(build_minimap2_index, monkeypatch)

    inputs = build_minimap2_index.Inputs(
        reference_sequence_chunks=chunk_dir, reference_idx=3, work_ticket_idx=1
    )
    asyncio.run(build_minimap2_index.execute(inputs, tmp_path / "ws"))

    assert captured["rows"] == [(1, "ACGTACGT"), (2, "TTTTAAAA")]


def test_build_minimap2_index_clears_stale_index_on_rerun(tmp_path, monkeypatch):
    """A retry re-runs against the same persistent path; any prior `.mmi` is
    cleared first (it's a FILE, so `unlink`, not `rmtree`)."""
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    shared_root = tmp_path / "shared"
    monkeypatch.setenv("PATH_DERIVED", str(shared_root))
    parquet = _write_chunks_parquet(tmp_path / "chunks.parquet", [(1, 0, "ACGTACGTACGT")])

    index_path = shared_root / "references" / "9" / "minimap2" / "index.mmi"
    index_path.parent.mkdir(parents=True)
    index_path.write_bytes(b"STALE")

    def fake_save(conn, subject_table, output_path, *, preset):
        assert not Path(output_path).exists(), "stale .mmi not cleared before rebuild"
        Path(output_path).write_bytes(b"NEW")
        return True

    monkeypatch.setattr(build_minimap2_index, "_run_save_minimap2_index", fake_save)

    inputs = build_minimap2_index.Inputs(
        reference_sequence_chunks=parquet, reference_idx=9, work_ticket_idx=1
    )
    asyncio.run(build_minimap2_index.execute(inputs, tmp_path / "ws"))
    assert index_path.read_bytes() == b"NEW"


def test_build_minimap2_index_cleans_duckdb_tmp(tmp_path, monkeypatch):
    """The DuckDB spill dir under the workspace is removed after a run (success
    and failure paths), so it doesn't accumulate on the shared filesystem."""
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    parquet = _write_chunks_parquet(tmp_path / "chunks.parquet", [(1, 0, "ACGTACGT")])

    # Success path.
    _stub_capture_save(build_minimap2_index, monkeypatch)
    ws_ok = tmp_path / "ws_ok"
    inputs = build_minimap2_index.Inputs(
        reference_sequence_chunks=parquet, reference_idx=1, work_ticket_idx=1
    )
    asyncio.run(build_minimap2_index.execute(inputs, ws_ok))
    assert not (ws_ok / ".duckdb_tmp").exists()

    # Failure path (save reports failure) — tmp still cleaned.
    monkeypatch.setattr(build_minimap2_index, "_run_save_minimap2_index", lambda *a, **k: False)
    ws_fail = tmp_path / "ws_fail"
    with pytest.raises(RuntimeError):
        asyncio.run(build_minimap2_index.execute(inputs, ws_fail))
    assert not (ws_fail / ".duckdb_tmp").exists()


def test_build_minimap2_index_raises_on_failure(tmp_path, monkeypatch):
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    parquet = _write_chunks_parquet(tmp_path / "chunks.parquet", [(1, 0, "ACGTACGT")])

    def fake_save(conn, subject_table, output_path, *, preset):
        return False

    monkeypatch.setattr(build_minimap2_index, "_run_save_minimap2_index", fake_save)

    inputs = build_minimap2_index.Inputs(
        reference_sequence_chunks=parquet, reference_idx=1, work_ticket_idx=1
    )
    with pytest.raises(RuntimeError, match="save_minimap2_index"):
        asyncio.run(build_minimap2_index.execute(inputs, tmp_path / "ws"))


def test_build_minimap2_index_missing_input_raises(tmp_path, monkeypatch):
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    inputs = build_minimap2_index.Inputs(
        reference_sequence_chunks=tmp_path / "nope.parquet",
        reference_idx=1,
        work_ticket_idx=1,
    )
    with pytest.raises(FileNotFoundError):
        asyncio.run(build_minimap2_index.execute(inputs, tmp_path / "ws"))


def test_build_minimap2_index_empty_reference_raises(tmp_path, monkeypatch):
    """A chunked Parquet with no rows (an empty reference) leaves nothing to
    index — fail loudly rather than emit a degenerate/zero-subject index."""
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    empty = _write_chunks_parquet(tmp_path / "empty.parquet", [(1, 0, "")])
    # Strip to zero rows: a real empty reference produces no chunk rows at all.
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT * FROM read_parquet('{empty}') WHERE false) "
            f"TO '{empty}' (FORMAT PARQUET)"
        )

    inputs = build_minimap2_index.Inputs(
        reference_sequence_chunks=empty, reference_idx=1, work_ticket_idx=1
    )
    with pytest.raises(ValueError, match="no sequence chunks"):
        asyncio.run(build_minimap2_index.execute(inputs, tmp_path / "ws"))


# Synthetic but STRUCTURED contigs: each is a distinct motif tiled many times so
# minimap2 sees real (non-random, reproducible) k-mer content. ~3.6 kb each,
# comfortably indexable under the 'sr' preset. Deterministic — no RNG.
_SMOKE_CONTIGS: dict[int, str] = {
    1: "ACGTACGTGGCCTTAAACGTTGCA" * 150,
    2: "TTGGCCAATTGGCCAAGTGTGTGT" * 150,
}


def test_build_minimap2_index_real_smoke(tmp_path, monkeypatch):
    """Smoke the REAL `save_minimap2_index` (seam NOT stubbed): assert it
    returns success and writes a non-empty `.mmi` from chunked Parquet, with one
    contig split across two chunks to drive the `string_agg ... ORDER BY
    chunk_index` reassembly.

    Runs against the team-mirror miint build (conftest sets MIINT_EXTENSION_REPO),
    which carries save_minimap2_index. Verifies the seam's call shape — two
    positional args (subject_table, output_path) plus `preset`, and a BIGINT
    feature_idx subject read_id — against the real function.
    """
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    shared_root = tmp_path / "shared"
    monkeypatch.setenv("PATH_DERIVED", str(shared_root))

    reference_idx = 4242
    seq_a = _SMOKE_CONTIGS[1]
    mid = len(seq_a) // 2
    parquet = _write_chunks_parquet(
        tmp_path / "chunks.parquet",
        [
            (1, 0, seq_a[:mid]),
            (1, 1, seq_a[mid:]),
            (2, 0, _SMOKE_CONTIGS[2]),
        ],
    )

    inputs = build_minimap2_index.Inputs(
        reference_sequence_chunks=parquet, reference_idx=reference_idx, work_ticket_idx=1
    )
    out = asyncio.run(build_minimap2_index.execute(inputs, tmp_path / "ws"))

    meta = json.loads(Path(out["minimap2_index_meta"]).read_text())
    mmi = Path(meta["fs_path"])
    assert mmi.is_file(), "save_minimap2_index did not write the .mmi file"
    assert mmi.stat().st_size > 0, "minimap2 index is empty (failed/partial build)"
    assert meta["params"]["preset"] == "sr"
    assert meta["params"]["num_subjects"] == 2


# ---------------------------------------------------------------------------
# Shard mode (streaming via B6s) + plan()
# ---------------------------------------------------------------------------


def _write_roster(path: Path, rows: list[tuple[int, int]]) -> Path:
    """Write a shard feature roster Parquet `(feature_idx BIGINT,
    sequence_length_bp BIGINT)` — the per-shard subset B5 will stage from
    reference_membership.shard_id joined with the sequence lengths."""
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


def _fake_stream_from_parquet(parquet: Path, captured: dict):
    """Build a fake `open_reference_chunk_stream` that registers a local chunk
    Parquet as the streamed relation (simulating the B6s DoGet), capturing the
    ticket scope args."""

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


def test_build_minimap2_index_shard_mode(tmp_path, monkeypatch):
    """Shard mode streams the roster's chunks (via open_reference_chunk_stream),
    reassembles the subject, writes to `.../minimap2-shards/{shard_id}.mmi` (the
    flat `{shard_directory}/{shard_name}.mmi` shape `align_minimap2_sharded`
    binds), and records `shard_id` + a stream-source params block in the meta."""
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    shared_root = tmp_path / "shared"
    monkeypatch.setenv("PATH_DERIVED", str(shared_root))

    reference_idx = 7
    shard_id = 3
    # The streamed (roster-scoped) chunks: feature 100 across two out-of-order
    # chunks, feature 300 single-chunk.
    stream_parquet = _write_chunks_parquet(
        tmp_path / "stream.parquet",
        [(100, 1, "GGGG"), (100, 0, "ACGT"), (300, 0, "TTTT")],
    )
    roster = _write_roster(tmp_path / "roster.parquet", [(100, 8), (300, 4)])

    stream_capture: dict = {}
    monkeypatch.setattr(
        build_minimap2_index,
        "open_reference_chunk_stream",
        _fake_stream_from_parquet(stream_parquet, stream_capture),
    )
    save_capture = _stub_capture_save(build_minimap2_index, monkeypatch)

    inputs = build_minimap2_index.Inputs(
        reference_idx=reference_idx,
        work_ticket_idx=42,
        shard_id=shard_id,
        shard_features=roster,
    )
    out = asyncio.run(build_minimap2_index.execute(inputs, tmp_path / "ws"))

    # Ticket scoped to the roster's feature_idx list.
    assert stream_capture["reference_idx"] == reference_idx
    assert sorted(stream_capture["feature_idx"]) == [100, 300]
    # Subject reassembled from the stream; feature 100 in chunk_index order.
    assert save_capture["cols"] == ["read_id", "sequence1"]
    assert save_capture["rows"] == [(100, "ACGTGGGG"), (300, "TTTT")]

    expected = (
        shared_root / "references" / str(reference_idx) / "minimap2-shards" / f"{shard_id}.mmi"
    )
    assert save_capture["output_path"] == str(expected)
    assert expected.is_file()

    meta = json.loads(Path(out["minimap2_index_meta"]).read_text())
    assert meta["index_type"] == "minimap2"
    assert meta["fs_path"] == str(expected)
    assert meta["shard_id"] == shard_id
    assert meta["params"] == {
        "preset": "sr",
        "source": "stream",
        "feature_count": 2,
        "num_subjects": 2,
    }


def test_build_minimap2_index_shard_empty_roster_raises(tmp_path, monkeypatch):
    """An empty shard roster is a fail-fast before any stream is opened."""
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    roster = _write_roster(tmp_path / "roster.parquet", [])

    def _boom(*a, **k):  # open_reference_chunk_stream must never be reached
        raise AssertionError("stream opened for an empty roster")

    monkeypatch.setattr(build_minimap2_index, "open_reference_chunk_stream", _boom)

    inputs = build_minimap2_index.Inputs(
        reference_idx=1, work_ticket_idx=1, shard_id=0, shard_features=roster
    )
    with pytest.raises(ValueError, match="roster"):
        asyncio.run(build_minimap2_index.execute(inputs, tmp_path / "ws"))


def test_build_minimap2_index_shard_inputs_both_or_neither(tmp_path):
    """shard_id and shard_features must be supplied together."""
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    with pytest.raises(ValueError):
        build_minimap2_index.Inputs(
            reference_idx=1, work_ticket_idx=1, shard_id=0
        )  # missing shard_features
    with pytest.raises(ValueError):
        build_minimap2_index.Inputs(
            reference_idx=1, work_ticket_idx=1, shard_features=tmp_path / "r.parquet"
        )  # missing shard_id


def test_build_minimap2_index_host_mode_requires_chunks():
    """Host/whole-reference mode (no shard) requires reference_sequence_chunks."""
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    with pytest.raises(ValueError, match="reference_sequence_chunks"):
        build_minimap2_index.Inputs(reference_idx=1, work_ticket_idx=1)


def test_build_minimap2_index_plan_host_mode_no_opinion(tmp_path):
    """Host mode → empty JobPlan (no resource opinion)."""
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    inputs = build_minimap2_index.Inputs(
        reference_sequence_chunks=tmp_path / "c", reference_idx=1, work_ticket_idx=1
    )
    assert build_minimap2_index.plan(inputs).resources is None


def test_build_minimap2_index_plan_shard_mode_sizes_mem(tmp_path):
    """Shard mode sizes mem_gb from the shard's total bp, floored at the
    runtime-consistent minimum (DuckDB fallback + minimap2 reserve + headroom)."""
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    roster = _write_roster(tmp_path / "roster.parquet", [(100, 500_000_000), (300, 500_000_000)])
    inputs = build_minimap2_index.Inputs(
        reference_idx=1, work_ticket_idx=1, shard_id=0, shard_features=roster
    )
    plan = build_minimap2_index.plan(inputs)
    total_bp = 1_000_000_000
    expected = build_minimap2_index._SHARD_PLAN_FLOOR_GB + math.ceil(
        total_bp / build_minimap2_index._SHARD_PLAN_BP_PER_GB
    )
    assert plan.resources is not None
    assert plan.resources.mem_gb == expected


def test_build_minimap2_index_plan_shard_mem_scales_with_bp(tmp_path):
    from qiita_compute_orchestrator.jobs import build_minimap2_index

    small = _write_roster(tmp_path / "small.parquet", [(1, 100_000_000)])
    big = _write_roster(tmp_path / "big.parquet", [(1, 40_000_000_000)])

    def _mem(roster):
        inputs = build_minimap2_index.Inputs(
            reference_idx=1, work_ticket_idx=1, shard_id=0, shard_features=roster
        )
        return build_minimap2_index.plan(inputs).resources.mem_gb

    assert _mem(big) > _mem(small)
