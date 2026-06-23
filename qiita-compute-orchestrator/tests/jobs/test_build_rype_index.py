"""Isolated unit tests for `build_rype_index.execute`.

Calls `execute()` directly with a synthesized feature-keyed chunk Parquet.
The real rype build (`rype_index_create`) needs the miint extension and real
sequence bytes, so it's exercised by the integration/system suite; here we
stub the build seam (`_run_rype_index_create`) and assert the orchestration
around it:

  - a single-bucket `(feature_idx, bucket_name)` mapping table is built that
    covers every feature in the chunks, with the default bucket name;
  - the persistent index path is `{shared_root}/references/{idx}/rype/index.ryxdi`
    and its parent directory is created;
  - the only returned binding is the in-tree `rype_index_meta`, whose meta JSON
    records index_type / fs_path / params (k, w, bucket_name); the persistent
    `.ryxdi` is NOT a step output (it lives outside the workspace).

The chunk Parquet shape `(feature_idx BIGINT, chunk_index INTEGER, chunk_data
VARCHAR)` is the feature-keyed form `reference_load` emits.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import duckdb
import pytest


def _write_chunks_dir(chunks_dir: Path, rows: list[tuple[int, int, str]]) -> Path:
    """Write a `reference_sequence_chunks/part_0.parquet` directory with the
    feature-keyed chunk shape."""
    chunks_dir.mkdir(parents=True, exist_ok=True)
    part = chunks_dir / "part_0.parquet"
    with duckdb.connect(":memory:") as conn:
        values_sql = ", ".join(
            "(CAST(? AS BIGINT), CAST(? AS INTEGER), CAST(? AS VARCHAR))" for _ in rows
        )
        params: list = []
        for fidx, cidx, data in rows:
            params.extend([fidx, cidx, data])
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values_sql}) "
            "AS t(feature_idx, chunk_index, chunk_data)) "
            f"TO '{part}' (FORMAT PARQUET)",
            params,
        )
    return chunks_dir


def test_build_rype_index_orchestration(tmp_path, monkeypatch):
    from qiita_compute_orchestrator.jobs import build_rype_index

    # Persistent index root → tmp via env (get_settings falls back to from_env).
    shared_root = tmp_path / "shared"
    monkeypatch.setenv("PATH_DERIVED", str(shared_root))

    reference_idx = 7
    # Two features, one multi-chunk; build the chunks dir.
    chunks_dir = _write_chunks_dir(
        tmp_path / "reference_sequence_chunks",
        [(100, 0, "ACGT"), (100, 1, "TTTT"), (200, 0, "GGGG")],
    )

    captured: dict = {}

    def fake_build(conn, chunk_table, output_path, mapping_table, *, k, w, max_memory):
        # Inspect the mapping table the job built before handing off to rype.
        n_rows, n_buckets = conn.execute(
            f"SELECT count(*), count(DISTINCT bucket_name) FROM {mapping_table}"
        ).fetchone()
        bucket = conn.execute(f"SELECT DISTINCT bucket_name FROM {mapping_table}").fetchone()[0]
        features = [
            r[0]
            for r in conn.execute(
                f"SELECT feature_idx FROM {mapping_table} ORDER BY feature_idx"
            ).fetchall()
        ]
        captured.update(
            chunk_table=chunk_table,
            output_path=output_path,
            mapping_table=mapping_table,
            k=k,
            w=w,
            max_memory=max_memory,
            n_rows=n_rows,
            n_buckets=n_buckets,
            bucket=bucket,
            features=features,
        )
        # rype creates the .ryxdi dir; simulate it.
        Path(output_path).mkdir(parents=True, exist_ok=True)
        (Path(output_path) / "manifest.toml").write_text("k=64\n")
        return "ok"

    monkeypatch.setattr(build_rype_index, "_run_rype_index_create", fake_build)

    inputs = build_rype_index.Inputs(
        reference_sequence_chunks=chunks_dir,
        reference_idx=reference_idx,
        work_ticket_idx=42,
    )
    out = asyncio.run(build_rype_index.execute(inputs, tmp_path / "workspace"))

    # Single bucket covering both features, default name.
    assert captured["n_buckets"] == 1
    assert captured["n_rows"] == 2
    assert captured["features"] == [100, 200]
    assert captured["bucket"] == f"reference_{reference_idx}"
    assert captured["k"] == 64
    assert captured["w"] == 20

    expected_dir = shared_root / "references" / str(reference_idx) / "rype" / "index.ryxdi"
    # The persistent .ryxdi is NOT a step output (it lives outside the
    # workspace); its location reaches register-index via the meta `fs_path`.
    assert "rype_index_path" not in out
    assert expected_dir.is_dir()
    assert captured["output_path"] == str(expected_dir)

    meta = json.loads(Path(out["rype_index_meta"]).read_text())
    assert meta["index_type"] == "rype"
    assert meta["fs_path"] == str(expected_dir)
    assert meta["params"] == {"k": 64, "w": 20, "bucket_name": f"reference_{reference_idx}"}


def test_build_rype_index_honours_bucket_name_override(tmp_path, monkeypatch):
    from qiita_compute_orchestrator.jobs import build_rype_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    chunks_dir = _write_chunks_dir(
        tmp_path / "reference_sequence_chunks", [(1, 0, "ACGT"), (2, 0, "CCCC")]
    )

    seen: dict = {}

    def fake_build(conn, chunk_table, output_path, mapping_table, *, k, w, max_memory):
        seen["bucket"] = conn.execute(
            f"SELECT DISTINCT bucket_name FROM {mapping_table}"
        ).fetchone()[0]
        Path(output_path).mkdir(parents=True, exist_ok=True)
        return "ok"

    monkeypatch.setattr(build_rype_index, "_run_rype_index_create", fake_build)

    inputs = build_rype_index.Inputs(
        reference_sequence_chunks=chunks_dir,
        reference_idx=3,
        work_ticket_idx=1,
        bucket_name="human",
    )
    out = asyncio.run(build_rype_index.execute(inputs, tmp_path / "ws"))
    assert seen["bucket"] == "human"
    meta = json.loads(Path(out["rype_index_meta"]).read_text())
    assert meta["params"]["bucket_name"] == "human"


def test_build_rype_index_clears_stale_index_on_rerun(tmp_path, monkeypatch):
    """A retry re-runs against the same persistent path; any prior (partial)
    .ryxdi is cleared first so the rebuild is deterministic."""
    from qiita_compute_orchestrator.jobs import build_rype_index

    shared_root = tmp_path / "shared"
    monkeypatch.setenv("PATH_DERIVED", str(shared_root))
    chunks_dir = _write_chunks_dir(tmp_path / "reference_sequence_chunks", [(1, 0, "ACGT")])

    # Pre-seed a stale/partial index at the persistent location.
    index_dir = shared_root / "references" / "9" / "rype" / "index.ryxdi"
    index_dir.mkdir(parents=True)
    (index_dir / "stale.partial").write_text("leftover")

    def fake_build(conn, chunk_table, output_path, mapping_table, *, k, w, max_memory):
        # By the time rype runs, the stale dir must be gone.
        assert not Path(output_path).exists()
        Path(output_path).mkdir(parents=True, exist_ok=True)
        return "ok"

    monkeypatch.setattr(build_rype_index, "_run_rype_index_create", fake_build)

    inputs = build_rype_index.Inputs(
        reference_sequence_chunks=chunks_dir, reference_idx=9, work_ticket_idx=1
    )
    asyncio.run(build_rype_index.execute(inputs, tmp_path / "ws"))
    assert not (index_dir / "stale.partial").exists()


def test_build_rype_index_raises_on_non_ok_status(tmp_path, monkeypatch):
    from qiita_compute_orchestrator.jobs import build_rype_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    chunks_dir = _write_chunks_dir(tmp_path / "reference_sequence_chunks", [(1, 0, "ACGT")])

    def fake_build(conn, chunk_table, output_path, mapping_table, *, k, w, max_memory):
        return "error"

    monkeypatch.setattr(build_rype_index, "_run_rype_index_create", fake_build)

    inputs = build_rype_index.Inputs(
        reference_sequence_chunks=chunks_dir, reference_idx=1, work_ticket_idx=1
    )
    with pytest.raises(RuntimeError, match="rype_index_create"):
        asyncio.run(build_rype_index.execute(inputs, tmp_path / "ws"))


# Synthetic but STRUCTURED references: each feature is a distinct motif tiled
# many times, so rype sees real (non-random, reproducible) minimizer content
# and the three features are clearly separable. Each is ~640 bp (> k=64),
# single-chunk. Deterministic — no RNG.
_SMOKE_FEATURES: dict[int, str] = {
    100: "ACGTACGTGGCCTTAAACGTTGCA" * 30,
    200: "TTGGCCAATTGGCCAAGTGTGTGT" * 30,
    300: "ACACACGTGTGTCCGGATGCATGC" * 30,
}


def test_build_rype_index_real_rype_smoke(tmp_path, monkeypatch):
    """Smoke test the REAL `rype_index_create` (seam NOT stubbed): assert it
    runs to status 'ok' on structured synthetic data and writes a populated
    `.ryxdi` — i.e. it isn't segfaulting or silently producing nothing.

    Runs against the team-mirror miint build (conftest sets
    MIINT_EXTENSION_REPO), which carries rype_index_create.
    """
    from qiita_compute_orchestrator.jobs import build_rype_index

    shared_root = tmp_path / "shared"
    monkeypatch.setenv("PATH_DERIVED", str(shared_root))

    reference_idx = 4242
    rows = [(fidx, 0, seq) for fidx, seq in _SMOKE_FEATURES.items()]
    chunks_dir = _write_chunks_dir(tmp_path / "reference_sequence_chunks", rows)

    inputs = build_rype_index.Inputs(
        reference_sequence_chunks=chunks_dir,
        reference_idx=reference_idx,
        work_ticket_idx=1,
    )
    # No seam monkeypatch → real rype_index_create runs. execute() raises if
    # the function returns a non-'ok' status.
    out = asyncio.run(build_rype_index.execute(inputs, tmp_path / "ws"))

    meta = json.loads(Path(out["rype_index_meta"]).read_text())
    index_dir = Path(meta["fs_path"])
    assert index_dir.is_dir(), "rype did not create the .ryxdi directory"
    # A real index has a manifest plus Parquet content (buckets + inverted
    # shards per docs). Assert the manifest exists and the tree carries data,
    # without over-coupling to exact internal filenames.
    assert (index_dir / "manifest.toml").is_file(), "no manifest.toml in .ryxdi"
    parquet_files = list(index_dir.rglob("*.parquet"))
    assert parquet_files, "rype index has no Parquet content (empty/partial build)"

    assert meta["params"] == {"k": 64, "w": 20, "bucket_name": f"reference_{reference_idx}"}


def test_build_rype_index_missing_chunks_raises(tmp_path, monkeypatch):
    from qiita_compute_orchestrator.jobs import build_rype_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    inputs = build_rype_index.Inputs(
        reference_sequence_chunks=tmp_path / "does-not-exist",
        reference_idx=1,
        work_ticket_idx=1,
    )
    with pytest.raises(FileNotFoundError):
        asyncio.run(build_rype_index.execute(inputs, tmp_path / "ws"))


def _run_memory_split(tmp_path, monkeypatch, alloc_gb):
    """Run execute() under a `alloc_gb` SLURM cgroup with rype + DuckDB stubbed,
    returning the captured (duckdb_memory_gb, rype max_memory) split."""
    from qiita_compute_orchestrator.jobs import build_rype_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    # slurm_alloc_gb reads SLURM_MEM_PER_NODE in MB.
    monkeypatch.setenv("SLURM_MEM_PER_NODE", str(alloc_gb * 1024))
    chunks_dir = _write_chunks_dir(
        tmp_path / "reference_sequence_chunks", [(1, 0, "ACGT"), (2, 0, "CCCC")]
    )

    captured: dict = {}

    def fake_build(conn, chunk_table, output_path, mapping_table, *, k, w, max_memory):
        captured["max_memory"] = max_memory
        Path(output_path).mkdir(parents=True, exist_ok=True)
        return "ok"

    monkeypatch.setattr(build_rype_index, "_run_rype_index_create", fake_build)

    # Spy on the DuckDB-side cap directly (rype's share alone is only an inverse
    # proxy — pin both sides of the split).
    real_apply = build_rype_index.apply_duckdb_settings

    def spy_apply(conn, duckdb_tmp, *, memory_gb, threads):
        captured["duckdb_memory_gb"] = memory_gb
        real_apply(conn, duckdb_tmp, memory_gb=memory_gb, threads=threads)

    monkeypatch.setattr(build_rype_index, "apply_duckdb_settings", spy_apply)

    inputs = build_rype_index.Inputs(
        reference_sequence_chunks=chunks_dir, reference_idx=5, work_ticket_idx=1
    )
    asyncio.run(build_rype_index.execute(inputs, tmp_path / "ws"))
    return captured


def test_build_rype_index_memory_split_under_slurm(tmp_path, monkeypatch):
    """Under SLURM the cgroup is split DuckDB(bounded) / rype(elastic). DuckDB is
    hard-capped at `_DUCKDB_MEMORY_CAP_GB` (NOT the 4 GB off-SLURM fallback, which
    OOMed feeding a genome-scale chunk scan); rype gets the remainder above its
    floor. At the 64 GB starting allocation + 8-thread headroom (2 + ceil(8*0.5)
    = 6): DuckDB = min(64-6, 30) = 30 (the cap binds), leaving 64 - 30 - 6 = 28 GB
    for rype — below its 30 GB floor, so rype gets the floor. Pins both sides."""
    captured = _run_memory_split(tmp_path, monkeypatch, alloc_gb=64)
    # DuckDB hard-capped at _DUCKDB_MEMORY_CAP_GB (30), NOT the 4 GB fallback.
    assert captured["duckdb_memory_gb"] == 30
    # Remainder (28) is below rype's _RYPE_MAX_MEMORY_GB floor (30) → floor wins.
    assert captured["max_memory"] == 30 * 1024**3


def test_build_rype_index_rype_memory_grows_on_oom_retry(tmp_path, monkeypatch):
    """rype's `max_memory` increases with each OOM retry. The control plane
    doubles a 64 GB OOM to the 128 GB action_ceiling; at 128 GB DuckDB STAYS
    capped at 30 (a bigger allocation must not grow DuckDB), so rype's elastic
    share grows to 128 - 30 - 6 = 92 GB, well above its 30 GB floor."""
    captured = _run_memory_split(tmp_path, monkeypatch, alloc_gb=128)
    # DuckDB unchanged by the bigger allocation — still hard-capped at 30.
    assert captured["duckdb_memory_gb"] == 30
    # rype gets (alloc - DuckDB cap - headroom) = 128 - 30 - 6 = 92 GB.
    assert captured["max_memory"] == 92 * 1024**3
