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


def _write_roster(path: Path, rows: list[tuple[int, int]]) -> Path:
    """Write a shard feature roster Parquet `(feature_idx BIGINT,
    sequence_length_bp BIGINT)` — the per-shard subset B5 will stage from
    reference_membership.shard_id joined with the sequence lengths."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(":memory:") as conn:
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


def test_build_rype_index_shard_mode(tmp_path, monkeypatch):
    """In shard mode the build indexes ONLY the shard's features (the roster
    subset), writes to `.../shards/{shard_id}/index.ryxdi`, uses a shard-qualified
    bucket, and records `shard_id` in the meta JSON."""
    from qiita_compute_orchestrator.jobs import build_rype_index

    shared_root = tmp_path / "shared"
    monkeypatch.setenv("PATH_DERIVED", str(shared_root))

    reference_idx = 7
    shard_id = 3
    # Chunks carry three features; the shard roster covers only two of them.
    chunks_dir = _write_chunks_dir(
        tmp_path / "reference_sequence_chunks",
        [(100, 0, "ACGT"), (200, 0, "GGGG"), (300, 0, "TTTT")],
    )
    roster = _write_roster(tmp_path / "roster.parquet", [(100, 4), (300, 4)])

    captured: dict = {}

    def fake_build(conn, chunk_table, output_path, mapping_table, *, k, w, max_memory):
        captured["features"] = [
            r[0]
            for r in conn.execute(
                f"SELECT feature_idx FROM {mapping_table} ORDER BY feature_idx"
            ).fetchall()
        ]
        captured["bucket"] = conn.execute(
            f"SELECT DISTINCT bucket_name FROM {mapping_table}"
        ).fetchone()[0]
        captured["output_path"] = output_path
        Path(output_path).mkdir(parents=True, exist_ok=True)
        return "ok"

    monkeypatch.setattr(build_rype_index, "_run_rype_index_create", fake_build)

    inputs = build_rype_index.Inputs(
        reference_sequence_chunks=chunks_dir,
        reference_idx=reference_idx,
        work_ticket_idx=42,
        shard_id=shard_id,
        shard_features=roster,
    )
    out = asyncio.run(build_rype_index.execute(inputs, tmp_path / "ws"))

    # Feature 200 is excluded — only the roster's {100, 300} are indexed.
    assert captured["features"] == [100, 300]
    assert captured["bucket"] == f"reference_{reference_idx}_shard_{shard_id}"

    expected_dir = (
        shared_root / "references" / str(reference_idx) / "shards" / str(shard_id) / "index.ryxdi"
    )
    assert captured["output_path"] == str(expected_dir)
    assert expected_dir.is_dir()

    meta = json.loads(Path(out["rype_index_meta"]).read_text())
    assert meta["index_type"] == "rype"
    assert meta["fs_path"] == str(expected_dir)
    assert meta["shard_id"] == shard_id


def test_build_rype_index_host_meta_shard_id_none(tmp_path, monkeypatch):
    """Host (unsharded) mode records shard_id None — the register-index arm's
    `meta.get('shard_id')` yields None, preserving the whole-reference path."""
    from qiita_compute_orchestrator.jobs import build_rype_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    chunks_dir = _write_chunks_dir(tmp_path / "reference_sequence_chunks", [(1, 0, "ACGT")])

    def fake_build(conn, chunk_table, output_path, mapping_table, *, k, w, max_memory):
        Path(output_path).mkdir(parents=True, exist_ok=True)
        return "ok"

    monkeypatch.setattr(build_rype_index, "_run_rype_index_create", fake_build)

    inputs = build_rype_index.Inputs(
        reference_sequence_chunks=chunks_dir, reference_idx=1, work_ticket_idx=1
    )
    out = asyncio.run(build_rype_index.execute(inputs, tmp_path / "ws"))
    meta = json.loads(Path(out["rype_index_meta"]).read_text())
    assert meta.get("shard_id") is None


def test_build_rype_index_shard_inputs_both_or_neither(tmp_path):
    """shard_id and shard_features must be supplied together — one without the
    other is a misconfiguration and fails validation (fail fast)."""
    from qiita_compute_orchestrator.jobs import build_rype_index

    with pytest.raises(ValueError):
        build_rype_index.Inputs(
            reference_sequence_chunks=tmp_path / "c",
            reference_idx=1,
            work_ticket_idx=1,
            shard_id=0,  # missing shard_features
        )
    with pytest.raises(ValueError):
        build_rype_index.Inputs(
            reference_sequence_chunks=tmp_path / "c",
            reference_idx=1,
            work_ticket_idx=1,
            shard_features=tmp_path / "roster.parquet",  # missing shard_id
        )


def test_build_rype_index_plan_host_mode_no_opinion(tmp_path):
    """Host mode → empty JobPlan (no resource opinion), so the whole-reference
    baseline is never down-sized."""
    from qiita_compute_orchestrator.jobs import build_rype_index

    inputs = build_rype_index.Inputs(
        reference_sequence_chunks=tmp_path / "c", reference_idx=1, work_ticket_idx=1
    )
    plan = build_rype_index.plan(inputs)
    assert plan.resources is None


def test_build_rype_index_plan_shard_mode_sizes_mem_down(tmp_path):
    """Shard mode sizes mem_gb from the shard's total bp, floored at the
    runtime-consistent minimum (rype floor + DuckDB cap + headroom) and below the
    64 GB whole-reference baseline for a small shard."""
    import math

    from qiita_compute_orchestrator.jobs import build_rype_index

    roster = _write_roster(tmp_path / "roster.parquet", [(100, 500_000_000), (300, 500_000_000)])
    inputs = build_rype_index.Inputs(
        reference_sequence_chunks=tmp_path / "c",
        reference_idx=1,
        work_ticket_idx=1,
        shard_id=0,
        shard_features=roster,
    )
    plan = build_rype_index.plan(inputs)
    total_bp = 1_000_000_000
    expected = build_rype_index._SHARD_PLAN_FLOOR_GB + math.ceil(
        total_bp / build_rype_index._SHARD_PLAN_BP_PER_GB
    )
    assert plan.resources is not None
    assert plan.resources.mem_gb == expected
    assert plan.resources.mem_gb < 64  # below the host baseline → down-only composition applies it


def test_build_rype_index_plan_shard_mem_scales_with_bp(tmp_path):
    from qiita_compute_orchestrator.jobs import build_rype_index

    small = _write_roster(tmp_path / "small.parquet", [(1, 100_000_000)])
    big = _write_roster(tmp_path / "big.parquet", [(1, 40_000_000_000)])

    def _mem(roster):
        inputs = build_rype_index.Inputs(
            reference_sequence_chunks=tmp_path / "c",
            reference_idx=1,
            work_ticket_idx=1,
            shard_id=0,
            shard_features=roster,
        )
        return build_rype_index.plan(inputs).resources.mem_gb

    assert _mem(big) > _mem(small)


def test_build_rype_index_real_rype_shard_smoke(tmp_path, monkeypatch):
    """Smoke the REAL `rype_index_create` over a SHARD SUBSET (seam not stubbed):
    a roster covering two of three features builds a populated `.ryxdi` at the
    shard path, and the meta records shard_id. This is the authoritative miint
    re-verification for the per-shard build."""
    from qiita_compute_orchestrator.jobs import build_rype_index

    shared_root = tmp_path / "shared"
    monkeypatch.setenv("PATH_DERIVED", str(shared_root))

    reference_idx = 4243
    shard_id = 2
    rows = [(fidx, 0, seq) for fidx, seq in _SMOKE_FEATURES.items()]
    chunks_dir = _write_chunks_dir(tmp_path / "reference_sequence_chunks", rows)
    # Shard roster: two of the three smoke features (lengths are ~640 bp each).
    roster = _write_roster(
        tmp_path / "roster.parquet",
        [(100, len(_SMOKE_FEATURES[100])), (300, len(_SMOKE_FEATURES[300]))],
    )

    inputs = build_rype_index.Inputs(
        reference_sequence_chunks=chunks_dir,
        reference_idx=reference_idx,
        work_ticket_idx=1,
        shard_id=shard_id,
        shard_features=roster,
    )
    out = asyncio.run(build_rype_index.execute(inputs, tmp_path / "ws"))

    meta = json.loads(Path(out["rype_index_meta"]).read_text())
    index_dir = Path(meta["fs_path"])
    assert (
        index_dir
        == shared_root
        / "references"
        / str(reference_idx)
        / "shards"
        / str(shard_id)
        / "index.ryxdi"
    )
    assert index_dir.is_dir(), "rype did not create the shard .ryxdi directory"
    assert (index_dir / "manifest.toml").is_file(), "no manifest.toml in shard .ryxdi"
    assert list(index_dir.rglob("*.parquet")), "shard rype index has no Parquet content"
    assert meta["shard_id"] == shard_id


def test_build_rype_index_memory_split_under_slurm(tmp_path, monkeypatch):
    """Under SLURM the cgroup is split DuckDB(bounded) / rype(elastic). DuckDB is
    hard-capped at `_DUCKDB_MEMORY_CAP_GB` (small because `rype_index_create`
    windows its chunk feed, so DuckDB's working set is bounded by window size, not
    corpus size); rype gets the remainder above its floor. At the 64 GB starting
    allocation + 8-thread headroom (2 + ceil(8*0.5) = 6): DuckDB = min(64-6, 8) = 8
    (the cap binds), leaving 64 - 8 - 6 = 50 GB for rype — above its 30 GB floor,
    so rype gets the remainder. Pins both sides."""
    captured = _run_memory_split(tmp_path, monkeypatch, alloc_gb=64)
    # DuckDB hard-capped at _DUCKDB_MEMORY_CAP_GB (8), NOT the 4 GB fallback.
    assert captured["duckdb_memory_gb"] == 8
    # Remainder (50) is above rype's _RYPE_MAX_MEMORY_GB floor (30) → remainder wins.
    assert captured["max_memory"] == 50 * 1024**3


def test_build_rype_index_rype_memory_grows_on_oom_retry(tmp_path, monkeypatch):
    """rype's `max_memory` increases with each OOM retry. The control plane
    doubles a 64 GB OOM to the 128 GB action_ceiling; at 128 GB DuckDB STAYS
    capped at 8 (a bigger allocation must not grow DuckDB), so rype's elastic
    share grows to 128 - 8 - 6 = 114 GB, well above its 30 GB floor."""
    captured = _run_memory_split(tmp_path, monkeypatch, alloc_gb=128)
    # DuckDB unchanged by the bigger allocation — still hard-capped at 8.
    assert captured["duckdb_memory_gb"] == 8
    # rype gets (alloc - DuckDB cap - headroom) = 128 - 8 - 6 = 114 GB.
    assert captured["max_memory"] == 114 * 1024**3
