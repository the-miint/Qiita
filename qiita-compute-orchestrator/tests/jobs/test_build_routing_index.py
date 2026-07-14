"""Isolated unit tests for `build_routing_index.execute`.

The routing index is a WHOLE-REFERENCE, MULTI-bucket rype `.ryxdi` (one bucket
per shard) that `rype_classify` turns into the `read_to_shard` table the sharded
aligners consume. These tests stub the two heavy seams — the stream
(`open_reference_chunk_stream`) with a local chunk Parquet, and the real rype
build (`_run_rype_index_create`) — and assert the orchestration around them:

  - the whole reference is streamed (`feature_idx=None`), persisted to a
    workspace Parquet, and exposed to rype as a VIEW over that Parquet;
  - the multi-bucket mapping handed to rype is the staged `shard_mapping` verbatim
    (one `(feature_idx, bucket_name=str(shard_id))` row per feature);
  - the persistent index path is `rype_router_index_path` and its parent is made;
  - the meta records `index_type='rype_router'` / fs_path / params, and carries NO
    `shard_id` (whole-reference → register-index reads None).

A single real-`rype_index_create` smoke at the bottom (build seam NOT stubbed)
proves a multi-bucket router actually builds a populated `.ryxdi`; the DP-stream
+ `rype_classify` round-trip is the separate integration smoke.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

import duckdb
import pytest


def _write_chunks_parquet(path: Path, rows: list[tuple[int, int, str]]) -> Path:
    """Write a feature-keyed chunk Parquet `(feature_idx BIGINT, chunk_index
    INTEGER, chunk_data VARCHAR)` — the shape the stream carries."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(":memory:") as conn:
        if not rows:
            # 0-row but correctly-typed Parquet (a reference that streams nothing).
            conn.execute(
                "COPY (SELECT CAST(NULL AS BIGINT) AS feature_idx, "
                "CAST(NULL AS INTEGER) AS chunk_index, CAST(NULL AS VARCHAR) AS chunk_data "
                "WHERE false) "
                f"TO '{path}' (FORMAT PARQUET)"
            )
            return path
        values_sql = ", ".join(
            "(CAST(? AS BIGINT), CAST(? AS INTEGER), CAST(? AS VARCHAR))" for _ in rows
        )
        params: list = []
        for fidx, cidx, data in rows:
            params.extend([fidx, cidx, data])
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values_sql}) "
            "AS t(feature_idx, chunk_index, chunk_data)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )
    return path


def _write_mapping(path: Path, rows: list[tuple[int, str]]) -> Path:
    """Write a shard_mapping Parquet `(feature_idx BIGINT, bucket_name VARCHAR)`
    — one row per sharded feature, `bucket_name = str(shard_id)`. Empty writes a
    0-row but correctly-typed Parquet."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(":memory:") as conn:
        if not rows:
            conn.execute(
                "COPY (SELECT CAST(NULL AS BIGINT) AS feature_idx, "
                "CAST(NULL AS VARCHAR) AS bucket_name WHERE false) "
                f"TO '{path}' (FORMAT PARQUET)"
            )
            return path
        values_sql = ", ".join("(CAST(? AS BIGINT), CAST(? AS VARCHAR))" for _ in rows)
        params: list = []
        for fidx, bucket in rows:
            params.extend([fidx, bucket])
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values_sql}) AS t(feature_idx, bucket_name)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )
    return path


def _fake_stream_from_parquet(parquet: Path, captured: dict):
    """A fake `open_reference_chunk_stream` that registers a local chunk Parquet as
    the streamed relation (simulating the whole-reference DoGet), capturing the
    ticket scope args — mirrors the build_rype_index shard-mode test stub."""

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


def test_build_routing_index_orchestration(tmp_path, monkeypatch):
    from qiita_compute_orchestrator.jobs import build_routing_index

    shared_root = tmp_path / "shared"
    monkeypatch.setenv("PATH_DERIVED", str(shared_root))

    reference_idx = 7
    # Whole-reference stream: features 100 (multi-chunk), 200, 300.
    stream_parquet = _write_chunks_parquet(
        tmp_path / "stream.parquet",
        [(100, 1, "GGGG"), (100, 0, "ACGT"), (200, 0, "TTTT"), (300, 0, "CCCC")],
    )
    # shard_mapping: 100+200 -> shard 0, 300 -> shard 1 (bucket_name = str(shard_id)).
    mapping = _write_mapping(
        tmp_path / "shard_mapping.parquet", [(100, "0"), (200, "0"), (300, "1")]
    )

    stream_capture: dict = {}
    monkeypatch.setattr(
        build_routing_index,
        "open_reference_chunk_stream",
        _fake_stream_from_parquet(stream_parquet, stream_capture),
    )

    captured: dict = {}

    def fake_build(conn, chunk_table, output_path, mapping_table, *, k, w, max_memory):
        captured["mapping_rows"] = conn.execute(
            f"SELECT feature_idx, bucket_name FROM {mapping_table} ORDER BY feature_idx"
        ).fetchall()
        captured["chunk_features"] = [
            r[0]
            for r in conn.execute(
                f"SELECT DISTINCT feature_idx FROM {chunk_table} ORDER BY feature_idx"
            ).fetchall()
        ]
        captured.update(output_path=output_path, k=k, w=w, max_memory=max_memory)
        Path(output_path).mkdir(parents=True, exist_ok=True)
        (Path(output_path) / "manifest.toml").write_text("k=64\n")
        return "ok"

    monkeypatch.setattr(build_routing_index, "_run_rype_index_create", fake_build)

    inputs = build_routing_index.Inputs(
        reference_idx=reference_idx, work_ticket_idx=42, shard_mapping=mapping
    )
    out = asyncio.run(build_routing_index.execute(inputs, tmp_path / "workspace"))

    # Whole-reference stream (NOT feature-scoped).
    assert stream_capture["reference_idx"] == reference_idx
    assert stream_capture["feature_idx"] is None
    # The mapping handed to rype is the staged shard_mapping verbatim.
    assert captured["mapping_rows"] == [(100, "0"), (200, "0"), (300, "1")]
    # The chunk view covers every streamed feature.
    assert captured["chunk_features"] == [100, 200, 300]
    assert captured["k"] == 64
    assert captured["w"] == 20

    expected_dir = shared_root / "references" / str(reference_idx) / "rype-router.ryxdi"
    assert expected_dir.is_dir()
    assert captured["output_path"] == str(expected_dir)
    # The persistent .ryxdi is NOT a step output (lives outside the workspace).
    assert "rype_router_index_path" not in out

    meta = json.loads(Path(out["routing_index_meta"]).read_text())
    assert meta["index_type"] == "rype_router"
    assert meta["fs_path"] == str(expected_dir)
    assert meta["params"] == {
        "k": 64,
        "w": 20,
        "source": "stream",
        "feature_count": 3,
        "shard_count": 2,  # two distinct shard buckets
    }
    # Whole-reference router → no shard_id (register-index reads None).
    assert "shard_id" not in meta


def test_build_routing_index_meta_shard_id_none(tmp_path, monkeypatch):
    """The router meta carries no shard_id, so register-index's
    `meta.get('shard_id')` yields None — a whole-reference (NULL shard) row."""
    from qiita_compute_orchestrator.jobs import build_routing_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    stream_parquet = _write_chunks_parquet(tmp_path / "stream.parquet", [(1, 0, "ACGT")])
    mapping = _write_mapping(tmp_path / "shard_mapping.parquet", [(1, "0")])
    monkeypatch.setattr(
        build_routing_index,
        "open_reference_chunk_stream",
        _fake_stream_from_parquet(stream_parquet, {}),
    )

    def fake_build(conn, chunk_table, output_path, mapping_table, *, k, w, max_memory):
        Path(output_path).mkdir(parents=True, exist_ok=True)
        return "ok"

    monkeypatch.setattr(build_routing_index, "_run_rype_index_create", fake_build)

    inputs = build_routing_index.Inputs(reference_idx=1, work_ticket_idx=1, shard_mapping=mapping)
    out = asyncio.run(build_routing_index.execute(inputs, tmp_path / "ws"))
    meta = json.loads(Path(out["routing_index_meta"]).read_text())
    assert meta.get("shard_id") is None


def test_build_routing_index_clears_stale_on_rerun(tmp_path, monkeypatch):
    """A retry re-runs against the same persistent path; any prior (partial)
    router `.ryxdi` is rmtree'd first so the rebuild is deterministic."""
    from qiita_compute_orchestrator.jobs import build_routing_index

    shared_root = tmp_path / "shared"
    monkeypatch.setenv("PATH_DERIVED", str(shared_root))
    stream_parquet = _write_chunks_parquet(tmp_path / "stream.parquet", [(1, 0, "ACGT")])
    mapping = _write_mapping(tmp_path / "shard_mapping.parquet", [(1, "0")])

    router_dir = shared_root / "references" / "9" / "rype-router.ryxdi"
    router_dir.mkdir(parents=True)
    (router_dir / "stale.partial").write_text("leftover")

    monkeypatch.setattr(
        build_routing_index,
        "open_reference_chunk_stream",
        _fake_stream_from_parquet(stream_parquet, {}),
    )

    def fake_build(conn, chunk_table, output_path, mapping_table, *, k, w, max_memory):
        assert not Path(output_path).exists()  # stale dir gone before rype runs
        Path(output_path).mkdir(parents=True, exist_ok=True)
        return "ok"

    monkeypatch.setattr(build_routing_index, "_run_rype_index_create", fake_build)

    inputs = build_routing_index.Inputs(reference_idx=9, work_ticket_idx=1, shard_mapping=mapping)
    asyncio.run(build_routing_index.execute(inputs, tmp_path / "ws"))
    assert not (router_dir / "stale.partial").exists()


def test_build_routing_index_raises_on_non_ok_status(tmp_path, monkeypatch):
    from qiita_compute_orchestrator.jobs import build_routing_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    stream_parquet = _write_chunks_parquet(tmp_path / "stream.parquet", [(1, 0, "ACGT")])
    mapping = _write_mapping(tmp_path / "shard_mapping.parquet", [(1, "0")])
    monkeypatch.setattr(
        build_routing_index,
        "open_reference_chunk_stream",
        _fake_stream_from_parquet(stream_parquet, {}),
    )

    def fake_build(conn, chunk_table, output_path, mapping_table, *, k, w, max_memory):
        return "error"

    monkeypatch.setattr(build_routing_index, "_run_rype_index_create", fake_build)

    inputs = build_routing_index.Inputs(reference_idx=1, work_ticket_idx=1, shard_mapping=mapping)
    with pytest.raises(RuntimeError, match="rype_index_create"):
        asyncio.run(build_routing_index.execute(inputs, tmp_path / "ws"))


def test_build_routing_index_missing_mapping_raises(tmp_path, monkeypatch):
    from qiita_compute_orchestrator.jobs import build_routing_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    inputs = build_routing_index.Inputs(
        reference_idx=1, work_ticket_idx=1, shard_mapping=tmp_path / "does-not-exist.parquet"
    )
    with pytest.raises(FileNotFoundError):
        asyncio.run(build_routing_index.execute(inputs, tmp_path / "ws"))


def test_build_routing_index_empty_mapping_raises(tmp_path, monkeypatch):
    """An empty shard_mapping (no feature→shard rows) is a fail-fast — there is
    nothing to route."""
    from qiita_compute_orchestrator.jobs import build_routing_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    stream_parquet = _write_chunks_parquet(tmp_path / "stream.parquet", [(1, 0, "ACGT")])
    mapping = _write_mapping(tmp_path / "shard_mapping.parquet", [])
    monkeypatch.setattr(
        build_routing_index,
        "open_reference_chunk_stream",
        _fake_stream_from_parquet(stream_parquet, {}),
    )

    def fake_build(conn, chunk_table, output_path, mapping_table, *, k, w, max_memory):
        raise AssertionError("rype build reached for an empty mapping")

    monkeypatch.setattr(build_routing_index, "_run_rype_index_create", fake_build)

    inputs = build_routing_index.Inputs(reference_idx=1, work_ticket_idx=1, shard_mapping=mapping)
    with pytest.raises(ValueError, match="shard_mapping"):
        asyncio.run(build_routing_index.execute(inputs, tmp_path / "ws"))


def test_build_routing_index_empty_stream_raises(tmp_path, monkeypatch):
    """A reference that streams no chunks is a fail-fast — nothing to route."""
    from qiita_compute_orchestrator.jobs import build_routing_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    empty_stream = _write_chunks_parquet(tmp_path / "stream.parquet", [])
    mapping = _write_mapping(tmp_path / "shard_mapping.parquet", [(1, "0")])
    monkeypatch.setattr(
        build_routing_index,
        "open_reference_chunk_stream",
        _fake_stream_from_parquet(empty_stream, {}),
    )

    def fake_build(conn, chunk_table, output_path, mapping_table, *, k, w, max_memory):
        raise AssertionError("rype build reached for an empty stream")

    monkeypatch.setattr(build_routing_index, "_run_rype_index_create", fake_build)

    inputs = build_routing_index.Inputs(reference_idx=1, work_ticket_idx=1, shard_mapping=mapping)
    with pytest.raises(ValueError, match="streamed any sequence chunks"):
        asyncio.run(build_routing_index.execute(inputs, tmp_path / "ws"))


def _boom_build(conn, chunk_table, output_path, mapping_table, *, k, w, max_memory):
    raise AssertionError("rype build reached despite a mapping-integrity failure")


def test_build_routing_index_no_genome_feature_excluded_from_router(tmp_path, monkeypatch):
    """A streamed feature ABSENT from shard_mapping is a legitimate no-genome member
    (a partial genome map / deferred 16S leaves it shard_id NULL). The router corpus
    is scoped to the MAPPED set and builds over only those features, rather than
    hard-failing AFTER the per-shard fan-out has already committed its index builds."""
    from qiita_compute_orchestrator.jobs import build_routing_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    # Stream has 100, 200, 300; mapping omits 300 (a no-genome member).
    stream_parquet = _write_chunks_parquet(
        tmp_path / "stream.parquet", [(100, 0, "ACGT"), (200, 0, "TTTT"), (300, 0, "GGGG")]
    )
    mapping = _write_mapping(tmp_path / "shard_mapping.parquet", [(100, "0"), (200, "1")])
    monkeypatch.setattr(
        build_routing_index,
        "open_reference_chunk_stream",
        _fake_stream_from_parquet(stream_parquet, {}),
    )

    captured: dict = {}

    def fake_build(conn, chunk_table, output_path, mapping_table, *, k, w, max_memory):
        captured["chunk_features"] = [
            r[0]
            for r in conn.execute(
                f"SELECT DISTINCT feature_idx FROM {chunk_table} ORDER BY feature_idx"
            ).fetchall()
        ]
        Path(output_path).mkdir(parents=True, exist_ok=True)
        return "ok"

    monkeypatch.setattr(build_routing_index, "_run_rype_index_create", fake_build)

    ws = tmp_path / "ws"
    inputs = build_routing_index.Inputs(reference_idx=1, work_ticket_idx=1, shard_mapping=mapping)
    out = asyncio.run(build_routing_index.execute(inputs, ws))

    # The router corpus is scoped to the mapped set — the no-genome feature 300 is
    # excluded, not a fatal error.
    assert captured["chunk_features"] == [100, 200]
    meta = json.loads(Path(out["routing_index_meta"]).read_text())
    assert meta["params"]["feature_count"] == 2
    # The whole-reference chunk dump is cleaned up (not leaked into shared scratch).
    assert not (ws / "router_chunks.parquet").exists()


def test_build_routing_index_unchunked_mapped_feature_raises(tmp_path, monkeypatch):
    """A mapped feature with no streamed chunks is a bucket over nothing — fail-fast."""
    from qiita_compute_orchestrator.jobs import build_routing_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    # Stream has 100, 200; mapping adds a phantom 300.
    stream_parquet = _write_chunks_parquet(
        tmp_path / "stream.parquet", [(100, 0, "ACGT"), (200, 0, "TTTT")]
    )
    mapping = _write_mapping(
        tmp_path / "shard_mapping.parquet", [(100, "0"), (200, "1"), (300, "1")]
    )
    monkeypatch.setattr(
        build_routing_index,
        "open_reference_chunk_stream",
        _fake_stream_from_parquet(stream_parquet, {}),
    )
    monkeypatch.setattr(build_routing_index, "_run_rype_index_create", _boom_build)

    inputs = build_routing_index.Inputs(reference_idx=1, work_ticket_idx=1, shard_mapping=mapping)
    with pytest.raises(ValueError, match="no streamed chunks"):
        asyncio.run(build_routing_index.execute(inputs, tmp_path / "ws"))


def test_build_routing_index_duplicate_feature_raises(tmp_path, monkeypatch):
    """A feature mapped to more than one shard violates the one-shard-per-feature
    contract — fail-fast."""
    from qiita_compute_orchestrator.jobs import build_routing_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    stream_parquet = _write_chunks_parquet(tmp_path / "stream.parquet", [(100, 0, "ACGT")])
    mapping = _write_mapping(tmp_path / "shard_mapping.parquet", [(100, "0"), (100, "1")])
    monkeypatch.setattr(
        build_routing_index,
        "open_reference_chunk_stream",
        _fake_stream_from_parquet(stream_parquet, {}),
    )
    monkeypatch.setattr(build_routing_index, "_run_rype_index_create", _boom_build)

    inputs = build_routing_index.Inputs(reference_idx=1, work_ticket_idx=1, shard_mapping=mapping)
    with pytest.raises(ValueError, match="more than one shard"):
        asyncio.run(build_routing_index.execute(inputs, tmp_path / "ws"))


def test_build_routing_index_null_bucket_raises(tmp_path, monkeypatch):
    """A NULL bucket_name (or feature_idx) row is a malformed mapping — fail-fast."""
    from qiita_compute_orchestrator.jobs import build_routing_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    stream_parquet = _write_chunks_parquet(
        tmp_path / "stream.parquet", [(100, 0, "ACGT"), (200, 0, "TTTT")]
    )
    mapping = _write_mapping(tmp_path / "shard_mapping.parquet", [(100, "0"), (200, None)])
    monkeypatch.setattr(
        build_routing_index,
        "open_reference_chunk_stream",
        _fake_stream_from_parquet(stream_parquet, {}),
    )
    monkeypatch.setattr(build_routing_index, "_run_rype_index_create", _boom_build)

    inputs = build_routing_index.Inputs(reference_idx=1, work_ticket_idx=1, shard_mapping=mapping)
    with pytest.raises(ValueError, match="NULL feature_idx or"):
        asyncio.run(build_routing_index.execute(inputs, tmp_path / "ws"))


def test_build_routing_index_memory_split_under_slurm(tmp_path, monkeypatch):
    """Under SLURM the cgroup is split DuckDB(bounded)/rype(elastic), identical to
    build_rype_index: at a 64 GB alloc + 8-thread headroom (6), DuckDB = min(64-6,
    8) = 8 (cap binds), rype gets 64-8-6 = 50 GB (above its 30 GB floor)."""
    from qiita_compute_orchestrator.jobs import build_routing_index

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "shared"))
    monkeypatch.setenv("SLURM_MEM_PER_NODE", str(64 * 1024))
    stream_parquet = _write_chunks_parquet(
        tmp_path / "stream.parquet", [(1, 0, "ACGT"), (2, 0, "CCCC")]
    )
    mapping = _write_mapping(tmp_path / "shard_mapping.parquet", [(1, "0"), (2, "1")])
    monkeypatch.setattr(
        build_routing_index,
        "open_reference_chunk_stream",
        _fake_stream_from_parquet(stream_parquet, {}),
    )

    captured: dict = {}

    def fake_build(conn, chunk_table, output_path, mapping_table, *, k, w, max_memory):
        captured["max_memory"] = max_memory
        Path(output_path).mkdir(parents=True, exist_ok=True)
        return "ok"

    monkeypatch.setattr(build_routing_index, "_run_rype_index_create", fake_build)

    real_apply = build_routing_index.apply_duckdb_settings

    def spy_apply(conn, duckdb_tmp, *, memory_gb, threads):
        captured["duckdb_memory_gb"] = memory_gb
        real_apply(conn, duckdb_tmp, memory_gb=memory_gb, threads=threads)

    monkeypatch.setattr(build_routing_index, "apply_duckdb_settings", spy_apply)

    inputs = build_routing_index.Inputs(reference_idx=5, work_ticket_idx=1, shard_mapping=mapping)
    asyncio.run(build_routing_index.execute(inputs, tmp_path / "ws"))
    assert captured["duckdb_memory_gb"] == 8  # cap binds, not the 4 GB fallback
    assert captured["max_memory"] == 50 * 1024**3  # remainder above rype's 30 GB floor


# Synthetic but STRUCTURED features (distinct motifs tiled), each ~640 bp (> k=64),
# single-chunk. Two shards: {100,200} -> "0", {300} -> "1". Deterministic — no RNG.
_SMOKE_FEATURES: dict[int, str] = {
    100: "ACGTACGTGGCCTTAAACGTTGCA" * 30,
    200: "TTGGCCAATTGGCCAAGTGTGTGT" * 30,
    300: "ACACACGTGTGTCCGGATGCATGC" * 30,
}
_SMOKE_MAPPING = [(100, "0"), (200, "0"), (300, "1")]


def test_build_routing_index_real_rype_smoke(tmp_path, monkeypatch):
    """Smoke the REAL `rype_index_create` with a MULTI-bucket mapping (seam NOT
    stubbed, stream seam stubbed with a local Parquet): a whole-reference router
    over structured features assigned to two shards builds a populated `.ryxdi`
    with status 'ok'. Runs against the team-mirror miint build (conftest sets
    MIINT_EXTENSION_REPO). The DP-stream + `rype_classify` round-trip is the
    separate integration smoke."""
    from qiita_compute_orchestrator.jobs import build_routing_index

    shared_root = tmp_path / "shared"
    monkeypatch.setenv("PATH_DERIVED", str(shared_root))

    reference_idx = 5252
    stream_parquet = _write_chunks_parquet(
        tmp_path / "stream.parquet", [(f, 0, seq) for f, seq in _SMOKE_FEATURES.items()]
    )
    mapping = _write_mapping(tmp_path / "shard_mapping.parquet", _SMOKE_MAPPING)
    monkeypatch.setattr(
        build_routing_index,
        "open_reference_chunk_stream",
        _fake_stream_from_parquet(stream_parquet, {}),
    )

    inputs = build_routing_index.Inputs(
        reference_idx=reference_idx, work_ticket_idx=1, shard_mapping=mapping
    )
    out = asyncio.run(build_routing_index.execute(inputs, tmp_path / "ws"))

    meta = json.loads(Path(out["routing_index_meta"]).read_text())
    index_dir = Path(meta["fs_path"])
    assert index_dir == shared_root / "references" / str(reference_idx) / "rype-router.ryxdi"
    assert index_dir.is_dir(), "rype did not create the router .ryxdi directory"
    assert (index_dir / "manifest.toml").is_file(), "no manifest.toml in router .ryxdi"
    assert list(index_dir.rglob("*.parquet")), "router index has no Parquet content"
    assert meta["params"]["shard_count"] == 2
    assert meta["params"]["feature_count"] == 3
