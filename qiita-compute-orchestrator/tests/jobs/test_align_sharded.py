"""Isolated unit tests for `align_sharded.execute` + `plan`.

The real miint seams — `rype_classify` (read_to_shard build) and
`align_{minimap2,bowtie2}_sharded` — need the extension, real sequence bytes, and
per-shard indexes, so they are exercised by the integration smoke
(`tests/integration/test_sharded_alignment.py`). Here both seams are stubbed with
QUERY-AWARE fakes (they read the sub-batch's query so the SE/PE split is honoured)
and we assert the orchestration around them:

  - the query is `(read_id = sequence_idx, sequence1[, sequence2])`, split into a
    single-end sub-batch (no `sequence2`) and a paired-end sub-batch (with it);
  - `read_to_shard` is rebuilt per sub-batch and handed to align;
  - the aligner is dispatched by `Inputs.aligner` (minimap2 carries a preset,
    bowtie2 does not);
  - the sorted `alignment.parquet` stamps `prep_sample_idx` PER ROW from the reads
    and emits every alignment with NO cross-shard dedup;
  - an empty alignment set is VALID (no fail-fast);
  - a failed align leaves no partial output.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import duckdb
import pytest


def _write_reads_parquet(path: Path, rows: list[tuple[int, int, str, str | None]]) -> Path:
    """Write a staged read-block Parquet with the columns align_sharded reads:
    `(prep_sample_idx BIGINT, sequence_idx BIGINT, sequence1 VARCHAR, sequence2
    VARCHAR)`. `rows` = (prep_sample_idx, sequence_idx, sequence1, sequence2)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(":memory:") as conn:
        if not rows:
            conn.execute(
                "COPY (SELECT CAST(NULL AS BIGINT) AS prep_sample_idx, "
                "CAST(NULL AS BIGINT) AS sequence_idx, CAST(NULL AS VARCHAR) AS sequence1, "
                "CAST(NULL AS VARCHAR) AS sequence2 WHERE false) "
                f"TO '{path}' (FORMAT PARQUET)"
            )
            return path
        values_sql = ", ".join(
            "(CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS VARCHAR))"
            for _ in rows
        )
        params: list = []
        for ps, sidx, s1, s2 in rows:
            params.extend([ps, sidx, s1, s2])
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values_sql}) "
            "AS t(prep_sample_idx, sequence_idx, sequence1, sequence2)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )
    return path


def _make_indexes(tmp_path):
    """A populated router `.ryxdi` dir + a shard_directory (both just need to be
    non-empty for the validators — the real align is stubbed)."""
    router = tmp_path / "rype-router.ryxdi"
    router.mkdir(parents=True)
    (router / "manifest.toml").write_text("k=64\n")
    shard_dir = tmp_path / "minimap2-shards"
    shard_dir.mkdir(parents=True)
    (shard_dir / "0.mmi").write_bytes(b"MMI")
    return router, shard_dir


def _install_stubs(align_sharded, monkeypatch, *, routing, alignments, calls=None, captured=None):
    """Install QUERY-AWARE stubs for the read_to_shard build + both align seams.

    `routing`: {read_id: [shard_name, ...]} — the read_to_shard build inserts a row
    per (read in THIS sub-batch's query, shard_name). `alignments`: {read_id:
    [(feature_idx, flags, position, stop_position, mapq, cigar), ...]} — the align
    seam inserts those rows for each read present in the sub-batch's query. `calls`
    (optional list) records each align call's (aligner, query_columns, preset);
    `captured` (optional dict) records the routing `threshold`."""

    def fake_r2s(conn, router_index_path, query_table, dest_table, *, threshold):
        if captured is not None:
            captured["threshold"] = threshold
        read_ids = [r[0] for r in conn.execute(f"SELECT read_id FROM {query_table}").fetchall()]
        for rid in read_ids:
            for shard_name in routing.get(rid, []):
                conn.execute(
                    f"INSERT INTO {dest_table} VALUES (CAST(? AS BIGINT), CAST(? AS VARCHAR))",
                    [rid, shard_name],
                )

    def _do_align(conn, query_table, dest_table, *, aligner, preset):
        cols = [d[0] for d in conn.execute(f"SELECT * FROM {query_table} LIMIT 0").description]
        if calls is not None:
            calls.append({"aligner": aligner, "cols": cols, "preset": preset})
        read_ids = [r[0] for r in conn.execute(f"SELECT read_id FROM {query_table}").fetchall()]
        for rid in read_ids:
            for row in alignments.get(rid, []):
                conn.execute(f"INSERT INTO {dest_table} VALUES (?, ?, ?, ?, ?, ?, ?)", [rid, *row])

    def fake_mm2(conn, query_table, shard_directory, read_to_shard_table, dest_table, *, preset):
        _do_align(conn, query_table, dest_table, aligner="minimap2", preset=preset)

    def fake_bt2(conn, query_table, shard_directory, read_to_shard_table, dest_table):
        _do_align(conn, query_table, dest_table, aligner="bowtie2", preset=None)

    monkeypatch.setattr(align_sharded, "_build_read_to_shard", fake_r2s)
    monkeypatch.setattr(align_sharded, "_run_align_minimap2_sharded", fake_mm2)
    monkeypatch.setattr(align_sharded, "_run_align_bowtie2_sharded", fake_bt2)


def _read_alignment(path: Path):
    with duckdb.connect(":memory:") as conn:
        cols = [
            d[0] for d in conn.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 0").description
        ]
        rows = conn.execute(
            f"SELECT prep_sample_idx, sequence_idx, feature_idx, flags, position, "
            f"stop_position, mapq, cigar FROM read_parquet('{path}') "
            "ORDER BY prep_sample_idx, sequence_idx, feature_idx, position, flags"
        ).fetchall()
    return cols, rows


def test_align_sharded_orchestration_minimap2(tmp_path, monkeypatch):
    from qiita_compute_orchestrator.jobs import align_sharded

    # read 1 SE, read 2 PE (routes nowhere), read 3 SE. prep_sample per row.
    reads = _write_reads_parquet(
        tmp_path / "reads.parquet",
        [(10, 1, "ACGT", None), (10, 2, "TTGG", "CCAA"), (20, 3, "GGCC", None)],
    )
    router, shard_dir = _make_indexes(tmp_path)

    calls: list = []
    captured: dict = {}
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={1: ["0"], 3: ["1"]},
        alignments={1: [(100, 0, 5, 45, 60, "40M")], 3: [(200, 0, 12, 52, 60, "40M")]},
        calls=calls,
        captured=captured,
    )

    inputs = align_sharded.Inputs(
        reads=reads,
        reference_idx=42,
        aligner="minimap2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    out = asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))

    # Both sub-batches ran the minimap2 seam: SE query (no sequence2), PE query (with).
    assert [c["aligner"] for c in calls] == ["minimap2", "minimap2"]
    assert [c["preset"] for c in calls] == ["sr", "sr"]
    assert calls[0]["cols"] == ["read_id", "sequence1"]  # SE sub-batch
    assert calls[1]["cols"] == ["read_id", "sequence1", "sequence2"]  # PE sub-batch
    # The documented routing threshold is what actually reaches rype_classify.
    assert captured["threshold"] == align_sharded._ROUTING_THRESHOLD

    cols, rows = _read_alignment(Path(out["alignment"]))
    assert cols == [
        "prep_sample_idx",
        "sequence_idx",
        "feature_idx",
        "flags",
        "position",
        "stop_position",
        "mapq",
        "cigar",
    ]
    # prep_sample_idx stamped PER ROW from the reads (read 1 -> 10, read 3 -> 20).
    assert rows == [
        (10, 1, 100, 0, 5, 45, 60, "40M"),
        (20, 3, 200, 0, 12, 52, 60, "40M"),
    ]


def test_align_sharded_dispatch_bowtie2(tmp_path, monkeypatch):
    """aligner='bowtie2' routes to the bowtie2 seam (no preset), never minimap2."""
    from qiita_compute_orchestrator.jobs import align_sharded

    reads = _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGT", None)])
    router, shard_dir = _make_indexes(tmp_path)

    calls: list = []
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={1: ["0"]},
        alignments={1: [(100, 0, 1, 41, 60, "40M")]},
        calls=calls,
    )

    inputs = align_sharded.Inputs(
        reads=reads,
        reference_idx=42,
        aligner="bowtie2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    out = asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))
    # Only the SE sub-batch has reads → one bowtie2 call, no preset.
    assert [c["aligner"] for c in calls] == ["bowtie2"]
    assert calls[0]["preset"] is None
    _cols, rows = _read_alignment(Path(out["alignment"]))
    assert rows == [(10, 1, 100, 0, 1, 41, 60, "40M")]


def test_align_sharded_multiplicity_no_dedup(tmp_path, monkeypatch):
    """A read routed to two shards aligns to a DISTINCT feature per shard and emits
    BOTH rows — no cross-shard dedup (feature unique per shard)."""
    from qiita_compute_orchestrator.jobs import align_sharded

    reads = _write_reads_parquet(tmp_path / "reads.parquet", [(10, 7, "ACGTTTGG", None)])
    router, shard_dir = _make_indexes(tmp_path)
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={7: ["0", "1"]},  # routes to BOTH shards
        alignments={7: [(100, 0, 1, 41, 60, "40M"), (200, 0, 3, 43, 60, "40M")]},
    )

    inputs = align_sharded.Inputs(
        reads=reads,
        reference_idx=42,
        aligner="minimap2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    out = asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))
    _cols, rows = _read_alignment(Path(out["alignment"]))
    assert rows == [
        (10, 7, 100, 0, 1, 41, 60, "40M"),
        (10, 7, 200, 0, 3, 43, 60, "40M"),
    ]


def test_align_sharded_pe_mate_rows_both_survive(tmp_path, monkeypatch):
    """A paired-end read aligning within ONE shard emits one SAM row per mate —
    two rows sharing (sequence_idx, feature_idx) but differing in flags/position.
    BOTH must survive (no dedup): `(sequence_idx, feature_idx)` is not a key. This
    pins the per-mate multiplicity source at the unit level (the integration smoke
    verifies it against real miint)."""
    from qiita_compute_orchestrator.jobs import align_sharded

    reads = _write_reads_parquet(tmp_path / "reads.parquet", [(10, 5, "ACGTACGT", "TTGGCCAA")])
    router, shard_dir = _make_indexes(tmp_path)
    # One PE read routed to a single shard; the align seam emits two mate rows,
    # same feature 100, distinct flags/position (mimicking R1 fwd + R2 rev).
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={5: ["0"]},
        alignments={5: [(100, 97, 1, 151, 60, "150M"), (100, 145, 151, 301, 60, "150M")]},
    )

    inputs = align_sharded.Inputs(
        reads=reads,
        reference_idx=42,
        aligner="minimap2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    out = asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))
    _cols, rows = _read_alignment(Path(out["alignment"]))
    # Both mate rows kept, ordered by position (mate1 at 1, mate2 at 151).
    assert rows == [
        (10, 5, 100, 97, 1, 151, 60, "150M"),
        (10, 5, 100, 145, 151, 301, 60, "150M"),
    ]


def test_align_sharded_se_pe_split_isolates_batches(tmp_path, monkeypatch):
    """A mixed SE/PE block runs the aligner on two uniform sub-batches: the SE
    query omits sequence2, the PE query carries it, and each read aligns in exactly
    one sub-batch (no double-counting)."""
    from qiita_compute_orchestrator.jobs import align_sharded

    reads = _write_reads_parquet(
        tmp_path / "reads.parquet",
        [(10, 1, "ACGT", None), (10, 2, "TTGG", "CCAA")],  # 1 SE, 2 PE
    )
    router, shard_dir = _make_indexes(tmp_path)
    calls: list = []
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={1: ["0"], 2: ["1"]},
        alignments={1: [(100, 0, 1, 41, 60, "40M")], 2: [(200, 0, 2, 42, 60, "40M")]},
        calls=calls,
    )

    inputs = align_sharded.Inputs(
        reads=reads,
        reference_idx=42,
        aligner="bowtie2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    out = asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))
    # SE sub-batch first (no sequence2), then PE (with sequence2).
    assert calls[0]["cols"] == ["read_id", "sequence1"]
    assert calls[1]["cols"] == ["read_id", "sequence1", "sequence2"]
    _cols, rows = _read_alignment(Path(out["alignment"]))
    # Each read aligned once, in its own sub-batch.
    assert rows == [
        (10, 1, 100, 0, 1, 41, 60, "40M"),
        (10, 2, 200, 0, 2, 42, 60, "40M"),
    ]


def test_align_sharded_empty_alignment_is_valid(tmp_path, monkeypatch):
    """A block whose reads align nowhere yields an EMPTY alignment.parquet — valid,
    not a fail-fast."""
    from qiita_compute_orchestrator.jobs import align_sharded

    reads = _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGT", None)])
    router, shard_dir = _make_indexes(tmp_path)
    _install_stubs(align_sharded, monkeypatch, routing={}, alignments={})

    inputs = align_sharded.Inputs(
        reads=reads,
        reference_idx=42,
        aligner="minimap2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    out = asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))
    alignment = Path(out["alignment"])
    assert alignment.exists()
    _cols, rows = _read_alignment(alignment)
    assert rows == []


def test_align_sharded_partial_output_removed_on_failure(tmp_path, monkeypatch):
    """A failed align leaves no partial alignment.parquet (the manifest walker must
    not promote it)."""
    from qiita_compute_orchestrator.jobs import align_sharded

    reads = _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGT", None)])
    router, shard_dir = _make_indexes(tmp_path)
    _install_stubs(align_sharded, monkeypatch, routing={1: ["0"]}, alignments={})

    def boom(conn, query_table, shard_directory, read_to_shard_table, dest_table, *, preset):
        raise RuntimeError("align blew up")

    monkeypatch.setattr(align_sharded, "_run_align_minimap2_sharded", boom)

    inputs = align_sharded.Inputs(
        reads=reads,
        reference_idx=42,
        aligner="minimap2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    with pytest.raises(RuntimeError, match="align blew up"):
        asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))
    assert not (tmp_path / "ws" / "alignment.parquet").exists()


def test_align_sharded_missing_reads_raises(tmp_path):
    from qiita_compute_orchestrator.jobs import align_sharded

    router, shard_dir = _make_indexes(tmp_path)
    inputs = align_sharded.Inputs(
        reads=tmp_path / "nope.parquet",
        reference_idx=1,
        aligner="minimap2",
        router_index_path=router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    with pytest.raises(FileNotFoundError, match="reads parquet"):
        asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))


def test_align_sharded_missing_router_raises(tmp_path):
    from qiita_compute_orchestrator.jobs import align_sharded

    reads = _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGT", None)])
    _router, shard_dir = _make_indexes(tmp_path)
    inputs = align_sharded.Inputs(
        reads=reads,
        reference_idx=1,
        aligner="minimap2",
        router_index_path=tmp_path / "absent.ryxdi",
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    with pytest.raises(FileNotFoundError, match="router_index_path"):
        asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))


def test_align_sharded_empty_router_raises(tmp_path):
    from qiita_compute_orchestrator.jobs import align_sharded

    reads = _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGT", None)])
    _router, shard_dir = _make_indexes(tmp_path)
    empty_router = tmp_path / "empty.ryxdi"
    empty_router.mkdir()
    inputs = align_sharded.Inputs(
        reads=reads,
        reference_idx=1,
        aligner="minimap2",
        router_index_path=empty_router,
        shard_directory=shard_dir,
        work_ticket_idx=1,
    )
    with pytest.raises(ValueError, match="populated .ryxdi"):
        asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))


def test_align_sharded_missing_shard_directory_raises(tmp_path):
    from qiita_compute_orchestrator.jobs import align_sharded

    reads = _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGT", None)])
    router, _shard_dir = _make_indexes(tmp_path)
    inputs = align_sharded.Inputs(
        reads=reads,
        reference_idx=1,
        aligner="minimap2",
        router_index_path=router,
        shard_directory=tmp_path / "absent-shards",
        work_ticket_idx=1,
    )
    with pytest.raises(FileNotFoundError, match="shard_directory"):
        asyncio.run(align_sharded.execute(inputs, tmp_path / "ws"))


def test_align_sharded_rejects_unknown_aligner(tmp_path):
    """Inputs validation (Literal) rejects an aligner other than minimap2/bowtie2."""
    from pydantic import ValidationError

    from qiita_compute_orchestrator.jobs import align_sharded

    with pytest.raises(ValidationError):
        align_sharded.Inputs(
            reads=tmp_path / "reads.parquet",
            reference_idx=1,
            aligner="bwa",
            router_index_path=tmp_path / "r.ryxdi",
            shard_directory=tmp_path / "shards",
            work_ticket_idx=1,
        )


def test_align_sharded_plan_sizes_walltime_from_read_count(tmp_path):
    """plan() returns a walltime hint (memory/cpu untouched) that grows with the
    read-block cardinality."""
    from qiita_compute_orchestrator.jobs import align_sharded

    def _walltime(n_rows):
        reads = _write_reads_parquet(
            tmp_path / f"reads_{n_rows}.parquet",
            [(1, i, "ACGT", None) for i in range(n_rows)],
        )
        inputs = align_sharded.Inputs(
            reads=reads,
            reference_idx=1,
            aligner="minimap2",
            router_index_path=tmp_path / "r.ryxdi",
            shard_directory=tmp_path / "shards",
            work_ticket_idx=1,
        )
        plan = align_sharded.plan(inputs)
        assert plan.resources is not None
        assert plan.resources.mem_gb is None and plan.resources.cpu is None
        return plan.resources.walltime

    small = _walltime(1)
    big = _walltime(500)
    assert small is not None and big is not None
    assert big >= small  # non-decreasing in read count
