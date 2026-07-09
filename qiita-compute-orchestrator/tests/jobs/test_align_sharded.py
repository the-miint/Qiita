"""Isolated unit tests for `align_sharded.execute` + `plan`.

The real miint seams — `rype_classify` (read_to_shard build) and
`align_{minimap2,bowtie2}_sharded` — need the extension, real sequence bytes, and
per-shard indexes, so they are exercised by the integration smoke
(`tests/integration/test_sharded_alignment.py`). Here both seams are stubbed and we
assert the orchestration around them:

  - the query is the WHOLE read set as `(read_id = sequence_idx, sequence1,
    sequence2)` — ONE query, no SE/PE split (a read set is uniformly SE or PE by
    construction; the tools handle the mode natively);
  - a SINGLE align call runs (the aligner is dispatched by `Inputs.aligner` —
    minimap2 carries a preset, bowtie2 does not);
  - the aligner's FULL output is passed through VERBATIM (nothing dropped — the
    mate columns survive), with only `prep_sample_idx` (stamped PER ROW from the
    reads) and `feature_idx` (`CAST(reference)`) added;
  - a paired-end read's two mate rows both survive AND keep their mate columns, so
    the pairing is explicit (not two unrelated rows);
  - cross-shard multiplicity emits one distinct-feature row per shard (no dedup);
  - an empty alignment set is VALID (no fail-fast);
  - a failed align leaves no partial output.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import duckdb
import pytest

# The columns the stubbed align seam materialises, mimicking the real
# align_*_sharded output (a representative subset of the full SAM columns — enough
# to prove the mate columns pass through verbatim). `reference`/`mate_reference` are
# VARCHAR subject ids (our feature_idx), matching the real function.
_ALIGN_COLS = (
    "read_id",
    "flags",
    "reference",
    "position",
    "stop_position",
    "mapq",
    "cigar",
    "mate_reference",
    "mate_position",
    "template_length",
)


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
    per (read in the query, shard_name). `alignments`: {read_id: [align_row, ...]}
    where an `align_row` is the tuple `(flags, reference, position, stop_position,
    mapq, cigar, mate_reference, mate_position, template_length)` the align seam
    emits for each read present in the query (one row per mate for a PE read). The
    seam CTAS's a full-schema raw table + inserts those rows, mirroring the real
    align_*_sharded (whose FULL output execute() passes through verbatim). `calls`
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
        if calls is not None:
            cols = [d[0] for d in conn.execute(f"SELECT * FROM {query_table} LIMIT 0").description]
            calls.append({"aligner": aligner, "cols": cols, "preset": preset})
        # CTAS the raw alignments table with the full align schema (verbatim), so
        # execute()'s `a.* EXCLUDE (read_id)` passes every column through.
        conn.execute(
            f"CREATE TABLE {dest_table} ("
            "read_id BIGINT, flags INTEGER, reference VARCHAR, position BIGINT, "
            "stop_position BIGINT, mapq INTEGER, cigar VARCHAR, "
            "mate_reference VARCHAR, mate_position BIGINT, template_length BIGINT)"
        )
        read_ids = [r[0] for r in conn.execute(f"SELECT read_id FROM {query_table}").fetchall()]
        for rid in read_ids:
            for row in alignments.get(rid, []):
                conn.execute(
                    f"INSERT INTO {dest_table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [rid, *row]
                )

    def fake_mm2(conn, query_table, shard_directory, read_to_shard_table, dest_table, *, preset):
        _do_align(conn, query_table, dest_table, aligner="minimap2", preset=preset)

    def fake_bt2(conn, query_table, shard_directory, read_to_shard_table, dest_table):
        _do_align(conn, query_table, dest_table, aligner="bowtie2", preset=None)

    monkeypatch.setattr(align_sharded, "_build_read_to_shard", fake_r2s)
    monkeypatch.setattr(align_sharded, "_run_align_minimap2_sharded", fake_mm2)
    monkeypatch.setattr(align_sharded, "_run_align_bowtie2_sharded", fake_bt2)


def _read_alignment(path: Path):
    """Return (columns, rows) of alignment.parquet. Rows project the columns the
    tests assert on, in a stable order; the parquet itself carries the full set."""
    with duckdb.connect(":memory:") as conn:
        cols = [
            d[0] for d in conn.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 0").description
        ]
        rows = conn.execute(
            "SELECT prep_sample_idx, sequence_idx, feature_idx, mate_feature_idx, "
            "flags, reference, position, stop_position, mapq, cigar, mate_reference, "
            f"mate_position, template_length FROM read_parquet('{path}') "
            "ORDER BY prep_sample_idx, sequence_idx, feature_idx, position, flags"
        ).fetchall()
    return cols, rows


# An align row for a simple single-end primary hit to `feature`: no mate (mate_*
# NULL, template_length 0). `(flags, reference, position, stop_position, mapq,
# cigar, mate_reference, mate_position, template_length)`.
def _se_hit(feature, *, flags=0, position=1, stop=41, mapq=60, cigar="40M"):
    return (flags, str(feature), position, stop, mapq, cigar, None, None, 0)


def test_align_sharded_single_call_and_full_passthrough_minimap2(tmp_path, monkeypatch):
    """A uniformly-SE block runs ONE minimap2 call over the whole set (no split),
    and the output carries the FULL align columns + prep_sample_idx + feature_idx,
    with prep_sample_idx stamped per row and NOTHING dropped."""
    from qiita_compute_orchestrator.jobs import align_sharded

    # reads 1 & 3 align (distinct prep_samples), read 2 routes nowhere.
    reads = _write_reads_parquet(
        tmp_path / "reads.parquet",
        [(10, 1, "ACGT", None), (10, 2, "TTGG", None), (20, 3, "GGCC", None)],
    )
    router, shard_dir = _make_indexes(tmp_path)

    calls: list = []
    captured: dict = {}
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={1: ["0"], 3: ["1"]},
        alignments={
            1: [_se_hit(100, position=5, stop=45)],
            3: [_se_hit(200, position=12, stop=52)],
        },
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

    # Exactly ONE align call over the WHOLE set (no SE/PE split), carrying the
    # full query columns; the documented preset + routing threshold reach miint.
    assert [c["aligner"] for c in calls] == ["minimap2"]
    assert calls[0]["preset"] == "sr"
    assert calls[0]["cols"] == ["read_id", "sequence1", "sequence2"]
    assert captured["threshold"] == align_sharded._ROUTING_THRESHOLD

    cols, rows = _read_alignment(Path(out["alignment"]))
    # Full aligner output preserved + the three added identity columns; read_id was
    # renamed to sequence_idx (same value), nothing dropped. Both the raw VARCHAR
    # reference/mate_reference AND the typed feature_idx/mate_feature_idx are present.
    assert cols == [
        "prep_sample_idx",
        "sequence_idx",
        "feature_idx",
        "mate_feature_idx",
        "flags",
        "reference",
        "position",
        "stop_position",
        "mapq",
        "cigar",
        "mate_reference",
        "mate_position",
        "template_length",
    ]
    # prep_sample_idx stamped PER ROW (read 1 -> 10, read 3 -> 20); feature_idx is
    # CAST(reference); mate columns (incl. mate_feature_idx) are NULL for these SE hits.
    assert rows == [
        (10, 1, 100, None, 0, "100", 5, 45, 60, "40M", None, None, 0),
        (20, 3, 200, None, 0, "200", 12, 52, 60, "40M", None, None, 0),
    ]


def test_align_sharded_dispatch_bowtie2(tmp_path, monkeypatch):
    """aligner='bowtie2' routes to the bowtie2 seam (no preset), never minimap2,
    in a single call."""
    from qiita_compute_orchestrator.jobs import align_sharded

    reads = _write_reads_parquet(tmp_path / "reads.parquet", [(10, 1, "ACGT", None)])
    router, shard_dir = _make_indexes(tmp_path)

    calls: list = []
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={1: ["0"]},
        alignments={1: [_se_hit(100)]},
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
    assert [c["aligner"] for c in calls] == ["bowtie2"]
    assert calls[0]["preset"] is None
    _cols, rows = _read_alignment(Path(out["alignment"]))
    assert rows == [(10, 1, 100, None, 0, "100", 1, 41, 60, "40M", None, None, 0)]


def test_align_sharded_cross_shard_multiplicity_no_dedup(tmp_path, monkeypatch):
    """A read routed to two shards aligns to a DISTINCT feature per shard and emits
    BOTH rows — no cross-shard dedup (a feature is in exactly one shard)."""
    from qiita_compute_orchestrator.jobs import align_sharded

    reads = _write_reads_parquet(tmp_path / "reads.parquet", [(10, 7, "ACGTTTGG", None)])
    router, shard_dir = _make_indexes(tmp_path)
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={7: ["0", "1"]},  # routes to BOTH shards
        alignments={7: [_se_hit(100, position=1, stop=41), _se_hit(200, position=3, stop=43)]},
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
        (10, 7, 100, None, 0, "100", 1, 41, 60, "40M", None, None, 0),
        (10, 7, 200, None, 0, "200", 3, 43, 60, "40M", None, None, 0),
    ]


def test_align_sharded_pe_pair_keeps_mate_columns(tmp_path, monkeypatch):
    """A paired-end read aligning within ONE shard emits one SAM row per mate —
    two rows sharing (sequence_idx, feature_idx). BOTH survive AND keep their mate
    columns (mate_reference / mate_position / template_length) so the pairing is
    EXPLICIT — they are one read's alignment to a feature, not two unrelated rows.
    Also pins the `mate_feature_idx` cast across BOTH SAM RNEXT encodings of a mate
    on the same feature: `'='` and the numeric id. This pins the correct PE
    representation at the unit level (the integration smoke verifies it against
    real miint)."""
    from qiita_compute_orchestrator.jobs import align_sharded

    reads = _write_reads_parquet(tmp_path / "reads.parquet", [(10, 5, "ACGTACGT", "TTGGCCAA")])
    router, shard_dir = _make_indexes(tmp_path)
    # One PE read routed to a single shard; the align seam emits two mate rows to
    # the same feature 100 with a signed template_length (+/- the insert size),
    # mimicking an fr pair (R1 fwd flags 99, R2 rev flags 147). mate_reference is
    # given as '=' on R1 and the numeric id "100" on R2 so both cast branches
    # resolve to mate_feature_idx 100.
    _install_stubs(
        align_sharded,
        monkeypatch,
        routing={5: ["0"]},
        alignments={
            5: [
                (99, "100", 1, 151, 60, "150M", "=", 151, 300),
                (147, "100", 151, 301, 60, "150M", "100", 1, -300),
            ]
        },
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
    # Both mate rows kept (ordered by position), each carrying its mate columns so
    # the pair is explicit — the raw mate_reference ('='/'100'), a mate_position, a
    # signed template_length, AND the decoded mate_feature_idx (100 either way).
    # NOT collapsed, NOT stripped to two anonymous rows.
    assert rows == [
        (10, 5, 100, 100, 99, "100", 1, 151, 60, "150M", "=", 151, 300),
        (10, 5, 100, 100, 147, "100", 151, 301, 60, "150M", "100", 1, -300),
    ]


def test_align_sharded_empty_alignment_is_valid(tmp_path, monkeypatch):
    """A block whose reads align nowhere yields an EMPTY alignment.parquet — valid,
    not a fail-fast — while keeping the full column schema."""
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
    cols, rows = _read_alignment(alignment)
    assert rows == []
    # The schema is intact even when empty (full aligner columns + the three added).
    assert cols[:4] == ["prep_sample_idx", "sequence_idx", "feature_idx", "mate_feature_idx"]
    assert "mate_reference" in cols and "template_length" in cols


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
