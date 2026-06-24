"""Isolated unit tests for `host_filter.execute` (build seams stubbed).

`host_filter` merges host-filter hits into the partial `qc_mask` and emits the
final DuckLake `read_mask` (one row per read; NO read dropped). It runs the host
filter in two stages — rype `rype_classify` FIRST, then minimap2 `align_minimap2`
on rype's survivors — over the QC-PASS subset (trimmed as the masked view would
serve), and marks any hit read `host_rype` / `host_minimap2`. Paired-end mates
share one `sequence_idx`, so flagging either mate marks the whole pair.

The real rype/minimap2 calls need the miint extension + real indexes, so they're
exercised in `test_host_filter_smoke.py`; here the seams (`_run_rype_classify`,
`_run_align_minimap2`) are stubbed and we assert the orchestration:

  - host set = rype-flagged ∪ minimap2-flagged; reason precedence
    minimap2 > rype > the qc_mask reason (host only overrides pass);
  - two-stage ordering: minimap2 only ever sees rype's survivors;
  - host classify runs ONLY on reason='pass' reads;
  - no host stage when neither index is bound (mask == qc_mask);
  - mask_idx / prep_sample_idx stamped on every output row;
  - fail-fast on missing reads / missing qc_mask / empty `.ryxdi` / zero-byte `.mmi`.

The `write_reads` fixture (tests/jobs/conftest.py) owns the fastq_to_parquet
reads schema; `_qc_mask` here writes the partial mask qc emits.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import duckdb
import pytest
from qiita_common.models import ReadMaskReason

_MASK_SCHEMA = [
    "mask_idx",
    "prep_sample_idx",
    "sequence_idx",
    "reason",
    "left_trim1",
    "right_trim1",
    "left_trim2",
    "right_trim2",
]

_MASK_IDX = 999
_PREP_SAMPLE_IDX = 5


def _schema(path: Path) -> list[str]:
    with duckdb.connect(":memory:") as conn:
        return [
            d[0] for d in conn.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 0").description
        ]


def _reasons(path: Path) -> dict[int, str]:
    with duckdb.connect(":memory:") as conn:
        return {
            r[0]: r[1]
            for r in conn.execute(
                f"SELECT sequence_idx, reason FROM read_parquet('{path}')"
            ).fetchall()
        }


def _qc_mask(path: Path, rows: list[tuple[int, str, bool]]) -> Path:
    """Write a qc_mask.parquet. `rows` are (sequence_idx, reason, is_paired);
    trims are zero (paired -> 0 mate trims, single-end -> NULL mate trims)."""
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "CREATE TABLE m(sequence_idx BIGINT, reason VARCHAR, "
            "left_trim1 UINTEGER, right_trim1 UINTEGER, "
            "left_trim2 UINTEGER, right_trim2 UINTEGER)"
        )
        for sidx, reason, paired in rows:
            mate = "0, 0" if paired else "NULL, NULL"
            conn.execute(f"INSERT INTO m VALUES (?, ?, 0, 0, {mate})", [sidx, reason])
        conn.execute(f"COPY m TO '{path}' (FORMAT PARQUET)")
    return path


def _ryxdi(tmp_path: Path, name: str = "host.ryxdi") -> Path:
    """A non-empty `.ryxdi` directory (rype index is a DIRECTORY)."""
    d = tmp_path / name
    d.mkdir()
    (d / "manifest.toml").write_text("k=64\n")
    return d


def _mmi(tmp_path: Path, name: str = "host.mmi") -> Path:
    """A non-empty `.mmi` file (minimap2 index is a FILE)."""
    p = tmp_path / name
    p.write_bytes(b"MMI-bytes")
    return p


def _inputs(host_filter, **kw):
    base = dict(mask_idx=_MASK_IDX, prep_sample_idx=_PREP_SAMPLE_IDX, work_ticket_idx=1)
    base.update(kw)
    return host_filter.Inputs(**base)


def test_host_filter_marks_rype_union_minimap2(tmp_path, monkeypatch, write_reads):
    """host set = rype ∪ minimap2; minimap2 sees ONLY rype's survivors; host
    overrides pass; the mask schema + mask_idx/prep_sample_idx are stamped."""
    from qiita_compute_orchestrator.jobs import host_filter

    reads = write_reads(
        tmp_path / "reads.parquet",
        [
            (10, "rA", "ACGTACGTAC", "ACGTACGTAC"),  # rype-flagged
            (20, "rB", "ACGTACGTAC", "ACGTACGTAC"),  # clean → pass
            (30, "rC", "ACGTACGTAC", "ACGTACGTAC"),  # minimap2-flagged
            (40, "rD", "ACGTACGTAC", None),  # single-end, clean → pass
        ],
    )
    qc_mask = _qc_mask(
        tmp_path / "qc_mask.parquet",
        [
            (10, ReadMaskReason.PASS.value, True),
            (20, ReadMaskReason.PASS.value, True),
            (30, ReadMaskReason.PASS.value, True),
            (40, ReadMaskReason.PASS.value, False),
        ],
    )

    seen: dict = {}

    def fake_rype(conn, index_path, sequence_table, dest_table, *, threshold):
        seen["threshold"] = threshold
        seen["rype_query_ids"] = sorted(
            r[0] for r in conn.execute(f"SELECT read_id FROM {sequence_table}").fetchall()
        )
        conn.execute(f"INSERT INTO {dest_table} VALUES (10)")

    def fake_mm2(conn, index_path, query_table, dest_table, *, preset):
        seen["preset"] = preset
        seen["mm2_query_ids"] = sorted(
            r[0] for r in conn.execute(f"SELECT DISTINCT read_id FROM {query_table}").fetchall()
        )
        conn.execute(f"INSERT INTO {dest_table} VALUES (30)")

    monkeypatch.setattr(host_filter, "_run_rype_classify", fake_rype)
    monkeypatch.setattr(host_filter, "_run_align_minimap2", fake_mm2)

    out = asyncio.run(
        host_filter.execute(
            _inputs(
                host_filter,
                reads=reads,
                qc_mask=qc_mask,
                host_rype_path=_ryxdi(tmp_path),
                host_minimap2_path=_mmi(tmp_path),
            ),
            tmp_path / "ws",
        )
    )

    assert _schema(out["read_mask"]) == _MASK_SCHEMA
    reasons = _reasons(out["read_mask"])
    assert reasons[10] == ReadMaskReason.HOST_RYPE.value
    assert reasons[20] == ReadMaskReason.PASS.value
    assert reasons[30] == ReadMaskReason.HOST_MINIMAP2.value
    assert reasons[40] == ReadMaskReason.PASS.value
    # rype saw one query row per pass read (both mates ride sequence1/sequence2).
    assert seen["rype_query_ids"] == [10, 20, 30, 40]
    assert seen["threshold"] == host_filter._RYPE_THRESHOLD
    # Two-stage: minimap2's query is rype's survivors — 10 excluded.
    assert seen["mm2_query_ids"] == [20, 30, 40]
    assert seen["preset"] == host_filter._MINIMAP2_PRESET
    # Every row carries the per-run constants.
    with duckdb.connect(":memory:") as conn:
        consts = conn.execute(
            f"SELECT DISTINCT mask_idx, prep_sample_idx FROM read_parquet('{out['read_mask']}')"
        ).fetchall()
    assert consts == [(_MASK_IDX, _PREP_SAMPLE_IDX)]


def test_host_filter_classifies_only_pass_reads(tmp_path, monkeypatch, write_reads):
    """Host classify runs ONLY on reason='pass' reads; a qc-failed read keeps its
    qc_* reason and is never handed to the tools (privacy: host never re-runs on a
    QC-failed, possibly-human read)."""
    from qiita_compute_orchestrator.jobs import host_filter

    reads = write_reads(
        tmp_path / "reads.parquet",
        [(10, "pass", "ACGTACGTAC", None), (20, "short", "ACGTACGTAC", None)],
    )
    qc_mask = _qc_mask(
        tmp_path / "qc_mask.parquet",
        [
            (10, ReadMaskReason.PASS.value, False),
            (20, ReadMaskReason.QC_TOO_SHORT.value, False),
        ],
    )

    seen: dict = {}

    def fake_rype(conn, index_path, sequence_table, dest_table, *, threshold):
        seen["query_ids"] = sorted(
            r[0] for r in conn.execute(f"SELECT read_id FROM {sequence_table}").fetchall()
        )
        # Flag everything it sees — but it must only see the pass read.
        conn.execute(f"INSERT INTO {dest_table} SELECT read_id FROM {sequence_table}")

    monkeypatch.setattr(host_filter, "_run_rype_classify", fake_rype)

    out = asyncio.run(
        host_filter.execute(
            _inputs(host_filter, reads=reads, qc_mask=qc_mask, host_rype_path=_ryxdi(tmp_path)),
            tmp_path / "ws",
        )
    )
    assert seen["query_ids"] == [10]  # the qc_too_short read never reached classify
    reasons = _reasons(out["read_mask"])
    assert reasons[10] == ReadMaskReason.HOST_RYPE.value
    assert reasons[20] == ReadMaskReason.QC_TOO_SHORT.value  # unchanged


def test_host_filter_no_indexes_keeps_qc_mask(tmp_path, monkeypatch, write_reads):
    """Neither index bound (host filtering disabled) → the mask is the qc_mask
    unchanged, neither seam invoked, mask_idx stamped."""
    from qiita_compute_orchestrator.jobs import host_filter

    reads = write_reads(
        tmp_path / "reads.parquet",
        [(10, "a", "ACGTACGTAC", "ACGTACGTAC"), (20, "b", "ACGTAC", None)],
    )
    qc_mask = _qc_mask(
        tmp_path / "qc_mask.parquet",
        [(10, ReadMaskReason.PASS.value, True), (20, ReadMaskReason.QC_TOO_SHORT.value, False)],
    )

    def boom(*a, **k):
        raise AssertionError("a filter seam was called with no index bound")

    monkeypatch.setattr(host_filter, "_run_rype_classify", boom)
    monkeypatch.setattr(host_filter, "_run_align_minimap2", boom)

    out = asyncio.run(
        host_filter.execute(_inputs(host_filter, reads=reads, qc_mask=qc_mask), tmp_path / "ws")
    )
    reasons = _reasons(out["read_mask"])
    assert reasons[10] == ReadMaskReason.PASS.value
    assert reasons[20] == ReadMaskReason.QC_TOO_SHORT.value


def test_host_filter_minimap2_only(tmp_path, monkeypatch, write_reads):
    """minimap2 alone (rype unbound): the survivors relation is all pass reads (no
    rype prefilter), and the flagged read becomes host_minimap2."""
    from qiita_compute_orchestrator.jobs import host_filter

    reads = write_reads(
        tmp_path / "reads.parquet",
        [(10, "a", "ACGTACGTAC", "ACGTACGTAC"), (20, "b", "ACGTACGTAC", "ACGTACGTAC")],
    )
    qc_mask = _qc_mask(
        tmp_path / "qc_mask.parquet",
        [(10, ReadMaskReason.PASS.value, True), (20, ReadMaskReason.PASS.value, True)],
    )

    def boom(*a, **k):  # rype must NOT be invoked when its path is unbound
        raise AssertionError("rype seam called with no rype index bound")

    def fake_mm2(conn, index_path, query_table, dest_table, *, preset):
        assert sorted(
            r[0] for r in conn.execute(f"SELECT DISTINCT read_id FROM {query_table}").fetchall()
        ) == [10, 20]
        conn.execute(f"INSERT INTO {dest_table} VALUES (10)")

    monkeypatch.setattr(host_filter, "_run_rype_classify", boom)
    monkeypatch.setattr(host_filter, "_run_align_minimap2", fake_mm2)

    out = asyncio.run(
        host_filter.execute(
            _inputs(host_filter, reads=reads, qc_mask=qc_mask, host_minimap2_path=_mmi(tmp_path)),
            tmp_path / "ws",
        )
    )
    reasons = _reasons(out["read_mask"])
    assert reasons[10] == ReadMaskReason.HOST_MINIMAP2.value
    assert reasons[20] == ReadMaskReason.PASS.value


def test_host_filter_missing_reads_raises(tmp_path):
    from qiita_compute_orchestrator.jobs import host_filter

    qc_mask = _qc_mask(tmp_path / "qc_mask.parquet", [(10, ReadMaskReason.PASS.value, False)])
    with pytest.raises(FileNotFoundError):
        asyncio.run(
            host_filter.execute(
                _inputs(host_filter, reads=tmp_path / "nope.parquet", qc_mask=qc_mask),
                tmp_path / "ws",
            )
        )


def test_host_filter_missing_qc_mask_raises(tmp_path, write_reads):
    from qiita_compute_orchestrator.jobs import host_filter

    reads = write_reads(tmp_path / "reads.parquet", [(10, "a", "ACGTAC", "ACGTAC")])
    with pytest.raises(FileNotFoundError):
        asyncio.run(
            host_filter.execute(
                _inputs(host_filter, reads=reads, qc_mask=tmp_path / "nope.parquet"),
                tmp_path / "ws",
            )
        )


def test_host_filter_empty_ryxdi_raises(tmp_path, write_reads):
    """An empty `.ryxdi` directory (no index content) is fail-fast."""
    from qiita_compute_orchestrator.jobs import host_filter

    reads = write_reads(tmp_path / "reads.parquet", [(10, "a", "ACGTAC", "ACGTAC")])
    qc_mask = _qc_mask(tmp_path / "qc_mask.parquet", [(10, ReadMaskReason.PASS.value, True)])
    empty = tmp_path / "empty.ryxdi"
    empty.mkdir()
    with pytest.raises((ValueError, FileNotFoundError)):
        asyncio.run(
            host_filter.execute(
                _inputs(host_filter, reads=reads, qc_mask=qc_mask, host_rype_path=empty),
                tmp_path / "ws",
            )
        )


def test_host_filter_zero_byte_mmi_raises(tmp_path, write_reads):
    """A zero-byte `.mmi` is a broken index — fail fast."""
    from qiita_compute_orchestrator.jobs import host_filter

    reads = write_reads(tmp_path / "reads.parquet", [(10, "a", "ACGTAC", "ACGTAC")])
    qc_mask = _qc_mask(tmp_path / "qc_mask.parquet", [(10, ReadMaskReason.PASS.value, True)])
    mmi = tmp_path / "empty.mmi"
    mmi.write_bytes(b"")
    with pytest.raises((ValueError, FileNotFoundError)):
        asyncio.run(
            host_filter.execute(
                _inputs(host_filter, reads=reads, qc_mask=qc_mask, host_minimap2_path=mmi),
                tmp_path / "ws",
            )
        )
