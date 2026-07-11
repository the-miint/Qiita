"""Unit tests for `syndna.execute` (the rype classify seam stubbed).

`syndna` is the FIRST step of the read-mask chain: it classifies the RAW reads
against a spike-in rype index and emits a PARTIAL mask (the 6-column qc_mask
shape) marking hits `spikein_syndna`, everything else `pass`. The real
`rype_classify` needs the miint extension and a built `.ryxdi`, so it is stubbed
here and exercised in test_syndna_smoke.py.

Asserted here:
  - hits become `spikein_syndna`, everything else `pass`;
  - the output is a partial mask (6 columns, all trims zero — SynDNA does not
    trim), under the `partial_mask` binding — NOT the final read_mask;
  - the mate-trim columns follow the read_mask convention (NULL single-end,
    0 paired) so both-mates counting stays correct downstream;
  - a missing or empty `.ryxdi` fails fast.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import duckdb
import pytest
from qiita_common.models import ReadMaskReason

_PASS = ReadMaskReason.PASS.value
_SPIKEIN = ReadMaskReason.SPIKEIN_SYNDNA.value


def _write_reads(path: Path, rows: list[tuple[int, str, str | None]]) -> Path:
    """(sequence_idx, sequence1, sequence2|None); no quals needed for classify."""
    values = ", ".join(
        "(CAST(5 AS BIGINT), CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS VARCHAR), "
        "CAST(NULL AS UTINYINT[]), CAST(? AS VARCHAR), CAST(NULL AS UTINYINT[]))"
        for _ in rows
    )
    params: list = []
    for sidx, s1, s2 in rows:
        params.extend([sidx, f"r{sidx}", s1, s2])
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values}) AS t("
            "prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )
    return path


def _rows(path: Path) -> list[tuple]:
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            f"SELECT sequence_idx, reason FROM read_parquet('{path}') ORDER BY sequence_idx"
        ).fetchall()


def _index(tmp_path: Path) -> Path:
    d = tmp_path / "syndna.ryxdi"
    d.mkdir()
    (d / "index.bin").write_text("x")
    return d


def _run(tmp_path, reads, index):
    from qiita_compute_orchestrator.jobs import syndna

    return asyncio.run(
        syndna.execute(
            syndna.Inputs(reads=reads, syndna_rype_path=index, work_ticket_idx=1),
            tmp_path / "ws",
        )
    )


def _stub_hits(monkeypatch, hits: list[int]):
    """Stub rype_classify: insert the flagged sequence_idx set directly."""
    from qiita_compute_orchestrator.jobs import syndna

    def fake(conn, index_path, sequence_table, dest_table, *, threshold):
        # Only reads visible in the query view may be flagged — mirrors the real
        # function, which classifies exactly that relation (here: every raw read).
        visible = {r[0] for r in conn.execute(f"SELECT read_id FROM {sequence_table}").fetchall()}
        for sidx in hits:
            assert sidx in visible, f"stub flagged {sidx}, not in the query view"
            conn.execute(f"INSERT INTO {dest_table} VALUES (?)", [sidx])

    monkeypatch.setattr(syndna, "_run_rype_classify", fake)


def test_syndna_marks_hits_and_passes_the_rest(tmp_path, monkeypatch):
    reads = _write_reads(tmp_path / "reads.parquet", [(1, "ACGT", None), (2, "TTTT", None)])
    _stub_hits(monkeypatch, [1])
    out = _run(tmp_path, reads, _index(tmp_path))
    assert set(out) == {"partial_mask"}
    assert _rows(out["partial_mask"]) == [(1, _SPIKEIN), (2, _PASS)]


def test_syndna_emits_a_zero_trim_partial_mask(tmp_path, monkeypatch):
    """Six columns, all trims zero (SynDNA does not trim); single-end leaves the
    mate trims NULL. This is the shape qc / lima_mask consume."""
    reads = _write_reads(tmp_path / "reads.parquet", [(1, "ACGT", None)])
    _stub_hits(monkeypatch, [1])
    out = _run(tmp_path, reads, _index(tmp_path))
    with duckdb.connect(":memory:") as conn:
        cols = [
            d[0]
            for d in conn.execute(
                f"SELECT * FROM read_parquet('{out['partial_mask']}') LIMIT 0"
            ).description
        ]
        row = conn.execute(f"SELECT * FROM read_parquet('{out['partial_mask']}')").fetchone()
    assert cols == [
        "sequence_idx",
        "reason",
        "left_trim1",
        "right_trim1",
        "left_trim2",
        "right_trim2",
    ]
    assert row == (1, _SPIKEIN, 0, 0, None, None)


def test_syndna_paired_end_mate_trims_are_zero_not_null(tmp_path, monkeypatch):
    """A PE read gets left_trim2/right_trim2 = 0 (not NULL), so the downstream
    both-mates count(right_trim2) treats it as a pair. (Spike-ins are single-end
    today, but the convention must hold for a future PE absquant.)"""
    reads = _write_reads(tmp_path / "reads.parquet", [(1, "ACGT", "TTTT")])
    _stub_hits(monkeypatch, [])
    out = _run(tmp_path, reads, _index(tmp_path))
    with duckdb.connect(":memory:") as conn:
        row = conn.execute(
            f"SELECT left_trim2, right_trim2 FROM read_parquet('{out['partial_mask']}')"
        ).fetchone()
    assert row == (0, 0)


def test_syndna_no_hits_marks_everything_pass(tmp_path, monkeypatch):
    reads = _write_reads(tmp_path / "reads.parquet", [(1, "ACGT", None), (2, "TTTT", None)])
    _stub_hits(monkeypatch, [])
    out = _run(tmp_path, reads, _index(tmp_path))
    assert _rows(out["partial_mask"]) == [(1, _PASS), (2, _PASS)]


def test_syndna_missing_index_raises(tmp_path, monkeypatch):
    reads = _write_reads(tmp_path / "reads.parquet", [(1, "ACGT", None)])
    _stub_hits(monkeypatch, [])
    with pytest.raises(FileNotFoundError, match="syndna_rype_path"):
        _run(tmp_path, reads, tmp_path / "nope.ryxdi")


def test_syndna_empty_index_raises(tmp_path, monkeypatch):
    """An empty .ryxdi would classify nothing and silently report zero spike-ins."""
    reads = _write_reads(tmp_path / "reads.parquet", [(1, "ACGT", None)])
    _stub_hits(monkeypatch, [])
    empty = tmp_path / "empty.ryxdi"
    empty.mkdir()
    with pytest.raises(ValueError, match="empty directory"):
        _run(tmp_path, reads, empty)
