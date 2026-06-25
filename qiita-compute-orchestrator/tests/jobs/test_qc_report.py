"""Unit tests for `qc_report.execute` — the native fastqc-equivalent summary.

`qc_report` reads a reads.parquet and writes a `qc_report.json` with per-mate
summary stats + three distributions (mean-quality, GC-percent, length
histograms). It reports on whichever of `reads` (raw point) / `filtered_reads`
(post-filter point) is bound, naming its output binding to match.

The `write_reads_q` fixture (tests/jobs/conftest.py) owns the 6-col schema with
REAL `UTINYINT[]` quals, so we feed known sequences/quals and assert exact stats.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest


def _run(reads_path: Path, workspace: Path, *, filtered: bool = False) -> dict:
    from qiita_compute_orchestrator.jobs import qc_report

    field = "filtered_reads" if filtered else "reads"
    inputs = qc_report.Inputs(**{field: reads_path}, prep_sample_idx=5, work_ticket_idx=1)
    out = asyncio.run(qc_report.execute(inputs, workspace))
    binding = "filtered_qc_report" if filtered else "raw_qc_report"
    assert set(out) == {binding}
    return json.loads(out[binding].read_text())


def test_single_end_summary_and_histograms(tmp_path, write_reads_q):
    reads = write_reads_q(
        tmp_path / "reads.parquet",
        [
            (1, "a", "ACGT", [30, 30, 30, 30], None, None),
            (2, "b", "GGCC", [20, 20, 20, 20], None, None),
        ],
    )
    report = _run(reads, tmp_path / "ws")

    assert report["point"] == "raw"
    assert report["layout"] == "single"
    assert report["read_pairs"] == 2
    assert report["mates"]["r2"] is None
    r1 = report["mates"]["r1"]
    assert r1["reads"] == 2
    assert r1["total_bases"] == 8
    assert r1["gc_content"] == pytest.approx(0.75)  # (2 + 4) / 8
    assert r1["n_content"] == 0.0
    assert r1["mean_quality"] == pytest.approx(25.0)  # (30*4 + 20*4) / 8
    assert (r1["min_length"], r1["max_length"], r1["mean_length"]) == (4, 4, 4.0)
    # per-sequence mean-quality: read a -> 30, read b -> 20
    assert r1["quality_histogram"] == {"30": 1, "20": 1}
    # GC%: ACGT -> 50, GGCC -> 100
    assert r1["gc_histogram"] == {"50": 1, "100": 1}
    assert r1["length_histogram"] == {"4": 2}


def test_paired_end_reports_both_mates(tmp_path, write_reads_q):
    reads = write_reads_q(
        tmp_path / "reads.parquet",
        [
            (1, "a", "ACGT", [40, 40, 40, 40], "TTGG", [10, 10, 10, 10]),
            (2, "b", "AAAA", [40, 40, 40, 40], "CCCC", [10, 10, 10, 10]),
        ],
    )
    report = _run(reads, tmp_path / "ws")

    assert report["layout"] == "paired"
    assert report["read_pairs"] == 2
    r1, r2 = report["mates"]["r1"], report["mates"]["r2"]
    assert r1["reads"] == 2 and r2["reads"] == 2
    # r1: ACGT(GC2) + AAAA(GC0) = 2/8; r2: TTGG(GC2) + CCCC(GC4) = 6/8
    assert r1["gc_content"] == pytest.approx(0.25)
    assert r2["gc_content"] == pytest.approx(0.75)
    assert r1["mean_quality"] == pytest.approx(40.0)
    assert r2["mean_quality"] == pytest.approx(10.0)


def test_n_content(tmp_path, write_reads_q):
    reads = write_reads_q(
        tmp_path / "reads.parquet", [(1, "a", "ACGN", [30, 30, 30, 30], None, None)]
    )
    r1 = _run(reads, tmp_path / "ws")["mates"]["r1"]
    assert r1["n_content"] == pytest.approx(0.25)  # 1 N in 4 bases
    assert r1["gc_content"] == pytest.approx(0.5)  # C, G in 4 bases


def test_filtered_point_names_output_binding(tmp_path, write_reads_q):
    reads = write_reads_q(
        tmp_path / "filtered.parquet", [(1, "a", "ACGT", [30, 30, 30, 30], None, None)]
    )
    report = _run(reads, tmp_path / "ws", filtered=True)
    assert report["point"] == "filtered"


def test_empty_parquet_is_well_formed(tmp_path):
    import duckdb

    path = tmp_path / "empty.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "COPY (SELECT CAST(NULL AS BIGINT) AS sequence_idx, CAST(NULL AS VARCHAR) AS read_id,"
            " CAST(NULL AS VARCHAR) AS sequence1, CAST(NULL AS UTINYINT[]) AS qual1,"
            " CAST(NULL AS VARCHAR) AS sequence2, CAST(NULL AS UTINYINT[]) AS qual2 WHERE false)"
            f" TO '{path}' (FORMAT PARQUET)"
        )
    report = _run(path, tmp_path / "ws")
    assert report["read_pairs"] == 0
    assert report["layout"] == "single"
    assert report["mates"]["r1"] is None
    assert report["mates"]["r2"] is None


def test_exactly_one_input_required(tmp_path, write_reads_q):
    from qiita_compute_orchestrator.jobs import qc_report

    reads = write_reads_q(tmp_path / "r.parquet", [(1, "a", "ACGT", [30, 30, 30, 30], None, None)])
    # neither bound
    with pytest.raises(ValueError, match="exactly one"):
        asyncio.run(
            qc_report.execute(
                qc_report.Inputs(prep_sample_idx=5, work_ticket_idx=1), tmp_path / "ws"
            )
        )
    # both bound
    with pytest.raises(ValueError, match="exactly one"):
        asyncio.run(
            qc_report.execute(
                qc_report.Inputs(
                    reads=reads, filtered_reads=reads, prep_sample_idx=5, work_ticket_idx=1
                ),
                tmp_path / "ws",
            )
        )
