"""Isolated unit tests for `pacbio_export_reads.execute` — the reads.parquet →
FASTQ head of the pacbio-processing workflow.

Calls execute() directly (no LocalBackend) so failures point at the conversion,
not framework wiring. Covers: happy-path FASTQ encoding + run_config; empty input
→ StepNoData; a quality-less read → BAD_INPUT.
"""

from __future__ import annotations

import asyncio
import gzip
import json

import duckdb
import pytest
from qiita_common.backend_failure import BackendFailure, FailureKind, StepNoData

from qiita_compute_orchestrator.jobs.pacbio_export_reads import Inputs, execute


def _run(inputs: Inputs, workspace) -> dict:
    return asyncio.run(execute(inputs, workspace))


def _write_reads_parquet(path, rows: list[tuple]) -> None:
    """rows: (read_id, sequence1, qual1|None, sequence_idx). Writes a Parquet
    with the masked-reads schema subset the job reads."""
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE r (read_id VARCHAR, sequence1 VARCHAR, qual1 UTINYINT[], sequence_idx BIGINT)"
    )
    if rows:
        con.executemany("INSERT INTO r VALUES (?, ?, ?, ?)", rows)
    con.execute(f"COPY r TO '{path}' (FORMAT PARQUET)")
    con.close()


def test_happy_path_writes_fastq_and_run_config(tmp_path):
    reads = tmp_path / "reads.parquet"
    _write_reads_parquet(
        reads,
        [
            ("read1", "ACGT", [30, 31, 32, 33], 1),
            ("read2", "TT", [40, 2], 2),
        ],
    )
    ws = tmp_path / "ws"
    out = _run(
        Inputs(masked_reads=reads, assembler="hifiasm_meta", prep_sample_idx=5, work_ticket_idx=9),
        ws,
    )

    # PHRED offset 33: 30->'?', 31->'@', 32->'A', 33->'B'; 40->'I', 2->'#'.
    with gzip.open(out["reads_fastq"], "rt") as fh:
        records = {}
        lines = fh.read().splitlines()
    for i in range(0, len(lines), 4):
        records[lines[i]] = (lines[i + 1], lines[i + 3])
    assert records["@read1"] == ("ACGT", "?@AB")
    assert records["@read2"] == ("TT", "I#")

    assert json.loads(out["run_config"].read_text()) == {"assembler": "hifiasm_meta"}


def test_empty_reads_is_no_data(tmp_path):
    reads = tmp_path / "reads.parquet"
    _write_reads_parquet(reads, [])
    with pytest.raises(StepNoData):
        _run(Inputs(masked_reads=reads, prep_sample_idx=1, work_ticket_idx=1), tmp_path / "ws")


def test_missing_quality_is_bad_input(tmp_path):
    reads = tmp_path / "reads.parquet"
    _write_reads_parquet(reads, [("noqual", "ACGT", None, 1)])
    with pytest.raises(BackendFailure) as ei:
        _run(Inputs(masked_reads=reads, prep_sample_idx=1, work_ticket_idx=1), tmp_path / "ws")
    assert ei.value.kind is FailureKind.BAD_INPUT


def test_unknown_assembler_rejected():
    with pytest.raises(ValueError):
        Inputs(masked_reads="x", assembler="spades", prep_sample_idx=1, work_ticket_idx=1)
