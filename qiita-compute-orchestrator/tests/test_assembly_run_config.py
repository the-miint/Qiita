"""Isolated unit tests for `assembly_run_config.execute` — now just emits the
assembly run-config (the masked reads are streamed to FASTQ by the CP runner)."""

from __future__ import annotations

import asyncio
import json

import pytest

from qiita_compute_orchestrator.jobs.assembly_run_config import Inputs, execute


def _run(inputs: Inputs, workspace) -> dict:
    return asyncio.run(execute(inputs, workspace))


def test_writes_run_config(tmp_path):
    out = _run(
        Inputs(assembler="hifiasm_meta", prep_sample_idx=5, work_ticket_idx=9),
        tmp_path / "ws",
    )
    assert json.loads(out["run_config"].read_text()) == {"assembler": "hifiasm_meta"}


def test_assembler_defaults_to_hifiasm_meta(tmp_path):
    out = _run(Inputs(prep_sample_idx=1, work_ticket_idx=1), tmp_path / "ws")
    assert json.loads(out["run_config"].read_text()) == {"assembler": "hifiasm_meta"}


def test_unknown_assembler_rejected():
    with pytest.raises(ValueError):
        Inputs(assembler="spades", prep_sample_idx=1, work_ticket_idx=1)
