"""Isolated unit tests for `assembly_run_config.execute` — now just emits the
assembly run-config (the masked reads are streamed to FASTQ by the CP runner)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
import yaml

from qiita_compute_orchestrator.jobs.assembly_run_config import Inputs, execute

_WORKFLOWS = Path(__file__).resolve().parents[2] / "workflows"


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


@pytest.mark.parametrize("assembler", ["hifiasm_meta", "myloasm"])
def test_run_config_bytes_are_the_resource_profile_keys(tmp_path, assembler):
    """`run_config.json`'s stripped bytes are the key of `assemble`'s `profiles:`
    lookup — the runner reads the whole file, strips it, and matches it against the
    keys in the workflow YAML (`runner/_dispatch.py`). No default: a key that does
    not match fails the step at dispatch.

    So the serialization is a contract with the YAML, not a private detail. This is
    the producer side of it: adding a field to run_config.json, reordering, or
    changing `json.dumps` separators breaks here rather than on a ticket. The
    consumer side — that the YAML names every assembler and no others — is
    `test_assemble_profiles_cover_every_assembler`.
    """
    out = _run(
        Inputs(assembler=assembler, prep_sample_idx=5, work_ticket_idx=9),
        tmp_path / "ws",
    )
    key = out["run_config"].read_text(encoding="utf-8").strip()

    checked = 0
    for yaml_path in sorted(_WORKFLOWS.glob("long-read-assembly/*.yaml")):
        data = yaml.safe_load(yaml_path.read_text())
        step = next((e for e in data["steps"] if e.get("step") == "assemble"), None)
        if step is None:
            continue
        profiles = step["baseline_resources"].get("profiles")
        if profiles is None:
            continue
        assert key in profiles, (
            f"{yaml_path.relative_to(_WORKFLOWS.parent)}: assembly_run_config writes "
            f"{key!r}, which is not one of the assemble profile keys {sorted(profiles)}"
        )
        checked += 1
    assert checked, "no long-read-assembly version uses the assemble profiles lookup"
