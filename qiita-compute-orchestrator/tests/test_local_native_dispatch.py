"""Tests for LocalBackend's `module:` (native-step) dispatch path.

LocalBackend.run_step has two branches:

- Container form (name-dispatch on "hash" / "load"): exercised by
  test_hash_job.py and test_load_job.py, which run the real
  DuckDB+miint helpers (and require the miint extension to be
  installable on the host).
- Native form (`module` kwarg set): delegates to
  `qiita_compute_orchestrator.jobs.run_native_job`. This file covers
  that branch with a stub job injected into sys.modules — the same
  pattern test_run_native_job.py uses.

The shared dispatcher (run_native_job) already has full unit-test
coverage in test_run_native_job.py. The point of these tests is the
*wiring*: LocalBackend forwards `module`, `inputs`, `reference_idx`,
`work_ticket_idx`, and `workspace` in the shape the dispatcher
expects, and propagates failures without re-wrapping them.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest
from pydantic import BaseModel
from qiita_common.backend_failure import BackendFailure, FailureKind

from qiita_compute_orchestrator.backends.local import LocalBackend


class _Inputs(BaseModel):
    fastq_path: Path
    reference_idx: int
    work_ticket_idx: int


def _install_stub(monkeypatch, *, short_name: str, execute_fn) -> str:
    full = f"qiita_compute_orchestrator.jobs.{short_name}"
    mod = types.ModuleType(full)
    mod.Inputs = _Inputs
    mod.execute = execute_fn
    monkeypatch.setitem(sys.modules, full, mod)
    return full


async def test_dispatches_to_native_module(monkeypatch, tmp_path):
    """The module's execute() is invoked with validated Inputs and the
    workspace; its return value flows back as LocalBackend's output."""
    captured: list[tuple] = []

    async def execute(inputs, workspace):
        captured.append((inputs, workspace))
        out = workspace / "result.parquet"
        out.write_bytes(b"FAKE")
        return {"result": out}

    full = _install_stub(monkeypatch, short_name="local_dispatch", execute_fn=execute)
    backend = LocalBackend()

    outputs = await backend.run_step(
        "fastq",
        {"fastq_path": tmp_path / "in.fa"},
        tmp_path,
        reference_idx=7,
        work_ticket_idx=99,
        module=full,
    )

    assert outputs == {"result": tmp_path / "result.parquet"}
    assert len(captured) == 1
    inputs, workspace = captured[0]
    assert inputs.fastq_path == tmp_path / "in.fa"
    assert inputs.reference_idx == 7
    assert inputs.work_ticket_idx == 99
    assert workspace == tmp_path


async def test_skeleton_not_implemented_maps_to_unknown_permanent(tmp_path):
    """The real fastq_to_parquet skeleton raises NotImplementedError;
    run_native_job translates it to BackendFailure(UNKNOWN_PERMANENT)
    and LocalBackend propagates without re-wrapping."""
    backend = LocalBackend()

    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "fastq",
            {"fastq_path": tmp_path / "in.fa"},
            tmp_path,
            reference_idx=1,
            work_ticket_idx=1,
            module="qiita_compute_orchestrator.jobs.fastq_to_parquet",
        )
    assert ei.value.kind is FailureKind.UNKNOWN_PERMANENT
    assert "fastq_to_parquet" in ei.value.reason


async def test_bad_prefix_maps_to_contract_violation(tmp_path):
    """A module path outside NATIVE_MODULE_PREFIX is rejected by
    run_native_job; LocalBackend propagates the typed failure
    (CONTRACT_VIOLATION) so the runner classifies it correctly."""
    backend = LocalBackend()

    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "x",
            {},
            tmp_path,
            reference_idx=1,
            work_ticket_idx=1,
            module="os.system",
        )
    assert ei.value.kind is FailureKind.CONTRACT_VIOLATION
    assert "os.system" in ei.value.reason


async def test_dispatcher_failures_propagate_through_local_backend(monkeypatch, tmp_path):
    """A BackendFailure raised inside run_native_job flows through
    LocalBackend unchanged — LocalBackend doesn't intercept or
    re-classify (so retry classification on the runner side stays
    consistent with the SLURM-launcher path)."""

    async def execute(inputs, workspace):
        raise FileNotFoundError("/missing/in.parquet")

    full = _install_stub(monkeypatch, short_name="fnf_passthrough", execute_fn=execute)
    backend = LocalBackend()

    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "x",
            {"fastq_path": tmp_path / "in.fa"},
            tmp_path,
            reference_idx=1,
            work_ticket_idx=1,
            module=full,
        )
    # run_native_job maps FileNotFoundError → BAD_INPUT; LocalBackend
    # must not catch it and rewrap as UNKNOWN_PERMANENT (which its
    # generic except-block would do for container steps).
    assert ei.value.kind is FailureKind.BAD_INPUT
    assert "/missing/in.parquet" in ei.value.reason
