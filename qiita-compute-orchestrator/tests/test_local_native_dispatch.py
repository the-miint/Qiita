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
from qiita_compute_orchestrator.jobs import RESERVED_INPUT_KEYS


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
        scope_target={"kind": "reference", "reference_idx": 7},
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
    and LocalBackend propagates without re-wrapping. The failure
    carries the YAML step name (not the module path) so it lines up
    with the work_ticket.failure_step_name DB column."""
    backend = LocalBackend()

    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "fastq",  # YAML step name
            {"fastq_path": tmp_path / "in.fa"},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=1,
            module="qiita_compute_orchestrator.jobs.fastq_to_parquet",
        )
    assert ei.value.kind is FailureKind.UNKNOWN_PERMANENT
    assert ei.value.step_name == "fastq"
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
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=1,
            module="os.system",
        )
    assert ei.value.kind is FailureKind.CONTRACT_VIOLATION
    assert "os.system" in ei.value.reason


async def test_rejects_both_container_and_module(tmp_path):
    """Mirror of SlurmBackend's S3 guard: both runtime fields set is a
    contract violation. The wire validator catches this upstream; this
    guard protects direct callers so LocalBackend can't silently pick
    one runtime over the other."""
    backend = LocalBackend()

    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "x",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=1,
            container="qiita/test:1.0.0",
            module="qiita_compute_orchestrator.jobs.fastq_to_parquet",
        )
    assert ei.value.kind is FailureKind.CONTRACT_VIOLATION
    assert "exactly one" in ei.value.reason


async def test_rejects_neither_container_nor_module(tmp_path):
    """Mirror of SlurmBackend's S3 guard: neither runtime field is also
    a contract violation. With both runtime fields default-None, calls
    that forgot to declare a runtime fail fast with a typed failure
    rather than falling through to the legacy name-dispatch."""
    backend = LocalBackend()

    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=1,
        )
    assert ei.value.kind is FailureKind.CONTRACT_VIOLATION
    assert "exactly one" in ei.value.reason


@pytest.mark.parametrize("reserved_key", sorted(RESERVED_INPUT_KEYS))
async def test_rejects_inputs_overlap_with_framework_scalars(reserved_key, monkeypatch, tmp_path):
    """If the workflow's `inputs:` list collides with a framework
    scalar (any name in RESERVED_INPUT_KEYS), LocalBackend surfaces
    it as BackendFailure(CONTRACT_VIOLATION) — symmetric with the
    SLURM launcher's path through the same `flatten_native_inputs`
    helper. Parameterized so adding a fourth reserved name doesn't
    need a hardcoded-string test update."""

    async def execute(inputs, workspace):
        raise RuntimeError("should not reach execute()")

    full = _install_stub(monkeypatch, short_name="collision", execute_fn=execute)
    backend = LocalBackend()

    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "fastq",
            # The step's input map happens to declare a name that
            # shadows the framework scalar — the helper rejects.
            {reserved_key: tmp_path / "evil.fa"},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=1,
            module=full,
        )
    assert ei.value.kind is FailureKind.CONTRACT_VIOLATION
    assert reserved_key in ei.value.reason


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
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=1,
            module=full,
        )
    # run_native_job maps FileNotFoundError → BAD_INPUT; LocalBackend
    # must not catch it and rewrap as UNKNOWN_PERMANENT (which its
    # generic except-block would do for container steps).
    assert ei.value.kind is FailureKind.BAD_INPUT
    assert "/missing/in.parquet" in ei.value.reason
