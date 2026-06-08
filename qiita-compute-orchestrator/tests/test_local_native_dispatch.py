"""Tests for LocalBackend's `module:` (native-step) dispatch path.

LocalBackend.submit_step (which runs the module in-process at submit time)
has two branches:

- Container form: rejected — container execution lives on SlurmBackend.
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
from qiita_common.testing.native_steps import FASTQ_TO_PARQUET_MODULE

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
    workspace; its return value flows back as the terminal handle's outputs
    (LocalBackend runs the module in-process at submit time)."""
    captured: list[tuple] = []

    async def execute(inputs, workspace):
        captured.append((inputs, workspace))
        out = workspace / "result.parquet"
        out.write_bytes(b"FAKE")
        return {"result": out}

    full = _install_stub(monkeypatch, short_name="local_dispatch", execute_fn=execute)
    backend = LocalBackend()

    handle = await backend.submit_step(
        "fastq",
        {"fastq_path": tmp_path / "in.fa"},
        tmp_path,
        scope_target={"kind": "reference", "reference_idx": 7},
        work_ticket_idx=99,
        module=full,
    )

    assert handle.terminal_outputs == {"result": tmp_path / "result.parquet"}
    assert len(captured) == 1
    inputs, workspace = captured[0]
    assert inputs.fastq_path == tmp_path / "in.fa"
    assert inputs.reference_idx == 7
    assert inputs.work_ticket_idx == 99
    assert workspace == tmp_path


async def test_submit_status_result_runs_synchronously(monkeypatch, tmp_path):
    """LocalBackend implements the decoupled interface as a synchronous
    backend: submit_step runs the native job to completion and returns a
    terminal handle (compute_target=local, no SLURM job id, outputs in
    hand); status_step is immediately COMPLETED; result_step returns the
    captured outputs. This is what lets the runner treat local and SLURM
    uniformly without lying about a non-existent job id."""
    from qiita_common.models import ComputeTarget, StepStatus

    async def execute(inputs, workspace):
        out = workspace / "result.parquet"
        out.write_bytes(b"FAKE")
        return {"result": out}

    full = _install_stub(monkeypatch, short_name="sync_iface", execute_fn=execute)
    backend = LocalBackend()

    handle = await backend.submit_step(
        "fastq",
        {"fastq_path": tmp_path / "in.fa"},
        tmp_path,
        scope_target={"kind": "reference", "reference_idx": 7},
        work_ticket_idx=99,
        module=full,
    )
    assert handle.compute_target == ComputeTarget.LOCAL
    assert handle.slurm_job_id is None
    assert handle.job_name is None
    assert handle.terminal_outputs == {"result": tmp_path / "result.parquet"}

    info = await backend.status_step(handle)
    assert info.status == StepStatus.COMPLETED

    outputs = await backend.result_step(handle, info)
    assert outputs == {"result": tmp_path / "result.parquet"}


async def test_find_jobs_by_name_always_empty():
    """LocalBackend never submits to SLURM, so there is never an orphaned
    job to find — find_jobs_by_name is always empty (interface parity so the
    CP→CO route works against either backend without an isinstance check)."""
    backend = LocalBackend()
    assert await backend.find_jobs_by_name("qiita-wt1-fastq-a0") == []


async def test_result_step_raises_on_non_completed_status(tmp_path):
    """LocalBackend.result_step honors the ABC contract: a non-COMPLETED
    status is a caller bug and raises, rather than silently returning
    outputs for a step that didn't succeed."""
    from qiita_common.models import ComputeTarget, StepStatus

    from qiita_compute_orchestrator.backend import StepHandle, StepStatusInfo

    backend = LocalBackend()
    handle = StepHandle(
        compute_target=ComputeTarget.LOCAL,
        step_name="x",
        terminal_outputs={"a": tmp_path / "a"},
    )
    with pytest.raises(BackendFailure) as ei:
        await backend.result_step(handle, StepStatusInfo(status=StepStatus.FAILED))
    assert ei.value.kind is FailureKind.UNKNOWN_PERMANENT
    assert "non-COMPLETED" in ei.value.reason


async def test_submit_step_rejects_container(tmp_path):
    """The decoupled entry point keeps LocalBackend's container refusal —
    a container-shaped submit is a contract violation at submit_step, not
    a silent SLURM bypass."""
    backend = LocalBackend()
    with pytest.raises(BackendFailure) as ei:
        await backend.submit_step(
            "legacy",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=1,
            container="qiita/test:1.0.0",
        )
    assert ei.value.kind is FailureKind.CONTRACT_VIOLATION
    assert "container" in ei.value.reason.lower()


async def test_bad_prefix_maps_to_contract_violation(tmp_path):
    """A module path outside NATIVE_MODULE_PREFIX is rejected by
    run_native_job; LocalBackend propagates the typed failure
    (CONTRACT_VIOLATION) so the runner classifies it correctly."""
    backend = LocalBackend()

    with pytest.raises(BackendFailure) as ei:
        await backend.submit_step(
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
        await backend.submit_step(
            "x",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=1,
            container="qiita/test:1.0.0",
            module=FASTQ_TO_PARQUET_MODULE,
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
        await backend.submit_step(
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
        await backend.submit_step(
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
        await backend.submit_step(
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
