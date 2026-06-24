"""Unit tests for `qiita_compute_orchestrator.jobs.run_native_job` —
the framework dispatcher for native steps. Both LocalBackend and the
shared SLURM launcher route through this function; the tests below
exercise each branch of its error classification.

Test stubs are injected into `sys.modules` under the
`qiita_compute_orchestrator.jobs.<name>` prefix so the dispatcher's
`importlib.import_module` finds them. monkeypatch auto-removes them
when the test exits.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest
from pydantic import BaseModel
from qiita_common.backend_failure import BackendFailure, FailureKind, StepNoData
from qiita_common.models import WorkTicketFailureStage

from qiita_compute_orchestrator.jobs import run_native_job


def _install_stub(
    monkeypatch,
    *,
    short_name: str,
    inputs_cls: type | None,
    execute_fn,
) -> str:
    """Inject a fake job module under qiita_compute_orchestrator.jobs.<short_name>
    and return its full module path. Used by every test below to
    exercise a specific dispatcher branch without touching real job
    files on disk."""
    full = f"qiita_compute_orchestrator.jobs.{short_name}"
    mod = types.ModuleType(full)
    if inputs_cls is not None:
        mod.Inputs = inputs_cls
    if execute_fn is not None:
        mod.execute = execute_fn
    monkeypatch.setitem(sys.modules, full, mod)
    return full


class _Inputs(BaseModel):
    x: int


async def test_happy_path_returns_outputs(monkeypatch, tmp_path):
    received: list[tuple] = []

    async def execute(inputs, workspace):
        received.append((inputs, workspace))
        return {"out": workspace / "out.txt"}

    name = _install_stub(monkeypatch, short_name="happy", inputs_cls=_Inputs, execute_fn=execute)

    result = await run_native_job(name, {"x": 42}, tmp_path, step_name="step")
    assert result == {"out": tmp_path / "out.txt"}
    assert received[0][0].x == 42
    assert received[0][1] == tmp_path


async def test_bad_prefix_raises_contract_violation():
    with pytest.raises(BackendFailure) as ei:
        await run_native_job("os.system", {}, Path("/"), step_name="step")
    assert ei.value.kind is FailureKind.CONTRACT_VIOLATION
    assert ei.value.stage is WorkTicketFailureStage.STEP_RUN
    assert "qiita_compute_orchestrator.jobs." in ei.value.reason


async def test_unimportable_module_raises_contract_violation():
    """Module name has the right prefix but no real module by that path
    exists — the dispatcher must surface this as a typed failure, not
    let ImportError propagate raw."""
    with pytest.raises(BackendFailure) as ei:
        await run_native_job(
            "qiita_compute_orchestrator.jobs.definitely_not_a_real_job",
            {},
            Path("/"),
            step_name="step",
        )
    assert ei.value.kind is FailureKind.CONTRACT_VIOLATION
    assert "failed to import" in ei.value.reason


async def test_non_importerror_at_import_time_maps_to_contract_violation(monkeypatch):
    """A job module that raises something other than ImportError at
    import time (SyntaxError, NameError, Pydantic model-construction
    failures, etc.) must still surface as CONTRACT_VIOLATION rather
    than leaking a raw traceback past the runner's classifier."""
    import importlib as _importlib

    def explode(name):
        raise SyntaxError("simulated module-body parse error")

    monkeypatch.setattr(_importlib, "import_module", explode)

    with pytest.raises(BackendFailure) as ei:
        await run_native_job(
            "qiita_compute_orchestrator.jobs.broken",
            {},
            Path("/"),
            step_name="step",
        )
    assert ei.value.kind is FailureKind.CONTRACT_VIOLATION
    assert "SyntaxError" in ei.value.reason
    assert "simulated module-body parse error" in ei.value.reason


async def test_missing_execute_raises_contract_violation(monkeypatch):
    """Dispatcher reports specifically WHICH export is missing (not a
    generic 'must export Inputs and execute'). Matches the boot scan's
    granularity — both layers go through _validate_native_job_module."""
    name = _install_stub(monkeypatch, short_name="no_execute", inputs_cls=_Inputs, execute_fn=None)
    with pytest.raises(BackendFailure) as ei:
        await run_native_job(name, {"x": 1}, Path("/"), step_name="step")
    assert ei.value.kind is FailureKind.CONTRACT_VIOLATION
    assert "missing `execute`" in ei.value.reason
    # Should NOT mention the export that's present.
    assert "missing `Inputs`" not in ei.value.reason


async def test_missing_inputs_raises_contract_violation(monkeypatch):
    async def execute(inputs, workspace):
        return {}

    name = _install_stub(monkeypatch, short_name="no_inputs", inputs_cls=None, execute_fn=execute)
    with pytest.raises(BackendFailure) as ei:
        await run_native_job(name, {}, Path("/"), step_name="step")
    assert ei.value.kind is FailureKind.CONTRACT_VIOLATION
    assert "missing `Inputs`" in ei.value.reason
    assert "missing `execute`" not in ei.value.reason


async def test_inputs_not_basemodel_raises_contract_violation(monkeypatch):
    """A job that exports `Inputs` as a non-BaseModel (e.g. plain dict
    or wrong base class) is a contract violation — the dispatcher's
    runtime check catches it even if the job module imports cleanly."""

    async def execute(inputs, workspace):
        return {}

    class NotABaseModel:
        pass

    name = _install_stub(
        monkeypatch,
        short_name="bad_inputs_class",
        inputs_cls=NotABaseModel,
        execute_fn=execute,
    )
    with pytest.raises(BackendFailure) as ei:
        await run_native_job(name, {}, Path("/"), step_name="step")
    assert ei.value.kind is FailureKind.CONTRACT_VIOLATION
    assert "BaseModel" in ei.value.reason


async def test_input_validation_failure_raises_bad_input(monkeypatch):
    async def execute(inputs, workspace):
        return {}

    name = _install_stub(
        monkeypatch, short_name="validates", inputs_cls=_Inputs, execute_fn=execute
    )
    with pytest.raises(BackendFailure) as ei:
        await run_native_job(name, {"x": "not-an-int"}, Path("/"), step_name="step")
    assert ei.value.kind is FailureKind.BAD_INPUT
    assert "input validation" in ei.value.reason


async def test_not_implemented_maps_to_unknown_permanent(monkeypatch, tmp_path):
    """The skeleton path: a job whose execute() is still a placeholder
    surfaces as UNKNOWN_PERMANENT so the runner classifies it as
    non-retriable. Documents the chosen resolution for the plan's
    Open Implementation Detail #1."""

    async def execute(inputs, workspace):
        raise NotImplementedError("placeholder for real implementation")

    name = _install_stub(monkeypatch, short_name="skeleton", inputs_cls=_Inputs, execute_fn=execute)
    with pytest.raises(BackendFailure) as ei:
        await run_native_job(name, {"x": 1}, tmp_path, step_name="step")
    assert ei.value.kind is FailureKind.UNKNOWN_PERMANENT
    assert "not implemented" in ei.value.reason


async def test_filenotfound_in_execute_maps_to_bad_input(monkeypatch, tmp_path):
    async def execute(inputs, workspace):
        raise FileNotFoundError("/missing/input.fastq")

    name = _install_stub(monkeypatch, short_name="fnf", inputs_cls=_Inputs, execute_fn=execute)
    with pytest.raises(BackendFailure) as ei:
        await run_native_job(name, {"x": 1}, tmp_path, step_name="step")
    assert ei.value.kind is FailureKind.BAD_INPUT
    assert "/missing/input.fastq" in ei.value.reason


async def test_value_error_in_execute_maps_to_bad_input(monkeypatch, tmp_path):
    async def execute(inputs, workspace):
        raise ValueError("malformed FASTA: duplicate read_id")

    name = _install_stub(monkeypatch, short_name="bad_data", inputs_cls=_Inputs, execute_fn=execute)
    with pytest.raises(BackendFailure) as ei:
        await run_native_job(name, {"x": 1}, tmp_path, step_name="step")
    assert ei.value.kind is FailureKind.BAD_INPUT
    assert "duplicate read_id" in ei.value.reason


async def test_step_no_data_propagates_unchanged(monkeypatch, tmp_path):
    """StepNoData is a TERMINAL no-data outcome, not a failure — the
    dispatcher must re-raise it unchanged (NOT reclassify it into a
    BackendFailure(BAD_INPUT)) so the backend can round-trip it to the
    runner's NO_DATA transition. Its except arm sits above the generic
    `except ValueError`, even though StepNoData is not a ValueError, to
    keep the ordering intent explicit."""

    async def execute(inputs, workspace):
        raise StepNoData(step_name="fastq", reason="FASTQ file contains no records: x.fastq")

    name = _install_stub(
        monkeypatch, short_name="empty_well", inputs_cls=_Inputs, execute_fn=execute
    )
    with pytest.raises(StepNoData) as ei:
        await run_native_job(name, {"x": 1}, tmp_path, step_name="fastq")
    assert ei.value.step_name == "fastq"
    assert "contains no records" in ei.value.reason


async def test_step_name_carries_yaml_name_not_module_path(monkeypatch, tmp_path):
    """The BackendFailure raised inside run_native_job uses the YAML
    step name (the kwarg), NOT the module path. This matches the
    work_ticket.failure_step_name DB column's documented contract
    (the YAML step name, e.g. "fastq"). The module path stays in the
    reason text for debugging context."""

    async def execute(inputs, workspace):
        raise NotImplementedError("placeholder")

    name = _install_stub(
        monkeypatch, short_name="skeleton2", inputs_cls=_Inputs, execute_fn=execute
    )
    with pytest.raises(BackendFailure) as ei:
        await run_native_job(name, {"x": 1}, tmp_path, step_name="fastq")
    assert ei.value.step_name == "fastq"
    # Module path still in reason for the operator to find the offending file.
    assert "qiita_compute_orchestrator.jobs.skeleton2" in ei.value.reason


async def test_unknown_exception_propagates(monkeypatch, tmp_path):
    """Exceptions outside the classified set (FileNotFoundError,
    ValueError, NotImplementedError) propagate uncaught so the
    orchestrator's logs surface them with full traceback rather than
    silently re-labelling them."""

    class CustomError(Exception):
        pass

    async def execute(inputs, workspace):
        raise CustomError("something weird")

    name = _install_stub(monkeypatch, short_name="weird", inputs_cls=_Inputs, execute_fn=execute)
    with pytest.raises(CustomError):
        await run_native_job(name, {"x": 1}, tmp_path, step_name="step")
