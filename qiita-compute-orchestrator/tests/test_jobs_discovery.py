"""Tests for the boot-time native-job discovery scan.

Two layers:

- `_validate_native_job_module(mod)`: pure function over a single
  module object. Each test below constructs a synthetic
  `types.ModuleType` and asserts the right errors come back.
- `scan_native_jobs()`: walks the real jobs/ directory via
  `pkgutil.walk_packages`. The happy-path test confirms the
  fastq_to_parquet skeleton passes. The stray-file test writes a
  temporary non-dunder file into the real jobs/ directory, runs the
  scan, asserts the error, and cleans up.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest
from pydantic import BaseModel

from qiita_compute_orchestrator.jobs import (
    _validate_native_job_module,
    scan_native_jobs,
)


class _Inputs(BaseModel):
    x: int


async def _good_execute(inputs, workspace):
    return {}


def _mod(**attrs) -> types.ModuleType:
    """Helper: build a throwaway module with the given attributes."""
    mod = types.ModuleType("test_synthetic")
    for k, v in attrs.items():
        setattr(mod, k, v)
    return mod


def test_validate_accepts_well_formed_module():
    mod = _mod(Inputs=_Inputs, execute=_good_execute)
    assert _validate_native_job_module(mod) == []


def test_validate_flags_missing_inputs():
    mod = _mod(execute=_good_execute)
    errors = _validate_native_job_module(mod)
    assert errors == ["missing `Inputs`"]


def test_validate_flags_missing_execute():
    mod = _mod(Inputs=_Inputs)
    errors = _validate_native_job_module(mod)
    assert errors == ["missing `execute`"]


def test_validate_flags_missing_both_simultaneously():
    """When both exports are missing, both errors fire so the operator
    sees the full picture in one shot."""
    mod = _mod()
    errors = _validate_native_job_module(mod)
    assert errors == ["missing `Inputs`", "missing `execute`"]


def test_validate_flags_inputs_not_basemodel():
    class NotABaseModel:
        pass

    mod = _mod(Inputs=NotABaseModel, execute=_good_execute)
    errors = _validate_native_job_module(mod)
    assert errors == ["`Inputs` must be a BaseModel subclass"]


def test_validate_flags_execute_not_async():
    """A sync execute() can't be awaited by the dispatcher; the boot
    scan must reject it before submission time."""

    def sync_execute(inputs, workspace):
        return {}

    mod = _mod(Inputs=_Inputs, execute=sync_execute)
    errors = _validate_native_job_module(mod)
    assert errors == ["`execute` must be an async function"]


def test_scan_native_jobs_succeeds_on_real_tree():
    """The shipped jobs/ tree validates cleanly: the fastq_to_parquet
    skeleton exports Inputs + execute and `__init__`/`__main__` are
    skipped."""
    validated = scan_native_jobs()
    assert "qiita_compute_orchestrator.jobs.fastq_to_parquet" in validated


def test_scan_native_jobs_rejects_stray_non_dunder_file(tmp_path):
    """A non-dunder file under jobs/ that doesn't export Inputs +
    execute must fail the scan. The error message must mention where
    shared helpers belong so the operator knows what to do."""
    import qiita_compute_orchestrator.jobs as jobs_pkg

    jobs_dir = Path(jobs_pkg.__file__).parent
    stray = jobs_dir / "_stray_for_test.py"
    stray.write_text("# not a valid native job module — no Inputs, no execute\n")
    try:
        with pytest.raises(RuntimeError) as ei:
            scan_native_jobs()
        msg = str(ei.value)
        assert "_stray_for_test" in msg
        assert "missing `Inputs`" in msg
        assert "missing `execute`" in msg
        # The error message must tell the operator where helpers go.
        assert "job_helpers" in msg
    finally:
        stray.unlink()
        # Importing the stub may have cached it; drop the cache so
        # subsequent tests don't see a stale module.
        sys.modules.pop("qiita_compute_orchestrator.jobs._stray_for_test", None)


def test_scan_native_jobs_rejects_module_with_missing_execute(tmp_path):
    """A stray .py that DOES define Inputs but not execute. Catches
    the "I started writing a job and forgot to finish" failure mode
    where the import succeeds but the contract is half-met."""
    import qiita_compute_orchestrator.jobs as jobs_pkg

    jobs_dir = Path(jobs_pkg.__file__).parent
    stray = jobs_dir / "_half_done_for_test.py"
    stray.write_text(
        "from pydantic import BaseModel\n"
        "class Inputs(BaseModel):\n"
        "    pass\n"
        "# execute() not yet written\n"
    )
    try:
        with pytest.raises(RuntimeError) as ei:
            scan_native_jobs()
        msg = str(ei.value)
        assert "_half_done_for_test" in msg
        assert "missing `execute`" in msg
        assert "missing `Inputs`" not in msg  # Inputs is present
    finally:
        stray.unlink()
        sys.modules.pop("qiita_compute_orchestrator.jobs._half_done_for_test", None)
