"""Tests for the boot-time native-job discovery scan.

Two layers:

- `_validate_native_job_module(mod)`: pure function over a single
  module object. Each test below constructs a synthetic
  `types.ModuleType` and asserts the right errors come back.
- `scan_native_jobs()`: walks a package's __path__ via
  `pkgutil.walk_packages`. The happy-path test confirms the real
  fastq_to_parquet skeleton passes. The stray-file tests build a
  *synthetic* jobs-shaped package in `tmp_path` and point
  `scan_native_jobs` at it via the `package_path`/`prefix` kwargs.
  Synthetic-package style avoids writing into the real jobs/
  directory (which would orphan files on crash and collide under
  pytest-xdist).
"""

from __future__ import annotations

import types
import uuid

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


@pytest.fixture
def fake_jobs_pkg(monkeypatch, tmp_path):
    """Build a synthetic jobs-shaped package in tmp_path.

    Returns `(pkg_path, prefix, add_module)` where `pkg_path` is the
    directory list to pass as scan_native_jobs(package_path=...),
    `prefix` is the corresponding module-path prefix, and `add_module`
    is a helper to drop a stub .py into the synthetic package.

    Cleanup is automatic: monkeypatch undoes the syspath_prepend and
    tmp_path is removed by pytest. The package name carries a uuid so
    parallel pytest-xdist workers don't collide and a crashed test
    leaves nothing behind under the real jobs/ tree.
    """
    pkg_name = f"_test_jobs_{uuid.uuid4().hex[:8]}"
    pkg_dir = tmp_path / pkg_name
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("")
    monkeypatch.syspath_prepend(str(tmp_path))

    def add_module(name: str, content: str) -> str:
        """Write `<name>.py` with `content` and return the full module path."""
        (pkg_dir / f"{name}.py").write_text(content)
        return f"{pkg_name}.{name}"

    return [str(pkg_dir)], f"{pkg_name}.", add_module


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


def test_scan_native_jobs_rejects_stray_non_dunder_file(fake_jobs_pkg):
    """A non-dunder file in the jobs tree that doesn't export Inputs +
    execute must fail the scan. The error message must mention where
    shared helpers belong so the operator knows what to do."""
    pkg_path, prefix, add_module = fake_jobs_pkg
    add_module("_stray", "# not a valid native job module — no Inputs, no execute\n")

    with pytest.raises(RuntimeError) as ei:
        scan_native_jobs(package_path=pkg_path, prefix=prefix)
    msg = str(ei.value)
    assert "_stray" in msg
    assert "missing `Inputs`" in msg
    assert "missing `execute`" in msg
    # The error message must tell the operator where helpers go.
    assert "job_helpers" in msg


def test_scan_native_jobs_rejects_module_with_missing_execute(fake_jobs_pkg):
    """A stub that DOES define Inputs but not execute. Catches the "I
    started writing a job and forgot to finish" failure mode where the
    import succeeds but the contract is half-met."""
    pkg_path, prefix, add_module = fake_jobs_pkg
    add_module(
        "half_done",
        "from pydantic import BaseModel\n"
        "class Inputs(BaseModel):\n"
        "    pass\n"
        "# execute() not yet written\n",
    )

    with pytest.raises(RuntimeError) as ei:
        scan_native_jobs(package_path=pkg_path, prefix=prefix)
    msg = str(ei.value)
    assert "half_done" in msg
    assert "missing `execute`" in msg
    assert "missing `Inputs`" not in msg  # Inputs is present


def test_scan_native_jobs_succeeds_on_synthetic_well_formed_module(fake_jobs_pkg):
    """Positive control for the synthetic-package path: a well-formed
    stub passes the scan via the same kwargs the negative tests use."""
    pkg_path, prefix, add_module = fake_jobs_pkg
    full = add_module(
        "ok",
        "from pathlib import Path\n"
        "from pydantic import BaseModel\n"
        "class Inputs(BaseModel):\n"
        "    x: int\n"
        "async def execute(inputs, workspace):\n"
        "    return {}\n",
    )
    validated = scan_native_jobs(package_path=pkg_path, prefix=prefix)
    assert full in validated
