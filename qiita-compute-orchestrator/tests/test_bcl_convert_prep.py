"""Tests for the bcl_convert_prep native job module.

Three layers, none requiring the optional ``run_preflight`` dep at
collection time:

- Discovery: the shipped module is importable and passes the native-job
  contract scan. This is the regression guard for the import chain
  (bcl_convert_prep → sequence_range); an import-time error in either
  silently drops the module from the discovered set.
- ``Inputs`` contract: the typed input model accepts the framework-injected
  shape and rejects missing/ill-typed fields.
- ``execute`` fail-fast: with the optional ``run_preflight`` dep stubbed in,
  the up-front absolute/exists/is-dir guards raise ValueError before any
  CP round-trip.
"""

from __future__ import annotations

import sys
import types

import pytest
from pydantic import ValidationError

from qiita_compute_orchestrator.jobs import scan_native_jobs
from qiita_compute_orchestrator.jobs.bcl_convert_prep import Inputs, execute

_MODULE = "qiita_compute_orchestrator.jobs.bcl_convert_prep"


def test_module_passes_native_job_scan():
    """The shipped bcl_convert_prep module imports cleanly and satisfies
    the Inputs+execute contract — so it appears in the boot-time scan.

    Importing it pulls in qiita_compute_orchestrator.sequence_range; this
    is the regression guard against an import-time error in either."""
    assert _MODULE in scan_native_jobs()


def test_inputs_accepts_framework_injected_shape():
    """The four scalars the launcher supplies (action_context path +
    SEQUENCED_POOL scope scalars + work_ticket_idx) validate."""
    inputs = Inputs(
        bcl_input_dir="/data/runs/250101_M00001_0001_000000000-ABCDE",
        sequenced_pool_idx=3,
        sequencing_run_idx=7,
        work_ticket_idx=99,
    )
    assert inputs.bcl_input_dir.name == "250101_M00001_0001_000000000-ABCDE"
    assert inputs.sequenced_pool_idx == 3
    assert inputs.sequencing_run_idx == 7
    assert inputs.work_ticket_idx == 99


def test_inputs_rejects_missing_scope_scalar():
    """A missing framework-injected scalar (sequencing_run_idx here) is a
    contract violation, not a silent default."""
    with pytest.raises(ValidationError):
        Inputs(
            bcl_input_dir="/data/runs/x",
            sequenced_pool_idx=3,
            work_ticket_idx=99,
        )


def test_inputs_rejects_non_int_idx():
    with pytest.raises(ValidationError):
        Inputs(
            bcl_input_dir="/data/runs/x",
            sequenced_pool_idx="not-an-int",
            sequencing_run_idx=7,
            work_ticket_idx=99,
        )


@pytest.fixture
def _stub_run_preflight(monkeypatch):
    """Install a stub ``run_preflight.legacy.api.save_legacy_csv`` so
    execute()'s deferred import succeeds without the real dep, letting the
    up-front fail-fast guards run. The stub raises if actually called —
    the fail-fast tests must error out before reaching it."""
    api = types.ModuleType("run_preflight.legacy.api")

    def _save_legacy_csv(conn, path):  # pragma: no cover - must not be reached
        raise AssertionError("save_legacy_csv should not be called on a fail-fast path")

    api.save_legacy_csv = _save_legacy_csv
    legacy = types.ModuleType("run_preflight.legacy")
    legacy.api = api
    root = types.ModuleType("run_preflight")
    root.legacy = legacy
    monkeypatch.setitem(sys.modules, "run_preflight", root)
    monkeypatch.setitem(sys.modules, "run_preflight.legacy", legacy)
    monkeypatch.setitem(sys.modules, "run_preflight.legacy.api", api)


async def test_execute_rejects_relative_bcl_input_dir(_stub_run_preflight, tmp_path):
    """A non-absolute bcl_input_dir fails fast with a clear message
    before any CP fetch."""
    inputs = Inputs(
        bcl_input_dir="relative/run-folder",
        sequenced_pool_idx=3,
        sequencing_run_idx=7,
        work_ticket_idx=99,
    )
    with pytest.raises(ValueError, match="must be absolute"):
        await execute(inputs, tmp_path)


async def test_execute_rejects_missing_bcl_input_dir(_stub_run_preflight, tmp_path):
    """An absolute path that doesn't exist (or isn't a directory) fails
    fast before any CP fetch."""
    inputs = Inputs(
        bcl_input_dir=str(tmp_path / "does-not-exist"),
        sequenced_pool_idx=3,
        sequencing_run_idx=7,
        work_ticket_idx=99,
    )
    with pytest.raises(ValueError, match="not found or not a directory"):
        await execute(inputs, tmp_path)
