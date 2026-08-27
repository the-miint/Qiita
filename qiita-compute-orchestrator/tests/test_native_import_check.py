"""The venv-freshness check both deploy call sites run.

Pins the two stale-`qiita_common` shapes that reached production — a missing
SUBMODULE and a missing NAME — because neither is visible to the package-root
probe that used to guard the sync, and the second is not visible at any
granularity on the qiita_common side. Why that is, and why the check is anchored
on the job modules instead, is on `native_import_check` itself.

Each case simulates the stale venv by damaging an already-imported module in a
subprocess, then runs the real check against it. A subprocess per case because the
damage is global to an interpreter; the REAL jobs closure rather than a synthetic
tree because what is being pinned is that a job's own imports are what fail.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

# What redeploy.sh ran before this check existed, and what compute-readiness ran
# until it was pointed here. Kept verbatim as the CONTROL: each test below asserts
# it passes on the damage the new check fails on.
_OLD_PROBE = (
    "import qiita_common, qiita_compute_orchestrator.config, qiita_compute_orchestrator.jobs"
)

_NEW_CHECK = (
    "from qiita_compute_orchestrator.native_import_check import main; raise SystemExit(main())"
)


def _run(damage: str, body: str) -> subprocess.CompletedProcess[str]:
    """Apply `damage` to a freshly-imported module tree, then run `body`."""
    return subprocess.run(
        [sys.executable, "-c", f"{damage}\n{body}"], capture_output=True, text=True
    )


def _run_as_module(damage: str) -> subprocess.CompletedProcess[str]:
    """Run the check the way both deploy call sites do — `-P -m`, through
    `runpy` and the `__main__` guard — with `damage` applied first.

    Separate from `_run` because the guard is not covered by importing `main`:
    delete it and `python -m` runs the module body, defines `main`, and exits 0.
    Every `main()`-based test still passes while both deploy checks go green on a
    stale venv, which is the one failure mode this module exists to remove.
    """
    return subprocess.run(
        [
            sys.executable,
            "-P",
            "-c",
            f"{damage}\n"
            "import runpy, sys\n"
            "sys.argv = ['native_import_check']\n"
            "runpy.run_module('qiita_compute_orchestrator.native_import_check',"
            " run_name='__main__')",
        ],
        capture_output=True,
        text=True,
    )


# `qiita_common.api_paths` imports fine; only the NAME the jobs need is absent —
# the 2026-08-27 deploy, where `URL_ASSEMBLY_DOGET` was new in the wave. The two
# symbols below are incident fixtures, not a contract: any symbol a job module
# imports at top level exercises the same path, so a rename here is a fixture
# update, not a regression.
_MISSING_NAME = "import qiita_common.api_paths as ap\ndel ap.URL_ASSEMBLY_DOGET\n"

# `qiita_common` imports fine; a SUBMODULE is absent — the 2026-08-21 deploy,
# where `assembly_constants` was new in the wave. Blocking the import is how a
# subprocess reproduces "this file is not in site-packages".
_MISSING_SUBMODULE = "import sys\nsys.modules['qiita_common.assembly_constants'] = None\n"


@pytest.mark.parametrize(
    ("damage", "wanted"),
    [
        pytest.param(_MISSING_NAME, "URL_ASSEMBLY_DOGET", id="missing-name"),
        pytest.param(_MISSING_SUBMODULE, "assembly_constants", id="missing-submodule"),
    ],
)
def test_a_stale_qiita_common_fails_the_check(damage: str, wanted: str) -> None:
    """Both observed shapes fail, and the message names what is missing — that
    name is the operator's whole diagnosis, since the remedy (`uv sync
    --reinstall-package qiita-common`) is the same either way.

    On STDOUT: the compute-node caller captures stdout into its `err=` field, so
    a diagnosis written to stderr would be discarded there."""
    result = _run(damage, _NEW_CHECK)
    assert result.returncode != 0, result.stdout
    assert wanted in result.stdout, result.stdout


@pytest.mark.parametrize(
    "damage",
    [
        pytest.param(_MISSING_NAME, id="missing-name"),
        pytest.param(_MISSING_SUBMODULE, id="missing-submodule"),
    ],
)
def test_the_old_package_root_probe_passed_on_both(damage: str) -> None:
    """The control the test above needs to mean anything: the probe that guarded
    the sync is green on the identical damage. Without this the new check could be
    failing for some unrelated reason and nothing would say so."""
    assert _run(damage, _OLD_PROBE).returncode == 0


def test_an_undamaged_venv_passes() -> None:
    """The other control — the check is not simply always red."""
    result = _run("", _NEW_CHECK)
    assert result.returncode == 0, result.stderr
    assert "job modules" in result.stdout


@pytest.mark.parametrize(
    ("damage", "wanted"),
    [
        pytest.param(_MISSING_NAME, "URL_ASSEMBLY_DOGET", id="missing-name"),
        pytest.param(_MISSING_SUBMODULE, "assembly_constants", id="missing-submodule"),
    ],
)
def test_the_shipped_module_entry_path_fails_too(damage: str, wanted: str) -> None:
    """The tests above call `main()`; the deploy calls `-m`. Those are different
    code paths, and only this one ships."""
    result = _run_as_module(damage)
    assert result.returncode != 0, result.stdout
    assert wanted in result.stdout, result.stdout


def test_the_shipped_module_entry_path_passes_undamaged() -> None:
    """Control for the pair above."""
    result = _run_as_module("")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "job modules" in result.stdout


def test_the_failure_line_is_one_line_on_stdout() -> None:
    """The compute-node caller captures stdout into `err=` and `_parse_probe_log`
    is one-check-per-line, so a multi-line failure would truncate the operator's
    only diagnosis at the first newline. `scan_native_jobs` raises a multi-line
    message, so the collapse is doing real work — this is what pins it."""
    result = _run_as_module(_MISSING_NAME)
    assert result.returncode != 0
    assert len(result.stdout.strip().splitlines()) == 1, result.stdout


def test_both_deploy_call_sites_invoke_the_same_module() -> None:
    """redeploy.sh (head node) and the compute-readiness probe job (compute node)
    ask one venv's two filesystem views the same question. Separate probes would
    let the weaker one answer whichever question it happened to cover — which is
    how compute-readiness came to import only `jobs` while redeploy.sh imported
    three things.

    `-P` is part of what is pinned: without it a probe launched from inside a
    source tree shadows the installed package it is checking. The compute side is
    asserted on the GENERATED script rather than the source, since the module name
    reaches it through a shell variable.
    """
    from qiita_compute_orchestrator.cli.compute_readiness import build_probe_script

    module = "qiita_compute_orchestrator.native_import_check"
    redeploy = (Path(__file__).resolve().parents[2] / "deploy" / "redeploy.sh").read_text()
    assert f"-P -m {module}" in redeploy

    probe = build_probe_script(path_scratch="/scratch")
    assert module in probe
    assert '-P -m "$NATIVE_IMPORT_MOD"' in probe


def test_the_compute_probe_reports_the_reason_not_a_bare_fail() -> None:
    """A bare `native-import=fail` is what the operator had before, and it names
    nothing: `_parse_probe_log` keeps only `compute-readiness:`-prefixed lines and
    the log file is then unlinked, so an unprefixed message is gone. The probe
    must therefore fold the reason onto the prefixed line, as the miint probes do."""
    from qiita_compute_orchestrator.cli.compute_readiness import (
        _parse_probe_log,
        build_probe_script,
    )

    assert "err=$NATIVE_IMPORT_ERR" in build_probe_script(path_scratch="/scratch")
    (result,) = _parse_probe_log(
        "compute-readiness: native-import=fail err=RuntimeError: jobs.qc: ImportError\n"
    )
    assert result.status == "fail"
    assert "jobs.qc" in result.detail


def _run_compute_native_import_block(module: str) -> str:
    """Run the generated probe's native-import block against `module`.

    The block is taken from `build_probe_script` rather than retyped, so what runs
    here is what ships to the compute node.
    """
    from qiita_compute_orchestrator.cli.compute_readiness import build_probe_script

    script = build_probe_script(path_scratch="/scratch")
    start = script.index("NATIVE_IMPORT_MOD=")
    block = script[start : script.index("\nfi\n", start) + 4]
    block = block.replace(
        "NATIVE_IMPORT_MOD=qiita_compute_orchestrator.native_import_check",
        f"NATIVE_IMPORT_MOD={module}",
    )
    return subprocess.run(
        ["bash", "-c", f"PYTHON={sys.executable}\n{block}"],
        capture_output=True,
        text=True,
    ).stdout.strip()


def test_the_compute_probe_names_the_reason_when_the_module_is_absent() -> None:
    """A venv predating this module is the first-deploy case the deploy checklist
    tells the operator to expect, and there the failure lands on stderr with stdout
    empty. Capturing stdout alone reports `err=` with nothing after it — the bare
    `=fail` this probe was changed to stop emitting."""
    line = _run_compute_native_import_block("qiita_compute_orchestrator.does_not_exist")
    assert line.endswith("=fail") is False, line
    assert "native-import=fail err=" in line
    assert "does_not_exist" in line


def test_the_compute_probe_reports_ok_on_a_healthy_venv() -> None:
    """The control: the block is not simply always red — which it would be if the
    `if` read a pipeline's status instead of the interpreter's."""
    line = _run_compute_native_import_block("qiita_compute_orchestrator.native_import_check")
    assert line.endswith("native-import=ok"), line
