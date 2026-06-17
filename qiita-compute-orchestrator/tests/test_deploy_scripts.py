"""Smoke-check the deploy shell scripts (deploy/*.sh).

These run on the Linux deploy host, not in CI's Python env, so they have no
unit-test harness of their own. This pure-unit guard (under `make test`) catches
the cheap-but-real failures — syntax errors and shellcheck warnings — before they
ship to a host where a broken deploy script is expensive. Mirrors the `bash -n`
precedent in test_compute_readiness.py::test_probe_script_is_valid_bash and the
repo-root reach in test_sif_build_spec.py.

shellcheck is optional: when it isn't installed the shellcheck assertion skips
gracefully (same posture as the apptainer-optional workflow tests).
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEPLOY = _REPO_ROOT / "deploy"
_COMMON = _DEPLOY / "_common.sh"

# The scripts introduced/maintained for the issue-#72 deploy-ease work. Kept
# explicit (not a glob) so a new deploy script is a deliberate add here.
_SCRIPTS = ("preflight.sh", "verify.sh", "redeploy.sh")
# Sourced-only fragments (no shebang-as-entrypoint, not executable). _common.sh
# carries real logic (qiita_native_checkout_from_python etc.) the executable
# scripts rely on, so it gets the same bash -n + shellcheck gate — but NOT the
# executable-bit check below, since it's never run directly.
_SOURCED = ("_common.sh",)


@pytest.mark.parametrize("name", _SCRIPTS)
def test_deploy_script_exists_and_executable(name: str) -> None:
    path = _DEPLOY / name
    assert path.is_file(), f"{path} missing"
    assert path.stat().st_mode & 0o111, f"{path} is not executable"


@pytest.mark.parametrize("name", _SCRIPTS + _SOURCED)
def test_deploy_script_is_valid_bash(name: str) -> None:
    """`bash -n` parses the script without executing it — catches the unmatched
    quote / stray fi class of bug that broke deploys before."""
    path = _DEPLOY / name
    result = subprocess.run(
        ["bash", "-n", str(path)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"bash -n failed for {name}:\n{result.stderr}"


@pytest.mark.parametrize("name", _SCRIPTS + _SOURCED)
def test_deploy_script_passes_shellcheck(name: str) -> None:
    # Gate on warnings+ (`-S warning`), not the default info/style level, so the
    # check is deterministic across shellcheck versions — info checks like SC2015
    # ("A && B || C is not if-then-else") are enabled/disabled differently between
    # releases (CI's apt build flags some that a newer local build doesn't), which
    # would otherwise flake CI on a stylistic note. Mirrors the repo's
    # `cargo clippy -- -D warnings` posture: catch the substantive issues
    # (unquoted expansions, real logic bugs), not the version-unstable nits.
    if shutil.which("shellcheck") is None:
        pytest.skip("shellcheck not installed")
    path = _DEPLOY / name
    result = subprocess.run(
        ["shellcheck", "-S", "warning", str(path)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"shellcheck flagged {name}:\n{result.stdout}\n{result.stderr}"


def _call_native_checkout(native_python: str) -> subprocess.CompletedProcess[str]:
    """Source _common.sh and invoke qiita_native_checkout_from_python with one arg.
    Returns the CompletedProcess so a test can assert on returncode + stdout."""
    return subprocess.run(
        [
            "bash",
            "-c",
            f'source "{_COMMON}"; qiita_native_checkout_from_python "$1"',
            "_",
            native_python,
        ],
        capture_output=True,
        text=True,
    )


def _fake_native_checkout(tmp_path: Path) -> Path:
    """Build a `<repo>/qiita-compute-orchestrator/.venv/bin/python` layout under a
    git clone, matching what SLURM_NATIVE_PYTHON points at in production."""
    checkout = tmp_path / "qiita-compute-orchestrator"
    (checkout / ".venv" / "bin").mkdir(parents=True)
    (tmp_path / ".git").mkdir()  # the repo root the checkout sits under
    (checkout / "pyproject.toml").write_text("[project]\nname='qiita-compute-orchestrator'\n")
    py = checkout / ".venv" / "bin" / "python"
    py.write_text("#!/bin/sh\n")
    py.chmod(0o755)
    return py


def test_native_checkout_resolves_valid_layout(tmp_path: Path) -> None:
    py = _fake_native_checkout(tmp_path)
    result = _call_native_checkout(str(py))
    assert result.returncode == 0, result.stderr
    # Realpath both sides — macOS /tmp is a /private symlink, and the helper cd's.
    assert os.path.realpath(result.stdout) == os.path.realpath(str(py.parents[2]))


@pytest.mark.parametrize("arg", ["", "python"])
def test_native_checkout_skips_when_unset_or_path_based(arg: str) -> None:
    """Empty / bare `python` (local backend) is a SKIP signal (rc=1), not a FAIL —
    redeploy.sh degrades like the miint stage rather than aborting the deploy."""
    result = _call_native_checkout(arg)
    assert result.returncode == 1
    assert result.stdout == ""


def test_native_checkout_fails_on_wrong_basename(tmp_path: Path) -> None:
    """A python whose grandparent dir isn't qiita-compute-orchestrator is a hard
    FAIL (rc=2) so redeploy.sh refuses to `uv sync` a wrong path."""
    bad = tmp_path / "some-other-dir" / ".venv" / "bin"
    bad.mkdir(parents=True)
    (tmp_path / ".git").mkdir()
    py = bad / "python"
    py.write_text("#!/bin/sh\n")
    result = _call_native_checkout(str(py))
    assert result.returncode == 2
    assert "qiita-compute-orchestrator" in result.stderr


def test_native_checkout_fails_outside_git_clone(tmp_path: Path) -> None:
    """Right shape but no ../.git → not a checkout → hard FAIL (rc=2)."""
    checkout = tmp_path / "qiita-compute-orchestrator"
    (checkout / ".venv" / "bin").mkdir(parents=True)
    (checkout / "pyproject.toml").write_text("[project]\n")
    py = checkout / ".venv" / "bin" / "python"
    py.write_text("#!/bin/sh\n")
    result = _call_native_checkout(str(py))
    assert result.returncode == 2
    assert "git clone" in result.stderr
