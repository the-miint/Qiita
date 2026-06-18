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


# --- qiita_buckets_12: the "skip the bucket 1 & 2 ack when there's nothing to
# apply" predicate redeploy.sh uses. rc 0 = empty (skip prompt), 1 = has steps
# (prompt), 2 = unreadable/markers-absent (fail safe → prompt). -----------------

# Empty Pending-deploy buckets 1 & 2 — only headers + the "_None yet._"
# placeholder. The "### 3. Migrations" header bounds the range.
_EMPTY_BUCKETS = """\
## Pending deploy

### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast)

_None yet._

### 2. One-time host setup

_None yet._

### 3. Migrations

_None yet._
"""

# A real step in bucket 1 — the operator must apply it, so the ack must NOT skip.
_NONEMPTY_BUCKETS = """\
## Pending deploy

### 1. Env vars — set BEFORE the deploy

- (#123) sudo bash -c 'echo "FOO=bar" >> /etc/qiita/compute-orchestrator.env'

### 2. One-time host setup

_None yet._

### 3. Migrations

_None yet._
"""


def _call_buckets_12(checklist: Path) -> subprocess.CompletedProcess[str]:
    """Source _common.sh and invoke qiita_buckets_12 with a checklist path."""
    return subprocess.run(
        ["bash", "-c", f'source "{_COMMON}"; qiita_buckets_12 "$1"', "_", str(checklist)],
        capture_output=True,
        text=True,
    )


def test_buckets_12_empty_returns_zero(tmp_path: Path) -> None:
    """Placeholder-only buckets → rc 0 so redeploy.sh skips the prompt; the text
    is still echoed so the caller could print it."""
    f = tmp_path / "DEPLOY_CHECKLIST.md"
    f.write_text(_EMPTY_BUCKETS)
    result = _call_buckets_12(f)
    assert result.returncode == 0, result.stdout
    assert "_None yet._" in result.stdout
    # The bounding "### 3. Migrations" header is dropped, not part of buckets 1+2.
    assert "Migrations" not in result.stdout


def test_buckets_12_nonempty_returns_one(tmp_path: Path) -> None:
    """A real step present → rc 1 so the operator is prompted, and the step text
    is echoed for them to read."""
    f = tmp_path / "DEPLOY_CHECKLIST.md"
    f.write_text(_NONEMPTY_BUCKETS)
    result = _call_buckets_12(f)
    assert result.returncode == 1
    assert "FOO=bar" in result.stdout


def test_buckets_12_unreadable_returns_two(tmp_path: Path) -> None:
    """Missing/unreadable checklist → rc 2 so the caller falls back to prompting
    (fail safe) rather than silently skipping the ack."""
    result = _call_buckets_12(tmp_path / "does-not-exist.md")
    assert result.returncode == 2
    assert result.stdout == ""


def test_buckets_12_markers_absent_returns_two(tmp_path: Path) -> None:
    """Readable file but no bucket markers → can't judge → rc 2 (prompt)."""
    f = tmp_path / "DEPLOY_CHECKLIST.md"
    f.write_text("# Some unrelated file\n\nNo bucket headers here.\n")
    result = _call_buckets_12(f)
    assert result.returncode == 2


def test_buckets_12_pins_real_checklist_headers() -> None:
    """qiita_buckets_12's bucket-1/bucket-3 markers are the contract with the
    live DEPLOY_CHECKLIST.md. Run it against the REAL file (not a fixture copy):
    it must return 0 (empty) or 1 (has steps) — never 2, which would mean the
    headers it keys on no longer match the file and every deploy quietly fell
    back to prompting. A bucket rename in DEPLOY_CHECKLIST.md fails here."""
    result = _call_buckets_12(_REPO_ROOT / "DEPLOY_CHECKLIST.md")
    assert result.returncode in (0, 1), (
        "qiita_buckets_12 could not locate buckets 1 & 2 in the real "
        f"DEPLOY_CHECKLIST.md (rc={result.returncode}); the '### 1. Env vars' / "
        "'### 3. Migrations' markers it keys on have drifted from the file."
    )


# --- qiita_paths_touch_native: the pure path-prefix predicate behind redeploy.sh's
# "did this pull touch a package the native SLURM venv runs?" decision. rc 0 = a
# path is under qiita-common/ or qiita-compute-orchestrator/, 1 = none are. -------


def _call_paths_touch_native(paths: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-c", f'source "{_COMMON}"; qiita_paths_touch_native "$1"', "_", paths],
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize(
    "paths",
    [
        "qiita-common/src/qiita_common/config.py\nother/x",
        "qiita-compute-orchestrator/pyproject.toml",
        "docs/a.md\nqiita-common/f",  # native path is not the first line
    ],
)
def test_paths_touch_native_matches(paths: str) -> None:
    """Any path under the two native packages → rc 0 (refresh needed)."""
    assert _call_paths_touch_native(paths).returncode == 0


@pytest.mark.parametrize(
    "paths",
    [
        "docs/a.md\nworkflows/b.yaml\nqiita-data-plane/src/main.rs",
        "qiita-common-extra/z.py",  # sibling prefix must NOT match
        "",  # empty diff → nothing touched
    ],
)
def test_paths_touch_native_no_match(paths: str) -> None:
    """No path under the native packages (incl. a sibling-prefix dir or an empty
    list) → rc 1 (no refresh needed)."""
    assert _call_paths_touch_native(paths).returncode == 1
