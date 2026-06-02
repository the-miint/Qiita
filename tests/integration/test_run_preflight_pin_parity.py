"""Cross-component check: the two pyproject.toml files must pin
identical SHAs of run-preflight so a coordinated re-vendor is enforced
by the build, not by a comment that one side could forget to update."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CP_PYPROJECT = _REPO_ROOT / "qiita-control-plane" / "pyproject.toml"
_CO_PYPROJECT = _REPO_ROOT / "qiita-compute-orchestrator" / "pyproject.toml"
_GIT_SHA_RE = re.compile(r"git\+https://[^@]+@([0-9a-f]+)\b")


def _run_preflight_sha(pyproject_path: Path) -> str:
    """Return the run-preflight SHA pinned in `pyproject_path`'s
    [project] dependencies."""
    with pyproject_path.open("rb") as fh:
        data = tomllib.load(fh)

    # Locate the single dependency string that pins run-preflight
    matches = [
        dep
        for dep in data["project"]["dependencies"]
        if dep.startswith("run-preflight")
    ]
    assert len(matches) == 1, (
        f"{pyproject_path} should declare run-preflight exactly once,"
        f" got {len(matches)}: {matches}"
    )

    # Pull the SHA suffix from the git+url spec
    m = _GIT_SHA_RE.search(matches[0])
    assert m, f"{pyproject_path}: run-preflight pin {matches[0]!r} has no @SHA"
    return m.group(1)


def test_run_preflight_sha_pin_parity():
    """Tests the case where the control-plane and compute-orchestrator
    pyproject.toml files declare run-preflight at different SHAs —
    asserts they are identical so a coordinated bump is enforced."""
    cp_sha = _run_preflight_sha(_CP_PYPROJECT)
    co_sha = _run_preflight_sha(_CO_PYPROJECT)
    assert cp_sha == co_sha, (
        f"run-preflight SHA drift between components:\n"
        f"  qiita-control-plane:        {cp_sha}\n"
        f"  qiita-compute-orchestrator: {co_sha}\n"
        f"Update both pyproject.toml files in the same PR."
    )
