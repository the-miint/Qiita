"""`scripts/check-review-loop.sh` — the PR-description check behind `review-loop-check`.

Each test builds a throwaway repository with one base commit and two PR commits, so
"is this sha one of the PR's own commits" is decided by real git ancestry.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "check-review-loop.sh"

_BLOCK = """## Summary

Something.

## Reviewer loop

- Reviewed at: {sha} · rounds: 2 · stopped: nothing new
- Fixed: 3
- Declined:
  - none
- Deferred:
  - none
- Not probed:
  - none

## Notes
"""


def _git(repo: Path, *args: str) -> str:
    out = subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    )
    return out.stdout.strip()


@pytest.fixture
def repo(tmp_path: Path) -> dict[str, str]:
    """A repo with `base` (one commit) and a PR branch two commits ahead of it."""
    _git(tmp_path, "init", "-q", "-b", "base")
    _git(tmp_path, "config", "user.email", "t@example.org")
    _git(tmp_path, "config", "user.name", "t")
    shas = {}
    for name in ("base", "first", "second"):
        if name == "first":
            _git(tmp_path, "checkout", "-q", "-b", "pr")
        (tmp_path / "f").write_text(name)
        _git(tmp_path, "add", "f")
        _git(tmp_path, "commit", "-q", "-m", name)
        shas[name] = _git(tmp_path, "rev-parse", "--short", "HEAD")
    shas["path"] = str(tmp_path)
    return shas


def _run(repo: dict[str, str], body: str, labels: str = "") -> subprocess.CompletedProcess:
    env = {
        **os.environ,
        "PR_BODY": body,
        "LABELS": labels,
        "HEAD_SHA": "pr",
        "BASE_SHA": "base",
    }
    return subprocess.run(
        ["bash", str(_SCRIPT)], cwd=repo["path"], env=env, capture_output=True, text=True
    )


def test_accepts_a_block_naming_a_pr_commit(repo):
    result = _run(repo, _BLOCK.format(sha=repo["first"]))
    assert result.returncode == 0, result.stderr
    assert repo["first"] in result.stdout


def test_accepts_crlf_line_endings_and_a_backticked_sha(repo):
    body = _BLOCK.format(sha=f"`{repo['second']}`").replace("\n", "\r\n")
    assert _run(repo, body).returncode == 0


def test_label_opts_out_without_a_block(repo):
    result = _run(repo, "", labels="ci-macos,no-agent-review")
    assert result.returncode == 0
    assert "no-agent-review" in result.stdout


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ("## Summary\n\nNo section here.\n", "no '## Reviewer loop' section"),
        (_BLOCK.format(sha="<sha>"), "does not name a commit sha"),
        (_BLOCK.format(sha="0123456789abcdef"), "is not a commit in this repository"),
    ],
    ids=["no-section", "template-placeholder", "unknown-sha"],
)
def test_refuses_a_missing_or_unfilled_block(repo, body, message):
    result = _run(repo, body)
    assert result.returncode == 1
    assert message in result.stderr


def test_refuses_a_base_branch_commit(repo):
    """The base tip is an ancestor of the PR head, so ancestry alone would accept it."""
    result = _run(repo, _BLOCK.format(sha=repo["base"]))
    assert result.returncode == 1
    assert "a commit of the base branch" in result.stderr


@pytest.mark.parametrize("heading", ["Fixed:", "Declined:", "Deferred:", "Not probed:"])
def test_refuses_a_block_missing_a_ledger_heading(repo, heading):
    body = _BLOCK.format(sha=repo["first"]).replace(f"- {heading}", "- Other:")
    result = _run(repo, body)
    assert result.returncode == 1
    assert f"no '- {heading}' line" in result.stderr


def test_a_later_section_does_not_supply_the_block(repo):
    """Only lines under the heading count; the same lines under another heading do not."""
    body = _BLOCK.format(sha=repo["first"]).replace("## Reviewer loop", "## Something else")
    assert _run(repo, body).returncode == 1
