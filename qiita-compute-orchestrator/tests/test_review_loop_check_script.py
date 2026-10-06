"""`scripts/check-review-loop.sh` — the PR-description check behind `review-loop-check`.

Each test builds a throwaway repository — a base commit, a PR branch two commits ahead
of it, and one commit on a third branch — so "is this sha one of the PR's own commits"
is decided by real git ancestry.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO_ROOT / "scripts" / "check-review-loop.sh"
_PR_TEMPLATE = _REPO_ROOT / ".github" / "pull_request_template.md"
_SKILL = _REPO_ROOT / ".claude" / "skills" / "qiita-review" / "SKILL.md"

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
        ["git", "-C", str(repo), "-c", "commit.gpgsign=false", *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return out.stdout.strip()


@pytest.fixture
def repo(tmp_path: Path) -> dict[str, str]:
    """A repo with `base` (one commit), a `pr` branch two commits ahead of it (checked
    out), and an `elsewhere` branch holding a commit that is on neither."""
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
    # A commit that exists in the repository but is on neither branch.
    _git(tmp_path, "checkout", "-q", "-b", "elsewhere", "base")
    (tmp_path / "f").write_text("elsewhere")
    _git(tmp_path, "commit", "-q", "-am", "elsewhere")
    shas["elsewhere"] = _git(tmp_path, "rev-parse", "--short", "HEAD")
    _git(tmp_path, "checkout", "-q", "pr")
    shas["path"] = str(tmp_path)
    return shas


def _run(
    repo: dict[str, str], body: str, labels: str = "", base: str = "base"
) -> subprocess.CompletedProcess:
    env = {
        **os.environ,
        "PR_BODY": body,
        "LABELS": labels,
        "HEAD_SHA": "pr",
        "BASE_SHA": base,
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


def test_refuses_a_commit_that_is_not_on_the_branch(repo):
    result = _run(repo, _BLOCK.format(sha=repo["elsewhere"]))
    assert result.returncode == 1
    assert "is not on this branch" in result.stderr


def test_an_empty_section_is_reported_as_unfilled_not_missing(repo):
    result = _run(repo, "## Summary\n\n## Reviewer loop\n\n\n## Notes\n")
    assert result.returncode == 1
    assert "does not name a commit sha" in result.stderr


def test_an_unresolvable_base_is_an_error_not_a_pass(repo):
    """With the base unresolved, a base-branch sha must not slip through."""
    result = _run(repo, _BLOCK.format(sha=repo["base"]), base="no-such-ref")
    assert result.returncode == 2
    assert "cannot resolve 'no-such-ref'" in result.stderr


def _skill_block() -> str:
    """The fenced example under the skill's PR-body heading."""
    text = _SKILL.read_text()
    start = text.index("```markdown\n", text.index("## Then write the PR-body block"))
    return text[start + len("```markdown\n") : text.index("```", start + 3)]


@pytest.mark.parametrize(
    "source", [_PR_TEMPLATE.read_text, _skill_block], ids=["pr-template", "skill-example"]
)
def test_the_published_block_shapes_pass_once_the_sha_is_filled_in(repo, source):
    """The template and the skill each carry a copy of the block; both must be what the
    script accepts, with only the sha placeholder replaced."""
    text = source()
    line = next(ln for ln in text.splitlines() if ln.startswith("- Reviewed at:"))
    filled = text.replace(
        line, f"- Reviewed at: {repo['second']} · rounds: 1 · stopped: nothing new"
    )
    result = _run(repo, filled)
    assert result.returncode == 0, result.stderr


def test_a_long_description_does_not_hide_the_section(repo):
    """Larger than a pipe buffer: the section check must not depend on a writer
    surviving `grep -q` exiting early."""
    body = _BLOCK.format(sha=repo["first"]) + "\n" + ("x" * 1000 + "\n") * 400
    result = _run(repo, body)
    assert result.returncode == 0, result.stderr
