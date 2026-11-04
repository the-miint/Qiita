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
    repo: dict[str, str],
    body: str,
    labels: str = "",
    base: str = "base",
    pr_number: str = "",
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess:
    env = {
        **os.environ,
        **(extra_env or {}),
        "PR_BODY": body,
        "LABELS": labels,
        "HEAD_SHA": "pr",
        "BASE_SHA": base,
        "PR_NUMBER": pr_number,
    }
    return subprocess.run(
        ["bash", str(_SCRIPT)], cwd=repo["path"], env=env, capture_output=True, text=True
    )


def test_accepts_a_block_naming_the_pr_head(repo):
    result = _run(repo, _BLOCK.format(sha=repo["second"]))
    assert result.returncode == 0, result.stderr
    assert repo["second"] in result.stdout


def test_refuses_a_later_commit_the_line_does_not_name(repo):
    """A round that read `first` does not cover `second`, pushed after it."""
    result = _run(repo, _BLOCK.format(sha=repo["first"]))
    assert result.returncode == 1
    assert "not named on that line" in result.stderr
    assert repo["second"][:7] in result.stderr


def test_accepts_a_later_commit_named_on_the_line(repo):
    sha = f"{repo['first']} (plus 1 later commit: {repo['second']}, round 3's fix)"
    result = _run(repo, _BLOCK.format(sha=sha))
    assert result.returncode == 0, result.stderr


def test_a_later_commit_named_outside_the_line_does_not_count(repo):
    """Only the 'Reviewed at' line is read: a sha elsewhere in the section, here under
    Fixed:, does not declare the commit."""
    body = _BLOCK.format(sha=repo["first"]).replace(
        "- Fixed: 3", f"- Fixed: 3\n  - something, `f` — outside the PR's subject, {repo['second']}"
    )
    assert _run(repo, body).returncode == 1


def test_a_merge_of_the_base_needs_no_naming(repo):
    """Merging the base into the branch adds a merge commit and the base's commits;
    neither was written on the branch, so neither has to be named."""
    path = Path(repo["path"])
    _git(path, "checkout", "-q", "base")
    (path / "g").write_text("moved on")
    _git(path, "add", "g")
    _git(path, "commit", "-q", "-m", "base moved on")
    _git(path, "checkout", "-q", "pr")
    _git(path, "merge", "-q", "--no-edit", "base")
    result = _run(repo, _BLOCK.format(sha=repo["second"]))
    assert result.returncode == 0, result.stderr


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
    body = _BLOCK.format(sha=repo["second"]).replace(f"- {heading}", "- Other:")
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
    """The section check must not depend on a writer surviving `grep -q` exiting early.

    100 KB is under Linux's 128 KiB cap on one environment string (400 KB raised E2BIG
    there) and over a pipe buffer. Measured with a body this size against the piped
    form of the script: macOS reported the section missing; Linux did not in the one
    run made, so this pins the fix on macOS and is only a smoke test on Linux.
    """
    body = _BLOCK.format(sha=repo["second"]) + "\n" + ("x" * 999 + "\n") * 100
    result = _run(repo, body)
    assert result.returncode == 0, result.stderr


def _merge_elsewhere(repo) -> str:
    """Merge the `elsewhere` branch, whose commit is not on the base, into `pr`."""
    path = Path(repo["path"])
    _git(path, "merge", "-q", "--no-edit", "-X", "theirs", "elsewhere")
    return _git(path, "rev-parse", "--short", "HEAD")


def test_a_merge_bringing_in_other_commits_has_to_be_named(repo):
    """A pull of work pushed from elsewhere, or another PR's branch merged in before it
    was squash-merged, brings commits no round read; the merge is what declares them."""
    merge = _merge_elsewhere(repo)
    result = _run(repo, _BLOCK.format(sha=repo["second"]))
    assert result.returncode == 1
    assert "not named on that line" in result.stderr
    assert merge[:7] in result.stderr


def test_naming_the_merge_covers_what_it_brought_in(repo):
    merge = _merge_elsewhere(repo)
    sha = f"{repo['second']} (plus 1 later commit: {merge}, merge of elsewhere)"
    result = _run(repo, _BLOCK.format(sha=sha))
    assert result.returncode == 0, result.stderr


def test_a_commit_after_a_named_merge_still_has_to_be_named(repo):
    merge = _merge_elsewhere(repo)
    path = Path(repo["path"])
    (path / "h").write_text("after the merge")
    _git(path, "add", "h")
    _git(path, "commit", "-q", "-m", "after the merge")
    sha = f"{repo['second']} (plus 1 later commit: {merge}, merge of elsewhere)"
    result = _run(repo, _BLOCK.format(sha=sha))
    assert result.returncode == 1
    assert "not named on that line" in result.stderr


def test_a_git_failure_listing_later_commits_fails_the_check(repo, tmp_path):
    """A `git` that cannot list the later commits must not read as "none"."""
    stub = tmp_path / "stub"
    stub.mkdir()
    real_git = subprocess.run(["which", "git"], capture_output=True, text=True).stdout.strip()
    (stub / "git").write_text(f'#!/bin/sh\n[ "$1" = rev-list ] && exit 128\nexec {real_git} "$@"\n')
    (stub / "git").chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{stub}:{os.environ['PATH']}",
        "PR_BODY": _BLOCK.format(sha=repo["first"]),
        "LABELS": "",
        "HEAD_SHA": "pr",
        "BASE_SHA": "base",
    }
    result = subprocess.run(
        ["bash", str(_SCRIPT)], cwd=repo["path"], env=env, capture_output=True, text=True
    )
    assert result.returncode != 0
    assert "Reviewer loop block present" not in result.stdout


_ENTRY = "docs/changelog-pending/entry.md"
_CHECKLIST = "DEPLOY_CHECKLIST.md"
# Another PR's checklist line, whose placeholder the tag commit leaves alone.
_OTHER = "- Other PR's step (#feat/other).\n"
_TAGGED_ENTRY = "- **Thing is done (#12).** It works.\n  More text.\n"
_TAGGED_CHECKLIST = "- `[operator]` (#12) `make migrate`\n- Thing needs a step (#12).\n" + _OTHER


def _commit_files(repo, files: dict[str, str | None], message: str) -> str:
    """Write each file (None deletes it) and commit."""
    path = Path(repo["path"])
    for name, text in files.items():
        if text is None:
            _git(path, "rm", "-q", name)
            continue
        (path / name).parent.mkdir(parents=True, exist_ok=True)
        (path / name).write_text(text)
        _git(path, "add", name)
    _git(path, "commit", "-q", "-m", message)
    return _git(path, "rev-parse", "--short", "HEAD")


@pytest.fixture
def tagged(repo):
    """`repo` with a reviewed commit adding a changelog entry and checklist lines, each
    carrying a placeholder or no tag, so a later commit can write the PR number in."""
    repo["reviewed"] = _commit_files(
        repo,
        {
            _ENTRY: "- **Thing is done (#TBD).** It works.\n  More text.\n",
            _CHECKLIST: "- `[operator]` (#feat/thing) `make migrate`\n- Thing needs a step.\n"
            + _OTHER,
        },
        "the reviewed commit",
    )
    return repo


def _run_tagged(repo, pr_number: str = "12") -> subprocess.CompletedProcess:
    return _run(repo, _BLOCK.format(sha=repo["reviewed"]), pr_number=pr_number)


@pytest.mark.parametrize(
    "files",
    [
        {_ENTRY: _TAGGED_ENTRY},
        {_ENTRY: _TAGGED_ENTRY, _CHECKLIST: _TAGGED_CHECKLIST},
    ],
    ids=["placeholder-replaced", "checklist-tag-added"],
)
def test_a_later_commit_that_only_tags_the_pr_number_needs_no_naming(tagged, files):
    _commit_files(tagged, files, "tag the changelog entry")
    result = _run_tagged(tagged)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    ("files", "pr_number"),
    [
        ({_ENTRY: _TAGGED_ENTRY}, ""),
        ({_ENTRY: _TAGGED_ENTRY.replace("works.", "works well.")}, "12"),
        ({_ENTRY: "- **Thing is done (#12).** It works.\n"}, "12"),
        ({_ENTRY: "- **Thing is done (#12).** It\n  works. More text.\n"}, "12"),
        ({_ENTRY: _TAGGED_ENTRY, "f": "x"}, "12"),
        ({_ENTRY: _TAGGED_ENTRY.replace("More text.", "More (#12).")}, "12"),
        ({_ENTRY: _TAGGED_ENTRY.replace("(#12)", "(#13)")}, "12"),
        ({_ENTRY: _TAGGED_ENTRY.replace("(#12)", "(#feat/thing)")}, "12"),
        (
            {
                _ENTRY: _TAGGED_ENTRY,
                _CHECKLIST: "- Thing needs a step.\n- `[operator]` (#12) `make migrate`\n" + _OTHER,
            },
            "12",
        ),
        (
            {
                _ENTRY: _TAGGED_ENTRY + "- Thing needs a step.\n",
                _CHECKLIST: "- `[operator]` (#12) `make migrate`\n" + _OTHER,
            },
            "12",
        ),
        ({_ENTRY: _TAGGED_ENTRY, "docs/changelog-pending/new.md": "- New (#12).\n"}, "12"),
        ({_ENTRY: None, "docs/changelog-pending/renamed.md": _TAGGED_ENTRY}, "12"),
        ({_ENTRY: _TAGGED_ENTRY.replace("More text.", "More text (#skip-step).")}, "12"),
    ],
    ids=[
        "no-pr-number",
        "wording-changed",
        "line-dropped",
        "rewrapped",
        "other-file",
        "tag-replacing-a-word",
        "another-pr-number",
        "no-number",
        "line-moved-within-a-file",
        "line-moved-across-files",
        "file-added",
        "file-renamed",
        "placeholder-added",
    ],
)
def test_a_later_commit_doing_more_than_tagging_has_to_be_named(tagged, files, pr_number):
    later = _commit_files(tagged, files, "tag and something else")
    result = _run_tagged(tagged, pr_number=pr_number)
    assert result.returncode == 1
    assert later[:7] in result.stderr


def test_a_file_renamed_into_the_changelog_directory_has_to_be_named(tagged):
    """Rename detection would list only the destination, inside the allowed paths."""
    path = Path(tagged["path"])
    _git(path, "mv", "f", "docs/changelog-pending/f.md")
    later = _commit_files(tagged, {_ENTRY: _TAGGED_ENTRY}, "move a file in and tag")
    result = _run_tagged(tagged)
    assert result.returncode == 1
    assert later[:7] in result.stderr


def test_a_commit_after_the_tag_commit_still_has_to_be_named(tagged):
    _commit_files(tagged, {_ENTRY: _TAGGED_ENTRY}, "tag")
    after = _commit_files(tagged, {"f": "after the tag"}, "after the tag")
    result = _run_tagged(tagged)
    assert result.returncode == 1
    assert after[:7] in result.stderr


def test_a_change_after_a_non_utf8_byte_has_to_be_named(tagged):
    """macOS sed fails on a byte that is not UTF-8 in a UTF-8 locale; a failure on both
    sides must not read as two identical files. GNU sed passes the byte through, so this
    tells the two script versions apart only on macOS."""
    path = Path(tagged["path"])
    (path / _ENTRY).write_bytes(b"- **Thing is done (#TBD).** It works.\n\xff\nMore text.\n")
    _git(path, "commit", "-q", "-am", "a non-UTF-8 byte")
    tagged["reviewed"] = _git(path, "rev-parse", "--short", "HEAD")
    (path / _ENTRY).write_bytes(b"- **Thing is done (#12).** It works.\n\xff\nLess text.\n")
    _git(path, "commit", "-q", "-am", "tag and change a line")
    later = _git(path, "rev-parse", "--short", "HEAD")
    result = _run(
        tagged,
        _BLOCK.format(sha=tagged["reviewed"]),
        pr_number="12",
        extra_env={"LANG": "en_US.UTF-8", "LC_ALL": "en_US.UTF-8"},
    )
    assert result.returncode == 1
    assert later[:7] in result.stderr
