"""Smoke-check the deploy shell scripts (deploy/*.sh) and the workflow entrypoints.

These run on the Linux deploy host, not in CI's Python env, so they have no
unit-test harness of their own. This pure-unit guard (under `make test`) catches
the cheap-but-real failures — syntax errors and shellcheck warnings — before they
ship to a host where a broken deploy script is expensive. Mirrors the `bash -n`
precedent in test_compute_readiness.py::test_probe_script_is_valid_bash and the
repo-root reach in test_sif_build_spec.py.

The workflow entrypoints get the `bash -n` half for the same reason and a sharper
one: they run INSIDE a SIF, so a syntax error surfaces as a container step dying
on a real ticket, after the image has been rebuilt and staged. Several embed a
long single-quoted awk program, where an apostrophe in a comment closes the quote
and breaks the script — the shape this gate is here to stop.

shellcheck is optional: when it isn't installed the shellcheck assertion skips
gracefully (same posture as the apptainer-optional workflow tests).
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEPLOY = _REPO_ROOT / "deploy"
_COMMON = _DEPLOY / "_common.sh"
_BUILD_SIF = _REPO_ROOT / "scripts" / "build-sif.sh"
_LAKE_SHELL = _REPO_ROOT / "scripts" / "lake-shell.sh"
_LAKE_GC = _REPO_ROOT / "scripts" / "lake-gc.sh"

# The scripts introduced/maintained for the deploy-ease work. Kept
# explicit (not a glob) so a new deploy script is a deliberate add here.
# build-sifs.sh is the deploy-time SIF auto-builder (wraps scripts/build-sif.sh).
_SCRIPTS = ("preflight.sh", "verify.sh", "redeploy.sh", "build-sifs.sh")
# Sourced-only fragments (no shebang-as-entrypoint, not executable). _common.sh
# carries real logic (qiita_native_checkout_from_python etc.) the executable
# scripts rely on, so it gets the same bash -n + shellcheck gate — but NOT the
# executable-bit check below, since it's never run directly.
_SOURCED = ("_common.sh",)

# The per-step entrypoints and the shared helper they source. A glob, not a list:
# a new workflow step ships a new .sh, and it wants this gate by default rather
# than by remembering to register it.
_WORKFLOW_SCRIPTS = sorted((_REPO_ROOT / "workflows").rglob("*.sh"))


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


# --- qiita_deploy_self_fingerprint / qiita_deploy_reexec_if_changed ----------
# redeploy.sh pulls the clone it lives in, so the pull can replace redeploy.sh and
# _common.sh under the running shell. The fingerprint is how the script notices;
# the re-exec is what it does about it. Both are exercised end to end below, on a
# fixture that copies the real _common.sh so the helpers under test are the
# shipped ones. -----------------------------------------------------------------

_REDEPLOY = _DEPLOY / "redeploy.sh"


def _fake_deploy_dir(tmp_path: Path, *, script: str, common_suffix: str = "") -> Path:
    """A `deploy/`-shaped dir: a script beside a copy of the real `_common.sh`.
    The copy is what `QIITA_COMMON_SH` resolves to, so a test can edit or delete
    it. `common_suffix` appends bytes, standing in for a pull that touched only
    `_common.sh`."""
    d = tmp_path / "deploy"
    d.mkdir(parents=True)
    (d / "_common.sh").write_text(_COMMON.read_text() + common_suffix)
    path = d / "redeploy.sh"
    path.write_text(script)
    return path


def _call_self_fingerprint(script: Path, *, prelude: str = "") -> subprocess.CompletedProcess[str]:
    """Source the fixture's `_common.sh`, then fingerprint the fixture script."""
    common = script.parent / "_common.sh"
    return subprocess.run(
        [
            "bash",
            "-c",
            f'source "{common}"; {prelude} qiita_deploy_self_fingerprint "$1"',
            "_",
            str(script),
        ],
        capture_output=True,
        text=True,
    )


def test_self_fingerprint_changes_when_the_script_is_replaced_by_rename(tmp_path: Path) -> None:
    """The production event, exactly: `git checkout` writes a temp file and
    renames it over the tracked path. Same path, new inode, new bytes."""
    script = _fake_deploy_dir(tmp_path, script="# old\n")
    before = _call_self_fingerprint(script)
    assert before.returncode == 0, before.stderr
    assert before.stdout.strip() != ""

    replacement = script.parent / "redeploy.sh.pulled"
    replacement.write_text("# new\n")
    os.replace(replacement, script)

    after = _call_self_fingerprint(script)
    assert after.returncode == 0, after.stderr
    assert after.stdout != before.stdout


def test_self_fingerprint_covers_the_sourced_common(tmp_path: Path) -> None:
    """`_common.sh` is read into the same process, so a pull that changes only it
    leaves the running script just as stale. Anchoring on the script alone would
    miss that."""
    script = _fake_deploy_dir(tmp_path, script="# script\n")
    before = _call_self_fingerprint(script).stdout
    common = script.parent / "_common.sh"
    common.write_text(common.read_text() + "\n# pulled\n")
    assert _call_self_fingerprint(script).stdout != before


def test_self_fingerprint_separates_the_two_files(tmp_path: Path) -> None:
    """Digesting each file under its name, rather than hashing the two
    concatenated, is what keeps bytes moving from one to the other visible."""
    moved = "# moved line\n"
    in_script = _fake_deploy_dir(tmp_path / "a", script=f"# script\n{moved}")
    in_common = _fake_deploy_dir(tmp_path / "b", script="# script\n", common_suffix=moved)
    assert _call_self_fingerprint(in_script).stdout != _call_self_fingerprint(in_common).stdout


def test_self_fingerprint_fails_loudly_when_the_script_is_unreadable(tmp_path: Path) -> None:
    """rc=1 + a stderr reason, not an empty digest. An empty digest compares
    unequal to the pre-pull one, which would re-exec a file that just failed to
    read."""
    script = _fake_deploy_dir(tmp_path, script="# script\n")
    script.unlink()
    result = _call_self_fingerprint(script)
    assert result.returncode == 1
    assert result.stdout.strip() == ""
    assert "redeploy.sh" in result.stderr


def test_self_fingerprint_fails_loudly_when_common_is_unreadable(tmp_path: Path) -> None:
    """Same, for the sourced half: the file is gone from disk after this process
    read it, which is what an interrupted pull can leave behind."""
    script = _fake_deploy_dir(tmp_path, script="# script\n")
    common = script.parent / "_common.sh"
    result = _call_self_fingerprint(script, prelude=f'rm "{common}";')
    assert result.returncode == 1
    assert result.stdout.strip() == ""
    assert "_common.sh" in result.stderr


# A script that mimics redeploy.sh step 1 against the fixture's own _common.sh:
# fingerprint, optionally let a "pull" replace it by rename, then hand off to the
# helper. v1 prints `original`; the replacement prints `pulled`, so stdout says
# which body finished the run.
_PULLED_DEFAULT = "printf 'pulled\\n'"

_STEP1_V1 = """#!/usr/bin/env bash
set -euo pipefail
source "{common}"
SELF="{self}"
printf 'original\\n'
before=$(qiita_deploy_self_fingerprint "$SELF")
if [ -n "{replace}" ]; then
    cat > "$SELF.pulled" <<'PULLED'
#!/usr/bin/env bash
{pulled}
PULLED
    mv "$SELF.pulled" "$SELF"
fi
qiita_deploy_reexec_if_changed "$SELF" "$before"
printf 'original-continued\\n'
"""


def _run_step1(
    tmp_path: Path,
    *,
    replace: bool,
    env: dict[str, str] | None = None,
    pulled: str = _PULLED_DEFAULT,
) -> subprocess.CompletedProcess[str]:
    d = tmp_path / "deploy"
    d.mkdir(parents=True)
    (d / "_common.sh").write_text(_COMMON.read_text())
    script = d / "redeploy.sh"
    script.write_text(
        _STEP1_V1.format(
            common=d / "_common.sh",
            self=script,
            replace="1" if replace else "",
            pulled=pulled,
        )
    )
    return subprocess.run(
        ["bash", str(script)],
        capture_output=True,
        text=True,
        env={**os.environ, **(env or {})},
    )


def test_reexec_runs_the_pulled_body_and_abandons_the_original(tmp_path: Path) -> None:
    """The whole point, end to end: the helper `exec`s, so the run finishes on the
    replacement and never returns to the line after the call."""
    run = _run_step1(tmp_path, replace=True)
    assert run.returncode == 0, run.stderr
    assert run.stdout.splitlines()[0] == "original"
    assert run.stdout.splitlines()[-1] == "pulled"
    assert "original-continued" not in run.stdout


def test_reexec_is_a_no_op_when_nothing_changed(tmp_path: Path) -> None:
    """Control for the test above: same fixture, no replacement. The helper must
    return so the caller carries on — a deploy that re-execs every time would
    never leave step 1."""
    run = _run_step1(tmp_path, replace=False)
    assert run.returncode == 0, run.stderr
    assert run.stdout.splitlines() == ["original", "original-continued"]


def test_reexec_refuses_a_second_time_rather_than_looping(tmp_path: Path) -> None:
    """With the sentinel already set, a further change aborts. Nothing is deployed
    at step 1, so the caller can be stopped for the cost of a re-run; carrying on
    would be running code the process knows is not the pulled code."""
    run = _run_step1(tmp_path, replace=True, env={"QIITA_REDEPLOY_REEXECED": "1"})
    assert run.returncode != 0
    assert "changed again after the re-exec" in run.stderr
    assert "pulled" not in run.stdout
    assert "original-continued" not in run.stdout


def test_reexec_sentinel_reaches_the_replacement(tmp_path: Path) -> None:
    """The sentinel is exported, so the re-exec'd process sees it. Were it a plain
    shell variable the replacement would start with it unset, and a clone that
    kept changing could re-exec without end."""
    run = _run_step1(
        tmp_path,
        replace=True,
        pulled="printf 'pulled sentinel=%s\\n' \"$QIITA_REDEPLOY_REEXECED\"",
    )
    assert run.returncode == 0, run.stderr
    assert "pulled sentinel=1" in run.stdout


def test_bash_keeps_executing_a_script_replaced_by_rename(tmp_path: Path) -> None:
    """The premise the re-exec rests on, asserted rather than read off the docs.

    bash holds the script's fd, so a rename over the path swaps the inode without
    touching what the running shell reads: the process finishes on the original
    body. That is why steps 2-8 of a redeploy run pre-pull code, and it is a
    property of bash, not something the deploy arranges. The body is padded well
    past a single read so the case is not decided by read-ahead alone; either way
    what is asserted is the observable. The control below shows the swap landed."""
    script = tmp_path / "self-replacing.sh"
    padding = "\n".join(f"# pad {i:05d}" * 4 for i in range(4096))
    body = "\n".join(
        [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            "printf 'start\\n'",
            padding,
            # The replacement is a script in its own right, so the control below
            # can run it and show the rename landed.
            "cat > \"$0.new\" <<'NEW'",
            "printf 'REPLACEMENT\\n'",
            "NEW",
            'mv "$0.new" "$0"',
            "printf 'end-of-original\\n'",
            "",
        ]
    )
    script.write_text(body)
    assert len(body) > 65536, "keep the body well past a single read"

    run = subprocess.run(["bash", str(script)], capture_output=True, text=True)
    assert run.returncode == 0, run.stderr
    assert run.stdout.splitlines() == ["start", "end-of-original"], (
        "bash executed something other than the original body after the rename; "
        f"stdout was {run.stdout!r}"
    )

    # Control: the swap landed, so a FRESH bash on the same path runs the new body.
    fresh = subprocess.run(["bash", str(script)], capture_output=True, text=True)
    assert fresh.stdout.splitlines() == ["REPLACEMENT"], (
        "the rename did not replace the script, so the assertion above proved "
        f"nothing (stdout={fresh.stdout!r}, stderr={fresh.stderr!r})"
    )


def test_redeploy_refuses_a_self_outside_the_clone(tmp_path: Path) -> None:
    """The re-exec check can only see a pull that lands in $QIITA_CLONE, so
    redeploy.sh aborts when it is not running from that clone — otherwise the
    check is green for a reason unrelated to freshness."""
    text = _REDEPLOY.read_text()
    guard = text.find('case "$SELF" in')
    pull = text.find('git -C "$QIITA_CLONE" pull --ff-only')
    assert 0 <= guard < pull, "the containment guard must run before the pull"
    assert '"$QIITA_CLONE"/*' in text


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


def test_deployed_history_heading_pins_the_live_section_boundary() -> None:
    """`## Deployed history` terminates the sed range that prints the live section.

    Two consumers slice DEPLOY_CHECKLIST.md with
    `sed -n '/^## Pending deploy/,/^## Deployed history/p'` — the operator, in
    redeploy.md §1, and the agent, in /deploy-note. Since the archived deploys
    moved out to docs/deploy-archive/, the heading is a short pointer stub with no
    content under it, which makes it look like dead weight a tidy-up would delete.
    It isn't: delete it and both ranges run to EOF. Pin it."""
    text = (_REPO_ROOT / "DEPLOY_CHECKLIST.md").read_text()
    assert "\n## Deployed history\n" in text, (
        "DEPLOY_CHECKLIST.md lost its '## Deployed history' heading. It is the "
        "terminator of the `## Pending deploy` sed range used by redeploy.md §1 "
        "and /deploy-note; without it both print the rest of the file."
    )
    assert text.index("\n## Pending deploy\n") < text.index("\n## Deployed history\n"), (
        "'## Deployed history' must come after '## Pending deploy' — the sed range "
        "between them is empty otherwise."
    )


def test_deploy_archive_index_covers_every_archived_deploy() -> None:
    """`docs/deploy-archive/README.md` indexes exactly the files beside it.

    The index is hand-maintained (by `/deploy-archive`, which adds a line when it
    writes a file), so it drifts the moment someone writes one and forgets the
    other. A missing line hides a deploy from the only listing anyone reads; a
    stale line is a dead link. Both are silent."""
    archive = _REPO_ROOT / "docs" / "deploy-archive"
    index = (archive / "README.md").read_text()

    on_disk = {p.name for p in archive.glob("*.md")} - {"README.md"}
    linked = set(re.findall(r"\]\((\d{4}-\d{2}-\d{2}-[^)]+\.md)\)", index))

    assert on_disk == linked, (
        "docs/deploy-archive/ and its README index disagree. Missing from the "
        f"index: {sorted(on_disk - linked)}. Indexed but absent from disk (dead "
        f"links): {sorted(linked - on_disk)}."
    )


# --- scripts/build-sif.sh: now sources deploy/_common.sh for the build-inputs
# hash, so it gets the same bash -n + shellcheck gate as the deploy scripts. ------


def test_build_sif_exists_and_executable() -> None:
    assert _BUILD_SIF.is_file(), f"{_BUILD_SIF} missing"
    assert _BUILD_SIF.stat().st_mode & 0o111, f"{_BUILD_SIF} is not executable"


def test_build_sif_is_valid_bash() -> None:
    result = subprocess.run(["bash", "-n", str(_BUILD_SIF)], capture_output=True, text=True)
    assert result.returncode == 0, f"bash -n failed for build-sif.sh:\n{result.stderr}"


def test_build_sif_passes_shellcheck() -> None:
    if shutil.which("shellcheck") is None:
        pytest.skip("shellcheck not installed")
    # -S warning to match the deploy-script gate above; the `# shellcheck source=`
    # directive in build-sif.sh keeps the cross-dir _common.sh source from flagging.
    result = subprocess.run(
        ["shellcheck", "-S", "warning", str(_BUILD_SIF)], capture_output=True, text=True
    )
    assert result.returncode == 0, (
        f"shellcheck flagged build-sif.sh:\n{result.stdout}\n{result.stderr}"
    )


# --- qiita_sif_build_inputs_hash: the content stamp build-sif.sh uses to detect a
# changed def/entrypoint/manifest (the trap VERIFY_MATCH can't see). --------------


def _make_workflow_tree(root: Path) -> tuple[Path, Path]:
    """A minimal repo layout: workflows/<wf>/ + workflows/_shared/. Returns
    (workflow_dir, shared_dir). Includes files the hash must IGNORE (the spec, a
    .gitignore, a vendored *.rpm) so a test can prove they don't affect the digest."""
    wf = root / "workflows" / "demo"
    shared = root / "workflows" / "_shared"
    wf.mkdir(parents=True)
    shared.mkdir(parents=True)
    (wf / "Apptainer.def").write_text("Bootstrap: docker\nFrom: oraclelinux:8\n")
    (wf / "entrypoint.sh").write_text("#!/bin/sh\necho hi\n")
    (wf / "sif-build.env").write_text('SIF_FILENAME="demo.sif"\n')  # must be ignored
    (wf / ".gitignore").write_text("*.rpm\n")  # must be ignored
    (wf / "demo-1.0.rpm").write_text("binary-ish")  # vendored SOURCE — must be ignored
    (shared / "manifest_writer.py").write_text("x = 1\n")
    return wf, shared


def _call_build_inputs_hash(repo_root: Path, wf: Path, shared: Path) -> str:
    result = subprocess.run(
        [
            "bash",
            "-c",
            f'source "{_COMMON}"; qiita_sif_build_inputs_hash "$1" "$2" "$3"',
            "_",
            str(repo_root),
            str(wf),
            str(shared),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def test_build_inputs_hash_is_deterministic(tmp_path: Path) -> None:
    wf, shared = _make_workflow_tree(tmp_path)
    assert _call_build_inputs_hash(tmp_path, wf, shared) == _call_build_inputs_hash(
        tmp_path, wf, shared
    )


def test_build_inputs_hash_changes_on_entrypoint_edit(tmp_path: Path) -> None:
    """A def/entrypoint/manifest edit MUST change the hash — that's the whole point
    (it triggers the rebuild VERIFY_MATCH would have skipped)."""
    wf, shared = _make_workflow_tree(tmp_path)
    before = _call_build_inputs_hash(tmp_path, wf, shared)
    (wf / "entrypoint.sh").write_text("#!/bin/sh\necho changed\n")
    assert _call_build_inputs_hash(tmp_path, wf, shared) != before


def test_build_inputs_hash_ignores_spec_gitignore_and_sources(tmp_path: Path) -> None:
    """Changing the spec, .gitignore, or a vendored *.rpm must NOT change the hash —
    re-vendoring 4.5.4-1 → 4.5.4-2 must not force a rebuild (VERIFY_MATCH's loose
    patch component), and the spec/gitignore aren't baked into the image."""
    wf, shared = _make_workflow_tree(tmp_path)
    before = _call_build_inputs_hash(tmp_path, wf, shared)
    (wf / "sif-build.env").write_text('SIF_FILENAME="demo.sif"\nSOURCES="demo-2.0.rpm"\n')
    (wf / ".gitignore").write_text("*.rpm\n*.sif\n")
    (wf / "demo-1.0.rpm").write_text("re-vendored bytes")
    assert _call_build_inputs_hash(tmp_path, wf, shared) == before


def test_build_inputs_hash_is_location_independent(tmp_path: Path) -> None:
    """Same content under a different repo root → same digest (the keys are
    repo-RELATIVE paths). Matters: activate.sh runs from the clone, the CI path
    from /opt/qiita/incoming — both must agree on 'unchanged'."""
    wf_a, shared_a = _make_workflow_tree(tmp_path / "clone")
    wf_b, shared_b = _make_workflow_tree(tmp_path / "incoming")
    assert _call_build_inputs_hash(tmp_path / "clone", wf_a, shared_a) == _call_build_inputs_hash(
        tmp_path / "incoming", wf_b, shared_b
    )


def test_build_inputs_hash_survives_unreadable_cwd(tmp_path: Path) -> None:
    """Regression: a manual `sudo -u qiita-orch build-sif.sh` launched from an
    admin's 0700 home left `find` unable to restore that cwd, so it exited
    non-zero and aborted the build under `set -o pipefail`. The helper now cd's to
    / in a subshell, so it doesn't depend on (or need to restore) the caller cwd.

    Reproduce deterministically: run from a directory, strip its traversal bit
    (chmod 000) for the duration of the call so a naive `find` could not chdir
    back, then restore it for cleanup. Skipped under root, which ignores the
    permission and so can't exercise the failure."""
    if os.geteuid() == 0:
        pytest.skip("root ignores the dir-traversal bit; can't reproduce the failure")
    wf, shared = _make_workflow_tree(tmp_path)
    expected = _call_build_inputs_hash(tmp_path, wf, shared)  # baseline from a normal cwd
    locked = tmp_path / "locked"
    locked.mkdir()
    # Spawn with cwd=locked (still traversable), then drop its traversal bit from
    # inside so a naive `find` could not chdir back to it. Restore perms in Python
    # via the ABSOLUTE path in finally — the parents stay traversable, so cleanup
    # never depends on the now-unreadable cwd (chmod'ing "." from a 000 cwd is
    # unreliable). With the fix the helper cd's to / and so doesn't care.
    try:
        result = subprocess.run(
            [
                "bash",
                "-c",
                f'chmod 000 .; source "{_COMMON}"; qiita_sif_build_inputs_hash "$1" "$2" "$3"',
                "_",
                str(tmp_path),
                str(wf),
                str(shared),
            ],
            cwd=str(locked),
            capture_output=True,
            text=True,
        )
    finally:
        os.chmod(locked, 0o755)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == expected


# --- qiita_sif_build_inputs_hash_scoped: the per-tool-image variant. Hashes an
# EXPLICIT file list + _shared instead of the whole workflow dir, so an edit to a
# sibling tool's def/entrypoint leaves this image's stamp unchanged (the rebuild
# granularity the multi-image split delivers). ------------------------------------


def _call_scoped_hash(repo_root: Path, shared: Path, files: list[Path]) -> str:
    result = subprocess.run(
        [
            "bash",
            "-c",
            f'source "{_COMMON}"; qiita_sif_build_inputs_hash_scoped "$@"',
            "_",
            str(repo_root),
            str(shared),
            *[str(f) for f in files],
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def test_scoped_hash_is_deterministic(tmp_path: Path) -> None:
    wf, shared = _make_workflow_tree(tmp_path)
    files = [wf / "Apptainer.def", wf / "entrypoint.sh"]
    assert _call_scoped_hash(tmp_path, shared, files) == _call_scoped_hash(tmp_path, shared, files)


def test_scoped_hash_changes_when_a_listed_input_edits(tmp_path: Path) -> None:
    wf, shared = _make_workflow_tree(tmp_path)
    files = [wf / "Apptainer.def", wf / "entrypoint.sh"]
    before = _call_scoped_hash(tmp_path, shared, files)
    (wf / "entrypoint.sh").write_text("#!/bin/sh\necho changed\n")
    assert _call_scoped_hash(tmp_path, shared, files) != before


def test_scoped_hash_ignores_files_not_in_its_input_set(tmp_path: Path) -> None:
    """The whole point of scoping: a sibling tool's file (present in the workflow
    dir but NOT in this image's declared input list) must NOT change the digest —
    that is what lets one tool's image rebuild independently of the others."""
    wf, shared = _make_workflow_tree(tmp_path)
    files = [wf / "Apptainer.def", wf / "entrypoint.sh"]
    before = _call_scoped_hash(tmp_path, shared, files)
    (wf / "sibling-tool.def").write_text("Bootstrap: docker\nFrom: oraclelinux:9\n")
    (wf / "sibling.sh").write_text("#!/bin/sh\necho sibling\n")
    assert _call_scoped_hash(tmp_path, shared, files) == before


def test_scoped_hash_changes_on_shared_edit(tmp_path: Path) -> None:
    """_shared/ is always in scope (every image %files-copies manifest_writer.py),
    so a change there rebuilds every image — the intended fan-out."""
    wf, shared = _make_workflow_tree(tmp_path)
    files = [wf / "Apptainer.def", wf / "entrypoint.sh"]
    before = _call_scoped_hash(tmp_path, shared, files)
    (shared / "manifest_writer.py").write_text("x = 2\n")
    assert _call_scoped_hash(tmp_path, shared, files) != before


# --- qiita_sif_missing_sources: gates whether build-sifs.sh SKIPS an image whose
# licensed artifact isn't staged. rc 0 = all present, 1 = some missing (echoed). --


def _call_missing_sources(sources_dir: Path, sources: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "bash",
            "-c",
            f'source "{_COMMON}"; qiita_sif_missing_sources "$1" "$2"',
            "_",
            str(sources_dir),
            sources,
        ],
        capture_output=True,
        text=True,
    )


def test_missing_sources_all_present_returns_zero(tmp_path: Path) -> None:
    (tmp_path / "a.rpm").write_text("x")
    (tmp_path / "b.rpm").write_text("y")
    result = _call_missing_sources(tmp_path, "a.rpm b.rpm")
    assert result.returncode == 0
    assert result.stdout == ""


def test_missing_sources_empty_list_returns_zero(tmp_path: Path) -> None:
    """A workflow that vendors nothing from sources/ (empty SOURCES) → nothing
    missing → rc 0, so build-sifs.sh proceeds to build it."""
    result = _call_missing_sources(tmp_path, "")
    assert result.returncode == 0
    assert result.stdout == ""


def test_missing_sources_some_missing_returns_one_and_lists_them(tmp_path: Path) -> None:
    (tmp_path / "present.rpm").write_text("x")
    result = _call_missing_sources(tmp_path, "present.rpm gone.rpm also-gone.rpm")
    assert result.returncode == 1
    missing = set(result.stdout.split())
    assert missing == {"gone.rpm", "also-gone.rpm"}
    assert "present.rpm" not in missing


# --- scripts/lake-shell.sh: the read-only DuckLake/CP shell. Sources
# deploy/_common.sh for read_env_var + qiita_split_conn_password, so it gets the
# same bash -n + shellcheck gate as the deploy scripts. ------------------------


def test_lake_data_path_helper_derives_exactly_like_the_data_plane() -> None:
    """DuckLake pins DATA_PATH into the catalog at creation and rejects an attach
    whose DATA_PATH differs by even a slash, so the derivation must reproduce
    config.rs's bare `format!("{path_persistent_raw}/ducklake")` — no trailing-slash
    normalization. A `${PERSISTENT%/}` anywhere breaks every host whose
    PATH_PERSISTENT ends in `/`. Exercised through the helper rather than grepped
    for, and both callers are pinned to it."""

    def derive(persistent: str) -> str:
        result = subprocess.run(
            ["bash", "-c", f'source "{_COMMON}"; qiita_lake_data_path "$1"', "_", persistent],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout

    assert derive("/data") == "/data/ducklake"
    assert derive("/data/") == "/data//ducklake", "a trailing slash must survive verbatim"
    for script in (_LAKE_SHELL, _LAKE_GC):
        body = script.read_text()
        assert "PERSISTENT%/" not in body, f"{script.name} normalizes the trailing slash"
        assert "qiita_lake_data_path" in body, f"{script.name} must use the shared helper"


def _call_split_conn_password(connstr: str) -> list[str]:
    """Returns [sanitized, user, password] as the helper echoes them."""
    result = subprocess.run(
        ["bash", "-c", f'source "{_COMMON}"; qiita_split_conn_password "$1"', "_", connstr],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.rstrip("\n").split("\t")


@pytest.mark.parametrize(
    ("connstr", "expected"),
    [
        # libpq key=value: password lifted out, remaining whitespace collapsed.
        (
            "dbname=lake host=db port=5432 user=lake_rw password=s3cr3t sslmode=prefer",
            ["dbname=lake host=db port=5432 user=lake_rw sslmode=prefer", "lake_rw", "s3cr3t"],
        ),
        # libpq with no password at all (peer auth): untouched.
        (
            "dbname=lake host=db user=lake_rw",
            ["dbname=lake host=db user=lake_rw", "lake_rw", ""],
        ),
        # postgres:// URI: credentials come out of the authority section.
        (
            "postgresql://cp_rw:pw@db:5432/qiita",
            ["postgresql://cp_rw@db:5432/qiita", "cp_rw", "pw"],
        ),
        # URI with a user but no password.
        (
            "postgresql://cp_rw@db:5432/qiita",
            ["postgresql://cp_rw@db:5432/qiita", "cp_rw", ""],
        ),
        # A ':' inside the password is legal unencoded and must not truncate it.
        (
            "postgresql://cp_rw:pa:ss@db/qiita",
            ["postgresql://cp_rw@db/qiita", "cp_rw", "pa:ss"],
        ),
    ],
)
def test_split_conn_password_extracts(connstr: str, expected: list[str]) -> None:
    assert _call_split_conn_password(connstr) == expected


@pytest.mark.parametrize(
    "connstr",
    [
        # Percent-encoded URI password: pgpass wants the DECODED value, so the
        # helper must decline rather than hand libpq the still-encoded string.
        "postgresql://cp_rw:pa%40ss@db/qiita",
        # No username to key a pgpass entry on.
        "dbname=lake host=db password=s3cr3t",
    ],
)
def test_split_conn_password_declines_when_it_cannot_key_a_pgpass_entry(connstr: str) -> None:
    sanitized, _user, password = _call_split_conn_password(connstr)
    assert password == "", "password must not be lifted out when it cannot be keyed"
    assert sanitized == connstr, "connstr must be left intact for the caller to use as-is"


def test_lake_shell_refuses_to_open_without_the_staged_miint_extension(tmp_path: Path) -> None:
    """miint is a core dependency, so the shell must fail LOUD rather than open a
    session whose bioinformatics functions differ from production's. Everything
    else it needs is present here — only MIINT_EXTENSION_DIRECTORY is missing."""
    persistent = tmp_path / "persistent"
    (persistent / "ducklake").mkdir(parents=True)
    dp_env = tmp_path / "data-plane.env"
    dp_env.write_text(
        "DUCKLAKE_CATALOG_CONNSTR='dbname=lake host=localhost user=lake_rw'\n"
        f"PATH_PERSISTENT={persistent}\n"
    )
    cp_env = tmp_path / "control-plane.env"
    cp_env.write_text("DATABASE_URL='postgresql://cp_rw@localhost:5432/qiita'\n")

    env = {
        **os.environ,
        "DP_ENV": str(dp_env),
        "CP_ENV": str(cp_env),
        "CO_ENV": str(tmp_path / "absent.env"),
        "QIITA_DUCKDB_BIN": "/bin/true",
    }
    env.pop("MIINT_EXTENSION_DIRECTORY", None)

    result = subprocess.run(
        ["bash", str(_LAKE_SHELL), "-c", "SELECT 1"], capture_output=True, text=True, env=env
    )
    assert result.returncode == 1, f"expected a hard failure, got:\n{result.stdout}"
    assert "MIINT_EXTENSION_DIRECTORY" in result.stderr


# --- scripts/lake-gc.sh: DuckLake reclamation. Reports by default; --reclaim
# expires snapshots and unlinks files. Sources deploy/_common.sh like
# lake-shell.sh, so it gets the same bash -n + shellcheck gate. ---------------


_LAKE_SCRIPTS = (_LAKE_SHELL, _LAKE_GC)


@pytest.mark.parametrize("script", _LAKE_SCRIPTS, ids=lambda p: p.name)
def test_lake_script_exists_and_executable(script: Path) -> None:
    assert script.is_file(), f"{script} missing"
    assert script.stat().st_mode & 0o111, f"{script} is not executable"


@pytest.mark.parametrize("script", _LAKE_SCRIPTS, ids=lambda p: p.name)
def test_lake_script_is_valid_bash(script: Path) -> None:
    result = subprocess.run(["bash", "-n", str(script)], capture_output=True, text=True)
    assert result.returncode == 0, f"bash -n failed for {script.name}:\n{result.stderr}"


@pytest.mark.parametrize("script", _LAKE_SCRIPTS, ids=lambda p: p.name)
def test_lake_script_passes_shellcheck(script: Path) -> None:
    if shutil.which("shellcheck") is None:
        pytest.skip("shellcheck not installed")
    result = subprocess.run(
        ["shellcheck", "-S", "warning", str(script)], capture_output=True, text=True
    )
    assert result.returncode == 0, (
        f"shellcheck flagged {script.name}:\n{result.stdout}\n{result.stderr}"
    )


def _lake_gc_code() -> str:
    """The script with comment-only lines stripped, so an invariant test can
    assert about what the script *runs* without matching the header prose that
    explains the same thing."""
    return "\n".join(
        line for line in _LAKE_GC.read_text().splitlines() if not line.lstrip().startswith("#")
    )


def test_lake_gc_help_exits_zero_and_names_reclaim() -> None:
    result = subprocess.run(["bash", str(_LAKE_GC), "--help"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert "--reclaim" in result.stdout


@pytest.mark.parametrize(
    "bad",
    [
        "7; DROP TABLE x",  # injection shape — the value lands in an INTERVAL literal
        "7 FORTNIGHTS",  # not an INTERVAL unit
        "DAYS",  # no quantity
        "-1 DAYS",  # negative retention would expire everything
    ],
)
def test_lake_gc_rejects_malformed_retention(bad: str) -> None:
    """--older-than is interpolated into `now() - INTERVAL <value>`, so it is
    constrained to the shape an INTERVAL takes rather than screened afterwards."""
    result = subprocess.run(
        ["bash", str(_LAKE_GC), "--older-than", bad], capture_output=True, text=True
    )
    assert result.returncode == 1, f"expected refusal for {bad!r}:\n{result.stdout}"
    assert "--older-than must be" in result.stderr


def test_lake_gc_rejects_unknown_argument() -> None:
    result = subprocess.run(["bash", str(_LAKE_GC), "--wat"], capture_output=True, text=True)
    assert result.returncode == 1
    assert "unknown argument" in result.stderr


def _lake_gc_env(tmp_path, *, writable: bool = True) -> dict:
    """A data-plane env plus a lake dir, with duckdb stubbed by `true` so the
    script's own logic runs without a catalog. `true` prints nothing, which is the
    same shape as a maintenance call that reclaims nothing. Resolved via PATH —
    it is /usr/bin/true on macOS and /bin/true on most Linux."""
    persistent = tmp_path / "persistent"
    (persistent / "ducklake").mkdir(parents=True)
    if not writable:
        (persistent / "ducklake").chmod(0o500)
    dp_env = tmp_path / "data-plane.env"
    dp_env.write_text(
        "DUCKLAKE_CATALOG_CONNSTR='dbname=lake host=localhost user=lake_rw'\n"
        f"PATH_PERSISTENT={persistent}\n"
    )
    # Stub duckdb with a shim that SAVES the SQL it is handed. `true` would
    # discard it, leaving the thing these tests are about — dry_run, the
    # transaction split — unasserted: a hardcoded `dry_run := false` would pass.
    captured = tmp_path / "captured.sql"
    stub = tmp_path / "duckdb-stub"
    stub.write_text(
        "#!/bin/sh\n"
        "while [ $# -gt 0 ]; do\n"
        f'  if [ "$1" = -f ]; then cat "$2" >> "{captured}"; fi\n'
        "  shift\n"
        "done\n"
        "exit 0\n"
    )
    stub.chmod(0o755)
    return {**os.environ, "DP_ENV": str(dp_env), "QIITA_DUCKDB_BIN": str(stub)}


def test_lake_gc_defaults_to_report_only(tmp_path) -> None:
    """No flag must never reclaim: the whole point of the default run is that an
    operator can look before anything is unlinked."""
    result = subprocess.run(
        ["bash", str(_LAKE_GC)], capture_output=True, text=True, env=_lake_gc_env(tmp_path)
    )
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    assert "report only" in result.stdout
    assert "Nothing was removed" in result.stdout
    sql = (tmp_path / "captured.sql").read_text()
    assert "dry_run := true" in sql, "report mode must ask the database for a dry run"
    assert "dry_run := false" not in sql


def test_lake_gc_reclaim_mode_announces_it_acts(tmp_path) -> None:
    result = subprocess.run(
        ["bash", str(_LAKE_GC), "--reclaim"],
        capture_output=True,
        text=True,
        env={**_lake_gc_env(tmp_path), "ASSUME_YES": "1"},
    )
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    assert "Nothing was removed" not in result.stdout
    # The banner alone is a weak signal — report mode's banner also contains the
    # string "--reclaim" ("pass --reclaim to act"). The SQL is what discriminates,
    # and --reclaim is deliberately mixed: steps 1-2 act, step 3 stays dry.
    sql = (tmp_path / "captured.sql").read_text()
    assert "ducklake_expire_snapshots('qiita_lake', dry_run := false" in sql
    assert "ducklake_cleanup_old_files('qiita_lake', dry_run := false" in sql
    assert "ducklake_delete_orphaned_files('qiita_lake', dry_run := true" in sql, (
        "--reclaim must NOT delete orphans: a registration in flight has its file "
        "on disk with no catalog row, which is what that step deletes"
    )


def test_lake_gc_reclaim_orphans_is_the_only_way_to_delete_orphans(tmp_path) -> None:
    """The orphan step is the only one that can reach a file belonging to a
    registration in flight (register_files moves the file before opening the
    catalog transaction). It gets its own flag so the safe bulk reclaim needs no
    quiescing, and only this one carries that precondition."""
    result = subprocess.run(
        ["bash", str(_LAKE_GC), "--reclaim-orphans"],
        capture_output=True,
        text=True,
        env={**_lake_gc_env(tmp_path), "ASSUME_YES": "1"},
    )
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    sql = (tmp_path / "captured.sql").read_text()
    assert "ducklake_delete_orphaned_files('qiita_lake', dry_run := false" in sql
    assert "dry_run := true" not in sql, "--reclaim-orphans acts on all three steps"


def test_lake_gc_reclaim_requires_typed_confirmation(tmp_path) -> None:
    """--reclaim expires snapshot history irreversibly, so it must not proceed on
    the flag alone. Anything other than the typed word aborts before the first
    maintenance call."""
    env = _lake_gc_env(tmp_path)
    for reply in ("", "yes\n", "y\n", "RECLAIM\n"):
        result = subprocess.run(
            ["bash", str(_LAKE_GC), "--reclaim"],
            capture_output=True,
            text=True,
            input=reply,
            env=env,
        )
        assert result.returncode == 1, f"expected an abort for {reply!r}:\n{result.stdout}"
        assert "Aborted" in result.stderr
    assert not (tmp_path / "captured.sql").exists(), "aborting must not reach the database"


def test_lake_gc_refuses_unwritable_data_path(tmp_path) -> None:
    """The gate is unconditional, so it fires in report mode too — which is what
    this exercises. DuckLake needs to write the data path even for the dry-run
    orphan scan (it fails read-only there), so a group-read operator cannot run
    even the report. Failing here names the right run-as instead of surfacing as
    a permission error from inside duckdb."""
    if os.geteuid() == 0:
        pytest.skip("root ignores the write bit")
    result = subprocess.run(
        ["bash", str(_LAKE_GC)],
        capture_output=True,
        text=True,
        env=_lake_gc_env(tmp_path, writable=False),
    )
    assert result.returncode == 1
    assert "not writable" in result.stderr
    assert "qiita-data" in result.stderr


def test_lake_gc_never_passes_cleanup_all() -> None:
    """`cleanup_all := true` drops the mtime filter outright, so a reclaim would
    also sweep files produced inside the cutoff. The script header carries the
    rationale, including why that filter is not sufficient on its own."""
    assert "cleanup_all" not in _lake_gc_code()


def test_lake_gc_uses_the_documented_call_form() -> None:
    """DuckLake documents these table functions as `CALL f(...)`. `SELECT * FROM
    f(...)` behaves identically (measured on 1.5.4 — same rows, same deletions),
    but a reader checking this against the docs should find the documented form.
    https://ducklake.select/docs/stable/duckdb/maintenance/expire_snapshots"""
    code = _lake_gc_code()
    assert "SELECT * FROM ducklake_" not in code
    for fn in ("expire_snapshots", "cleanup_old_files", "delete_orphaned_files"):
        assert f"CALL ducklake_{fn}(" in code, f"{fn} must be invoked with CALL"


def test_lake_gc_scopes_the_orphan_scan_to_its_own_transaction() -> None:
    """Steps 1-2 share a transaction (cleanup only sees the expiry from inside
    it). Step 3 must open a NEW one: measured on 1.5.4, an orphan scan inside a
    transaction that opened earlier reports a file another session registered and
    COMMITTED in the meantime, so acting on it would unlink a live file whose
    catalog row survives. Asserting positions, not mere presence — a CALL that
    drifts across a COMMIT is exactly the regression this guards."""
    code = _lake_gc_code()
    assert code.count("BEGIN TRANSACTION;") == 2, "expected the 1-2 / 3 split"
    assert code.count("COMMIT;") == 2
    begin1 = code.index("BEGIN TRANSACTION;")
    commit1 = code.index("COMMIT;")
    begin2 = code.index("BEGIN TRANSACTION;", commit1)
    commit2 = code.index("COMMIT;", begin2)
    expire = code.index("CALL ducklake_expire_snapshots(")
    cleanup = code.index("CALL ducklake_cleanup_old_files(")
    orphans = code.index("CALL ducklake_delete_orphaned_files(")
    assert begin1 < expire < cleanup < commit1, "expire+cleanup belong to the first transaction"
    assert begin2 < orphans < commit2, "the orphan scan belongs to its own, later transaction"


def test_lake_gc_always_passes_older_than_explicitly() -> None:
    """Relying on the extension's default retention would let an upstream change
    silently move how much history this script destroys."""
    code = _lake_gc_code()
    pattern = r"ducklake_(?:expire_snapshots|cleanup_old_files|delete_orphaned_files)\([^;]*"
    calls = re.findall(pattern, code)
    assert len(calls) == 3, f"expected the three maintenance calls, found {len(calls)}"
    for call in calls:
        assert "older_than :=" in call, f"call without an explicit older_than: {call}"
        assert "dry_run :=" in call, f"call without an explicit dry_run: {call}"


def test_workflow_scripts_were_found() -> None:
    """Anti-vacuity guard: the parametrization below is a glob, so an empty or
    moved `workflows/` tree would leave it silently exercising nothing."""
    assert len(_WORKFLOW_SCRIPTS) >= 5


@pytest.mark.parametrize("path", _WORKFLOW_SCRIPTS, ids=lambda p: p.name)
def test_workflow_script_is_valid_bash(path: Path) -> None:
    """`bash -n` parses without executing — the container never gets that chance
    until a ticket is already running inside it."""
    result = subprocess.run(["bash", "-n", str(path)], capture_output=True, text=True)
    assert result.returncode == 0, f"bash -n failed for {path}:\n{result.stderr}"
