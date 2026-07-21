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
import re
import shutil
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEPLOY = _REPO_ROOT / "deploy"
_COMMON = _DEPLOY / "_common.sh"
_BUILD_SIF = _REPO_ROOT / "scripts" / "build-sif.sh"

# The scripts introduced/maintained for the deploy-ease work. Kept
# explicit (not a glob) so a new deploy script is a deliberate add here.
# build-sifs.sh is the deploy-time SIF auto-builder (wraps scripts/build-sif.sh).
_SCRIPTS = ("preflight.sh", "verify.sh", "redeploy.sh", "build-sifs.sh")
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


def _call_data_plane_ports(value: str | None) -> subprocess.CompletedProcess[str]:
    """Source _common.sh and invoke qiita_data_plane_ports with QIITA_DATA_PLANE_PORTS
    set to `value` (unset when None). Returns the CompletedProcess."""
    env = {**os.environ}
    if value is None:
        env.pop("QIITA_DATA_PLANE_PORTS", None)
    else:
        env["QIITA_DATA_PLANE_PORTS"] = value
    return subprocess.run(
        ["bash", "-c", f'source "{_COMMON}"; qiita_data_plane_ports'],
        capture_output=True,
        text=True,
        env=env,
    )


def test_data_plane_ports_defaults_to_the_single_instance() -> None:
    """Unset ⇒ the one instance every deploy has had. Scaling is opt-in."""
    result = _call_data_plane_ports(None)
    assert result.returncode == 0, result.stderr
    assert result.stdout == "50051"


def test_data_plane_ports_passes_through_a_scaled_list() -> None:
    result = _call_data_plane_ports("50051 50052 50053")
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == ["50051", "50052", "50053"]


@pytest.mark.parametrize(
    "value",
    [
        "50051 abc",  # non-numeric entry
        "0",  # not a valid TCP port
        "99999",  # out of range
        "-1",  # negative
        "50051; rm -rf /",  # the value reaches a systemd unit name + nginx config
        "   ",  # blank
    ],
)
def test_data_plane_ports_rejects_malformed_values(value: str) -> None:
    """A bad entry must abort the deploy, not render broken config.

    This value becomes a systemd unit name (`qiita-data-plane@<port>`), an nginx
    `server 127.0.0.1:<port>;` line, and a health-check target, so validating it
    once here is what keeps three downstream consumers honest.
    """
    result = _call_data_plane_ports(value)
    assert result.returncode != 0, f"{value!r} should be rejected, got {result.stdout!r}"
    assert result.stdout == ""


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


# --- qiita_paths_touch_cli: the pure path-prefix predicate behind redeploy.sh's
# "did this pull touch a package the operator's checkout CLI venv runs?" decision.
# rc 0 = a path is under qiita-common/ or qiita-control-plane/, 1 = none are. -----


def _call_paths_touch_cli(paths: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-c", f'source "{_COMMON}"; qiita_paths_touch_cli "$1"', "_", paths],
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize(
    "paths",
    [
        "qiita-common/src/qiita_common/api_paths.py\nother/x",
        "qiita-control-plane/src/qiita_control_plane/cli/user.py",
        "docs/a.md\nqiita-control-plane/f",  # CLI path is not the first line
    ],
)
def test_paths_touch_cli_matches(paths: str) -> None:
    """Any path under qiita-common or qiita-control-plane → rc 0 (refresh needed)."""
    assert _call_paths_touch_cli(paths).returncode == 0


@pytest.mark.parametrize(
    "paths",
    [
        # The native packages alone do NOT change what the CLI venv imports.
        "docs/a.md\nqiita-compute-orchestrator/pyproject.toml\nqiita-data-plane/src/main.rs",
        "qiita-control-plane-extra/z.py",  # sibling prefix must NOT match
        "",  # empty diff → nothing touched
    ],
)
def test_paths_touch_cli_no_match(paths: str) -> None:
    """No path under the CLI packages (incl. a sibling-prefix dir or an empty
    list) → rc 1 (no refresh needed)."""
    assert _call_paths_touch_cli(paths).returncode == 1


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
