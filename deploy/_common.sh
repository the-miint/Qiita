#!/usr/bin/env bash
# Shared shell fragments for the deploy/*.sh scripts. Source via:
#   source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
# (every deploy script lives in this same directory, so resolving paths from
# THIS file's location is equivalent to resolving them from the caller's).
#
# Sourced by activate.sh, local-deploy.sh, redeploy.sh, preflight.sh, verify.sh,
# build-sifs.sh, and scripts/build-sif.sh (the last reaches over from scripts/ for
# the pure SIF-build helpers at the bottom — safe because sourcing has no side effects).
# Putting the shared pieces here so a change in one script does NOT silently
# drift from the others. Everything below is a definition (var or function) with
# no side effects, so sourcing under `set -euo pipefail` is safe and a caller
# can source it before its own logic runs.

# Rsync excludes used by every stage:
#   .venv/      — dev .venv in source tree must not overwrite the
#                 deployed venv (activate.sh's venv-python sanity
#                 check would fail if it did)
#   target/     — cargo build artifacts; the deployed data-plane
#                 binary lands via a separate `install` call
#   __pycache__/  — Python bytecode caches; harmless but noisy
#   build.env   — deploy-written build stamp under the control-plane
#                 rsync target; excluded so a `--delete` rsync never
#                 wipes it. activate.sh (re)writes it every deploy, so
#                 the write no longer has to be ordered after the rsync.
# shellcheck disable=SC2034  # consumed by the sourcing scripts (activate.sh, local-deploy.sh)
RSYNC_EXCLUDES=(--exclude='.venv/' --exclude='target/' --exclude='__pycache__/' --exclude='build.env')

# /etc/qiita service env-file paths. Overridable for tests / alternate layouts;
# every script that reads them gets the same definitions instead of redeclaring.
CP_ENV="${CP_ENV:-/etc/qiita/control-plane.env}"
DP_ENV="${DP_ENV:-/etc/qiita/data-plane.env}"
CO_ENV="${CO_ENV:-/etc/qiita/compute-orchestrator.env}"

# Service accounts the deploy scripts `sudo -u` into. Overridable for sites that
# named them differently (defaults match first-deploy.md §0.1). The operator /
# checkout-owner account is QIITA_USER (resolved by qiita_resolve_user_clone).
QIITA_API_USER="${QIITA_API_USER:-qiita-api}"
QIITA_ORCH_USER="${QIITA_ORCH_USER:-qiita-orch}"

# Abort unless running as root. $1 = a reason appended to the error so each
# caller keeps its own "why root is needed" message.
require_root() {
    [ "$EUID" -eq 0 ] || { echo "ERROR: ${1:-must be run as root (sudo).}" >&2; exit 1; }
}

# Resolve + validate the operator account and git clone the build-path scripts
# (local-deploy.sh, redeploy.sh) share. Sets QIITA_USER (default qiita) and
# QIITA_CLONE (default: the repo root above this deploy/ dir), aborting if the
# account or the .git clone is missing. NB: QIITA_CLONE is derived from THIS
# file's location (deploy/_common.sh → repo root); since every deploy script is
# co-located here, that matches resolving from the caller.
qiita_resolve_user_clone() {
    QIITA_USER="${QIITA_USER:-qiita}"
    QIITA_CLONE="${QIITA_CLONE:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
    id "$QIITA_USER" >/dev/null 2>&1 || { echo "ERROR: operator account '$QIITA_USER' not found" >&2; exit 1; }
    [ -d "$QIITA_CLONE/.git" ] || { echo "ERROR: $QIITA_CLONE is not a git clone" >&2; exit 1; }
}

# Resolve the SLURM native-venv checkout from SLURM_NATIVE_PYTHON.
#
# Native SLURM jobs run from the venv SLURM_NATIVE_PYTHON points at — a separate
# checkout on the shared filesystem, NOT /opt/qiita. redeploy.sh (step 5) refreshes
# that venv after a deploy; this helper turns the configured python path into the
# `qiita-compute-orchestrator` checkout dir to `uv sync` in, and fails loud rather
# than ever syncing a wrong path. Pure (echo + return only) so redeploy.sh and the
# unit test in test_deploy_scripts.py can both call it.
#
# $1 = SLURM_NATIVE_PYTHON value. On success: echoes the checkout dir, returns 0.
# Returns 1 (a SKIP signal — caller degrades like the miint stage) when $1 is empty
# or the bare "python" (PATH-based local backend — no checkout to derive). Returns 2
# (a hard FAIL — caller must abort) with a stderr reason when $1 points somewhere
# that isn't the expected `<repo>/qiita-compute-orchestrator/.venv/bin/python`.
qiita_native_checkout_from_python() {
    local native_python="${1:-}"
    # Empty or PATH-based ("python") → nothing to derive; signal skip, not fail.
    [ -z "$native_python" ] && return 1
    [ "$native_python" = "python" ] && return 1
    # .venv/bin/python → up three dirnames is the qiita-compute-orchestrator dir.
    local checkout
    checkout=$(cd "$(dirname "$native_python")/../.." 2>/dev/null && pwd) || {
        echo "ERROR: cannot resolve native checkout from SLURM_NATIVE_PYTHON='$native_python'" >&2
        return 2
    }
    # Fail loud unless this really is the orchestrator checkout: named
    # qiita-compute-orchestrator, has a pyproject.toml, and sits under a git clone.
    if [ "$(basename "$checkout")" != "qiita-compute-orchestrator" ]; then
        echo "ERROR: derived native dir '$checkout' is not named qiita-compute-orchestrator" >&2
        echo "       (SLURM_NATIVE_PYTHON should be <checkout>/qiita-compute-orchestrator/.venv/bin/python)" >&2
        return 2
    fi
    if [ ! -f "$checkout/pyproject.toml" ]; then
        echo "ERROR: derived native dir '$checkout' has no pyproject.toml — not a checkout" >&2
        return 2
    fi
    if [ ! -d "$checkout/../.git" ]; then
        echo "ERROR: derived native checkout '$checkout' is not inside a git clone (no ../.git)" >&2
        return 2
    fi
    printf '%s' "$checkout"
}

# Does a list of changed paths touch a package a native SLURM venv runs
# (qiita-common or qiita-compute-orchestrator)? redeploy.sh feeds this the
# `git diff --name-only <before> <after>` of a pull to decide whether the native
# venv needs a refresh — the path-prefix match is the part worth unit-testing
# (e.g. it must match `qiita-common/...` but NOT a sibling like
# `qiita-common-extra/...`), so it lives here as a pure function while the git +
# sudo wiring stays in redeploy.sh. Pure (no side effects); the unit test in
# test_deploy_scripts.py exercises the matching directly.
#
# $1 = newline-separated path list. Returns 0 when at least one path is under
# qiita-common/ or qiita-compute-orchestrator/, 1 when none are (incl. empty).
qiita_paths_touch_native() {
    printf '%s\n' "${1:-}" | grep -qE '^(qiita-common|qiita-compute-orchestrator)/'
}

# Does a list of changed paths touch a package the operator's CHECKOUT CLI venv
# runs? Operators invoke `uv run qiita` / `qiita-admin` from the checkout's
# qiita-control-plane venv, which imports qiita_control_plane (and the path-dep
# qiita_common). redeploy.sh feeds this the same `git diff --name-only` of a pull
# it feeds qiita_paths_touch_native, to decide whether that CLI venv needs a
# `--reinstall-package qiita-common` refresh. Pure (no side effects); the unit
# test in test_deploy_scripts.py exercises the matching directly. NB: qiita-data-
# plane / qiita-compute-orchestrator are deliberately NOT here — they don't change
# what the control-plane CLI venv imports.
#
# $1 = newline-separated path list. Returns 0 when at least one path is under
# qiita-common/ or qiita-control-plane/, 1 when none are (incl. empty).
qiita_paths_touch_cli() {
    printf '%s\n' "${1:-}" | grep -qE '^(qiita-common|qiita-control-plane)/'
}

# --- SIF auto-build helpers (used by scripts/build-sif.sh + deploy/build-sifs.sh) ---
# Pure (echo/return only), so test_deploy_scripts.py exercises them directly while
# the apptainer/root/chown wiring stays in the entrypoint scripts. This is why the
# header says _common.sh is sourced by build-sif.sh too — these definitions have no
# side effects on source.

# sha256 of stdin, hex digest only. Prefers sha256sum (Linux deploy host); falls
# back to `shasum -a 256` on a macOS dev/test box. Internal to the hash below.
_qiita_sha256() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum | cut -d' ' -f1
    else shasum -a 256 | cut -d' ' -f1; fi
}

# Content hash of a container workflow's IN-REPO build inputs, used by
# build-sif.sh's idempotency check to detect a changed Apptainer.def /
# entrypoint.sh / manifest_writer.py — none of which VERIFY_MATCH (binary version
# only) can see, so such an edit would otherwise be skipped and never reach the
# host, forcing a manual FORCE=1. Hashes every file under the workflow dir (minus
# the spec, gitignore, and generated .sif/.rpm) plus _shared/, keyed by
# REPO-RELATIVE path so the digest is identical from the operator clone or an
# INCOMING stage. Deliberately EXCLUDES the vendored SOURCES (the licensed RPM):
# re-vendoring 4.5.4-1 → 4.5.4-2 must NOT force a rebuild, matching VERIFY_MATCH's
# intentionally-loose patch component.
#
# An empty input set would hash to a fixed "no inputs" digest (and so spuriously
# MATCH a prior stamp), but that can't happen on the real path: build-sif.sh
# requires Apptainer.def to exist before it calls this, so the workflow dir is
# always non-empty.
#
# All work runs in a subshell that first cd's to / — `find` restores its initial
# working directory when it finishes, and if that cwd is unreadable by the
# invoking user (e.g. a manual `sudo -u qiita-orch …` launched from an admin's
# 0700 home), GNU find exits non-zero with "Failed to restore initial working
# directory", which would break the `set -o pipefail` pipeline and abort the
# build. / is always traversable, and every path used here is absolute, so the cd
# is safe; the subshell keeps it from leaking into the caller's cwd.
# $1 = repo root, $2 = workflow dir, $3 = shared dir. Echoes the hex digest.
qiita_sif_build_inputs_hash() {
    local repo_root="$1" workflow_dir="$2" shared_dir="$3"
    (
        cd / || exit 1
        {
            find "$workflow_dir" -type f \
                ! -name sif-build.env ! -name '.gitignore' \
                ! -name '*.sif' ! -name '*.rpm' ! -path '*/__pycache__/*'
            find "$shared_dir" -type f ! -path '*/__pycache__/*'
        } | LC_ALL=C sort | while IFS= read -r f; do
            printf '%s ' "${f#"$repo_root"/}"   # repo-relative path → location-independent
            _qiita_sha256 < "$f"
        done | _qiita_sha256
    )
}

# Scoped variant of the hash above for a MULTI-IMAGE workflow (one that ships
# several per-tool images from a `sif-build.d/` — see build-sif.sh). Instead of
# hashing the whole workflow dir (which would make a change to ANY tool's
# def/entrypoint rebuild EVERY image), this hashes only the EXPLICIT files an
# image declares (its own def + entrypoint(s), via the spec's HASH_INPUTS) plus
# _shared/. That is what lets, e.g., an edit to the checkm image's def rebuild
# only long-read-assembly-checkm and leave long-read-assembly-assemble alone — the granularity the
# per-tool split exists to deliver. Same digest shape as the whole-dir hash
# (repo-relative path + sha256 per file, sorted, re-hashed) so a legacy single
# image and a scoped image are computed identically; only the input SET differs.
#
# $1 = repo root, $2 = shared dir, then N absolute file paths (the image's
# declared build inputs, each already validated to exist by the caller). Echoes
# the hex digest. The same cd-to-/ safety note as qiita_sif_build_inputs_hash
# applies (find restoring an unreadable cwd), so the work runs in a subshell.
qiita_sif_build_inputs_hash_scoped() {
    local repo_root="$1" shared_dir="$2"
    shift 2
    (
        cd / || exit 1
        {
            printf '%s\n' "$@"
            find "$shared_dir" -type f ! -path '*/__pycache__/*'
        } | LC_ALL=C sort | while IFS= read -r f; do
            printf '%s ' "${f#"$repo_root"/}"   # repo-relative path → location-independent
            _qiita_sha256 < "$f"
        done | _qiita_sha256
    )
}

# Which of a workflow's vendored SOURCES are NOT staged under the images/sources
# dir? build-sifs.sh uses this to SKIP (not fail) an image whose licensed artifact
# the operator hasn't placed out of band. $1 = sources dir, $2 = space-separated
# SOURCES list. Echoes each missing filename (one per line); returns 0 when all
# are present, 1 when any are missing. An empty SOURCES list → nothing missing → 0.
qiita_sif_missing_sources() {
    local sources_dir="$1" sources="$2" src rc=0
    for src in $sources; do
        [ -f "$sources_dir/$src" ] || { printf '%s\n' "$src"; rc=1; }
    done
    return $rc
}

# Read one KEY from an env file in a clean subshell. `set +eu` so a value that
# references another (unset) var doesn't abort under errexit/nounset and silently
# blank this and every later var; `set -a` exports the `KEY=val` lines into the
# subshell; printf the requested var. bash strips the `KEY=...` quoting, so the
# returned value matches what the service's own loader sees. The subshell
# contains the `set -a` pollution.
read_env_var() {
    local env_file="$1" var="$2"
    # shellcheck disable=SC1090,SC1091
    ( set +eu; set -a; source "$env_file" >/dev/null 2>&1; set +a; printf '%s' "${!var:-}" )
}

# Extract the Env-vars + one-time-host-setup buckets (buckets 1 & 2) from a
# DEPLOY_CHECKLIST.md and judge whether they are EMPTY. redeploy.sh uses this to
# skip the "have buckets 1 & 2 been applied?" acknowledgement when there is
# literally nothing to apply — the deploy stops only when the operator actually
# has out-of-band steps to run. Pure (echo + return only) so the unit test in
# test_deploy_scripts.py can exercise the emptiness logic directly.
#
# $1 = path to DEPLOY_CHECKLIST.md. Echoes the bucket 1+2 text to stdout, and:
#   returns 0 — buckets are EMPTY (only headers + "_None yet._" placeholders);
#               caller may skip the prompt.
#   returns 1 — buckets carry real steps; caller must print them and prompt.
#   returns 2 — checklist unreadable or the bucket markers weren't found; caller
#               can't judge, so it should fall back to prompting (fail safe).
qiita_buckets_12() {
    local checklist="$1" text substantive
    [ -r "$checklist" ] || return 2
    # CONTRACT: the literal bucket headers "### 1. Env vars" and "### 3. Migrations"
    # are the boundary markers. If DEPLOY_CHECKLIST.md ever renames or reorders
    # these, the range below finds nothing and the function returns 2 — i.e. the
    # caller falls back to PROMPTING, never to silently skipping a real ack. The
    # real-file test in test_deploy_scripts.py pins these markers to the live file.
    # From "### 1. Env vars" through the line before "### 3. Migrations" (drop the
    # trailing migrations header that the range pattern includes).
    text=$(sed -n '/^### 1\. Env vars/,/^### 3\. Migrations/p' "$checklist" | sed '$d')
    [ -n "$text" ] || return 2  # markers absent → can't judge; let the caller prompt
    printf '%s' "$text"
    # Substantive = any line that is not blank, not a "### " header, and not the
    # "_None yet._" placeholder. None left → the buckets hold no real steps.
    substantive=$(printf '%s\n' "$text" | grep -vE '^[[:space:]]*$|^### |^_None yet\._[[:space:]]*$' || true)
    [ -z "$substantive" ]
}

# Pass/fail/skip row printers + counters for the read-only check scripts
# (preflight.sh, verify.sh). The caller initialises `n_pass=0 n_fail=0 n_skip=0`
# (so the trailing summary + `[ "$n_fail" -eq 0 ]` are nounset-safe even when no
# check ran) and these increment them. The byte-escapes are ✓ / ✗ / · in UTF-8.
pass() { printf '  \xe2\x9c\x93 %s: %s\n' "$1" "$2"; n_pass=$((${n_pass:-0} + 1)); }
fail() { printf '  \xe2\x9c\x97 %s: %s\n' "$1" "$2"; n_fail=$((${n_fail:-0} + 1)); }
skip() { printf '  \xc2\xb7 %s: %s\n' "$1" "$2"; n_skip=$((${n_skip:-0} + 1)); }
