#!/usr/bin/env bash
# Shared shell fragments for the deploy/*.sh scripts. Source via:
#   source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
# (every deploy script lives in this same directory, so resolving paths from
# THIS file's location is equivalent to resolving them from the caller's).
#
# Sourced by activate.sh, local-deploy.sh, redeploy.sh, preflight.sh, verify.sh.
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

# Pass/fail/skip row printers + counters for the read-only check scripts
# (preflight.sh, verify.sh). The caller initialises `n_pass=0 n_fail=0 n_skip=0`
# (so the trailing summary + `[ "$n_fail" -eq 0 ]` are nounset-safe even when no
# check ran) and these increment them. The byte-escapes are ✓ / ✗ / · in UTF-8.
pass() { printf '  \xe2\x9c\x93 %s: %s\n' "$1" "$2"; n_pass=$((${n_pass:-0} + 1)); }
fail() { printf '  \xe2\x9c\x97 %s: %s\n' "$1" "$2"; n_fail=$((${n_fail:-0} + 1)); }
skip() { printf '  \xc2\xb7 %s: %s\n' "$1" "$2"; n_skip=$((${n_skip:-0} + 1)); }
