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

# sha256 of stdin, hex digest only. Prefers sha256sum (Linux deploy host); falls
# back to `shasum -a 256` on a macOS dev/test box. Used by
# qiita_deploy_self_fingerprint below and qiita_sif_build_inputs_hash further down.
_qiita_sha256() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum | cut -d' ' -f1
    else shasum -a 256 | cut -d' ' -f1; fi
}

# Fingerprint the bytes a running deploy script has already read into its own
# process: the script itself plus this file, which it sourced. redeploy.sh takes
# it either side of its `git pull` to notice that the pull rewrote the script
# under the running shell (see redeploy.sh step 1).
#
# $1 = path to the running script. Echoes a hex digest; returns 1 with a stderr
# reason if either file is unreadable, so the caller aborts rather than treating
# an unreadable file as a change.
qiita_deploy_self_fingerprint() {
    local script="${1:?qiita_deploy_self_fingerprint needs the path of the running script}"
    local common
    common="$(dirname "$script")/_common.sh"
    [ -r "$script" ] || { echo "ERROR: cannot read $script to fingerprint it" >&2; return 1; }
    [ -r "$common" ] || { echo "ERROR: cannot read $common to fingerprint it" >&2; return 1; }
    cat "$script" "$common" | _qiita_sha256
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

# --- SIF auto-build helpers (used by scripts/build-sif.sh + deploy/build-sifs.sh) ---
# Pure (echo/return only), so test_deploy_scripts.py exercises them directly while
# the apptainer/root/chown wiring stays in the entrypoint scripts. This is why the
# header says _common.sh is sourced by build-sif.sh too — these definitions have no
# side effects on source.

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

# Split the password out of a libpq connection string — key=value form
# ("host=… user=… password=…") or postgres:// URI form — so a caller can hand it
# to libpq through PGPASSFILE instead of leaving it in a SQL file or, worse, on a
# command line (argv is world-readable via /proc).
#
# $1 = the connection string. Echoes three TAB-separated fields, newline-terminated:
#   <connstr with the password removed>\t<username>\t<password>
#
# The password field comes back EMPTY, with the connstr unchanged, when it cannot
# be lifted out safely: no username to key a pgpass entry on, or a percent-encoded
# URI password that would have to be decoded first. The caller must then use the
# string as-is rather than authenticate with a wrong value — silently sending the
# still-encoded password would fail as an auth error far from its cause.
#
# Pure (echo + return only) so scripts/lake-shell.sh and the unit tests in
# test_deploy_scripts.py can both call it.
qiita_split_conn_password() {
    local connstr="$1" sanitized="$1" user="" password=""
    if [[ "$connstr" =~ ^postgres(ql)?:// ]]; then
        # postgres[ql]://user[:password]@host…  — a literal ':' '@' or '/' inside
        # either credential has to be percent-encoded, so the character classes
        # below cannot run past the credential they are matching.
        if [[ "$connstr" =~ ^(postgres(ql)?://)([^:@/]+):([^@/]+)@(.*)$ ]]; then
            user="${BASH_REMATCH[3]}"
            password="${BASH_REMATCH[4]}"
            sanitized="${BASH_REMATCH[1]}${user}@${BASH_REMATCH[5]}"
        elif [[ "$connstr" =~ ^(postgres(ql)?://)([^:@/]+)@ ]]; then
            user="${BASH_REMATCH[3]}"
        fi
        if [[ "$password" == *%* ]]; then
            sanitized="$connstr"
            password=""
        fi
    else
        if [[ "$connstr" =~ (^|[[:space:]])user=([^[:space:]]+) ]]; then
            user="${BASH_REMATCH[2]}"
        fi
        if [[ "$connstr" =~ (^|[[:space:]])password=([^[:space:]]+) ]]; then
            password="${BASH_REMATCH[2]}"
            sanitized="$(printf '%s' "$connstr" \
                | sed -E 's/(^|[[:space:]])password=[^[:space:]]+/\1/g' \
                | tr -s '[:space:]' ' ' | sed -E 's/^ +| +$//g')"
        fi
    fi
    if [[ -n "$password" && -z "$user" ]]; then
        sanitized="$connstr"
        password=""
    fi
    printf '%s\t%s\t%s\n' "$sanitized" "$user" "$password"
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

# ATTACH takes no bind parameters, so connection strings and paths are
# interpolated into SQL. Reject the same characters
# qiita-data-plane/src/ducklake.rs validate_sql_literal does — this is input
# validation, not sanitization. Exits non-zero rather than returning, because
# every caller's next act is to interpolate the value it just checked.
# $1 = value, $2 = label used in the error.
reject_sql_metacharacters() {
    local value="$1" label="$2"
    case "${value}" in
        *\'*|*\;*)
            echo "ERROR: ${label} contains a quote or semicolon; refusing to interpolate it into SQL." >&2
            exit 1
            ;;
    esac
}

# The lake data path, byte-identical to the data plane's derivation — config.rs
# does a bare format!("{path_persistent_raw}/ducklake") with NO normalization,
# and DuckLake pins that exact string into the catalog at creation. Do not
# "tidy" a trailing slash off the input: on a host with PATH_PERSISTENT=/data/
# the catalog holds "/data//ducklake", and the tidied "/data/ducklake" is
# rejected outright with `DATA_PATH parameter ... does not match existing data
# path in the catalog`. $1 = PATH_PERSISTENT.
qiita_lake_data_path() { printf '%s/ducklake' "$1"; }

# --- DuckDB CLI + pgpass plumbing, shared by scripts/lake-*.sh ---------------

# The DuckDB CLI must match the version the data plane links (duckdb crate
# 1.10504.0 == DuckDB 1.5.4): the ducklake extension is versioned with DuckDB,
# and a newer one may want to migrate the catalog schema it opens.
QIITA_DUCKDB_VERSION="1.5.4"

# Resolve the duckdb CLI into DUCKDB_BIN, or exit with install instructions.
# Two install sites because the callers run as different accounts: a human with
# a home, or a service account (qiita-data) whose home is /dev/null.
qiita_resolve_duckdb_bin() {
    DUCKDB_BIN="${QIITA_DUCKDB_BIN:-$(command -v duckdb || true)}"
    [ -n "${DUCKDB_BIN}" ] && return 0
    cat >&2 <<EOF
ERROR: no duckdb CLI on PATH.

Install v${QIITA_DUCKDB_VERSION} — it must match what the data plane links.

  cd "\$(mktemp -d)" \\
    && curl -sSfL -O https://github.com/duckdb/duckdb/releases/download/v${QIITA_DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip \\
    && unzip -q duckdb_cli-linux-amd64.zip

Then, for your own account (no root needed):
    mkdir -p ~/.local/bin && install -m 0755 duckdb ~/.local/bin/duckdb
Or, to reach it from a service account with no home (e.g. qiita-data):
    sudo install -m 0755 duckdb /usr/local/bin/duckdb

Then re-run this script (or point QIITA_DUCKDB_BIN at the binary).
EOF
    exit 1
}

# Print a script's own header comment block as its usage text — one copy, not
# two. $1 = the script file (pass "${BASH_SOURCE[0]}").
qiita_usage_from_header() {
    awk 'NR>1 && /^#/ { sub(/^# ?/, ""); print; next } NR>1 { exit }' "$1"
}

# A 0700 temp dir holding an empty 0600 pgpass, removed on exit. The file is
# created at 0600 BEFORE anything is written — a redirection would otherwise
# create it at the umask's mode and only narrow it afterwards. Traps the signals
# a dropped session actually sends, not just EXIT, so a pgpass never outlives
# the shell. Sets TMPROOT and PGPASS_FILE. $1 = temp-dir name prefix.
qiita_pgpass_init() {
    TMPROOT="$(mktemp -d "${TMPDIR:-/tmp}/$1.XXXXXX")"
    chmod 700 "${TMPROOT}"
    # shellcheck disable=SC2064  # expand TMPROOT now: it must not be re-read at trap time
    trap "rm -rf '${TMPROOT}'" EXIT INT TERM HUP
    PGPASS_FILE="${TMPROOT}/pgpass"
    : > "${PGPASS_FILE}"; chmod 600 "${PGPASS_FILE}"
}

# Append one entry to the pgpass. Lines are `host:port:database:user:password`
# with `:` and `\` escaped. Entries are keyed on the username alone (wildcard
# host/port/db), which is unambiguous as long as two connections do not share
# one — and if they do share a username with different passwords, error rather
# than let the first line win.
#
# The seen-set is two parallel indexed arrays and a linear scan rather than an
# associative array, because macOS ships bash 3.2 — which has none, and which
# `make test` exercises these scripts under on the mac CI runner. The `_COUNT`
# scalar bounds the scan instead of `${#array[@]}`: under `set -u`, bash 3.2
# treats an empty array as unset. Passwords may hold any byte, so a single
# delimited-string map would need escaping that a scan does not.
QIITA_PGPASS_SEEN_COUNT=0
QIITA_PGPASS_SEEN_USERS=()
QIITA_PGPASS_SEEN_PASSWORDS=()
qiita_pgpass_add() {
    local user="$1" password="$2" escaped_user escaped_password i
    for ((i = 0; i < QIITA_PGPASS_SEEN_COUNT; i++)); do
        [[ "${QIITA_PGPASS_SEEN_USERS[i]}" == "${user}" ]] || continue
        if [[ "${QIITA_PGPASS_SEEN_PASSWORDS[i]}" != "${password}" ]]; then
            echo "ERROR: two connections both connect as '${user}' with different" >&2
            echo "  passwords, so they cannot be keyed apart in a pgpass file." >&2
            echo "  Give one of them its own role (lake-shell.sh: or re-run --no-cp)." >&2
            exit 1
        fi
        return 0
    done
    QIITA_PGPASS_SEEN_USERS[QIITA_PGPASS_SEEN_COUNT]="${user}"
    QIITA_PGPASS_SEEN_PASSWORDS[QIITA_PGPASS_SEEN_COUNT]="${password}"
    QIITA_PGPASS_SEEN_COUNT=$((QIITA_PGPASS_SEEN_COUNT + 1))
    escaped_user="${user//\\/\\\\}"; escaped_user="${escaped_user//:/\\:}"
    escaped_password="${password//\\/\\\\}"; escaped_password="${escaped_password//:/\\:}"
    printf '*:*:*:%s:%s\n' "${escaped_user}" "${escaped_password}" >> "${PGPASS_FILE}"
}

# Every duckdb invocation goes through here so the PGPASSFILE handling exists in
# exactly one place. The file is only handed over when it actually holds an
# entry — libpq ignores an empty one anyway, but not setting the variable keeps
# a password-less deployment obviously password-less.
qiita_run_duckdb() {
    if [[ -s "${PGPASS_FILE}" ]]; then
        PGPASSFILE="${PGPASS_FILE}" "${DUCKDB_BIN}" "$@"
    else
        "${DUCKDB_BIN}" "$@"
    fi
}

# The lake data path must be a directory this account can traverse and read.
# $1 = the path. Callers needing to WRITE it check that themselves.
qiita_require_lake_data_path() {
    local path="$1"
    [ -d "${path}" ] || { echo "ERROR: lake data path ${path} is not a directory" >&2; exit 1; }
    if [ ! -r "${path}" ] || [ ! -x "${path}" ]; then
        echo "ERROR: cannot read ${path}" >&2
        echo "  It is mode 0750 qiita-data:qiita-data — you must be in the owning group." >&2
        echo "  Check with: ls -ld ${path} && id" >&2
        exit 1
    fi
}
