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
# back to `shasum -a 256` on a macOS dev/test box.
_qiita_sha256() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum | cut -d' ' -f1
    else shasum -a 256 | cut -d' ' -f1; fi
}

# Absolute path to THIS file, resolved as it is sourced. The two deploy-freshness
# helpers below hash it as the file the sourcing process actually read, rather
# than re-deriving it from the caller's location or a later cwd.
QIITA_COMMON_SH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"

# Fingerprint the bytes a running deploy script has already read into its own
# process: the script itself, plus this file, which it sourced. redeploy.sh takes
# it either side of its `git pull` (see redeploy.sh step 1).
#
# Same digest shape as qiita_sif_build_inputs_hash below — name plus per-file
# sha256, then a hash over that stream — so bytes moving from one file to the
# other cannot leave the digest unchanged, as hashing their concatenation would.
#
# $1 = path to the running script. Echoes a hex digest; returns 1 with a stderr
# reason if either file is unreadable, so the caller aborts rather than reading an
# empty digest as a change.
qiita_deploy_self_fingerprint() {
    local script="${1:?qiita_deploy_self_fingerprint needs the path of the running script}"
    local f
    for f in "$script" "$QIITA_COMMON_SH"; do
        [ -r "$f" ] || { echo "ERROR: cannot read $f to fingerprint it" >&2; return 1; }
    done
    for f in "$script" "$QIITA_COMMON_SH"; do
        printf '%s ' "${f##*/}"
        _qiita_sha256 < "$f"
    done | _qiita_sha256
}

# Re-exec the running deploy script when the pull replaced it.
#
# $1 = the running script (absolute), $2 = its fingerprint from before the pull.
# Returns 0 when the two match and the caller carries on. When they differ, execs
# the pulled copy — replacing this process, so nothing after the call runs.
#
# Returns 1 when the fingerprint cannot be taken, and when it changed again after
# a re-exec already happened: a second re-exec is how this loops, and the caller
# reaches here before anything is deployed, so aborting costs a re-run.
qiita_deploy_reexec_if_changed() {
    local script="${1:?qiita_deploy_reexec_if_changed needs the path of the running script}"
    local before="${2:?qiita_deploy_reexec_if_changed needs the pre-pull fingerprint}"
    local after
    after=$(qiita_deploy_self_fingerprint "$script") || return 1
    [ "$after" = "$before" ] && return 0
    if [ -n "${QIITA_REDEPLOY_REEXECED:-}" ]; then
        echo "ERROR: $script or $QIITA_COMMON_SH changed again after the re-exec." >&2
        echo "       Nothing has been deployed at this point; re-run once the clone" >&2
        echo "       has settled." >&2
        return 1
    fi
    echo "The pull changed ${script##*/} or ${QIITA_COMMON_SH##*/} — re-execing the"
    echo "pulled copy so the rest of the deploy runs the code that was just pulled."
    echo "The banner and step 1 repeat below; the second pull is a no-op."
    export QIITA_REDEPLOY_REEXECED=1
    exec bash "$script"
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
# entrypoint.sh / manifest_writer.py — none of which VERIFY_MATCH (a regex over
# VERIFY_CMD's output) can see, so such an edit would otherwise be skipped and
# never reach the host, forcing a manual FORCE=1. Hashes every file under the
# workflow dir (minus the spec, gitignore, and generated .sif/.rpm) plus _shared/, keyed by
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

# read_env_var, blind to the caller's environment. The subshell that sources the
# file inherits it, so read_env_var returns a KEY exported by whoever runs the script
# when the file does not assign it. $1 = env file, $2 = KEY.
read_env_file_var() {
    ( unset "$2"; read_env_var "$1" "$2" )
}

# Does the env file itself assign KEY, even to an empty value? read_env_var cannot
# tell "unset" from "set to empty"; a caller that must reject an empty assignment
# asks this first. KEY in the caller's environment does not count. $1 = env file,
# $2 = KEY.
env_file_sets_var() {
    local env_file="$1" var="$2"
    # shellcheck disable=SC1090,SC1091
    ( unset "$var"; set +eu; set -a; source "$env_file" >/dev/null 2>&1; set +a; [ -n "${!var+x}" ] )
}

# --- Data-plane topology ------------------------------------------------------
# Which data-plane instances this host runs, which other hosts' data planes nginx
# also balances to, and which address the local instances listen on are host
# settings in $DP_ENV:
#
#   QIITA_DATA_PLANE_PORTS      this host's instances, e.g. "50051 50052". Each port
#                               is the instance specifier of qiita-data-plane@.service.
#                               Default 50051.
#   QIITA_DATA_PLANE_PEERS      data planes on other hosts, as host:port. Default none.
#   QIITA_DATA_PLANE_BIND_HOST  the address this host's instances listen on. Default
#                               127.0.0.1.
#
# activate.sh renders the nginx upstream from them and enables/restarts one unit per
# port on every deploy; verify.sh health-checks the rendered members. Because they
# live in the file, a routine `make redeploy` reproduces the topology the host was
# given. They are read from the file only, never the deploy's environment, so what
# a deploy renders and enables is what the file says.
# docs/runbooks/data-plane-scaling.md is the operator procedure.

# The address and port of the loopback listener nginx serves the qiita_data_plane
# upstream on for on-host clients.
QIITA_DATA_PLANE_LB_HOST=127.0.0.1
QIITA_DATA_PLANE_LB_PORT=50050

# The commit this host's data planes were deployed from, as BUILD_COMMIT (activate.sh
# writes it; the qiita-data-plane@ units read it), and the health service name a
# data plane running that commit answers SERVING for (BUILD_SERVICE_PREFIX in
# qiita-data-plane/src/main.rs). verify.sh asks every upstream member for it, so a
# member deployed from another commit, or not restarted, fails.
QIITA_DATA_PLANE_BUILD_ENV=/opt/qiita/data-plane/build.env
# QIITA_BUILD_SHA, as the BUILD_COMMIT activate.sh writes: empty when it is unset, an
# error when it is not a full commit, on which a data plane refuses to boot
# (build_service_name in qiita-data-plane/src/main.rs).
qiita_data_plane_build_commit() {
    local sha=${QIITA_BUILD_SHA:-}
    if [ -n "$sha" ] && ! [[ $sha =~ ^[0-9a-f]{40}$ ]]; then
        echo "ERROR: QIITA_BUILD_SHA='$sha' is not a full 40-character lowercase commit sha, so the data planes would not boot on it" >&2
        return 1
    fi
    printf '%s' "$sha"
}
qiita_data_plane_build_service() {
    printf 'qiita-data-plane/build/%s' "$1"
}

# Succeeds when $DP_ENV is absent or unreadable (the defaults apply) or parses as
# shell. read_env_var discards the file's errors, so without this an unclosed quote
# would read every topology key as unset and deploy the default topology.
qiita_dp_env_parses() {
    [ -r "$DP_ENV" ] || return 0
    bash -n "$DP_ENV" 2>/dev/null && return 0
    echo "ERROR: $DP_ENV does not parse as shell (bash -n), so the data-plane topology cannot be read from it" >&2
    return 1
}

# Is $1 a plain TCP port (1-65535), written without a leading zero? $2 is a
# caller-supplied label used verbatim in the error. A leading zero is refused
# because `050051` and `50051` would name two systemd units for one port.
qiita_valid_tcp_port() {
    local port="$1" label="$2"
    case "$port" in
        ''|*[!0-9]*)
            echo "ERROR: $label is not a number" >&2
            return 1
            ;;
        0*)
            echo "ERROR: $label is not a valid TCP port (1-65535, no leading zero)" >&2
            return 1
            ;;
    esac
    if [ "$port" -gt 65535 ]; then
        echo "ERROR: $label is not a valid TCP port (1-65535, no leading zero)" >&2
        return 1
    fi
}

# Is $1 exactly four decimal octets, each 0-255 and written without a leading zero?
# A leading zero is refused because glibc's inet_aton reads `010` as octal 8, so
# nginx and the data plane could disagree about which address `010.0.0.1` names.
qiita_is_ipv4_literal() {
    local addr="$1" o1 o2 o3 o4 o
    # Exactly four dot-separated runs of digits, nothing before or after.
    [[ "$addr" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || return 1
    IFS=. read -r o1 o2 o3 o4 <<<"$addr"
    for o in "$o1" "$o2" "$o3" "$o4"; do
        case "$o" in
            0?*) return 1 ;;
        esac
        # Length first: `[` prints "integer expression expected" past int64.
        if [ "${#o}" -gt 3 ] || [ "$o" -gt 255 ]; then
            return 1
        fi
    done
}

# Is $1 an IPv4 address in 10.0.0.0/8, 172.16.0.0/12 or 192.168.0.0/16? Data-plane
# traffic is unencrypted gRPC (see deploy/nginx/qiita.conf), so neither a peer nor
# an instance's listener may be on a public address.
qiita_is_private_ipv4() {
    local o1 o2 rest
    qiita_is_ipv4_literal "$1" || return 1
    IFS=. read -r o1 o2 rest <<<"$1"
    [ "$o1" -eq 10 ] && return 0
    [ "$o1" -eq 172 ] && [ "$o2" -ge 16 ] && [ "$o2" -le 31 ] && return 0
    [ "$o1" -eq 192 ] && [ "$o2" -eq 168 ] && return 0
    return 1
}

# The systemd unit of each local data-plane instance, one per port in `$1` (the
# list qiita_data_plane_ports returns), space-separated.
qiita_data_plane_units() {
    local port units=()
    for port in $1; do
        units+=("qiita-data-plane@${port}")
    done
    echo "${units[*]}"
}

# The local qiita-data-plane@ units whose port is not in `$1` (the list
# qiita_data_plane_ports returns) and that are running or enabled, one
# "<unit> <active>/<enabled>" per line; docs/runbooks/data-plane-scaling.md, "Fewer
# instances on this host", says why one is a problem. `list-units --all` includes an
# instance that is enabled but stopped, and leaves out one that is neither (systemd
# 257). Fails when systemctl cannot list the units.
qiita_data_plane_stray_units() {
    local configured=" " port listing unit active enabled
    for port in $1; do configured+="$port "; done
    listing=$(systemctl list-units --all --no-legend --plain 'qiita-data-plane@*.service') || {
        echo "ERROR: systemctl could not list the qiita-data-plane@ units" >&2
        return 1
    }
    while read -r unit _ active _; do
        [ -n "$unit" ] || continue
        port=${unit#qiita-data-plane@}
        port=${port%.service}
        case "$configured" in *" $port "*) continue ;; esac
        enabled=$(systemctl is-enabled "$unit" 2>/dev/null) || true
        if [ "$enabled" = enabled ] || { [ "$active" != inactive ] && [ "$active" != failed ]; }; then
            printf '%s %s/%s\n' "$unit" "$active" "${enabled:-unknown}"
        fi
    done <<<"$listing"
}

# What the deploy does with the units qiita_data_plane_stray_units finds for the
# ports in `$1`. With QIITA_DP_DISABLE_REMOVED=1 in the deploy's environment, prints
# their names, space-separated, for activate.sh to stop and disable. Without it,
# fails naming each one, so the deploy stops before installing anything rather than
# leave an instance on the previous build. Prints nothing when there are none.
qiita_data_plane_units_to_disable() {
    local strays line
    case "${QIITA_DP_DISABLE_REMOVED:-}" in
        ''|1) ;;
        *) echo "ERROR: QIITA_DP_DISABLE_REMOVED must be 1 or unset, not '$QIITA_DP_DISABLE_REMOVED'" >&2; return 1 ;;
    esac
    strays=$(qiita_data_plane_stray_units "$1") || return 1
    [ -n "$strays" ] || return 0
    if [ "${QIITA_DP_DISABLE_REMOVED:-}" = 1 ]; then
        awk '{print $1}' <<<"$strays" | paste -sd ' ' -
        return 0
    fi
    echo "ERROR: data-plane units running or enabled outside QIITA_DATA_PLANE_PORTS ($1):" >&2
    while IFS= read -r line; do echo "  $line" >&2; done <<<"$strays"
    echo "  Re-run the deploy with QIITA_DP_DISABLE_REMOVED=1 to stop and disable them, add" >&2
    echo "  their ports back to QIITA_DATA_PLANE_PORTS, or run 'systemctl disable --now <unit>'." >&2
    return 1
}

# The local instance ports, whitespace-separated: QIITA_DATA_PLANE_PORTS in $DP_ENV,
# else 50051; set but blank is an error. Each port becomes a systemd unit name, an
# nginx `server` line and a health-check target, so a malformed list fails here
# rather than rendering broken config. Also refused: the loopback listener's port,
# which nginx binds, and a repeated port, which would render one member twice.
qiita_data_plane_ports() {
    local ports="" port seen=" "
    qiita_dp_env_parses || return 1
    if [ -r "$DP_ENV" ] && env_file_sets_var "$DP_ENV" QIITA_DATA_PLANE_PORTS; then
        ports=$(read_env_file_var "$DP_ENV" QIITA_DATA_PLANE_PORTS)
    else
        ports=50051
    fi
    # All whitespace, not just spaces: a tab-only value would otherwise validate
    # nothing and be echoed back as the list.
    [ -n "${ports//[[:space:]]/}" ] || { echo "ERROR: QIITA_DATA_PLANE_PORTS is set but empty in $DP_ENV" >&2; return 1; }
    for port in $ports; do
        qiita_valid_tcp_port "$port" "QIITA_DATA_PLANE_PORTS entry '$port'" || return 1
        if [ "$port" = "$QIITA_DATA_PLANE_LB_PORT" ]; then
            echo "ERROR: QIITA_DATA_PLANE_PORTS entry '$port' is the port nginx's loopback listener binds" >&2
            return 1
        fi
        case "$seen" in
            *" $port "*)
                echo "ERROR: QIITA_DATA_PLANE_PORTS lists '$port' more than once" >&2
                return 1
                ;;
        esac
        seen+="$port "
    done
    printf '%s' "$ports"
}

# Data planes on other hosts, as a space-separated `host:port` list:
# QIITA_DATA_PLANE_PEERS in $DP_ENV, else none.
# Upstream members only — this deploy neither starts, restarts, nor upgrades them.
# PORTS and PEERS are separate lists because only PORTS names systemd units here.
#
# Each entry is written verbatim into an nginx `server` directive, so the shape is
# validated: a stray `;` or `}` would otherwise inject config. A host made only of
# digits and dots must be a valid IPv4 address — nginx would otherwise either fail
# to resolve it at config load (`999.999.999.999`) or accept an abbreviated form as
# a different address (`1.2.3` is 1.2.0.3).
qiita_data_plane_peers() {
    local peers="" peer host port
    qiita_dp_env_parses || return 1
    [ -r "$DP_ENV" ] && peers=$(read_env_file_var "$DP_ENV" QIITA_DATA_PLANE_PEERS)
    [ -n "${peers//[[:space:]]/}" ] || { printf ''; return 0; }
    for peer in $peers; do
        case "$peer" in
            # Bracketed IPv6 literal: [::1]:50051
            \[*\]:*)
                host="${peer%]:*}]"
                port="${peer##*]:}"
                case "${host:1:${#host}-2}" in
                    ''|*[!0-9A-Fa-f:]*)
                        echo "ERROR: QIITA_DATA_PLANE_PEERS entry '$peer' has a malformed IPv6 host" >&2
                        return 1
                        ;;
                esac
                ;;
            # DNS name or IPv4, exactly one colon.
            *:*:*)
                echo "ERROR: QIITA_DATA_PLANE_PEERS entry '$peer' has more than one ':'" \
                     "(bracket an IPv6 literal, e.g. [::1]:50051)" >&2
                return 1
                ;;
            *:*)
                host="${peer%:*}"
                port="${peer##*:}"
                case "$host" in
                    ''|*[!A-Za-z0-9._-]*)
                        echo "ERROR: QIITA_DATA_PLANE_PEERS entry '$peer' has a malformed host" >&2
                        return 1
                        ;;
                    *[!0-9.]*) ;;
                    *)
                        qiita_is_ipv4_literal "$host" || {
                            echo "ERROR: QIITA_DATA_PLANE_PEERS entry '$peer' is not a valid IPv4 address" >&2
                            return 1
                        }
                        ;;
                esac
                ;;
            *)
                echo "ERROR: QIITA_DATA_PLANE_PEERS entry '$peer' is not host:port" >&2
                return 1
                ;;
        esac
        qiita_valid_tcp_port "$port" "QIITA_DATA_PLANE_PEERS entry '$peer' port '$port'" || return 1
    done
    printf '%s' "$peers"
}

# The address this host's instances listen on: QIITA_DATA_PLANE_BIND_HOST in $DP_ENV,
# else 127.0.0.1. Read from the file only, never the deploy's environment:
# qiita-data-plane@.service builds LISTEN_ADDR from the same key through
# EnvironmentFile=, so a value the deploy saw but the file lacks would render
# upstream members no instance listens on. An IPv4 address naming one interface,
# loopback (127.0.0.0/8) or private (qiita_is_private_ipv4), never 0.0.0.0 or a
# public address: traffic between nginx and the instances is unencrypted. A host
# other than 127.0.0.1 is how a peer host's instances become reachable from the
# host that balances to them.
qiita_data_plane_bind_host() {
    local host=""
    qiita_dp_env_parses || return 1
    if [ -r "$DP_ENV" ] && env_file_sets_var "$DP_ENV" QIITA_DATA_PLANE_BIND_HOST; then
        host=$(read_env_file_var "$DP_ENV" QIITA_DATA_PLANE_BIND_HOST)
        [ -n "$host" ] || { echo "ERROR: QIITA_DATA_PLANE_BIND_HOST is set but empty in $DP_ENV" >&2; return 1; }
    else
        host=127.0.0.1
    fi
    qiita_is_ipv4_literal "$host" || {
        echo "ERROR: QIITA_DATA_PLANE_BIND_HOST '$host' in $DP_ENV is not an IPv4 address" >&2
        return 1
    }
    [ "$host" != 0.0.0.0 ] || {
        echo "ERROR: QIITA_DATA_PLANE_BIND_HOST must name one interface, not 0.0.0.0" >&2
        return 1
    }
    case "$host" in
        127.*) ;;
        *)
            qiita_is_private_ipv4 "$host" || {
                echo "ERROR: QIITA_DATA_PLANE_BIND_HOST '$host' is neither loopback nor a private address" \
                     "(10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16)" >&2
                return 1
            }
            ;;
    esac
    printf '%s' "$host"
}

# Abort unless every address a peer names is private (qiita_is_private_ipv4).
# $1 = a validated `host:port` entry.
#
# A hostname is resolved with `getent ahosts`, which lists every address nginx
# could balance to; one public address among them fails the peer. activate.sh
# calls this before installing anything, so a name that does not resolve also
# fails here rather than at nginx's config load. Without getent nothing can be
# checked, so a hostname fails too.
qiita_assert_peer_private() {
    local peer="$1" host addrs addr
    case "$peer" in
        \[*\]:*)
            echo "ERROR: QIITA_DATA_PLANE_PEERS entry '$peer' is IPv6; a peer must be a private IPv4 address" >&2
            return 1
            ;;
        *:*) host="${peer%:*}" ;;
        *)   echo "ERROR: '$peer' is not host:port" >&2; return 1 ;;
    esac
    if qiita_is_ipv4_literal "$host"; then
        addrs="$host"
    else
        if ! command -v getent >/dev/null 2>&1; then
            echo "ERROR: getent unavailable — cannot check peer '$host' resolves to a private address" >&2
            return 1
        fi
        if ! addrs=$(getent ahosts "$host" 2>/dev/null | awk '{print $1}' | sort -u) || [ -z "$addrs" ]; then
            echo "ERROR: QIITA_DATA_PLANE_PEERS host '$host' does not resolve" >&2
            return 1
        fi
    fi
    for addr in $addrs; do
        qiita_is_private_ipv4 "$addr" || {
            echo "ERROR: QIITA_DATA_PLANE_PEERS entry '$peer' reaches $addr, not a private address" \
                 "(10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16)" >&2
            return 1
        }
    done
}

# --- Per-instance limits -------------------------------------------------------
# Each local instance's unit gets one drop-in, written by activate.sh on every
# deploy. It sets QIITA_DATA_PLANE_LOCAL_INSTANCES, the number of local instances,
# which the data plane divides the spill filesystem (spill_cap) and its CPU budget
# (derived_threads) by, both in qiita-data-plane/src/config.rs, and, when
# QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX is set, MemoryMax=. MemoryMax= sets the unit's cgroup memory.max, which the data
# plane reads at boot as its memory ceiling (resources::detect_memory_ceiling) and
# divides across its pool. Both take effect at the restart that follows the write.
#
#   QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX  in $DP_ENV: MemoryMax= of every local
#                               instance's unit. Default none (no per-instance limit).
QIITA_DATA_PLANE_DROPIN_NAME=instance-limits.conf
# Where systemd reads unit files and drop-ins from.
QIITA_SYSTEMD_UNIT_DIR=/etc/systemd/system

# The drop-in directory of one instance unit. $1 = a port, $2 = the systemd unit
# directory (default $QIITA_SYSTEMD_UNIT_DIR).
qiita_data_plane_dropin_dir() {
    printf '%s/qiita-data-plane@%s.service.d' "${2:-$QIITA_SYSTEMD_UNIT_DIR}" "$1"
}

# A systemd size as bytes: a positive integer with an optional K, M, G or T suffix,
# each a power of 1024 (systemd.resource-control(5), MemoryMax=). Anything else,
# including a value past 2^62 bytes, fails with nothing printed.
qiita_size_bytes() {
    local value="$1" n mult=1
    [[ $value =~ ^([1-9][0-9]{0,17})([KMGT]?)$ ]] || return 1
    n=${BASH_REMATCH[1]}
    case ${BASH_REMATCH[2]} in
        K) mult=1024 ;;
        M) mult=$((1024 ** 2)) ;;
        G) mult=$((1024 ** 3)) ;;
        T) mult=$((1024 ** 4)) ;;
    esac
    [ "$n" -le $(((1 << 62) / mult)) ] || return 1
    printf '%s' $((n * mult))
}

# QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX in $DP_ENV, else empty. Read from the file
# only, like the topology keys. Set but blank, or not a size qiita_size_bytes
# accepts, is an error: the value is written verbatim into a unit file.
qiita_data_plane_instance_memory_max() {
    local value=""
    qiita_dp_env_parses || return 1
    [ -r "$DP_ENV" ] && env_file_sets_var "$DP_ENV" QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX || return 0
    value=$(read_env_file_var "$DP_ENV" QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX)
    [ -n "$value" ] || {
        echo "ERROR: QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX is set but empty in $DP_ENV" >&2
        return 1
    }
    qiita_size_bytes "$value" >/dev/null || {
        echo "ERROR: QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX '$value' in $DP_ENV is not a positive" \
             "integer with an optional K, M, G or T suffix (at most 2^62 bytes)" >&2
        return 1
    }
    printf '%s' "$value"
}

# Fails when $DP_ENV assigns QIITA_DATA_PLANE_LOCAL_INSTANCES. The drop-in sets it
# with Environment=, and a variable in EnvironmentFile= overrides one in
# Environment= (systemd 257), so a value in the file would replace the count the
# deploy computed.
qiita_dp_env_omits_local_instances() {
    qiita_dp_env_parses || return 1
    [ -r "$DP_ENV" ] && env_file_sets_var "$DP_ENV" QIITA_DATA_PLANE_LOCAL_INSTANCES || return 0
    echo "ERROR: $DP_ENV sets QIITA_DATA_PLANE_LOCAL_INSTANCES, which the deploy computes from" \
         "QIITA_DATA_PLANE_PORTS and the file would override; remove it" >&2
    return 1
}

# Fails when the local instances' MemoryMax= values add up to more than the host's
# memory. $1 = the ports (qiita_data_plane_ports), $2 = the validated
# QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX (empty: nothing to check), $3 = the meminfo
# file (default /proc/meminfo). An unreadable meminfo or one without MemTotal fails.
qiita_assert_instance_memory_fits() {
    local ports="$1" value="$2" meminfo="${3:-/proc/meminfo}" count bytes total_kib total
    [ -n "$value" ] || return 0
    bytes=$(qiita_size_bytes "$value") || {
        echo "ERROR: '$value' is not a systemd size" >&2
        return 1
    }
    count=$(wc -w <<<"$ports")
    count=${count//[[:space:]]/}
    total_kib=$(awk '$1 == "MemTotal:" { print $2; exit }' "$meminfo" 2>/dev/null) || total_kib=""
    [[ $total_kib =~ ^[0-9]+$ ]] || {
        echo "ERROR: no MemTotal in $meminfo — cannot check QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX against the host" >&2
        return 1
    }
    total=$((total_kib * 1024))
    # count × bytes > total exactly when bytes > floor(total / count), for integers;
    # the product itself could overflow.
    if [ "$bytes" -gt $((total / count)) ]; then
        echo "ERROR: $count local data-plane instance(s) × QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX=$value" \
             "exceeds this host's MemTotal of $total_kib kB ($meminfo)" >&2
        return 1
    fi
}

# The data plane's connection pool size when DUCKLAKE_POOL_SIZE is unset:
# DEFAULT_POOL_SIZE in qiita-data-plane/src/config.rs.
QIITA_DATA_PLANE_DEFAULT_POOL_SIZE=8

# DUCKLAKE_POOL_SIZE in $DP_ENV, else QIITA_DATA_PLANE_DEFAULT_POOL_SIZE. Blank,
# surrounding whitespace included, reads as unset, as in the data plane; anything
# but a positive integer fails.
qiita_data_plane_pool_size() {
    local value=""
    qiita_dp_env_parses || return 1
    if [ -r "$DP_ENV" ]; then
        value=$(read_env_file_var "$DP_ENV" DUCKLAKE_POOL_SIZE)
    fi
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    if [ -z "$value" ]; then
        printf '%s' "$QIITA_DATA_PLANE_DEFAULT_POOL_SIZE"
        return 0
    fi
    if ! [[ $value =~ ^[0-9]{1,18}$ ]] || [ $((10#$value)) -lt 1 ]; then
        echo "ERROR: DUCKLAKE_POOL_SIZE '$value' in $DP_ENV is not a positive integer" >&2
        return 1
    fi
    printf '%s' $((10#$value))
}

# A DUCKDB_MEMORY_LIMIT as bytes, for the two spellings the deploy compares: a
# positive integer without a leading zero, then MiB or GiB, as written. DuckDB
# accepts more than this; anything else, including a value past 2^62 bytes, fails
# with nothing printed.
qiita_duckdb_size_bytes() {
    local value="$1" n mult
    [[ $value =~ ^([1-9][0-9]{0,17})(MiB|GiB)$ ]] || return 1
    n=${BASH_REMATCH[1]}
    case ${BASH_REMATCH[2]} in
        MiB) mult=$((1024 ** 2)) ;;
        GiB) mult=$((1024 ** 3)) ;;
    esac
    [ "$n" -le $(((1 << 62) / mult)) ] || return 1
    printf '%s' $((n * mult))
}

# Fails when DUCKDB_MEMORY_LIMIT in $DP_ENV, times the pool size
# (qiita_data_plane_pool_size), exceeds the instance's MemoryMax=. DuckDB applies
# the limit to each pooled connection, and memory.max bounds the whole process, so
# such a pool may account for more memory than its cgroup allows it to hold.
# $1 = the validated QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX (empty: nothing to check).
# A blank or unset DUCKDB_MEMORY_LIMIT has nothing to check: the data plane then
# derives each connection's limit from memory.max (derived_share_mib in
# qiita-data-plane/src/config.rs).
qiita_assert_pool_memory_fits() {
    local value="$1" limit max_bytes limit_bytes pool
    [ -n "$value" ] || return 0
    qiita_dp_env_parses || return 1
    [ -r "$DP_ENV" ] || return 0
    limit=$(read_env_file_var "$DP_ENV" DUCKDB_MEMORY_LIMIT)
    [[ $limit =~ [^[:space:]] ]] || return 0
    max_bytes=$(qiita_size_bytes "$value") || {
        echo "ERROR: '$value' is not a systemd size" >&2
        return 1
    }
    limit_bytes=$(qiita_duckdb_size_bytes "$limit") || {
        echo "ERROR: DUCKDB_MEMORY_LIMIT '$limit' in $DP_ENV: write DUCKDB_MEMORY_LIMIT in MiB" \
             "or GiB, or unset it" >&2
        return 1
    }
    pool=$(qiita_data_plane_pool_size) || return 1
    # pool × limit > max exactly when limit > floor(max / pool), for integers; the
    # product itself could overflow.
    if [ "$limit_bytes" -gt $((max_bytes / pool)) ]; then
        echo "ERROR: a pool of $pool connection(s) × DUCKDB_MEMORY_LIMIT=$limit exceeds" \
             "QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX=$value ($DP_ENV). Unset DUCKDB_MEMORY_LIMIT:" \
             "the data plane then derives each connection's limit from the instance's MemoryMax" >&2
        return 1
    fi
}

# The drop-in qiita_write_data_plane_dropins writes for each instance. $1 = the
# number of local instances, $2 = the MemoryMax= value, or empty for none.
qiita_data_plane_dropin_content() {
    printf '[Service]\n'
    printf 'Environment=QIITA_DATA_PLANE_LOCAL_INSTANCES=%s\n' "$1"
    [ -z "$2" ] || printf 'MemoryMax=%s\n' "$2"
}

# Write the drop-in of every port in $1 and remove it from every other
# qiita-data-plane@ instance, so an instance whose port left the list (including
# one QIITA_DP_DISABLE_REMOVED=1 disables) keeps no stale count or limit. $2 = the
# MemoryMax= value or empty, $3 = the systemd unit directory (default
# $QIITA_SYSTEMD_UNIT_DIR). When it applies: "Per-instance limits" above.
qiita_write_data_plane_dropins() {
    local ports="$1" value="$2" unitdir="${3:-$QIITA_SYSTEMD_UNIT_DIR}" count port dir conf configured=" "
    count=$(wc -w <<<"$ports")
    count=${count//[[:space:]]/}
    for port in $ports; do
        configured+="$port "
        dir=$(qiita_data_plane_dropin_dir "$port" "$unitdir")
        install -d -m 0755 "$dir"
        qiita_data_plane_dropin_content "$count" "$value" > "$dir/$QIITA_DATA_PLANE_DROPIN_NAME"
        chmod 0644 "$dir/$QIITA_DATA_PLANE_DROPIN_NAME"
    done
    for conf in "$unitdir"/qiita-data-plane@*.service.d/"$QIITA_DATA_PLANE_DROPIN_NAME"; do
        [ -e "$conf" ] || continue
        dir=${conf%/*}
        port=${dir##*/qiita-data-plane@}
        port=${port%.service.d}
        case "$configured" in *" $port "*) continue ;; esac
        rm -f "$conf"
        rmdir "$dir" 2>/dev/null || true
    done
}

# The deployed nginx config.
QIITA_NGINX_CONF=/etc/nginx/conf.d/qiita.conf

# The certificate and key the 443 server block in deploy/nginx/qiita.conf names;
# without them `nginx -t` fails.
QIITA_NGINX_TLS_CERT=/etc/ssl/certs/qiita.crt
QIITA_NGINX_TLS_KEY=/etc/ssl/private/qiita.key
qiita_nginx_tls_present() {
    [ -r "$QIITA_NGINX_TLS_CERT" ] && [ -r "$QIITA_NGINX_TLS_KEY" ]
}

# Render the nginx config template $1 into $2: the server name $3, the loopback
# listener's port, and the qiita_data_plane upstream — one member per local port in
# $5 at the bind host $4, then each peer in $6, verbatim. $4-$6 come from the
# validators above. Fails if any `__QIITA_…__` placeholder survives.
#
# The upstream is spliced in with `sed /pattern/r` because it is multi-line. sed
# writes to a new file rather than editing in place, so the same function runs
# under GNU sed on the deploy host and BSD sed on a macOS dev box.
#
# Each step checks its own status: qiita_render_and_test_nginx_conf calls this as
# `… || {…}`, where bash ignores errexit for the whole body, so an unchecked `sed`
# on a missing template would install an empty file, which `nginx -t` accepts
# (nginx 1.26.3, Rocky 10).
qiita_render_nginx_conf() {
    local template="$1" dest="$2" server_name="$3" bind_host="$4" ports="$5" peers="$6"
    local members port peer lines=""
    # Built in a variable and written once, so the write's status is the one checked.
    for port in $ports; do
        lines+="    server $bind_host:$port;"$'\n'
    done
    for peer in $peers; do
        lines+="    server $peer;"$'\n'
    done
    members=$(mktemp) || return 1
    if ! printf '%s' "$lines" >"$members"; then
        rm -f "$members"
        return 1
    fi
    if ! sed -e "s/__QIITA_HOSTNAME__/${server_name}/g" \
        -e "s/__QIITA_DATA_PLANE_LB_HOST__/${QIITA_DATA_PLANE_LB_HOST}/g" \
        -e "s/__QIITA_DATA_PLANE_LB_PORT__/${QIITA_DATA_PLANE_LB_PORT}/g" \
        -e "/__QIITA_DATA_PLANE_UPSTREAM__/r $members" \
        -e "/__QIITA_DATA_PLANE_UPSTREAM__/d" \
        "$template" >"$dest.tmp"; then
        echo "ERROR: could not render $template; $dest left as it was" >&2
        rm -f "$members" "$dest.tmp"
        return 1
    fi
    rm -f "$members"
    if grep -n '__QIITA_[A-Z_]*__' "$dest.tmp" >&2; then
        echo "ERROR: placeholder left unrendered in $template (lines above)" >&2
        rm -f "$dest.tmp"
        return 1
    fi
    mv "$dest.tmp" "$dest"
}

# qiita_render_nginx_conf with the same arguments, then `nginx -t`. When nginx
# rejects the rendered file, the file it replaced is put back (or the new one
# removed, if there was none) and this fails: nginx refuses to start on a config
# `nginx -t` rejects, so a rejected file left in conf.d would stop nginx at its next
# start. The copy is `$2.previous`, a name nginx's conf.d/*.conf include skips.
qiita_render_and_test_nginx_conf() {
    local dest="$2" previous="$2.previous"
    if [ -e "$dest" ]; then
        cp -p "$dest" "$previous" || return 1
    fi
    qiita_render_nginx_conf "$@" || { rm -f "$previous"; return 1; }
    if nginx -t; then
        rm -f "$previous"
        return 0
    fi
    if [ -e "$previous" ]; then
        mv "$previous" "$dest"
        echo "ERROR: nginx -t rejected the rendered $dest; the file it replaced is back in place" >&2
    else
        rm -f "$dest"
        echo "ERROR: nginx -t rejected the rendered $dest; removed it (there was no previous file)" >&2
    fi
    return 1
}

# The data-plane upstream members in a rendered config, one per line. $1 = config
# path (default $QIITA_NGINX_CONF). verify.sh health-checks these rather than
# re-deriving the list from $DP_ENV or the environment, so it checks the last render
# activate.sh kept (qiita_render_and_test_nginx_conf covers a rejected one). That is
# what nginx serves unless the deploy stopped between the render and its `systemctl
# reload nginx`, skipped the reload for lack of TLS material, or the reload did not
# apply: `systemctl reload nginx` returns 0 when the config passes `nginx -t` but the
# running master cannot apply it, such as a listen address it cannot bind, and nginx
# keeps serving the old config (nginx 1.26.3, Rocky 10).
#
# Three outcomes, kept distinct:
#   0 — members on stdout.
#   1 — no config to read (first deploy, before nginx was configured). The caller
#       falls back to the $DP_ENV lists.
#   2 — the config is there but no members parsed (block renamed, file truncated).
#       The caller must not fall back: the fallback yields the single default port
#       whatever the host runs, so a broken parse would pass as one green row.
qiita_data_plane_rendered_members() {
    local conf="${1:-$QIITA_NGINX_CONF}" members
    [ -r "$conf" ] || return 1
    # Bounded to the qiita_data_plane block: qiita_control_plane is a sibling
    # upstream in the same file. $2, not the rest of the line: a `server` line may
    # carry parameters (`server 127.0.0.1:50051 max_fails=3;`).
    members=$(awk '
        /^[[:space:]]*upstream[[:space:]]+qiita_data_plane[[:space:]]*\{/ { inblock = 1; next }
        inblock && /^[[:space:]]*\}/                                      { inblock = 0 }
        inblock && /^[[:space:]]*server[[:space:]]/ {
            addr = $2
            sub(/;.*$/, "", addr)
            if (addr != "") print addr
        }
    ' "$conf")
    if [ -z "$members" ]; then
        echo "ERROR: $conf has no 'upstream qiita_data_plane' members" >&2
        return 2
    fi
    printf '%s\n' "$members"
}

# verify.sh's row label for upstream member $1, given this host's bind host $2:
# a local instance by port, a peer by host:port.
qiita_data_plane_member_label() {
    case "$1" in
        "$2":*) printf 'health/data-plane@%s' "${1##*:}" ;;
        *)      printf 'health/data-plane-peer@%s' "$1" ;;
    esac
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

# The DuckDB CLI the lake scripts require — held equal to the data plane's `duckdb`
# crate by qiita-common/tests/test_duckdb_version_sync.py.
QIITA_DUCKDB_VERSION="1.5.5"

# Resolve the duckdb CLI into DUCKDB_BIN and require exactly v${QIITA_DUCKDB_VERSION},
# or exit with install instructions. Exact, because the CLI opens the data plane's
# catalog with the ducklake extension of its own version — a newer one may migrate
# the catalog schema under the running data plane — and lake-shell LOADs the miint
# staged for its own version. A binary that reports no version is refused too.
# Two install sites because the callers run as different accounts: a human with
# a home, or a service account (qiita-data) whose home is /dev/null.
qiita_resolve_duckdb_bin() {
    DUCKDB_BIN="${QIITA_DUCKDB_BIN:-$(command -v duckdb || true)}"
    local problem reported
    if [ -z "${DUCKDB_BIN}" ]; then
        problem="no duckdb CLI on PATH."
    else
        # `duckdb --version` prints e.g. `v1.5.5 (Variegata) d8cdaa33fd`. stderr is
        # kept, so a binary that cannot run says why in the message below.
        reported=$("${DUCKDB_BIN}" --version 2>&1 | head -n 1) || true
        [ "${reported%% *}" = "v${QIITA_DUCKDB_VERSION}" ] && return 0
        problem="\`${DUCKDB_BIN} --version\` printed '${reported:-nothing}'; the data plane links v${QIITA_DUCKDB_VERSION}."
    fi
    cat >&2 <<EOF
ERROR: ${problem}

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

# Wait until the data-plane units `$2...` (env file `$1`) have nothing in
# flight, for up to QIITA_DP_IDLE_WAIT_S seconds (default: the unit's
# TimeoutStopSec, deploy/systemd/qiita-data-plane@.service; 0 skips the wait), then
# return 0 whether or not it went idle: the data plane keeps serving while this
# waits, so under steady load it may not go idle, and its drain lets what is still
# running finish. Returns at once when the env file is absent (first deploy) or no
# unit has a main process. Exits non-zero, before any restart, when
# QIITA_DP_IDLE_WAIT_S is not a whole number or the catalog query fails.
#
# In flight, as the data plane already exposes it: a DoGet stream or a
# register_files holds a catalog session in `active` or `idle in transaction`,
# while an idle data plane's sessions are `idle`, and a DoPut holds its
# `upload.parquet.partial` open (all measured against a running data plane, macOS).
# A read export writes through a `.partial` too, so it counts as well. Open files
# are found through each unit's /proc/<pid>/fd (the match measured in a Linux
# container). The catalog query, QIITA_CATALOG_IDLE_SQL, connects with the data
# plane's own role.
wait_for_data_plane_idle() {
    local env_file=$1
    shift
    local unit_names="$*"
    local wait_s=${QIITA_DP_IDLE_WAIT_S:-3900}
    case $wait_s in
        '' | *[!0-9]*)
            echo "ERROR: QIITA_DP_IDLE_WAIT_S must be a whole number of seconds, got '$wait_s'" >&2
            return 1
            ;;
    esac
    # Base 10, so a leading zero is not read as octal.
    wait_s=$((10#$wait_s))
    [ -r "$env_file" ] && [ "$wait_s" -gt 0 ] || return 0
    local unit pid pids=""
    for unit in "$@"; do
        pid=$(systemctl show -p MainPID --value "$unit")
        [ "${pid:-0}" -gt 0 ] && pids="$pids $pid"
    done
    [ -n "$pids" ] || return 0
    # A subshell, so qiita_pgpass_init's EXIT trap removes the pgpass when the wait
    # ends rather than replacing this script's own exit handling. The password
    # reaches libpq through that 0600 file, not psql's argv (see
    # qiita_split_conn_password).
    (
        local conn user password lake partials n
        IFS=$'\t' read -r conn user password \
            < <(qiita_split_conn_password "$(read_env_var "$env_file" DUCKLAKE_CATALOG_CONNSTR)")
        qiita_pgpass_init "qiita-dp-idle"
        [ -z "$password" ] || qiita_pgpass_add "$user" "$password"
        local deadline=$((SECONDS + wait_s)) next_report=$SECONDS
        while :; do
            lake=$(PGPASSFILE="$PGPASS_FILE" psql "$conn" -Atc "$QIITA_CATALOG_IDLE_SQL") || {
                echo "ERROR: cannot query the data plane's catalog to tell whether $unit_names is idle; set QIITA_DP_IDLE_WAIT_S=0 to restart without waiting" >&2
                exit 1
            }
            partials=0
            for pid in $pids; do
                # `|| true`: find fails once the process has exited (no fd dir), and
                # pipefail would carry that out of the assignment.
                n=$(find "/proc/$pid/fd" -lname '*.partial' 2>/dev/null | wc -l || true)
                partials=$((partials + n))
            done
            if [ "$lake" -eq 0 ] && [ "$partials" -eq 0 ]; then
                exit 0
            fi
            if [ "$SECONDS" -ge "$deadline" ]; then
                echo "$unit_names still busy after ${wait_s}s (catalog sessions: $lake, open partial files: $partials); restarting, and its drain lets them finish" >&2
                exit 0
            fi
            if [ "$SECONDS" -ge "$next_report" ]; then
                echo "waiting for $unit_names to go idle (catalog sessions: $lake, open partial files: $partials); up to $((deadline - SECONDS))s more" >&2
                next_report=$((SECONDS + 60))
            fi
            sleep "${QIITA_DP_IDLE_POLL_S:-5}"
        done
    )
}

# --- Catalog connection budget ------------------------------------------------
# Every data-plane instance, on every host, holds up to its pool size of backends
# on the catalog's Postgres (DEFAULT_POOL_SIZE in qiita-data-plane/src/config.rs),
# and builds them as requests arrive, so the backends connected now undercount
# what the pools may open. Each instance therefore names its catalog sessions
# `qiita-data-plane:<listen port>:pool=<pool size>` (catalog_application_name in
# qiita-data-plane/src/ducklake.rs), and this check reads the pool sizes back from
# the catalog server's pg_stat_activity: an instance is one (client address,
# application_name) pair, however many of its backends are open. It fails when
#   Σ pool sizes + the other client backends connected now
#     > max_connections − superuser_reserved_connections − reserved_connections,
# the slots a role without those privileges can take. Other backends — lake-gc,
# lake-shell, a data plane still on a build without the name — count as they are
# now, not at their peak.
#
# The query connects with the data plane's own role, the user= in
# DUCKLAKE_CATALOG_CONNSTR. Postgres shows a role the client_addr and
# backend_type of another role's sessions only with pg_read_all_stats (without
# it both are NULL), so the check fails rather than undercount when the role
# lacks it. A Unix-socket session has no client_addr; its client_port is -1, and
# it is labelled `local`.

QIITA_CATALOG_SETTINGS_SQL="SELECT current_setting('max_connections'),
    current_setting('superuser_reserved_connections'),
    coalesce(current_setting('reserved_connections', true), '0'),
    pg_has_role('pg_read_all_stats', 'USAGE'), current_user"
QIITA_CATALOG_BACKENDS_SQL="SELECT
    coalesce(host(client_addr), CASE WHEN client_port = -1 THEN 'local' END, ''),
    count(*), coalesce(usename, ''), coalesce(application_name, '')
  FROM pg_stat_activity
  WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()
  GROUP BY 1, 3, 4 ORDER BY 1, 4"

# wait_for_data_plane_idle's in-flight count. It counts only the connecting role's
# sessions (`usename = current_user`): those of every data-plane instance on the
# catalog, a peer host's included, and a lake-gc or lake-shell session under that
# role. Without pg_read_all_stats another role's rows are listed but their state
# reads NULL, so they never match; with the grant (which the connection budget
# check above needs) their state is visible, and without the filter an admin's or
# a monitor's open transaction on the catalog would hold the restart.
QIITA_CATALOG_IDLE_SQL="SELECT count(*) FROM pg_stat_activity
  WHERE datname = current_database() AND pid <> pg_backend_pid()
    AND usename = current_user
    AND state IN ('active', 'idle in transaction')"

# Run one query against the catalog named by env file `$1`, printing psql's
# unaligned `|`-separated rows. A subshell, so qiita_pgpass_init's EXIT trap
# removes the pgpass when the query ends; the password reaches libpq through that
# file, not psql's argv (see qiita_split_conn_password). $2 = the SQL.
qiita_catalog_psql() (
    local conn user password
    IFS=$'\t' read -r conn user password \
        < <(qiita_split_conn_password "$(read_env_var "$1" DUCKLAKE_CATALOG_CONNSTR)")
    qiita_pgpass_init "qiita-catalog"
    [ -z "$password" ] || qiita_pgpass_add "$user" "$password"
    PGPASSFILE="$PGPASS_FILE" psql "$conn" -XAtq -F '|' -v ON_ERROR_STOP=1 -c "$2"
)

# The budget arithmetic over the two queries' output, as pass/fail rows named
# `catalog-connections`, followed by one line per instance and per other client.
# Pure (rows in, rows out) so the unit tests can feed it psql output.
# $1 = the QIITA_CATALOG_SETTINGS_SQL row; stdin = the QIITA_CATALOG_BACKENDS_SQL rows.
qiita_catalog_connection_budget() {
    local max su reserved has_stats role
    IFS='|' read -r max su reserved has_stats role <<<"$1"
    local n
    for n in "$max" "$su" "$reserved"; do
        case $n in
            '' | *[!0-9]*)
                fail "catalog-connections" "unexpected settings row from the catalog server: '$1'"
                return 0
                ;;
        esac
    done
    if [ "$has_stats" != t ]; then
        fail "catalog-connections" "role '$role' cannot see other roles' sessions on the catalog server; an admin there runs: GRANT pg_read_all_stats TO $role; (see docs/runbooks/data-plane-scaling.md)"
        return 0
    fi
    local host count usename appname port pool label
    local instances="" seen=$'\n' others="" n_instances=0 pools=0 n_others=0 bad=""
    while IFS='|' read -r host count usename appname; do
        [ -n "$host$count" ] || continue
        if [[ "$appname" == qiita-data-plane:* ]]; then
            if [[ ! "$appname" =~ ^qiita-data-plane:([0-9]+):pool=([0-9]+)$ ]]; then
                bad+="application_name '$appname' from ${host:-an unknown address} is not qiita-data-plane:<port>:pool=<n>; "
                continue
            fi
            port=${BASH_REMATCH[1]} pool=$((10#${BASH_REMATCH[2]}))
            if [ -z "$host" ]; then
                bad+="no client address for '$appname' — grant pg_read_all_stats to role '$role'; "
                continue
            fi
            case $seen in *$'\n'"$host $appname"$'\n'*) continue ;; esac
            seen+="$host $appname"$'\n'
            [[ "$host" == *:* ]] && label="[$host]:$port" || label="$host:$port"
            instances+="    $label pool=$pool"$'\n'
            n_instances=$((n_instances + 1))
            pools=$((pools + pool))
        else
            others+="    other: ${host:-?} user=${usename:-?} application_name=${appname:-(none)} x$count"$'\n'
            n_others=$((n_others + count))
        fi
    done
    local available=$((max - su - reserved))
    local demand=$((pools + n_others))
    local numbers="$n_instances data-plane instance(s), pools $pools + $n_others other client backend(s) = $demand of $available (max_connections $max - superuser_reserved_connections $su - reserved_connections $reserved)"
    if [ -n "$bad" ]; then
        fail "catalog-connections" "${bad%; }"
    elif [ "$n_instances" -eq 0 ]; then
        fail "catalog-connections" "no catalog session is named qiita-data-plane:<port>:pool=<n> — no instance on this build is connected; $numbers"
    elif [ "$demand" -gt "$available" ]; then
        fail "catalog-connections" "$numbers — lower DUCKLAKE_POOL_SIZE or the instance count, or raise max_connections on the catalog server"
    else
        pass "catalog-connections" "$numbers"
    fi
    printf '%s%s' "$instances" "$others"
}

# The verify-deploy row: both queries against the catalog in env file `$1`, then
# the budget. A query failure fails the row with psql's message.
qiita_check_catalog_connection_budget() {
    local env_file=$1 settings backends
    if ! settings=$(qiita_catalog_psql "$env_file" "$QIITA_CATALOG_SETTINGS_SQL" 2>&1); then
        fail "catalog-connections" "cannot query the catalog server: $settings"
        return 0
    fi
    if ! backends=$(qiita_catalog_psql "$env_file" "$QIITA_CATALOG_BACKENDS_SQL" 2>&1); then
        fail "catalog-connections" "cannot query the catalog server: $backends"
        return 0
    fi
    qiita_catalog_connection_budget "$settings" <<<"$backends"
}
