#!/usr/bin/env bash
# Read-only config/secret preflight for an established qiita-miint host.
#
# Validates cross-file consistency that otherwise fails silently at runtime
# (PATH_SCRATCH drift, HMAC_SECRET_KEY mismatch between CP and DP, missing /
# mis-permed token files) and prints NON-SECRET fingerprints so an operator can
# confirm matches without reading the 0440/0400 files. Run BEFORE a restart so
# a bad config aborts the deploy instead of 500ing (or silently mis-staging)
# afterwards. Read-only: never writes, never connects — safe to re-run.
#
# Run as root for the FULL picture (token fingerprints need to read the 0400/0440
# token files):
#   sudo deploy/preflight.sh
# Or run as the OPERATOR account that holds the config-read ACL on the .env files
# (first-deploy.md §0.1): the .env consistency checks (PATH_SCRATCH, HMAC,
# connection-string shape) all run; token owner/mode is still verified via stat,
# but the token fingerprints degrade to "n/a" since the operator can't read the
# token contents. No sudo needed for that path.
#
# Exit: non-zero iff any ATTEMPTED check failed. Unreadable env files (first
# deploy, or an unprivileged caller without the ACL) degrade to skip rows, exit
# 0. SKIP_PREFLIGHT=1 bypasses entirely (logged). See docs/runbooks/redeploy.md.

set -euo pipefail

[ -n "${SKIP_PREFLIGHT:-}" ] && { echo "preflight: skipped via SKIP_PREFLIGHT=1" >&2; exit 0; }
# Deliberately NOT root-gated — an operator with the .env config-read ACL can run
# the env-consistency checks without sudo. When non-root, token fingerprints
# degrade gracefully (owner/mode still checked); run with sudo for the full set.
if [ "$EUID" -ne 0 ]; then
    echo "preflight: running as $(id -un) (non-root) — token fingerprints need root; .env checks need the operator config-read ACL (first-deploy.md §0.1)." >&2
fi

# shellcheck source=deploy/_common.sh
source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"  # CP_ENV/DP_ENV/CO_ENV, read_env_var, pass/fail/skip

n_pass=0 n_fail=0 n_skip=0

# Non-secret fingerprint: 12 hex chars of SHA-256 over the value. Trim
# surrounding whitespace first so env-file formatting differences don't make
# identical secrets fingerprint differently (a false mismatch). 12 hex chars
# (48 bits) over a high-entropy secret is not reversible — safe to print; for a
# leftover placeholder it just confirms the placeholder was never replaced.
fingerprint() {
    local value="$1"
    value="${value#"${value%%[![:space:]]*}"}"   # ltrim
    value="${value%"${value##*[![:space:]]}"}"    # rtrim
    [ -n "$value" ] || { printf '(empty)'; return; }
    printf '%s' "$value" | sha256sum | head -c 12
}

echo "preflight: config/secret consistency"

# --- PATH_SCRATCH byte-identical across all three env files ------------------
present_envs=()
for ef in "$CP_ENV" "$DP_ENV" "$CO_ENV"; do
    [ -r "$ef" ] && present_envs+=("$ef")
done
if [ "${#present_envs[@]}" -eq 0 ]; then
    skip "path-scratch" "no env files present (first deploy)"
else
    # CO PATH_SCRATCH is optional (defaults to $TMPDIR/qiita in dev); only compare
    # the env files that actually set it. A set value MUST match the others.
    first=""
    mismatch=""
    details=""
    n_set=0
    for ef in "${present_envs[@]}"; do
        v=$(read_env_var "$ef" PATH_SCRATCH)
        details+="$(basename "$ef")=${v:-<unset>} "
        [ -n "$v" ] || continue
        n_set=$((n_set + 1))
        if [ -z "$first" ]; then first="$v"; elif [ "$v" != "$first" ]; then mismatch="yes"; fi
    done
    if [ -z "$first" ]; then
        skip "path-scratch" "PATH_SCRATCH not set in any present env file"
    elif [ -n "$mismatch" ]; then
        fail "path-scratch" "values differ across env files: ${details}(must be byte-identical)"
    else
        pass "path-scratch" "$first (identical across $n_set env file(s) that set it)"
    fi
fi

# --- HMAC_SECRET_KEY matches CP <-> DP (CO has no HMAC) ----------------------
if [ -r "$CP_ENV" ] && [ -r "$DP_ENV" ]; then
    cp_hmac=$(read_env_var "$CP_ENV" HMAC_SECRET_KEY)
    dp_hmac=$(read_env_var "$DP_ENV" HMAC_SECRET_KEY)
    cp_fp=$(fingerprint "$cp_hmac")
    dp_fp=$(fingerprint "$dp_hmac")
    if [ -z "$cp_hmac" ] || [ -z "$dp_hmac" ]; then
        fail "hmac-match" "HMAC_SECRET_KEY missing (CP=${cp_fp}, DP=${dp_fp}) — both required"
    elif [ "$cp_hmac" = "$dp_hmac" ]; then
        pass "hmac-match" "CP == DP (sha256:${cp_fp})"
    else
        fail "hmac-match" "CP (sha256:${cp_fp}) != DP (sha256:${dp_fp}) — Flight tickets will fail to verify"
    fi
else
    skip "hmac-match" "control-plane.env and/or data-plane.env absent (first deploy)"
fi

# --- Token files present + correctly permed ---------------------------------
# Each token's documented (owner group mode) — first-deploy.md / slurm-backend-setup.md.
# Format: "path|owner|group|mode|gate" where gate is "always" or "slurm".
check_token() {
    local path="$1" owner="$2" group="$3" mode="$4" name="$5"
    if [ ! -e "$path" ]; then
        fail "token/$name" "$path missing"
        return
    fi
    local actual fp
    actual=$(stat -c '%U %G %a' "$path" 2>/dev/null) || { fail "token/$name" "stat failed on $path (can the caller traverse /etc/qiita?)"; return; }
    # Owner/mode is verified via stat (no read needed). The fingerprint needs to
    # read the contents — available to root, but an unprivileged operator (the
    # .env ACL deliberately does NOT cover tokens) can't, so degrade to n/a
    # rather than printing a misleading "(empty)".
    if [ -r "$path" ]; then
        fp="sha256:$(fingerprint "$(cat "$path")")"
    else
        fp="fingerprint n/a — not readable as $(id -un); run as root"
    fi
    if [ "$actual" = "$owner $group $mode" ]; then
        pass "token/$name" "$path ($actual, ${fp})"
    else
        fail "token/$name" "$path is '$actual', expected '$owner $group $mode' (${fp})"
    fi
}

# COMPUTE_BACKEND decides whether the SLURM JWT is expected at all.
co_backend=""
[ -r "$CO_ENV" ] && co_backend=$(read_env_var "$CO_ENV" COMPUTE_BACKEND)
if [ -r "$CO_ENV" ]; then
    check_token /etc/qiita/cp-to-co.token root qiita-services 440 cp-to-co
    check_token /etc/qiita/co-to-cp.token qiita-orch qiita-orch 400 co-to-cp
    if [ "$co_backend" = "slurm" ]; then
        check_token /etc/qiita/slurmrestd.jwt qiita-job qiita-orch 640 slurmrestd-jwt
    else
        skip "token/slurmrestd-jwt" "COMPUTE_BACKEND=${co_backend:-<unset>} — JWT only required on the slurm backend"
    fi
else
    skip "token/cp-to-co" "compute-orchestrator.env absent (first deploy)"
    skip "token/co-to-cp" "compute-orchestrator.env absent (first deploy)"
fi

# --- Connection-string shape (no connect) -----------------------------------
if [ -r "$CP_ENV" ]; then
    db_url=$(read_env_var "$CP_ENV" DATABASE_URL)
    if [[ "$db_url" =~ ^postgres(ql)?://[^/[:space:]]+/[^[:space:]]+ ]]; then
        pass "connstr/database-url" "postgresql://…/<db> shape OK"
    else
        fail "connstr/database-url" "DATABASE_URL is not a postgresql://host/db URL: '${db_url:0:24}…'"
    fi
else
    skip "connstr/database-url" "control-plane.env absent (first deploy)"
fi
if [ -r "$DP_ENV" ]; then
    connstr=$(read_env_var "$DP_ENV" DUCKLAKE_CATALOG_CONNSTR)
    if [[ "$connstr" == *dbname=* && "$connstr" == *host=* ]]; then
        pass "connstr/ducklake" "libpq dbname=/host= keywords present"
    else
        fail "connstr/ducklake" "DUCKLAKE_CATALOG_CONNSTR missing libpq dbname=/host= keywords"
    fi
else
    skip "connstr/ducklake" "data-plane.env absent (first deploy)"
fi

echo "preflight: ${n_pass} pass, ${n_fail} fail, ${n_skip} skip"
[ "$n_fail" -eq 0 ]
