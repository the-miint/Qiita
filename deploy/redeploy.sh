#!/usr/bin/env bash
# Guided incremental redeploy for an established qiita-miint host — ALL-IN-ONE.
#
# Run ONCE as root (sudo) from your admin account. The script drives the whole
# fixed skeleton of docs/runbooks/redeploy.md and drops into the right account
# for each step via `sudo -u`, so the no-sudo operator account never has to log
# in and the admin never hand-copies per-account verify lines (the source of
# recurring deploy bugs — issue #72):
#
#   * operator steps (git pull, migration gate) → run as $QIITA_USER (e.g. qiita)
#   * admin steps    (preflight, local-deploy.sh) → run as root (this process)
#   * verify steps   (actions list, compute-readiness) → verify.sh sudo's into
#                      qiita-api / qiita-orch itself, each with its own env file
#
# This matches the documented two-role split (first-deploy.md "Account model":
# [operator] = a user literally named `qiita`, NO sudo, owns the clone; [admin] =
# your personal account WITH sudo) and mirrors local-deploy.sh, which is already
# root-run and `sudo -u "$QIITA_USER"` for the pull/build. One human with sudo
# runs the whole deploy; the script does the run-as switching.
#
# It does NOT replace the judgment steps: secrets / one-time host setup (buckets
# 1 & 2) stay manual, and migrations stay OUT-OF-BAND — the gate verifies they
# ran and REFUSES otherwise. RUN_MIGRATE=1 opts into applying them here after a
# typed confirm; it is never silent (unsafe for expand/contract changes), and
# activate.sh's in-deploy guard is the backstop.
#
# Usage:
#   sudo QIITA_HOSTNAME=qiita-miint.ucsd.edu /home/qiita/qiita-miint/deploy/redeploy.sh
#   (or: sudo make redeploy QIITA_HOSTNAME=qiita-miint.ucsd.edu)
#
# Env: QIITA_HOSTNAME (required), QIITA_USER (default: qiita — the no-sudo
#      operator/checkout owner), QIITA_CLONE (default: this script's parent's
#      parent), ASSUME_YES=1 (skip interactive acks — for automation),
#      RUN_MIGRATE=1 (apply pending migrations here after a typed confirm;
#      default off — leave off for expand/contract deploys), SKIP_STAGE_MIINT=1.

set -euo pipefail

# shellcheck source=deploy/_common.sh
source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"  # require_root, qiita_resolve_user_clone, read_env_var, CP_ENV/CO_ENV, QIITA_*_USER

require_root "run deploy/redeploy.sh as root (sudo) from your admin account — it drops into ${QIITA_USER:-qiita} / ${QIITA_API_USER} / ${QIITA_ORCH_USER} per step via sudo -u (see header)."
: "${QIITA_HOSTNAME:?QIITA_HOSTNAME must be set (e.g. qiita-miint.ucsd.edu)}"

# Sets + validates QIITA_USER (operator/checkout owner) and QIITA_CLONE (the git
# clone where pull/build/migrate run, NOT the deployed /opt/qiita copy).
qiita_resolve_user_clone

confirm() {
    # $1 = prompt. Honors ASSUME_YES=1; aborts on anything but an explicit yes.
    [ -n "${ASSUME_YES:-}" ] && { echo "$1 [auto-yes via ASSUME_YES=1]"; return 0; }
    local reply
    read -r -p "$1 [y/N] " reply
    [ "$reply" = "y" ] || [ "$reply" = "Y" ] || { echo "Aborted." >&2; exit 1; }
}

echo "=== redeploy: $QIITA_HOSTNAME (clone: $QIITA_CLONE, operator: $QIITA_USER) ==="

# --- 1. Pull source as the operator ----------------------------------------
echo "--- [1/7] Pull source (as $QIITA_USER) ---"
sudo -u "$QIITA_USER" git -C "$QIITA_CLONE" pull --ff-only

# --- 2. Pending-deploy buckets 1+2 (manual) + preflight ---------------------
echo "--- [2/7] Env vars + one-time host setup (buckets 1 & 2) ---"
checklist="$QIITA_CLONE/DEPLOY_CHECKLIST.md"
if [ -r "$checklist" ]; then
    # Print buckets 1 and 2 (Env-vars header up to, but excluding, Migrations).
    sed -n '/^### 1\. Env vars/,/^### 3\. Migrations/p' "$checklist" | sed '$d'
fi
echo "Apply any env-var + one-time-host-setup steps above BEFORE continuing —"
echo "they must be in place before the restart, and stay manual (secrets, dirs, scopes)."
confirm "Have buckets 1 (env vars) and 2 (one-time host setup) been applied?"

echo "--- Config/secret preflight (read-only; root → full token fingerprints) ---"
QIITA_HOSTNAME="$QIITA_HOSTNAME" "$QIITA_CLONE/deploy/preflight.sh"

# --- 3. Migration gate (out-of-band; verify-and-refuse, never silent) -------
echo "--- [3/7] Migration gate (out-of-band — this wrapper does not auto-apply) ---"
# DATABASE_URL comes from control-plane.env (root reads it) and is handed to the
# operator step via `env`, so the operator migrates exactly the DB activate.sh's
# guard checks — no "wrong-DB" drift, and no dependency on the operator's shell
# having DATABASE_URL exported.
db_url=""
[ -r "$CP_ENV" ] && db_url=$(read_env_var "$CP_ENV" DATABASE_URL)
if [ -z "$db_url" ]; then
    echo "WARNING: could not read DATABASE_URL from $CP_ENV — skipping the local"
    echo "         migration pre-check. activate.sh's guard still refuses a stale schema."
    confirm "Continue without the local migration pre-check?"
else
    status=$(sudo -u "$QIITA_USER" env DATABASE_URL="$db_url" bash -lc \
        "cd '$QIITA_CLONE/qiita-control-plane' && dbmate --migrations-table public.schema_migrations status" 2>/dev/null) || status=""
    pending_rows=$(printf '%s\n' "$status" | grep -E '^\[ \]' || true)
    if [ -z "$status" ]; then
        echo "WARNING: could not run 'dbmate status' as $QIITA_USER (dbmate on its PATH?)."
        echo "         activate.sh's guard still refuses a stale schema — but verify by hand."
        confirm "Continue without the local migration pre-check?"
    elif [ -n "$pending_rows" ]; then
        printf '%s\n' "$pending_rows"
        echo ""
        echo "Pending migrations detected (see '[ ]' rows above)."
        if [ -n "${RUN_MIGRATE:-}" ]; then
            echo "RUN_MIGRATE=1 set. Review the expand/contract caution in redeploy.md §5"
            echo "before applying — a contract migration must NOT ship with code that stops"
            echo "using the old column unless every instance is already on the new code."
            confirm "Apply these migrations now with 'make migrate' (as $QIITA_USER)?"
            sudo -u "$QIITA_USER" env DATABASE_URL="$db_url" bash -lc "make -C '$QIITA_CLONE' migrate"
        else
            echo "STOP: apply them out-of-band first — as $QIITA_USER, with DATABASE_URL"
            echo "      sourced from control-plane.env, run 'make -C $QIITA_CLONE migrate' —"
            echo "      then re-run this script. Or re-run now with RUN_MIGRATE=1 to apply"
            echo "      here after a typed confirmation."
            echo "(See redeploy.md §4–§5 for the expand/contract caution.)"
            exit 1
        fi
    else
        echo "No pending migrations."
    fi
fi

# --- 4. Deploy --------------------------------------------------------------
echo "--- [4/7] Deploy (local-deploy.sh; SKIP_PULL=1 — already pulled in step 1) ---"
env SKIP_PULL=1 QIITA_HOSTNAME="$QIITA_HOSTNAME" QIITA_USER="$QIITA_USER" QIITA_CLONE="$QIITA_CLONE" \
    "$QIITA_CLONE/deploy/local-deploy.sh"

# --- 5. SLURM native-venv refresh + miint staging (recurring footguns) ------
echo "--- [5/7] SLURM native env (redeploy.md §6) ---"
echo "REMINDER: local-deploy.sh only synced the /opt/qiita SERVICE venvs. Native"
echo "SLURM jobs run from the SLURM_NATIVE_PYTHON checkout on the shared FS; on any"
echo "deploy that changed qiita-common or qiita-compute-orchestrator, refresh it too"
echo "(as its owner $QIITA_USER), or native jobs import stale code:"
echo "    sudo -u $QIITA_USER bash -lc 'cd <native-checkout>/qiita-compute-orchestrator && uv sync --reinstall-package qiita-common'"
if [ -n "${SKIP_STAGE_MIINT:-}" ]; then
    echo "Skipping miint extension staging (SKIP_STAGE_MIINT=1)."
elif [ -r "$CO_ENV" ]; then
    derived=$(read_env_var "$CO_ENV" PATH_DERIVED)
    nativepy=$(read_env_var "$CO_ENV" SLURM_NATIVE_PYTHON)
    if [ -n "$derived" ] && [ -n "$nativepy" ]; then
        confirm "Stage the miint extension now (scripts/stage-miint-extension.sh, as $QIITA_ORCH_USER)?"
        sudo -u "$QIITA_ORCH_USER" env PATH_DERIVED="$derived" SLURM_NATIVE_PYTHON="$nativepy" \
            bash "$QIITA_CLONE/scripts/stage-miint-extension.sh"
    else
        echo "PATH_DERIVED / SLURM_NATIVE_PYTHON not both set in $CO_ENV — skipping miint stage"
        echo "(local backend, or stage manually per redeploy.md §6)."
    fi
else
    echo "$CO_ENV not readable — skipping miint stage."
fi

# --- 6. Verify --------------------------------------------------------------
echo "--- [6/7] Verify (health + actions + compute-readiness, correct run-as each) ---"
env QIITA_HOSTNAME="$QIITA_HOSTNAME" QIITA_API_USER="$QIITA_API_USER" QIITA_ORCH_USER="$QIITA_ORCH_USER" \
    "$QIITA_CLONE/deploy/verify.sh"

# --- 7. Report deployed commit + archive hand-off ---------------------------
echo "--- [7/7] Done ---"
commit=$(sudo -u "$QIITA_USER" git -C "$QIITA_CLONE" rev-parse HEAD)
echo "Deployed commit: $commit"
echo "Run every Pending-deploy bucket-5 check + Notes items not covered by verify-deploy."
echo "Then hand off for archiving (maintainer, off-host): /deploy-archive $commit"
echo "(see redeploy.md §8). Record the deployed commit somewhere durable."
