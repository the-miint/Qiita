#!/usr/bin/env bash
# Guided incremental redeploy for an established qiita-miint host.
#
# Codifies the FIXED skeleton of docs/runbooks/redeploy.md so an operator runs
# one command instead of hand-copying steps (the source of recurring deploy
# bugs — issue #72). It does NOT replace the judgment steps: secrets/host setup
# stay manual, and migrations stay OUT-OF-BAND (this wrapper verifies they ran
# and REFUSES otherwise — it never auto-applies, which is unsafe for
# expand/contract changes; activate.sh's guard is the backstop).
#
# Privilege model: invoke as the OPERATOR (the qiita checkout owner), NOT root.
# The wrapper runs git pull / dbmate status as you, and elevates via `sudo` for
# the root-only steps (preflight reads 0440 files; local-deploy.sh asserts root;
# verify sudo's per service account). This matches redeploy.md's
# [operator]/[admin] split and avoids root-owned objects in the qiita checkout.
#
# Usage:
#   QIITA_HOSTNAME=qiita-miint.ucsd.edu deploy/redeploy.sh
#
# Env: QIITA_HOSTNAME (required), QIITA_CLONE (default: this script's parent's
#      parent), DATABASE_URL (for the migration pre-check; same value as
#      control-plane.env), ASSUME_YES=1 (skip interactive acks — for automation),
#      RUN_MIGRATE=1 (opt in to running `make migrate` after a typed confirm;
#      default off — leave off for expand/contract deploys), SKIP_STAGE_MIINT=1.

set -euo pipefail

[ "$EUID" -eq 0 ] && { echo "ERROR: run deploy/redeploy.sh as the OPERATOR, not root — it elevates per-step via sudo (see header)." >&2; exit 1; }
: "${QIITA_HOSTNAME:?QIITA_HOSTNAME must be set (e.g. qiita-miint.ucsd.edu)}"

QIITA_CLONE="${QIITA_CLONE:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
[ -d "$QIITA_CLONE/.git" ] || { echo "ERROR: $QIITA_CLONE is not a git clone" >&2; exit 1; }
command -v sudo >/dev/null 2>&1 || { echo "ERROR: sudo not found — the deploy/preflight/verify steps need root." >&2; exit 1; }
# Surface a sudo password prompt now (or confirm passwordless) rather than mid-deploy.
sudo -v || { echo "ERROR: cannot obtain sudo — required for the deploy/verify steps." >&2; exit 1; }

confirm() {
    # $1 = prompt. Honors ASSUME_YES=1; aborts on anything but an explicit yes.
    [ -n "${ASSUME_YES:-}" ] && { echo "$1 [auto-yes via ASSUME_YES=1]"; return 0; }
    local reply
    read -r -p "$1 [y/N] " reply
    [ "$reply" = "y" ] || [ "$reply" = "Y" ] || { echo "Aborted." >&2; exit 1; }
}

echo "=== redeploy: $QIITA_HOSTNAME (clone: $QIITA_CLONE) ==="

# --- 1. Pull source as the operator ----------------------------------------
echo "--- [1/7] Pulling source (as $(whoami)) ---"
git -C "$QIITA_CLONE" pull --ff-only

# --- 2. Pending-deploy buckets 1+2 (manual) + preflight ---------------------
echo "--- [2/7] Env vars + one-time host setup (buckets 1 & 2) ---"
checklist="$QIITA_CLONE/DEPLOY_CHECKLIST.md"
if [ -r "$checklist" ]; then
    # Print buckets 1 and 2 (everything from the Env-vars header up to Migrations).
    sed -n '/^### 1\. Env vars/,/^### 3\. Migrations/p' "$checklist" | sed '$d'
fi
echo "Apply any env-var and one-time-host-setup steps above BEFORE continuing —"
echo "they must be in place before the restart, and stay operator-driven (secrets, dirs, scopes)."
confirm "Have buckets 1 (env vars) and 2 (one-time host setup) been applied?"

echo "--- Running config/secret preflight (read-only) ---"
sudo QIITA_HOSTNAME="$QIITA_HOSTNAME" "$QIITA_CLONE/deploy/preflight.sh"

# --- 3. Migration gate (verify-and-refuse; never auto-applies) --------------
echo "--- [3/7] Migration gate (out-of-band — this wrapper does not auto-apply) ---"
DBMATE_BIN=""
for cand in "$HOME/.local/bin/dbmate" "$(command -v dbmate 2>/dev/null || true)"; do
    [ -n "$cand" ] && [ -x "$cand" ] && { DBMATE_BIN="$cand"; break; }
done
pending=""
if [ -n "$DBMATE_BIN" ] && [ -n "${DATABASE_URL:-}" ]; then
    status=$(cd "$QIITA_CLONE/qiita-control-plane" && "$DBMATE_BIN" --migrations-table public.schema_migrations status 2>/dev/null || true)
    printf '%s\n' "$status" | grep -E '^\[ \]' && pending="yes" || true
    if [ -n "$pending" ]; then
        echo ""
        echo "Pending migrations detected (see '[ ]' rows above)."
        if [ -n "${RUN_MIGRATE:-}" ]; then
            echo "RUN_MIGRATE=1 set. Review the expand/contract caution in redeploy.md §5"
            echo "before applying — a contract migration must NOT ship with code that stops"
            echo "using the old column unless every instance is already on the new code."
            confirm "Apply these migrations now with 'make migrate'?"
            make -C "$QIITA_CLONE" migrate
        else
            echo "STOP: apply them out-of-band first, then re-run this script:"
            echo "    make -C $QIITA_CLONE migrate"
            echo "(See redeploy.md §4–§5 for the expand/contract caution. Or re-run with"
            echo " RUN_MIGRATE=1 to apply here after a typed confirmation.)"
            exit 1
        fi
    else
        echo "No pending migrations."
    fi
else
    echo "WARNING: cannot pre-check migrations (need dbmate + DATABASE_URL in your shell)."
    echo "         activate.sh's migration guard will still refuse to restart onto a stale"
    echo "         schema — but run 'make -C $QIITA_CLONE migrate' first to be sure."
    confirm "Continue without the local migration pre-check?"
fi

# --- 4. Deploy --------------------------------------------------------------
echo "--- [4/7] Deploy (sudo local-deploy.sh; SKIP_PULL=1 — already pulled) ---"
sudo SKIP_PULL=1 QIITA_HOSTNAME="$QIITA_HOSTNAME" "$QIITA_CLONE/deploy/local-deploy.sh"

# --- 5. SLURM native-venv refresh + miint staging (recurring footguns) ------
echo "--- [5/7] SLURM native env (redeploy.md §6) ---"
echo "REMINDER: local-deploy.sh only synced the /opt/qiita SERVICE venvs."
echo "Native SLURM jobs run from the SLURM_NATIVE_PYTHON checkout on the shared FS;"
echo "on any deploy that changed qiita-common or qiita-compute-orchestrator you must"
echo "refresh it too, or native jobs import stale code (and keep a stale cached miint):"
echo "    cd <native-checkout>/qiita-compute-orchestrator && uv sync --reinstall-package qiita-common"
# Stage the shared miint extension if the orchestrator env declares its dir.
CO_ENV=/etc/qiita/compute-orchestrator.env
if [ -n "${SKIP_STAGE_MIINT:-}" ]; then
    echo "Skipping miint extension staging (SKIP_STAGE_MIINT=1)."
elif sudo test -r "$CO_ENV"; then
    derived=$(sudo bash -c "set -a; source '$CO_ENV'; set +a; printf '%s' \"\${PATH_DERIVED:-}\"")
    nativepy=$(sudo bash -c "set -a; source '$CO_ENV'; set +a; printf '%s' \"\${SLURM_NATIVE_PYTHON:-}\"")
    if [ -n "$derived" ] && [ -n "$nativepy" ]; then
        confirm "Stage the miint extension now (scripts/stage-miint-extension.sh)?"
        sudo -u qiita-orch env PATH_DERIVED="$derived" SLURM_NATIVE_PYTHON="$nativepy" \
            bash "$QIITA_CLONE/scripts/stage-miint-extension.sh"
    else
        echo "PATH_DERIVED / SLURM_NATIVE_PYTHON not both set in $CO_ENV — skipping miint stage"
        echo "(local backend, or stage manually per redeploy.md §6)."
    fi
fi

# --- 6. Verify --------------------------------------------------------------
echo "--- [6/7] Verify ---"
sudo QIITA_HOSTNAME="$QIITA_HOSTNAME" "$QIITA_CLONE/deploy/verify.sh"

# --- 7. Report deployed commit + archive hand-off ---------------------------
echo "--- [7/7] Done ---"
commit=$(git -C "$QIITA_CLONE" rev-parse HEAD)
echo "Deployed commit: $commit"
echo "Run every Pending-deploy bucket-5 check + Notes items not covered by verify-deploy."
echo "Then hand off for archiving (maintainer, off-host): /deploy-archive $commit"
echo "(see redeploy.md §8). Record the deployed commit somewhere durable."
