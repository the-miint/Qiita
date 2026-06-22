#!/usr/bin/env bash
# Guided incremental redeploy for an established qiita-miint host — ALL-IN-ONE.
#
# Run ONCE as root (sudo) from your admin account. The script drives the whole
# fixed skeleton of docs/runbooks/redeploy.md and drops into the right account
# for each step via `sudo -u`, so the no-sudo operator account never has to log
# in and the admin never hand-copies per-account verify lines (the source of
# recurring deploy bugs):
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
# The script only STOPS to ask when there is real work or a real decision — it
# does not pause on no-ops:
#   * the buckets 1 & 2 acknowledgement is skipped when both are empty in
#     DEPLOY_CHECKLIST.md (nothing to apply out-of-band → nothing to confirm);
#   * the SLURM native-venv refresh is skipped entirely — no prompt, no `uv sync`
#     — when it is provably already current (the native checkout IS the clone we
#     just pulled, neither qiita-common nor qiita-compute-orchestrator changed in
#     that pull, and the existing venv still imports). A code change, a separate
#     native checkout, or a failing import probe all force the prompt+refresh as
#     before. One gap the skip can't see: a PRIOR run that died mid-`uv sync`
#     leaves a partial venv that may still import — a re-run would see "nothing
#     pulled" and skip it. After an interrupted deploy, re-run with
#     FORCE_NATIVE_REFRESH=1 (or clear the stale venv) to force the resync.
#
# Usage:
#   sudo QIITA_HOSTNAME=qiita-miint.ucsd.edu /home/qiita/qiita-miint/deploy/redeploy.sh
#   (or: sudo make redeploy QIITA_HOSTNAME=qiita-miint.ucsd.edu)
#
# Env: QIITA_HOSTNAME (required), QIITA_USER (default: qiita — the no-sudo
#      operator/checkout owner), QIITA_CLONE (default: this script's parent's
#      parent), ASSUME_YES=1 (skip interactive acks — for automation),
#      RUN_MIGRATE=1 (apply pending migrations here after a typed confirm;
#      default off — leave off for expand/contract deploys), SKIP_STAGE_MIINT=1
#      (skip miint staging entirely), FORCE_STAGE_MIINT=1 (always stage —
#      overrides the "already current" --check skip; use after a mirror bump the
#      HEAD can't see, or to recover a partial stage),
#      SKIP_NATIVE_REFRESH=1 (skip the SLURM native-venv `uv sync` in step 5),
#      FORCE_NATIVE_REFRESH=1 (always refresh it — overrides the "already current"
#      skip; use after a deploy that died mid-`uv sync`).

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

native_pkgs_changed() {
    # Did this pull touch the packages a native SLURM venv runs (qiita-common or
    # qiita-compute-orchestrator)? Uses the pre/post-pull commits captured in step
    # 1; runs the diff as the operator so it works on the operator-owned clone, then
    # delegates the path-prefix match to the pure qiita_paths_touch_native helper.
    #   returns 0 — changed, OR we can't tell (commits unreadable / git failed) →
    #               fail safe to "refresh needed";
    #   returns 1 — provably unchanged (nothing pulled, or no diff in those paths).
    [ -n "${before_head:-}" ] && [ -n "${after_head:-}" ] || return 0
    [ "$before_head" = "$after_head" ] && return 1
    local names
    names=$(sudo -u "$QIITA_USER" git -C "$QIITA_CLONE" diff --name-only \
        "$before_head" "$after_head" 2>/dev/null) || return 0
    qiita_paths_touch_native "$names"
}

echo "=== redeploy: $QIITA_HOSTNAME (clone: $QIITA_CLONE, operator: $QIITA_USER) ==="

# --- 1. Pull source as the operator ----------------------------------------
echo "--- [1/7] Pull source (as $QIITA_USER) ---"
# Capture HEAD either side of the pull so step 5 can tell whether the native venv
# even needs a refresh (did this pull touch qiita-common / qiita-compute-orchestrator?).
before_head=$(sudo -u "$QIITA_USER" git -C "$QIITA_CLONE" rev-parse HEAD 2>/dev/null || true)
sudo -u "$QIITA_USER" git -C "$QIITA_CLONE" pull --ff-only
after_head=$(sudo -u "$QIITA_USER" git -C "$QIITA_CLONE" rev-parse HEAD 2>/dev/null || true)

# --- 2. Pending-deploy buckets 1+2 (manual) + preflight ---------------------
echo "--- [2/7] Env vars + one-time host setup (buckets 1 & 2) ---"
checklist="$QIITA_CLONE/DEPLOY_CHECKLIST.md"
# qiita_buckets_12 echoes the bucket 1+2 text and returns 0 (empty), 1 (real
# steps), or 2 (unreadable / markers absent). Only stop to ask when there is
# something to apply; an unreadable checklist falls back to prompting (fail safe).
if buckets_text=$(qiita_buckets_12 "$checklist"); then
    echo "Buckets 1 (env vars) & 2 (one-time host setup) are empty in DEPLOY_CHECKLIST.md"
    echo "— nothing to apply out-of-band; continuing without a prompt."
else
    [ -n "$buckets_text" ] && printf '%s\n' "$buckets_text"
    echo "Apply any env-var + one-time-host-setup steps above BEFORE continuing —"
    echo "they must be in place before the restart, and stay manual (secrets, dirs, scopes)."
    confirm "Have buckets 1 (env vars) and 2 (one-time host setup) been applied?"
fi

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
    # Resolve dbmate as the operator the same way `make migrate` does: prefer the
    # operator's ~/.local/bin/dbmate (the Makefile's DBMATE_BIN install site — so
    # the pre-check finds it even on a bare service account whose non-interactive
    # login shell doesn't add ~/.local/bin to PATH), falling back to PATH. $HOME
    # here is the operator's home (the inner `bash -lc` login shell), not root's;
    # QIITA_CLONE is passed through `env` so the single-quoted body can use it.
    status=$(sudo -u "$QIITA_USER" env DATABASE_URL="$db_url" QIITA_CLONE="$QIITA_CLONE" bash -lc '
        DBMATE="$HOME/.local/bin/dbmate"; [ -x "$DBMATE" ] || DBMATE=dbmate
        cd "$QIITA_CLONE/qiita-control-plane" && "$DBMATE" --migrations-table public.schema_migrations status
    ' 2>/dev/null) || status=""
    pending_rows=$(printf '%s\n' "$status" | grep -E '^\[ \]' || true)
    if [ -z "$status" ]; then
        echo "WARNING: could not run 'dbmate status' as $QIITA_USER (dbmate not found at"
        echo "         ~/.local/bin/dbmate or on PATH, or the DB was unreachable)."
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
# Native SLURM jobs run from the venv SLURM_NATIVE_PYTHON points at — a separate
# checkout on the shared FS, NOT the /opt/qiita SERVICE venvs local-deploy.sh just
# synced. On any deploy that changed qiita-common or qiita-compute-orchestrator,
# that venv must be refreshed too, or native jobs silently import stale code.
# Both this refresh and the miint stage below feed native jobs, so refresh first.
#
# sudo's secure_path excludes /usr/local/bin on RHEL-family, and a non-login PATH
# (or qiita's login profile) need not carry uv either — `bash -lc` is NOT enough.
# Invoke uv by absolute path, matching activate.sh's $UV (never bare `uv`).
UV=/usr/local/bin/uv
nativepy=""
[ -r "$CO_ENV" ] && nativepy=$(read_env_var "$CO_ENV" SLURM_NATIVE_PYTHON)
if [ -n "${SKIP_NATIVE_REFRESH:-}" ]; then
    echo "Skipping SLURM native-venv refresh (SKIP_NATIVE_REFRESH=1). If qiita-common or"
    echo "qiita-compute-orchestrator changed, refresh it by hand (as its owner $QIITA_USER):"
    echo "    sudo -u $QIITA_USER bash -lc 'cd <native-checkout>/qiita-compute-orchestrator && /usr/local/bin/uv sync --reinstall-package qiita-common'"
elif native_checkout=$(qiita_native_checkout_from_python "$nativepy"); then
    # Skip the refresh entirely — no prompt, no `uv sync` — only when we can PROVE
    # the venv is already current:
    #   (a) the native checkout IS the clone we just pulled (so step 1's before/after
    #       diff actually describes its sources — true on the live single-clone host;
    #       a SEPARATE native checkout this script never pulled can't be reasoned
    #       about from here, so we refresh), AND
    #   (b) neither qiita-common nor qiita-compute-orchestrator changed in the pull, AND
    #   (c) the existing venv still imports what native jobs import.
    # A code change, a separate native checkout, or a failing import probe all fall
    # through to the prompt + refresh below — the skip never drops a refresh an actual
    # change requires. The one case the skip can't detect is a PRIOR run that died
    # mid-`uv sync` (a re-run sees "nothing pulled" + maybe-still-importing partial
    # venv); FORCE_NATIVE_REFRESH=1 overrides the skip for that recovery path.
    native_clone=$(cd "$native_checkout/.." 2>/dev/null && pwd || true)
    deploy_clone=$(cd "$QIITA_CLONE" 2>/dev/null && pwd || true)
    if [ -z "${FORCE_NATIVE_REFRESH:-}" ] \
       && [ -n "$native_clone" ] && [ "$native_clone" = "$deploy_clone" ] \
       && ! native_pkgs_changed \
       && sudo -u "$QIITA_USER" "$nativepy" -c 'import qiita_common, qiita_compute_orchestrator.jobs' 2>/dev/null; then
        echo "Native venv already current — neither qiita-common nor"
        echo "qiita-compute-orchestrator changed in this pull and the venv imports cleanly;"
        echo "skipping the refresh (no work to do)."
    else
        # Reached when the venv is NOT provably current: a code change, a SEPARATE
        # native checkout, or a failing import probe. When it's the SAME clone we
        # just pulled, the refresh is unambiguously needed and there's nothing for
        # the operator to decide — just run it (the "only stop for real work"
        # rule: don't prompt to do necessary work). Prompt ONLY for a separate
        # checkout, where redeploy is about to mutate a tree it didn't pull and
        # can't reason about — that's genuinely the operator's call.
        # Run as the checkout OWNER ($QIITA_USER), never root: a root-owned .venv the
        # operator can't clean is a known footgun. uv by absolute path ($UV) —
        # bare `uv` under `bash -lc` is not reliably on PATH (see $UV above).
        if [ -n "$native_clone" ] && [ "$native_clone" = "$deploy_clone" ]; then
            echo "Native venv needs a refresh (qiita-common / qiita-compute-orchestrator"
            echo "changed, or the import probe failed) — same clone we just pulled, so"
            echo "refreshing automatically (no prompt for necessary work)."
        else
            confirm "Refresh the SLURM native venv ('$UV sync --reinstall-package qiita-common' in $native_checkout, as $QIITA_USER)?"
        fi
        sudo -u "$QIITA_USER" bash -lc "cd '$native_checkout' && '$UV' sync --reinstall-package qiita-common"
        # Fail loud if the just-synced venv can't import what native jobs import — a
        # broken refresh must abort here, not surface as a stale job at the next
        # genome-scale reference-load. (compute-readiness's probe/native-import covers
        # the compute-node side in step 6; this is the cheap head-node check.)
        if ! sudo -u "$QIITA_USER" "$nativepy" -c 'import qiita_common, qiita_compute_orchestrator.jobs'; then
            echo "ERROR: native venv at $native_checkout cannot import qiita_common /" >&2
            echo "       qiita_compute_orchestrator.jobs after the refresh. The /opt/qiita" >&2
            echo "       SERVICE venvs are already deployed and serving (step 4) — only NATIVE" >&2
            echo "       SLURM jobs are at risk. Fix the checkout and re-run (idempotent)." >&2
            exit 1
        fi
        echo "Native venv refreshed and imports verified."
    fi
else
    rc=$?
    # rc=1 → SLURM_NATIVE_PYTHON unset/`python` (local backend): skip cleanly,
    # exactly as the miint stage degrades. rc=2 → a bad derivation already printed
    # its reason to stderr; abort rather than sync a wrong path.
    if [ "$rc" -eq 1 ]; then
        echo "SLURM_NATIVE_PYTHON not set in $CO_ENV — skipping native-venv refresh"
        echo "(local backend, or refresh manually per redeploy.md §6)."
    else
        echo "Refusing to refresh the native venv from a bad SLURM_NATIVE_PYTHON (see above)." >&2
        echo "The /opt/qiita SERVICE venvs are already deployed (step 4); only native SLURM" >&2
        echo "jobs are affected. Fix SLURM_NATIVE_PYTHON in $CO_ENV and re-run." >&2
        exit 1
    fi
fi
if [ -n "${SKIP_STAGE_MIINT:-}" ]; then
    echo "Skipping miint extension staging (SKIP_STAGE_MIINT=1)."
elif [ -r "$CO_ENV" ]; then
    derived=$(read_env_var "$CO_ENV" PATH_DERIVED)
    if [ -n "$derived" ] && [ -n "$nativepy" ]; then
        # Resolve MIINT_EXTENSION_DIRECTORY the SAME way stage-miint-extension.sh
        # does (explicit env var, else PATH_DERIVED/duckdb-ext) so the --check
        # probe and the stage look at the same dir; pass it to both.
        mext=$(read_env_var "$CO_ENV" MIINT_EXTENSION_DIRECTORY)
        [ -z "$mext" ] && mext="${derived%/}/duckdb-ext"
        # Gate staging on real work (no prompt): the --check probe (run as the
        # staging account, same interpreter + env) skips when the staged build
        # still matches the mirror, and stages otherwise (not staged, DuckDB-
        # version/platform change, or a mirror build bump it detects via a HEAD).
        # FORCE_STAGE_MIINT=1 stages unconditionally — for a mirror bump the HEAD
        # somehow can't see, or to recover a partial stage.
        if [ -z "${FORCE_STAGE_MIINT:-}" ] \
           && sudo -u "$QIITA_ORCH_USER" env PATH_DERIVED="$derived" \
                MIINT_EXTENSION_DIRECTORY="$mext" "$nativepy" \
                -m qiita_compute_orchestrator.cli.stage_miint --check; then
            echo "miint extension already current — skipping stage (no work to do)."
        else
            echo "Staging miint extension (not staged / DuckDB or mirror build changed)..."
            sudo -u "$QIITA_ORCH_USER" env PATH_DERIVED="$derived" \
                MIINT_EXTENSION_DIRECTORY="$mext" SLURM_NATIVE_PYTHON="$nativepy" \
                bash "$QIITA_CLONE/scripts/stage-miint-extension.sh"
        fi
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
