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
#   * the two venv refreshes (steps 5 and 6) are NOT among the things it can skip.
#     They run every time, and prompt only for a native checkout this script did
#     not pull — see step 5 for why "provably already current" is not something
#     this script can establish. Together they close a three-tree gap: activate.sh
#     refreshes only the /opt/qiita SERVICE venvs, step 5 the SLURM NATIVE venv,
#     and step 6 the operator's CHECKOUT CLI venv that `uv run qiita` /
#     `qiita-admin` run from, which nothing else touches.
#
# Step 1 pulls the clone this script lives in, so the pull can replace this file
# and the _common.sh it sourced while they are running. When it does, the script
# re-execs the pulled copy; without that, steps 2-8 keep running the code from
# before the pull (see step 1). QIITA_REDEPLOY_REEXECED marks the re-exec so it
# cannot loop.
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
#      SKIP_NATIVE_REFRESH=1 / SKIP_CLI_REFRESH=1 (skip the step-5 / step-6
#      `uv sync`; both refreshes otherwise run every deploy and abort it on
#      failure). FORCE_NATIVE_REFRESH=1 / FORCE_CLI_REFRESH=1 no longer do
#      anything — they overrode a skip that no longer exists. Where the refresh
#      runs, the script says so and refreshes regardless; where it is skipped
#      (SKIP_*, or step 5 with SLURM_NATIVE_PYTHON unset) they are silent.

set -euo pipefail

# shellcheck source=deploy/_common.sh
source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"  # require_root, qiita_resolve_user_clone, read_env_var, CP_ENV/CO_ENV, QIITA_*_USER

require_root "run deploy/redeploy.sh as root (sudo) from your admin account — it drops into ${QIITA_USER:-qiita} / ${QIITA_API_USER} / ${QIITA_ORCH_USER} per step via sudo -u (see header)."
: "${QIITA_HOSTNAME:?QIITA_HOSTNAME must be set (e.g. qiita-miint.ucsd.edu)}"

# Sets + validates QIITA_USER (operator/checkout owner) and QIITA_CLONE (the git
# clone where pull/build/migrate run, NOT the deployed /opt/qiita copy).
qiita_resolve_user_clone

# Absolute path to this script, for the step-1 re-exec. Resolved before the pull
# can move anything, and independent of the caller's cwd (`make redeploy` invokes
# it as the relative `deploy/redeploy.sh`).
SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"

confirm() {
    # $1 = prompt. Honors ASSUME_YES=1; aborts on anything but an explicit yes.
    [ -n "${ASSUME_YES:-}" ] && { echo "$1 [auto-yes via ASSUME_YES=1]"; return 0; }
    local reply
    read -r -p "$1 [y/N] " reply
    [ "$reply" = "y" ] || [ "$reply" = "Y" ] || { echo "Aborted." >&2; exit 1; }
}

echo "=== redeploy: $QIITA_HOSTNAME (clone: $QIITA_CLONE, operator: $QIITA_USER) ==="

# --- 1. Pull source as the operator ----------------------------------------
echo "--- [1/8] Pull source (as $QIITA_USER) ---"
# No before/after HEAD capture: it existed so steps 5 and 6 could skip a venv
# refresh when "nothing arrived in this pull", and that is not evidence a venv is
# current — an operator who pulls before running the deploy makes every pull a
# no-op. Both refreshes are unconditional now (see step 5).
#
# A fingerprint IS taken either side of the pull, for a different question. The
# pull rewrites the clone this script lives in, so it can replace redeploy.sh and
# _common.sh while they are running. `git checkout` replaces a tracked file by
# rename, so the running bash keeps reading the pre-pull inode and steps 2-8
# execute the code from BEFORE the pull: a deploy that ships a change to the
# deploy script does not run that change, and nothing in the log says so. It has
# already cost one deploy — the pull that made the step-5 native-venv refresh
# unconditional was itself run by the old conditional script, which skipped the
# refresh and left native SLURM jobs importing a stale qiita_common.
#
# Only this process is affected: every child script (preflight.sh,
# local-deploy.sh, verify.sh) is a fresh `bash` that reads the pulled bytes. So
# the fingerprint covers exactly what this process already read — itself and the
# _common.sh it sourced — and the recovery is to re-exec.
self_before=$(qiita_deploy_self_fingerprint "$SELF")
sudo -u "$QIITA_USER" git -C "$QIITA_CLONE" pull --ff-only
self_after=$(qiita_deploy_self_fingerprint "$SELF")
if [ "$self_after" != "$self_before" ]; then
    if [ -n "${QIITA_REDEPLOY_REEXECED:-}" ]; then
        # Re-execing again would loop. Reaching here means the clone changed a
        # second time (a concurrent write, or a pull that is not converging);
        # carrying on with the running copy is what terminates.
        echo "WARNING: redeploy.sh / _common.sh changed again after the re-exec —" >&2
        echo "         continuing with the running copy instead of re-execing in a" >&2
        echo "         loop. Re-run the deploy once the clone is settled." >&2
    else
        echo "The pull changed deploy/redeploy.sh or deploy/_common.sh — re-execing the"
        echo "pulled version so steps 2-8 run the code that was just pulled. Step 1"
        echo "repeats below; its pull is a no-op the second time."
        # Exported so the re-exec'd process sees it. No positional arguments to
        # forward — this script is driven entirely by the environment, which exec
        # carries over.
        export QIITA_REDEPLOY_REEXECED=1
        exec bash "$SELF"
    fi
fi

# --- 2. Pending-deploy buckets 1+2 (manual) + preflight ---------------------
echo "--- [2/8] Env vars + one-time host setup (buckets 1 & 2) ---"
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
echo "--- [3/8] Migration gate (out-of-band — this wrapper does not auto-apply) ---"
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
echo "--- [4/8] Deploy (local-deploy.sh; SKIP_PULL=1 — already pulled in step 1) ---"
env SKIP_PULL=1 QIITA_HOSTNAME="$QIITA_HOSTNAME" QIITA_USER="$QIITA_USER" QIITA_CLONE="$QIITA_CLONE" \
    "$QIITA_CLONE/deploy/local-deploy.sh"

# uv by ABSOLUTE path: sudo's secure_path excludes /usr/local/bin on RHEL-family,
# and a non-login PATH (or qiita's login profile) need not carry uv either —
# `bash -lc` is NOT enough. Matches activate.sh's $UV (never bare `uv`). Shared by
# the SLURM native-venv refresh (step 5) AND the checkout CLI-venv refresh (step 6),
# so it's defined here rather than scoped inside either step.
UV=/usr/local/bin/uv

# --- 5. SLURM native-venv refresh + miint staging (recurring footguns) ------
echo "--- [5/8] SLURM native env (redeploy.md §6) ---"
# Native SLURM jobs run from the venv SLURM_NATIVE_PYTHON points at — a separate
# checkout on the shared FS, NOT the /opt/qiita SERVICE venvs local-deploy.sh just
# synced. That venv is refreshed on every deploy, or native jobs silently import
# stale code. Both this refresh and the miint stage below feed native jobs, so
# refresh first.
nativepy=""
[ -r "$CO_ENV" ] && nativepy=$(read_env_var "$CO_ENV" SLURM_NATIVE_PYTHON)
if [ -n "${SKIP_NATIVE_REFRESH:-}" ]; then
    echo "Skipping SLURM native-venv refresh (SKIP_NATIVE_REFRESH=1). Refresh it by hand"
    echo "before native jobs run — do NOT reason from 'nothing changed in this pull', which"
    echo "is how two deploys shipped a stale venv. As its owner $QIITA_USER:"
    echo "    sudo -u $QIITA_USER bash -lc 'cd <native-checkout>/qiita-compute-orchestrator && $UV sync --reinstall-package qiita-common'"
elif native_checkout=$(qiita_native_checkout_from_python "$nativepy"); then
    # The refresh is UNCONDITIONAL: nothing available at deploy time establishes
    # that a venv is current, and two production incidents came of believing
    # otherwise. Which two, and why neither an import probe nor "nothing arrived in
    # this pull" can see them, is on
    # `qiita_compute_orchestrator.native_import_check`.
    #
    # `--reinstall-package qiita-common`, not a plain `uv sync` (CLAUDE.md,
    # "Cross-package staleness"). Step 6 syncs the CLI venv the same way.
    #
    # FORCE_NATIVE_REFRESH is read only to tell the operator it is now redundant.
    [ -n "${FORCE_NATIVE_REFRESH:-}" ] && \
        echo "(FORCE_NATIVE_REFRESH is set and no longer needed — the refresh is unconditional.)"
    native_clone=$(cd "$native_checkout/.." 2>/dev/null && pwd || true)
    deploy_clone=$(cd "$QIITA_CLONE" 2>/dev/null && pwd || true)
    # Prompt ONLY for a SEPARATE checkout, where redeploy is about to mutate a tree
    # it did not pull and cannot reason about — that is genuinely the operator's
    # call. For the same clone we just pulled there is nothing to decide, so we do
    # not stop ("only stop for real work" — don't prompt to do necessary work).
    if [ -n "$native_clone" ] && [ "$native_clone" = "$deploy_clone" ]; then
        echo "Refreshing the SLURM native venv (as $QIITA_USER):"
        echo "    cd $native_checkout && $UV sync --reinstall-package qiita-common"
    else
        # A native checkout OUTSIDE the deploy clone; the live host is single-clone,
        # so this is the branch not taken there. Scope of the refresh below on this
        # path: `uv sync` resolves against the tree it runs in, and this script never
        # pulled that tree, so a tree that is old but self-consistent syncs and
        # imports its own old symbols. Establishing that it is current needs its HEAD
        # compared against $QIITA_CLONE's; the sync alone does not.
        confirm "Refresh the SLURM native venv ('$UV sync --reinstall-package qiita-common' in $native_checkout, as $QIITA_USER)?"
    fi
    # Run as the checkout OWNER ($QIITA_USER), never root: a root-owned .venv the
    # operator can't clean is a known footgun. uv by absolute path ($UV) — bare
    # `uv` under `bash -lc` is not reliably on PATH (see $UV above).
    sudo -u "$QIITA_USER" bash -lc "cd '$native_checkout' && '$UV' sync --reinstall-package qiita-common"
    # Fail loud if the just-synced venv can't import what native jobs import — a
    # broken refresh must abort here, not surface as a stale job at the next
    # genome-scale reference-load. This is the cheap head-node check; compute-readiness
    # runs the same module on a COMPUTE node in step 7.
    if ! sudo -u "$QIITA_USER" "$nativepy" -P -m qiita_compute_orchestrator.native_import_check; then
        echo "ERROR: native venv at $native_checkout cannot import qiita_common /" >&2
        echo "       qiita_compute_orchestrator.config / every dispatchable job module" >&2
        echo "       after the refresh. The failing module and error are printed" >&2
        echo "       above. The /opt/qiita SERVICE venvs are already deployed and serving" >&2
        echo "       (step 4) — only NATIVE SLURM jobs are at risk." >&2
        echo "       Re-run this script (idempotent), or by hand — copy the command" >&2
        echo "       below as-is, absolute uv path included (see the \$UV note above):" >&2
        echo "         sudo -u $QIITA_USER bash -lc \"cd '$native_checkout' && $UV sync --reinstall-package qiita-common\"" >&2
        exit 1
    fi
    echo "Native venv refreshed and imports verified."
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

# --- 6. Operator checkout CLI-venv refresh (the two-tree gap) ----------------
echo "--- [6/8] Operator checkout CLI venv (uv run qiita / qiita-admin) ---"
# Two-tree footgun the rest of the deploy does NOT cover: operators run the
# interactive CLI (`uv run qiita ...` / `qiita-admin ...`) straight from the git
# CHECKOUT's qiita-control-plane venv ($QIITA_CLONE/qiita-control-plane/.venv).
# But:
#   * activate.sh `uv sync`s only the /opt/qiita SERVICE venvs (the running CP/CO
#     daemons) — not the checkout;
#   * step 5 above refreshes the SLURM NATIVE venv (qiita-compute-orchestrator),
#     not qiita-control-plane.
# So after a pull that changes qiita-common WITHOUT a version bump, a plain
# `uv sync` SKIPS reinstalling the unchanged-version path-dep, leaving stale
# qiita-common sources in the checkout CLI venv's site-packages — and the next
# `uv run qiita` ImportErrors on a symbol the pulled qiita-common added. The
# manual fix has been `uv sync --reinstall-package qiita-common` in the checkout;
# do it here so a routine redeploy leaves the operator CLI working.
#
# Run as the checkout OWNER ($QIITA_USER), never root — a root-owned .venv the
# operator can't clean is the same footgun the native refresh calls out. uv by
# absolute path ($UV, defined above step 5).
#
# Verification mechanism differs from step 5 on purpose: the native step has an
# explicit interpreter path (SLURM_NATIVE_PYTHON) it invokes directly, but there is
# no such configured path for the checkout CLI venv — so we reach its interpreter
# via `$UV run --no-sync python -c ...` (no-sync = probe only, never mutate)
# instead of hardcoding `.venv/bin/python`.
#
# Unconditional, for the reason step 5 states: an import probe cannot establish
# that a venv is current, and "nothing arrived in this pull" says nothing about
# whether an earlier deploy synced it. FORCE_CLI_REFRESH is kept as a no-op alias
# so a runbook that still passes it gets what it asked for.
#
# BOTH entrypoints are imported, not just cli.user (`qiita`). cli.admin
# (`qiita-admin`) imports names no cli.user closure reaches — SYSTEM_PRINCIPAL_IDX
# and TERMINAL_WORK_TICKET_STATES are admin-only at module level, and the user
# side's own path to the latter is a deferred in-function import — so a stale
# qiita_common missing either leaves a cli.user-only probe green and qiita-admin
# broken. That is the missing-NAME shape step 5 exists to catch, so the verification
# here has to cover the closure it claims to. The remedy is unchanged either way:
# the sync refreshes the whole venv.
cli_checkout="$QIITA_CLONE/qiita-control-plane"
if [ -n "${SKIP_CLI_REFRESH:-}" ]; then
    # The escape hatch step 5 has always had. It matters more now than it did: this
    # refresh runs on every deploy, and it aborts the script — so without an opt-out
    # a `uv sync` that fails for its own reasons (a network blip reaching the index)
    # takes down a deploy whose services are already up, before step 7 verifies them.
    echo "Skipping operator checkout CLI-venv refresh (SKIP_CLI_REFRESH=1). Refresh it"
    echo "by hand before using \`uv run qiita\` / \`qiita-admin\` (as $QIITA_USER):"
    echo "    sudo -u $QIITA_USER bash -lc 'cd $cli_checkout && $UV sync --reinstall-package qiita-common'"
elif [ ! -d "$cli_checkout" ]; then
    echo "No $cli_checkout — skipping CLI-venv refresh (unexpected layout)."
else
    # Read only to tell the operator it is now redundant, and only where the
    # refresh actually runs — the same placement step 5 uses, so SKIP_* wins on
    # both steps rather than printing "unconditional" and then skipping.
    [ -n "${FORCE_CLI_REFRESH:-}" ] && \
        echo "(FORCE_CLI_REFRESH is set and no longer needed — the refresh is unconditional.)"
    echo "Refreshing the operator checkout CLI venv (as $QIITA_USER):"
    echo "    cd $cli_checkout && $UV sync --reinstall-package qiita-common"
    sudo -u "$QIITA_USER" bash -lc "cd '$cli_checkout' && '$UV' sync --reinstall-package qiita-common"
    # Fail loud if the just-synced venv still can't import the CLI entrypoint — a
    # broken refresh must abort here, not surface as an ImportError the next time
    # the operator reaches for the CLI.
    if ! sudo -u "$QIITA_USER" bash -lc "cd '$cli_checkout' && '$UV' run --no-sync python -c 'import qiita_control_plane.cli.user, qiita_control_plane.cli.admin'"; then
        echo "ERROR: checkout CLI venv at $cli_checkout cannot import" >&2
        echo "       qiita_control_plane.cli.user / .admin after the refresh. The /opt/qiita SERVICE" >&2
        echo "       venvs are already deployed and serving (step 4) — only the operator's" >&2
        echo "       interactive CLI is affected." >&2
        echo "       Re-run this script (idempotent), or by hand — copy the command" >&2
        echo "       below as-is, absolute uv path included (see the \$UV note above):" >&2
        echo "         sudo -u $QIITA_USER bash -lc \"cd '$cli_checkout' && $UV sync --reinstall-package qiita-common\"" >&2
        exit 1
    fi
    echo "Checkout CLI venv refreshed and imports verified."
fi

# --- 7. Verify --------------------------------------------------------------
echo "--- [7/8] Verify (health + actions + compute-readiness, correct run-as each) ---"
env QIITA_HOSTNAME="$QIITA_HOSTNAME" QIITA_API_USER="$QIITA_API_USER" QIITA_ORCH_USER="$QIITA_ORCH_USER" \
    "$QIITA_CLONE/deploy/verify.sh"

# --- 8. Report deployed commit + archive hand-off ---------------------------
echo "--- [8/8] Done ---"
commit=$(sudo -u "$QIITA_USER" git -C "$QIITA_CLONE" rev-parse HEAD)
echo "Deployed commit: $commit"
echo "Run every Pending-deploy bucket-5 check + Notes items not covered by verify-deploy."
echo "Then hand off for archiving (maintainer, off-host): /deploy-archive $commit"
echo "(see redeploy.md §8). Record the deployed commit somewhere durable."
