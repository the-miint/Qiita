#!/usr/bin/env bash
# Single-command manual deploy: pull + build (as $QIITA_USER) → stage → activate.
# The CI-less equivalent of the deploy pipeline. CI invokes deploy/activate.sh
# directly after rsyncing pre-built artifacts to /opt/qiita/incoming/; this
# script does the rsync part locally from a git clone on the deploy host.
# See docs/runbooks/first-deploy.md for the surrounding flow.
#
# Usage:
#   sudo QIITA_HOSTNAME=qiita.example.org /home/qiita/qiita-miint/deploy/local-deploy.sh
#
# Env: QIITA_HOSTNAME (required), QIITA_USER (default: qiita),
#      QIITA_CLONE (default: this script's parent), SKIP_PULL=1, SKIP_BUILD=1.

set -euo pipefail

# shellcheck source=deploy/_common.sh
source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"  # require_root, qiita_resolve_user_clone, RSYNC_EXCLUDES

require_root "run as root (sudo)."

: "${QIITA_HOSTNAME:?QIITA_HOSTNAME must be set (e.g. qiita.example.org)}"
# Sets + validates QIITA_USER (default qiita) and QIITA_CLONE — the git clone
# where `git pull` + `make build` run, NOT the deployed /opt/qiita copy.
qiita_resolve_user_clone

[ -z "${SKIP_PULL:-}" ]  && sudo -u "$QIITA_USER" git -C "$QIITA_CLONE" pull --ff-only
[ -z "${SKIP_BUILD:-}" ] && sudo -u "$QIITA_USER" make -C "$QIITA_CLONE" build-data-plane

# Stamp the deployed commit so the CP landing page can show it. Captured
# here (the only stage with a git clone — /opt/qiita has no .git) and
# handed to activate.sh via the environment; activate.sh writes it into
# the deploy-owned build.env the systemd unit reads. The CI deploy path
# sets QIITA_BUILD_SHA from GITHUB_SHA before invoking activate.sh.
# Pass the FULL 40-char SHA (like CI's GITHUB_SHA); activate.sh owns the
# single truncation site so both paths get an identically-shaped short
# SHA. Non-fatal: an unset SHA just leaves the footer version-only.
QIITA_BUILD_SHA="$(sudo -u "$QIITA_USER" git -C "$QIITA_CLONE" rev-parse HEAD 2>/dev/null || true)"
export QIITA_BUILD_SHA

DP_BINARY="$QIITA_CLONE/qiita-data-plane/target/release/qiita-data-plane"
[ -x "$DP_BINARY" ] || { echo "ERROR: data-plane binary missing at $DP_BINARY" >&2; exit 1; }

INCOMING=/opt/qiita/incoming
install -d -o root -g root -m 0755 "$INCOMING"

rsync -a --delete --chown=root:root "${RSYNC_EXCLUDES[@]}" "$QIITA_CLONE/qiita-common/"              "$INCOMING/qiita-common/"
rsync -a --delete --chown=root:root "${RSYNC_EXCLUDES[@]}" "$QIITA_CLONE/qiita-control-plane/"       "$INCOMING/qiita-control-plane/"
rsync -a --delete --chown=root:root "${RSYNC_EXCLUDES[@]}" "$QIITA_CLONE/qiita-compute-orchestrator/" "$INCOMING/qiita-compute-orchestrator/"
rsync -a --delete --chown=root:root "${RSYNC_EXCLUDES[@]}" "$QIITA_CLONE/workflows/"                 "$INCOMING/workflows/"
rsync -a --delete --chown=root:root "${RSYNC_EXCLUDES[@]}" "$QIITA_CLONE/deploy/"                    "$INCOMING/deploy/"
# scripts/ carries build-sif.sh, which activate.sh's SIF auto-build (build-sifs.sh)
# invokes. On this local path activate.sh runs from $QIITA_CLONE (which has it), but
# the CI path runs activate.sh from $INCOMING — stage it so that path finds it too
# (else SIF auto-build cleanly skips). Cheap; keeps $INCOMING a self-contained tree.
rsync -a --delete --chown=root:root "${RSYNC_EXCLUDES[@]}" "$QIITA_CLONE/scripts/"                   "$INCOMING/scripts/"
install -m 0755 -o root -g root "$DP_BINARY" "$INCOMING/qiita-data-plane"

# Explicit export so QIITA_HOSTNAME crosses the exec boundary into
# activate.sh regardless of how the invoker provided it (`VAR=val script`
# already exports for the script's lifetime, but explicit is defensive).
export QIITA_HOSTNAME
exec "$QIITA_CLONE/deploy/activate.sh"
