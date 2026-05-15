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

[ "$EUID" -eq 0 ] || { echo "ERROR: run as root (sudo)." >&2; exit 1; }

: "${QIITA_HOSTNAME:?QIITA_HOSTNAME must be set (e.g. qiita.example.org)}"
QIITA_USER="${QIITA_USER:-qiita}"
# QIITA_CLONE must be a git clone (where `git pull` + `make build` work),
# NOT the deployed copy under /opt/qiita/ — that's the install target, not source.
QIITA_CLONE="${QIITA_CLONE:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

id "$QIITA_USER" >/dev/null 2>&1 || { echo "ERROR: account '$QIITA_USER' not found" >&2; exit 1; }
[ -d "$QIITA_CLONE/.git" ] || { echo "ERROR: $QIITA_CLONE is not a git clone" >&2; exit 1; }

[ -z "${SKIP_PULL:-}" ]  && sudo -u "$QIITA_USER" git -C "$QIITA_CLONE" pull --ff-only
[ -z "${SKIP_BUILD:-}" ] && sudo -u "$QIITA_USER" make -C "$QIITA_CLONE" build-data-plane

DP_BINARY="$QIITA_CLONE/qiita-data-plane/target/release/qiita-data-plane"
[ -x "$DP_BINARY" ] || { echo "ERROR: data-plane binary missing at $DP_BINARY" >&2; exit 1; }

INCOMING=/opt/qiita/incoming
install -d -o root -g root -m 0755 "$INCOMING"

rsync -a --delete --chown=root:root "$QIITA_CLONE/qiita-common/"              "$INCOMING/qiita-common/"
rsync -a --delete --chown=root:root "$QIITA_CLONE/qiita-control-plane/"       "$INCOMING/qiita-control-plane/"
rsync -a --delete --chown=root:root "$QIITA_CLONE/qiita-compute-orchestrator/" "$INCOMING/qiita-compute-orchestrator/"
rsync -a --delete --chown=root:root "$QIITA_CLONE/deploy/"                    "$INCOMING/deploy/"
install -m 755 -o root -g root "$DP_BINARY" "$INCOMING/qiita-data-plane"

exec "$QIITA_CLONE/deploy/activate.sh"
