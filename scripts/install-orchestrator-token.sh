#!/usr/bin/env bash
# Atomically install a service-account token at the given path.
#
# Reads plaintext from stdin (avoids process args / shell history). Writes
# to <target>.new with mode 0400, owner qiita-orch:qiita-orch, then renames
# over the target — POSIX same-filesystem rename is atomic, so readers see
# either the old file or the new one, never a truncated one.
#
# If the target already exists, its prior contents are saved at
# <target>.previous before replacement so the rollback procedure in
# docs/runbooks/orchestrator-token-rotation.md can recover it.
#
# Used by docs/runbooks/first-deploy.md (initial install) and
# docs/runbooks/orchestrator-token-rotation.md (rotation install). For
# long-running daemons, trigger the SIGHUP handler with `systemctl reload
# qiita-compute-orchestrator` separately — this script only manages the
# file. (Note: the orchestrator's reload handler is not yet implemented in
# v1; see the rotation runbook's status banner.)
#
# Usage:
#   ./scripts/install-orchestrator-token.sh /etc/qiita/orchestrator.token <<<"$TOKEN"
#
# Override owner/group via QIITA_TOKEN_OWNER / QIITA_TOKEN_GROUP env vars
# if needed (e.g. during a re-shaped deploy).

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: install-orchestrator-token.sh <target-path>  (token on stdin)" >&2
    exit 2
fi

target="$1"
owner="${QIITA_TOKEN_OWNER:-qiita-orch}"
group="${QIITA_TOKEN_GROUP:-qiita-orch}"

if [[ -t 0 ]]; then
    echo "refusing to read token from a tty — pipe stdin" >&2
    exit 2
fi

target_dir="$(dirname "$target")"
if [[ ! -d "$target_dir" ]]; then
    echo "target directory does not exist: $target_dir" >&2
    exit 1
fi

if [[ -f "$target" ]]; then
    cp -p "$target" "${target}.previous"
fi

install -m 0400 -o "$owner" -g "$group" /dev/stdin "${target}.new"
mv -f "${target}.new" "$target"

echo "installed token at $target (mode 0400, owner ${owner}:${group})" >&2
