#!/usr/bin/env bash
# Atomically install a service-account token at the given path.
#
# Reads plaintext from stdin (avoids process args / shell history). Writes
# to <target>.new with mode 0400, owner qiita:qiita, then renames over the
# target — POSIX same-filesystem rename is atomic, so readers see either
# the old file or the new one, never a truncated one.
#
# If the target already exists, its prior contents are saved at
# <target>.previous before replacement so the rollback procedure in
# docs/runbooks/orchestrator-token-rotation.md can recover it.
#
# Used by docs/runbooks/first-deploy.md (initial install) and
# docs/runbooks/orchestrator-token-rotation.md (rotation install). Trigger
# the orchestrator's SIGHUP handler with `systemctl reload qiita-orchestrator`
# separately — this script only manages the file.
#
# Usage:
#   ./scripts/install-orchestrator-token.sh /etc/qiita/orchestrator.token <<<"$TOKEN"
#
# Override owner/group via env if needed:
#   QIITA_TOKEN_OWNER=qiita QIITA_TOKEN_GROUP=qiita ./scripts/...

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: install-orchestrator-token.sh <target-path>  (token on stdin)" >&2
    exit 2
fi

target="$1"
owner="${QIITA_TOKEN_OWNER:-qiita}"
group="${QIITA_TOKEN_GROUP:-qiita}"

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
