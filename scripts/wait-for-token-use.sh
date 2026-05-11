#!/usr/bin/env bash
# Wait until any active token of <principal_idx> records a use.
#
# Captures MAX(last_used_at) at start, polls until it advances, exits 0.
# Exits 1 on timeout (operator should investigate before revoking the old
# token).
#
# Reads qiita.api_tokens directly via DATABASE_URL because: last_used_at
# is intentionally not emitted as an audit event (it would create one row
# per request per minute), and GET /auth/tokens is caller-scoped only —
# the rotation admin cannot read another principal's token list via HTTP.
#
# Used by docs/runbooks/orchestrator-token-rotation.md to confirm the new
# token has been exercised before revoking the old one.
#
# Usage:
#   DATABASE_URL=postgresql://... \
#       ./scripts/wait-for-token-use.sh <principal_idx> [timeout_seconds]

set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "usage: wait-for-token-use.sh <principal_idx> [timeout_seconds]" >&2
    exit 2
fi

principal_idx="$1"
timeout="${2:-180}"
poll_interval=5

if ! [[ "$principal_idx" =~ ^[0-9]+$ ]]; then
    echo "principal_idx must be a non-negative integer, got: $principal_idx" >&2
    exit 2
fi

if [[ -z "${DATABASE_URL:-}" ]]; then
    echo "DATABASE_URL is unset" >&2
    exit 1
fi

query_max() {
    psql "$DATABASE_URL" -tAc \
        "SELECT COALESCE(MAX(last_used_at)::text, '') FROM qiita.api_tokens
          WHERE principal_idx = $principal_idx AND revoked_at IS NULL"
}

baseline="$(query_max)"
echo "baseline last_used_at='${baseline}' for principal_idx=${principal_idx}" >&2

deadline=$(( $(date +%s) + timeout ))
while [[ $(date +%s) -lt $deadline ]]; do
    current="$(query_max)"
    if [[ -n "$current" && "$current" != "$baseline" ]]; then
        echo "token used: last_used_at='${current}'" >&2
        exit 0
    fi
    sleep "$poll_interval"
done

echo "timed out after ${timeout}s — last_used_at did not advance" >&2
echo "do NOT revoke the old token; investigate journalctl -u qiita-compute-orchestrator" >&2
exit 1
