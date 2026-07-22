#!/usr/bin/env bash
# Consolidated post-deploy verification for an established qiita-miint host.
#
# Runs the generic post-deploy checks — health aggregate, workflow actions
# list, compute-readiness, CP miint LOAD — each with the account + env file it
# actually needs baked in, so neither operators nor PR authors hand-copy the
# individual invocations. (Hand-copying is how the compute-readiness run-as bug
# recurred across deploys. The correct run-as is qiita-orch sourcing
# compute-orchestrator.env, NOT qiita-api/control-plane.env.)
# Also re-prints the config/secret fingerprint summary (deploy/preflight.sh).
#
# Run as root (it sudo's to the right service account per check and reads the
# 0440 env files):  sudo deploy/verify.sh   (or: make verify-deploy)
#
# Read-only. Exit non-zero iff any ATTEMPTED check failed; absent env files
# (first deploy) degrade to skip rows. Hatches: SKIP_HEALTH, SKIP_ACTIONS,
# SKIP_COMPUTE_READINESS, SKIP_SLURM_PROBE, SKIP_CP_MIINT, SKIP_PREFLIGHT (the
# last passes through to preflight.sh). See docs/runbooks/redeploy.md.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/_common.sh
source "$HERE/_common.sh"  # require_root, CP_ENV/CO_ENV, QIITA_API_USER/QIITA_ORCH_USER, pass/fail/skip

require_root "deploy/verify.sh must be run as root (sudo) — it sudo's per service account and reads the 0440 env files."

# The deployed orchestrator venv. The module-direct form below is PATH-independent
# and is exactly what `qiita-admin compute-readiness` subprocesses into.
ORCHESTRATOR_VENV="${ORCHESTRATOR_VENV:-/opt/qiita/compute-orchestrator/.venv}"
# The deployed CP venv (NOT the /home/qiita build checkout local-deploy.sh rsyncs from).
CONTROL_PLANE_VENV="${CONTROL_PLANE_VENV:-/opt/qiita/control-plane/.venv}"

n_pass=0 n_fail=0 n_skip=0

echo "verify-deploy: post-deploy checks"

# --- 1. Health -------------------------------------------------------------
if [ -n "${SKIP_HEALTH:-}" ]; then
    skip "health" "SKIP_HEALTH=1"
elif [ -n "${QIITA_HOSTNAME:-}" ] && curl -fsS -m 15 "https://${QIITA_HOSTNAME}/health" >/dev/null 2>&1; then
    pass "health" "https://${QIITA_HOSTNAME}/health → 200 (CP+CO+DP aggregate)"
else
    # Fallback: the localhost component checks `make verify-health` does. Used
    # when QIITA_HOSTNAME is unset or TLS isn't up yet (first deploy).
    [ -n "${QIITA_HOSTNAME:-}" ] && note=" (https aggregate unreachable — localhost fallback)" || note=" (QIITA_HOSTNAME unset — localhost fallback)"
    if curl -fsS -m 10 http://localhost:8080/health >/dev/null 2>&1; then
        pass "health/control-plane" "localhost:8080/health → 200${note}"
    else
        fail "health/control-plane" "localhost:8080/health unreachable${note}"
    fi
    if curl -fsS -m 10 http://localhost:8081/health >/dev/null 2>&1; then
        pass "health/compute-orchestrator" "localhost:8081/health → 200"
    else
        fail "health/compute-orchestrator" "localhost:8081/health unreachable"
    fi
fi

# --- 1b. Data-plane pool members -------------------------------------------
# UNCONDITIONAL, deliberately outside the branch above. The aggregate
# `https://<host>/health` answers "is the stack reachable through the edge?" — it
# hits whichever ONE member nginx balanced it to, so it stays green with every
# other instance down. That is the exact failure scaling out exists to avoid, so
# the per-member sweep cannot live in the fallback branch that only runs when the
# aggregate is unavailable.
if [ -n "${SKIP_HEALTH:-}" ]; then
    skip "health/data-plane-pool" "SKIP_HEALTH=1"
elif command -v grpcurl >/dev/null 2>&1; then
    # Every member individually. Checking only :50051 would report a healthy data
    # plane while a scaled-out instance was down — and nginx would keep routing a
    # share of every job's traffic into it.
    #
    # Members come from the RENDERED nginx config — what is actually being served —
    # not from this shell's env. See qiita_data_plane_rendered_members for why, and
    # for the meaning of the three return codes.
    dp_members=$(qiita_data_plane_rendered_members) && dp_rc=0 || dp_rc=$?
    if [ "$dp_rc" -eq 1 ]; then
        # No config rendered yet (first deploy) — env is the only source there is.
        # Assigned first, NOT iterated as `for x in $(f)`: that form swallows f's
        # failure, so a malformed list would make the loop iterate zero times and
        # verify could still exit 0 having checked nothing. The assignment trips
        # errexit.
        dp_ports=$(qiita_data_plane_ports)
        dp_peers=$(qiita_data_plane_peers)
        dp_members=""
        for dp_port in $dp_ports; do dp_members+="127.0.0.1:${dp_port}"$'\n'; done
        for dp_peer in $dp_peers; do dp_members+="${dp_peer}"$'\n'; done
        echo "  data-plane members from env (no rendered nginx upstream yet)"
    elif [ "$dp_rc" -ne 0 ]; then
        # Config present but unparseable. Falling back to env here would check the
        # single default port and report green, which is worse than saying so.
        fail "health/data-plane-pool" "could not read upstream members from $QIITA_NGINX_CONF"
        dp_members=""
    else
        echo "  data-plane members from the rendered nginx upstream"
    fi
    while IFS= read -r dp_member; do
        [ -n "$dp_member" ] || continue
        # Label local vs remote so a red row says which host to go look at. The
        # distinction is derivable from the member itself — activate.sh renders
        # locals as 127.0.0.1:<port> — so it survives the config round-trip.
        case "$dp_member" in
            127.0.0.1:*) dp_label="health/data-plane@${dp_member##*:}" ;;
            *)           dp_label="health/data-plane-peer@${dp_member}" ;;
        esac
        # </dev/null: the loop body inherits the here-string as stdin, so any
        # future stdin-reading command here would eat the remaining members and
        # silently check only the first.
        if grpcurl -plaintext "$dp_member" grpc.health.v1.Health/Check </dev/null >/dev/null 2>&1; then
            pass "$dp_label" "gRPC $dp_member Health/Check OK"
        else
            fail "$dp_label" "gRPC $dp_member Health/Check failed"
        fi
    done <<<"$dp_members"
    # The loopback balancer the control plane talks to. Distinct from the
    # per-instance checks above: this one proves nginx is actually fronting
    # the pool, which is what makes the CP's traffic spread at all.
    # The loopback balancer nginx fronts the pool with. Skipped rather than
    # failed when nginx has no TLS material: activate.sh skips the nginx
    # reload in that case, so the listener legitimately isn't up and a hard
    # fail would just be noise on a non-TLS host.
    lb="127.0.0.1:${QIITA_DATA_PLANE_LB_PORT}"
    if [ ! -r /etc/ssl/certs/qiita.crt ] || [ ! -r /etc/ssl/private/qiita.key ]; then
        skip "health/data-plane-lb" "$lb not checked — nginx not reloaded (no TLS material)"
    elif grpcurl -plaintext "$lb" grpc.health.v1.Health/Check >/dev/null 2>&1; then
        pass "health/data-plane-lb" "gRPC $lb (nginx → pool) Health/Check OK"
    else
        fail "health/data-plane-lb" "gRPC $lb (nginx → pool) Health/Check failed"
    fi
else
    skip "health/data-plane" "grpcurl not on PATH (run 'make verify-health' to auto-fetch it)"
fi

# --- 2. Workflow actions list (as qiita-api, control-plane.env) -------------
if [ -n "${SKIP_ACTIONS:-}" ]; then
    skip "actions" "SKIP_ACTIONS=1"
elif [ -r "$CP_ENV" ]; then
    if out=$(sudo -u "$QIITA_API_USER" bash -c '
        set -a
        # shellcheck disable=SC1091
        source /etc/qiita/control-plane.env  # nounset off: env values may interpolate
        set +a
        set -eo pipefail
        psql "$DATABASE_URL" -Atc "SELECT action_id, version, enabled FROM qiita.action ORDER BY action_id;"
    ' 2>&1); then
        n=$(printf '%s\n' "$out" | grep -c . || true)
        pass "actions" "qiita.action queryable ($n row(s) registered)"
    else
        fail "actions" "failed to query qiita.action: ${out}"
    fi
else
    skip "actions" "$CP_ENV absent (first deploy)"
fi

# --- 3. compute-readiness (as qiita-orch, compute-orchestrator.env) ---------
if [ -n "${SKIP_COMPUTE_READINESS:-}" ]; then
    skip "compute-readiness" "SKIP_COMPUTE_READINESS=1"
elif [ -r "$CO_ENV" ]; then
    probe_flag=""
    [ -n "${SKIP_SLURM_PROBE:-}" ] && probe_flag="--no-slurm-probe"
    # Correct run-as: qiita-orch sourcing compute-orchestrator.env. Module-direct
    # form is PATH-independent (== what `qiita-admin compute-readiness` execs).
    if sudo -u "$QIITA_ORCH_USER" bash -c "
        set -a
        # shellcheck disable=SC1091
        source /etc/qiita/compute-orchestrator.env; set +a
        exec '${ORCHESTRATOR_VENV}/bin/python' -m qiita_compute_orchestrator.cli.compute_readiness ${probe_flag}
    "; then
        pass "compute-readiness" "all checks passed (run as $QIITA_ORCH_USER)"
    else
        fail "compute-readiness" "reported failures (see rows above); re-run as shown in docs/runbooks/redeploy.md §7"
    fi
else
    skip "compute-readiness" "$CO_ENV absent (first deploy)"
fi

# --- 4. CP miint LOAD (as qiita-api, control-plane.env) ---------------------
# The CP runner LOADs miint in-process to stream a sample's masked reads (the
# long-read-assembly input binding). Service-side miint is LOAD-only from the
# deploy-staged MIINT_EXTENSION_DIRECTORY: with it unset DuckDB falls back to
# $HOME/.duckdb/extensions and qiita-api's home is /dev/null, so the whole
# workflow dies at submission. Nothing else fails when it is missing — the CP
# boots and serves every other route — so without this check the gap is
# invisible until someone submits an assembly.
if [ -n "${SKIP_CP_MIINT:-}" ]; then
    skip "cp-miint" "SKIP_CP_MIINT=1"
elif [ -r "$CP_ENV" ]; then
    if out=$(sudo -u "$QIITA_API_USER" bash -c "
        set -a
        # shellcheck disable=SC1091
        source /etc/qiita/control-plane.env; set +a
        exec '${CONTROL_PLANE_VENV}/bin/python' -c 'from qiita_control_plane.miint import connect_with_miint_staged; connect_with_miint_staged().close()'
    " 2>&1); then
        pass "cp-miint" "control plane can LOAD miint (run as $QIITA_API_USER)"
    else
        fail "cp-miint" "control plane cannot LOAD miint — long-read-assembly will fail at submission: ${out}"
    fi
else
    skip "cp-miint" "$CP_ENV absent (first deploy)"
fi

# --- 5. Config/secret fingerprint summary (preflight) -----------------------
echo "verify-deploy: config/secret fingerprints —"
if "$HERE/preflight.sh"; then
    pass "preflight" "config/secret consistency OK"
else
    fail "preflight" "config/secret inconsistency (see preflight rows above)"
fi

echo "verify-deploy: ${n_pass} pass, ${n_fail} fail, ${n_skip} skip"
[ "$n_fail" -eq 0 ]
