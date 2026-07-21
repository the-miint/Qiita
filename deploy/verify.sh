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
    if command -v grpcurl >/dev/null 2>&1; then
        if grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check >/dev/null 2>&1; then
            pass "health/data-plane" "gRPC localhost:50051 Health/Check OK"
        else
            fail "health/data-plane" "gRPC localhost:50051 Health/Check failed"
        fi
    else
        skip "health/data-plane" "grpcurl not on PATH (run 'make verify-health' to auto-fetch it)"
    fi
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
    # cd / first: qiita-api may not be able to traverse the invoking operator's
    # cwd (deploys are run from NFS home dirs), and a bash -c that cannot resolve
    # its cwd fails before it reaches the python.
    if out=$(cd / && sudo -u "$QIITA_API_USER" bash -c "
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
