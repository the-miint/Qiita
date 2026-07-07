#!/bin/bash
# Shared helpers for the pacbio-processing per-step entrypoints. Each step
# sources this, reads its inputs from params.json, runs its tool writing under
# $QIITA_OUTPUT_PATH, then calls qiita_finish to emit the manifest and apply the
# 0440 (files) / 0550 (dirs) mode contract the data-plane verifier requires.
set -euo pipefail

if [[ -z "${QIITA_INPUT_PATH:-}" ]]; then
    echo "QIITA_INPUT_PATH not set — orchestrator did not propagate the input dir" >&2
    exit 64
fi
if [[ -z "${QIITA_OUTPUT_PATH:-}" ]]; then
    echo "QIITA_OUTPUT_PATH not set — orchestrator did not propagate the output dir" >&2
    exit 64
fi

PARAMS_JSON="${QIITA_INPUT_PATH}/params.json"
if [[ ! -f "${PARAMS_JSON}" ]]; then
    echo "params.json not found at ${PARAMS_JSON}" >&2
    exit 64
fi

# Thread count from the SLURM allocation (cpu in baseline_resources); 1 off-SLURM.
# Exported so the tool subprocesses (and sourcing entrypoints) both see it.
export THREADS="${SLURM_CPUS_PER_TASK:-1}"

# Read a required .inputs.<key> host path (or scalar) from params.json.
qiita_input() { jq -er ".inputs.$1" "${PARAMS_JSON}"; }

# Emit manifest.json mapping <name>=<relpath> pairs, then apply the mode
# contract: files 0440 (verified), dirs 0550 (traversable so the verifier's
# rglob can descend). -mindepth 1 skips the host-owned QIITA_OUTPUT_PATH itself.
qiita_finish() {
    python3 /opt/qiita/manifest_writer.py "${QIITA_OUTPUT_PATH}" "$@"
    find "${QIITA_OUTPUT_PATH}" -mindepth 1 -type d -exec chmod 0550 {} +
    find "${QIITA_OUTPUT_PATH}" -mindepth 1 -type f -exec chmod 0440 {} +
}
