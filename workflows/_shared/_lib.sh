#!/bin/bash
# Shared helpers for EVERY workflow's container entrypoints (long-read-assembly's four
# steps and read-mask's lima). It lived in two byte-identical copies, one per workflow,
# which made "keep the two in lockstep" a load-bearing comment rather than a fact.
#
# Living under _shared/ means the SIF builder hashes it into EVERY image's
# build-inputs digest (`qiita_sif_build_inputs_hash*` in deploy/_common.sh hash the whole
# shared dir, exactly as they do manifest_writer.py). So editing this file rebuilds every
# image — including bcl-convert, which does not source it. That is the accepted cost: an
# occasional redundant rebuild is cheaper than two copies of an executable library that
# silently drift. Each step
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

# The step's resolved allocation: THREADS (cpu) and MEM_MB (memory ceiling).
#
# QIITA_CPUS / QIITA_MEM_MB come straight from the step's resolved
# baseline_resources, forwarded into the container by slurm/payload.py, so a
# per-step cpu/mem change in the workflow YAML needs no entrypoint change.
#
# DO NOT reach for SLURM_CPUS_PER_TASK or SLURM_MEM_PER_NODE here: `apptainer exec
# --containall` scrubs the environment, so NO SLURM_* var survives into a container
# step (measured on the deploy host: zero of them). They are still read below as a
# second choice because this file is also sourced outside a container.
#
# Off SLURM entirely (a local apptainer run) both are unset:
#   * cpu falls back to `nproc`, which is the honest answer there. Note `nproc`
#     reports the CPUSET, not the node — under SLURM it happens to equal the
#     allocation whenever the site cpuset-binds steps, which is why the old
#     SLURM_CPUS_PER_TASK-or-nproc chain produced a correct number for the wrong
#     reason. Do not rely on that accident; QIITA_CPUS is the contract.
#   * memory has NO such fallback available — nothing inside the container exposes
#     the cgroup ceiling — so it takes a deliberately small literal. A tool sized
#     off MEM_MB then under-uses a big dev box rather than OOM-killing a real step.
THREADS="${QIITA_CPUS:-${SLURM_CPUS_PER_TASK:-}}"
if [[ -z "${THREADS}" ]]; then
    THREADS=$(nproc 2>/dev/null || echo 1)
fi
export THREADS

MEM_MB="${QIITA_MEM_MB:-${SLURM_MEM_PER_NODE:-}}"
if [[ -z "${MEM_MB}" ]]; then
    MEM_MB=4096
fi
export MEM_MB

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
