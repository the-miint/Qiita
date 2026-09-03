#!/bin/bash
# Container entrypoint for the bcl-convert step of the bcl-convert workflow.
#
# Read the inputs from params.json (the SLURM native-step launcher writes
# this on the host before exec'ing apptainer), run bcl-convert against the
# rehydrated sample sheet and BCL run folder, emit the per-step manifest,
# and chmod every output *file* to 0440 — the data-plane verifier rejects
# any output file at a stricter or looser mode. Directories are left
# traversable (0550) so the verifier's rglob walk can descend.
set -euo pipefail

if [[ -z "${QIITA_INPUT_PATH:-}" ]]; then
    echo "QIITA_INPUT_PATH not set — orchestrator did not propagate the per-step input dir" >&2
    exit 64
fi
if [[ -z "${QIITA_OUTPUT_PATH:-}" ]]; then
    echo "QIITA_OUTPUT_PATH not set — orchestrator did not propagate the per-step output dir" >&2
    exit 64
fi

PARAMS_JSON="${QIITA_INPUT_PATH}/params.json"
if [[ ! -f "${PARAMS_JSON}" ]]; then
    echo "params.json not found at ${PARAMS_JSON}" >&2
    exit 64
fi

# The prep step writes samplesheet.csv into its own output dir; the
# orchestrator binds that path under inputs.samplesheet. bcl_input_dir is
# the absolute host path the operator passed via action_context — the
# orchestrator's _resolve_input_binds emits a --bind for it so the path
# is visible inside the container at the same location.
SAMPLESHEET=$(jq -er '.inputs.samplesheet' "${PARAMS_JSON}")
BCL_INPUT_DIR=$(jq -er '.inputs.bcl_input_dir' "${PARAMS_JSON}")

CONVERT_DIR="${QIITA_OUTPUT_PATH}/ConvertJob"
mkdir -p "${CONVERT_DIR}"

# bcl-convert thread counts track the CPUs allocated to this step rather than a
# hardcoded constant, so they can't drift from the resolved profile. QIITA_CPUS
# is the step's resolved baseline_resources.cpu, forwarded into the container by
# slurm/payload.py, so a future profile in workflows/bcl-convert/1.0.0.yaml with a
# different cpu needs no entrypoint change. SLURM_CPUS_PER_TASK is a second choice
# for non-container use only — `apptainer exec --containall` scrubs every SLURM_*
# var, so it is ALWAYS unset in here. nproc, then 1, for a local apptainer run.
# Same chain as workflows/_shared/_lib.sh, which this file deliberately does not
# source (bcl-convert's image predates it and carries no jq/manifest contract).
THREADS="${QIITA_CPUS:-${SLURM_CPUS_PER_TASK:-}}"
if [[ -z "${THREADS}" ]]; then
    THREADS=$(nproc 2>/dev/null || echo 1)
fi

bcl-convert \
    --sample-sheet "${SAMPLESHEET}" \
    --bcl-input-directory "${BCL_INPUT_DIR}" \
    --output-directory "${CONVERT_DIR}" \
    --bcl-num-decompression-threads "${THREADS}" \
    --bcl-num-conversion-threads "${THREADS}" \
    --bcl-num-compression-threads "${THREADS}" \
    --bcl-num-parallel-tiles "${THREADS}" \
    --bcl-sampleproject-subdirectories true \
    --force

# Walk the output tree and write manifest.json. Output names + relative
# paths are passed as `<name>=<rel>` so the orchestrator-side verifier
# can match the YAML's `outputs:` declaration against what the container
# produced.
# python3.11 (not OL8's default python3=3.6) — manifest_writer.py uses PEP 585
# builtin generics (`list[str]`) that 3.6 rejects at import; the Apptainer.def
# installs python3.11 explicitly. Keep the interpreter name in sync with it.
python3.11 /opt/qiita/manifest_writer.py "${QIITA_OUTPUT_PATH}" convert_dir=ConvertJob

# Verifier requires every file under QIITA_OUTPUT_PATH at mode 0440 and
# finds them by walking the tree (rglob), which needs the traverse (x) bit
# on every directory — a blanket `chmod -R 0440` would strip it and a
# non-owning verifier process could no longer descend. So chmod files and
# directories separately: files 0440 (the verified contract), directories
# 0550 (r-x: walkable, not writable). Runs after the manifest is written so
# manifest.json gets the same 0440 as the FASTQs. The verifier only checks
# file mode, never directory mode, so 0550 dirs pass.
#
# -mindepth 1 skips QIITA_OUTPUT_PATH itself: the orchestrator creates that
# directory on the host (SlurmBackend lays out <workspace>/output/ and binds
# it in), so it's owned by the orchestrator user, not the in-container user.
# A chmod on it fails with EPERM ("Operation not permitted") and, under
# `set -e`, fails an otherwise-successful job. We only need to fix the modes
# of what the container *created* inside output/ (ConvertJob/, manifest.json);
# the host already made output/ traversable and the verifier never checks the
# root dir's mode.
find "${QIITA_OUTPUT_PATH}" -mindepth 1 -type d -exec chmod 0550 {} +
find "${QIITA_OUTPUT_PATH}" -mindepth 1 -type f -exec chmod 0440 {} +
