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

# Thread flags are hardcoded to 16 — every A4 profile in workflows/bcl-convert/
# 1.0.0.yaml declares cpu=16. If a future workflow YAML adds a profile with a
# different cpu, this entrypoint will need a matching dispatch and the YAML
# must include the thread count in an `instrument_threads` output the prep
# step writes alongside instrument_model.
bcl-convert \
    --sample-sheet "${SAMPLESHEET}" \
    --bcl-input-directory "${BCL_INPUT_DIR}" \
    --output-directory "${CONVERT_DIR}" \
    --bcl-num-decompression-threads 16 \
    --bcl-num-conversion-threads 16 \
    --bcl-num-compression-threads 16 \
    --bcl-num-parallel-tiles 16 \
    --bcl-sampleproject-subdirectories true \
    --force

# Walk the output tree and write manifest.json. Output names + relative
# paths are passed as `<name>=<rel>` so the orchestrator-side verifier
# can match the YAML's `outputs:` declaration against what the container
# produced.
python3 /opt/qiita/manifest_writer.py "${QIITA_OUTPUT_PATH}" convert_dir=ConvertJob

# Verifier requires every file under QIITA_OUTPUT_PATH at mode 0440 and
# finds them by walking the tree (rglob), which needs the traverse (x) bit
# on every directory — a blanket `chmod -R 0440` would strip it and a
# non-owning verifier process could no longer descend. So chmod files and
# directories separately: files 0440 (the verified contract), directories
# 0550 (r-x: walkable, not writable). Runs after the manifest is written so
# manifest.json gets the same 0440 as the FASTQs. The verifier only checks
# file mode, never directory mode, so 0550 dirs pass.
find "${QIITA_OUTPUT_PATH}" -type d -exec chmod 0550 {} +
find "${QIITA_OUTPUT_PATH}" -type f -exec chmod 0440 {} +
