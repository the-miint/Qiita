#!/bin/bash
# Build (or verify) the bcl-convert Apptainer image.
#
# Idempotent: a SIF that already matches the embedded bcl-convert version
# is left in place; otherwise apptainer rebuilds it from the in-tree
# Apptainer.def. Designed to be run from the deploy host after the
# operator has placed the Illumina-licensed RPM at
# ${PATH_DERIVED}/images/sources/.
#
# Pre-conditions:
#   * PATH_DERIVED is set, and ${PATH_DERIVED}/images exists and is a
#     directory (the orchestrator's Settings.from_env() enforces this at
#     boot, but we re-check here so a misconfigured shell on the deploy
#     host fails before apptainer runs).
#   * ${PATH_DERIVED}/images/sources/bcl-convert-4.5.4-2.el8.x86_64.rpm exists.
#   * `apptainer` is on PATH.
#
# Usage:
#   PATH_DERIVED=/scratch/persistent bash scripts/build-bcl-convert-sif.sh
set -euo pipefail

BCL_CONVERT_VERSION="4.5.4"
RPM_FILENAME="bcl-convert-${BCL_CONVERT_VERSION}-2.el8.x86_64.rpm"
SIF_FILENAME="bcl-convert-${BCL_CONVERT_VERSION}.sif"

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "${SCRIPT_DIR}/.." && pwd )"
WORKFLOW_DIR="${REPO_ROOT}/workflows/bcl-convert"

if [[ -z "${PATH_DERIVED:-}" ]]; then
    echo "PATH_DERIVED is not set; set it to the derived-artifact FS root" >&2
    echo "(e.g. /scratch/persistent; SIFs live under PATH_DERIVED/images) and re-run" >&2
    exit 64
fi
# Built SIFs live under PATH_DERIVED/images (the orchestrator derives the
# same join). Everything below operates on that derived tier.
IMAGES_DIR="${PATH_DERIVED}/images"
if [[ ! -d "${IMAGES_DIR}" ]]; then
    echo "PATH_DERIVED/images=${IMAGES_DIR} is not a directory" >&2
    exit 64
fi
if ! command -v apptainer >/dev/null 2>&1; then
    echo "apptainer not on PATH; install apptainer before running this script" >&2
    exit 64
fi

SOURCES_DIR="${IMAGES_DIR}/sources"
RPM_PATH="${SOURCES_DIR}/${RPM_FILENAME}"
if [[ ! -f "${RPM_PATH}" ]]; then
    echo "Expected Illumina-licensed RPM not found at:" >&2
    echo "  ${RPM_PATH}" >&2
    echo "Download bcl-convert ${BCL_CONVERT_VERSION} (EULA-gated) from Illumina" >&2
    echo "and place it at the path above; see DEPLOY_CHECKLIST.md for the recipe." >&2
    exit 64
fi

SIF_PATH="${IMAGES_DIR}/${SIF_FILENAME}"

# Idempotency check: if a SIF already exists AND reports the expected
# bcl-convert version, leave it alone. `apptainer exec` runs the
# embedded binary in a fresh namespace; output is matched on
# "bcl-convert Version ${BCL_CONVERT_VERSION}.x" (patch component may
# differ between RPM revisions). The matching is intentionally loose on
# the patch so a re-vendor that bumps from 4.5.4-1 to 4.5.4-2 doesn't
# trip an unnecessary rebuild.
if [[ -f "${SIF_PATH}" ]]; then
    if apptainer exec "${SIF_PATH}" bcl-convert --version 2>&1 \
        | grep -qE "bcl-convert Version ${BCL_CONVERT_VERSION}"; then
        echo "Existing SIF at ${SIF_PATH} reports bcl-convert ${BCL_CONVERT_VERSION}"
        echo "— nothing to do."
        exit 0
    fi
    echo "Existing SIF at ${SIF_PATH} does not report bcl-convert ${BCL_CONVERT_VERSION};"
    echo "rebuilding."
fi

# apptainer build's %files directive resolves paths relative to the def
# file's directory. Stage the RPM next to Apptainer.def so the build can
# pick it up, then remove the staged copy whether the build succeeds or
# fails. The staged copy and the SIF are .gitignore'd inside the
# workflow directory.
STAGED_RPM="${WORKFLOW_DIR}/${RPM_FILENAME}"
cleanup() {
    rm -f "${STAGED_RPM}"
}
trap cleanup EXIT

cp "${RPM_PATH}" "${STAGED_RPM}"

# apptainer build --force overwrites any leftover SIF in $IMAGES_DIR.
# Run from the workflow dir so the relative paths in Apptainer.def resolve.
(
    cd "${WORKFLOW_DIR}"
    apptainer build --force "${SIF_PATH}" Apptainer.def
)

# Re-verify after build so a build that silently produced a broken SIF
# fails this script rather than only surfacing inside a SLURM job.
if ! apptainer exec "${SIF_PATH}" bcl-convert --version 2>&1 \
    | grep -qE "bcl-convert Version ${BCL_CONVERT_VERSION}"; then
    echo "Built SIF at ${SIF_PATH} does not report bcl-convert ${BCL_CONVERT_VERSION};" >&2
    echo "investigate the build log above before retrying." >&2
    exit 1
fi

echo "Built ${SIF_PATH} — bcl-convert ${BCL_CONVERT_VERSION}"
