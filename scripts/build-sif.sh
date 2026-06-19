#!/bin/bash
# Build (or verify) a workflow's Apptainer image — generically.
#
# This is the ONLY SIF build script. A container workflow opts into it by
# adding a `workflows/<workflow>/sif-build.env` declarative spec (see
# workflows/bcl-convert/sif-build.env for the canonical example); all the
# mechanics live here so there is no per-workflow build script to drift.
# A CI guard (qiita-compute-orchestrator/tests/test_sif_build_spec.py)
# forbids hand-rolled `scripts/build-*-sif.sh`, so new workflows are forced
# through this path.
#
# Critically, the build runs in a temp build root OWNED BY THE INVOKING
# USER — the in-repo checkout is only ever READ. That lets a locked-down
# service account (e.g. qiita-orch on the deploy host) build without write
# access to the qiita-owned checkout under /home/qiita. apptainer resolves
# %files source paths relative to the build CWD, and a def may reference
# ../_shared/manifest_writer.py, so the temp tree mirrors the repo layout
# (<build>/_shared alongside <build>/<workflow>) and we build from the
# latter.
#
# Idempotent: a SIF that already satisfies the spec's VERIFY_MATCH is left
# in place; otherwise apptainer rebuilds it. Designed to run on the deploy
# host after the operator has placed any licensed/vendored source artifacts
# at ${PATH_DERIVED}/images/sources/ (see DEPLOY_CHECKLIST.md).
#
# Pre-conditions:
#   * PATH_DERIVED is set, and ${PATH_DERIVED}/images is a directory (the
#     orchestrator's Settings.from_env() enforces this at boot; re-checked
#     here so a misconfigured shell fails before apptainer runs).
#   * Each SOURCES file exists at ${PATH_DERIVED}/images/sources/<file>.
#   * `apptainer` is on PATH.
#
# Usage:
#   PATH_DERIVED=/scratch/persistent bash scripts/build-sif.sh <workflow>
set -euo pipefail

WORKFLOW="${1:-}"
if [[ -z "${WORKFLOW}" ]]; then
    echo "usage: PATH_DERIVED=<root> bash scripts/build-sif.sh <workflow>" >&2
    exit 64
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "${SCRIPT_DIR}/.." && pwd )"
WORKFLOW_DIR="${REPO_ROOT}/workflows/${WORKFLOW}"
SHARED_DIR="${REPO_ROOT}/workflows/_shared"
SPEC="${WORKFLOW_DIR}/sif-build.env"
DEF="${WORKFLOW_DIR}/Apptainer.def"

if [[ ! -f "${SPEC}" ]]; then
    echo "No sif-build.env for workflow '${WORKFLOW}' at:" >&2
    echo "  ${SPEC}" >&2
    echo "A container workflow opts into the generic SIF build by adding one." >&2
    exit 64
fi
if [[ ! -f "${DEF}" ]]; then
    echo "Missing ${DEF}" >&2
    exit 64
fi

# Per-workflow declarative spec. Required keys: SIF_FILENAME, VERIFY_CMD,
# VERIFY_MATCH. Optional: SOURCES (space-separated licensed/vendored
# artifacts staged from images/sources next to the def).
# shellcheck source=/dev/null
source "${SPEC}"
for var in SIF_FILENAME VERIFY_CMD VERIFY_MATCH; do
    if [[ -z "${!var:-}" ]]; then
        echo "${SPEC} is missing required key ${var}" >&2
        exit 64
    fi
done

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
SIF_PATH="${IMAGES_DIR}/${SIF_FILENAME}"

# Idempotency check: if a SIF already exists AND satisfies VERIFY_MATCH,
# leave it alone. `apptainer exec` runs the embedded binary in a fresh
# namespace. VERIFY_CMD is intentionally word-split (it carries args).
#
# VERIFY_MATCH only probes the vendored binary's version — it is blind to
# the *image-baked* artifacts (entrypoint.sh, manifest_writer.py, the def's
# %post). So a fix that changes one of those without bumping the binary
# version would be skipped here and never reach the host. FORCE=1 opts out
# of the idempotency skip to rebuild unconditionally; the deploy checklist
# uses it whenever an entrypoint/manifest/def-only change ships.
if [[ "${FORCE:-}" == "1" ]]; then
    echo "FORCE=1 — rebuilding ${SIF_PATH} unconditionally (skipping idempotency check)."
elif [[ -f "${SIF_PATH}" ]]; then
    # shellcheck disable=SC2086
    if apptainer exec "${SIF_PATH}" ${VERIFY_CMD} 2>&1 | grep -qE "${VERIFY_MATCH}"; then
        echo "Existing SIF at ${SIF_PATH} satisfies '${VERIFY_MATCH}' — nothing to do."
        exit 0
    fi
    echo "Existing SIF at ${SIF_PATH} does not satisfy '${VERIFY_MATCH}'; rebuilding."
fi

# Stage into a build root OWNED BY THE INVOKING USER (never the checkout).
# Mirror the repo layout so a def's ../_shared/... reference resolves.
BUILD_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/qiita-sif-${WORKFLOW}.XXXXXX")"
cleanup() {
    rm -rf "${BUILD_ROOT}"
}
trap cleanup EXIT

BUILD_WF_DIR="${BUILD_ROOT}/${WORKFLOW}"
mkdir -p "${BUILD_WF_DIR}" "${BUILD_ROOT}/_shared"
cp -R "${SHARED_DIR}/." "${BUILD_ROOT}/_shared/"
rm -rf "${BUILD_ROOT}/_shared/__pycache__"

# Copy the workflow's build inputs (def + entrypoint + any aux files),
# excluding the spec, the gitignore, and generated .rpm/.sif artifacts.
for f in "${WORKFLOW_DIR}"/*; do
    base="$(basename "${f}")"
    case "${base}" in
        sif-build.env|.gitignore|__pycache__|*.sif|*.rpm) continue ;;
    esac
    cp -R "${f}" "${BUILD_WF_DIR}/${base}"
done

# Stage each vendored source artifact from images/sources next to the def
# (the def's %files references them by bare filename).
for src in ${SOURCES:-}; do
    src_path="${SOURCES_DIR}/${src}"
    if [[ ! -f "${src_path}" ]]; then
        echo "Expected vendored source not found at:" >&2
        echo "  ${src_path}" >&2
        echo "Place it there before building; see DEPLOY_CHECKLIST.md for the recipe." >&2
        exit 64
    fi
    cp "${src_path}" "${BUILD_WF_DIR}/${src}"
done

# apptainer build --force overwrites any leftover SIF in $IMAGES_DIR.
# Run from the staged workflow dir so the relative paths in the def resolve.
(
    cd "${BUILD_WF_DIR}"
    apptainer build --force "${SIF_PATH}" Apptainer.def
)

# Re-verify after build so a build that silently produced a broken SIF
# fails this script rather than only surfacing inside a SLURM job.
# shellcheck disable=SC2086
if ! apptainer exec "${SIF_PATH}" ${VERIFY_CMD} 2>&1 | grep -qE "${VERIFY_MATCH}"; then
    echo "Built SIF at ${SIF_PATH} does not satisfy '${VERIFY_MATCH}';" >&2
    echo "investigate the build log above before retrying." >&2
    exit 1
fi

echo "Built ${SIF_PATH} (satisfies '${VERIFY_MATCH}')"
