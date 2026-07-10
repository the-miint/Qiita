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
# Two spec layouts, both handled here (a workflow uses one OR the other):
#   * SINGLE image (legacy, unchanged): workflows/<wf>/sif-build.env builds
#     workflows/<wf>/Apptainer.def into one SIF. Invoke: `build-sif.sh <wf>`.
#   * MULTI image (per-tool): workflows/<wf>/sif-build.d/<image>.env each
#     builds its own def (DEF_FILE, relative to the workflow dir) into its own
#     SIF, so one workflow can ship N single-tool images that rebuild
#     independently. Invoke: `build-sif.sh <wf> <image>`. The entrypoints and
#     shared _lib.sh stay at the workflow root and each def %files-copies just
#     the ones it needs; the whole workflow tree is staged so those relative
#     paths still resolve. A multi spec declares HASH_INPUTS (its own build
#     inputs) so the idempotency hash is scoped to that image alone (see below).
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
#   PATH_DERIVED=/scratch/persistent bash scripts/build-sif.sh <workflow> [<image>]
set -euo pipefail

WORKFLOW="${1:-}"
IMAGE="${2:-}"
if [[ -z "${WORKFLOW}" ]]; then
    echo "usage: PATH_DERIVED=<root> bash scripts/build-sif.sh <workflow> [<image>]" >&2
    exit 64
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "${SCRIPT_DIR}/.." && pwd )"
WORKFLOW_DIR="${REPO_ROOT}/workflows/${WORKFLOW}"
SHARED_DIR="${REPO_ROOT}/workflows/_shared"

# Resolve the spec: legacy single (workflows/<wf>/sif-build.env) vs. a named
# per-tool image (workflows/<wf>/sif-build.d/<image>.env). The def to build and
# the idempotency-hash scope are derived from the spec after it is sourced.
if [[ -n "${IMAGE}" ]]; then
    SPEC="${WORKFLOW_DIR}/sif-build.d/${IMAGE}.env"
else
    SPEC="${WORKFLOW_DIR}/sif-build.env"
fi

# Reach over to the deploy/ shared shell helpers for qiita_sif_build_inputs_hash
# (pure; no side effects on source — see its header). Keeps the hash logic in one
# unit-tested place (test_deploy_scripts.py) instead of duplicated here.
# shellcheck source=../deploy/_common.sh
source "${REPO_ROOT}/deploy/_common.sh"

if [[ ! -f "${SPEC}" ]]; then
    echo "No sif build spec for workflow '${WORKFLOW}'${IMAGE:+ image '${IMAGE}'} at:" >&2
    echo "  ${SPEC}" >&2
    echo "A container workflow opts into the generic SIF build by adding one." >&2
    exit 64
fi

# Per-image declarative spec. Required keys: SIF_FILENAME, VERIFY_CMD,
# VERIFY_MATCH. Optional: SOURCES (space-separated licensed/vendored artifacts
# staged from images/sources next to the def); DEF_FILE (the def to build,
# relative to the workflow dir — defaults to Apptainer.def, the legacy name);
# HASH_INPUTS (space-separated workflow-relative files that are THIS image's
# build inputs — set by multi-image specs to scope the idempotency hash).
# shellcheck source=/dev/null
source "${SPEC}"
for var in SIF_FILENAME VERIFY_CMD VERIFY_MATCH; do
    if [[ -z "${!var:-}" ]]; then
        echo "${SPEC} is missing required key ${var}" >&2
        exit 64
    fi
done

DEF="${WORKFLOW_DIR}/${DEF_FILE:-Apptainer.def}"
if [[ ! -f "${DEF}" ]]; then
    echo "Missing ${DEF} (DEF_FILE=${DEF_FILE:-Apptainer.def} declared by ${SPEC})" >&2
    exit 64
fi

if [[ -z "${PATH_DERIVED:-}" ]]; then
    echo "PATH_DERIVED is not set; set it to the derived-artifact FS root" >&2
    echo "(e.g. /scratch/persistent; SIFs live under PATH_DERIVED/images) and re-run" >&2
    exit 64
fi
# Must be absolute: the `cd /` below rebases every relative path, so a relative
# PATH_DERIVED would pass the `-d` check here (resolved against the caller cwd)
# and then silently retarget the build at /<rel>/images after the cd. Reject it
# up front — mirrors the orchestrator's from_env() is_absolute() guard, which this
# standalone script doesn't run through.
if [[ "${PATH_DERIVED}" != /* ]]; then
    echo "PATH_DERIVED must be an absolute path (got '${PATH_DERIVED}')" >&2
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

# Make the rest of the script cwd-independent. `find` (in the build-inputs hash)
# and `apptainer exec` (the verify steps) both touch the invoking process's cwd;
# when this is launched as a service account from a directory that account can't
# read — e.g. a manual `sudo -u qiita-orch …` from an admin's 0700 home — `find`
# fails to restore that cwd and aborts the build, and `apptainer exec` warns. cd
# to / (always traversable) so neither cares where we were invoked from. Safe
# because every path used below is absolute (PATH_DERIVED is the derived FS root)
# and the actual `apptainer build` cd's into its own temp build dir in a subshell.
cd / || { echo "could not cd / — refusing to run from an unstable cwd" >&2; exit 1; }

SOURCES_DIR="${IMAGES_DIR}/sources"
SIF_PATH="${IMAGES_DIR}/${SIF_FILENAME}"
# Content stamp written next to the SIF. Lets the idempotency check below detect
# a changed Apptainer.def / entrypoint.sh / manifest_writer.py — none of which
# VERIFY_MATCH (binary version only) can see, so such an edit would otherwise be
# skipped and never reach the host, forcing a manual FORCE=1. See the hash helper
# qiita_sif_build_inputs_hash in deploy/_common.sh.
HASH_PATH="${SIF_PATH}.buildhash"

# Content hash of the in-repo build inputs (in deploy/_common.sh): a changed
# def/entrypoint/manifest changes it and so triggers a rebuild below, while a
# re-vendored SOURCES (deliberately excluded) does not.
#
# A multi-image spec declares HASH_INPUTS (its own def + entrypoint(s) + any
# shared helper it %files-copies, workflow-relative) so the hash is scoped to
# THIS image — an edit to a sibling tool's def then leaves this image's stamp
# unchanged and skips its rebuild. Without HASH_INPUTS (the legacy single-image
# case) the whole workflow dir is hashed, exactly as before.
if [[ -n "${HASH_INPUTS:-}" ]]; then
    hash_files=("${DEF}")
    for rel in ${HASH_INPUTS}; do
        f="${WORKFLOW_DIR}/${rel}"
        if [[ ! -f "${f}" ]]; then
            echo "${SPEC} lists HASH_INPUTS entry '${rel}' but ${f} does not exist" >&2
            exit 64
        fi
        hash_files+=("${f}")
    done
    WANT_HASH="$(qiita_sif_build_inputs_hash_scoped "${REPO_ROOT}" "${SHARED_DIR}" "${hash_files[@]}")"
else
    WANT_HASH="$(qiita_sif_build_inputs_hash "${REPO_ROOT}" "${WORKFLOW_DIR}" "${SHARED_DIR}")"
fi

# Idempotency check: if a SIF already exists AND satisfies VERIFY_MATCH,
# leave it alone. `apptainer exec` runs the embedded binary in a fresh
# namespace. VERIFY_CMD is intentionally word-split (it carries args).
#
# The skip now requires BOTH gates to pass: the vendored binary satisfies
# VERIFY_MATCH *and* the build-inputs hash matches the stamp from the last
# build. VERIFY_MATCH alone is blind to the image-baked artifacts (entrypoint.sh,
# manifest_writer.py, the def's %post) — a fix to one of those used to be skipped
# here and never reach the host, which forced a manual FORCE=1. The hash closes
# that gap, so FORCE=1 is now only an emergency override (e.g. to rebuild against a
# re-vendored SOURCES, which the hash intentionally ignores).
if [[ "${FORCE:-}" == "1" ]]; then
    echo "FORCE=1 — rebuilding ${SIF_PATH} unconditionally (skipping idempotency check)."
elif [[ -f "${SIF_PATH}" ]]; then
    have_hash=""
    [[ -f "${HASH_PATH}" ]] && have_hash="$(cat "${HASH_PATH}")"
    # shellcheck disable=SC2086
    if [[ "${have_hash}" == "${WANT_HASH}" ]] \
        && apptainer exec "${SIF_PATH}" ${VERIFY_CMD} 2>&1 | grep -qE "${VERIFY_MATCH}"; then
        echo "Existing SIF at ${SIF_PATH} satisfies '${VERIFY_MATCH}' and build inputs are unchanged — nothing to do."
        exit 0
    fi
    if [[ "${have_hash}" != "${WANT_HASH}" ]]; then
        echo "Build inputs for ${WORKFLOW} changed since the last build (def/entrypoint/manifest); rebuilding ${SIF_PATH}."
    else
        echo "Existing SIF at ${SIF_PATH} does not satisfy '${VERIFY_MATCH}'; rebuilding."
    fi
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
        sif-build.env|sif-build.d|.gitignore|__pycache__|*.sif|*.rpm) continue ;;
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
# DEF_FILE is workflow-dir-relative, so it names the staged def directly.
(
    cd "${BUILD_WF_DIR}"
    apptainer build --force "${SIF_PATH}" "${DEF_FILE:-Apptainer.def}"
)

# Re-verify after build so a build that silently produced a broken SIF
# fails this script rather than only surfacing inside a SLURM job.
# shellcheck disable=SC2086
if ! apptainer exec "${SIF_PATH}" ${VERIFY_CMD} 2>&1 | grep -qE "${VERIFY_MATCH}"; then
    echo "Built SIF at ${SIF_PATH} does not satisfy '${VERIFY_MATCH}';" >&2
    echo "investigate the build log above before retrying." >&2
    exit 1
fi

# Stamp the build-inputs hash next to the SIF so the next run's idempotency
# check can detect a def/entrypoint/manifest change. Written only after the SIF
# verifies, so a failed build never leaves a stamp that would skip the retry.
printf '%s\n' "${WANT_HASH}" > "${HASH_PATH}"

echo "Built ${SIF_PATH} (satisfies '${VERIFY_MATCH}'; build-inputs stamp ${HASH_PATH})"
