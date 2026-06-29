#!/usr/bin/env bash
# Build (or verify) every container workflow's Apptainer SIF as part of a deploy.
#
# PROTOTYPE — see SIF_AUTOMATION_FEASIBILITY.md. This wraps the existing single
# generic builder (scripts/build-sif.sh) in the deploy's "build as root, then
# chown to the orchestrator account" model: activate.sh already runs as root, so
# we build there (no host fakeroot/subuid setup needed) and hand the produced SIF
# to qiita-orch, which is what owns ${PATH_DERIVED}/images and runs SLURM jobs.
#
# Iterates workflows/*/sif-build.env, skipping:
#   * names starting with "_" (e.g. _sif-build-smoke — a test fixture, _shared);
#   * a spec opting out with AUTO_BUILD=0;
#   * any image whose licensed/vendored SOURCES aren't staged under
#     images/sources/ (EULA-gated artifacts the operator places out of band) —
#     a SKIP with a warning, NOT a deploy failure.
#
# A real `apptainer build` *failure* IS a hard error: the script finishes the
# other images for a complete report, then exits non-zero so activate.sh aborts
# BEFORE any service restart — the same "refuse to deploy onto a broken
# precondition" stance as the migration guard. Absence (no apptainer, no
# PATH_DERIVED, no staged source) degrades to a clean skip instead.
#
# Run as root (activate.sh's context). Honors the same CO_ENV / QIITA_ORCH_USER
# overrides as the other deploy scripts.

set -euo pipefail

# shellcheck source=deploy/_common.sh
source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"  # require_root, read_env_var, CO_ENV, QIITA_ORCH_USER

require_root "deploy/build-sifs.sh must run as root (it builds SIFs, then chowns them to ${QIITA_ORCH_USER})."

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_SIF="${REPO_ROOT}/scripts/build-sif.sh"
WORKFLOWS_DIR="${REPO_ROOT}/workflows"

# The build needs the in-repo builder + workflow sources. The deploy runs
# activate.sh from the operator CLONE (which has them); an activate.sh invoked
# straight from /opt/qiita/incoming finds them too, since local-deploy.sh stages
# scripts/ + workflows/ there. A stage that somehow lacks either skips cleanly
# rather than fails.
if [[ ! -x "${BUILD_SIF}" ]] || [[ ! -d "${WORKFLOWS_DIR}" ]]; then
    echo "SIF auto-build: skipped — ${BUILD_SIF} or ${WORKFLOWS_DIR} not present in this stage." >&2
    exit 0
fi

# apptainer is Linux-host only; absent on a macOS dev box or a minimal host.
if ! command -v apptainer >/dev/null 2>&1; then
    echo "SIF auto-build: skipped — apptainer not on PATH (non-Linux host or not installed)." >&2
    exit 0
fi

# PATH_DERIVED lives in the orchestrator env (the SLURM backend requires it);
# unset means the local backend / no container tier → nothing to build.
derived=""
[[ -r "${CO_ENV}" ]] && derived="$(read_env_var "${CO_ENV}" PATH_DERIVED)"
if [[ -z "${derived}" ]]; then
    echo "SIF auto-build: skipped — PATH_DERIVED not set in ${CO_ENV} (local backend / no container tier)." >&2
    exit 0
fi
images_dir="${derived%/}/images"
if [[ ! -d "${images_dir}" ]]; then
    echo "SIF auto-build: skipped — ${images_dir} is not a directory (image tier not provisioned yet)." >&2
    exit 0
fi
sources_dir="${images_dir}/sources"

built=() skipped=() failed=()
for spec in "${WORKFLOWS_DIR}"/*/sif-build.env; do
    [[ -e "${spec}" ]] || continue
    wf="$(basename "$(dirname "${spec}")")"
    case "${wf}" in _*) continue ;; esac   # _sif-build-smoke, _shared, …

    # Read the spec's declarative keys in a subshell so its `source` can't leak
    # into ours (and so a stray exit in one spec can't abort the loop). Pre-declared
    # so shellcheck sees the assignment the eval below performs.
    spec_sif="" spec_sources="" spec_auto=1
    eval "$(
        # shellcheck disable=SC1090
        ( source "${spec}"
          printf 'spec_sif=%q\n'     "${SIF_FILENAME:-}"
          printf 'spec_sources=%q\n' "${SOURCES:-}"
          printf 'spec_auto=%q\n'    "${AUTO_BUILD:-1}" )
    )"

    if [[ "${spec_auto}" == "0" ]]; then
        echo "SIF auto-build: ${wf} — opted out (AUTO_BUILD=0); skipping."
        skipped+=("${wf} (AUTO_BUILD=0)")
        continue
    fi

    # Licensed/vendored sources are placed under images/sources/ out of band
    # (EULA gating). If any are missing, this host isn't set up to build that
    # image — skip with a clear pointer, never fail the deploy over it.
    if ! missing="$(qiita_sif_missing_sources "${sources_dir}" "${spec_sources}")"; then
        missing_oneline="$(printf '%s' "${missing}" | tr '\n' ' ')"
        echo "SIF auto-build: ${wf} — skipping; vendored source(s) not staged under ${sources_dir}: ${missing_oneline}" >&2
        echo "                place them there per DEPLOY_CHECKLIST.md, then re-run the deploy." >&2
        skipped+=("${wf} (missing sources: ${missing_oneline})")
        continue
    fi

    echo "SIF auto-build: ${wf} → ${images_dir}/${spec_sif} (building as root)…"
    # build-sif.sh is idempotent (VERIFY_MATCH + the build-inputs hash stamp), so
    # an unchanged image prints "nothing to do" and exits 0 cheaply. A non-zero
    # exit here is a genuine build/verify failure — record it, keep going.
    if PATH_DERIVED="${derived}" bash "${BUILD_SIF}" "${wf}"; then
        # Hand the produced SIF (+ its build-inputs stamp) to the orchestrator
        # account — the deploy's "build as root, chown to qiita-orch" model. This
        # ownership IS the point: qiita-orch must own the SIF to run it, so a chown
        # failure is a real defect (bad QIITA_ORCH_USER, perms) — treat it as a
        # build failure rather than swallowing it and shipping a root-owned SIF that
        # only fails later inside a SLURM job. The .buildhash stamp always exists
        # after a successful build (build-sif.sh writes it), so chowning both is safe.
        sif="${images_dir}/${spec_sif}"
        if chown "${QIITA_ORCH_USER}:${QIITA_ORCH_USER}" "${sif}" "${sif}.buildhash"; then
            built+=("${wf}")
        else
            echo "SIF auto-build: ${wf} — built, but chown to ${QIITA_ORCH_USER} FAILED (see error above)." >&2
            failed+=("${wf} (chown)")
        fi
    else
        echo "SIF auto-build: ${wf} — BUILD FAILED (see log above)." >&2
        failed+=("${wf}")
    fi
done

echo "SIF auto-build summary: built/verified=${#built[@]} skipped=${#skipped[@]} failed=${#failed[@]}"
[[ "${#skipped[@]}" -gt 0 ]] && printf '  skipped: %s\n' "${skipped[@]}" >&2
if [[ "${#failed[@]}" -gt 0 ]]; then
    printf '  FAILED: %s\n' "${failed[@]}" >&2
    echo "Refusing to continue the deploy with an unbuildable image — aborting before any restart." >&2
    exit 1
fi
exit 0
