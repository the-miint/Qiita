#!/usr/bin/env bash
# Stage the miint DuckDB extension once into the shared extension_directory so
# every cluster runtime path (native SLURM jobs, the compute-readiness probe)
# only LOADs it — no per-job download, no compute-node mirror dependency, no
# writable-$HOME requirement. Mirrors scripts/build-sif.sh: the heavy lifting is
# in Python (reusing qiita_common's single-sourced install SQL + connect config
# via `python -m qiita_compute_orchestrator.cli.stage_miint`); this script only
# resolves the venv + target dir and invokes it. Idempotent — it FORCE-installs
# the mirror's current build, so re-run after a miint or DuckDB version bump.
#
# Run as the account that owns MIINT_EXTENSION_DIRECTORY (qiita-orch in prod):
#   PATH_DERIVED=/scratch/persistent \
#   SLURM_NATIVE_PYTHON=/opt/qiita/qiita-compute-orchestrator/.venv/bin/python \
#   bash scripts/stage-miint-extension.sh
#
# Env:
#   SLURM_NATIVE_PYTHON         (required) the compute-orchestrator venv python —
#       the SAME interpreter native jobs run, so the staged build matches their
#       DuckDB version+platform (DuckDB namespaces the extension dir by both).
#   MIINT_EXTENSION_DIRECTORY   (required, or derived from PATH_DERIVED as
#       $PATH_DERIVED/duckdb-ext) the shared dir to stage into; must already
#       exist — the operator creates it with `install -d` (see DEPLOY_CHECKLIST).
#   MIINT_EXTENSION_REPO        (optional) override the team mirror for a
#       local/dev build; honored by qiita_common.duckdb_miint.
set -euo pipefail

PY="${SLURM_NATIVE_PYTHON:?SLURM_NATIVE_PYTHON must point at the compute-orchestrator venv python}"

if [[ -z "${MIINT_EXTENSION_DIRECTORY:-}" ]]; then
    : "${PATH_DERIVED:?set MIINT_EXTENSION_DIRECTORY, or PATH_DERIVED to derive it as PATH_DERIVED/duckdb-ext}"
    export MIINT_EXTENSION_DIRECTORY="${PATH_DERIVED%/}/duckdb-ext"
fi

if [[ ! -x "${PY}" ]]; then
    echo "stage-miint: SLURM_NATIVE_PYTHON is not an executable file: ${PY}" >&2
    exit 1
fi
if [[ ! -d "${MIINT_EXTENSION_DIRECTORY}" ]]; then
    echo "stage-miint: extension_directory does not exist: ${MIINT_EXTENSION_DIRECTORY}" >&2
    echo "  create it first, e.g.:" >&2
    echo "    sudo install -d -o qiita-orch -g qiita-orch -m 0755 '${MIINT_EXTENSION_DIRECTORY}'" >&2
    exit 1
fi

echo "stage-miint: staging into ${MIINT_EXTENSION_DIRECTORY} using ${PY}"
exec "${PY}" -m qiita_compute_orchestrator.cli.stage_miint
