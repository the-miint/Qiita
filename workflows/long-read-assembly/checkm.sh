#!/bin/bash
# Step 4 (qp-pacbio step 7): CheckM quality assessment of the refined MAGs.
# Output `checkm_dir` = $QIITA_OUTPUT_PATH/checkm holding CheckM's RAW --tab_table
# output verbatim (the container does NO column normalization — one CSV framework,
# DuckDB, owns all parsing in assembly_load):
#   lineage.tsv   `checkm lineage_wf --tab_table` — Bin Id, Marker lineage,
#                 Completeness, Contamination, Strain heterogeneity, ...
#   qa.tsv        `checkm qa -o 2 --tab_table` — Bin Id, Genome size (bp),
#                 # contigs, ... (the extended stats not in lineage_wf)
# assembly_load reads BOTH with DuckDB read_csv and joins them by "Bin Id".
# No MAGs -> empty checkm_dir (the raw files are simply absent); assembly_load
# then writes bin_quality empty. CheckM is not run on an empty dir.
#
# CheckM needs its ~1.4 GB reference data. It is bind-mounted at run time (NOT
# baked into the image) and located via CHECKM_DATA_PATH; the operator provisions
# it under PATH_DERIVED and the orchestrator binds it in. A plain bind is not
# enough — CheckM reads CHECKM_DATA_PATH (set below). Its ABSENCE is an operator
# config error, not a data condition: with MAGs present but no DB to score them,
# checkm.sh FAILS LOUD rather than silently emitting an empty checkm_dir (see the
# DB check below). The genuinely-no-MAGs case above is a separate benign success.
source /opt/qiita/_lib.sh

REFINED_DIR="$(qiita_input refined_bins_dir)"
OUT="${QIITA_OUTPUT_PATH}/checkm"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "${OUT}"

# No MAGs to assess -> empty checkm_dir (assembly_load writes bin_quality empty).
if ! ls "${REFINED_DIR}"/*.fa >/dev/null 2>&1; then
    qiita_finish checkm_dir=checkm
    exit 0
fi

export CHECKM_DATA_PATH="${QIITA_CHECKM_DB:-/opt/checkm_data}"

# The CheckM reference DB is bind-mounted at run time (deploy checklist bucket 2),
# NOT baked into the SIF, and located via CHECKM_DATA_PATH (`checkm data setRoot`
# reads the same var). Reaching here means there ARE MAGs to assess, so an
# absent/empty DB is an OPERATOR CONFIG ERROR, not a data condition — silently
# emitting an empty checkm_dir would let the ticket COMPLETE with MAG quality
# permanently uncaptured. FAIL LOUD instead (mirrors bin_refine.sh's DAS_Tool
# fail-loud): the operator must stage the DB + bind it in before the workflow can
# run. This is distinct from the genuinely-no-MAGs benign empty success above.
if [[ ! -d "${CHECKM_DATA_PATH}" || -z "$(ls -A "${CHECKM_DATA_PATH}" 2>/dev/null)" ]]; then
    echo "ERROR: CheckM reference data not found at CHECKM_DATA_PATH=${CHECKM_DATA_PATH}." >&2
    echo "       This is a deploy/config error: stage CheckM's ~1.4 GB reference DB" >&2
    echo "       under PATH_DERIVED and bind it in (or set QIITA_CHECKM_DB to its" >&2
    echo "       in-container path). Refusing to report MAG quality as empty when" >&2
    echo "       MAGs are present — failing loud." >&2
    exit 78
fi

# Emit CheckM's RAW --tab_table output straight into checkm_dir. lineage_wf carries
# marker lineage + completeness/contamination/strain heterogeneity; qa -o 2 adds
# genome size / # contigs. assembly_load joins the two by "Bin Id" in DuckDB.
micromamba run -n checkm checkm lineage_wf "${REFINED_DIR}" "${WORK}/checkm_out" \
    -x fa -t "${THREADS}" --tab_table -f "${OUT}/lineage.tsv" --pplacer_threads 2

micromamba run -n checkm checkm qa "${WORK}/checkm_out/lineage.ms" "${WORK}/checkm_out" \
    -o 2 -t "${THREADS}" --tab_table -f "${OUT}/qa.tsv"

qiita_finish checkm_dir=checkm
