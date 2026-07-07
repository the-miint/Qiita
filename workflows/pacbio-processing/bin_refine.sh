#!/bin/bash
# Step 3 (qp-pacbio steps 5+6): DAS_Tool consensus refinement over the three
# binners' output, then expose the winning bins + DAS_Tool's RAW summary table.
# Output `refined_bins_dir` = $QIITA_OUTPUT_PATH/refined_bins:
#   <bin>.fa                 one refined MAG per file (ingested as MAG)
#   das_tool_summary.tsv     DAS_Tool's RAW *_DASTool_summary.tsv, verbatim (no
#                            column normalization — assembly_load reads the `bin`,
#                            `bin_score`, `bin_set` columns with DuckDB read_csv).
# "No bins with score >0.5" (a normal outcome) leaves the dir with NO summary
# (and no .fa) — checkm and assembly_load skip cleanly (LCG-only is a valid
# success); assembly_load treats DAS_Tool provenance as optional (absent -> NULL).
source /opt/qiita/_lib.sh

GENOMES_DIR="$(qiita_input genomes_dir)"
BINS_DIR="$(qiita_input bins_dir)"
NOLCG="${GENOMES_DIR}/noLCG.fa"
OUT="${QIITA_OUTPUT_PATH}/refined_bins"
WORK="$(mktemp -d)"
mkdir -p "${OUT}"

# Per-binner contig->bin tables. metabat2's Fasta_to_Contig2Bin output needs the
# `$1,$4` projection; concoct/maxbin2 use the raw output (qp-pacbio's special
# case). Labels are DAS_Tool's expected CONCOCT/MaxBin/MetaBAT.
declare -a das_bins das_labels
for binner in concoct maxbin2 metabat2; do
    d="${BINS_DIR}/${binner}_bins"
    [[ -d "${d}" ]] || continue
    ls "${d}"/*.fa >/dev/null 2>&1 || continue
    tsv="${WORK}/${binner}.tsv"
    if [[ "${binner}" == "metabat2" ]]; then
        micromamba run -n dastool Fasta_to_Contig2Bin.sh -i "${d}" -e fa \
            | awk 'BEGIN{FS=OFS="\t"}{print $1,$4}' > "${tsv}"
    else
        micromamba run -n dastool Fasta_to_Contig2Bin.sh -i "${d}" -e fa > "${tsv}"
    fi
    [[ -s "${tsv}" ]] || continue
    das_bins+=("${tsv}")
    case "${binner}" in
        concoct)  das_labels+=("CONCOCT") ;;
        maxbin2)  das_labels+=("MaxBin") ;;
        metabat2) das_labels+=("MetaBAT") ;;
    esac
done

if [[ "${#das_bins[@]}" -eq 0 || ! -s "${NOLCG}" ]]; then
    qiita_finish refined_bins_dir=refined_bins
    exit 0
fi

bins_csv="$(IFS=,; echo "${das_bins[*]}")"
labels_csv="$(IFS=,; echo "${das_labels[*]}")"

# DAS_Tool exits non-zero when no bin clears its score threshold — a normal
# "no MAGs" outcome, not a failure. Treat a missing bins dir as empty.
set +e
micromamba run -n dastool DAS_Tool \
    --bins="${bins_csv}" --contigs="${NOLCG}" \
    --outputbasename="${WORK}/dastool" --labels="${labels_csv}" \
    --threads="${THREADS}" --search_engine=diamond --write_bins 1
set -e

# REVIEW (assembly correctness): DAS_Tool exits non-zero both when no bin clears
# its score threshold (a legitimate "no MAGs" outcome for a low-biomass sample)
# AND when it genuinely crashes (OOM, missing diamond, corrupt input). The `set
# +e` above + this "no output bins => success, exit 0" check cannot tell the two
# apart, so a real DAS_Tool failure is silently reported as an LCG-only success.
# This matches qp-pacbio's tolerance, but trades the repo's fail-loud ethos for
# it. DAS_Tool (an R script) exposes no distinct exit code to disambiguate.
# Needs a human/bioinformatics call on whether to tighten (e.g. grep DAS_Tool's
# log for its "no bins" message before treating a non-zero exit as success).
DAS_BINS_DIR="${WORK}/dastool_DASTool_bins"
if ! ls "${DAS_BINS_DIR}"/*.fa >/dev/null 2>&1; then
    qiita_finish refined_bins_dir=refined_bins
    exit 0
fi

cp "${DAS_BINS_DIR}"/*.fa "${OUT}/"

# Emit DAS_Tool's RAW summary verbatim (no normalization — assembly_load reads it
# with DuckDB). The summary's `bin` column matches CheckM's "Bin Id" (both the MAG
# FASTA stem), so assembly_load LEFT-joins scores on it. DAS_Tool writes the
# summary whenever it produces bins; if it is somehow absent, warn but don't fail
# (provenance is optional — the MAG sequences still store).
SUMMARY="${WORK}/dastool_DASTool_summary.tsv"
if [[ -f "${SUMMARY}" ]]; then
    cp "${SUMMARY}" "${OUT}/das_tool_summary.tsv"
else
    echo "WARNING: DAS_Tool produced bins but no summary at ${SUMMARY};" >&2
    echo "         DAS_Tool provenance (score/binner) UNCAPTURED this run." >&2
fi

qiita_finish refined_bins_dir=refined_bins
