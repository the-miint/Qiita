#!/bin/bash
# Step 3 (qp-pacbio steps 5+6): DAS_Tool consensus refinement over the three
# binners' output, then expose the winning bins + a normalized provenance table.
# Output `refined_bins_dir` = $QIITA_OUTPUT_PATH/refined_bins:
#   <bin>.fa                 one refined MAG per file (ingested as MAG)
#   das_tool_scores.tsv      genome_local_id, das_tool_score, source_binner
# "No bins with score >0.5" (a normal outcome) leaves the dir with only the TSV
# header — checkm and pacbio_ingest skip cleanly (LCG-only is a valid success).
source /opt/qiita/_lib.sh

GENOMES_DIR="$(qiita_input genomes_dir)"
BINS_DIR="$(qiita_input bins_dir)"
NOLCG="${GENOMES_DIR}/noLCG.fa"
OUT="${QIITA_OUTPUT_PATH}/refined_bins"
WORK="$(mktemp -d)"
mkdir -p "${OUT}"

_write_empty_scores() {
    printf 'genome_local_id\tdas_tool_score\tsource_binner\n' > "${OUT}/das_tool_scores.tsv"
}

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
    _write_empty_scores
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

DAS_BINS_DIR="${WORK}/dastool_DASTool_bins"
if ! ls "${DAS_BINS_DIR}"/*.fa >/dev/null 2>&1; then
    _write_empty_scores
    qiita_finish refined_bins_dir=refined_bins
    exit 0
fi

cp "${DAS_BINS_DIR}"/*.fa "${OUT}/"

# Normalize the DAS_Tool summary -> das_tool_scores.tsv. Column names vary by
# version, so resolve by header name and tolerate absence (pacbio_ingest treats
# the score/binner columns as optional). VALIDATE the header names on the first
# Linux build against a real *_DASTool_summary.tsv.
SUMMARY="${WORK}/dastool_DASTool_summary.tsv"
python3 - "${SUMMARY}" > "${OUT}/das_tool_scores.tsv" <<'PY'
import csv, sys

print("genome_local_id\tdas_tool_score\tsource_binner")
try:
    with open(sys.argv[1], newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        cols = {c.lower(): c for c in (reader.fieldnames or [])}
        id_c = cols.get("bin")
        score_c = next((cols[k] for k in ("score", "das_tool_score", "scg_score") if k in cols), None)
        binner_c = next((cols[k] for k in ("binner", "source_binner", "bin_set") if k in cols), None)
        if id_c:
            for r in reader:
                print("\t".join((
                    r.get(id_c, ""),
                    r.get(score_c, "") if score_c else "",
                    r.get(binner_c, "") if binner_c else "",
                )))
except FileNotFoundError:
    pass
PY

qiita_finish refined_bins_dir=refined_bins
