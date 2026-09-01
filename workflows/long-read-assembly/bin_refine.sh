#!/bin/bash
# DAS_Tool consensus refinement over the three binners' output, then expose the
# winning bins + DAS_Tool's RAW summary table.
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
trap 'rm -rf "$WORK"' EXIT
mkdir -p "${OUT}"

# Per-binner contig->bin tables. One path for all three: `Fasta_to_Contig2Bin.sh`
# emits <contig>\t<bin id> and contig2bin_filter.awk decides which of those rows
# are candidate bins — it carries the column measurement and the catch-all rule.
# Labels are DAS_Tool's expected CONCOCT/MaxBin/MetaBAT.
# `=()`, not a bare `declare -a`: a declared-but-never-assigned array is UNSET, so
# `${#das_bins[@]}` further down trips the `set -u` from _lib.sh — "das_bins:
# unbound variable", exit 1 — whenever no binner contributes a table, which is the
# benign empty outcome that check exists to serve. Measured on the image's own base
# (mambaorg/micromamba:1.5.8, bash 5.2.15). The filter below makes it reachable for
# a binner whose only .fa files are catch-alls, on top of the empty-bins_dir case
# binning.sh already hands over. macOS ships bash 3.2, where the bare form reads 0,
# so this does not reproduce on a dev laptop.
declare -a das_bins=() das_labels=()
for binner in concoct maxbin2 metabat2; do
    d="${BINS_DIR}/${binner}_bins"
    [[ -d "${d}" ]] || continue
    ls "${d}"/*.fa >/dev/null 2>&1 || continue
    tsv="${WORK}/${binner}.tsv"
    rejects="${WORK}/${binner}.rejects"
    micromamba run -n dastool Fasta_to_Contig2Bin.sh -i "${d}" -e fa \
        | awk -v rejects="${rejects}" -f /opt/qiita/contig2bin_filter.awk > "${tsv}"
    if [[ -s "${rejects}" ]]; then
        echo "bin_refine: ${binner}'s contig2bin table holds row(s) that are neither a" >&2
        echo "            numbered bin nor a known catch-all — contig2bin_filter.awk" >&2
        echo "            lists both shapes. First few, distinct:" >&2
        # awk, not `sort -u | head`: head closing the pipe would SIGPIPE sort, and
        # under pipefail that aborts the script before the exit below.
        awk '!seen[$0]++ && ++n <= 5' "${rejects}" >&2
        echo "            Either Fasta_to_Contig2Bin.sh's columns or the binner's output" >&2
        echo "            naming has moved; both decide which contigs DAS_Tool scores." >&2
        exit 65
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

# DAS_Tool exits non-zero for TWO very different reasons: (a) a legitimate
# "no bin clears the score threshold" outcome (a low-biomass sample — a valid
# LCG-only success), and (b) a real crash (OOM, missing diamond, corrupt input).
# We must NOT swallow (b). Capture the exit code + full log, and treat a non-zero
# exit as the benign empty case ONLY when the log carries DAS_Tool's specific
# no-bins message; anything else fails the step loudly (the repo's fail-fast
# ethos). The default on a non-zero exit is to FAIL.
set +e
# `--write_bins` is a BOOLEAN flag (DAS_Tool 1.1.x docopt spec: "Export bins as
# fasta files.", no value) — pass it bare, exactly as qp-pacbio does. A trailing
# value (`--write_bins 1`) is an unexpected positional that r-docopt 0.7.2 renders
# as `'short' is not a valid field or method name for reference class "Argument"`
# and Execution-halts before DAS_Tool runs — a real crash that this step's
# no-bins/other-crash split (below) correctly fails loud on. Probed against
# das_tool 1.1.7 / r-docopt 0.7.2: the bare form parses, the `1` form crashes.
micromamba run -n dastool DAS_Tool \
    --bins="${bins_csv}" --contigs="${NOLCG}" \
    --outputbasename="${WORK}/dastool" --labels="${labels_csv}" \
    --threads="${THREADS}" --search_engine=diamond --write_bins \
    > "${WORK}/dastool.log" 2>&1
das_rc=$?
set -e
cat "${WORK}/dastool.log" >&2

if [[ "${das_rc}" -ne 0 ]]; then
    # DAS_Tool's benign "nothing passed the score threshold" message. This regex is
    # a best-effort match over the phrasings DAS_Tool uses for that outcome; the
    # default on any non-zero exit is to FAIL — only this specific pattern is
    # accepted as an empty success.
    if grep -qiE 'no bins.*(score|threshold|passed|found)|no high.?quality bins' "${WORK}/dastool.log"; then
        echo "bin_refine: DAS_Tool reported no bins above the score threshold — LCG-only success." >&2
        qiita_finish refined_bins_dir=refined_bins
        exit 0
    fi
    echo "bin_refine: DAS_Tool failed (exit ${das_rc}); log does not match the benign no-bins message — failing the step." >&2
    exit "${das_rc}"
fi

# Exit 0 but no bins written (edge case): treat an empty output dir as the benign
# empty outcome.
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
