#!/bin/bash
# Step 1 (qp-pacbio steps 1+2): assemble masked HiFi reads, then split circular
# genomes (LCG) from the linear contigs (noLCG). Output `genomes_dir` =
# $QIITA_OUTPUT_PATH/genomes:
#   LCG/<contig>.fna   one circular genome >=512 kb per file (ingested as LCG)
#   noLCG.fa           the non-circular contigs (input to binning + bin_refine)
# Zero contigs is left as an empty genomes_dir; downstream steps skip cleanly and
# pacbio_ingest turns the all-empty result into StepNoData.
source /opt/qiita/_lib.sh

READS_FASTQ="$(qiita_input masked_reads_fastq)"
RUN_CONFIG="$(qiita_input run_config)"
ASSEMBLER="$(jq -er '.assembler' "${RUN_CONFIG}")"

WORK="$(mktemp -d)"
OUT="${QIITA_OUTPUT_PATH}/genomes"
mkdir -p "${OUT}/LCG"

case "${ASSEMBLER}" in
    hifiasm_meta)
        micromamba run -n assemble hifiasm_meta -t "${THREADS}" -o "${WORK}/asm" "${READS_FASTQ}"
        GFA="${WORK}/asm.p_ctg.gfa"
        ;;
    myloasm)
        echo "assembler 'myloasm' is not implemented in this image yet" >&2
        exit 64
        ;;
    *)
        echo "unknown assembler: ${ASSEMBLER}" >&2
        exit 64
        ;;
esac

# GFA S-line -> FASTA. hifiasm-meta marks a circular unitig with a trailing 'c'
# in its segment name (qp-pacbio's convention: `$2 ~ /c$/`). Circular -> LCG
# candidates; the rest -> noLCG.fa (fed to binning). VALIDATE the 'c' marker on
# the first Linux build against a real hifiasm-meta GFA.
if [[ -s "${GFA}" ]]; then
    awk '$1=="S" && $2 ~ /c$/  {printf ">%s\n%s\n", $2, $3}' "${GFA}" > "${WORK}/circular.fa"
    awk '$1=="S" && $2 !~ /c$/ {printf ">%s\n%s\n", $2, $3}' "${GFA}" > "${OUT}/noLCG.fa"
fi

# One file per circular contig (a circular genome is single-contig), then keep
# only those >=512 kb as LCG (qp-pacbio's `find -size -512k` filter, inverted).
if [[ -s "${WORK}/circular.fa" ]]; then
    awk -v d="${OUT}/LCG" '
        /^>/ { id=substr($1,2); f=d"/"id".fna" }
        { print > f }
    ' "${WORK}/circular.fa"
    for f in "${OUT}/LCG"/*.fna; do
        [[ -e "$f" ]] || continue
        if [[ "$(stat -c%s "$f")" -lt 524288 ]]; then
            rm -f "$f"
        fi
    done
fi

qiita_finish genomes_dir=genomes
