#!/bin/bash
# Assemble masked HiFi reads, then split circular genomes (LCG) from the linear
# contigs (noLCG). Output `genomes_dir` =
# $QIITA_OUTPUT_PATH/genomes:
#   circular.fa   every circular contig, ANY size, as one multi-FASTA (ingested as
#                 LCG; the >=512 kb "large complete genome" cut is a query-time
#                 predicate on the stored length, not a filter applied here). The
#                 native assembly_hash step reads this with read_fastx, so there is
#                 no per-contig split and bin_id is the contig id from the record.
#   noLCG.fa      the non-circular contigs (input to binning + bin_refine)
# Zero contigs is left as an empty genomes_dir; downstream steps skip cleanly and
# assembly_hash turns the all-empty result into StepNoData.
source /opt/qiita/_lib.sh

READS_FASTQ="$(qiita_input masked_reads_fastq)"
RUN_CONFIG="$(qiita_input run_config)"
ASSEMBLER="$(jq -er '.assembler' "${RUN_CONFIG}")"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
OUT="${QIITA_OUTPUT_PATH}/genomes"
mkdir -p "${OUT}"

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

# GFA S-line -> FASTA. hifiasm-meta encodes circularity in the segment NAME: the
# documented contig-name shape is `s[0-9]+\.[uc]tg[0-9]{6}[lc]` where the trailing
# letter is `c` (circular) or `l` (linear) — e.g. `s1.utg000001c` vs
# `s1.utg000001l` (hifiasm-meta man page / README). We anchor the match to
# `tg[0-9]+c$` rather than a bare `c$` so only a well-formed circular segment name
# matches (a bare `c$` would also catch any non-canonical name ending in 'c'); a
# name that doesn't match the circular shape falls through to noLCG (binned),
# which is the safe default. Circular -> LCG candidates; the rest -> noLCG.fa (fed
# to binning).
#
# A linear chromosome is a single contig too, but LCG means a large *circular*
# genome. A complete but LINEAR chromosome is not circular, so it flows to noLCG
# and is recovered through binning (as a single-contig MAG if it bins alone). Only
# closed circular molecules shortcut past binning as LCG.
# Circular segments -> circular.fa (ALL of them, any size); the rest -> noLCG.fa.
# We keep EVERY circular contig: the >=512 kb "large complete genome" (LCG) cut is
# a query-time predicate on the stored length (WHERE sequence_length_bp >= 524288),
# NOT a delete here. A circular contig <512 kb is very often a REAL molecule — a
# plasmid, phage, or other small replicon — and (being circular) never reaches
# noLCG/binning, so a `find -size -512k` delete (qp-pacbio's original) would drop
# it with no recovery. circular.fa is a single multi-FASTA (no per-contig split):
# the native assembly_hash step reads it with read_fastx and each record's id is
# the LCG bin_id. Everything here is ingested under kind='LCG'.
if [[ -s "${GFA}" ]]; then
    awk '$1=="S" && $2 ~ /tg[0-9]+c$/  {printf ">%s\n%s\n", $2, $3}' "${GFA}" > "${OUT}/circular.fa"
    awk '$1=="S" && $2 !~ /tg[0-9]+c$/ {printf ">%s\n%s\n", $2, $3}' "${GFA}" > "${OUT}/noLCG.fa"
fi

qiita_finish genomes_dir=genomes
