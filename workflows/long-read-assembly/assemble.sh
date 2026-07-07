#!/bin/bash
# Assemble masked HiFi reads, then split circular genomes (LCG) from the linear
# contigs (noLCG). Output `genomes_dir` =
# $QIITA_OUTPUT_PATH/genomes:
#   LCG/<contig>.fna   one circular contig per file, ANY size (ingested as LCG;
#                      the >=512 kb "large complete genome" cut is a query-time
#                      predicate on the stored length, not a filter applied here)
#   noLCG.fa           the non-circular contigs (input to binning + bin_refine)
# Zero contigs is left as an empty genomes_dir; downstream steps skip cleanly and
# assembly_hash turns the all-empty result into StepNoData.
source /opt/qiita/_lib.sh

READS_FASTQ="$(qiita_input masked_reads_fastq)"
RUN_CONFIG="$(qiita_input run_config)"
ASSEMBLER="$(jq -er '.assembler' "${RUN_CONFIG}")"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
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
if [[ -s "${GFA}" ]]; then
    awk '$1=="S" && $2 ~ /tg[0-9]+c$/  {printf ">%s\n%s\n", $2, $3}' "${GFA}" > "${WORK}/circular.fa"
    awk '$1=="S" && $2 !~ /tg[0-9]+c$/ {printf ">%s\n%s\n", $2, $3}' "${GFA}" > "${OUT}/noLCG.fa"
fi

# One file per circular contig (a circular genome is single-contig). We keep
# EVERY circular contig regardless of size — the >=512 kb "large complete genome"
# (LCG) distinction is applied ON THE FLY at query time against the stored
# sequence_length_bp (WHERE sequence_length_bp >= 524288), NOT by deleting here.
# A circular contig <512 kb is very often a REAL biological molecule — a plasmid,
# phage, or other small replicon — and (being circular) it never reaches
# noLCG/binning, so a `find -size -512k` delete (qp-pacbio's original) would
# discard that sequence with no recovery path. Storing all circulars keeps them
# queryable; the size cut is a predicate downstream, not a destructive filter.
# Everything here is ingested under kind='LCG'; a consumer that wants only
# chromosome-scale genomes filters on length.
if [[ -s "${WORK}/circular.fa" ]]; then
    # Split the multi-FASTA of circular contigs into one file per sequence with
    # seqkit (a proper FASTA tool, not a hand-rolled awk parser). seqkit names its
    # split outputs by its own convention, so normalise each to LCG/<contig_id>.fna:
    # assembly_hash derives each LCG's bin_id from the FILENAME stem (it scans with
    # read_fastx include_filepath), so the file name must be exactly the contig id.
    SPLIT_DIR="${WORK}/lcg_split"
    micromamba run -n assemble seqkit split -i -O "${SPLIT_DIR}" "${WORK}/circular.fa" >/dev/null
    for f in "${SPLIT_DIR}"/*; do
        [[ -f "${f}" ]] || continue
        # The contig id is the file's single record's first header token — the same
        # id seqkit split keyed on and the id read_fastx will report downstream.
        id="$(micromamba run -n assemble seqkit seq -n -i "${f}")"
        mv "${f}" "${OUT}/LCG/${id}.fna"
    done
fi

qiita_finish genomes_dir=genomes
