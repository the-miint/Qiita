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

# GFA S-line -> FASTA. hifiasm-meta encodes circularity in the segment NAME: the
# documented contig-name shape is `s[0-9]+\.[uc]tg[0-9]{6}[lc]` where the trailing
# letter is `c` (circular) or `l` (linear) — e.g. `s1.utg000001c` vs
# `s1.utg000001l` (hifiasm-meta man page / README). We anchor the match to
# `tg[0-9]+c$` rather than a bare `c$` so only a well-formed circular segment name
# matches (a bare `c$` would also catch any non-canonical name ending in 'c'); a
# name that doesn't match the circular shape falls through to noLCG (binned),
# which is the safe default. Circular -> LCG candidates; the rest -> noLCG.fa (fed
# to binning). Still worth re-confirming on the first Linux build against a real
# hifiasm-meta GFA (the S-line byte layout was not directly inspected).
#
# G:43 (reviewer "a linear chromosome is a single contig too?"): yes, and that is
# intended — LCG means a large *circular* genome. A complete but LINEAR chromosome
# is not circular, so it flows to noLCG and is recovered through binning (as a
# single-contig MAG if it bins alone). Only closed circular molecules shortcut
# past binning as LCG. This matches qp-pacbio's split.
if [[ -s "${GFA}" ]]; then
    awk '$1=="S" && $2 ~ /tg[0-9]+c$/  {printf ">%s\n%s\n", $2, $3}' "${GFA}" > "${WORK}/circular.fa"
    awk '$1=="S" && $2 !~ /tg[0-9]+c$/ {printf ">%s\n%s\n", $2, $3}' "${GFA}" > "${OUT}/noLCG.fa"
fi

# One file per circular contig (a circular genome is single-contig), then keep
# only those >=512 kb as LCG (qp-pacbio's `find -size -512k` filter, inverted).
#
# REVIEW (G:44 — needs a human/bioinformatics call; NOT changed here): a circular
# contig <512 kb is very often a REAL biological molecule — a plasmid (or a phage /
# small replicon) — not an assembly artifact. Deleting it here silently DISCARDS
# that sequence with no recovery: it is circular so it never reaches noLCG/binning
# either, so a sample's plasmids are lost entirely. A better outcome is to KEEP
# these and store them under a distinct `kind` (e.g. 'plasmid' or 'small_circular')
# — the storage schema's `kind` column is plain TEXT specifically to allow new
# kinds, so this is additive. But it changes the closed kind value set that flows
# end-to-end (assembly_hash `_KIND_*`, assembly_load, the DuckLake
# assembly_membership/bin_quality `kind` column), and the exact size cutoff /
# whether to bin-quality-assess plasmids is a biology decision. Left as-is (bare
# deletion) pending that confirmation; when confirmed, thread a new kind through
# `_file_meta` in assembly_hash rather than rm'ing here.
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
