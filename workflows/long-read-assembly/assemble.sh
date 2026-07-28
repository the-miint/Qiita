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
#
# HOW EACH ASSEMBLER SIGNALS CIRCULARITY — the two do NOT agree, and that is the
# whole reason this is a per-assembler branch rather than one shared tail:
#   hifiasm_meta  in the GFA segment NAME (`…tg……c` circular / `…l` linear)
#   myloasm       in the assembly_primary.fa HEADER (`_circular-yes`); its GFA
#                 segment names carry no circularity marker at all
# Each branch below therefore writes circular.fa + noLCG.fa itself. Reusing one
# assembler's rule on the other's output fails SILENTLY — it matches nothing, so
# every circular genome is quietly demoted to binning input and the LCG class
# disappears with no error anywhere.
#
# A linear chromosome is a single contig too, but LCG means a large *circular*
# genome. A complete but LINEAR chromosome is not circular, so it flows to noLCG
# and is recovered through binning (as a single-contig MAG if it bins alone). Only
# closed circular molecules shortcut past binning as LCG.
#
# We keep EVERY circular contig: the >=512 kb "large complete genome" (LCG) cut is
# a query-time predicate on the stored length (WHERE sequence_length_bp >= 524288),
# NOT a delete here. A circular contig <512 kb is very often a REAL molecule — a
# plasmid, phage, or other small replicon — and (being circular) never reaches
# noLCG/binning, so a `find -size -512k` delete (qp-pacbio's original) would drop
# it with no recovery. circular.fa is a single multi-FASTA (no per-contig split):
# the native assembly_hash step reads it with read_fastx and each record's id is
# the LCG bin_id. Everything here is ingested under kind='LCG'.
source /opt/qiita/_lib.sh

READS_FASTQ="$(qiita_input masked_reads_fastq)"
RUN_CONFIG="$(qiita_input run_config)"
ASSEMBLER="$(jq -er '.assembler' "${RUN_CONFIG}")"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
OUT="${QIITA_OUTPUT_PATH}/genomes"
mkdir -p "${OUT}"

# Create both files up front, and ONLY once the assembler produced something, so
# an empty CLASS is an empty file while an empty ASSEMBLY is an empty
# genomes_dir. Downstream depends on that distinction: assembly_coverage treats a
# missing OR zero-byte noLCG.fa as "nothing to bin", and assembly_hash raises
# StepNoData only when neither an LCG nor a MAG exists.
init_outputs() {
    : > "${OUT}/circular.fa"
    : > "${OUT}/noLCG.fa"
}

case "${ASSEMBLER}" in
    hifiasm_meta)
        micromamba run -n hifiasm_meta hifiasm_meta -t "${THREADS}" -o "${WORK}/asm" "${READS_FASTQ}"
        GFA="${WORK}/asm.p_ctg.gfa"

        # GFA S-line -> FASTA. hifiasm-meta's documented contig-name shape is
        # `s[0-9]+\.[uc]tg[0-9]{6}[lc]` where the trailing letter is `c` (circular) or `l`
        # (linear) — e.g. `s1.utg000001c` vs `s1.utg000001l` (hifiasm-meta man page /
        # README). We anchor the match to `tg[0-9]+c$` rather than a bare `c$` so only a
        # well-formed circular segment name matches (a bare `c$` would also catch any
        # non-canonical name ending in 'c'); a name that doesn't match the circular shape
        # falls through to noLCG (binned), which is the safe default.
        if [[ -s "${GFA}" ]]; then
            init_outputs
            awk '$1=="S" && $2 ~ /tg[0-9]+c$/  {printf ">%s\n%s\n", $2, $3}' "${GFA}" > "${OUT}/circular.fa"
            awk '$1=="S" && $2 !~ /tg[0-9]+c$/ {printf ">%s\n%s\n", $2, $3}' "${GFA}" > "${OUT}/noLCG.fa"
        fi
        ;;
    myloasm)
        # `--hifi` because this workflow's input is masked PacBio HiFi reads
        # end to end (assembly_coverage maps them back with minimap2 `map-hifi`).
        # Supporting ONT would mean threading a read type through the action
        # context and swapping this for `--nano-r10` / `--nano-r9`; it is a
        # deliberate match to the input, not a default nobody chose.
        micromamba run -n myloasm myloasm "${READS_FASTQ}" -o "${WORK}/myloasm" -t "${THREADS}" --hifi
        PRIMARY="${WORK}/myloasm/assembly_primary.fa"

        # A MISSING primary FASTA is a contract violation, not an empty assembly.
        # _lib.sh sets `set -e`, so reaching this line means myloasm exited 0 — and
        # myloasm was probed to exit NON-zero and write nothing when it cannot
        # assemble (3 reads: "No k-mers found. Exiting.", exit 1). So a zero exit
        # with no output file means the path or filename moved under us, e.g. a
        # release renaming it to `assembly_primary.fasta`. Left fail-open, that
        # produces an empty genomes_dir, which assembly_hash reports as the
        # terminal StepNoData "this sample assembled nothing" — every sample in the
        # run silently discarded, with no error anywhere and no retry. Fail loud
        # instead.
        if [[ ! -e "${PRIMARY}" ]]; then
            echo "myloasm exited 0 but wrote no ${PRIMARY} — the output filename or" >&2
            echo "layout has moved; re-probe it against the pinned myloasm version" >&2
            exit 64
        fi

        # Present-but-empty IS a legitimate empty assembly: leave genomes_dir empty,
        # exactly as the hifiasm branch does for an empty GFA. It must also not
        # reach the splitter — miint's `read_fastx` RAISES on a zero-record input
        # ("Error Empty file: …") rather than returning no rows.
        #
        # Circularity comes from the FASTA header, NOT the GFA — myloasm_split.py
        # carries the probe that established it, why only `circular-yes` counts,
        # and why the id is cut at `_len-`. It reads with miint's read_fastx and
        # writes with `COPY … (FORMAT FASTA)`, LOADing the SAME deploy-staged
        # extension the rest of the system runs (bind-mounted read-only via this
        # step's `derived_inputs: MIINT_EXTENSION_DIRECTORY`). No init_outputs
        # here: a zero-row COPY still creates its file, so an empty class is an
        # empty file already.
        if [[ -s "${PRIMARY}" ]]; then
            python3 /opt/qiita/myloasm_split.py \
                "${PRIMARY}" "${OUT}/circular.fa" "${OUT}/noLCG.fa"
        fi
        ;;
    *)
        echo "unknown assembler: ${ASSEMBLER}" >&2
        exit 64
        ;;
esac

qiita_finish genomes_dir=genomes
