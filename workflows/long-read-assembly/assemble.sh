#!/bin/bash
# Assemble masked HiFi reads, then split circular genomes (LCG) from the linear
# contigs (noLCG). One output. `genomes_dir` =
# $QIITA_OUTPUT_PATH/genomes:
#   circular.fa   every circular contig, ANY size, as one multi-FASTA (ingested as
#                 LCG; the >=512 kb "large complete genome" cut is a query-time
#                 predicate on the stored length, not a filter applied here). The
#                 native assembly_hash step reads this with read_fastx, so there is
#                 no per-contig split and bin_id is the contig id from the record.
#   noLCG.fa      the non-circular contigs (input to binning + bin_refine)
#   contig_attributes.tsv
#                 one row per contig across both FASTAs — the assembler's own
#                 per-contig report, joined onto qiita.assembly_membership by
#                 both writers. Not read by any container step.
# Zero contigs is left as an empty genomes_dir; downstream steps skip cleanly and
# assembly_hash turns the all-empty result into StepNoData.
#
# $QIITA_OUTPUT_PATH/assembler holds the assembler's own output tree, whole and
# unedited. It is not a declared step output and no step reads it; it is retained
# by being written under the output root, where `qiita_finish` lists it in
# manifest.json and the verifier checks each file's size and mode. It is not a
# second copy of anything either: each arm below points the assembler's `-o`
# straight at it and then reads its one file back out, so the two published
# FASTAs are derived from this tree rather than duplicated from it.
#
# ASM_DIR must stay under $QIITA_OUTPUT_PATH. With no `outputs:` entry naming it,
# nothing at run time proves it does — a path pointing elsewhere would restore
# the original defect silently, since files outside the output root are neither
# listed nor verified. `test_assemble_runs_the_assembler_into_its_own_output`
# is what holds it.
#
# Nothing here names a file inside the RETAINED tree, which is what lets one
# directory hold both arms' layouts and survive a release that renames one — a
# rename changes this directory's contents instead of needing a change here. The
# two files each arm reads back to build its published output are named below,
# and a moved name fails the step there rather than silently emptying it.
#
# It is not free. On a 1.24 Gbp masked read set — 12% of one real ticket's reads
# — the tree is 1.44 GB for myloasm and 697 MB for hifiasm_meta, against the
# 102 MB / 76 MB single file each arm reads back. Assembly output does not scale
# linearly with input and has not been measured at a second point, so treat that
# as a floor rather than a per-ticket figure. Roughly a third of myloasm's is one
# m/t/r parameter sweep over near-identical copies of the same graph
# (`1-light_resolve` + `2-heavy_path_resolve`), and another 564 MB is
# `binary_temp/`, its own scratch. Keeping those is the price of naming no
# filenames. Nothing is copied at step end: the assembler writes here directly,
# so the bytes are charged to the per-attempt ticket workspace.
#
# That workspace does not currently expire. docs/architecture/storage.md states
# ephemeral per-ticket directories are deleted 45 days past the ticket's terminal
# state, but no sweep implementing it exists in this repo, so workspaces — and
# now these trees — accumulate. This retains in place; it does not archive.
# Anything that must survive the sweep being implemented needs storage of its
# own.
#
# What bounds the file count is the manifest: `qiita_finish` lists every file
# under $QIITA_OUTPUT_PATH, and the orchestrator caps how large that manifest may
# be (`slurm/verify.py`), rejecting an over-large one as a permanent
# CONTRACT_VIOLATION. The count is structural rather than a function of input
# size — the sweep above emits a fixed number of graphs — and completed runs
# wrote 152 files for myloasm and 17 for hifiasm_meta, orders of magnitude under
# that cap. Several come out zero-byte
# (`asm.bins.tsv`, `asm.rescue*.fa`); the gates take those as they are.
#
# HOW EACH ASSEMBLER SIGNALS CIRCULARITY — the two do NOT agree, and that is the
# whole reason this is a per-assembler branch rather than one shared tail:
#   hifiasm_meta  in the GFA segment NAME (`…tg……c` circular / `…l` linear)
#   myloasm       in the assembly_primary.fa HEADER (`_circular-yes`); its GFA
#                 segment names carry no circularity marker at all
# Each branch below therefore writes circular.fa + noLCG.fa itself. Reusing one
# assembler's rule on the other's output fails SILENTLY — it matches nothing, so
# every circular genome is quietly demoted to binning input and the LCG class
# disappears with no error anywhere. myloasm_split.py holds the myloasm side of
# this, including the probe that established it; don't restate it there and here.
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

OUT="${QIITA_OUTPUT_PATH}/genomes"
ASM_DIR="${QIITA_OUTPUT_PATH}/assembler"
# Both output dirs are cleared before use, not just created. `qiita_finish`
# leaves every directory 0550 and every file 0440, and a directory stripped of
# its write bit does not give up its entries — over such a tree `rm -rf` alone
# exits 1 (probed as a non-root uid on Linux and macOS), and the awk redirect
# into circular.fa cannot truncate a 0440 file either. So restore write first.
# The CP hands an ordinary retry a fresh attempt dir; a SLURM-side requeue
# re-enters this one, which is the case this covers.
for d in "${OUT}" "${ASM_DIR}"; do
    [[ -d "${d}" ]] || continue
    chmod -R u+w "${d}"
    rm -rf "${d}"
done
mkdir -p "${OUT}" "${ASM_DIR}"

# Both arms create BOTH FASTAs whenever the assembler produced anything, and
# neither when it didn't — so an empty CLASS is an empty file while an empty
# ASSEMBLY is an empty genomes_dir. Downstream depends on that distinction:
# assembly_coverage treats a missing OR zero-byte noLCG.fa as "nothing to bin",
# and assembly_hash drops the empty files before its scan (its module docstring
# states the StepNoData boundary).
# Each arm gets this for free from its writer (a shell `>` redirect truncates into
# existence; a zero-row COPY still writes its file), so there is no pre-creation
# step to keep in sync.
#
# The attribute sidecar is written under that same guard, so it is present
# whenever the two FASTAs are. Its ABSENCE therefore means a run assembled before
# it existed, which is what both readers key on (see
# qiita_common.assembly_constants.register_contig_attribute_table).
case "${ASSEMBLER}" in
    hifiasm_meta)
        micromamba run -n hifiasm_meta hifiasm_meta -t "${THREADS}" -o "${ASM_DIR}/asm" "${READS_FASTQ}"
        GFA="${ASM_DIR}/asm.p_ctg.gfa"

        # GFA S-line -> FASTA. hifiasm-meta's documented contig-name shape is
        # `s[0-9]+\.[uc]tg[0-9]{6}[lc]` where the trailing letter is `c` (circular)
        # or `l` (linear) — e.g. `s1.utg000001c` vs `s1.utg000001l` (hifiasm-meta
        # man page / README). Both letters are matched as `tg[0-9]+[cl]$` rather
        # than a bare `[cl]$`, so only a well-formed name matches: a bare `c$`
        # would also catch any non-canonical name ending in 'c'.
        #
        # A name matching NEITHER shape fails the step. The circularity call is
        # stored per contig, so an unclassified name would otherwise be routed to
        # noLCG and ALSO write a `no` into the lake for a contig nobody
        # classified. The myloasm arm fails on an unparseable header for the same
        # reason, and this arm's version is pinned (assemble.def), so a name
        # outside the documented shape means the grammar moved. A missing GFA is
        # an empty assembly, not a violation.
        #
        # The attribute pass runs FIRST so a bad name stops the step before any
        # FASTA is written. The two FASTA writers keep their own `>` redirects,
        # which is what holds the "empty CLASS is an empty FILE" invariant above:
        # the shell truncates each into existence whether or not the awk matches,
        # where awk's own redirection would create a file only on first write.
        if [[ -s "${GFA}" ]]; then
            # One grammar, defined once and shared by the attribute pass and both
            # FASTA writers, so the three cannot drift into disagreeing about
            # which names are circular.
            CIRC_RE='tg[0-9]+c$'
            LIN_RE='tg[0-9]+l$'

            # S-line -> one attribute row. Probed against the pinned build
            # (hamtv0.3.5) on a 60 kb synthetic replicon: `p_ctg.gfa` carries one
            # S-line per contig as
            #     S  s0.ctg000001c  <seq>  LN:i:60000  dp:f:98  ts:B:I,0
            # with the circular/linear call in the name's trailing letter and the
            # depth in `dp:f`. The control was the same generator with wrap-around
            # removed, which produced `s0.ctg000001l` — so the suffix tracks
            # topology rather than being incidental, and both classes carry the
            # tag. `dp:f` is searched for BY TAG among fields 4+ rather than by
            # position: it sits at $5 in that layout, but GFA does not fix tag
            # order and reading $5 would silently store `ts:B:I`'s value the day
            # it moves. `mult` is empty for every row — hifiasm_meta has no
            # counterpart to myloasm's k-mer multiplicity.
            #
            # Re-measured on a real metagenome assembled with the same pinned
            # build (host qm, ~antoniog/claude-probe/run_real; the binary reports
            # 0.13-r308 / 0.3-r079, which is what assemble.def's %test asserts):
            # 2899 contigs, EVERY one carrying dp:f, depth 1-145, and every name
            # matching the grammar above -- 1 circular, 2898 linear, 0 unmatched.
            # All 2899 S-lines carry exactly LN:i dp:f ts:B. That is what makes
            # the tag's absence, rather than its position, the thing worth
            # guarding; the search is still by TAG because GFA does not fix the
            # order and the cost of not assuming it is one loop.
            awk -v attrs="${OUT}/contig_attributes.tsv" -v circ="${CIRC_RE}" -v lin="${LIN_RE}" '
                BEGIN { OFS = "\t"; print "contig_id", "raw_name", "circularity", "depth", "mult" > attrs }
                $1 != "S" { next }
                {
                    if ($2 ~ circ)     { call = "yes" }
                    else if ($2 ~ lin) { call = "no" }
                    else {
                        bad++
                        if (bad <= 3) { names = names " " $2 }
                        next
                    }
                    dp = ""
                    for (i = 4; i <= NF; i++) { if ($i ~ /^dp:f:/) { dp = substr($i, 6) } }
                    if (dp != "") { with_dp++ }
                    seen++
                    print $2, $2, call, dp, "" > attrs
                }
                END {
                    if (bad) {
                        printf "assemble: %d GFA segment name(s) match neither the circular (tg<N>c) nor the linear (tg<N>l) shape, e.g.%s\n", bad, names > "/dev/stderr"
                        printf "          re-probe hifiasm_meta name grammar against the version pinned in assemble.def\n" > "/dev/stderr"
                        exit 65
                    }
                    # NONE carrying dp:f is the shape a renamed or moved tag
                    # produces, and it is the only shape this pass can tell apart
                    # from data: fail rather than store a depth-less run, the same
                    # fail-closed rule myloasm_split.py applies to its own depth.
                    if (seen && !with_dp) {
                        printf "assemble: none of %d GFA segment(s) carried a dp:f tag\n", seen > "/dev/stderr"
                        printf "          re-probe hifiasm_meta depth tag against the version pinned in assemble.def\n" > "/dev/stderr"
                        exit 65
                    }
                    # SOME carrying it is tolerated, and NOT because the reach of
                    # the tag is in doubt -- the run above puts it on 2899 of 2899.
                    # It is tolerated because such a segment stays readable after
                    # the fact: its row still carries raw_name and circularity, so
                    # a NULL depth beside a non-NULL raw_name reads as "this
                    # segment reported none", where a run assembled before the
                    # sidecar existed has all four NULL. Probed, and pinned in
                    # test_assembly_constants.py in qiita-common, since that
                    # property is what this tolerance rests on. Counted onto
                    # stderr so it shows in the step log as well as in the data.
                    if (seen && with_dp < seen) {
                        printf "assemble: %d of %d GFA segment(s) carried no dp:f tag; depth stored NULL for those\n", seen - with_dp, seen > "/dev/stderr"
                    }
                }' "${GFA}"

            awk -v re="${CIRC_RE}" '$1=="S" && $2 ~ re {printf ">%s\n%s\n", $2, $3}' "${GFA}" > "${OUT}/circular.fa"
            awk -v re="${CIRC_RE}" '$1=="S" && $2 !~ re {printf ">%s\n%s\n", $2, $3}' "${GFA}" > "${OUT}/noLCG.fa"
        fi
        ;;
    myloasm)
        # `--hifi` because this workflow's input is masked PacBio HiFi reads
        # end to end (assembly_coverage maps them back with minimap2 `map-hifi`).
        # Supporting ONT would mean threading a read type through the action
        # context and swapping this for `--nano-r10` / `--nano-r9`; it is a
        # deliberate match to the input, not a default nobody chose.
        micromamba run -n myloasm myloasm "${READS_FASTQ}" -o "${ASM_DIR}" -t "${THREADS}" --hifi
        PRIMARY="${ASM_DIR}/assembly_primary.fa"

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
        # exactly as the hifiasm arm does for an empty GFA. It must also not reach
        # the splitter — miint's `read_fastx` RAISES on a zero-record input ("Error
        # Empty file: …") rather than returning no rows.
        #
        # myloasm_split.py owns the split and carries the probe behind it: why the
        # header and not the GFA, why only `circular-yes` counts, and why the id is
        # cut at `_len-`. It LOADs the deploy-staged miint bind-mounted by this
        # step's `derived_inputs: MIINT_EXTENSION_DIRECTORY`.
        if [[ -s "${PRIMARY}" ]]; then
            python3 /opt/qiita/myloasm_split.py \
                "${PRIMARY}" "${OUT}/circular.fa" "${OUT}/noLCG.fa" \
                "${OUT}/contig_attributes.tsv"
        fi
        ;;
    *)
        echo "unknown assembler: ${ASSEMBLER}" >&2
        exit 64
        ;;
esac

qiita_finish genomes_dir=genomes
