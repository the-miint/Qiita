#!/bin/bash
# metaWRAP binning of the noLCG contigs with three binners (metabat2 + maxbin2 +
# concoct). Output `bins_dir` =
# $QIITA_OUTPUT_PATH/bins/{metabat2_bins,maxbin2_bins,concoct_bins}/ (whichever
# binners produced anything). No contigs, or no bins at all, leaves an empty
# bins_dir — bin_refine handles that.
#
# COVERAGE COMES FROM minimap2, NOT bwa — read this before touching work_files/.
#
# metaWRAP computes coverage by self-aligning the reads with bwa, a SHORT-read
# aligner, and there is no aligner-selection flag on `metawrap binning`. The only
# mechanism for using a different aligner is to place a pre-made BAM at
# <out>/work_files/<sample>.bam: metaWRAP guards its own `bwa mem` behind
# `if [[ ! -f ... ]]` and skips it when that file already exists, then derives
# depth from `work_files/*.bam`. This is the same seam qp-pacbio uses.
#
# THE GUARD WRAPS THE SORT TOO. In metaWRAP's binning.sh the `bwa mem` and the
# `samtools sort` that follows it are both inside that one
# `if [[ ! -f ${out}/work_files/${sample}.bam ]]` block (verified against
# /opt/conda/envs/metawrap/bin/metawrap-modules/binning.sh in the deployed image),
# so pre-placing the BAM skips the sort as well as the alignment, silently.
# Whatever is staged must therefore already be coordinate sorted.
#
# So the native `assembly_coverage` step pre-maps with miint's embedded minimap2
# (`map-hifi`) and writes that BAM coordinate sorted (`ORDER BY reference,
# position` over a name-sorted @SQ — see its module docstring), and this entrypoint
# stages it into work_files/ under the name metaWRAP will look for. bwa is still
# INSTALLED and still runs: `bwa index` is unconditional (guarded only by
# assembly.fa.bwt) and produces an index nothing then uses — that is also why
# qp-pacbio's environment carries bwa.
#
# The BAM's @SQ names must match the contigs metaWRAP indexes, which they do
# because both sides are noLCG.fa. Their ORDER must match too — metabat2 aborts if
# the depth matrix (in @SQ order) and the assembly disagree — which is why the
# assembly is reordered to @SQ order below before metaWRAP sees it — a requirement
# independent of record order; see its comment. samtools is required regardless of
# this path: metaWRAP's concoct block runs `samtools index` over work_files/*.bam.
source /opt/qiita/_lib.sh

GENOMES_DIR="$(qiita_input genomes_dir)"
READS_FASTQ="$(qiita_input masked_reads_fastq)"
COVERAGE_BAM="$(qiita_input coverage_bam)"
NOLCG="${GENOMES_DIR}/noLCG.fa"
OUT="${QIITA_OUTPUT_PATH}/bins"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "${OUT}"

# Nothing to bin (all-circular or empty assembly) -> empty bins_dir, exit 0.
if [[ ! -s "${NOLCG}" ]]; then
    qiita_finish bins_dir=bins
    exit 0
fi

# metaWRAP's --single-end wants a plain (uncompressed) .fastq path.
# One stem for both the FASTQ and the staged BAM. metaWRAP derives its `sample`
# from the reads filename, then looks for work_files/<sample>.bam — so these two
# names MUST agree, and a single variable makes that structural rather than a
# comment someone can miss.
READS_STEM="reads"
READS_FQ="${WORK}/${READS_STEM}.fastq"
if [[ "${READS_FASTQ}" == *.gz ]]; then
    pigz -dc "${READS_FASTQ}" > "${READS_FQ}"
else
    cp "${READS_FASTQ}" "${READS_FQ}"
fi

# Stage the pre-mapped BAM into metaWRAP's alignment cache, so it skips its own
# `bwa mem` (and, per the header, its `samtools sort`).
# The name is NOT free: metaWRAP derives `sample` from the READS filename
# (`tmp=${reads##*/}; sample=${tmp%.*}`) and then looks for
# work_files/${sample}.bam — so this must track READS_FQ's basename, and the two
# have to be renamed together. metaWRAP only mkdir's work_files when it is absent,
# so pre-creating it here is safe.
#
# The empty-BAM check below is belt-and-braces, not a reachable branch today:
# assembly_coverage only emits a zero-byte BAM when noLCG.fa is empty, and this
# script already exited for that case above. It is still worth failing on,
# because the consequence of getting it wrong is SILENT — metaWRAP would fall
# back to bwa self-alignment and produce plausible bwa-derived coverage, with
# nothing in the output to say the minimap2 pre-map had been skipped. Checked
# BEFORE the directory is created so a failure leaves nothing half-staged.
if [[ ! -s "${COVERAGE_BAM}" ]]; then
    echo "coverage_bam is empty but noLCG.fa is not — refusing to let metaWRAP" >&2
    echo "silently fall back to bwa self-alignment. Check the assembly_coverage step." >&2
    exit 64
fi
WORK_FILES="${OUT}/work_files"
mkdir -p "${WORK_FILES}"
# Place the pre-mapped BAM at the name metaWRAP will look for — the step metaWRAP
# skips along with its `bwa mem` (see the header). The file arrives coordinate
# sorted: assembly_coverage COPYs it `ORDER BY reference, position` and miint emits
# @SQ sorted by reference NAME, so name order is tid order. jgi and `samtools index`
# were both measured against an unordered control on this image's pins (metabat2
# 2.15, samtools 1.10); docs/duckdb-miint.md's `FORMAT BAM` section carries that.
#
# DEPENDS ON assembly_coverage'S ORDER BY. Without it this stages an unsorted BAM,
# which is how a ticket died here before (11,390 of 925,483 records stepping
# backwards in tid across 20,975 contigs). test_written_bam_is_tid_monotonic fails
# on that, so it surfaces in the orchestrator's tests rather than in a job.
#
# The copy leaves a second reads-sized artifact under QIITA_OUTPUT_PATH for the life
# of the ticket (coverage_bam carries SEQ+QUAL for the whole read set). A hardlink
# does not avoid it: coverage_bam's directory and QIITA_OUTPUT_PATH are separate
# apptainer `--bind`s, and `link()` refuses to cross a mount even when both sides
# are one filesystem (measured: same device id, `ln` returns EXDEV/"Cross-device
# link", while a same-mount `ln` succeeds). A symlink does cross it, but work_files/
# sits under the declared bins_dir output, where `qiita_finish`'s
# `find -type f -exec chmod 0440` skips it and the link outlives its target.
# Dropping the copy means metaWRAP reading a path outside this step's output.
#
# The copy also carries the source's mode: coverage_bam is a native-job output at
# 0440, where the previous `samtools sort -o` created a writable file. Both
# consumers are fine with that — jgi and `samtools index` run clean on a 0440 BAM
# as a non-owner-writable file and report depth identical to a 0644 copy (measured
# on this image's pins, running as a non-root uid).
#
# Staging name then rename: `mv` within one directory is a rename, so a `cp` killed
# mid-write cannot leave a truncated file at the name metaWRAP reads. It has to be a
# name inside work_files/ rather than WORK, or the rename crosses a bind mount
# (EXDEV) and degrades to a second copy. The `.partial` name is not caught by
# metaWRAP's `work_files/*.bam` globs. `rm -f` first because `cp` of a 0440 source
# produces a 0440 file, which a second `cp` cannot open for writing: the CP hands
# each retry a fresh attempt dir, so this only fires where the same output dir is
# re-entered, e.g. a SLURM-side requeue.
#
# No `|| true`: an absent BAM surfaces two commands later as jgi's error rather
# than this one, so fail here.
STAGED_BAM="${WORK_FILES}/.${READS_STEM}.bam.partial"
rm -f "${STAGED_BAM}"
cp "${COVERAGE_BAM}" "${STAGED_BAM}"
mv "${STAGED_BAM}" "${WORK_FILES}/${READS_STEM}.bam"

# Reorder the assembly to the BAM's @SQ order before metaWRAP sees it. metaWRAP
# runs jgi_summarize_bam_contig_depths over the staged BAM, which writes the depth
# matrix in the BAM's @SQ order; metabat2 then REQUIRES the assembly FASTA to be in
# that SAME contig order and aborts otherwise ("the order of contigs in abundance
# file is not the same as the assembly file: <contig>"). noLCG.fa is in hifiasm's
# NUMERIC order (s0, s1, s2, …, s10); miint emits @SQ sorted by reference NAME, so
# the depth matrix is LEXICOGRAPHIC (s0, s1, s10, …, s2). Verified on the shipped
# samtools 1.10 / metabat2 2.15: a numeric-order assembly reproduces the abort, the
# @SQ-reordered one clears it.
#
# Independent of the BAM's record order: ordering records by (reference, position)
# is what makes the BAM coordinate sorted, and it does not move an @SQ line. Two
# things would remove this reorder — a steerable @SQ upstream (duckdb-miint#173
# delivered a *defined* order, not a steerable one), or the assembler emitting
# contigs in name order. docs/duckdb-miint.md's "Open upstream gaps" carries it.
#
# samtools faidx writes its .fai next to the FASTA and genomes_dir is a read-only
# bind, so index a WORK copy rather than noLCG in place. xargs batches the ~21k
# region names under ARG_MAX; faidx emits regions in argument order and xargs
# preserves order across batches, so the output follows @SQ order exactly. (samtools
# 1.10 has no `-r region-file`; xargs is the version-safe equivalent.)
#
# `xargs --no-run-if-empty` is load-bearing, not a nicety: without it, an empty
# order file makes xargs run `faidx` ONCE with no regions, which emits the WHOLE
# assembly in its original (numeric) order — same contig count as noLCG, so the
# count guard below would pass and metaWRAP would get the un-reordered assembly,
# the exact bug. Unreachable today (the @SQ set equals noLCG's, so an empty order
# file implies an empty noLCG, which exited above), but the flag makes the guard's
# fail-loud cover that seam instead of silently passing.
#
# Disk: two assembly-sized copies (assembly.fa + assembly.ordered.fa) land in WORK.
# The assembly is ≪ the read set, so this is small next to READS_FQ and the staged
# BAM, and WORK is a mktemp -d cleaned on EXIT.
STAGED_ORDER="${WORK}/sq_order.txt"
micromamba run -n metawrap samtools view -H "${WORK_FILES}/${READS_STEM}.bam" \
    | awk '/^@SQ/{sub(/.*SN:/,"");sub(/\t.*/,"");print}' > "${STAGED_ORDER}"
cp "${NOLCG}" "${WORK}/assembly.fa"
micromamba run -n metawrap samtools faidx "${WORK}/assembly.fa"
ORDERED_NOLCG="${WORK}/assembly.ordered.fa"
xargs --no-run-if-empty -a "${STAGED_ORDER}" \
    micromamba run -n metawrap samtools faidx "${WORK}/assembly.fa" > "${ORDERED_NOLCG}"

# Fail loud on any contig-set drift: the reorder must neither drop nor invent a
# contig. assembly_coverage maps the reads to noLCG, so the BAM's @SQ set equals
# noLCG's by construction — an inequality is a real upstream bug, not a data
# condition. Both directions fail loud, probed on the shipped samtools 1.10: faidx
# exits non-zero on a region absent from the FASTA, so xargs fails the step under
# set -e if an @SQ name is not in noLCG; and the count check below catches the
# reverse (a noLCG contig missing from @SQ is silently dropped from the order list).
# Count with awk, not `grep -c`: `grep -c` exits 1 on zero matches, and under the
# `set -euo pipefail` from _lib.sh an assignment from a failing command
# substitution aborts the script — so an empty reordered FASTA (the very
# can't-happen case this guard is for) would die with a bare exit 1 instead of the
# message below. awk always exits 0 here and prints 0 for no matches, while still
# failing loud on a genuine read error.
n_ordered=$(awk '/^>/{n++} END{print n+0}' "${ORDERED_NOLCG}")
n_nolcg=$(awk '/^>/{n++} END{print n+0}' "${NOLCG}")
if [[ "${n_ordered}" -ne "${n_nolcg}" ]]; then
    echo "binning: reordered assembly has ${n_ordered} contigs but noLCG.fa has ${n_nolcg}" >&2
    echo "         — the coverage BAM's @SQ set does not match noLCG.fa. Check assembly_coverage." >&2
    exit 65
fi

# A single binner finding nothing is non-fatal — bin_refine consolidates whatever
# bin dirs exist. Only a hard metaWRAP crash should fail the step, so we let its
# real exit code through except for the empty-result case metaWRAP signals with a
# clean run and no bins.
# -m 90 (not 100): the step's SLURM allocation is 100 GB (baseline_resources), so
# cap metaWRAP below it to leave ~10 GB headroom for its Python/aligner runtime
# (else it can OOM-kill at the cgroup boundary).
micromamba run -n metawrap metawrap binning \
    -a "${ORDERED_NOLCG}" -o "${OUT}" -t "${THREADS}" -m 90 -l 16000 \
    --single-end --metabat2 --maxbin2 --concoct --universal "${READS_FQ}"

qiita_finish bins_dir=bins
