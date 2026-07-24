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
# so pre-placing the BAM skips the sort as well as the alignment, silently. Hence
# the `samtools sort` below: whatever we stage must already be coordinate sorted,
# and this is the only place left that can guarantee it.
#
# So the native `assembly_coverage` step pre-maps with miint's embedded minimap2
# (`map-hifi`) and this entrypoint sorts that BAM into work_files/ under the name
# metaWRAP will look for. bwa is still INSTALLED and still runs: `bwa index` is
# unconditional (guarded only by assembly.fa.bwt) and produces an index nothing
# then uses — that is also why qp-pacbio's environment carries bwa.
#
# The BAM's @SQ names must match the contigs metaWRAP indexes, which they do
# because both sides are noLCG.fa. samtools is required regardless of this path:
# metaWRAP's concoct block runs `samtools index` over work_files/*.bam.
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
# `bwa mem` (and, per the header, its `samtools sort` — which is why we sort here).
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
# Coordinate-sort the pre-mapped BAM into the name metaWRAP will look for — the
# `samtools sort` metaWRAP skips along with its `bwa mem` (see the header).
#
# DO NOT "optimise" this back into a copy or a hardlink of coverage_bam. The
# durable rule: a BAM is coordinate sorted by TID (the @SQ index), and the @SQ
# order miint's `FORMAT BAM` writer emits is not derivable from the
# REFERENCE_LENGTHS table it is built from (see docs/duckdb-miint.md), so no
# ordering assembly_coverage can apply makes its output sorted. Measured on the
# production BAM that exposed this: 11,390 of 925,483 records step backwards in
# tid across 20,975 contigs, and jgi_summarize_bam_contig_depths rejects the file
# outright. After this sort, zero.
#
# SIZING. `-m` is PER THREAD and `-@` is ADDITIONAL threads, so samtools' ceiling
# is about (-@ + 1) * -m. Deriving that from the thread count alone is what makes
# it unbounded, so do the opposite: fix the TOTAL at a third of the step's own
# allocation and divide it out. The result is bounded by MEM_MB no matter what
# THREADS resolves to (33 GB against this step's 100 GB at 1, 16 or 128 threads).
# Threads come DOWN before per-thread memory goes below 256 MB, so the floor can
# never push the total past the budget either.
#
# Measured inside this image on the 2.0 GB production BAM at 16 cpu: peak RSS
# 11.1 GiB, 19 s wall, unchanged between a 12.75 GiB and a 34 GiB budget — past
# the budget samtools spills to `-T` rather than growing, so a smaller total costs
# nothing here and cannot OOM the step's cgroup (which is set at exactly --mem).
SORT_TOTAL_MB=$(( MEM_MB / 3 ))
SORT_THREADS=$(( SORT_TOTAL_MB / 256 - 1 ))
if (( SORT_THREADS > THREADS )); then SORT_THREADS="${THREADS}"; fi
if (( SORT_THREADS < 1 )); then SORT_THREADS=1; fi
SORT_MEM_MB=$(( SORT_TOTAL_MB / (SORT_THREADS + 1) ))
if (( SORT_MEM_MB < 256 )); then SORT_MEM_MB=256; fi

# Sort to a staging name and rename. `mv` within one directory is a rename, so a
# sort killed mid-write can never leave a truncated file at the name metaWRAP
# reads. It has to be a name inside work_files/ rather than WORK: payload.py binds
# the workspace and QIITA_OUTPUT_PATH as separate mounts, so a rename across them
# is EXDEV and degrades to copying a reads-sized BAM (coverage_bam carries SEQ+QUAL
# for the whole read set). The `.partial` name is not caught by metaWRAP's
# `work_files/*.bam` globs. `-T` still lands the spill shards in WORK — a mktemp -d
# with an EXIT trap — so a failure leaves nothing under QIITA_OUTPUT_PATH for
# qiita_finish to sweep into the manifest's neighbourhood.
#
# DISK COST, unavoidable now: the sorted BAM is a SECOND reads-sized artifact
# under QIITA_OUTPUT_PATH for the life of the ticket (coverage_bam carries SEQ+QUAL
# for the whole read set, so it is roughly FASTQ-sized — 2.0 GB on the ticket
# measured above). The previous `ln`-then-`cp` staging tried to avoid that with a
# hardlink; it cannot survive here, because a sort has to produce a new file, and
# in production the `ln` never fired anyway (input and output are separate binds,
# so `link()` returns EXDEV and only the shared-mount local backend hardlinked).
#
# No `|| true`: an unsorted or absent BAM surfaces two commands later as jgi's
# error rather than this one, so fail here, loudly.
STAGED_BAM="${WORK_FILES}/.${READS_STEM}.bam.partial"
micromamba run -n metawrap samtools sort \
    -@ "${SORT_THREADS}" -m "${SORT_MEM_MB}M" -T "${WORK}/samtools-sort" \
    -o "${STAGED_BAM}" "${COVERAGE_BAM}"
mv "${STAGED_BAM}" "${WORK_FILES}/${READS_STEM}.bam"

# A single binner finding nothing is non-fatal — bin_refine consolidates whatever
# bin dirs exist. Only a hard metaWRAP crash should fail the step, so we let its
# real exit code through except for the empty-result case metaWRAP signals with a
# clean run and no bins.
# -m 90 (not 100): the step's SLURM allocation is 100 GB (baseline_resources), so
# cap metaWRAP below it to leave ~10 GB headroom for its Python/aligner runtime
# (else it can OOM-kill at the cgroup boundary).
micromamba run -n metawrap metawrap binning \
    -a "${NOLCG}" -o "${OUT}" -t "${THREADS}" -m 90 -l 16000 \
    --single-end --metabat2 --maxbin2 --concoct --universal "${READS_FQ}"

qiita_finish bins_dir=bins
