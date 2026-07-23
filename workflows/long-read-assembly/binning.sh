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
# So the native `assembly_coverage` step pre-maps with miint's embedded minimap2
# (`map-hifi`) and this entrypoint stages that BAM into work_files/ under the name
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
# `bwa mem`. The name is NOT free: metaWRAP derives `sample` from the READS
# filename (`tmp=${reads##*/}; sample=${tmp%.*}`) and then looks for
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
# Stage the pre-mapped BAM under the name metaWRAP will look for. It carries
# SEQ+QUAL for the whole read set (the SEQUENCE_DATA arg in assembly_coverage —
# required for correct depth), so it is roughly FASTQ-sized: a copy leaves a second
# reads-sized artifact under QIITA_OUTPUT_PATH per ticket.
#
# Try `ln` to avoid that, but expect `cp` to be the normal path in production: the
# SLURM backend bind-mounts the input (COVERAGE_BAM) and QIITA_OUTPUT_PATH as
# SEPARATE mounts, so `link()` returns EXDEV even on one physical filesystem, and
# only the local backend (shared mount) actually hardlinks. Two consequences of a
# successful `ln`, both benign: the file shares an inode with the coverage step's
# own output, so qiita_finish's `chmod 0440` sweep flips that source too (nothing
# rereads coverage_bam after this), and there is no second copy to size for.
ln "${COVERAGE_BAM}" "${WORK_FILES}/${READS_STEM}.bam" 2>/dev/null \
    || cp "${COVERAGE_BAM}" "${WORK_FILES}/${READS_STEM}.bam"

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
