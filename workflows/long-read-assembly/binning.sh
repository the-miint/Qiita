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
# THE GUARD WRAPS MORE THAN THE ALIGNMENT — this is the part that is easy to miss
# and that cost a production ticket. In metaWRAP's binning.sh the `bwa mem` and the
# `samtools sort` that follows it live INSIDE THE SAME
# `if [[ ! -f ${out}/work_files/${sample}.bam ]]` block (verified against
# /opt/conda/envs/metawrap/bin/metawrap-modules/binning.sh in the deployed image:
# `bwa mem` and `samtools sort` are both under that one test). Pre-placing the BAM
# therefore skips the SORT as well as the alignment — silently. metaWRAP never
# sorts what we hand it, and `jgi_summarize_bam_contig_depths` rejects an unsorted
# BAM outright ("ERROR: the bam file ... is not sorted!"), failing the step. So
# whatever we stage must ALREADY be coordinate sorted, and this entrypoint is the
# only place that can guarantee it — hence the `samtools sort` below, which is
# literally the command metaWRAP would have run.
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
# Coordinate-sort the pre-mapped BAM into the name metaWRAP will look for. This
# IS the `samtools sort` metaWRAP skipped along with its `bwa mem` (see the
# header) — same tool, same env, run at the one point in the pipeline that can
# still put it back.
#
# Sorting on tid/position rather than trusting the writer's record order is also
# the only robust option. assembly_coverage writes its records `ORDER BY reference
# ASC, position ASC`, but a BAM is coordinate sorted by TID — the @SQ index — and
# the @SQ order that miint's `FORMAT BAM` writer emits is NOT derivable from the
# REFERENCE_LENGTHS table's row order (established by probe 2026-07-24; see
# docs/duckdb-miint.md). Name order and tid order therefore diverge on a real
# multi-thousand-contig assembly, which is what jgi rejected in production. Sorting
# here makes the staged BAM correct regardless of what the writer emits.
#
# `-m` is PER THREAD, so the sort's ceiling is roughly (THREADS + 1) * -m: 2G at
# this step's 16 cpu is ~34 GB against a 100 GB allocation. It does not stack with
# metaWRAP's own `-m 90` because the sort finishes before metawrap is invoked.
# Reference point: the 2.0 GB BAM from the production ticket that exposed this
# sorted in 13.5 s wall at `-@ 8 -m 2G`.
#
# `-T` lands the spill prefix in WORK (a mktemp -d with an EXIT trap), not next to
# the output — a failed sort must not leave `.tmp.NNNN.bam` shards inside
# QIITA_OUTPUT_PATH, where qiita_finish would chmod them into the manifest's
# neighbourhood.
#
# No `|| true`: an unsorted or absent BAM makes metaWRAP fail two commands later
# with jgi's error rather than this one, so fail here, loudly.
micromamba run -n metawrap samtools sort \
    -@ "${THREADS}" -m 2G -T "${WORK}/samtools-sort" \
    -o "${WORK_FILES}/${READS_STEM}.bam" "${COVERAGE_BAM}"

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
