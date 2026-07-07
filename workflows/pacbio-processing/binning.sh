#!/bin/bash
# Step 2 (qp-pacbio step 4): metaWRAP binning of the noLCG contigs with three
# binners (metabat2 + maxbin2 + concoct). Output `bins_dir` =
# $QIITA_OUTPUT_PATH/bins/{metabat2_bins,maxbin2_bins,concoct_bins}/ (whichever
# binners produced anything). No contigs, or no bins at all, leaves an empty
# bins_dir — bin_refine handles that.
#
# NOTE: metaWRAP aligns reads with bwa (a short-read aligner) internally to
# compute coverage; on HiFi reads that is a known suboptimality inherited from
# qp-pacbio (a minimap2-hifi depth path is a future improvement). qp-pacbio's
# separate minimap2 pre-map produced a BAM metaWRAP never consumed, so it is
# dropped here.
source /opt/qiita/_lib.sh

GENOMES_DIR="$(qiita_input genomes_dir)"
READS_FASTQ="$(qiita_input reads_fastq)"
NOLCG="${GENOMES_DIR}/noLCG.fa"
OUT="${QIITA_OUTPUT_PATH}/bins"
mkdir -p "${OUT}"

# Nothing to bin (all-circular or empty assembly) -> empty bins_dir, exit 0.
if [[ ! -s "${NOLCG}" ]]; then
    qiita_finish bins_dir=bins
    exit 0
fi

# metaWRAP's --single-end wants a plain (uncompressed) .fastq path.
READS_FQ="$(mktemp -d)/reads.fastq"
mkdir -p "$(dirname "${READS_FQ}")"
if [[ "${READS_FASTQ}" == *.gz ]]; then
    pigz -dc "${READS_FASTQ}" > "${READS_FQ}"
else
    cp "${READS_FASTQ}" "${READS_FQ}"
fi

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
