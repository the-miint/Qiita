#!/bin/bash
# CheckM quality assessment of the refined MAGs and of the circular genomes (LCGs).
# Output `checkm_dir` = $QIITA_OUTPUT_PATH/checkm holding CheckM's RAW --tab_table
# output verbatim (the container does NO column normalization — one CSV framework,
# DuckDB, owns all parsing in assembly_load):
#   lineage.tsv       `checkm lineage_wf --tab_table` over the refined bins — Bin Id,
#                     Marker lineage, Completeness, Contamination, Strain heterogeneity, ...
#   qa.tsv            `checkm qa -o 2 --tab_table` over the same — Bin Id, Genome
#                     size (bp), # contigs, ... (the extended stats not in lineage_wf)
#   lcg_lineage.tsv   the same two tables over the circular genomes, one CheckM run
#   lcg_qa.tsv        per class rather than one over both
# assembly_load reads all four with DuckDB read_csv and joins each pair by "Bin Id".
# Either class may be absent (its two files simply are not written); with neither,
# checkm_dir is empty and assembly_load writes bin_quality empty. CheckM is not run
# on an empty dir.
#
# WHY TWO RUNS AND NOT ONE MERGED DIRECTORY
# A quality row's `kind` is what tells a MAG from an LCG downstream, and CheckM
# reports only a "Bin Id" — the filename stem. Merged, `kind` would have to be
# recovered by prefixing the stems and parsing the prefix back off, and the prefix
# would then have to be stripped before the row could join
# assembly_membership.bin_id. Scored per class, the file a row came from IS its kind
# and every stem reaches assembly_load unmodified. It also keeps the two namespaces
# apart: a refined-bin stem and a contig id are minted by different tools and
# nothing makes them disjoint.
#
# CheckM needs its ~1.4 GB reference data. It is bind-mounted at run time (NOT
# baked into the image) and located via CHECKM_DATA_PATH; the operator provisions
# it under PATH_DERIVED and the orchestrator binds it in. A plain bind is not
# enough — CheckM reads CHECKM_DATA_PATH (set below). Its ABSENCE is an operator
# config error, not a data condition: with genomes present but no DB to score them,
# checkm.sh FAILS LOUD rather than silently emitting an empty checkm_dir (see the
# DB check below). The genuinely-nothing-to-score case above is a separate benign
# success.
source /opt/qiita/_lib.sh

REFINED_DIR="$(qiita_input refined_bins_dir)"
GENOMES_DIR="$(qiita_input genomes_dir)"
CIRCULAR="${GENOMES_DIR}/circular.fa"
OUT="${QIITA_OUTPUT_PATH}/checkm"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "${OUT}"

HAVE_MAG=0
ls "${REFINED_DIR}"/*.fa >/dev/null 2>&1 && HAVE_MAG=1
# -s, not -f: assemble.sh writes circular.fa unconditionally and a zero-byte file is
# a legitimate no-circular-contig assembly, which is nothing to score rather than a
# missing input. It is also what lcg_split.py refuses, since read_fastx raises on a
# zero-record input.
HAVE_LCG=0
[[ -s "${CIRCULAR}" ]] && HAVE_LCG=1

# Nothing to assess -> empty checkm_dir (assembly_load writes bin_quality empty).
if [[ "${HAVE_MAG}" -eq 0 && "${HAVE_LCG}" -eq 0 ]]; then
    qiita_finish checkm_dir=checkm
    exit 0
fi

export CHECKM_DATA_PATH="${QIITA_CHECKM_DB:-/opt/checkm_data}"

# The CheckM reference DB is bind-mounted at run time (deploy checklist bucket 2),
# NOT baked into the SIF, and located via CHECKM_DATA_PATH (`checkm data setRoot`
# reads the same var). Reaching here means there ARE genomes to assess, so an
# absent/empty DB is an OPERATOR CONFIG ERROR, not a data condition — silently
# emitting an empty checkm_dir would let the ticket COMPLETE with quality
# permanently uncaptured. FAIL LOUD instead (mirrors bin_refine.sh's DAS_Tool
# fail-loud): the operator must stage the DB + bind it in before the workflow can
# run. This is distinct from the genuinely-nothing-to-score benign empty success
# above.
if [[ ! -d "${CHECKM_DATA_PATH}" || -z "$(ls -A "${CHECKM_DATA_PATH}" 2>/dev/null)" ]]; then
    echo "ERROR: CheckM reference data not found at CHECKM_DATA_PATH=${CHECKM_DATA_PATH}." >&2
    echo "       This is a deploy/config error: stage CheckM's ~1.4 GB reference DB" >&2
    echo "       under PATH_DERIVED and bind it in (or set QIITA_CHECKM_DB to its" >&2
    echo "       in-container path). Refusing to report genome quality as empty when" >&2
    echo "       genomes are present — failing loud." >&2
    exit 78
fi

# CheckM's markerGeneFinder runs `multiprocessing.Manager()`, which binds an
# AF_UNIX socket at $TMPDIR/pymp-XXXXXXXX/listener-XXXXXXXX. The SLURM payload sets
# TMPDIR=<workspace>/tmp — a ~85-char path on real disk (so temp doesn't land on
# the tiny --containall /tmp tmpfs) — and Python's ~32-char suffix overflows the
# ~108-char AF_UNIX sun_path limit: `OSError: AF_UNIX path too long`, which crashes
# lineage_wf on EVERY run. Point TMPDIR at a SHORT symlink into the same real temp
# (WORK): the socket path stays short, its files still land on disk. Kept for the
# rest of the step — the qa calls, the split and qiita_finish tolerate it. Symlink
# lives in the container's tmpfs /tmp (a link is bytes) and is cleaned with WORK on
# exit.
CK_TMP_LINK="/tmp/ck.$$"
ln -sfn "${WORK}" "${CK_TMP_LINK}"
trap 'rm -rf "$WORK"; rm -f "$CK_TMP_LINK"' EXIT
export TMPDIR="${CK_TMP_LINK}"

# Emit CheckM's RAW --tab_table output straight into checkm_dir. lineage_wf carries
# marker lineage + completeness/contamination/strain heterogeneity; qa -o 2 adds
# genome size / # contigs. assembly_load joins each pair by "Bin Id" in DuckDB.
run_checkm() {
    local genomes="$1" ck="${WORK}/$2" lineage_out="$3" qa_out="$4"
    micromamba run -n checkm checkm lineage_wf "${genomes}" "${ck}" \
        -x fa -t "${THREADS}" --tab_table -f "${lineage_out}" --pplacer_threads 2
    micromamba run -n checkm checkm qa "${ck}/lineage.ms" "${ck}" \
        -o 2 -t "${THREADS}" --tab_table -f "${qa_out}"
}

if [[ "${HAVE_MAG}" -eq 1 ]]; then
    run_checkm "${REFINED_DIR}" mag_out "${OUT}/lineage.tsv" "${OUT}/qa.tsv"
fi

if [[ "${HAVE_LCG}" -eq 1 ]]; then
    # One FASTA per circular contig, named for the contig id, because lineage_wf
    # scores a directory of files and would otherwise read the whole multi-FASTA as
    # one genome. lcg_split.py states why the stem is the id verbatim.
    python3 /opt/qiita/lcg_split.py "${CIRCULAR}" "${WORK}/lcg"
    run_checkm "${WORK}/lcg" lcg_out "${OUT}/lcg_lineage.tsv" "${OUT}/lcg_qa.tsv"
fi

qiita_finish checkm_dir=checkm
