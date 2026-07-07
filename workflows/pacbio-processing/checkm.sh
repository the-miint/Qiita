#!/bin/bash
# Step 4 (qp-pacbio step 7): CheckM quality assessment of the refined MAGs.
# Output `checkm_dir` = $QIITA_OUTPUT_PATH/checkm/checkm_quality.tsv, a normalized
# table pacbio_ingest reads (one row per MAG):
#   genome_local_id, marker_lineage, completeness, contamination,
#   strain_heterogeneity, genome_size, n_contigs
# No MAGs -> just the header (LCG-only sample); CheckM is not run on an empty dir.
#
# CheckM needs its ~1.4 GB reference data. It is bind-mounted at run time (NOT
# baked into the image) and located via CHECKM_DATA_PATH; the operator provisions
# it under PATH_DERIVED and the orchestrator binds it in. A plain bind is not
# enough — CheckM reads CHECKM_DATA_PATH (set below).
source /opt/qiita/_lib.sh

REFINED_DIR="$(qiita_input refined_bins_dir)"
OUT="${QIITA_OUTPUT_PATH}/checkm"
WORK="$(mktemp -d)"
mkdir -p "${OUT}"

_header() {
    printf 'genome_local_id\tmarker_lineage\tcompleteness\tcontamination\tstrain_heterogeneity\tgenome_size\tn_contigs\n'
}

# No MAGs to assess -> normalized table is just the header.
if ! ls "${REFINED_DIR}"/*.fa >/dev/null 2>&1; then
    _header > "${OUT}/checkm_quality.tsv"
    qiita_finish checkm_dir=checkm
    exit 0
fi

export CHECKM_DATA_PATH="${QIITA_CHECKM_DB:-/opt/checkm_data}"
LINEAGE="${WORK}/lineage.tsv"
QA="${WORK}/qa.tsv"

micromamba run -n checkm checkm lineage_wf "${REFINED_DIR}" "${WORK}/checkm_out" \
    -x fa -t "${THREADS}" --tab_table -f "${LINEAGE}" --pplacer_threads 2

# genome_size / # contigs are NOT in lineage_wf --tab_table — they come from
# `checkm qa -o 2` over the same run.
micromamba run -n checkm checkm qa "${WORK}/checkm_out/lineage.ms" "${WORK}/checkm_out" \
    -o 2 -t "${THREADS}" --tab_table -f "${QA}"

# Join the two CheckM tables by Bin Id into the normalized schema. Resolve
# columns by header name (CheckM's headers are stable but spaced/parenthesized).
# VALIDATE the qa column labels ("Genome size (bp)", "# contigs") on first build.
python3 - "${LINEAGE}" "${QA}" > "${OUT}/checkm_quality.tsv" <<'PY'
import csv, sys

def load(path):
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        cols = {c.strip().lower(): c for c in (reader.fieldnames or [])}
        return cols, list(reader)

def pick(cols, *names):
    for n in names:
        if n in cols:
            return cols[n]
    return None

lin_cols, lin_rows = load(sys.argv[1])
qa_cols, qa_rows = load(sys.argv[2])

lid = pick(lin_cols, "bin id")
lmark = pick(lin_cols, "marker lineage")
lcomp = pick(lin_cols, "completeness")
lcont = pick(lin_cols, "contamination")
lstrain = pick(lin_cols, "strain heterogeneity")

qid = pick(qa_cols, "bin id")
qsize = pick(qa_cols, "genome size (bp)", "genome size")
qn = pick(qa_cols, "# contigs", "number of contigs")
qa_by_id = {r[qid]: r for r in qa_rows} if qid else {}

print("genome_local_id\tmarker_lineage\tcompleteness\tcontamination\t"
      "strain_heterogeneity\tgenome_size\tn_contigs")
for r in lin_rows:
    bid = r.get(lid, "")
    q = qa_by_id.get(bid, {})
    print("\t".join((
        bid,
        r.get(lmark, "") if lmark else "",
        r.get(lcomp, "") if lcomp else "",
        r.get(lcont, "") if lcont else "",
        r.get(lstrain, "") if lstrain else "",
        q.get(qsize, "") if qsize else "",
        q.get(qn, "") if qn else "",
    )))
PY

qiita_finish checkm_dir=checkm
