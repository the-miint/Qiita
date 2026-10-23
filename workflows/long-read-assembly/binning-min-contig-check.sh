#!/bin/bash
# Build-time test of this image; binning.def's %files comment on it says where it
# runs. Why it is not the spec's VERIFY_CMD is in
# test_min_contig_check_ships_runs_at_build_and_scopes_its_rebuild.
#
# What it tests: binning.sh skips metaWRAP when no contig reaches MIN_CONTIG_LEN,
# because of how this image's metaWRAP and MaxBin2 handle a contig under -l (see the
# comment above MIN_CONTIG_LEN there). The solve pins none of metawrap-mg, maxbin2 or
# FragGeneScan to one version, so a rebuild can change that. This runs metaWRAP twice
# on a synthetic one-contig assembly of exactly MIN_CONTIG_LEN bp, read from the
# shipped binning.sh:
#   -l MIN_CONTIG_LEN     MaxBin2 keeps the contig and reaches its marker-gene
#                         search, so binning.sh's `>=` passes it to metaWRAP.
#   -l MIN_CONTIG_LEN+1   MetaBAT2 runs with that as its minContig, MaxBin2 reports
#                         its FragGeneScan error, and metaWRAP stops before CONCOCT
#                         without a bin: what binning.sh skips when no contig
#                         reaches the cut.
# A single contig cannot form a bin at either cut, so the bin count alone does not
# discriminate; the MaxBin2 and MetaBAT2 lines do.
set -euo pipefail

BINNING_SH=/opt/qiita/binning.sh
min_len=$(awk -F= '/^MIN_CONTIG_LEN=/ {print $2}' "${BINNING_SH}")
if [[ ! "${min_len}" =~ ^[0-9]+$ ]]; then
    echo "binning-min-contig-check: no numeric MIN_CONTIG_LEN= line in ${BINNING_SH} (got '${min_len}')" >&2
    exit 1
fi
over=$(( min_len + 1 ))

# The marker-gene line and the MaxBin2-failure line, read from the grep -qxF / grep -qF
# arguments binning.sh matches them with, so the two cannot drift apart. The
# FragGeneScan line is MaxBin2's when its marker-gene search gets an empty input;
# binning.sh does not match it.
binning_sh_match() {
    local flags="$1" prefix="$2" found
    found=$(sed -n "s/.*grep ${flags} '\(${prefix}[^']*\)'.*/\1/p" "${BINNING_SH}")
    if [[ -z "${found}" || "${found}" == *$'\n'* ]]; then
        echo "binning-min-contig-check: expected one \`grep ${flags} '${prefix}...'\` in ${BINNING_SH}, got: '${found}'" >&2
        exit 1
    fi
    printf '%s' "${found}"
}
VERDICT=$(binning_sh_match -qxF 'Marker gene search reveals')
MAXBIN2_FAILED=$(binning_sh_match -qF 'Something went wrong with running MaxBin2')
FGS_ERROR='Error running FragGeneScan.'

work=$(mktemp -d)
trap 'rm -rf "${work}"' EXIT

# The fixture: a random contig of min_len bp, and single-end 150 bp reads starting
# every 40 bp along it, so every binner has coverage to work with.
python3 - "${work}" "${min_len}" <<'EOF'
import random
import sys

work, length = sys.argv[1], int(sys.argv[2])
rng = random.Random(0)
seq = "".join(rng.choice("ACGT") for _ in range(length))
with open(f"{work}/assembly.fa", "w") as f:
    f.write(f">c0\n{seq}\n")
with open(f"{work}/reads.fastq", "w") as f:
    for i, p in enumerate(range(0, length - 150, 40)):
        f.write(f"@r{i}\n{seq[p:p + 150]}\n+\n{'I' * 150}\n")
EOF

# metaWRAP exits non-zero in both runs (MaxBin2 fails either way), so its status is
# not what is asserted; its stdout and output dir are.
run_metawrap() {
    local l="$1"
    micromamba run -n metawrap metawrap binning \
        -a "${work}/assembly.fa" -o "${work}/l${l}" -t 1 -m 1 -l "${l}" \
        --single-end --metabat2 --maxbin2 --concoct --universal "${work}/reads.fastq" \
        > "${work}/l${l}.stdout" 2>&1 || true
}

failures=()
check() {
    local l="$1" what="$2"
    shift 2
    "$@" || failures+=("at -l ${l}: ${what}")
}
has_line() { grep -qxF "$2" "${work}/l$1.stdout"; }
has_text() { grep -qF "$2" "${work}/l$1.stdout"; }
lacks_line() { ! has_line "$@"; }
# A real bin is bin.<N>.fa; contig2bin_filter.awk says why the other names are catch-alls.
no_bin() { ! find "${work}/l$1" -path '*_bins/*' -type f | grep -qE '/bin\.[0-9]+\.fa$'; }

run_metawrap "${min_len}"
check "${min_len}" "MaxBin2 did not reach its marker-gene search on a ${min_len} bp contig" \
    has_line "${min_len}" "${VERDICT}"

run_metawrap "${over}"
check "${over}" "MetaBAT2's log does not name ${over} as its minContig" \
    has_text "${over}" "using minContig ${over},"
check "${over}" "MaxBin2 reported no FragGeneScan error" \
    has_text "${over}" "${FGS_ERROR}"
check "${over}" "metaWRAP did not report MaxBin2 failing" \
    has_text "${over}" "${MAXBIN2_FAILED}"
check "${over}" "MaxBin2 reached its marker-gene search, so it kept a contig under -l" \
    lacks_line "${over}" "${VERDICT}"
check "${over}" "CONCOCT ran" \
    test ! -e "${work}/l${over}/concoct_bins"
check "${over}" "a binner formed a bin" no_bin "${over}"

if (( ${#failures[@]} > 0 )); then
    echo "binning-min-contig-check: this image's metaWRAP no longer behaves as binning.sh's" >&2
    echo "minimum-contig-length check assumes (its comment states the assumption):" >&2
    printf '  - %s\n' "${failures[@]}" >&2
    for l in "${min_len}" "${over}"; do
        echo "--- metaWRAP stdout at -l ${l} (last 40 lines):" >&2
        tail -40 "${work}/l${l}.stdout" >&2
    done
    exit 1
fi

echo "binning-min-contig-check: a ${min_len} bp contig reaches MaxBin2's marker-gene search at -l ${min_len}; at -l ${over} MaxBin2 fails before CONCOCT and nothing is binned"
