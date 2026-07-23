#!/bin/bash
# Assert this image can actually run `metawrap binning`. Baked into the SIF and
# invoked from BOTH the def's %test (fails the build) and the spec's VERIFY_CMD
# (fails the post-build check and the idempotency skip), so "what this image must
# contain" is stated once.
#
# This exists because `micromamba env list | grep metawrap` — the previous
# verification — passes on an env containing ZERO binners. The bioconda package
# `metawrap-mg` ships metaWRAP's *scripts only*; it declares none of the tools
# those scripts invoke. The image built and verified clean, then every binning
# job died at `binning.sh: line 215: bwa: command not found`.
#
# Absolute paths, never `command -v` under `bash -lc`: a login shell resets PATH
# and reports every binary missing even when they are all installed. That false
# negative inverts the result, so don't reintroduce it.
set -u

ENV_BIN=/opt/conda/envs/metawrap/bin

# Every external tool stock metaWRAP 1.3.0's binning module invokes under the
# flags binning.sh passes (--metabat2 --maxbin2 --concoct --universal
# --single-end), derived by reading its binning.sh rather than from its docs:
#   bwa .............................. `bwa index` + `bwa mem` (self-alignment)
#   samtools ......................... sort/view around that alignment
#   metabat2, jgi_summarize_bam_contig_depths .... metabat2 package; the depth
#                                      table feeds ALL THREE binners
#   run_MaxBin.pl .................... maxbin2 >=2.2.6. NOT interchangeable with
#                                      2.2.1, whose executable is `MaxBin` —
#                                      an unpinned solve silently picks 2.2.1
#                                      and metaWRAP then fails at runtime.
#   concoct, cut_up_fasta.py, concoct_coverage_table.py,
#   merge_cutup_clustering.py ........ concoct package
# `checkm` is deliberately absent: binning.sh does not pass --run-checkm, and
# CheckM is a separate image (checkm.def) run as its own step.
REQUIRED=(
    bwa
    samtools
    metabat2
    jgi_summarize_bam_contig_depths
    run_MaxBin.pl
    concoct
    cut_up_fasta.py
    concoct_coverage_table.py
    merge_cutup_clustering.py
)

missing=()
unrunnable=()
for tool in "${REQUIRED[@]}"; do
    if [[ ! -x "${ENV_BIN}/${tool}" ]]; then
        missing+=("${tool}")
        continue
    fi
    # Presence is not enough — an unresolved shared library, or a Perl script
    # whose interpreter/deps are absent, is present-and-executable and still dies
    # in the job. That is the SAME bug class this file exists to catch, one level
    # down. So actually invoke it and reject only the loader's verdicts: 126
    # (cannot execute) and 127 (not found / missing .so). Any other exit is fine
    # — these tools disagree wildly about whether `--version` is valid, and
    # several exit non-zero while printing usage, which proves they ran.
    # Invoked through an inner `bash -c` whose OWN stderr is redirected. metabat2
    # has no --version and SIGABRTs (exit 134); the "Aborted" text is printed by
    # the shell that reaps the signalled child, not by the child, so neither
    # redirecting the command nor a plain subshell suppresses it. Letting the
    # inner shell be the reaper puts that message on a stream we control —
    # otherwise every deploy log carries a line that reads like a build failure.
    # The `; echo $?` is load-bearing twice over: it makes bash FORK instead of
    # exec-optimising the single command (so the inner shell, not ours, reaps the
    # signal), and it hands the real exit code back through stdout.
    rc=$(bash -c '"$0" --version >/dev/null 2>&1; echo $?' "${ENV_BIN}/${tool}" 2>/dev/null)
    if (( rc == 126 || rc == 127 )); then
        unrunnable+=("${tool} (exit ${rc})")
    fi
done

if (( ${#missing[@]} > 0 || ${#unrunnable[@]} > 0 )); then
    if (( ${#missing[@]} > 0 )); then
        echo "binning image is missing ${#missing[@]} required tool(s) in ${ENV_BIN}:" >&2
        printf '  - %s\n' "${missing[@]}" >&2
    fi
    if (( ${#unrunnable[@]} > 0 )); then
        echo "binning image has ${#unrunnable[@]} present-but-unrunnable tool(s):" >&2
        printf '  - %s\n' "${unrunnable[@]}" >&2
    fi
    echo "the metawrap env resolved without them; check the micromamba create line" >&2
    echo "in binning.def (metawrap-mg alone ships no binners)." >&2
    exit 1
fi

# maxbin2 2.2.1 ships `MaxBin`, not `run_MaxBin.pl`, so the presence check above
# already excludes it — this pins the version in the output for the build log.
echo "binning image: all ${#REQUIRED[@]} required tools present in ${ENV_BIN}"
echo "BINNING_IMAGE_OK"
