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

# concoct's `vbgmm` C-extension links libgfortran.so.3. If the env ships only
# libgfortran.so.5 the import fails at RUNTIME with an ImportError — exit 1, NOT a
# loader verdict (126/127) — so the runnability loop above PASSES it and concoct
# still dies inside metaWRAP, failing the whole step (metabat2 + maxbin2 succeed).
# That is exactly how a broken concoct shipped once. Assert the import directly;
# binning.def installs libgfortran=3.0.0 (provides .so.3, coexists with
# libgfortran5) to satisfy it.
if ! "${ENV_BIN}/python" -c 'import vbgmm' >/dev/null 2>&1; then
    echo "concoct's vbgmm fails to import — the metawrap env is missing" >&2
    echo "libgfortran.so.3. binning.def must 'micromamba install libgfortran=3.0.0'" >&2
    echo "(it provides .so.3 and coexists with libgfortran5). Without it metaWRAP's" >&2
    echo "concoct binner dies at runtime and fails the whole binning step." >&2
    exit 1
fi

# Presence is not enough for the two tools whose BEHAVIOUR the workflow depends
# on. Both are pinned in binning.def; assert the pins actually took, because the
# solver is otherwise the only thing enforcing them and a drifted solve would
# ship a green image. This is the one place that can observe the built result —
# a test in the repo can only read the spec string, which is already visible in
# a diff.
#   samtools .... binning.sh reads the staged BAM's @SQ order with
#                 `samtools view -H` (parsed by awk on `SN:`) and reorders the
#                 assembly with `samtools faidx`; metaWRAP's concoct block also
#                 runs `samtools index` over the staged BAM.
#   metabat2 .... owns jgi_summarize_bam_contig_depths, whose "is not sorted!"
#                 rejection is the acceptance criterion the coordinate-sorted
#                 coverage BAM is written to meet.
# Version is read from the tool, not the package metadata, so a hand-modified
# env cannot satisfy it. The two disagree about HOW to ask, and guessing gets it
# wrong: samtools takes --version, while jgi REJECTS it ("unrecognized option")
# and prints its version on the first line of its no-argument usage banner, on
# stderr. Hence the per-tool argument — an empty value means "invoke bare".
declare -A PINNED=(
    [samtools]=1.10
    [jgi_summarize_bam_contig_depths]=2.15
)
declare -A VERSION_ARG=(
    [samtools]="--version"
    [jgi_summarize_bam_contig_depths]=""
)
drifted=()
for tool in "${!PINNED[@]}"; do
    want="${PINNED[$tool]}"
    arg="${VERSION_ARG[$tool]}"
    if [[ -n "${arg}" ]]; then
        got=$("${ENV_BIN}/${tool}" "${arg}" 2>&1 | head -1)
    else
        got=$("${ENV_BIN}/${tool}" 2>&1 | head -1)
    fi
    # Match the version as a whole token so 1.10 does not accept 1.100.
    if ! grep -qE "(^|[^0-9.])${want}([^0-9.]|$)" <<<"${got}"; then
        drifted+=("${tool}: want ${want}, got '${got}'")
    fi
done
if (( ${#drifted[@]} > 0 )); then
    echo "binning image resolved ${#drifted[@]} pinned tool(s) at the wrong version:" >&2
    printf '  - %s\n' "${drifted[@]}" >&2
    echo "binning.def pins these; the solve moved anyway. Reconcile the two before" >&2
    echo "shipping — the assembly reorder and jgi's acceptance criteria are" >&2
    echo "version-bound." >&2
    exit 1
fi

# maxbin2 2.2.1 ships `MaxBin`, not `run_MaxBin.pl`, so the presence check above
# already excludes it — this pins the version in the output for the build log.
echo "binning image: all ${#REQUIRED[@]} required tools present in ${ENV_BIN}"
for tool in "${!PINNED[@]}"; do
    echo "binning image: ${tool} at pinned ${PINNED[$tool]}"
done
echo "BINNING_IMAGE_OK"
