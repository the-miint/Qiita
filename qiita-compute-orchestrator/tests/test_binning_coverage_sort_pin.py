"""Pin that `binning.sh` sorts the coverage BAM before staging it for metaWRAP.

metaWRAP guards its own `bwa mem` AND its own `samtools sort` behind one
`if [[ ! -f <out>/work_files/<sample>.bam ]]`. The workflow pre-places that BAM
to skip bwa (a short-read aligner) in favour of a minimap2 pre-map -- which
silently skips the sort along with it. miint's `COPY ... FORMAT BAM` orders
records by reference NAME, and its `@SQ` order (which assigns tid) is not that
order, so the file is not coordinate-sorted and
`jgi_summarize_bam_contig_depths` refuses it outright:

    ERROR: the bam file 'reads.bam' is not sorted!

That reached production and failed a real ticket at the binning step. The fix is
the `samtools sort` in `binning.sh` that restores what metaWRAP would have done.

Why this test exists, and why it reads text
-------------------------------------------
The behavioural test (`test_samtools_sort_makes_the_bam_tid_monotonic` in
`jobs/test_assembly_coverage.py`) needs the samtools binary and is SKIPPED
everywhere it is absent -- CI and a stock dev box included. It also invokes
samtools directly, so it pins *samtools'* behaviour rather than ours: deleting
the sort from `binning.sh` would not fail it.

This one needs no binary, runs everywhere, and pins the part that is actually
ours -- that the entrypoint still sorts, still stages atomically, and has not
regressed to the copy-the-writer's-output shape that caused the incident. It is
a text assertion because the alternative is building a 517 MB metaWRAP image;
it therefore cannot prove the command *works*, only that it is still there and
still shaped correctly. Correct-operation evidence lives in the skipped test
above and in the deploy-checklist verify step.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOW_DIR = _REPO_ROOT / "workflows" / "long-read-assembly"
_BINNING_SH = _WORKFLOW_DIR / "binning.sh"
_BINNING_DEF = _WORKFLOW_DIR / "binning.def"


def _code_lines(path: Path) -> list[str]:
    """Script lines with comments and blanks dropped, `\\` continuations folded.

    Every assertion below runs against these, never the raw text. The
    deploy-checklist verify step for this same change originally grepped the raw
    file for `samtools sort` and passed on a file where the only occurrences were
    the comments describing it -- deleting the command still greened the check.
    A test that greps raw text would have exactly that hole.

    Continuations are folded so one logical command is one entry, the way the
    shell reads it. Without that, a multi-line invocation is several fragments
    and any assertion spanning a command and its arguments silently cannot match
    -- passing only for tests phrased as absences.
    """
    out: list[str] = []
    pending = ""
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.endswith("\\"):
            pending += line[:-1].rstrip() + " "
            continue
        out.append(pending + line)
        pending = ""
    if pending:
        out.append(pending.rstrip())
    return out


def test_binning_sorts_the_coverage_bam() -> None:
    """The staged BAM is produced by `samtools sort`, not a copy of the input."""
    code = "\n".join(_code_lines(_BINNING_SH))
    assert "samtools sort" in code, (
        "binning.sh no longer runs `samtools sort`. metaWRAP's own sort is skipped "
        "along with its bwa when work_files/<sample>.bam is pre-placed, so without "
        "this the staged BAM is in reference-NAME order and jgi rejects it with "
        "'the bam file is not sorted!'"
    )
    assert re.search(r"samtools\s+sort\b[^\n]*\$\{COVERAGE_BAM\}", code), (
        "`samtools sort` no longer reads ${COVERAGE_BAM}. The sort must consume "
        "the coverage step's output directly; sorting anything else would stage "
        "an unsorted BAM under a sorted-looking name."
    )


def test_binning_does_not_stage_the_unsorted_bam_directly() -> None:
    """The pre-fix shape -- `ln`/`cp` of COVERAGE_BAM into work_files/ -- is gone.

    This is the regression that caused the incident, so it is asserted as an
    absence rather than inferred from the presence of the sort: a script could
    contain both and still stage the unsorted file.
    """
    for line in _code_lines(_BINNING_SH):
        if not re.match(r"^(ln|cp)\b", line):
            continue
        assert "COVERAGE_BAM" not in line, (
            f"binning.sh stages the coverage BAM with a plain copy/link: {line!r}. "
            "That is the pre-fix shape -- it puts miint's name-ordered records at "
            "the path metaWRAP reads, which jgi rejects. Sort into place instead."
        )


def test_binning_stages_the_sorted_bam_atomically() -> None:
    """Sort to a scratch name, then rename, so a killed sort cannot leave a
    truncated file at the path metaWRAP reads.

    The rename must stay inside work_files/: the workspace and QIITA_OUTPUT_PATH
    are separate bind mounts, so a cross-directory `mv` is EXDEV and silently
    degrades to copying a reads-sized BAM.
    """
    code = _code_lines(_BINNING_SH)
    joined = "\n".join(code)
    assert "STAGED_BAM=" in joined, "binning.sh no longer sorts to a staging name"
    staged = next(line for line in code if line.startswith("STAGED_BAM="))
    assert "WORK_FILES" in staged, (
        f"the staging name left work_files/: {staged!r}. A rename out of "
        "QIITA_OUTPUT_PATH crosses a bind mount (EXDEV) and stops being atomic."
    )
    assert re.search(r"^mv\s+\"\$\{STAGED_BAM\}\"", joined, re.MULTILINE), (
        "the sorted BAM is no longer renamed into place with `mv`"
    )


def test_binning_sort_memory_is_bounded_by_the_allocation() -> None:
    """The sort's total memory derives from MEM_MB, not from a bare literal.

    `samtools sort -m` is PER THREAD, so pairing it with an unbounded thread
    count makes the ceiling unbounded too. Inside a container `--containall`
    scrubs every SLURM_* var, so the thread count falls through to `nproc` --
    the node's core count, not the allocation, unless the site happens to
    cpuset-bind. Sizing off MEM_MB is what keeps the product under the cgroup
    limit regardless.
    """
    code = "\n".join(_code_lines(_BINNING_SH))
    assert re.search(r"SORT_TOTAL_MB=.*MEM_MB", code), (
        "the sort's memory budget no longer derives from ${MEM_MB}. `-m` is "
        "per-thread; without a total derived from the step's own allocation the "
        "ceiling scales with the host's core count and can OOM the cgroup."
    )
    assert re.search(r"-m\s+\"\$\{SORT_MEM_MB\}M\"", code), (
        "`samtools sort -m` no longer uses the derived per-thread value"
    )


@pytest.mark.parametrize("package", ["samtools", "maxbin2"])
def test_binning_image_pins_version_bound_tools(package: str) -> None:
    """`samtools` and `maxbin2` carry version pins in the image def.

    Neither is cosmetic. maxbin2 <2.2.6 ships `MaxBin` rather than the
    `run_MaxBin.pl` metaWRAP invokes -- the image passes every build check and
    then fails inside the job. samtools arrived only as a transitive dependency
    of metawrap-mg's solve, so any later re-solve could move it; `binning.sh`
    now sizes its sort against a peak RSS measured on one specific build, and
    the sort semantics are version-bound.
    """
    create = next(
        (line for line in _code_lines(_BINNING_DEF) if "micromamba create" in line),
        None,
    )
    assert create is not None, "binning.def no longer creates the metawrap env"
    # The create spans a line continuation; the package list is on the next line.
    body = "\n".join(_code_lines(_BINNING_DEF))
    spec = re.search(rf"{package}(=|>=)[0-9][^\s\"']*", body)
    assert spec is not None, (
        f"{package} is unpinned in binning.def. It is installed from bioconda, so "
        f"an unpinned spec is resolved fresh on every rebuild and can move without "
        f"any change to this repo."
    )
