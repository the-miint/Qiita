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

The sort is defensive FOREVER, not pending an upstream fix. Nothing here pins
that miint still emits a non-monotonic `@SQ` order, so if miint ever gives that
order a definition, the sort silently becomes pure cost (~19 s and a second
reads-sized artifact per ticket) and no test would say so. Removing it would
need a fresh probe of miint's writer, not an inference from its changelog.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOW_DIR = _REPO_ROOT / "workflows" / "long-read-assembly"
_BINNING_SH = _WORKFLOW_DIR / "binning.sh"
_BINNING_DEF = _WORKFLOW_DIR / "binning.def"
_BINNING_VERIFY = _WORKFLOW_DIR / "binning-verify.sh"


@pytest.mark.parametrize("path", [_BINNING_SH, _BINNING_DEF, _BINNING_VERIFY])
def test_source_file_is_present_and_parses(path: Path) -> None:
    """Anti-vacuity guard, matching the sibling static pins.

    Every assertion below reads one of these files through `_code_lines`. A
    moved file or a `_REPO_ROOT` that stopped resolving would make the
    absence-shaped pins pass for the wrong reason, so fail loudly here first.
    """
    assert path.is_file(), f"{path} is missing -- the pins below would be vacuous"
    assert _code_lines(path), f"{path} has no non-comment lines"


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

    TRAILING comments are stripped too, not just whole-line ones: `cmd # samtools
    sort ${COVERAGE_BAM}` would otherwise satisfy the assertions below, which is
    the same hole in a different place. `#` is only a comment when it starts a
    word and is not inside quotes, so this tracks quote state rather than
    splitting on the first `#`. Neither script here contains a quoted `#` today;
    the parser handles it so that a future one cannot quietly break the pin.
    """
    out: list[str] = []
    pending = ""
    for raw in path.read_text().splitlines():
        line = _strip_comment(raw).strip()
        if not line:
            continue
        if line.endswith("\\"):
            pending += line[:-1].rstrip() + " "
            continue
        out.append(pending + line)
        pending = ""
    if pending:
        out.append(pending.rstrip())
    return out


def _strip_comment(line: str) -> str:
    """Drop a shell comment, honouring quotes.

    A `#` opens a comment only at the start of a word (preceded by whitespace or
    line start) and only outside quotes -- `echo "a#b"` and `x=a#b` are not
    comments.
    """
    quote: str | None = None
    for i, ch in enumerate(line):
        if quote is not None:
            if ch == quote:
                quote = None
            continue
        if ch in ("'", '"'):
            quote = ch
        elif ch == "#" and (i == 0 or line[i - 1].isspace()):
            return line[:i]
    return line


def test_binning_sorts_the_coverage_bam() -> None:
    """The staged BAM is produced by `samtools sort`, not a copy of the input."""
    code = "\n".join(_code_lines(_BINNING_SH))
    assert "samtools sort" in code, (
        "binning.sh no longer runs `samtools sort`. metaWRAP's own sort is skipped "
        "along with its bwa when work_files/<sample>.bam is pre-placed, so without "
        "this the staged BAM is in reference-NAME order and jgi rejects it with "
        "'the bam file is not sorted!'"
    )
    sort_cmd = [ln for ln in _code_lines(_BINNING_SH) if "samtools sort" in ln]
    assert len(sort_cmd) == 1, f"expected exactly one `samtools sort`, got {sort_cmd!r}"
    assert "COVERAGE_BAM" in sort_cmd[0], (
        f"`samtools sort` no longer reads ${{COVERAGE_BAM}}: {sort_cmd[0]!r}. The "
        "sort must consume the coverage step's output directly; sorting anything "
        "else would stage an unsorted BAM under a sorted-looking name."
    )


def test_only_the_sorted_bam_is_staged_for_metawrap() -> None:
    """The ONLY thing written to the path metaWRAP reads is the sorted BAM.

    Fail-closed on purpose. The pre-fix shape was `ln`/`cp` of the writer's
    output straight to `work_files/<sample>.bam`, and the obvious test is to
    forbid that -- but a denylist passes on every shape nobody thought of
    (`install`, `cat >`, a `;`-joined `cp`, a redirect). So instead: enumerate
    every logical line that mentions the staged path and require each to be
    either the sort's own staging name or the `mv` that renames it into place.
    Any new way of putting a file there has to be added here deliberately.
    """
    staged = f"{'${WORK_FILES}'}/{'${READS_STEM}'}.bam"
    writers = [
        ln
        for ln in _code_lines(_BINNING_SH)
        if staged in ln and not ln.startswith(("STAGED_BAM=", "WORK_FILES="))
    ]
    assert writers, (
        "nothing in binning.sh writes ${WORK_FILES}/${READS_STEM}.bam -- metaWRAP "
        "would fall back to its own bwa self-alignment. This test is vacuous as "
        "written; the staging path or its variable names must have changed."
    )
    for line in writers:
        assert line.startswith("mv ") and "STAGED_BAM" in line, (
            f"something other than the staging `mv` writes the path metaWRAP "
            f"reads: {line!r}. If miint's name-ordered BAM lands there unsorted, "
            "jgi rejects it with 'the bam file is not sorted!' -- the production "
            "failure this test exists for. Stage via the sort, or extend this "
            "test on purpose."
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
    assert re.search(r"^mv\b.*STAGED_BAM", joined, re.MULTILINE), (
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


@pytest.mark.parametrize("package", ["samtools", "metabat2", "maxbin2"])
def test_binning_image_pins_version_bound_tools(package: str) -> None:
    """The three tools whose VERSION the workflow's behaviour depends on are pinned.

    None is cosmetic:
      maxbin2 <2.2.6 ships `MaxBin` rather than the `run_MaxBin.pl` metaWRAP
        invokes -- see binning.def for the full rationale.
      samtools provides the `samtools sort` binning.sh runs.
      metabat2 owns `jgi_summarize_bam_contig_depths`, the tool that rejects an
        unsorted BAM -- pinning the sort's producer but not the consumer that
        adjudicates it would leave the acceptance criterion free to move.

    samtools and metabat2 were both transitive dependencies of metawrap-mg's
    solve before this, so a rebuild could move either with no change to this repo.

    Asserted against the resolved `micromamba create` line only -- not the whole
    file -- so a version mentioned anywhere else cannot satisfy it.
    """
    create = next(
        (line for line in _code_lines(_BINNING_DEF) if "micromamba create" in line),
        None,
    )
    assert create is not None, "binning.def no longer creates the metawrap env"
    assert re.search(rf"\b{package}(=|>=)[0-9]", create), (
        f"{package} is unpinned on binning.def's `micromamba create` line: "
        f"{create!r}. It comes from bioconda, so an unpinned spec is resolved "
        f"fresh on every rebuild and can move without any change to this repo."
    )


def test_image_pins_are_asserted_at_build_time() -> None:
    """Every `=`-pinned tool in the def is also version-checked by binning-verify.sh.

    The def's pin is enforced only by the solver; nothing about it is observable
    in the built image, and a test in this repo can read the spec string but not
    the result. `binning-verify.sh` runs both as the def's `%test` and as the
    spec's `VERIFY_CMD`, so it is the one place that can fail a BUILD whose solve
    drifted. This keeps the two files in lockstep -- adding a pin without a
    matching assertion is the gap that lets a drifted image ship green.

    `>=` pins are excluded: they express a floor, not an expected version.
    """
    create = next(line for line in _code_lines(_BINNING_DEF) if "micromamba create" in line)
    exact = dict(re.findall(r"\b([A-Za-z0-9_.-]+)=([0-9][^\s\"']*)", create))
    assert exact, f"no `=`-pinned package on the create line: {create!r}"
    verify = "\n".join(_code_lines(_BINNING_VERIFY))
    for package, version in exact.items():
        assert version in verify, (
            f"binning.def pins {package}={version} but binning-verify.sh never "
            f"asserts that version, so a solve that drifted would build and ship "
            f"green. Add it to the PINNED map there (keyed by the binary the "
            f"package provides -- metabat2's is jgi_summarize_bam_contig_depths)."
        )
