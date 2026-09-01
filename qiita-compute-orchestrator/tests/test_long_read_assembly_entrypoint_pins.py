"""Static pins on the `long-read-assembly` container entrypoints and their images.

Most of the file is about `binning.sh` and the coverage BAM (below), but it also
pins `bin_refine.sh`'s `--write_bins` flag, `checkm.sh`'s TMPDIR shortening for the
AF_UNIX socket, the genomes_dir basenames the entrypoints write and read against
their Python constants, and the version constraints in `binning.def` /
`bin_refine.def` that each entrypoint's behaviour depends on. All of it is the same
kind of assertion: read the shipped file, check the command it pins is still there
and still shaped correctly.

The coverage BAM: how `binning.sh` puts it where metaWRAP will read it.

metaWRAP guards its own `bwa mem` AND its own `samtools sort` behind one
`if [[ ! -f <out>/work_files/<sample>.bam ]]`. The workflow pre-places that BAM
to skip bwa (a short-read aligner) in favour of a minimap2 pre-map -- which
silently skips the sort along with it. So whatever is staged there must ALREADY be
coordinate sorted; `jgi_summarize_bam_contig_depths` refuses anything else:

    ERROR: the bam file 'reads.bam' is not sorted!

That reached production and failed a real ticket at the binning step. It could,
because the writer's `@SQ` was in hash-bucket order then (duckdb-miint#173) and the
step's reference-NAME `ORDER BY` was therefore never the sort it looked like.

`@SQ` is now sorted by reference name, so `assembly_coverage` writes the file
coordinate sorted (`ORDER BY reference, position`) and this entrypoint stages it
unmodified rather than running its own `samtools sort` over it. Sortedness is
pinned where it can be observed -- `test_written_bam_is_tid_monotonic` in
`jobs/test_assembly_coverage.py`, which runs the real step. This file pins the
entrypoint's half: that the file staged for metaWRAP is that BAM, that it arrives
atomically, and that nothing else reaches the path metaWRAP reads.

Why these read text
-------------------
Exercising the entrypoint means building a 517 MB metaWRAP image, so these are text
assertions: they show the commands are present and shaped correctly, not that they
succeed. They need no binary and run everywhere, including CI and a stock dev box.
Correct-operation evidence is elsewhere: the behavioural test above, the consumer
measurements in `docs/duckdb-miint.md`, and the deploy verify step.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from qiita_common.assembly_constants import (
    CONTIG_ATTRIBUTE_COLUMNS,
    CONTIG_ATTRIBUTES_FILE,
)

from qiita_compute_orchestrator.jobs._assembly import LCG_FILE, NOLCG_FILE

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOW_DIR = _REPO_ROOT / "workflows" / "long-read-assembly"
_ASSEMBLE_SH = _WORKFLOW_DIR / "assemble.sh"
_BINNING_SH = _WORKFLOW_DIR / "binning.sh"
_BINNING_DEF = _WORKFLOW_DIR / "binning.def"
_BINNING_VERIFY = _WORKFLOW_DIR / "binning-verify.sh"
_BIN_REFINE_SH = _WORKFLOW_DIR / "bin_refine.sh"
_BIN_REFINE_DEF = _WORKFLOW_DIR / "bin_refine.def"
_CHECKM_SH = _WORKFLOW_DIR / "checkm.sh"
_ASSEMBLE_DEF = _WORKFLOW_DIR / "assemble.def"

# What each assembler's `%test` must grep its `--version` output for. myloasm
# reports its conda version verbatim; hifiasm_meta reports two internal versions
# that its conda string (`hamtv0.3.5`) does not contain, so neither can be derived
# from the pin and both are written out here.
_ASSERTED_VERSIONS = {
    "hifiasm_meta": ("0.13-r308", "0.3-r079"),
    "myloasm": ("0.6.0",),
}


@pytest.mark.parametrize(
    "path",
    [
        _ASSEMBLE_SH,
        _ASSEMBLE_DEF,
        _BINNING_SH,
        _BINNING_DEF,
        _BINNING_VERIFY,
        _BIN_REFINE_SH,
        _BIN_REFINE_DEF,
        _CHECKM_SH,
    ],
)
def test_source_file_is_present_and_parses(path: Path) -> None:
    """Anti-vacuity guard, matching the sibling static pins.

    Every assertion below reads one of these files through `_code_lines` — all
    eight, which is why all eight are listed here rather than the three `binning`
    ones. A moved file or a `_REPO_ROOT` that stopped resolving would make the
    absence-shaped pins pass for the wrong reason, so fail loudly here first.
    """
    assert path.is_file(), f"{path} is missing -- the pins below would be vacuous"
    assert _code_lines(path), f"{path} has no non-comment lines"


def _code_lines(path: Path) -> list[str]:
    """Script lines with comments and blanks dropped, `\\` continuations folded.

    Every assertion below runs against these, never the raw text. A deploy-checklist
    verify step for this same entrypoint once grepped the raw file for the command
    it cared about and passed on a file where the only occurrences were the comments
    describing it -- deleting the command still greened the check. A test that greps
    raw text would have exactly that hole, and this file is mostly comments.

    Continuations are folded so one logical command is one entry, the way the
    shell reads it. Without that, a multi-line invocation is several fragments
    and any assertion spanning a command and its arguments silently cannot match
    -- passing only for tests phrased as absences.

    TRAILING comments are stripped too, not just whole-line ones. That matters for
    the presence-shaped pins below, which ask whether a command appears at all:
    `cmd # samtools faidx "${WORK}/assembly.fa"` would otherwise satisfy the
    reorder pin, the same hole in a different place. (The fail-closed staging pin
    is immune — a commented `cp` still mentions ${STAGED_BAM} and would fail its
    allowlist instead.) `#` is only a comment when it starts a word and is not
    inside quotes, so this tracks quote state rather than splitting on the first
    `#`. No script here contains a quoted `#` today; the parser handles it so that
    a future one cannot quietly break the pin.
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


@pytest.mark.parametrize(
    ("path", "dir_var", "expected"),
    [
        (_ASSEMBLE_SH, "OUT", {LCG_FILE, NOLCG_FILE, CONTIG_ATTRIBUTES_FILE}),
        (_BINNING_SH, "GENOMES_DIR", {NOLCG_FILE}),
        (_BIN_REFINE_SH, "GENOMES_DIR", {NOLCG_FILE}),
    ],
    ids=["assemble", "binning", "bin_refine"],
)
def test_genomes_dir_basenames_match_the_python_constants(
    path: Path, dir_var: str, expected: set[str]
) -> None:
    """The genomes_dir basenames are spelled the same in the shell and in Python.

    `assemble.sh` writes both files into its output genomes_dir; `binning.sh` and
    `bin_refine.sh` read noLCG back out of the one they are handed. The native jobs
    reach the same files through `_assembly.LCG_FILE` / `NOLCG_FILE`, and nothing
    joins the two spellings at runtime. `assembly_hash._file_meta` looks genomes_dir
    up by exact name, not by glob, so renaming one side alone drops the circular and
    unbinned contigs from the run with no error; a sample with no refined bin either
    becomes the terminal StepNoData "no contigs to hash", discarded with no retry.

    Set equality, not membership: a new file under genomes_dir has to be added here
    and given a constant, rather than reaching the native jobs unnamed. `dir_var` is
    per script because `${OUT}` is genomes_dir only in `assemble.sh` -- in the other
    two it is the bins output.
    """
    code = "\n".join(_code_lines(path))
    found = set(re.findall(rf"\$\{{{dir_var}\}}/([^\s\"']+)", code))
    assert found == expected, (
        f"{path.name} reaches genomes_dir by ${{{dir_var}}}/{sorted(found)}, but the "
        f"Python constants spell it {sorted(expected)}. The two are pinned together "
        "here because nothing checks them at runtime -- a rename on one side leaves "
        "assembly_hash scanning for a file the entrypoint no longer writes."
    )


def test_assemble_runs_the_assembler_into_its_own_output() -> None:
    """Both arms point the assembler's `-o` at ${ASM_DIR}, under $QIITA_OUTPUT_PATH.

    That is the whole mechanism: the assembler's tree is retained because it is
    written where the step's output already lives, not copied there afterwards.
    An `-o` pointed anywhere else -- a mktemp dir, $TMPDIR, the workspace --
    discards everything the arm does not read back, and does it silently, since
    the two published FASTAs are unaffected.
    """
    lines = _code_lines(_ASSEMBLE_SH)
    code = "\n".join(lines)
    assert 'ASM_DIR="${QIITA_OUTPUT_PATH}/assembler"' in code, (
        "ASM_DIR is not defined as ${QIITA_OUTPUT_PATH}/assembler. It is not a "
        "declared output, so nothing at run time checks where it points -- a path "
        "outside the output root is neither listed in the manifest nor verified, "
        "and the assembler's tree would be discarded exactly as it was before."
    )
    dasho = re.findall(r'-o "([^"]+)"', code)
    assert dasho, "assemble.sh passes no -o at all -- neither arm names an output dir"
    assert all(t.startswith("${ASM_DIR}") for t in dasho), (
        f"assemble.sh runs an assembler with -o {dasho} -- every arm must write into "
        "${ASM_DIR} so its tree lands under $QIITA_OUTPUT_PATH"
    )
    assert len(dasho) == 2, f"expected one -o per assembler arm, found {len(dasho)}: {dasho}"


def test_assemble_deletes_nothing_on_exit() -> None:
    """No `trap ... EXIT` and no mktemp in assemble.sh.

    The three sibling entrypoints each stage working files through a `mktemp -d`
    they delete on exit. This one must not: everything the assembler writes is an
    output of the step, and the arms read their one file back out of that same
    tree. A trap reintroduced here removes it after the manifest is written, so
    the verifier's `files` list would name paths that no longer exist -- gate 2,
    a permanent CONTRACT_VIOLATION -- rather than failing quietly.
    """
    code = "\n".join(_code_lines(_ASSEMBLE_SH))
    assert "mktemp" not in code, (
        "assemble.sh calls mktemp; the assembler's tree is a step output and "
        "belongs under $QIITA_OUTPUT_PATH"
    )
    assert not re.search(r"\btrap\b", code), (
        "assemble.sh installs a trap; an EXIT handler here deletes files the "
        "manifest already declared"
    )


def test_assemble_restores_write_before_clearing_its_output_dirs() -> None:
    """Both output dirs are chmod'd writable, then removed, then re-created.

    `qiita_finish` leaves directories 0550 and files 0440. A directory without
    its write bit does not give up its entries, so `rm -rf` over such a tree
    exits 1 -- and under `set -e` that aborts the step rather than clearing it.
    The chmod is what makes the clear work, so it is pinned with it, in order.

    Both dirs, not just the assembler tree: the awk redirect into circular.fa
    cannot truncate a 0440 file either. The CP gives an ordinary retry a fresh
    attempt dir; a SLURM-side requeue re-enters this one.
    """
    code = _code_lines(_ASSEMBLE_SH)
    chmod = next((i for i, ln in enumerate(code) if ln.startswith("chmod -R u+w")), None)
    clear = next((i for i, ln in enumerate(code) if ln.startswith("rm -rf")), None)
    mkdir = next((i for i, ln in enumerate(code) if ln.startswith("mkdir -p")), None)
    assert chmod is not None, (
        "assemble.sh does not restore write before clearing; `rm -rf` over a tree "
        "qiita_finish left at 0550/0440 exits 1"
    )
    assert clear is not None, "nothing in assemble.sh clears its output dirs"
    assert mkdir is not None, "assemble.sh never creates its output dirs"
    assert chmod < clear < mkdir, (
        f"assemble.sh orders chmod/rm/mkdir at {chmod}/{clear}/{mkdir}; the chmod "
        "must precede the rm (or the rm fails) and both must precede the mkdir "
        "(or the clear undoes it)"
    )
    loop = next((i for i, ln in enumerate(code) if ln.startswith("for d in ")), None)
    assert loop is not None, "the clear is not a loop over the output dirs"
    assert "${OUT}" in code[loop] and "${ASM_DIR}" in code[loop], (
        f"the clear covers {code[loop]!r}; both ${{OUT}} and ${{ASM_DIR}} carry "
        "0440 files from a previous attempt"
    )
    done = next((i for i, ln in enumerate(code) if ln == "done" and i > loop), None)
    assert done is not None, "the clear loop is never closed"
    # Both must act on the loop VARIABLE and sit inside the loop. `chmod -R u+w
    # "${OUT}"` in the body would pass an order-only check while leaving
    # ${ASM_DIR} at 0550, so its `rm -rf` exits 1 and aborts the step -- the exact
    # failure this pin is for, on the one directory the order says nothing about.
    assert loop < chmod < done and loop < clear < done, (
        f"chmod at {chmod} and rm at {clear} are not both inside the clear loop ({loop}..{done})"
    )
    assert '"${d}"' in code[chmod] and '"${d}"' in code[clear], (
        f"the loop body chmods {code[chmod]!r} and removes {code[clear]!r}; both "
        'must act on "${d}" or an iteration operates on the wrong directory'
    )


def test_both_arms_emit_the_contig_attribute_sidecar() -> None:
    """Each assembler arm writes the sidecar, with the same columns.

    Two producers, two consumers: `assembly_load` and the control plane's
    membership write both read this file by column NAME, so the arms must agree
    with each other and with `CONTIG_ATTRIBUTE_COLUMNS`. They agree on nothing
    else -- one is an awk over a GFA, the other a DuckDB COPY -- so nothing but
    this checks it.
    """
    code = "\n".join(_code_lines(_ASSEMBLE_SH))
    assert code.count(CONTIG_ATTRIBUTES_FILE) == 2, (
        f"expected both arms to name {CONTIG_ATTRIBUTES_FILE}; found "
        f"{code.count(CONTIG_ATTRIBUTES_FILE)} mention(s)"
    )
    # The hifiasm arm's header row, written literally in the awk BEGIN block.
    header = ", ".join(f'"{c}"' for c in CONTIG_ATTRIBUTE_COLUMNS)
    assert header in code, (
        f"the hifiasm_meta arm's sidecar header is not {header} -- the two arms "
        "and the Python constant must spell the columns identically"
    )
    # The hifiasm arm's DATA row, which must carry the same five fields in the
    # same order as the header above -- a reordered `print` would leave the header
    # correct and every value under the wrong column.
    assert 'print $2, $2, call, dp, "" > attrs' in code, (
        "the hifiasm_meta arm's sidecar data row no longer writes "
        f"{list(CONTIG_ATTRIBUTE_COLUMNS)} in header order"
    )
    # The myloasm arm's projection, in myloasm_split.py's COPY.
    split = "\n".join(_code_lines(_WORKFLOW_DIR / "myloasm_split.py"))
    assert "SELECT contig_id, header AS raw_name, circularity, depth, mult" in split, (
        "myloasm_split.py's attribute projection no longer matches "
        f"{list(CONTIG_ATTRIBUTE_COLUMNS)}"
    )


def test_hifiasm_arm_fails_on_an_unrecognised_segment_name() -> None:
    """A GFA segment name matching neither shape stops the step.

    The call is stored per contig, so a name matching neither shape would write a
    circularity into the lake for a contig nothing classified -- and hifiasm_meta
    is pinned, so a name outside the documented shape means the grammar moved
    rather than that the assembler produced something unusual.
    """
    code = "\n".join(_code_lines(_ASSEMBLE_SH))
    assert "LIN_RE='tg[0-9]+l$'" in code, (
        "the hifiasm_meta arm no longer recognises the LINEAR name shape, so every "
        "linear contig would be counted as unrecognised"
    )
    # One grammar for the attribute pass and both FASTA writers: the circular
    # pattern is defined once and every user reads that variable, so the router
    # and the stored call cannot disagree about which names are circular.
    assert "CIRC_RE='tg[0-9]+c$'" in code, (
        "the circular name shape is no longer defined once in assemble.sh"
    )
    assert "tg[0-9]+c$/" not in code, (
        "a literal circular pattern is back alongside ${CIRC_RE}; the attribute "
        "pass and the FASTA writers can now drift apart"
    )
    assert re.search(r"exit 65", code), (
        "nothing in assemble.sh exits non-zero for an unrecognised segment name; "
        "a fall-through stores a circularity nobody determined"
    )


def test_hifiasm_arm_reads_depth_by_tag_not_by_position() -> None:
    """`dp:f` is found by scanning the optional fields, not by column index.

    GFA does not fix the order of a segment's optional tags. Reading $5 would
    silently store `LN:i`'s or `ts:B:I`'s value as depth the day the order
    changes, which no downstream check could catch -- a depth is a plausible
    number whatever it came from.
    """
    code = "\n".join(_code_lines(_ASSEMBLE_SH))
    assert "for (i = 4; i <= NF; i++)" in code and "$i ~ /^dp:f:/" in code, (
        "the hifiasm_meta arm no longer searches fields 4+ for the dp:f tag"
    )


def test_assemble_def_pins_and_asserts_both_assemblers() -> None:
    """Both assemblers are `=`-pinned in the def AND version-asserted in %test.

    A pin binds the solver only. Nothing about it is observable in the built
    image, and a rebuild of the same conda version against different upstream
    sources satisfies the pin while moving the tool -- so the pin without the
    assertion is the gap that lets a drifted image ship green. This is the
    assemble image's counterpart to the binning-verify.sh check below.
    """
    lines = _code_lines(_ASSEMBLE_DEF)
    creates = [ln for ln in lines if "micromamba create" in ln]
    pinned = {}
    for line in creates:
        pinned.update(dict(re.findall(r"\b([A-Za-z0-9_.-]+)=([A-Za-z0-9][^\s\"\']*)", line)))
    assert set(pinned) == {"hifiasm_meta", "myloasm"}, (
        f"expected both assemblers `=`-pinned on their create lines; got {pinned}"
    )
    assert pinned == {"hifiasm_meta": "hamtv0.3.5", "myloasm": "0.6.0"}, (
        f"the assembler pins moved; got {pinned}. Update _ASSERTED_VERSIONS below "
        "in the same change, or the %test greps go on checking the old build"
    )
    for package, expected in _ASSERTED_VERSIONS.items():
        # The invocation alone is not the assertion: `<tool> --version` on its own
        # line satisfies a search for it while checking nothing. Require the output
        # to reach a grep, so the %test line has to compare against something.
        asserted = [
            ln for ln in lines if re.search(rf"{re.escape(package)} --version", ln) and "grep" in ln
        ]
        assert asserted, (
            f"{package} is pinned but its --version output is never compared in "
            "%test, so a drifted solve would build green"
        )
        # And each grep must name the version it is checking for. Spelled out
        # rather than derived from the conda pin: hifiasm_meta's conda string
        # (`hamtv0.3.5`) appears in none of them, because the binary reports two
        # internal versions instead -- so a derived check would have to accept any
        # version-shaped token, and would pass with the pin bumped and the greps
        # left behind.
        for token in expected:
            assert any(token in ln for ln in asserted), (
                f"{package}'s %test never greps for {token!r}; the pin and the "
                "build-time assertion have drifted apart"
            )


def test_binning_stages_the_coverage_bam_unrewritten() -> None:
    """The staging name is filled by copying ${COVERAGE_BAM}, byte for byte.

    Fail-closed, like the sibling below: enumerate every logical line touching
    ${STAGED_BAM} and require each to be one of the three steps of the staging
    itself. A denylist would pass on every shape nobody thought of. One shape this
    excludes is a re-added `samtools sort` writing there: it would absorb a
    regression in `assembly_coverage`'s ORDER BY, which is what makes the file
    sorted.
    """
    code = _code_lines(_BINNING_SH)
    touching = [ln for ln in code if "STAGED_BAM" in ln and not ln.startswith("STAGED_BAM=")]
    assert touching, (
        "nothing in binning.sh fills ${STAGED_BAM} -- the staging shape or its "
        "variable names changed, and the assertions here are vacuous as written."
    )

    fill = [ln for ln in touching if ln.startswith("cp ")]
    assert len(fill) == 1, f"expected exactly one `cp` into the staging name, got {fill!r}"
    assert "COVERAGE_BAM" in fill[0], (
        f"the staged BAM no longer comes from ${{COVERAGE_BAM}}: {fill[0]!r}. metaWRAP "
        "reads whatever sits at work_files/<sample>.bam, so staging anything else "
        "silently substitutes the coverage the depth matrix is built from."
    )

    for line in touching:
        allowed = line.startswith(("cp ", "rm -f ", "mv "))
        assert allowed, (
            f"something other than the staging steps writes ${{STAGED_BAM}}: {line!r}. "
            "A `samtools sort` here would also absorb an unsorted BAM arriving from "
            "assembly_coverage -- the sort belongs there, and is pinned there."
        )


def test_only_the_staged_bam_reaches_metawrap() -> None:
    """The ONLY thing written to the path metaWRAP reads is the staged BAM.

    Fail-closed: a denylist passes on every shape nobody thought of
    (`install`, `cat >`, a `;`-joined `cp`, a redirect). So instead: enumerate
    every logical line that mentions the staged path and require each to be
    either the staging name it is renamed from or the `mv` that does it.
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
        is_staging_mv = line.startswith("mv ") and "STAGED_BAM" in line
        # Deliberately permitted READER (not a writer of this path): the reorder
        # step reads the staged BAM's @SQ header to derive the assembly order, so
        # the staged path is an input to `samtools view -H` here and the redirect
        # target is STAGED_ORDER, not the staged BAM. Anything else still fails.
        is_sq_header_read = "samtools view -H" in line and "STAGED_ORDER" in line
        assert is_staging_mv or is_sq_header_read, (
            f"something other than the staging `mv` (or the @SQ-order read) touches "
            f"the path metaWRAP reads: {line!r}. Whatever lands there is what jgi "
            "reads, and it rejects an unsorted file with 'the bam file is not "
            "sorted!' -- the production failure this test exists for. Stage via the "
            "`mv`, or extend this test to cover the new path."
        )


def test_binning_stages_the_bam_atomically() -> None:
    """Fill a scratch name, then rename, so a `cp` killed mid-write cannot leave a
    truncated file at the path metaWRAP reads.

    The rename must stay inside work_files/: the workspace and QIITA_OUTPUT_PATH
    are separate bind mounts, so a cross-directory `mv` is EXDEV and silently
    degrades to copying a reads-sized BAM.
    """
    code = _code_lines(_BINNING_SH)
    joined = "\n".join(code)
    assert "STAGED_BAM=" in joined, "binning.sh no longer stages under a scratch name"
    staged = next(line for line in code if line.startswith("STAGED_BAM="))
    assert "WORK_FILES" in staged, (
        f"the staging name left work_files/: {staged!r}. A rename out of "
        "QIITA_OUTPUT_PATH crosses a bind mount (EXDEV) and stops being atomic."
    )
    assert re.search(r"^mv\b.*STAGED_BAM", joined, re.MULTILINE), (
        "the staged BAM is no longer renamed into place with `mv`"
    )


def test_binning_reorders_the_assembly_to_sq_order() -> None:
    """The assembly is reordered to the staged BAM's @SQ order before metaWRAP.

    A separate requirement from record order, and one that outlives it: metaWRAP's
    jgi_summarize writes the depth matrix in @SQ order, and metabat2 aborts unless
    the assembly FASTA is in that SAME order ("the order of contigs in abundance
    file is not the same as the assembly file"). noLCG.fa is in hifiasm's numeric
    order and @SQ is name-sorted, i.e. lexicographic, so the two disagree. Ordering
    records by (reference, position) does not move an @SQ line, so the coordinate
    sort neither causes nor cures this; it comes out when @SQ becomes steerable
    upstream or the assembler emits name order. Reproduced and the reorder-fix
    confirmed on samtools 1.10 / metabat2 2.15 (a numeric-order assembly aborts,
    the @SQ-reordered one binds).

    Text pin, like the staging pins above: proving the reorder *works* needs the
    metaWRAP/metabat2 image; this proves it is still WIRED -- the order comes from
    the staged BAM's @SQ header and metaWRAP is handed the reordered file, not the
    raw noLCG.
    """
    code = _code_lines(_BINNING_SH)
    joined = "\n".join(code)

    # The @SQ order is extracted from the staged BAM header -- the file jgi will
    # read -- rather than from noLCG, whose contig order is the thing being fixed.
    order_src = [ln for ln in code if "ORDERED_NOLCG" in ln or "assembly.ordered" in ln]
    assert any("samtools faidx" in ln for ln in order_src), (
        "binning.sh no longer builds the reordered assembly with `samtools faidx`. "
        "Without it the assembly stays in hifiasm's numeric order while jgi's depth "
        "matrix is in @SQ order, and metabat2 aborts on the mismatch."
    )
    assert re.search(r"samtools view -H .*READS_STEM.*\.bam", joined), (
        "the @SQ order is no longer read from the STAGED BAM header. It must come "
        "from the file jgi reads (work_files/<sample>.bam), or the assembly order "
        "and the depth order can diverge again."
    )


def test_metawrap_gets_the_reordered_assembly_not_raw_nolcg() -> None:
    """metaWRAP's `-a` is the @SQ-reordered assembly, never the raw noLCG.

    Fail-closed: the whole fix is inert if metaWRAP is handed `${NOLCG}` (numeric
    order) instead of `${ORDERED_NOLCG}`. Assert the single `metawrap binning`
    invocation carries the reordered file and not the raw one.
    """
    binning_call = [ln for ln in _code_lines(_BINNING_SH) if "metawrap binning" in ln]
    assert len(binning_call) == 1, f"expected one `metawrap binning`, got {binning_call!r}"
    call = binning_call[0]
    assert "ORDERED_NOLCG" in call, (
        f"`metawrap binning` no longer receives the reordered assembly: {call!r}. "
        "Handing it ${NOLCG} (hifiasm's numeric order) puts the assembly out of "
        "step with jgi's @SQ-ordered depth matrix and metabat2 aborts."
    )
    assert re.search(r'-a\s+"\$\{NOLCG\}"', call) is None, (
        f"`metawrap binning` is passed the raw ${{NOLCG}} on its `-a`: {call!r}. "
        "That is the numeric-order assembly the reorder exists to replace."
    )


def test_binning_fails_loud_on_contig_set_drift() -> None:
    """The reorder asserts the @SQ set equals noLCG's, rather than silently
    dropping or inventing contigs.

    assembly_coverage maps to noLCG, so the sets are equal by construction; an
    inequality is a real upstream bug. A silent reorder that dropped contigs would
    bin an incomplete assembly.
    """
    code = "\n".join(_code_lines(_BINNING_SH))
    assert "n_ordered" in code and "n_nolcg" in code, (
        "binning.sh no longer compares the reordered contig count against noLCG's. "
        "A dropped or extra contig from an @SQ/noLCG set mismatch would slip "
        "through silently."
    )
    assert re.search(r'"\$\{n_ordered\}"\s*-ne\s*"\$\{n_nolcg\}"', code), (
        "the contig-count equality guard changed shape -- it must still fail the "
        "step when the reordered assembly and noLCG disagree on contig count."
    )


def test_binning_image_installs_libgfortran_for_concoct() -> None:
    """concoct's `vbgmm` links libgfortran.so.3, which the metawrap solve omits.

    The env ships only libgfortran.so.5, so `import vbgmm` fails at runtime with an
    ImportError and metaWRAP's concoct binner dies — failing the whole step, even
    though metabat2 + maxbin2 succeed. binning.def must install libgfortran=3.0.0
    (provides .so.3, coexists with libgfortran5), and binning-verify.sh must assert
    the import at build time — the plain runnability check can't catch it, because
    the failure is a Python ImportError (exit 1), not a loader verdict (126/127).
    Probed on a real assembly: the install takes `import vbgmm` from ImportError to
    OK and concoct runs to completion.
    """
    defsrc = "\n".join(_code_lines(_BINNING_DEF))
    assert re.search(r"micromamba install\b[^\n]*\blibgfortran=3", defsrc), (
        "binning.def no longer installs libgfortran=3 into the metawrap env. "
        "Without libgfortran.so.3, concoct's vbgmm ImportErrors at runtime and "
        "metaWRAP fails the binning step."
    )
    verify = "\n".join(_code_lines(_BINNING_VERIFY))
    assert "import vbgmm" in verify, (
        "binning-verify.sh no longer asserts `import vbgmm`. The tool-runnability "
        "loop cannot catch this failure (it's a Python ImportError, exit 1, not a "
        "loader 126/127), so without this assertion a concoct-broken image ships "
        "green — which is exactly how it shipped once."
    )


def test_bin_refine_passes_write_bins_as_a_bare_flag() -> None:
    """DAS_Tool's `--write_bins` is boolean; a trailing value crashes r-docopt.

    bin_refine.sh once passed `--write_bins 1`; the spurious `1` is an unexpected
    positional that r-docopt 0.7.2 renders as `'short' is not a valid field or
    method name for reference class "Argument"` and Execution-halts BEFORE DAS_Tool
    runs — a real crash the step then fails loud on, having reached bin_refine for
    the first time only after binning was fixed. qp-pacbio passes it bare; probed
    on das_tool 1.1.7 / r-docopt 0.7.2, the bare form parses and the `1` form
    crashes.
    """
    call = [ln for ln in _code_lines(_BIN_REFINE_SH) if "DAS_Tool" in ln and "--write_bins" in ln]
    assert call, "bin_refine.sh no longer invokes DAS_Tool with --write_bins"
    joined = "\n".join(call)
    # A redirect / next flag after --write_bins is fine; an alphanumeric token
    # (the `1`) is the bug.
    assert not re.search(r"--write_bins\s+[0-9A-Za-z]", joined), (
        f"bin_refine.sh passes a value to the boolean --write_bins flag: {joined!r}. "
        "A trailing token (e.g. `--write_bins 1`) is an unexpected positional that "
        "r-docopt renders as the 'short is not a valid field' crash. Pass it bare."
    )


def test_bin_refine_image_pins_das_tool() -> None:
    """das_tool is pinned to 1.1.x, matching the summary-columns invariant.

    bin_refine.sh + assembly_load depend on DAS_Tool 1.1.x's summary columns, and
    the dastool image rebuilds on any bin_refine.sh change — an unpinned create
    line lets the solve drift off that invariant on a routine rebuild.
    """
    create = next(
        (
            ln
            for ln in _code_lines(_BIN_REFINE_DEF)
            if "micromamba create" in ln and "dastool" in ln
        ),
        None,
    )
    assert create is not None, "bin_refine.def no longer creates the dastool env"
    assert re.search(r"\bdas_tool=1\.1", create), (
        f"das_tool is unpinned or off 1.1.x on bin_refine.def's create line: {create!r}."
    )


def test_checkm_shortens_tmpdir_for_the_afunix_socket() -> None:
    """CheckM's multiprocessing.Manager binds an AF_UNIX socket under $TMPDIR.

    The SLURM payload sets TMPDIR=<workspace>/tmp (~85 chars, on real disk so temp
    doesn't fill the tiny --containall /tmp tmpfs), and Python appends
    `/pymp-XXXXXXXX/listener-XXXXXXXX` — overflowing the ~108-char AF_UNIX sun_path
    limit, so lineage_wf dies with `OSError: AF_UNIX path too long` on EVERY run.
    checkm.sh must repoint TMPDIR at a SHORT /tmp path symlinked into WORK before
    running checkm. Reproduced on the real ticket; the short symlink clears it.
    """
    code = _code_lines(_CHECKM_SH)
    joined = "\n".join(code)
    assert any(ln.startswith("export TMPDIR=") for ln in code), (
        "checkm.sh no longer repoints TMPDIR — CheckM's mp.Manager AF_UNIX socket "
        "path overflows under the payload's long workspace TMPDIR."
    )
    assert re.search(r"/tmp/ck", joined), (
        "checkm.sh's short-TMPDIR target is not a /tmp-rooted path; a long TMPDIR "
        "overflows the ~108-char AF_UNIX sun_path limit."
    )
    assert re.search(r'\bln -s\S*\s+"\$\{WORK\}"', joined), (
        "the short TMPDIR is not symlinked to WORK — CheckM's temp would land on "
        "the tiny tmpfs /tmp instead of real disk."
    )


@pytest.mark.parametrize("package", ["samtools", "metabat2", "maxbin2"])
def test_binning_image_pins_version_bound_tools(package: str) -> None:
    """The three tools whose VERSION the workflow's behaviour depends on are pinned.

    None is cosmetic:
      maxbin2 <2.2.6 ships `MaxBin` rather than the `run_MaxBin.pl` metaWRAP
        invokes -- see binning.def for the full rationale.
      samtools provides the `samtools faidx` that reorders the assembly, whose
        region-order and missing-region behaviour the reorder's guard rests on,
        and the `samtools index` metaWRAP's concoct block runs.
      metabat2 owns `jgi_summarize_bam_contig_depths`, which rejects an unsorted
        BAM and whose depth matrix must agree with the assembly's contig order --
        it adjudicates both criteria this entrypoint is built around.

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
