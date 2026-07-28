"""Pin how the `assemble` step reads circularity out of myloasm's output.

Why this is not the hifiasm rule
--------------------------------
The hifiasm_meta branch splits circular from linear on the GFA SEGMENT NAME
(`…tg……c` / `…l`). myloasm does not encode circularity there at all — it states it
in the `assembly_primary.fa` HEADER. Carrying the hifiasm regex across would not
error; it would match nothing, leaving circular.fa empty and quietly demoting
every closed genome to binning input. The whole LCG class would vanish with a
green run, which is why the split is pinned by execution here rather than trusted.

What was probed, and how
------------------------
myloasm 0.6.0 (bioconda), against ONE real genome — M. genitalium G37
(NC_000908.2), a complete circular chromosome — sampled twice into read sets
identical in every respect (same bases, coverage, read-length distribution, error
model, RNG seed) EXCEPT wrap-around, then assembled in two separate runs:

    reads sampled WITH wrap-around  -> u713ctg_len-580076_circular-yes_…
    reads sampled WITHOUT wrap      -> u278ctg_len-577882_circular-no_…

Topology was the only variable, and the control reproduced the negative case, so
the `circular-` field does track real circularity. `circular-possibly` is
documented by myloasm but was NOT reproduced (runs starved to 1-3x depth still
reported `circular-no`); it is therefore accepted as a well-formed value and
routed to noLCG, which is both the safe direction and what the assay owner does
by hand.

These tests EXECUTE `myloasm_split.awk` rather than text-matching it, so they
prove the split works, not merely that it is still spelled a certain way. The
static pins at the bottom cover the parts that cannot be executed without
building the image.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOW_DIR = _REPO_ROOT / "workflows" / "long-read-assembly"
_SPLIT_AWK = _WORKFLOW_DIR / "myloasm_split.awk"
_ASSEMBLE_SH = _WORKFLOW_DIR / "assemble.sh"
_ASSEMBLE_DEF = _WORKFLOW_DIR / "assemble.def"
_ASSEMBLE_ENV = _WORKFLOW_DIR / "sif-build.d" / "assemble.env"

# The two headers verbatim from the probe described in the module docstring. Kept
# byte-exact (trailing `mult=1.00` included) so a myloasm release that changes the
# header shape breaks these tests rather than the production step.
_CIRC_HEADER = ">u713ctg_len-580076_circular-yes_depth-32-32-32_duplicated-no mult=1.00"
_LIN_HEADER = ">u278ctg_len-577882_circular-no_depth-33-33-33_duplicated-no mult=1.00"

_EXIT_CONTRACT_VIOLATION = 64


def _require_awk() -> str:
    awk = shutil.which("awk")
    if awk is None:
        pytest.skip("awk not available; the split cannot be executed here")
    return awk


def _split(tmp_path: Path, fasta: str) -> subprocess.CompletedProcess[str]:
    """Run the splitter exactly the way assemble.sh does.

    assemble.sh pre-creates both outputs before invoking awk (so an empty CLASS is
    an empty file, not a missing one) — mirrored here, or these tests would be
    exercising a different contract than production.
    """
    awk = _require_awk()
    primary = tmp_path / "assembly_primary.fa"
    primary.write_text(fasta)
    circ = tmp_path / "circular.fa"
    nolcg = tmp_path / "noLCG.fa"
    circ.write_text("")
    nolcg.write_text("")
    return subprocess.run(
        [
            awk,
            "-v",
            f"circ_out={circ}",
            "-v",
            f"nolcg_out={nolcg}",
            "-f",
            str(_SPLIT_AWK),
            str(primary),
        ],
        capture_output=True,
        text=True,
    )


def _ids(path: Path) -> list[str]:
    return [ln[1:] for ln in path.read_text().splitlines() if ln.startswith(">")]


def test_split_program_is_present() -> None:
    """Anti-vacuity guard: every test below shells out to this file, and a missing
    one would make `awk -f` fail in a way easy to mistake for a real assertion."""
    assert _SPLIT_AWK.is_file(), f"{_SPLIT_AWK} is missing — the pins below are vacuous"


def test_real_probe_headers_split_on_circularity(tmp_path: Path) -> None:
    """The circular arm lands in circular.fa, the linear control in noLCG.fa.

    This is the probe's own result replayed through our code: the two headers
    differ ONLY in what myloasm concluded about topology, so a splitter that sent
    them the same way would be ignoring the field entirely.
    """
    result = _split(tmp_path, f"{_CIRC_HEADER}\nACGTACGT\n{_LIN_HEADER}\nTTTTGGGG\n")
    assert result.returncode == 0, result.stderr

    assert _ids(tmp_path / "circular.fa") == ["u713ctg"]
    assert _ids(tmp_path / "noLCG.fa") == ["u278ctg"]


def test_sequence_bytes_are_copied_verbatim(tmp_path: Path) -> None:
    """The splitter routes records; it must never rewrite sequence.

    These contigs are hashed downstream with the SAME canonical hash a reference
    sequence gets, so a single altered or re-wrapped byte would mint a different
    feature_idx for an identical molecule.
    """
    seq = "ACGTACGTAC"
    result = _split(tmp_path, f"{_CIRC_HEADER}\n{seq}\n")
    assert result.returncode == 0, result.stderr
    lines = (tmp_path / "circular.fa").read_text().splitlines()
    assert [ln for ln in lines if not ln.startswith(">")] == [seq]


def test_line_wrapped_fasta_is_preserved(tmp_path: Path) -> None:
    """A wrapped record keeps every sequence line, in order.

    myloasm 0.6.0 was observed to write one line per contig, but nothing in the
    splitter relies on that and a future release must not silently truncate.
    """
    result = _split(tmp_path, f"{_CIRC_HEADER}\nACGT\nTTGG\nCC\n")
    assert result.returncode == 0, result.stderr
    lines = (tmp_path / "circular.fa").read_text().splitlines()
    assert lines == [">u713ctg", "ACGT", "TTGG", "CC"]


def test_circular_possibly_is_not_treated_as_circular(tmp_path: Path) -> None:
    """`circular-possibly` goes to noLCG — the recoverable direction.

    A contig wrongly sent to noLCG is still recovered through binning (as a
    single-contig MAG if it bins alone). A contig wrongly called circular bypasses
    binning entirely and is stored as a complete genome that was never closed, so
    the asymmetry decides which way an uncertain call should fall.
    """
    header = ">u9ctg_len-1234_circular-possibly_depth-10-9-9_duplicated-no mult=1.00"
    result = _split(tmp_path, f"{header}\nACGT\n")
    assert result.returncode == 0, result.stderr
    assert _ids(tmp_path / "circular.fa") == []
    assert _ids(tmp_path / "noLCG.fa") == ["u9ctg"]


def test_contig_id_is_stable_when_only_the_length_drifts(tmp_path: Path) -> None:
    """Re-assembling the same sample must not rename the contig.

    A circular contig's reported length moves by a few bp between runs because its
    rotational start does. The id becomes the LCG's bin_id in assembly_hash, so
    carrying `_len-<N>` into it would make the same genome a different bin on every
    re-assembly.
    """
    first = tmp_path / "a"
    second = tmp_path / "b"
    first.mkdir()
    second.mkdir()
    _split(first, ">u713ctg_len-580076_circular-yes_depth-32-32-32_duplicated-no mult=1.00\nAC\n")
    _split(second, ">u713ctg_len-580071_circular-yes_depth-32-32-32_duplicated-no mult=1.00\nAC\n")
    assert _ids(first / "circular.fa") == _ids(second / "circular.fa") == ["u713ctg"]


def test_trailing_header_fields_do_not_reach_the_id(tmp_path: Path) -> None:
    """The space-separated `mult=…` tail is never part of the id.

    miint's read_fastx takes a record id to be the header's FIRST token, so an id
    carrying a space would be silently truncated downstream instead of here.
    """
    result = _split(tmp_path, f"{_CIRC_HEADER}\nAC\n")
    assert result.returncode == 0, result.stderr
    ids = _ids(tmp_path / "circular.fa")
    assert ids == ["u713ctg"]
    assert not any(" " in i or "mult" in i for i in ids)


@pytest.mark.parametrize(
    ("case", "fasta"),
    [
        # A renamed/reordered field set — the exact drift a myloasm upgrade could
        # introduce, and the one that would otherwise pass silently with an empty
        # circular.fa.
        ("unknown header shape", ">u1ctg_length-10_loop-yes mult=1.00\nACGT\n"),
        # A fourth circularity value we have never probed must stop the step, not
        # be guessed at.
        ("unknown circularity value", ">u1ctg_len-10_circular-maybe_depth-1-1-1\nACGT\n"),
        # Two genomes collapsing onto one bin_id downstream.
        (
            "duplicate contig id",
            ">x_len-10_circular-yes_d\nAC\n>x_len-99_circular-no_d\nGT\n",
        ),
        # A truncated or mis-detected file.
        ("sequence before any header", "ACGT\n>x_len-10_circular-no_d\nAC\n"),
    ],
)
def test_malformed_input_fails_loud(tmp_path: Path, case: str, fasta: str) -> None:
    """Every shape we cannot interpret exits 64 instead of producing a partial split.

    Silence is the dangerous outcome here: an unrecognised header simply fails to
    match `_circular-yes`, so the step would exit 0 having classified every genome
    as linear. Fail-closed converts that into a step failure an operator sees.
    """
    result = _split(tmp_path, fasta)
    assert result.returncode == _EXIT_CONTRACT_VIOLATION, (
        f"{case}: expected exit {_EXIT_CONTRACT_VIOLATION}, got {result.returncode}. "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert "myloasm_split" in result.stderr, (
        f"{case}: nothing diagnostic on stderr: {result.stderr!r}"
    )


def test_empty_assembly_yields_two_empty_files(tmp_path: Path) -> None:
    """A zero-record primary FASTA leaves both classes empty, not missing.

    assembly_coverage reads a missing OR zero-byte noLCG.fa as "nothing to bin"
    and assembly_hash raises StepNoData when neither class exists, so an empty
    assembly must stay a clean no-data outcome rather than a crash.
    """
    result = _split(tmp_path, "")
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "circular.fa").read_text() == ""
    assert (tmp_path / "noLCG.fa").read_text() == ""


# ---------------------------------------------------------------------------
# Static pins: the wiring that cannot be executed without building the image.
# ---------------------------------------------------------------------------


def test_assemble_sh_runs_myloasm_and_the_splitter() -> None:
    """The myloasm branch invokes the tool in its env and the real splitter file."""
    code = _ASSEMBLE_SH.read_text()
    assert "micromamba run -n assemble myloasm" in code, (
        "assemble.sh no longer runs myloasm from the `assemble` env; the base env "
        "has no such binary and the step would die mid-run."
    )
    assert "-f /opt/qiita/myloasm_split.awk" in code, (
        "assemble.sh no longer runs myloasm_split.awk — the tested split is not the "
        "one production uses."
    )
    assert "assembly_primary.fa" in code, (
        "assemble.sh no longer reads myloasm's assembly_primary.fa. Circularity is "
        "in that file's headers; myloasm's GFA carries no circularity marker."
    )


def test_myloasm_branch_does_not_reuse_the_hifiasm_gfa_rule() -> None:
    """The hifiasm segment-name regex must not be applied to myloasm output.

    It would match nothing there, so circular.fa would come out empty on every
    myloasm run with no error — the silent failure this whole change exists to
    avoid. Assert the regex appears only inside the hifiasm_meta branch.
    """
    body = _ASSEMBLE_SH.read_text()
    myloasm_branch = body.split("myloasm)", 1)
    assert len(myloasm_branch) == 2, "assemble.sh no longer has a `myloasm)` case arm"
    # Cut at the arm terminator so this looks at the myloasm branch alone.
    arm = myloasm_branch[1].split(";;", 1)[0]
    assert "tg[0-9]+c$" not in arm, (
        "the hifiasm-meta GFA segment-name regex appears in the myloasm branch. It "
        "matches no myloasm contig, so every circular genome would be silently "
        "routed to binning."
    )


def test_image_pins_myloasm_and_asserts_the_pin_at_build_time() -> None:
    """myloasm is version-pinned in the def AND the pin is checked in %test.

    The pin is enforced only by the solver, and nothing about it is observable from
    this repo. Since the circularity contract is a header STRING probed against one
    version, a drifted solve must fail the BUILD — a test here can read the spec but
    not the built image.
    """
    defsrc = _ASSEMBLE_DEF.read_text()
    create = next(
        (ln for ln in defsrc.splitlines() if "micromamba create" in ln and "myloasm" in ln),
        None,
    )
    assert create is not None, "assemble.def no longer installs myloasm into the assemble env"
    pin = re.search(r"\bmyloasm=([0-9][^\s\"']*)", create)
    assert pin is not None, (
        f"myloasm is unpinned on assemble.def's create line: {create!r}. It comes "
        "from bioconda, so an unpinned spec resolves fresh on every rebuild and can "
        "move off the probed header format with no change to this repo."
    )
    version = pin.group(1)
    assert re.search(rf"myloasm --version[^\n]*{re.escape(version)}", defsrc), (
        f"assemble.def pins myloasm={version} but its %test never asserts that "
        "version, so a drifted solve would build and ship green."
    )


def test_build_spec_hashes_the_splitter_and_verifies_the_myloasm_version() -> None:
    """The SIF spec rebuilds on a splitter edit and verifies the pinned version.

    HASH_INPUTS scopes the two-gate idempotency check. myloasm_split.awk is
    %files-copied into the image and decides which contigs become LCGs, so omitting
    it would let an edited splitter be skipped as "unchanged" and never reach the
    host.
    """
    spec = _ASSEMBLE_ENV.read_text()
    hash_inputs = re.search(r'^HASH_INPUTS="([^"]*)"', spec, re.MULTILINE)
    assert hash_inputs is not None, "assemble.env no longer declares HASH_INPUTS"
    assert "myloasm_split.awk" in hash_inputs.group(1), (
        f"myloasm_split.awk is missing from HASH_INPUTS ({hash_inputs.group(1)!r}). "
        "Editing the splitter would not rebuild the image."
    )
    verify_match = re.search(r'^VERIFY_MATCH="([^"]*)"', spec, re.MULTILINE)
    assert verify_match is not None, "assemble.env no longer declares VERIFY_MATCH"
    create = next(ln for ln in _ASSEMBLE_DEF.read_text().splitlines() if "micromamba create" in ln)
    version = re.search(r"\bmyloasm=([0-9][^\s\"']*)", create).group(1)
    assert version in verify_match.group(1), (
        f"VERIFY_MATCH ({verify_match.group(1)!r}) does not assert the pinned "
        f"myloasm version {version}. The def and the spec would drift apart."
    )
