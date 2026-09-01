"""Execution tests for the hifiasm_meta arm's GFA -> attribute-sidecar awk.

The myloasm arm is a Python program with its own harness
(`test_myloasm_split.py`); the hifiasm arm is an awk program embedded in
`assemble.sh`, and the static pins in `test_long_read_assembly_entrypoint_pins.py`
only assert that its source text is present and shaped right. String equality on
a program's source passes through any change that preserves the characters, so
the four things this awk actually decides are pinned here by RUNNING it:

  * the circular/linear call from the segment name,
  * `dp:f` found by TAG rather than by column position,
  * a segment with no `dp:f` writing an empty field and being REPORTED,
  * a name matching neither shape exiting 65 instead of being routed,
  * and a GFA where NO segment carries `dp:f` failing rather than storing a
    depth-less run.

The GFA fixtures reproduce the layout probed on the pinned build (hamtv0.3.5),
whose `p_ctg.gfa` writes one S-line per contig as

    S  s0.ctg000001c  <seq>  LN:i:60000  dp:f:98  ts:B:I,0

Both the awk program and the two name patterns are extracted from the shipped
`assemble.sh` rather than copied, so neither can drift from what the container
runs while these tests pass.

One caveat: these run the HOST awk, not the image's gawk. They pin the program's
logic, not a difference between awk implementations.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
from qiita_common.assembly_constants import CONTIG_ATTRIBUTE_COLUMNS

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ASSEMBLE_SH = _REPO_ROOT / "workflows" / "long-read-assembly" / "assemble.sh"

_STRICT_CIRC = "tg[0-9]+c" + "$"
_STRICT_LIN = "tg[0-9]+l" + "$"


def _grammar(var: str) -> str:
    """The `CIRC_RE` / `LIN_RE` value assemble.sh defines once and shares between
    the attribute pass and both FASTA writers.

    Read out of the script rather than restated here: a second copy would let the
    shipped grammar loosen while these tests went on exercising the strict one.
    """
    text = _ASSEMBLE_SH.read_text()
    marker = f"{var}='"
    start = text.index(marker) + len(marker)
    return text[start : text.index("'", start)]


def _awk_program() -> str:
    """The attribute-pass awk body, lifted verbatim from assemble.sh.

    Delimited by the `awk -v attrs=` invocation and the closing quote before the
    GFA argument — the same single-quoted literal the shell hands awk.
    """
    text = _ASSEMBLE_SH.read_text()
    start = text.index("awk -v attrs=")
    open_quote = text.index("'", start)
    close_quote = text.index("'", open_quote + 1)
    program = text[open_quote + 1 : close_quote]
    assert "dp:f:" in program, "extracted the wrong span from assemble.sh"
    return program


def _run(tmp_path: Path, gfa_text: str):
    gfa = tmp_path / "asm.p_ctg.gfa"
    gfa.write_text(gfa_text)
    attrs = tmp_path / "contig_attributes.tsv"
    result = subprocess.run(
        [
            "awk",
            "-v",
            f"attrs={attrs}",
            "-v",
            f"circ={_grammar('CIRC_RE')}",
            "-v",
            f"lin={_grammar('LIN_RE')}",
            _awk_program(),
            str(gfa),
        ],
        capture_output=True,
        text=True,
    )
    return result, attrs


def _rows(attrs: Path) -> list[list[str]]:
    return [line.split("\t") for line in attrs.read_text().splitlines()]


pytestmark = pytest.mark.skipif(shutil.which("awk") is None, reason="awk not available")


def test_assemble_sh_is_present() -> None:
    """Anti-vacuity guard: every test here extracts its program from this file."""
    assert _ASSEMBLE_SH.is_file()
    assert _awk_program()


def test_the_shipped_name_grammar_is_anchored_to_the_unitig_prefix() -> None:
    """Both patterns require `tg<N>` before the trailing letter.

    A bare trailing-letter match would call any name ending in 'c' circular,
    which bypasses binning and stores the contig as a closed genome. The tests
    below read these same two values out of the script, so this is what keeps
    them exercising the strict grammar rather than whatever it currently says.
    """
    assert _grammar("CIRC_RE") == _STRICT_CIRC
    assert _grammar("LIN_RE") == _STRICT_LIN


def test_the_awk_writes_one_row_per_segment_with_the_declared_columns(tmp_path: Path) -> None:
    """Circularity from the name; `dp:f` read by tag, at two different column
    positions, with the tag-less segment writing an empty depth and being counted
    onto stderr.

    The positions are the point: GFA does not fix the order of a segment's
    optional tags, so a reader keyed on `$5` would store `ts:B:I`'s value as
    depth on the second line here and nothing downstream could notice — a depth
    is a plausible number whatever it came from.

    The stderr count is what keeps a partial absence out of the data alone: the
    step tolerates it (see the no-tag-anywhere case below for why), so the log is
    the only place it announces itself while it is still cheap to look into.
    """
    result, attrs = _run(
        tmp_path,
        "H\tVN:Z:1.0\n"
        # dp:f in position 5
        "S\ts0.ctg000001c\tACGT\tLN:i:200000\tdp:f:29\tts:B:I,0\n"
        # dp:f LAST, after two other tags
        "S\ts1.utg000002l\tTTTT\tLN:i:5000\tts:B:I,1\tdp:f:3.5\n"
        # no dp:f at all
        "S\ts2.ctg000003c\tCCCC\tLN:i:9000\tts:B:I,2\n"
        # a non-S line must be ignored entirely
        "L\ts0.ctg000001c\t+\ts1.utg000002l\t+\t0M\n",
    )
    assert result.returncode == 0, result.stderr
    rows = _rows(attrs)
    assert rows[0] == list(CONTIG_ATTRIBUTE_COLUMNS)
    assert rows[1:] == [
        ["s0.ctg000001c", "s0.ctg000001c", "yes", "29", ""],
        ["s1.utg000002l", "s1.utg000002l", "no", "3.5", ""],
        # empty depth, not a zero: nothing was reported.
        ["s2.ctg000003c", "s2.ctg000003c", "yes", "", ""],
    ]
    assert "1 of 3 GFA segment(s) carried no dp:f tag" in result.stderr


def test_an_unrecognised_segment_name_exits_65_and_names_it(tmp_path: Path) -> None:
    """A name matching neither shape stops the step.

    The circularity call is stored per contig, so routing such a name to noLCG
    would also write a `no` into the lake for a contig nothing classified.
    """
    result, _ = _run(
        tmp_path,
        "S\ts0.ctg000001c\tACGT\tdp:f:29\nS\tweird_name_x\tACGT\tdp:f:1\n",
    )
    assert result.returncode == 65, result.stderr
    assert "weird_name_x" in result.stderr


def test_a_bare_trailing_c_is_not_treated_as_circular(tmp_path: Path) -> None:
    """`s0.scaffold_arc` ends in 'c' but carries no `tg<N>` prefix.

    Under a loosened grammar it would be called circular and stored as a closed
    genome; under the shipped one it matches neither shape and fails the step.
    """
    result, _ = _run(tmp_path, "S\ts0.scaffold_arc\tACGT\tdp:f:9\n")
    assert result.returncode == 65, result.stderr
    assert "s0.scaffold_arc" in result.stderr


def test_a_gfa_with_no_dp_tag_anywhere_fails_rather_than_storing_no_depth(
    tmp_path: Path,
) -> None:
    """None carrying `dp:f` is the shape a renamed or moved tag produces, and the
    only one this pass can tell apart from data.

    Left fail-open, `depth` would be NULL for every contig of every hifiasm run
    and read downstream as "not recorded", which is the same silent all-NULL
    outcome `myloasm_split.py` refuses for its own depth field. SOME segments
    lacking the tag is not that shape and must still succeed (the case above) --
    not because the reach of the tag is in doubt, which `assemble.sh` records a
    real-assembly measurement for, but because such a row stays readable after
    the fact: a NULL depth beside a non-NULL raw_name means the assembler
    reported on that contig without a depth, where an absent sidecar leaves all
    four NULL.
    """
    result, _ = _run(
        tmp_path,
        "S\ts0.ctg000001c\tACGT\tLN:i:200000\tts:B:I,0\n"
        "S\ts1.utg000002l\tTTTT\tLN:i:5000\tts:B:I,1\n",
    )
    assert result.returncode == 65, result.stderr
    assert "dp:f" in result.stderr
