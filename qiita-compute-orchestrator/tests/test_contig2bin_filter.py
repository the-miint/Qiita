"""Pin which rows of a binner's contig2bin table reach DAS_Tool.

Why this exists
---------------
`bin_refine.sh` hands DAS_Tool one contig->bin table per binner, built by piping
`Fasta_to_Contig2Bin.sh` through `contig2bin_filter.awk`. Two things about that
table decide what the consensus can select, and both were wrong: the bin id was
read from a column the table does not have, so every metabat2 row named the empty
bin; and each binner's catch-all file — the contigs it did NOT place — was passed
on as a candidate bin. `contig2bin_filter.awk`'s header carries the measurements.

Neither is visible in a green run: the first shows up as a binner that quietly
contributes nothing, the second as a candidate DAS_Tool happens to reject. So the
filter is pinned by EXECUTION here, against the program the image ships.

One caveat, the same one `test_assemble_hifiasm_awk.py` carries: these run the HOST
awk. The image's is gawk — `/opt/conda/bin/awk` is a symlink to `gawk` and
`/opt/conda/bin` leads `PATH`, read off the shipped
`long-read-assembly-dastool-1.0.0.sif` — and the program's output was compared
byte-for-byte across gawk, mawk, busybox awk, BWK awk and macOS's BSD awk before
these were written. What they pin is the program's logic, not that agreement.

Fixtures use the shapes measured on the deploy host against that SIF: real bins are
`bin.<N>` in all three binners' dirs, numbered from 0; metabat2's catch-alls are
`bin.unbinned` / `bin.tooShort` / `bin.lowDepth`, concoct's is `unbinned`, maxbin2
has none.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOW_DIR = _REPO_ROOT / "workflows" / "long-read-assembly"
_FILTER_AWK = _WORKFLOW_DIR / "contig2bin_filter.awk"
_BIN_REFINE_SH = _WORKFLOW_DIR / "bin_refine.sh"
_BIN_REFINE_DEF = _WORKFLOW_DIR / "bin_refine.def"
_BIN_REFINE_ENV = _WORKFLOW_DIR / "sif-build.d" / "bin_refine.env"

# The catch-all a binner writes for the contigs it did not place, by binner.
_METABAT2_CATCH_ALLS = ("bin.unbinned", "bin.tooShort", "bin.lowDepth")
_CONCOCT_CATCH_ALL = "unbinned"

pytestmark = pytest.mark.skipif(shutil.which("awk") is None, reason="awk not available")


def _run(rows: list[tuple[str, ...]], tmp_path: Path) -> tuple[list[str], list[str]]:
    """Run the shipped filter over `rows`; return (kept lines, rejected lines)."""
    stdin = "".join("\t".join(row) + "\n" for row in rows)
    rejects = tmp_path / "rejects"
    proc = subprocess.run(
        ["awk", "-v", f"rejects={rejects}", "-f", str(_FILTER_AWK)],
        input=stdin,
        capture_output=True,
        text=True,
        check=True,
    )
    rejected = rejects.read_text().splitlines() if rejects.exists() else []
    return proc.stdout.splitlines(), rejected


def test_a_numbered_bin_passes_through_carrying_its_id(tmp_path: Path) -> None:
    """The kept row is the input row: contig in column 1, bin id in column 2.

    This is the `$4` regression stated as behaviour. Reading the id from a column
    the table does not have does not fail — it yields a row whose bin id is the
    empty string, which is a valid-looking table naming one bin.
    """
    kept, rejected = _run([("u38040ctg", "bin.1")], tmp_path)

    assert kept == ["u38040ctg\tbin.1"]
    assert rejected == []


@pytest.mark.parametrize("bin_id", ["bin.0", "bin.1", "bin.28", "bin.111"])
def test_every_numbered_bin_is_a_bin(bin_id: str, tmp_path: Path) -> None:
    """`bin.0` included: metaWRAP's concoct and maxbin2 dirs both start at zero."""
    kept, rejected = _run([("c1", bin_id)], tmp_path)

    assert kept == [f"c1\t{bin_id}"]
    assert rejected == []


@pytest.mark.parametrize("catch_all", [*_METABAT2_CATCH_ALLS, _CONCOCT_CATCH_ALL])
def test_a_binner_catch_all_reaches_neither_das_tool_nor_the_rejects(
    catch_all: str, tmp_path: Path
) -> None:
    """Dropped, not rejected: an unplaced-contig file is an expected output.

    Both spellings are covered — metabat2 prefixes its catch-alls `bin.` and
    concoct does not, so a rule written for one misses the other.
    """
    kept, rejected = _run([("c1", catch_all)], tmp_path)

    assert kept == []
    assert rejected == []


@pytest.mark.parametrize("bin_id", ["bin.weird", "Bin.1", "bin.1a", "1", "", "bin."])
def test_a_bin_id_in_neither_shape_is_rejected(bin_id: str, tmp_path: Path) -> None:
    """Rejected, so bin_refine.sh can fail the step rather than guess.

    Keeping it would put an unrecognized catch-all back in front of DAS_Tool;
    dropping it would take a real bin out of the consensus. The empty id is the
    `$4` outcome, and it must not read as a bin.
    """
    kept, rejected = _run([("c1", bin_id)], tmp_path)

    assert kept == []
    assert rejected == [f"c1\t{bin_id}"]


@pytest.mark.parametrize("row", [("c1",), ("c1", "bin.1", "extra"), ("c1", "unbinned", "extra")])
def test_a_row_that_is_not_two_fields_is_rejected(row: tuple[str, ...], tmp_path: Path) -> None:
    """The two-column shape is what makes column 2 the bin id, so it is asserted.

    A three-field row is rejected even when its second field would have passed:
    the widths cannot both be right, and which column holds the id is the thing
    that was wrong.
    """
    kept, rejected = _run([row], tmp_path)

    assert kept == []
    assert rejected == ["\t".join(row)]


def test_a_mixed_table_keeps_only_the_bins(tmp_path: Path) -> None:
    """A metabat2-shaped table: numbered bins, plus the three catch-alls."""
    rows = [
        ("c1", "bin.1"),
        ("c2", "bin.1"),
        ("c3", "bin.2"),
        ("c4", "bin.unbinned"),
        ("c5", "bin.lowDepth"),
        ("c6", "bin.tooShort"),
    ]

    kept, rejected = _run(rows, tmp_path)

    assert kept == ["c1\tbin.1", "c2\tbin.1", "c3\tbin.2"]
    assert rejected == []


def test_the_filter_refuses_to_run_without_a_rejects_path(tmp_path: Path) -> None:
    """Without it a rejected row would be silently discarded — the failure mode
    the rejects file exists to end."""
    proc = subprocess.run(
        ["awk", "-f", str(_FILTER_AWK)],
        input="c1\tbin.weird\n",
        capture_output=True,
        text=True,
    )

    # The exact code, like the sibling `assemble.sh` awk test: `_lib.sh` uses 64 for
    # a usage error, and pinning it separates this guard firing from awk dying for
    # some other reason.
    assert proc.returncode == 64, proc.stdout
    assert "rejects" in proc.stderr


def test_every_binner_goes_through_the_filter(tmp_path: Path) -> None:
    """One `Fasta_to_Contig2Bin.sh` call, piped into the filter, for all three.

    The per-binner branch is what let metabat2 carry a projection nobody
    re-measured while concoct and maxbin2 used the raw output. With one path a
    column change is one edit, and the table DAS_Tool reads is built the same way
    whichever binner produced it.
    """
    code = [
        ln
        for ln in _BIN_REFINE_SH.read_text().splitlines()
        if ln.strip() and not ln.lstrip().startswith("#")
    ]
    # `-i <dir>`: the invocation, not the error message that names the script.
    calls = [ln for ln in code if "Fasta_to_Contig2Bin.sh -i" in ln]

    assert len(calls) == 1, f"bin_refine.sh builds the contig2bin table {len(calls)} ways: {calls}"
    piped = "\n".join(code)
    assert re.search(
        r"Fasta_to_Contig2Bin\.sh[^\n]*\\\n\s*\|\s*awk[^\n]*-f /opt/qiita/contig2bin_filter\.awk",
        piped,
    ), "bin_refine.sh no longer pipes Fasta_to_Contig2Bin.sh through contig2bin_filter.awk"


def test_bin_refine_fails_the_step_on_a_rejected_row() -> None:
    """A non-empty rejects file must stop the step, not just print.

    Continuing would hand DAS_Tool a table missing the rejected contigs with
    nothing in the output to say so.
    """
    body = _BIN_REFINE_SH.read_text()
    guard = re.search(r'if \[\[ -s "\$\{rejects\}" \]\]; then(.+?)\n    fi', body, re.DOTALL)

    assert guard is not None, "bin_refine.sh no longer guards on the rejects file"
    assert re.search(r"^\s*exit [1-9]", guard.group(1), re.MULTILINE), (
        f"the rejects guard does not fail the step: {guard.group(1)!r}"
    )


def test_the_filter_ships_in_the_image_and_scopes_its_rebuild() -> None:
    """%files-copied and named in HASH_INPUTS.

    Omitting it from HASH_INPUTS leaves the two-gate idempotency check green on
    an edited filter, so the deploy would keep serving the old SIF while the repo
    says the bug is fixed.
    """
    def_body = _BIN_REFINE_DEF.read_text()
    assert "contig2bin_filter.awk /opt/qiita/contig2bin_filter.awk" in def_body, (
        "bin_refine.def no longer %files-copies contig2bin_filter.awk"
    )

    hash_inputs = re.search(r'^HASH_INPUTS="([^"]*)"', _BIN_REFINE_ENV.read_text(), re.MULTILINE)
    assert hash_inputs is not None, "bin_refine.env no longer declares HASH_INPUTS"
    assert "contig2bin_filter.awk" in hash_inputs.group(1).split(), (
        f"contig2bin_filter.awk is missing from HASH_INPUTS ({hash_inputs.group(1)!r}); "
        "editing it would not rebuild the SIF."
    )


def test_a_contig_id_holding_spaces_still_reaches_das_tool(tmp_path: Path) -> None:
    """The separator is a tab, and the contig field is a whole FASTA header.

    `Fasta_to_Contig2Bin.sh` is `grep ">" | perl -pe "s/\\n/\\t$binname\\n/g" |
    perl -pe "s/>//g"` — read out of the shipped SIF — so it emits the entire header
    line, not its first token. Under awk's default whitespace `FS` this row is five
    fields, `NF == 2` fails, and a legitimate bin is rejected: the step dies with
    exit 65 on data that is fine. Without this fixture nothing distinguishes the
    shipped program from one carrying no `FS` at all — every other row here is
    single-token, where whitespace and tab splitting agree.
    """
    kept, rejected = _run([("ctg2 length=500 depth=3.1", "bin.2")], tmp_path)

    assert kept == ["ctg2 length=500 depth=3.1\tbin.2"]
    assert rejected == []
