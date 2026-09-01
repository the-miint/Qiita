"""Pin how the `checkm` step turns circular.fa into the genomes CheckM scores.

Why this exists
---------------
`checkm lineage_wf` scores a DIRECTORY of FASTA files, one genome per file, and
keys its output on the filename stem. The assemble step publishes every circular
contig in one multi-FASTA. Handed that directly, CheckM would report a single
genome stitched from every LCG in the sample — a green run whose completeness and
contamination describe nothing, and whose one `"Bin Id"` joins no membership row.
So the split is pinned by EXECUTION here, not by spelling.

The stem is the join key
------------------------
assembly_load reads CheckM's `"Bin Id"` straight into `bin_quality.bin_id`, and for
an LCG `assembly_membership.bin_id` is the contig id itself. The two meet only if
the stem is the contig id byte-for-byte, so the tests below assert the FILENAMES,
not just the record count.

CheckM strips one extension: measured on the deploy host, where the refined bin
`CONCOCT_bin.13_sub.fa` came back as `"Bin Id" = CONCOCT_bin.13_sub`. A hifiasm
contig id carries a dot (`s0.ctg000001c`), which is why that case is a fixture here
rather than a remark.

Like the other real-miint smokes in this tree these EXECUTE the program against the
extension this component's conftest stages, and carry no offline skip — a missing
extension fails the session.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOW_DIR = _REPO_ROOT / "workflows" / "long-read-assembly"
_SPLIT_PY = _WORKFLOW_DIR / "lcg_split.py"
_MIINT_CONNECT_PY = _WORKFLOW_DIR / "miint_connect.py"
_CHECKM_SH = _WORKFLOW_DIR / "checkm.sh"
_CHECKM_DEF = _WORKFLOW_DIR / "checkm.def"
_CHECKM_ENV = _WORKFLOW_DIR / "sif-build.d" / "checkm.env"
_WORKFLOW_YAML = _WORKFLOW_DIR / "1.0.0.yaml"

_EXIT_CONTRACT_VIOLATION = 64

# Imported, not retyped: these four basenames are the producer/consumer contract
# between checkm.sh and assembly_load, so a rename must not be able to leave this
# pin green.
from qiita_compute_orchestrator.jobs.assembly_load import (  # noqa: E402
    _CHECKM_LCG_LINEAGE_TSV,
    _CHECKM_LCG_QA_TSV,
    _CHECKM_LINEAGE_TSV,
    _CHECKM_QA_TSV,
)

# Real contig ids from the two assemblers, and both are load-bearing shapes: the
# hifiasm one carries a DOT, which is what makes "CheckM strips one extension"
# something to hold rather than assume.
_HIFIASM_ID = "s0.ctg000001c"
_MYLOASM_ID = "u713ctg"


@pytest.fixture(scope="module")
def staged_miint() -> str:
    """The extension directory this component's conftest already staged.

    Same fixture and same reasoning as `test_myloasm_split.py`: a directory somebody
    else staged, which the split only ever LOADs, which is the production posture.
    """
    ext_dir = os.environ.get("MIINT_EXTENSION_DIRECTORY")
    assert ext_dir, (
        "MIINT_EXTENSION_DIRECTORY is unset — conftest's setup_miint_test_env() "
        "should have set it. Without it these tests would silently exercise a "
        "different extension resolution than production."
    )
    return ext_dir


def _split(tmp_path: Path, fasta: str, staged: str | None) -> subprocess.CompletedProcess[str]:
    """Run the splitter the way checkm.sh does."""
    circular = tmp_path / "circular.fa"
    circular.write_text(fasta)
    env = dict(os.environ)
    if staged is None:
        env.pop("MIINT_EXTENSION_DIRECTORY", None)
    else:
        env["MIINT_EXTENSION_DIRECTORY"] = staged
    return subprocess.run(
        [sys.executable, str(_SPLIT_PY), str(circular), str(tmp_path / "lcg")],
        capture_output=True,
        text=True,
        env=env,
    )


def test_split_program_is_present() -> None:
    """Anti-vacuity guard: every test below shells out to this file."""
    assert _SPLIT_PY.is_file(), f"{_SPLIT_PY} is missing — the pins below are vacuous"


def test_each_contig_becomes_one_file_named_for_its_id(tmp_path, staged_miint) -> None:
    """One FASTA per record, stem == contig id, bytes preserved.

    The dotted hifiasm id is the case that matters: a splitter that took
    `Path(name).stem` of its own output, or that sanitized the id, would produce
    `s0.fa` here and the quality row would join nothing.
    """
    result = _split(
        tmp_path,
        f">{_HIFIASM_ID}\nACGTACGTAC\n>{_MYLOASM_ID}\nTTTTGGGGCC\n",
        staged_miint,
    )
    assert result.returncode == 0, result.stderr

    out = tmp_path / "lcg"
    assert sorted(p.name for p in out.glob("*")) == [
        f"{_HIFIASM_ID}.fa",
        f"{_MYLOASM_ID}.fa",
    ]
    # Sequence bytes come back through miint's FASTA writer, so assert them rather
    # than only the framing: a split that dropped or reordered records would still
    # produce two correctly-named files.
    assert (out / f"{_HIFIASM_ID}.fa").read_text().split("\n")[1] == "ACGTACGTAC"
    assert (out / f"{_MYLOASM_ID}.fa").read_text().split("\n")[1] == "TTTTGGGGCC"
    assert _read_ids(out / f"{_HIFIASM_ID}.fa") == [_HIFIASM_ID]


def _read_ids(path: Path) -> list[str]:
    return [ln[1:].split()[0] for ln in path.read_text().splitlines() if ln.startswith(">")]


def test_one_contig_is_one_genome_not_one_directory(tmp_path, staged_miint) -> None:
    """The control for the test above: the whole point is that N records do not
    collapse into one file. A single multi-FASTA in the output would be the failure
    mode CheckM cannot see, since it would score it green as one genome."""
    fasta = "".join(f">ctg{i}\nACGT\n" for i in range(5))
    assert _split(tmp_path, fasta, staged_miint).returncode == 0
    assert len(list((tmp_path / "lcg").glob("*.fa"))) == 5


@pytest.mark.parametrize(
    ("fasta", "expected"),
    [
        (">dup\nACGT\n>dup\nTTTT\n", "duplicate contig id"),
        (">has/slash\nACGT\n", "cannot be used as a filename stem"),
        (">-leading-dash\nACGT\n", "cannot be used as a filename stem"),
        (f">{'x' * 253}\nACGT\n", "cannot be used as a filename stem"),
    ],
    ids=["duplicate-id", "path-separator", "leading-dash", "over-name-max"],
)
def test_an_unusable_id_stops_the_step(tmp_path, staged_miint, fasta, expected) -> None:
    """Rejected, never sanitized or overwritten.

    A duplicate would have the second COPY overwrite the first, so CheckM would
    score one genome where the lake holds two memberships. A rewritten stem would
    come back from CheckM as a bin_id that joins nothing. Both are silent
    downstream, so they stop the step here.
    """
    result = _split(tmp_path, fasta, staged_miint)
    assert result.returncode == _EXIT_CONTRACT_VIOLATION, result.stdout
    assert expected in result.stderr


def test_without_the_staged_extension_the_split_fails(tmp_path) -> None:
    """No MIINT_EXTENSION_DIRECTORY must fail, never fall back to INSTALL.

    A per-job INSTALL is the footgun the deploy-staged directory replaced: it needs
    the mirror reachable from every compute node and a writable $HOME.
    """
    result = _split(tmp_path, f">{_MYLOASM_ID}\nACGT\n", None)
    assert result.returncode == _EXIT_CONTRACT_VIOLATION
    assert "MIINT_EXTENSION_DIRECTORY" in result.stderr


def test_split_uses_miint_and_never_installs() -> None:
    """Reads/writes through miint, connects through the shared module, LOAD-only.

    Hand-rolling a FASTA reader/writer where miint has one is the repo's named
    review smell. The connect config lives in `miint_connect.py` because assemble.def
    copies the same file for `myloasm_split.py`; a second copy here would be a second
    set of staged-extension settings, free to drift from what the services run.
    """
    code = _SPLIT_PY.read_text()
    # Anchored at a CALL, not a mention: this file's own docstring names both symbols,
    # so a bare substring search stays green on a hand-rolled parser whose prose still
    # credits miint — which is the substitution these two assertions exist to catch.
    assert re.search(r"FROM read_fastx\(", code), (
        "the split no longer reads with miint's read_fastx"
    )
    assert re.search(r"\(FORMAT FASTA\)\"", code), (
        "the split no longer writes with miint's FASTA writer"
    )
    assert "from miint_connect import" in code, (
        "the split no longer connects through miint_connect, so the staged-miint "
        "LOAD asserted in test_myloasm_split is not the connection it opens"
    )
    assert "duckdb.connect" not in code, (
        "the split opens its own DuckDB connection beside miint_connect.connect"
    )
    # Anchored at a string-literal opening quote so the word INSTALL in the prose
    # explaining why we don't install cannot satisfy — or break — this.
    assert re.search(r"""["']\s*INSTALL\b""", code) is None, (
        "the split issues an INSTALL statement. Service-side connects are LOAD-only."
    )


def test_checkm_entrypoint_runs_the_split_and_scores_the_classes_apart() -> None:
    """checkm.sh must split circular.fa and score it in its OWN CheckM run.

    Merged into the refined-bin run, a row's `kind` would have to be recovered from
    its stem — and assembly_load reads the two table pairs by filename precisely so
    no stem is ever parsed.
    """
    sh = _CHECKM_SH.read_text()
    assert "python3 /opt/qiita/lcg_split.py" in sh, (
        "checkm.sh no longer runs the splitter, so lineage_wf would read the whole "
        "circular multi-FASTA as one genome"
    )
    # Read off the run_checkm INVOCATIONS, not the file: checkm.sh's header comment
    # lists all four basenames, so searching the whole file passes with both calls
    # deleted. Two calls is the assertion — one would mean a class went unscored.
    calls = [ln.strip() for ln in sh.splitlines() if ln.strip().startswith("run_checkm ")]
    assert len(calls) == 2, calls
    invoked = " ".join(calls)
    for name in (
        _CHECKM_LINEAGE_TSV,
        _CHECKM_QA_TSV,
        _CHECKM_LCG_LINEAGE_TSV,
        _CHECKM_LCG_QA_TSV,
    ):
        assert name in invoked, (
            f"checkm.sh no longer writes {name}, which assembly_load reads by that name"
        )


def test_checkm_step_binds_miint_and_reads_the_genomes_dir() -> None:
    """The YAML must give the step what the splitter needs.

    Without `genomes_dir` the container never sees circular.fa (a container step is
    bind-mounted per declared input, so an undeclared directory is not visible at
    all); without MIINT_EXTENSION_DIRECTORY the splitter has no extension to LOAD.
    Either way the LCG arm dies at run time, after CheckM has scored the MAGs.
    """
    action = yaml.safe_load(_WORKFLOW_YAML.read_text())
    step = next(s for s in action["steps"] if s.get("step") == "checkm")
    assert "genomes_dir" in step["inputs"], step["inputs"]
    assert "refined_bins_dir" in step["inputs"], step["inputs"]
    assert step["derived_inputs"]["MIINT_EXTENSION_DIRECTORY"] == "duckdb-ext"
    assert step["derived_inputs"]["QIITA_CHECKM_DB"] == "checkm_data"


def test_checkm_image_ships_the_split_and_rebuilds_on_its_edit() -> None:
    """The def %files-copies both modules and the spec hashes them.

    HASH_INPUTS scopes the two-gate idempotency check: omitting a %files-copied file
    lets an edit to it be skipped as "unchanged" and never reach the host.
    """
    defsrc = _CHECKM_DEF.read_text()
    for name in ("lcg_split.py", "miint_connect.py"):
        assert f"{name} /opt/qiita/{name}" in defsrc, f"checkm.def no longer copies {name}"
    hash_inputs = re.search(r'^HASH_INPUTS="([^"]*)"', _CHECKM_ENV.read_text(), re.MULTILINE)
    assert hash_inputs is not None, "checkm.env no longer declares HASH_INPUTS"
    for name in ("checkm.sh", "lcg_split.py", "miint_connect.py"):
        assert name in hash_inputs.group(1), (
            f"{name} is missing from HASH_INPUTS ({hash_inputs.group(1)!r}); editing "
            "it would not rebuild the image"
        )
