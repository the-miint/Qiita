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
the `circular-` field does track real circularity. `circular-possibly` was not reproduced by
starving depth (runs at 1-3x still reported `circular-no`), but it does occur:
six contigs across one sample's real masked reads carried it. It is accepted as
a well-formed value and routed to noLCG, which is both the safe direction and
what the assay owner does by hand.

Three further behaviours were probed and are relied on below:
  * myloasm is DETERMINISTIC — two runs over the same reads gave byte-identical
    headers.
  * Across two read samplings of the SAME genome, `depth-` moved (32→31) and the
    unitig id moved (u713ctg→u932ctg) while `_len-` was identical (580076).
  * With nothing assemblable (3 reads) myloasm EXITS NON-ZERO and writes no
    `assembly_primary.fa` at all — which is why assemble.sh treats a MISSING
    primary FASTA after a zero exit as a contract violation, not an empty
    assembly.

The behavioural tests here EXECUTE `myloasm_split.py` against the real miint
extension this component's conftest stages, so they prove the split works rather
than that it is still spelled a certain way. Like the other real-miint smokes in
this tree they carry no offline skip — a missing extension fails the session. The
static pins below cover the wiring that cannot be executed without building the
image.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOW_DIR = _REPO_ROOT / "workflows" / "long-read-assembly"
_SPLIT_PY = _WORKFLOW_DIR / "myloasm_split.py"
_ASSEMBLE_SH = _WORKFLOW_DIR / "assemble.sh"
_ASSEMBLE_DEF = _WORKFLOW_DIR / "assemble.def"
_ASSEMBLE_ENV = _WORKFLOW_DIR / "sif-build.d" / "assemble.env"
_WORKFLOW_YAML = _WORKFLOW_DIR / "1.0.0.yaml"
_CO_LOCK = _REPO_ROOT / "qiita-compute-orchestrator" / "uv.lock"

# The two headers verbatim from the probe described in the module docstring. Kept
# byte-exact (trailing `mult=1.00` included) so a myloasm release that changes the
# header shape breaks these tests rather than the production step.
_CIRC_HEADER = ">u713ctg_len-580076_circular-yes_depth-32-32-32_duplicated-no mult=1.00"
_LIN_HEADER = ">u278ctg_len-577882_circular-no_depth-33-33-33_duplicated-no mult=1.00"

_EXIT_CONTRACT_VIOLATION = 64


@pytest.fixture(scope="module")
def staged_miint() -> str:
    """The extension directory this component's conftest already staged.

    Deliberately NOT a second INSTALL into a fresh tmpdir: `conftest.py`'s
    session-scoped `_stage_miint_extension` installs once into the stable
    per-component dir from `setup_miint_test_env("orchestrator")`, cached across
    runs on purpose. Re-downloading here would undo that for every `make test`,
    and would pin the mirror URL in a second place instead of honouring
    `MIINT_EXTENSION_REPO`.

    It is also the right SHAPE for these tests: a directory somebody else staged,
    which the splitter only ever LOADs — exactly the production posture.
    """
    ext_dir = os.environ.get("MIINT_EXTENSION_DIRECTORY")
    assert ext_dir, (
        "MIINT_EXTENSION_DIRECTORY is unset — conftest's setup_miint_test_env() "
        "should have set it. Without it these tests would silently exercise a "
        "different extension resolution than production."
    )
    return ext_dir


def _split(tmp_path: Path, fasta: str, staged: str | None) -> subprocess.CompletedProcess[str]:
    """Run the splitter the way assemble.sh does."""
    primary = tmp_path / "assembly_primary.fa"
    primary.write_text(fasta)
    env = dict(os.environ)
    if staged is None:
        env.pop("MIINT_EXTENSION_DIRECTORY", None)
    else:
        env["MIINT_EXTENSION_DIRECTORY"] = staged
    return subprocess.run(
        [
            sys.executable,
            str(_SPLIT_PY),
            str(primary),
            str(tmp_path / "circular.fa"),
            str(tmp_path / "noLCG.fa"),
            str(tmp_path / "contig_attributes.tsv"),
        ],
        capture_output=True,
        text=True,
        env=env,
    )


def _ids(path: Path) -> list[str]:
    return [ln[1:] for ln in path.read_text().splitlines() if ln.startswith(">")]


def test_split_program_is_present() -> None:
    """Anti-vacuity guard: every test below shells out to this file."""
    assert _SPLIT_PY.is_file(), f"{_SPLIT_PY} is missing — the pins below are vacuous"


def test_real_probe_headers_split_on_circularity(tmp_path: Path, staged_miint: str) -> None:
    """The circular arm lands in circular.fa, the linear control in noLCG.fa.

    This is the probe's own result replayed through our code: the two headers
    differ ONLY in what myloasm concluded about topology, so a splitter that sent
    them the same way would be ignoring the field entirely.
    """
    result = _split(tmp_path, f"{_CIRC_HEADER}\nACGTACGT\n{_LIN_HEADER}\nTTTTGGGG\n", staged_miint)
    assert result.returncode == 0, result.stderr
    assert _ids(tmp_path / "circular.fa") == ["u713ctg"]
    assert _ids(tmp_path / "noLCG.fa") == ["u278ctg"]


def test_sequence_bytes_are_copied_verbatim(tmp_path: Path, staged_miint: str) -> None:
    """The splitter routes records; it must never rewrite sequence.

    These contigs are hashed downstream with the SAME canonical hash a reference
    sequence gets, so a single altered byte would mint a different feature_idx for
    an identical molecule.
    """
    seq = "ACGTACGTACGTACGTACGT"
    result = _split(tmp_path, f"{_CIRC_HEADER}\n{seq}\n", staged_miint)
    assert result.returncode == 0, result.stderr
    body = "".join(
        ln for ln in (tmp_path / "circular.fa").read_text().splitlines() if not ln.startswith(">")
    )
    assert body == seq


def test_line_wrapped_input_is_reassembled(tmp_path: Path, staged_miint: str) -> None:
    """A wrapped input record keeps its full sequence.

    myloasm 0.6.0 was observed to write one line per contig, but read_fastx is the
    parser precisely so a wrapped release needs no change here.
    """
    result = _split(tmp_path, f"{_CIRC_HEADER}\nACGT\nTTGG\nCC\n", staged_miint)
    assert result.returncode == 0, result.stderr
    body = "".join(
        ln for ln in (tmp_path / "circular.fa").read_text().splitlines() if not ln.startswith(">")
    )
    assert body == "ACGTTTGGCC"


def test_circular_possibly_is_not_treated_as_circular(tmp_path: Path, staged_miint: str) -> None:
    """`circular-possibly` goes to noLCG — the recoverable direction.

    A contig wrongly sent to noLCG is still recovered through binning (as a
    single-contig MAG if it bins alone). A contig wrongly called circular bypasses
    binning entirely and is stored as a complete genome that was never closed, so
    the asymmetry decides which way an uncertain call should fall.
    """
    header = ">u9ctg_len-1234_circular-possibly_depth-10-9-9_duplicated-no mult=1.00"
    result = _split(tmp_path, f"{header}\nACGT\n", staged_miint)
    assert result.returncode == 0, result.stderr
    assert _ids(tmp_path / "circular.fa") == []
    assert _ids(tmp_path / "noLCG.fa") == ["u9ctg"]


def test_sidecar_carries_the_depth_mean_and_the_mult_length_gate(
    tmp_path: Path, staged_miint: str
) -> None:
    """The attribute values, not just the file's presence.

    Three records discriminate the two derivations the splitter performs:
      * a >=1 kb contig with a SKEWED depth triple, so the stored value can only
        be the mean (4+5+9)/3 == 6.0 -- a min, max or median would each differ;
      * a <1 kb contig, where myloasm reports `mult=0.00` for absence of signal
        rather than a measured zero, so `mult` must be NULL while `depth` is
        still read;
      * a contig whose `_len-` field DISAGREES with the sequence it carries, to
        pin which of the two drives the gate. Its header says 5000 while the
        sequence is 200 bp, so a gate reading the header would keep its `mult`
        and a gate reading `length(sequence1)` -- the one this code uses --
        nulls it. Nothing else in the file distinguishes them, because myloasm's
        own headers always agree with their sequence.
    """
    long_seq = "ACGT" * 300  # 1200 bp, over the gate
    short_seq = "ACGT" * 50  # 200 bp, under the gate
    fasta = (
        f">u1ctg_len-1200_circular-yes_depth-4-5-9_duplicated-no mult=1.75\n{long_seq}\n"
        f">u2ctg_len-200_circular-no_depth-7-7-7_duplicated-no mult=0.00\n{short_seq}\n"
        f">u3ctg_len-5000_circular-no_depth-2-2-2_duplicated-no mult=3.50\n{short_seq}\n"
    )
    result = _split(tmp_path, fasta, staged_miint)
    assert result.returncode == 0, result.stderr

    rows = (tmp_path / "contig_attributes.tsv").read_text().splitlines()
    assert rows[0] == "contig_id\traw_name\tcircularity\tdepth\tmult"
    parsed = {r.split("\t")[0]: r.split("\t") for r in rows[1:]}
    assert set(parsed) == {"u1ctg", "u2ctg", "u3ctg"}

    assert parsed["u1ctg"][2] == "yes"
    assert float(parsed["u1ctg"][3]) == 6.0, "depth must be the MEAN of the triple"
    assert float(parsed["u1ctg"][4]) == 1.75
    # raw_name is the header's first token, which is what bin_map.contig_id is
    # keyed on before the id is cut.
    assert parsed["u1ctg"][1] == "u1ctg_len-1200_circular-yes_depth-4-5-9_duplicated-no"

    assert float(parsed["u2ctg"][3]) == 7.0
    assert parsed["u2ctg"][4] == "", "mult is NULL below the length gate"

    # Header says 5000, sequence is 200 bp: the gate reads the SEQUENCE, so this
    # is NULL. A gate keyed on the header's `_len-` field would keep 3.50 here.
    assert parsed["u3ctg"][4] == "", "the gate must read length(sequence1), not the _len- field"
    assert float(parsed["u3ctg"][3]) == 2.0, "depth is still read for a short contig"


def test_trailing_header_fields_do_not_reach_the_id(tmp_path: Path, staged_miint: str) -> None:
    """The decoration and the space-separated `mult=…` tail are both stripped.

    read_fastx puts `mult=1.00` in a separate `comment` column, and the `_len-…`
    decoration is cut here — the id that survives becomes the LCG bin_id, and the
    discarded `depth-` field was probed to vary between read samplings of the same
    genome.
    """
    result = _split(tmp_path, f"{_CIRC_HEADER}\nAC\n", staged_miint)
    assert result.returncode == 0, result.stderr
    ids = _ids(tmp_path / "circular.fa")
    assert ids == ["u713ctg"]
    assert not any(" " in i or "mult" in i or "depth" in i for i in ids)


@pytest.mark.parametrize(
    ("case", "fasta", "expected"),
    [
        # A renamed/reordered field set — the exact drift a myloasm upgrade could
        # introduce, and the one that would otherwise pass silently with an empty
        # circular.fa.
        (
            "unknown header shape",
            ">u1ctg_length-10_loop-yes mult=1.00\nACGT\n",
            "do not match the probed myloasm shape",
        ),
        # A fourth circularity value we have never probed must stop the step.
        (
            "unknown circularity value",
            ">u1ctg_len-10_circular-maybe_depth-1-1-1\nACGT\n",
            "do not match the probed myloasm shape",
        ),
        # Two genomes collapsing onto one bin_id downstream. The headers carry a
        # well-formed _depth- field on purpose: without one the all-NULL depth
        # guard fires FIRST and this case silently stops exercising the duplicate
        # check it is named for, which is what the message assertion below pins.
        (
            "duplicate contig id",
            ">x_len-10_circular-yes_depth-1-1-1_d\nAC\n>x_len-99_circular-no_depth-2-2-2_d\nGT\n",
            "duplicate contig id(s)",
        ),
        # Every header parses, but none carries a depth — the grammar moved.
        (
            "no depth on any contig",
            ">u1ctg_len-10_circular-yes_d\nACGT\n",
            "no contig yielded a depth",
        ),
    ],
)
def test_malformed_input_fails_loud(
    tmp_path: Path, staged_miint: str, case: str, fasta: str, expected: str
) -> None:
    """Every shape we cannot interpret exits 64 instead of producing a partial split.

    Silence is the dangerous outcome: an unrecognised header simply fails to match
    the circular pattern, so the step would exit 0 having classified every genome
    as linear. Fail-closed converts that into a step failure an operator sees.

    Each case asserts WHICH guard fired, not just that one did. `_validate` runs
    its checks in order, so a fixture that trips an earlier guard would otherwise
    keep this test green while the guard it is named for went unexercised.
    """
    result = _split(tmp_path, fasta, staged_miint)
    assert result.returncode == _EXIT_CONTRACT_VIOLATION, (
        f"{case}: expected exit {_EXIT_CONTRACT_VIOLATION}, got {result.returncode}. "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert expected in result.stderr, (
        f"{case}: expected the {expected!r} guard to fire; got {result.stderr!r}"
    )


def test_missing_extension_directory_fails_loud(tmp_path: Path) -> None:
    """Without the staged extension the step must fail, never fall back to INSTALL.

    A per-job INSTALL is the footgun the deploy-staged directory replaced: it needs
    the team mirror reachable from every compute node and a writable $HOME. Needs
    no staged extension itself, so it runs even offline.
    """
    result = _split(tmp_path, f"{_CIRC_HEADER}\nAC\n", None)
    assert result.returncode == _EXIT_CONTRACT_VIOLATION, result.stdout + result.stderr
    assert "MIINT_EXTENSION_DIRECTORY" in result.stderr


def test_empty_primary_fasta_fails_rather_than_raising_opaquely(tmp_path: Path) -> None:
    """A zero-byte primary FASTA is refused with our message, not read_fastx's.

    `read_fastx` RAISES on a zero-record input ("Error Empty file: …") instead of
    returning no rows. assemble.sh already skips the splitter in that case; this is
    the second gate, so a direct invocation cannot hit the raw raise either.
    """
    result = _split(tmp_path, "", None)
    assert result.returncode == _EXIT_CONTRACT_VIOLATION
    assert "empty" in result.stderr.lower()


# ---------------------------------------------------------------------------
# Static pins: the wiring that cannot be executed without building the image.
# ---------------------------------------------------------------------------


def test_assemble_sh_runs_myloasm_and_the_splitter() -> None:
    """The myloasm branch invokes the tool in its env and the real splitter file."""
    code = _ASSEMBLE_SH.read_text()
    assert "micromamba run -n myloasm myloasm" in code, (
        "assemble.sh no longer runs myloasm from its own env; the base env has no "
        "such binary and the step would die mid-run."
    )
    assert "python3 /opt/qiita/myloasm_split.py" in code, (
        "assemble.sh no longer runs myloasm_split.py — the tested split is not the "
        "one production uses."
    )
    assert "assembly_primary.fa" in code, (
        "assemble.sh no longer reads myloasm's assembly_primary.fa. Circularity is "
        "in that file's headers; myloasm's GFA carries no circularity marker."
    )


def test_missing_primary_fasta_is_a_hard_failure_not_an_empty_assembly() -> None:
    """assemble.sh exits 64 when myloasm succeeds but wrote no primary FASTA.

    Probed: myloasm exits NON-zero and writes nothing when it cannot assemble. So
    under `set -e` a zero exit with no output file means the filename or layout
    moved. Left fail-open that yields an empty genomes_dir, which assembly_hash
    reports as the terminal StepNoData "assembled nothing" — every sample silently
    discarded, no error, no retry.
    """
    code = _ASSEMBLE_SH.read_text()
    assert re.search(r'if \[\[ ! -e "\$\{PRIMARY\}" \]\]', code), (
        "assemble.sh no longer hard-fails on a MISSING assembly_primary.fa. A "
        "missing-or-empty guard cannot tell 'assembled nothing' from 'the output "
        "path moved', and the latter is silently discarded."
    )


def test_myloasm_branch_does_not_reuse_the_hifiasm_gfa_rule() -> None:
    """The hifiasm segment-name regex must not be applied to myloasm output.

    It would match nothing there, so circular.fa would come out empty on every
    myloasm run with no error — the silent failure this whole change exists to
    avoid.
    """
    body = _ASSEMBLE_SH.read_text()
    parts = body.split("myloasm)", 1)
    assert len(parts) == 2, "assemble.sh no longer has a `myloasm)` case arm"
    arm = parts[1].split(";;", 1)[0]
    assert "tg[0-9]+c$" not in arm, (
        "the hifiasm-meta GFA segment-name regex appears in the myloasm branch. It "
        "matches no myloasm contig, so every circular genome would be silently "
        "routed to binning."
    )


def test_splitter_uses_miint_and_never_installs() -> None:
    """The split reads/writes through miint and stays LOAD-only.

    Hand-rolling a FASTA reader/writer where miint has one is the repo's named
    review smell; a per-job INSTALL is the footgun the staged directory replaced.
    """
    code = _SPLIT_PY.read_text()
    assert "read_fastx" in code, "the splitter no longer reads with miint's read_fastx"
    assert "FORMAT FASTA" in code, "the splitter no longer writes with miint's FASTA writer"
    assert "LOAD miint" in code, "the splitter no longer LOADs miint"
    # Anchored at a string-literal opening quote so the word INSTALL in the prose
    # explaining WHY we don't install cannot satisfy — or break — this.
    assert re.search(r"""["']\s*INSTALL\b""", code) is None, (
        "the splitter issues an INSTALL statement. Service-side connects are "
        "LOAD-only: an INSTALL needs the mirror reachable from every compute node "
        "and a writable $HOME."
    )


def test_assemble_step_binds_where_the_stager_actually_stages() -> None:
    """The assemble step's derived_inputs path matches `stage-miint-extension.sh`.

    Without the declaration the container gets no miint at all — `payload.py`
    deliberately does not forward the native-only miint env to containers, so
    `derived_inputs` is the per-step mechanism that binds the staged directory in
    read-only.

    The path is compared against the STAGER rather than a literal: YAML cannot
    import a constant, so renaming the staged directory in the script would
    otherwise leave this test and the YAML agreeing with each other and disagreeing
    with the host, and every assemble ticket would bind a path that does not exist.
    """
    stager = (_REPO_ROOT / "scripts" / "stage-miint-extension.sh").read_text()
    staged = re.search(
        r'MIINT_EXTENSION_DIRECTORY="\$\{PATH_DERIVED%/\}/([A-Za-z0-9._-]+)"', stager
    )
    assert staged is not None, (
        "could not read the staged-extension directory out of "
        "scripts/stage-miint-extension.sh — this test can no longer verify that "
        "the workflow binds where the deploy stages."
    )

    yaml_text = _WORKFLOW_YAML.read_text()
    assemble = yaml_text.split("- step: assemble", 1)[1].split("- step:", 1)[0]
    assert "derived_inputs:" in assemble, (
        "the assemble step declares no derived_inputs, so the staged miint "
        "extension is never bind-mounted and the myloasm split cannot LOAD it."
    )
    bound = re.search(r"MIINT_EXTENSION_DIRECTORY:\s*(\S+)", assemble)
    assert bound is not None, "the assemble step no longer binds MIINT_EXTENSION_DIRECTORY"
    assert bound.group(1) == staged.group(1), (
        f"the assemble step binds PATH_DERIVED/{bound.group(1)} but "
        f"stage-miint-extension.sh stages into PATH_DERIVED/{staged.group(1)}. The "
        "bind would resolve to a path that does not exist, failing the step for "
        "BOTH assemblers (the bind is emitted regardless of branch)."
    )


def test_build_spec_verify_cmd_names_an_env_the_def_creates() -> None:
    """`VERIFY_CMD`'s `-n <env>` must be an env `assemble.def` actually creates.

    This is the one spec field whose staleness aborts a DEPLOY rather than just
    failing a check: `build-sif.sh` runs `VERIFY_CMD` both for the idempotency skip
    and for the post-build re-verify, so a stale env name can never be skipped as
    already-built and then fails the re-verify — and `activate.sh` aborts before any
    service restarts, taking every component down with it. The env names moved once
    already (a single `assemble` env became one per assembler).
    """
    spec = _ASSEMBLE_ENV.read_text()
    verify_cmd = re.search(r'^VERIFY_CMD="([^"]*)"', spec, re.MULTILINE)
    assert verify_cmd is not None, "assemble.env no longer declares VERIFY_CMD"
    named = re.search(r"-n\s+(\S+)", verify_cmd.group(1))
    assert named is not None, (
        f"VERIFY_CMD ({verify_cmd.group(1)!r}) does not run inside a named env; if "
        "that is deliberate, this test needs updating on purpose."
    )
    created = set(re.findall(r"micromamba create\b[^\n]*?-n\s+(\S+)", _ASSEMBLE_DEF.read_text()))
    assert named.group(1) in created, (
        f"VERIFY_CMD runs in env {named.group(1)!r}, which assemble.def does not "
        f"create (it creates {sorted(created)}). The build would fail its verify and "
        "abort the whole deploy before any service restart."
    )


def test_image_pins_myloasm_and_asserts_the_pin_at_build_time() -> None:
    """myloasm is version-pinned in the def AND the pin is checked in %test.

    The pin is enforced only by the solver, and nothing about it is observable from
    this repo. Since the circularity contract is a header STRING probed against one
    version, a drifted solve must fail the BUILD.
    """
    defsrc = _ASSEMBLE_DEF.read_text()
    create = next(
        (ln for ln in defsrc.splitlines() if "micromamba create" in ln and "myloasm" in ln),
        None,
    )
    assert create is not None, "assemble.def no longer creates the myloasm env"
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


def test_container_duckdb_matches_the_orchestrator_lock() -> None:
    """The image's DuckDB equals the orchestrator's resolved DuckDB.

    This is a genuine lockstep, not tidiness. The split LOADs the extension the
    deploy staged, and `stage-miint-extension.sh` stages with the ORCHESTRATOR's
    venv python — while DuckDB namespaces the extension directory by engine
    version + platform. A container on a different DuckDB finds no extension for
    its version and every myloasm assembly dies at LOAD, after doing all its work.
    A `uv lock` bump must therefore move the def's pin too, and this fails the unit
    suite when it doesn't.
    """
    lock = tomllib.loads(_CO_LOCK.read_text())
    locked = {p["name"]: p["version"] for p in lock["package"]}
    orchestrator_duckdb = locked.get("duckdb")
    assert orchestrator_duckdb, "duckdb is not in qiita-compute-orchestrator/uv.lock"

    defsrc = _ASSEMBLE_DEF.read_text()
    pin = re.search(r"\bpython-duckdb=([0-9][^\s\"']*)", defsrc)
    assert pin is not None, (
        "assemble.def does not pin python-duckdb. An unpinned solve can land on a "
        "DuckDB whose version has no staged miint extension."
    )
    assert pin.group(1) == orchestrator_duckdb, (
        f"assemble.def pins python-duckdb={pin.group(1)} but the orchestrator "
        f"resolves duckdb=={orchestrator_duckdb}. DuckDB namespaces the staged "
        "extension directory by engine version, so the container would LOAD from a "
        "version directory the deploy never staged."
    )
    assert re.search(rf"duckdb.__version__ == '{re.escape(orchestrator_duckdb)}'", defsrc), (
        "assemble.def's %test no longer asserts the resolved DuckDB version, so a "
        "drifted solve would build green and fail at LOAD on the cluster."
    )


def test_build_spec_hashes_the_splitter_and_verifies_the_myloasm_version() -> None:
    """The SIF spec rebuilds on a splitter edit and verifies the pinned version.

    HASH_INPUTS scopes the two-gate idempotency check. myloasm_split.py is
    %files-copied into the image and decides which contigs become LCGs, so omitting
    it would let an edited splitter be skipped as "unchanged" and never reach the
    host.
    """
    spec = _ASSEMBLE_ENV.read_text()
    hash_inputs = re.search(r'^HASH_INPUTS="([^"]*)"', spec, re.MULTILINE)
    assert hash_inputs is not None, "assemble.env no longer declares HASH_INPUTS"
    assert "myloasm_split.py" in hash_inputs.group(1), (
        f"myloasm_split.py is missing from HASH_INPUTS ({hash_inputs.group(1)!r}). "
        "Editing the splitter would not rebuild the image."
    )
    verify_match = re.search(r'^VERIFY_MATCH="([^"]*)"', spec, re.MULTILINE)
    assert verify_match is not None, "assemble.env no longer declares VERIFY_MATCH"
    # The def now creates one env PER assembler, so pick the myloasm line
    # specifically — the first `micromamba create` is hifiasm_meta's.
    create = next(
        ln
        for ln in _ASSEMBLE_DEF.read_text().splitlines()
        if "micromamba create" in ln and "myloasm=" in ln
    )
    version = re.search(r"\bmyloasm=([0-9][^\s\"']*)", create).group(1)
    assert version in verify_match.group(1), (
        f"VERIFY_MATCH ({verify_match.group(1)!r}) does not assert the pinned "
        f"myloasm version {version}. The def and the spec would drift apart."
    )
