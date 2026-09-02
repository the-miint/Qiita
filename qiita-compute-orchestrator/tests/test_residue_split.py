"""Pin which unbinned contigs the `checkm` step scores, and on what key.

Why this exists
---------------
`noLCG.fa` holds every non-circular contig, INCLUDING the ones a refined bin went on
to claim. Scoring it as-is would score those twice — once under the bin's stem, once
under the contig's own id — and the second row would join no `assembly_membership`
row at all. So `residue_split.py` subtracts the binned contigs first.

The key is the thing to pin
---------------------------
`assembly_hash` reduces its UNBINNED rows on the CANONICAL SEQUENCE HASH, not the
contig id:

    DELETE FROM contig WHERE kind = ?   -- bound to KIND_UNBINNED
      AND sequence_hash IN (SELECT sequence_hash FROM binned_hash)

Both sides import that expression from `qiita_common.chunking`, so what is at risk is
not the hash function but the RULE built on it: matching ids instead of bytes, folding
the subtraction the wrong way, or missing that the hash is strand- and case-canonical.
Each of those leaves a `bin_quality` row describing a genome no membership row names,
and none of them is visible in a green run.

The fixtures below therefore each isolate one of those, and every one of them would
pass under an id-matching splitter except the ones built to fail it. In particular
the residue set here contains a contig whose BYTES duplicate a binned contig under a
DIFFERENT id — the single case where id-matching and hash-matching disagree, and so
the case without which this file would assert nothing.

Following the same rule as the other split smokes in this tree: EXECUTE the shipped
program against the extension this component's conftest stages, and never
re-implement a production expression as the oracle.
"""

from __future__ import annotations

import gzip
import importlib.util
import os
import random
import re
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOW_DIR = _REPO_ROOT / "workflows" / "long-read-assembly"
_SPLIT_PY = _WORKFLOW_DIR / "residue_split.py"
_CHECKM_SH = _WORKFLOW_DIR / "checkm.sh"
_CHECKM_DEF = _WORKFLOW_DIR / "checkm.def"
_CHECKM_ENV = _WORKFLOW_DIR / "sif-build.d" / "checkm.env"

# Imported, not retyped: the basenames are the producer/consumer contract between
# checkm.sh and assembly_load, so a rename must not leave this pin green.
from qiita_compute_orchestrator.jobs.assembly_load import (  # noqa: E402
    _CHECKM_UNBINNED_LINEAGE_TSV,
    _CHECKM_UNBINNED_QA_TSV,
)


@pytest.fixture(scope="module")
def split_module():
    """The shipped program, imported by spec so its constants are read, not retyped.

    `residue_split.py` is not on any import path — it ships into the image, not the
    package — so it is loaded from its path.

    A FIXTURE, not module scope. Importing it runs its `sys.path.insert(0, <workflow
    dir>)`, which would otherwise persist for every module pytest collects after this
    one, and its import errors would take all of this file's tests down at collection
    instead of failing the tests that need it. `sys.path` and the module it cached are
    both undone on the way out; the tests that RUN the splitter do so in a subprocess,
    which a path mutation here would not have reached anyway.
    """
    before = list(sys.path)
    spec = importlib.util.spec_from_file_location("residue_split", _SPLIT_PY)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    try:
        yield module
    finally:
        sys.path[:] = before
        # `module_from_spec` never registers the module itself, but its top-level
        # `from miint_connect import ...` goes through the normal import system and
        # does — under a bare name, from a directory no longer on the path.
        sys.modules.pop("miint_connect", None)


# The cut, read off the shipped source. A literal `int` assignment is the one thing a
# regex can take from a file without importing it, which keeps collection free of the
# side effects above; `test_the_length_cut_matches_the_shipped_constant` checks this
# against the imported module so the two cannot drift.
_CUT_MATCH = re.search(r"^_MIN_RESIDUE_LENGTH_BP\s*=\s*([\d_]+)", _SPLIT_PY.read_text(), re.M)
assert _CUT_MATCH, (
    f"no `_MIN_RESIDUE_LENGTH_BP = <int>` line in {_SPLIT_PY}; every fixture below is sized off it"
)
_MIN_BP = int(_CUT_MATCH.group(1).replace("_", ""))


def _seq(n: int, seed: int) -> str:
    """A random ACGT run of length `n`.

    Random rather than a repeated motif so it is not its own reverse complement: a
    palindromic fixture makes the strand-canonical hash indistinguishable from a
    plain one, which is the failure `qiita-common`'s own hash tests were bitten by.
    """
    rng = random.Random(seed)
    return "".join(rng.choices("ACGT", k=n))


def _revcomp(seq: str) -> str:
    """Reverse complement, for BUILDING a fixture only — never as an oracle.

    The production expression uses miint's `sequence_dna_reverse_complement`. This is
    plain ACGT test input with no ambiguity codes and no case to preserve, so the two
    agree here by construction; the assertion is still about what the program did.
    """
    return seq.translate(str.maketrans("ACGT", "TGCA"))[::-1]


def _fasta(records: list[tuple[str, str]]) -> str:
    return "".join(f">{rid}\n{seq}\n" for rid, seq in records)


@pytest.fixture(scope="module")
def staged_miint() -> str:
    """The extension directory this component's conftest already staged."""
    ext_dir = os.environ.get("MIINT_EXTENSION_DIRECTORY")
    assert ext_dir, (
        "MIINT_EXTENSION_DIRECTORY is unset — conftest's setup_miint_test_env() "
        "should have set it. Without it these tests would silently exercise a "
        "different extension resolution than production."
    )
    return ext_dir


def _split(
    tmp_path: Path,
    residue: list[tuple[str, str]],
    bins: dict[str, list[tuple[str, str]]],
) -> tuple[subprocess.CompletedProcess[str], set[str]]:
    """Run the splitter the way checkm.sh does; return (proc, stems written)."""
    nolcg = tmp_path / "noLCG.fa"
    nolcg.write_text(_fasta(residue))
    bins_dir = tmp_path / "refined_bins"
    bins_dir.mkdir(exist_ok=True)
    for bin_id, records in bins.items():
        (bins_dir / f"{bin_id}.fa").write_text(_fasta(records))
    out = tmp_path / "residue"
    proc = subprocess.run(
        [sys.executable, str(_SPLIT_PY), str(nolcg), str(bins_dir), str(out)],
        capture_output=True,
        text=True,
    )
    written = {p.stem for p in out.glob("*.fa")} if out.is_dir() else set()
    return proc, written


def test_split_program_is_present() -> None:
    """Anti-vacuity guard: every test below shells out to this file."""
    assert _SPLIT_PY.is_file(), f"{_SPLIT_PY} is missing — the pins below are vacuous"


def test_a_contig_duplicating_a_binned_one_under_another_id_is_not_scored(
    tmp_path, staged_miint
) -> None:
    """THE discriminating case: same bytes, different id.

    An id-matching splitter keeps `residue_copy` — it never appears in the bins under
    that name — and CheckM then returns a `"Bin Id"` that `assembly_load` joins to no
    `assembly_membership` row, because `assembly_hash` dropped that contig on its
    hash. This is the only fixture here that separates the two rules, so without it
    the rest of this file would pass against the wrong implementation.
    """
    shared = _seq(_MIN_BP, seed=1)
    unique = _seq(_MIN_BP, seed=2)

    proc, written = _split(
        tmp_path,
        residue=[("residue_copy", shared), ("residue_only", unique)],
        bins={"bin.1": [("binned_original", shared)]},
    )

    assert proc.returncode == 0, proc.stderr
    assert written == {"residue_only"}, (
        "residue_split kept a contig whose sequence a refined bin already claims — "
        "it is matching contig ids, not the canonical sequence hash assembly_hash "
        "reduces UNBINNED on."
    )


def test_the_reverse_complement_of_a_binned_contig_is_not_scored(tmp_path, staged_miint) -> None:
    """The hash is strand-canonical, so the same molecule on either strand is one.

    Fails against a splitter that md5s the sequence as written: the revcomp hashes to
    something else and survives as its own scored subject. `assembly_hash` would still have
    dropped it, so the row would again join nothing.
    """
    binned = _seq(_MIN_BP, seed=3)
    assert binned != _revcomp(binned), "fixture must not be its own reverse complement"

    proc, written = _split(
        tmp_path,
        residue=[("residue_rc", _revcomp(binned))],
        bins={"bin.1": [("binned_fwd", binned)]},
    )

    assert proc.returncode == 0, proc.stderr
    assert written == set(), (
        "a residue contig that is the reverse complement of a binned one was scored; "
        "the canonical hash folds strand and assembly_hash drops it"
    )


def test_a_lowercase_duplicate_of_a_binned_contig_is_not_scored(tmp_path, staged_miint) -> None:
    """The hash folds case, so soft-masked bytes are the same sequence.

    Fails against a splitter that hashes the raw bytes — the lowercase copy survives
    as its own scored subject while `assembly_hash` drops it.
    """
    binned = _seq(_MIN_BP, seed=4)

    proc, written = _split(
        tmp_path,
        residue=[("residue_lower", binned.lower())],
        bins={"bin.1": [("binned_upper", binned)]},
    )

    assert proc.returncode == 0, proc.stderr
    assert written == set(), (
        "a soft-masked duplicate of a binned contig was scored; the canonical hash folds case"
    )


@pytest.mark.parametrize(
    ("length", "scored"),
    [(_MIN_BP, True), (_MIN_BP - 1, False)],
)
def test_the_length_cut_is_inclusive_at_the_boundary(
    length: int, scored: bool, tmp_path, staged_miint
) -> None:
    """`>=`, checked on both sides of the boundary.

    Two contigs one base apart cannot both be right, so this separates `>=` from `>`
    — the off-by-one that would silently drop a genome exactly at the cut.
    """
    proc, written = _split(tmp_path, residue=[("edge", _seq(length, seed=5))], bins={})

    assert proc.returncode == 0, proc.stderr
    assert written == ({"edge"} if scored else set()), (
        f"a {length} bp contig was {'dropped' if scored else 'scored'} against a "
        f"{_MIN_BP} bp inclusive cut"
    )


def test_no_refined_bin_is_not_an_error(tmp_path, staged_miint) -> None:
    """Every contig is residue when binning recovered nothing.

    A real assembly outcome, and the case where a glob over an empty `refined_bins`
    would make `read_fastx` raise — which would fail the whole step on a prep_sample
    that simply has no MAG.
    """
    proc, written = _split(tmp_path, residue=[("only_contig", _seq(_MIN_BP, seed=6))], bins={})

    assert proc.returncode == 0, proc.stderr
    assert written == {"only_contig"}


def test_nothing_surviving_writes_an_empty_directory_and_succeeds(tmp_path, staged_miint) -> None:
    """All-short residue is a success with no genomes, not a failure.

    checkm.sh guards its third run on this directory being non-empty, so the contract
    is that the splitter exits 0 having written nothing.
    """
    proc, written = _split(tmp_path, residue=[("tiny", _seq(1000, seed=7))], bins={})

    assert proc.returncode == 0, proc.stderr
    assert written == set()


def test_the_stem_is_the_contig_id_verbatim(tmp_path, staged_miint) -> None:
    """Including the dot a hifiasm id carries.

    `assembly_membership.bin_id` for an UNBINNED contig IS its contig id, and CheckM
    keys its output on the stem, so a sanitized stem yields a quality row that joins
    nothing. Same rule `lcg_split` follows, asserted here because this splitter names
    its files independently.
    """
    proc, written = _split(
        tmp_path,
        residue=[("s0.ctg000001c", _seq(_MIN_BP, seed=8))],
        bins={},
    )

    assert proc.returncode == 0, proc.stderr
    assert written == {"s0.ctg000001c"}


def test_checkm_runs_the_residue_pass_and_names_the_files_assembly_load_reads() -> None:
    """The entrypoint wires the splitter to the two basenames assembly_load opens.

    A rename on either side leaves the step green and `bin_quality` silently without
    UNBINNED rows, so the two names are asserted against the constants themselves.
    """
    body = _CHECKM_SH.read_text()

    assert "residue_split.py" in body, "checkm.sh no longer runs residue_split.py"
    assert _CHECKM_UNBINNED_LINEAGE_TSV in body, (
        f"checkm.sh does not write {_CHECKM_UNBINNED_LINEAGE_TSV}"
    )
    assert _CHECKM_UNBINNED_QA_TSV in body, f"checkm.sh does not write {_CHECKM_UNBINNED_QA_TSV}"


def test_the_splitter_and_its_qiita_common_sources_ship_in_the_image() -> None:
    """%files-copied and named in HASH_INPUTS.

    `chunking.py` and `duckdb_miint.py` are qiita-common's files, so the image carries
    the SAME hash expression `assembly_hash` uses and the SAME emptiness rule, rather
    than copies. Omitting any of the three from HASH_INPUTS leaves the two-gate
    idempotency check green on an edited splitter, so the deploy would keep serving
    the old SIF while the repo says the residue is scored.
    """
    def_body = _CHECKM_DEF.read_text()
    assert "residue_split.py /opt/qiita/residue_split.py" in def_body
    assert "chunking.py /opt/qiita/chunking.py" in def_body
    assert "duckdb_miint.py /opt/qiita/duckdb_miint.py" in def_body

    hash_inputs = _CHECKM_ENV.read_text()
    declared = next(line for line in hash_inputs.splitlines() if line.startswith("HASH_INPUTS="))
    assert "residue_split.py" in declared, declared
    # The path, not just the basename: each entry has to resolve to qiita-common's
    # file for the single-sourcing above to hold.
    for staged in ("chunking.py", "duckdb_miint.py"):
        assert f"qiita-common/src/qiita_common/{staged}" in declared, declared


def _assembly_hash_residue(fixture_dir: Path, tmp_path: Path) -> set[str]:
    """The UNBINNED contig ids `assembly_hash` stores, above the cut.

    Runs the real native job over the same directory and reads its own outputs —
    `bin_map.parquet` (read_id -> kind, contig_id) joined to `manifest.parquet`
    (read_id -> sequence_length_bp). Not a re-derivation of the rule: the set comes
    out of the implementation this splitter has to agree with.
    """
    import asyncio

    import duckdb
    from qiita_common.assembly_constants import KIND_UNBINNED

    from qiita_compute_orchestrator.jobs import assembly_hash

    workspace = tmp_path / "hash_ws"
    asyncio.run(
        assembly_hash.execute(
            assembly_hash.Inputs(
                genomes_dir=fixture_dir,
                refined_bins_dir=fixture_dir / "refined_bins",
                prep_sample_idx=1,
                work_ticket_idx=1,
            ),
            workspace,
        )
    )
    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT b.contig_id FROM read_parquet(?) b "
            "JOIN read_parquet(?) m ON b.read_id = m.read_id "
            "WHERE b.kind = ? AND m.sequence_length_bp >= ?",
            [
                str(workspace / "bin_map.parquet"),
                str(workspace / "manifest.parquet"),
                KIND_UNBINNED,
                _MIN_BP,
            ],
        ).fetchall()
    finally:
        con.close()
    return {r[0] for r in rows}


@pytest.mark.parametrize("bin_suffix", [".fa", ".fna"])
def test_the_scored_set_is_assembly_hashs_residue_above_the_cut(
    bin_suffix: str, tmp_path, staged_miint
) -> None:
    """The two implementations agree on WHICH contigs are the scored residue.

    This is the claim the whole design rests on and the one nothing else here checks:
    every other test in this file compares against a hand-written expectation, which
    cannot catch the two drifting together in the same direction.

    The fixture carries every case where a weaker rule diverges — a byte-duplicate of
    a binned contig under another id, a reverse complement, a soft-masked copy, a
    unique contig above the cut and one below it.

    `.fna` is the control, and it is the case that used to fail: `assembly_hash`
    accepts six FASTA suffixes and this splitter globbed `*.fa` alone, so a `.fna` bin
    left its contigs out of the subtraction here and only here. `bin_refine.sh` writes
    `.fa` today, so the parametrization is what keeps that from being re-broken
    silently.
    """
    binned = _seq(_MIN_BP, seed=11)
    other_binned = _seq(_MIN_BP, seed=12)
    assert other_binned != _revcomp(other_binned), "rc fixture must not be a palindrome"

    fixture = tmp_path / "genomes"
    (fixture / "refined_bins").mkdir(parents=True)
    (fixture / "refined_bins" / f"bin.1{bin_suffix}").write_text(
        _fasta([("binned_original", binned), ("binned_two", other_binned)])
    )
    (fixture / "noLCG.fa").write_text(
        _fasta(
            [
                ("dup_of_binned", binned),
                ("rc_of_binned", _revcomp(other_binned)),
                ("lower_of_binned", binned.lower()),
                ("genuinely_unbinned", _seq(_MIN_BP, seed=13)),
                ("below_the_cut", _seq(1000, seed=14)),
            ]
        )
    )

    expected = _assembly_hash_residue(fixture, tmp_path)

    out = tmp_path / "residue"
    proc = subprocess.run(
        [
            sys.executable,
            str(_SPLIT_PY),
            str(fixture / "noLCG.fa"),
            str(fixture / "refined_bins"),
            str(out),
        ],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    written = {p.stem for p in out.glob("*.fa")}

    assert written == expected, (
        f"residue_split scored {sorted(written)} but assembly_hash stores "
        f"{sorted(expected)} as UNBINNED above the cut — a bin_quality row whose "
        "bin_id joins no assembly_membership row, or a scored contig missing one"
    )
    # Anti-vacuity: both sides agreeing on the empty set would satisfy the above.
    # Exact, not membership: both sides failing to subtract would agree on the larger
    # set and the equality above would still pass.
    assert expected == {"genuinely_unbinned"}, expected


def test_the_bin_globs_match_assembly_hashs(split_module) -> None:
    """`residue_split._BIN_GLOBS` must be `assembly_hash._FASTA_GLOBS`.

    The equality test above covers `.fa` and `.fna`; this covers the rest of the set
    without a fixture per suffix. A suffix only `assembly_hash` accepts is a bin whose
    contigs this splitter does not subtract.
    """
    from qiita_compute_orchestrator.jobs.assembly_hash import _FASTA_GLOBS

    assert set(split_module._BIN_GLOBS) == set(_FASTA_GLOBS), (
        f"residue_split globs {sorted(split_module._BIN_GLOBS)}, assembly_hash accepts "
        f"{sorted(_FASTA_GLOBS)}"
    )


def test_the_splitter_imports_qiita_commons_emptiness_check(split_module) -> None:
    """`residue_split.is_empty_sequence_file` IS `qiita_common`'s, not a copy of it.

    The image has no qiita-common, so the module falls back to the `duckdb_miint.py`
    that `build-sif.sh` stages beside it from this same source file. Here, where
    qiita-common IS importable, the first branch must be the one that binds — a
    fallback that silently won.
    """
    from qiita_common.duckdb_miint import is_empty_sequence_file

    assert split_module.is_empty_sequence_file is is_empty_sequence_file


def test_an_empty_gzipped_bin_does_not_abort_the_split(tmp_path, staged_miint) -> None:
    """An empty `.fa.gz` beside a real bin is skipped, not handed to `read_fastx`.

    A bin file with no records is ~20 bytes on disk once gzipped, so a size check
    would call it non-empty; `read_fastx` raises on a zero-record input and one bad
    path aborts the whole scan, failing the step for every OTHER bin too.

    Two ways to be wrong, and this separates them: aborting shows up as a non-zero
    exit, and dropping the whole bins glob shows up as the binned contig surviving
    into the scored set.
    """
    binned = _seq(_MIN_BP, seed=11)
    residue_only = _seq(_MIN_BP, seed=12)

    bins_dir = tmp_path / "refined_bins"
    bins_dir.mkdir()
    empty_gz = bins_dir / "bin.2.fa.gz"
    with gzip.open(empty_gz, "wb"):
        pass
    assert empty_gz.stat().st_size > 0, "fixture must not be zero bytes on disk"

    proc, written = _split(
        tmp_path,
        residue=[("binned_one", binned), ("residue_one", residue_only)],
        bins={"bin.1": [("binned_one", binned)]},
    )

    assert proc.returncode == 0, proc.stderr
    assert written == {"residue_one"}


def test_residue_split_uses_miint_and_never_installs() -> None:
    """Reads through miint, writes through the shared helper, LOAD-only.

    The twin of `test_lcg_split.test_split_uses_miint_and_never_installs`, and not
    covered by it: replacing this splitter's read with a hand-rolled FASTA parser
    would leave that test green, and only the `.def` `%test` would catch it — at SIF
    build, on Linux, after review.
    """
    code = _SPLIT_PY.read_text()

    assert re.search(r"FROM read_fastx\(", code), (
        "residue_split no longer reads with miint's read_fastx"
    )
    assert "split_contigs_to_fasta" in code, (
        "residue_split no longer writes through the shared per-contig FASTA writer"
    )
    assert "from miint_connect import" in code, (
        "residue_split no longer connects through miint_connect, so the staged-miint "
        "LOAD asserted in test_lcg_split is not the connection it opens"
    )
    assert "duckdb.connect" not in code, (
        "residue_split opens its own DuckDB connection beside miint_connect.connect"
    )
    # The hash expression is IMPORTED, never retyped — a local copy is the drift the
    # single-sourcing exists to prevent.
    assert "canonical_sequence_hash_expr" in code, (
        "residue_split no longer imports the canonical hash expression; a local copy "
        "is the drift the single-sourcing exists to prevent"
    )
    # `duckdb_miint` is qiita-common's WHOLE miint module, so the copy staged into the
    # image carries the installer as well as the one function wanted here. The image's
    # `%test` guard is anchored at a quoted INSTALL and so cannot see an import of it;
    # this is the check that can. Both import branches are asserted, since only the
    # fallback one runs in the container.
    names = {
        m.split("#")[0].strip()
        for m in re.findall(r"^\s*from (?:qiita_common\.)?duckdb_miint import (.+)$", code, re.M)
    }
    assert names == {"is_empty_sequence_file"}, (
        f"residue_split imports {sorted(names)} from duckdb_miint; it may take only "
        "is_empty_sequence_file — this image LOADs the deploy-staged extension and "
        "never INSTALLs"
    )


def test_without_the_staged_extension_the_residue_split_fails(tmp_path) -> None:
    """No MIINT_EXTENSION_DIRECTORY must fail, never fall back to INSTALL."""
    nolcg = tmp_path / "noLCG.fa"
    nolcg.write_text(_fasta([("c1", _seq(_MIN_BP, seed=21))]))
    bins = tmp_path / "refined_bins"
    bins.mkdir()
    env = dict(os.environ)
    env.pop("MIINT_EXTENSION_DIRECTORY", None)
    proc = subprocess.run(
        [sys.executable, str(_SPLIT_PY), str(nolcg), str(bins), str(tmp_path / "out")],
        capture_output=True,
        text=True,
        env=env,
    )

    assert proc.returncode == 64, proc.stdout
    assert "MIINT_EXTENSION_DIRECTORY" in proc.stderr


def test_the_length_cut_matches_the_shipped_constant(split_module) -> None:
    """The regex above and the module agree on the cut.

    Every fixture in this file is sized off `_MIN_BP`, which is read from the source
    text so collection stays free of the import's side effects. If that read ever
    picked up something other than `_MIN_RESIDUE_LENGTH_BP`, the boundary tests would
    still pass — against the wrong number — so the two are compared once here.
    """
    assert _MIN_BP == split_module._MIN_RESIDUE_LENGTH_BP
