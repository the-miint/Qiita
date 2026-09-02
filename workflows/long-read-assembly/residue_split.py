"""Split the UNBINNED residue into one FASTA per contig, so CheckM can score the
large unbinned contigs the way it scores the refined bins and the circular contigs.

Run by checkm.sh:

    python3 /opt/qiita/residue_split.py <noLCG.fa> <refined_bins_dir> <out_dir>

Two sets, and they are not the same one:

  RESIDUE     every noLCG contig no refined bin claims. What `assembly_hash` stores
              as KIND_UNBINNED.
  SCORED      the residue at or above `_MIN_RESIDUE_LENGTH_BP`. What this writes,
              and a strict subset of the residue.

WHAT THE RESIDUE IS
-------------------
`noLCG.fa` is every non-circular contig the assembler produced — the input binning
and bin_refine consume. The contigs a refined bin claims are therefore IN it as well
as in the bins, and scoring the file as-is would score those twice, once under their
bin's `bin_id` and once under their own contig id.

MATCHED ON THE CANONICAL SEQUENCE HASH, NOT THE CONTIG ID
---------------------------------------------------------
`assembly_hash` reduces its UNBINNED rows with a `DELETE ... WHERE kind = ?` bound to
`KIND_UNBINNED`, keyed on `sequence_hash IN (SELECT sequence_hash FROM binned_hash)`,
so a contig whose BYTES duplicate a binned contig under a different id is dropped
there. Matching on ids here instead would keep it, and CheckM would return a
`"Bin Id"` that `assembly_load` joins to no `assembly_membership` row — a
`bin_quality` row describing a contig nothing names.

So this imports the same expression from the same file `assembly_hash` imports it
from (`chunking.py`, staged into this image by build-sif.sh), rather than restating
it — and reads the same set of bin files, which is why `_BIN_GLOBS` has to track
`assembly_hash._FASTA_GLOBS`. In `test_residue_split.py`,
`test_the_scored_set_is_assembly_hashs_residue_above_the_cut` runs both
implementations over one fixture and asserts the two sets equal.

WHY A LENGTH CUT, AND WHY IT IS HERE
------------------------------------
A completeness figure against a marker set is only meaningful for something that
could plausibly be a genome. The residue is mostly short fragments, and CheckM's
cost is per contig scored, so scoring all of them spends the step's walltime to
produce rows nobody can read. `_MIN_RESIDUE_LENGTH_BP` is the cut, and it is applied
here rather than at query time because the alternative is paying that cost every run.

The cut does exclude essentially every plasmid and DNA-virus contig, which the residue
is expected to contain and which the workflow stores deliberately. That exclusion is
intended rather than overlooked: CheckM scores against bacterial and archaeal marker
sets, which say nothing useful about either, so a completeness figure for one would be
misleading rather than merely missing.

Contigs below the cut keep their `assembly_membership` row and their feature — they
are stored exactly as before. What they do not get is a `bin_quality` row, which is
the same thing that was true of the whole residue before this splitter existed.

THE STEM IS THE CONTIG ID, UNMODIFIED
-------------------------------------
Same rule and same reason as `lcg_split.py`: an UNBINNED contig's
`assembly_membership.bin_id` IS its contig id (`assembly_hash` COALESCEs it from the
read_fastx record), and CheckM keys its output on the filename stem, so the two meet
only if the stem is that id byte-for-byte. `reject_unusable_contig_ids` is the guard.
"""

from __future__ import annotations

import gzip
import sys
from pathlib import Path

# `miint_connect` and `chunking` sit beside this file, both in the repo and at
# /opt/qiita in the image. Running the script directly puts that directory on
# sys.path already; the insert is for the `.def` %test, which loads this module by
# spec and does not.
sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    # Anywhere qiita-common is installed — the repo, the tests, the services.
    from qiita_common.chunking import canonical_sequence_hash_expr  # noqa: E402
except ModuleNotFoundError:
    # The image, where qiita-common is not installed and build-sif.sh has staged
    # that same file beside this one as /opt/qiita/chunking.py (declared in this
    # image's HASH_INPUTS). Two paths to ONE file, not two copies of an expression.
    from chunking import canonical_sequence_hash_expr  # noqa: E402

from miint_connect import (  # noqa: E402
    FASTA_SUFFIX,
    connect,
    die,
    require_non_empty_fasta,
    split_contigs_to_fasta,
    sql_path,
)

PROG = "residue_split"

# The shortest residue contig worth scoring. A CheckM completeness/contamination
# pair is computed against a lineage marker set, so below roughly this size the
# answer is "almost nothing found" for reasons that say nothing about the contig.
# Chosen with the assay owner; raising it drops rows from `bin_quality` and lowers
# the step's cost, and nothing else in the pipeline reads it.
_MIN_RESIDUE_LENGTH_BP = 300_000

# Which files under refined_bins_dir are bins. Must stay the set `assembly_hash`
# accepts (`_FASTA_GLOBS`), because the two have to agree on WHICH contigs are
# binned: a suffix only assembly_hash accepts leaves that bin out of the subtraction
# here, and the contigs it claims survive into CheckM as genomes whose `"Bin Id"`
# joins no membership row. `test_residue_split.py` fails on drift between the two.
# `bin_refine.sh` writes only `.fa` today, so the rest are unreachable rather than
# decorative — they are here so that stops being true silently.
_BIN_GLOBS = ("*.fna", "*.fna.gz", "*.fa", "*.fa.gz", "*.fasta", "*.fasta.gz")


def _is_empty_sequence_file(path: Path) -> bool:
    """True iff `path` DECOMPRESSES to zero bytes.

    Same rule as `qiita_common.duckdb_miint.is_empty_sequence_file`, which the image
    has no qiita-common to import it from:
    `read_fastx` raises on a zero-record input and one empty path aborts the whole
    scan, so an empty bin has to be dropped before the read rather than caught after.
    A size check alone is wrong for `.gz` — an empty gzip member is still ~20 bytes on
    disk. `test_residue_split.py` pins this against the qiita-common original.
    """
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rb") as fh:
        return not fh.read(1)


def _bin_files(bins_dir: Path) -> list[Path]:
    """Every non-empty refined-bin FASTA directly under `bins_dir`, sorted."""
    found: list[Path] = []
    for pattern in _BIN_GLOBS:
        found.extend(bins_dir.glob(pattern))
    return sorted(p for p in set(found) if not _is_empty_sequence_file(p))


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        die(PROG, f"usage: {argv[0]} <noLCG.fa> <refined_bins_dir> <out_dir>")
    residue, bins_dir, out_dir = argv[1], Path(argv[2]), Path(argv[3])

    require_non_empty_fasta(PROG, residue)

    # An ABSENT directory and an EMPTY one are different states and only the second
    # is legitimate. Both would otherwise land on the no-bins branch below and skip
    # the subtraction entirely, which is the one failure this program exists to
    # prevent — so a mis-bound path fails here instead of scoring every binned contig
    # a second time.
    if not bins_dir.is_dir():
        die(PROG, f"refined_bins_dir is not a directory: {bins_dir}")

    # A glob with no match makes read_fastx raise, and "no refined bin" is a real
    # assembly outcome (every contig is residue), so the empty case is its own scan
    # over a literal-empty hash set rather than a failure.
    bin_files = _bin_files(bins_dir)

    out_dir.mkdir(parents=True, exist_ok=True)
    con = connect(PROG, temp_subdir="duckdb-residue-split")
    try:
        if bin_files:
            bin_glob = "[" + ", ".join(sql_path(PROG, str(p)) for p in bin_files) + "]"
            con.execute(
                "CREATE TEMP TABLE binned_hash AS SELECT DISTINCT "
                f"{canonical_sequence_hash_expr('sequence1')} AS sequence_hash "
                f"FROM read_fastx({bin_glob})"
            )
        else:
            con.execute("CREATE TEMP TABLE binned_hash (sequence_hash UUID)")

        # Materialize then DELETE, the same two steps in the same order as
        # `assembly_hash`, so the surviving set is produced by the same operation and
        # not by a re-derivation that happens to agree. `IN` rather than `NOT IN`
        # falls out of that: `NOT IN` over a set holding one NULL is empty, which
        # here would silently keep every binned duplicate.
        con.execute(
            "CREATE TEMP TABLE contig AS SELECT read_id AS contig_id, sequence1, "
            f"{canonical_sequence_hash_expr('sequence1')} AS sequence_hash "
            f"FROM read_fastx({sql_path(PROG, residue)}) "
            f"WHERE length(sequence1) >= {_MIN_RESIDUE_LENGTH_BP}"
        )
        con.execute(
            "DELETE FROM contig WHERE sequence_hash IN (SELECT sequence_hash FROM binned_hash)"
        )

        written = split_contigs_to_fasta(PROG, con, out_dir, FASTA_SUFFIX)
    finally:
        con.close()

    print(
        f"residue_split: {written} unbinned contig(s) >= {_MIN_RESIDUE_LENGTH_BP} bp "
        "written as one FASTA each"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
