"""Real-miint contract pins for the `read_alignments` reader the `bam_to_parquet`
native job builds on. bam_to_parquet is the FIRST (and only) user of
`read_alignments` in the repo, so — like `test_qc_miint_contract.py` for the QC
funcs — this file pins the facts the job's SQL depends on that a stubbed unit
test can't see and the upstream docs don't spell out:

  * `read_alignments(path, include_seq_qual := true)` accepts the `:=` named-arg
    form and exposes `read_id`, `sequence`, `qual`, and `flags` columns.
  * `qual` comes back as `UTINYINT[]` — NOT an ASCII VARCHAR. The whole
    read.parquet → DuckLake `read` load hinges on this: the `read` table's
    `qual1` column is `UTINYINT[]` (qiita-data-plane/src/ducklake.rs), so an
    ASCII qual would fail at registration, far from here.
  * `flags` is `USMALLINT`, and `alignment_is_secondary` / `alignment_is_supplementary`
    take it and return `BOOLEAN` — the primary-record filter's contract.

If a future miint build changes any of these, this file fails — the signal to
re-pin the job's SQL. Runs against the team-mirror build staged by the
session-autouse fixture in tests/conftest.py (`open_miint_conn` is LOAD-only).
"""

from __future__ import annotations

from pathlib import Path

from qiita_compute_orchestrator.miint import open_miint_conn

# A minimal SAM: one primary unmapped read + one secondary + one supplementary.
# @SQ is required (htslib rejects a SAM with no reference dictionary) even though
# every record is unmapped (RNAME='*').
_SAM = "\n".join(
    [
        "@HD\tVN:1.6\tSO:unknown",
        "@SQ\tSN:chr1\tLN:1000",
        "\t".join(["r1", "4", "*", "0", "0", "*", "*", "0", "0", "ACGT", "IIII"]),
        "\t".join(["r2", "256", "*", "0", "0", "*", "*", "0", "0", "ACGT", "IIII"]),
        "\t".join(["r3", "2048", "*", "0", "0", "*", "*", "0", "0", "ACGT", "IIII"]),
    ]
)


def _write_sam(tmp_path: Path) -> str:
    path = tmp_path / "contract.sam"
    path.write_text(_SAM + "\n", encoding="utf-8")
    return str(path)


def test_read_alignments_column_types(tmp_path):
    """read_id/sequence VARCHAR, qual UTINYINT[], flags USMALLINT — the shape
    bam_to_parquet's intermediate SELECT and the `read` table both assume."""
    sam = _write_sam(tmp_path)
    with open_miint_conn() as conn:
        row = conn.execute(
            "SELECT typeof(read_id), typeof(sequence), typeof(qual), typeof(flags) "
            "FROM read_alignments(?, include_seq_qual := true) "
            "WHERE read_id = 'r1'",
            [sam],
        ).fetchone()
    assert row == ("VARCHAR", "VARCHAR", "UTINYINT[]", "USMALLINT")


def test_read_alignments_qual_is_phred_decoded(tmp_path):
    """qual is the phred-decoded array (Q40 'I' → 40), not the ASCII byte — so
    it drops straight into the `read` table's UTINYINT[] qual1 column."""
    sam = _write_sam(tmp_path)
    with open_miint_conn() as conn:
        qual = conn.execute(
            "SELECT qual FROM read_alignments(?, include_seq_qual := true) WHERE read_id = 'r1'",
            [sam],
        ).fetchone()[0]
    assert qual == [40, 40, 40, 40]


def test_alignment_flag_predicates_are_boolean(tmp_path):
    """alignment_is_secondary / alignment_is_supplementary take the flags column
    and return BOOLEAN — the primary-record filter's contract. r1 (FLAG 4) is
    neither; r2 (256) is secondary; r3 (2048) is supplementary."""
    sam = _write_sam(tmp_path)
    with open_miint_conn() as conn:
        rows = conn.execute(
            "SELECT read_id, typeof(alignment_is_secondary(flags)), "
            "  alignment_is_secondary(flags), alignment_is_supplementary(flags) "
            "FROM read_alignments(?) ORDER BY read_id",
            [sam],
        ).fetchall()
    by_id = {r[0]: r for r in rows}
    assert by_id["r1"][1] == "BOOLEAN"
    assert (by_id["r1"][2], by_id["r1"][3]) == (False, False)
    assert (by_id["r2"][2], by_id["r2"][3]) == (True, False)
    assert (by_id["r3"][2], by_id["r3"][3]) == (False, True)
