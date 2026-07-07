"""Real-miint contract pins for the `read_sequences_sam` reader the
`bam_to_parquet` native job builds on. bam_to_parquet is the FIRST (and only)
user of `read_sequences_sam` in the repo, and the function is UNDOCUMENTED
upstream, so — like `test_qc_miint_contract.py` for the QC funcs — this file pins
the facts the job's SQL depends on that a stubbed unit test can't see:

  * `read_sequences_sam(path)` emits a `read_fastx`-compatible schema:
    `sequence_index BIGINT`, `read_id VARCHAR`, `sequence1 VARCHAR`,
    `qual1 UTINYINT[]`, plus `sequence2`/`qual2`. The whole read.parquet →
    DuckLake `read` load hinges on `qual1` being `UTINYINT[]` (the `read` table's
    `qual1` is `UTINYINT[]` — qiita-data-plane/src/ducklake.rs); an ASCII qual
    would fail at registration, far from here.
  * `qual1` is phred-DECODED (Q40 'I' → 40), not the ASCII byte.
  * It emits ONE ROW PER SAM RECORD and does NOT filter secondary/supplementary
    (no FLAG column exists to filter on). This is load-bearing: the job's
    one-record-per-read guard (`count(DISTINCT read_id) != count(*)` → BAD_INPUT)
    exists precisely BECAUSE this reader passes every record through, so a paired
    mate or a secondary/supplementary alignment reaches the loader as a repeated
    QNAME. If a future build starts filtering (or adds a primary-only option),
    this pin fails and the job's guard/comment must be revisited.

Runs against the team-mirror build staged by the session-autouse fixture in
tests/conftest.py (`open_miint_conn` is LOAD-only).
"""

from __future__ import annotations

from pathlib import Path

from qiita_compute_orchestrator.miint import open_miint_conn

# A minimal SAM: one primary unmapped read (r1), plus a SECONDARY (r2) and a
# SUPPLEMENTARY (r3) — distinct QNAMEs so we can assert all three pass through
# (the reader does no flag filtering). @SQ is required (htslib rejects a SAM with
# no reference dictionary) even though the primary record is unmapped.
_SAM = "\n".join(
    [
        "@HD\tVN:1.6\tSO:unknown",
        "@SQ\tSN:chr1\tLN:1000",
        "\t".join(["r1", "4", "*", "0", "0", "*", "*", "0", "0", "ACGT", "IIII"]),
        "\t".join(["r2", "256", "chr1", "10", "0", "4M", "*", "0", "0", "AAAA", "IIII"]),
        "\t".join(["r3", "2048", "chr1", "20", "0", "4M", "*", "0", "0", "CCCC", "IIII"]),
    ]
)


def _write_sam(tmp_path: Path) -> str:
    path = tmp_path / "contract.sam"
    path.write_text(_SAM + "\n", encoding="utf-8")
    return str(path)


def test_read_sequences_sam_is_read_fastx_compatible(tmp_path):
    """The columns bam_to_parquet's SELECT names, with the types the `read` table
    assumes: sequence_index BIGINT, read_id/sequence1 VARCHAR, qual1 UTINYINT[]."""
    sam = _write_sam(tmp_path)
    with open_miint_conn() as conn:
        row = conn.execute(
            "SELECT typeof(sequence_index), typeof(read_id), typeof(sequence1), "
            "  typeof(qual1), typeof(sequence2), typeof(qual2) "
            "FROM read_sequences_sam(?) WHERE read_id = 'r1'",
            [sam],
        ).fetchone()
    assert row == ("BIGINT", "VARCHAR", "VARCHAR", "UTINYINT[]", "VARCHAR", "UTINYINT[]")


def test_read_sequences_sam_qual_is_phred_decoded(tmp_path):
    """qual1 is the phred-decoded array (Q40 'I' → 40), not the ASCII byte — so it
    drops straight into the `read` table's UTINYINT[] qual1 column."""
    sam = _write_sam(tmp_path)
    with open_miint_conn() as conn:
        qual = conn.execute(
            "SELECT qual1 FROM read_sequences_sam(?) WHERE read_id = 'r1'",
            [sam],
        ).fetchone()[0]
    assert qual == [40, 40, 40, 40]


def test_read_sequences_sam_emits_every_record_no_flag_filter(tmp_path):
    """One row per SAM record — secondary (r2) and supplementary (r3) are NOT
    dropped. This is why the job needs its own duplicate-read_id guard; if a build
    ever starts filtering, this pin fails and the guard must be revisited. Also
    pins that sequence_index is a per-file ordinal (1..N in file order)."""
    sam = _write_sam(tmp_path)
    with open_miint_conn() as conn:
        rows = conn.execute(
            "SELECT sequence_index, read_id FROM read_sequences_sam(?) ORDER BY sequence_index",
            [sam],
        ).fetchall()
    assert rows == [(1, "r1"), (2, "r2"), (3, "r3")]
