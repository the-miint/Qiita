"""Isolated unit tests for `bam_to_parquet.execute`.

Calls `execute()` directly (not through LocalBackend / run_native_job) so
failures point at the read-loading logic, not framework wiring. Inputs are tiny
hand-written SAM files — `read_alignments` reads SAM/BAM/CRAM, so a text SAM is a
zero-dependency fixture (no pysam, no binary BAM to check in).

Covers:
  - happy path: primary records become read.parquet rows with sequence_idx from
    the minted range, qual decoded to UTINYINT[], sequence2/qual2 NULL;
  - secondary/supplementary records are filtered out (one row per read);
  - header-only (no records) is terminal NO_DATA (StepNoData);
  - missing input raises FileNotFoundError;
  - pre_minted_range recovery (count match reuses the range).

mint_sequence_range is monkey-patched so no live CP is needed. All tests need the
miint extension available — set MIINT_EXTENSION_REPO if your host installs from
the team mirror.
"""

from __future__ import annotations

import asyncio

import duckdb
import pytest
from qiita_common.backend_failure import BackendFailure, FailureKind, StepNoData

import qiita_compute_orchestrator.jobs.bam_to_parquet as bam_module
from qiita_compute_orchestrator.jobs.bam_to_parquet import YAML_STEP_NAME, Inputs, execute
from qiita_compute_orchestrator.sequence_range import MintedSequenceRange, PreMintedRange

# Minimal SAM columns: QNAME FLAG RNAME POS MAPQ CIGAR RNEXT PNEXT TLEN SEQ QUAL.
# Unmapped (FLAG 4) records model a long-read uBAM; RNAME='*' needs no @SQ header.
_UNMAPPED = 4
_SECONDARY = 256
_SUPPLEMENTARY = 2048


def _sam_record(qname: str, seq: str, qual: str, flag: int = _UNMAPPED) -> str:
    return "\t".join([qname, str(flag), "*", "0", "0", "*", "*", "0", "0", seq, qual])


def _write_sam(path, records: list[str]) -> None:
    """Write a SAM file (header + the given record lines) to `path`.

    An `@SQ` line is included even though every record here is unmapped
    (RNAME='*'): htslib refuses a SAM with no reference dictionary ("File lacks a
    header, and no reference information provided"). The unmapped reads don't
    reference it."""
    lines = ["@HD\tVN:1.6\tSO:unknown", "@SQ\tSN:chr1\tLN:1000", *records]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _run(inputs: Inputs, workspace) -> dict:
    return asyncio.run(execute(inputs, workspace))


@pytest.fixture
def fake_mint(monkeypatch):
    """Replace mint_sequence_range with a recorder returning a range starting at
    1000. Returns the list of (prep_sample_idx, count) calls."""
    calls: list[tuple[int, int]] = []

    async def _fake(*, http, prep_sample_idx, count):
        calls.append((prep_sample_idx, count))
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=1000,
            sequence_idx_stop=1000 + count - 1,
        )

    monkeypatch.setattr(bam_module, "mint_sequence_range", _fake)
    return calls


def _read_parquet(path) -> list[tuple]:
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            "SELECT prep_sample_idx, sequence_idx, read_id, sequence1, qual1, "
            "sequence2, qual2 FROM read_parquet(?) ORDER BY sequence_idx",
            [str(path)],
        ).fetchall()


def test_execute_writes_read_parquet_and_filters_non_primary(fake_mint, tmp_path):
    """Two primary reads round-trip; a supplementary record for a third read is
    filtered out (so it never gets a sequence_idx). qual is phred-decoded to a
    UTINYINT[]; sequence2/qual2 are NULL (single-end)."""
    sam = tmp_path / "in.sam"
    _write_sam(
        sam,
        [
            _sam_record("r1", "ACGT", "IIII"),  # phred 40
            _sam_record("r2", "TTTT", "????"),  # phred 30
            _sam_record("r3", "GGGG", "IIII", flag=_SUPPLEMENTARY),  # excluded
        ],
    )

    outputs = _run(
        Inputs(bam_path=sam, prep_sample_idx=42, work_ticket_idx=1),
        tmp_path / "ws",
    )
    assert outputs["read_staging_dir"] == tmp_path / "ws"
    read_pq = tmp_path / "ws" / "read.parquet"
    assert read_pq.exists()
    # The intermediate must be gone before return (manifest walker cleanliness).
    assert not (tmp_path / "ws" / "_intermediate_reads.parquet").exists()

    # mint called once with the primary-only count (2), not 3.
    assert fake_mint == [(42, 2)]

    rows = _read_parquet(read_pq)
    assert rows == [
        (42, 1000, "r1", "ACGT", [40, 40, 40, 40], None, None),
        (42, 1001, "r2", "TTTT", [30, 30, 30, 30], None, None),
    ]


def test_header_only_sam_raises_stepnodata(fake_mint, tmp_path):
    """A SAM with no records → terminal NO_DATA: StepNoData, no mint, no output."""
    sam = tmp_path / "in.sam"
    _write_sam(sam, [])

    with pytest.raises(StepNoData) as exc:
        _run(Inputs(bam_path=sam, prep_sample_idx=1, work_ticket_idx=1), tmp_path / "ws")
    assert exc.value.step_name == YAML_STEP_NAME
    assert fake_mint == []
    assert not (tmp_path / "ws" / "read.parquet").exists()


def test_all_secondary_sam_raises_stepnodata(fake_mint, tmp_path):
    """A SAM whose only records are secondary alignments has no primary reads →
    the primary-only filter empties it → StepNoData."""
    sam = tmp_path / "in.sam"
    _write_sam(sam, [_sam_record("r1", "ACGT", "IIII", flag=_SECONDARY)])

    with pytest.raises(StepNoData):
        _run(Inputs(bam_path=sam, prep_sample_idx=1, work_ticket_idx=1), tmp_path / "ws")
    assert fake_mint == []


def test_duplicate_qname_rejected_as_bad_input(fake_mint, tmp_path):
    """A paired-end BAM — two primary records sharing a QNAME (FLAG 0x1 paired) —
    is rejected BAD_INPUT before the mint, not silently loaded as two reads with
    distinct sequence_idx."""
    sam = tmp_path / "in.sam"
    _write_sam(
        sam,
        [
            _sam_record("pair1", "ACGT", "IIII", flag=_UNMAPPED | 0x1),  # mate 1
            _sam_record("pair1", "TTTT", "????", flag=_UNMAPPED | 0x1),  # mate 2, same QNAME
        ],
    )

    with pytest.raises(BackendFailure) as exc:
        _run(Inputs(bam_path=sam, prep_sample_idx=1, work_ticket_idx=1), tmp_path / "ws")
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert exc.value.step_name == YAML_STEP_NAME
    assert fake_mint == []  # rejected before the mint
    assert not (tmp_path / "ws" / "read.parquet").exists()


def test_missing_input_raises_filenotfound(fake_mint, tmp_path):
    """No BAM at the path → FileNotFoundError (the dispatcher maps that to
    BAD_INPUT one layer up)."""
    with pytest.raises(FileNotFoundError):
        _run(
            Inputs(bam_path=tmp_path / "nope.sam", prep_sample_idx=1, work_ticket_idx=1),
            tmp_path / "ws",
        )


def test_pre_minted_range_matching_count_skips_mint(fake_mint, tmp_path):
    """Recovery path: a pre_minted_range whose width matches the read count is
    reused (no HTTP mint) and drives sequence_idx assignment."""
    sam = tmp_path / "in.sam"
    _write_sam(sam, [_sam_record("r1", "ACGT", "IIII"), _sam_record("r2", "TTTT", "????")])

    _run(
        Inputs(
            bam_path=sam,
            prep_sample_idx=9,
            work_ticket_idx=1,
            pre_minted_range=PreMintedRange(sequence_idx_start=500, sequence_idx_stop=501),
        ),
        tmp_path / "ws",
    )
    assert fake_mint == []  # HTTP mint skipped
    seqs = [r[1] for r in _read_parquet(tmp_path / "ws" / "read.parquet")]
    assert seqs == [500, 501]
