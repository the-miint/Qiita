"""Isolated unit tests for `bam_to_parquet.execute`.

Calls `execute()` directly (not through LocalBackend / run_native_job) so
failures point at the read-loading logic, not framework wiring. Inputs are tiny
hand-written SAM files — `read_sequences_sam` reads SAM/BAM/CRAM, so a text SAM is
a zero-dependency fixture (no pysam, no binary BAM to check in).

Covers:
  - happy path: reads become read.parquet rows with sequence_idx from the minted
    range (read_sequences_sam's sequence_index + start - 1), qual decoded to
    UTINYINT[], sequence2/qual2 NULL;
  - the unaligned verify: an aligned (mapped) record → BAD_INPUT, and a caller
    declaring expect_unaligned=False → BAD_INPUT;
  - the one-record-per-read guard: a paired uBAM (unmapped mates sharing a QNAME,
    which pass the verify) → BAD_INPUT before the mint;
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
# Unmapped (FLAG 4) records model a long-read uBAM. A mapped record (default
# args overridden) models an aligned BAM the loader must reject.
_UNMAPPED = 4
_REVERSE = 16  # reverse-strand, MAPPED (no 0x4) — the mis-orientation case


def _sam_record(
    qname: str,
    seq: str,
    qual: str,
    flag: int = _UNMAPPED,
    *,
    rname: str = "*",
    pos: int = 0,
    cigar: str = "*",
) -> str:
    return "\t".join([qname, str(flag), rname, str(pos), "0", cigar, "*", "0", "0", seq, qual])


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


def test_execute_writes_read_parquet(fake_mint, tmp_path):
    """Two reads round-trip: sequence_idx assigned from read_sequences_sam's
    per-file sequence_index (+ minted start), qual phred-decoded to a UTINYINT[],
    sequence2/qual2 NULL (single-end)."""
    sam = tmp_path / "in.sam"
    _write_sam(
        sam,
        [
            _sam_record("r1", "ACGT", "IIII"),  # phred 40
            _sam_record("r2", "TTTT", "????"),  # phred 30
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


def test_aligned_bam_rejected_as_bad_input(fake_mint, tmp_path):
    """An aligned BAM (a mapped, reverse-strand record) is rejected BAD_INPUT by
    the expect_unaligned verify pass — before the parse and before the mint — so
    reverse-strand reads are never stored in reference orientation. r1 is a normal
    unmapped read; r2 is mapped, which trips the guard."""
    sam = tmp_path / "in.sam"
    _write_sam(
        sam,
        [
            _sam_record("r1", "ACGT", "IIII"),  # unmapped
            _sam_record("r2", "TTTT", "IIII", flag=_REVERSE, rname="chr1", pos=30, cigar="4M"),
        ],
    )

    with pytest.raises(BackendFailure) as exc:
        _run(Inputs(bam_path=sam, prep_sample_idx=1, work_ticket_idx=1), tmp_path / "ws")
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert exc.value.step_name == YAML_STEP_NAME
    assert fake_mint == []
    assert not (tmp_path / "ws" / "read.parquet").exists()


def test_expect_unaligned_false_rejected_as_bad_input(fake_mint, tmp_path):
    """A caller that declares expect_unaligned=False (an aligned BAM) is rejected
    outright — aligned loading is not supported yet. Rejected before any parse."""
    sam = tmp_path / "in.sam"
    _write_sam(sam, [_sam_record("r1", "ACGT", "IIII")])

    with pytest.raises(BackendFailure) as exc:
        _run(
            Inputs(bam_path=sam, prep_sample_idx=1, work_ticket_idx=1, expect_unaligned=False),
            tmp_path / "ws",
        )
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert fake_mint == []


def test_duplicate_qname_rejected_as_bad_input(fake_mint, tmp_path):
    """A paired uBAM — two UNMAPPED mates sharing a QNAME (FLAG 4|0x1) — passes the
    unaligned verify (both unmapped) but is rejected BAD_INPUT by the
    one-record-per-read guard, not silently loaded as two reads with distinct
    sequence_idx."""
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
