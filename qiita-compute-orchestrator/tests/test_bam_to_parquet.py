"""Isolated unit tests for `bam_to_parquet.execute`.

Calls `execute()` directly (not through LocalBackend / run_native_job) so
failures point at the read-loading logic, not framework wiring. Inputs are tiny
hand-written SAM files — `read_sequences_sam` reads SAM/BAM/CRAM, so a text SAM is
a zero-dependency fixture (no pysam, no binary BAM to check in).

Covers:
  - happy path: reads become read.parquet rows with sequence_idx from the minted
    range (read_sequences_sam's sequence_index + start - 1), qual decoded to
    UTINYINT[], sequence2/qual2 NULL;
  - a caller declaring expect_unaligned=False → BAD_INPUT (aligned unsupported);
  - the one-record-per-read guard: a paired uBAM (unmapped mates sharing a QNAME)
    → BAD_INPUT before the mint;
  - header-only (no records) is terminal NO_DATA (StepNoData);
  - missing input raises FileNotFoundError;
  - pre_minted_range recovery (count match reuses the range).

mint_sequence_range is monkey-patched so no live CP is needed. All tests need the
miint extension available — set MIINT_EXTENSION_REPO if your host installs from
the team mirror.
"""

from __future__ import annotations

import asyncio
import os

import duckdb
import pytest
from qiita_common.backend_failure import BackendFailure, FailureKind, StepNoData

from qiita_compute_orchestrator import sequence_range_retry
from qiita_compute_orchestrator.jobs.bam_to_parquet import YAML_STEP_NAME, Inputs, execute
from qiita_compute_orchestrator.sequence_range import (
    MintedSequenceRange,
    PreMintedRange,
    SequenceRangeAlreadyExists,
)

# Minimal SAM columns: QNAME FLAG RNAME POS MAPQ CIGAR RNEXT PNEXT TLEN SEQ QUAL.
# Unmapped (FLAG 4) records model a long-read uBAM; RNAME='*' needs no @SQ header.
_UNMAPPED = 4


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

    monkeypatch.setattr(sequence_range_retry, "mint_sequence_range", _fake)
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
    """A paired uBAM — two mates sharing a QNAME (FLAG 4|0x1) — is rejected
    BAD_INPUT by the one-record-per-read guard, not silently loaded as two reads
    with distinct sequence_idx."""
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


def test_execute_reuses_range_left_by_a_crashed_attempt(monkeypatch, tmp_path):
    """A 409 on mint is RECOVERED, not fatal: the range a prior attempt minted
    before dying is read back and reused, and the step completes.

    This is what makes the step idempotent across runner retries. The prior
    behaviour — 409 -> UNKNOWN_PERMANENT — is what turned an OOM-killed first
    attempt into a permanent failure on the retry: it masked the OOM behind a mint
    conflict and defeated the runner's OOM memory escalation, which can only pay
    off if the escalated attempt gets past the mint. This is the exact sequence
    that failed 23 of 26 samples on the first real PacBio run.
    """

    async def _conflict(*, http, prep_sample_idx, count):
        raise SequenceRangeAlreadyExists(prep_sample_idx, count)

    async def _existing(*, http, prep_sample_idx):
        # The range the OOM-killed attempt minted: same count, so reusable.
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=1000,
            sequence_idx_stop=1001,
        )

    monkeypatch.setattr(sequence_range_retry, "mint_sequence_range", _conflict)
    monkeypatch.setattr(sequence_range_retry, "get_sequence_range", _existing)

    sam = tmp_path / "in.sam"
    _write_sam(sam, [_sam_record("r1", "ACGT", "IIII"), _sam_record("r2", "TTTT", "????")])

    _run(Inputs(bam_path=sam, prep_sample_idx=42, work_ticket_idx=1), tmp_path / "ws")

    # Reused the crashed attempt's range rather than consuming a fresh one.
    seqs = [r[1] for r in _read_parquet(tmp_path / "ws" / "read.parquet")]
    assert seqs == [1000, 1001]


def test_execute_range_left_with_a_different_count_is_bad_input(monkeypatch, tmp_path):
    """A read-back range whose width doesn't match this attempt's read count must
    NOT be reused — the written sequence_idx values would mismatch
    qiita.sequence_range at registration."""

    async def _conflict(*, http, prep_sample_idx, count):
        raise SequenceRangeAlreadyExists(prep_sample_idx, count)

    async def _wrong_size(*, http, prep_sample_idx):
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=1000,
            sequence_idx_stop=1005,  # 6 indices for a 2-read BAM
        )

    monkeypatch.setattr(sequence_range_retry, "mint_sequence_range", _conflict)
    monkeypatch.setattr(sequence_range_retry, "get_sequence_range", _wrong_size)

    sam = tmp_path / "in.sam"
    _write_sam(sam, [_sam_record("r1", "ACGT", "IIII"), _sam_record("r2", "TTTT", "????")])

    with pytest.raises(BackendFailure) as ei:
        _run(Inputs(bam_path=sam, prep_sample_idx=42, work_ticket_idx=1), tmp_path / "ws")

    assert ei.value.kind is FailureKind.BAD_INPUT
    assert ei.value.step_name == YAML_STEP_NAME
    assert "must match the prior mint count exactly" in ei.value.reason


def test_plan_downsizes_a_small_bam_and_never_upsizes_a_large_one(tmp_path):
    """plan() sizes from the BAM's st_size (a stat, no scan).

    It exists to keep a control-sized BAM off the long-read baseline. It can only
    ever LOWER a step (the CP composes hints down-only), so the assertion that
    matters is directional: a tiny BAM plans well under the YAML baseline, and a
    real HiFi-sized BAM plans at or above it (where the hint becomes a no-op and
    the baseline stands).
    """
    from qiita_compute_orchestrator.jobs.bam_to_parquet import plan

    baseline_mem_gb = 32  # workflows/bam-to-parquet/1.0.0.yaml

    small = tmp_path / "control.bam"
    small.write_bytes(b"\0" * 1024)
    small_plan = plan(Inputs(bam_path=small, prep_sample_idx=1, work_ticket_idx=1))
    assert small_plan.resources.mem_gb < baseline_mem_gb

    # A HiFi sample is many GB on disk; plan must not try to shrink it.
    big = tmp_path / "hifi.bam"
    big.touch()
    os.truncate(big, 12 * 1024**3)  # sparse — no bytes actually written
    big_plan = plan(Inputs(bam_path=big, prep_sample_idx=1, work_ticket_idx=1))
    assert big_plan.resources.mem_gb >= baseline_mem_gb
