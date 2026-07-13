"""Isolated unit tests for `bam_to_parquet.execute`.

Calls `execute()` directly (not through LocalBackend / run_native_job) so
failures point at the read-loading logic, not framework wiring. Inputs are tiny
hand-written SAM files — `read_sequences_sam` reads SAM/BAM/CRAM, so a text SAM is
a zero-dependency fixture (no pysam, no binary BAM to check in).

Covers:
  - happy path: reads become read/part_*.parquet rows with sequence_idx from the minted
    range (read_sequences_sam's sequence_index + start - 1), qual decoded to
    UTINYINT[], sequence2/qual2 NULL;
  - a caller declaring expect_unaligned=False → BAD_INPUT (aligned unsupported);
  - the one-record-per-read guard: a paired uBAM (unmapped mates sharing a QNAME)
    → BAD_INPUT before the mint;
  - header-only (no records) is terminal NO_DATA (StepNoData);
  - missing input raises FileNotFoundError;

mint_sequence_range is monkey-patched so no live CP is needed. All tests need the
miint extension available — set MIINT_EXTENSION_REPO if your host installs from
the team mirror.
"""

from __future__ import annotations

import asyncio

import duckdb
import pytest
from qiita_common.backend_failure import BackendFailure, FailureKind, StepNoData
from qiita_common.models import TERMINAL_WORK_TICKET_STATES, WorkTicketState

import qiita_compute_orchestrator.jobs.bam_to_parquet as bam_module
from qiita_compute_orchestrator import sequence_range_retry
from qiita_compute_orchestrator.jobs.bam_to_parquet import YAML_STEP_NAME, Inputs, execute
from qiita_compute_orchestrator.sequence_range import (
    MintedSequenceRange,
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

    async def _fake(*, http, prep_sample_idx, count, work_ticket_idx):
        calls.append((prep_sample_idx, count))
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=1000,
            sequence_idx_stop=1000 + count - 1,
        )

    monkeypatch.setattr(sequence_range_retry, "mint_sequence_range", _fake)
    return calls


def _read_parquet(path) -> list[tuple]:
    """Read the reads back. `path` may be a single parquet or the `read/` parts dir —
    the output is a DIRECTORY of part_*.parquet (see _write_read_parts)."""
    target = f"{path}/*.parquet" if path.is_dir() else str(path)
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            "SELECT prep_sample_idx, sequence_idx, read_id, sequence1, qual1, "
            "sequence2, qual2 FROM read_parquet(?) ORDER BY sequence_idx",
            [target],
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
    read_dir = tmp_path / "ws" / "read"
    assert read_dir.is_dir()
    assert sorted(p.name for p in read_dir.glob("*.parquet")) == ["part_00000.parquet"]
    # The intermediate must be gone before return (manifest walker cleanliness).
    assert not (tmp_path / "ws" / "_intermediate_reads.parquet").exists()

    assert fake_mint == [(42, 2)]

    rows = _read_parquet(read_dir)
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
    assert not (tmp_path / "ws" / "read").exists()


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
    assert not (tmp_path / "ws" / "read").exists()


def test_missing_input_raises_filenotfound(fake_mint, tmp_path):
    """No BAM at the path → FileNotFoundError (the dispatcher maps that to
    BAD_INPUT one layer up)."""
    with pytest.raises(FileNotFoundError):
        _run(
            Inputs(bam_path=tmp_path / "nope.sam", prep_sample_idx=1, work_ticket_idx=1),
            tmp_path / "ws",
        )


def test_execute_reuses_range_left_by_a_crashed_attempt(monkeypatch, tmp_path):
    """A 409 on mint is RECOVERED, not fatal: the range a prior attempt minted
    before dying is read back and reused, and the step completes.

    This is what makes the step idempotent across runner retries. The prior
    behaviour — 409 -> UNKNOWN_PERMANENT — is what turned an OOM-killed first
    attempt into a permanent failure on the retry: it masked the OOM behind a mint
    conflict and defeated the runner's OOM memory escalation, which can only pay
    off if the escalated attempt gets past the mint. This is the exact sequence
    that failed nearly every sample on the first real PacBio run.
    """

    async def _conflict(*, http, prep_sample_idx, count, work_ticket_idx):
        raise SequenceRangeAlreadyExists(prep_sample_idx, count)

    async def _existing(*, http, prep_sample_idx):
        # The range the OOM-killed attempt of THIS ticket minted: same count, and
        # minted_by matches, so it is reusable.
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=1000,
            sequence_idx_stop=1001,
            minted_by_work_ticket_idx=1,
            minted_by_work_ticket_state=WorkTicketState.PROCESSING.value,  # still in flight
        )

    monkeypatch.setattr(sequence_range_retry, "mint_sequence_range", _conflict)
    monkeypatch.setattr(sequence_range_retry, "get_sequence_range", _existing)

    sam = tmp_path / "in.sam"
    _write_sam(sam, [_sam_record("r1", "ACGT", "IIII"), _sam_record("r2", "TTTT", "????")])

    _run(Inputs(bam_path=sam, prep_sample_idx=42, work_ticket_idx=1), tmp_path / "ws")

    # Reused the crashed attempt's range rather than consuming a fresh one.
    seqs = [r[1] for r in _read_parquet(tmp_path / "ws" / "read")]
    assert seqs == [1000, 1001]


def test_execute_range_left_with_a_different_count_is_bad_input(monkeypatch, tmp_path):
    """A read-back range whose width doesn't match this attempt's read count must
    NOT be reused — the written sequence_idx values would mismatch
    qiita.sequence_range at registration."""

    async def _conflict(*, http, prep_sample_idx, count, work_ticket_idx):
        raise SequenceRangeAlreadyExists(prep_sample_idx, count)

    async def _wrong_size(*, http, prep_sample_idx):
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=1000,
            sequence_idx_stop=1005,  # 6 indices for a 2-read BAM
            minted_by_work_ticket_idx=1,  # ours...
            # ...and still in flight, so the width check is what fires — not the
            # ownership gate and not the in-flight gate.
            minted_by_work_ticket_state=WorkTicketState.PROCESSING.value,
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


def test_duckdb_memory_limit_tracks_the_slurm_cgroup(fake_mint, monkeypatch, tmp_path):
    """DuckDB's memory_limit is sized from the REAL cgroup, not a literal.

    This is the fix that makes the runner's OOM memory-escalation able to help at
    all: while the limit was hardcoded, doubling the SLURM allocation left DuckDB
    capped at the same in-process value, so the escalated attempt re-OOM'd
    identically. Assert the wiring directly — a regression that re-hardcodes it
    would otherwise keep every other test in this file green.
    """
    seen: list[int] = []
    real_apply = bam_module.apply_duckdb_settings

    def _spy(conn, duckdb_tmp, *, memory_gb, threads):
        seen.append(memory_gb)
        return real_apply(conn, duckdb_tmp, memory_gb=memory_gb, threads=threads)

    monkeypatch.setattr(bam_module, "apply_duckdb_settings", _spy)
    # 64 GB cgroup, minus duckdb_headroom_gb(threads=2) == 3.
    monkeypatch.setenv("SLURM_MEM_PER_NODE", str(64 * 1024))

    sam = tmp_path / "in.sam"
    _write_sam(sam, [_sam_record("r1", "ACGT", "IIII")])
    _run(Inputs(bam_path=sam, prep_sample_idx=1, work_ticket_idx=1), tmp_path / "ws")

    assert seen, "apply_duckdb_settings was never called"
    assert set(seen) == {61}, f"expected the cgroup-derived limit on every connection, got {seen}"


def test_execute_refuses_a_range_minted_by_a_different_ticket(monkeypatch, tmp_path):
    """A range minted by ANOTHER work_ticket must NOT be reused — the sample's reads
    are already registered, and reusing the range would register them a second time.

    This is the guard that makes the 409-reuse safe. The submit-time
    disallow-without-delete gate only blocks NON-terminal tickets, so a COMPLETED
    sample can be resubmitted; without this check the new ticket would mint, 409,
    read back the old range, rewrite the identical sequence_idx values, and
    register-files would add a SECOND DuckLake data file. DuckLake has no
    uniqueness, so every read would silently exist twice.
    """

    async def _conflict(*, http, prep_sample_idx, count, work_ticket_idx):
        raise SequenceRangeAlreadyExists(prep_sample_idx, count)

    async def _someone_elses(*, http, prep_sample_idx):
        # Width matches, so ONLY the ownership check can reject this.
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=1000,
            sequence_idx_stop=1001,
            minted_by_work_ticket_idx=999,  # a different ticket loaded these reads
        )

    monkeypatch.setattr(sequence_range_retry, "mint_sequence_range", _conflict)
    monkeypatch.setattr(sequence_range_retry, "get_sequence_range", _someone_elses)

    sam = tmp_path / "in.sam"
    _write_sam(sam, [_sam_record("r1", "ACGT", "IIII"), _sam_record("r2", "TTTT", "????")])

    with pytest.raises(BackendFailure) as ei:
        _run(Inputs(bam_path=sam, prep_sample_idx=42, work_ticket_idx=1), tmp_path / "ws")

    assert ei.value.kind is FailureKind.UNKNOWN_PERMANENT
    assert ei.value.step_name == YAML_STEP_NAME
    assert "work_ticket 999" in ei.value.reason
    assert "already loaded" in ei.value.reason
    # Nothing was written: the refusal happens before the durable rewrite.
    assert not (tmp_path / "ws" / "read").exists()


def test_execute_refuses_a_range_with_unknown_provenance(monkeypatch, tmp_path):
    """A NULL minted_by (a range predating the column, or one the backfill could not
    attribute) is treated as NOT-mine: fail closed. Reusing a range we cannot prove
    is ours risks duplicating reads that are already in the lake."""

    async def _conflict(*, http, prep_sample_idx, count, work_ticket_idx):
        raise SequenceRangeAlreadyExists(prep_sample_idx, count)

    async def _unattributed(*, http, prep_sample_idx):
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=1000,
            sequence_idx_stop=1001,
            minted_by_work_ticket_idx=None,
        )

    monkeypatch.setattr(sequence_range_retry, "mint_sequence_range", _conflict)
    monkeypatch.setattr(sequence_range_retry, "get_sequence_range", _unattributed)

    sam = tmp_path / "in.sam"
    _write_sam(sam, [_sam_record("r1", "ACGT", "IIII"), _sam_record("r2", "TTTT", "????")])

    with pytest.raises(BackendFailure) as ei:
        _run(Inputs(bam_path=sam, prep_sample_idx=42, work_ticket_idx=1), tmp_path / "ws")

    assert ei.value.kind is FailureKind.UNKNOWN_PERMANENT
    assert "unknown work_ticket" in ei.value.reason
    assert not (tmp_path / "ws" / "read").exists()


@pytest.mark.parametrize("terminal_state", list(TERMINAL_WORK_TICKET_STATES))
def test_execute_refuses_a_range_whose_ticket_is_no_longer_in_flight(
    terminal_state, monkeypatch, tmp_path
):
    """Ownership is necessary but NOT sufficient: MY OWN ticket's range must not be
    re-written once that ticket has left flight.

    The gate is an ALLOWLIST — reuse is legitimate only while the minting ticket is
    still in flight — not a denylist of `completed`. That matters because the failure
    mode is silent: reusing a range whose reads are already registered duplicates them
    in DuckLake, which has no uniqueness. A denylist would let a work_ticket_state
    added later fall through to the reuse path by default — fail-open — and this
    parametrisation is what pins it: it walks EVERY terminal state, so a new one is
    covered the day it is added.

    Reachable if a stale attempt outlives the attempt that finished the ticket (an
    orphaned SLURM job never reaped): it reaches the mint, 409s, reads back a range
    whose minter matches its own idx, and on an ownership check alone would happily
    reuse it and rewrite the output.
    """

    async def _conflict(*, http, prep_sample_idx, count, work_ticket_idx):
        raise SequenceRangeAlreadyExists(prep_sample_idx, count)

    async def _mine_but_terminal(*, http, prep_sample_idx):
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=1000,
            sequence_idx_stop=1001,
            minted_by_work_ticket_idx=1,  # ours — ownership alone would allow reuse
            minted_by_work_ticket_state=terminal_state,  # ...but it is no longer running
        )

    monkeypatch.setattr(sequence_range_retry, "mint_sequence_range", _conflict)
    monkeypatch.setattr(sequence_range_retry, "get_sequence_range", _mine_but_terminal)

    sam = tmp_path / "in.sam"
    _write_sam(sam, [_sam_record("r1", "ACGT", "IIII"), _sam_record("r2", "TTTT", "????")])

    with pytest.raises(BackendFailure) as ei:
        _run(Inputs(bam_path=sam, prep_sample_idx=42, work_ticket_idx=1), tmp_path / "ws")

    assert ei.value.kind is FailureKind.UNKNOWN_PERMANENT
    assert "no longer in flight" in ei.value.reason
    assert terminal_state in ei.value.reason
    assert not (tmp_path / "ws" / "read").exists()

    # The refusal must name a recovery the CP will actually ACCEPT. `/run` takes a
    # ticket in PENDING or FAILED only, so `failed` gets the redrive and every other
    # terminal state gets delete-then-resubmit; pointing `completed` or `no_data` at
    # `ticket run` would send the operator to a 409.
    if terminal_state == WorkTicketState.FAILED.value:
        assert f"qiita ticket run {1}" in ei.value.reason
        assert "DELETE the prep_sample" not in ei.value.reason
    else:
        assert "DELETE the prep_sample" in ei.value.reason
        assert "ticket run" not in ei.value.reason


def test_reads_are_written_as_monotone_disjoint_parts(fake_mint, monkeypatch, tmp_path):
    """The batched write must reproduce EXACTLY what the global sort used to give.

    The old form was one `COPY ... ORDER BY sequence_idx`, a blocking sort over the
    whole seq+qual payload — tens of GB for a HiFi sample, and where the first real
    PacBio run OOM'd. What that sort actually bought was not a globally sorted file
    (PARQUET_OPTS writes row groups in thread-finish order regardless) but tight
    per-row-group min/max on sequence_idx, for DuckLake pruning.

    Batching gets the same thing for free, because the data is already monotone:
    sequence_idx = sequence_index + start - 1. So this pins the properties pruning
    depends on:

      1. every read is written exactly once, with the minted sequence_idx values;
      2. the parts partition the range — part N's max < part N+1's min — so each
         part's row groups carry a tight, disjoint window.

    Forcing a tiny per-part budget makes several parts out of a handful of reads.
    """
    monkeypatch.setattr(bam_module, "_ROWS_PER_PART_TARGET_BYTES", 1)  # ~1 read per part

    reads = [_sam_record(f"r{i}", "ACGT", "IIII") for i in range(1, 8)]
    sam = tmp_path / "in.sam"
    _write_sam(sam, reads)

    _run(Inputs(bam_path=sam, prep_sample_idx=42, work_ticket_idx=1), tmp_path / "ws")

    read_dir = tmp_path / "ws" / "read"
    parts = sorted(read_dir.glob("*.parquet"))
    assert len(parts) > 1, "the budget should have forced a multi-part write"

    # 1. every read exactly once, sequence_idx contiguous from the minted start
    rows = _read_parquet(read_dir)
    assert [r[2] for r in rows] == [f"r{i}" for i in range(1, 8)]
    assert [r[1] for r in rows] == list(range(1000, 1007))

    # 2. the parts are monotone and disjoint — what row-group pruning reads
    bounds = []
    with duckdb.connect(":memory:") as conn:
        for part in parts:
            lo, hi = conn.execute(
                "SELECT min(sequence_idx), max(sequence_idx) FROM read_parquet(?)",
                [str(part)],
            ).fetchone()
            bounds.append((lo, hi))
    for (_, prev_hi), (next_lo, _) in zip(bounds, bounds[1:], strict=False):
        assert prev_hi < next_lo, f"parts overlap: {bounds}"


def test_execute_refuses_when_the_minter_state_is_unknown(monkeypatch, tmp_path):
    """Minter idx matches ours, but its state is absent — refuse.

    The read-back LEFT JOINs the minting ticket precisely so a range whose ticket row
    is gone still comes back; the repo comment there says the caller must read a
    missing state as "cannot prove this is safe to reuse". This gate is the only thing
    that makes that true, and `minted_by_work_ticket_idx` carries no FK, so a dangling
    idx is reachable. Fail closed.
    """

    async def _conflict(*, http, prep_sample_idx, count, work_ticket_idx):
        raise SequenceRangeAlreadyExists(prep_sample_idx, count)

    async def _mine_but_stateless(*, http, prep_sample_idx):
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=1000,
            sequence_idx_stop=1001,
            minted_by_work_ticket_idx=1,  # ours — the ownership check passes
            minted_by_work_ticket_state=None,  # ...but the ticket row is gone
        )

    monkeypatch.setattr(sequence_range_retry, "mint_sequence_range", _conflict)
    monkeypatch.setattr(sequence_range_retry, "get_sequence_range", _mine_but_stateless)

    sam = tmp_path / "in.sam"
    _write_sam(sam, [_sam_record("r1", "ACGT", "IIII"), _sam_record("r2", "TTTT", "????")])

    with pytest.raises(BackendFailure) as ei:
        _run(Inputs(bam_path=sam, prep_sample_idx=42, work_ticket_idx=1), tmp_path / "ws")

    assert ei.value.kind is FailureKind.UNKNOWN_PERMANENT
    assert "no longer in flight" in ei.value.reason
    # No ticket row to redrive — `/run` would 404. Delete-first is the only recovery.
    assert "DELETE the prep_sample" in ei.value.reason
    assert "ticket run" not in ei.value.reason
    assert not (tmp_path / "ws" / "read").exists()
