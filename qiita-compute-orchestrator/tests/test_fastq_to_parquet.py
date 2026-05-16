"""Isolated unit tests for `fastq_to_parquet.execute`.

Calls `execute()` directly (not through LocalBackend / run_native_job)
so failures here point at the conversion logic, not framework wiring.
The full-stack happy path lives in
`tests/integration/test_native_step_smoke.py`; this file covers
branches that path doesn't exercise:

  - FASTQ with duplicate sequences (no dedup; sequence_idx still unique).
  - FASTA input (no quality scores) writes NULL into the quality column.
  - Empty input writes an empty (header-only) Parquet rather than
    failing, and DOES NOT mint a range (the CP would reject count=0).
  - Missing input path raises FileNotFoundError (the framework
    dispatcher maps that to BackendFailure(BAD_INPUT) one layer up).

mint_sequence_range is monkey-patched in every test that exercises the
mint path so the HTTP call doesn't need a live CP. The fake returns a
deterministic MintedSequenceRange and records the calls; assertions verify
the count passed in and the sequence_idx values written to the Parquet.

All tests need the miint extension available — set
MIINT_EXTENSION_REPO if your host installs from the team mirror.
"""

from __future__ import annotations

import asyncio

import duckdb
import pytest

import qiita_compute_orchestrator.jobs.fastq_to_parquet as fastq_module
from qiita_compute_orchestrator.jobs.fastq_to_parquet import Inputs, execute
from qiita_compute_orchestrator.sequence_range import MintedSequenceRange


def _run(inputs: Inputs, workspace) -> dict:
    """Drive the coroutine synchronously so tests stay sync-styled.
    Mirrors the run_native_job → execute boundary without dragging in
    the dispatcher's BackendFailure wrapping."""
    return asyncio.run(execute(inputs, workspace))


def _read_parquet(path) -> list[tuple]:
    """Materialize reads.parquet into a list of tuples ordered by
    sequence_idx (the file's natural sort)."""
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            "SELECT sequence_idx, read_id, sequence, quality, sequence_length"
            f" FROM read_parquet('{path}') ORDER BY sequence_idx"
        ).fetchall()


@pytest.fixture
def fake_mint(monkeypatch):
    """Replace mint_sequence_range with a recorder. Returns the list of
    recorded calls; each entry is (prep_sample_idx, count). The fake
    hands back a MintedSequenceRange starting at 1000 (a non-zero base so the
    "+ row_number() - 1" arithmetic is visible)."""
    calls: list[tuple[int, int]] = []

    async def _fake(*, http, prep_sample_idx, count):
        calls.append((prep_sample_idx, count))
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=1000,
            sequence_idx_stop=1000 + count - 1,
        )

    monkeypatch.setattr(fastq_module, "mint_sequence_range", _fake)
    return calls


def test_execute_writes_reads_parquet_for_fastq(fake_mint, tmp_path):
    """Happy path under direct invocation: a 3-read FASTQ with two
    identical sequences round-trips faithfully — both duplicates appear
    as separate rows (no dedup), each gets a unique sequence_idx from
    the minted range, and the qual1 column comes back as a phred-decoded
    UTINYINT[]."""
    fastq = tmp_path / "in.fastq"
    fastq.write_text(
        "@r1\nACGT\n+\n!!!!\n"  # quality "!" * 4 → phred [0, 0, 0, 0]
        "@r2\nTGCA\n+\n####\n"  # quality "#" * 4 → phred [2, 2, 2, 2]
        "@r3\nACGT\n+\n$$$$\n"  # duplicate of r1's sequence
    )

    outputs = _run(
        Inputs(fastq_path=fastq, prep_sample_idx=42, work_ticket_idx=1),
        tmp_path / "ws",
    )
    parquet = outputs["reads"]
    assert parquet.name == "reads.parquet"
    assert parquet.exists()

    # mint was called once with the exact count.
    assert fake_mint == [(42, 3)]

    rows = _read_parquet(parquet)
    assert len(rows) == 3
    # sequence_idx values are contiguous, starting at the minted start (1000).
    assert [r[0] for r in rows] == [1000, 1001, 1002]
    # Duplicate sequences kept; sequence_idx is assigned by read_id sort
    # so r1 (gets 1000) and r3 (gets 1002) keep their identical sequence.
    by_read_id = {r[1]: r for r in rows}
    assert by_read_id["r1"][2] == "ACGT"
    assert by_read_id["r3"][2] == "ACGT"
    # Quality is UTINYINT[] (phred-decoded), not the ASCII string.
    assert by_read_id["r1"][3] == [0, 0, 0, 0]
    assert by_read_id["r2"][3] == [2, 2, 2, 2]
    # sequence_length is BIGINT — fixture is uniformly 4 bp.
    assert {r[4] for r in rows} == {4}


def test_execute_handles_fasta_with_null_quality(fake_mint, tmp_path):
    """FASTA input has no quality line — the Parquet must write NULL
    into the quality column for every row. Confirms the FASTA branch
    on miint's read_fastx and that the output's quality column is
    nullable end-to-end."""
    fasta = tmp_path / "in.fasta"
    fasta.write_text(">r1\nACGT\n>r2\nTGCA\n")

    outputs = _run(
        Inputs(fastq_path=fasta, prep_sample_idx=42, work_ticket_idx=1),
        tmp_path / "ws",
    )
    parquet = outputs["reads"]

    assert fake_mint == [(42, 2)]

    rows = _read_parquet(parquet)
    assert len(rows) == 2
    assert [r[0] for r in rows] == [1000, 1001]
    # Every quality value is None — FASTA has no quality scores.
    assert all(r[3] is None for r in rows)
    # Sequences still round-trip.
    by_read_id = {r[1]: r for r in rows}
    assert by_read_id["r1"][2] == "ACGT"
    assert by_read_id["r2"][2] == "TGCA"


def test_execute_handles_empty_input(fake_mint, tmp_path):
    """An empty input file must produce an empty (header-only) Parquet
    rather than raising, AND must skip the mint (the CP rejects count
    <= 0 — calling it with count=0 would be both wasteful and an
    error). Schema stays the five-column shape so downstream consumers
    don't see a different shape for empty samples."""
    empty = tmp_path / "empty.fastq"
    empty.write_text("")

    outputs = _run(
        Inputs(fastq_path=empty, prep_sample_idx=42, work_ticket_idx=1),
        tmp_path / "ws",
    )
    parquet = outputs["reads"]
    assert parquet.exists()

    # No mint call for an empty sample.
    assert fake_mint == []

    with duckdb.connect(":memory:") as conn:
        n = conn.execute(f"SELECT count(*) FROM read_parquet('{parquet}')").fetchone()[0]
        cols = [
            r[0]
            for r in conn.execute(
                f"SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('{parquet}'))"
            ).fetchall()
        ]
    assert n == 0
    assert cols == ["sequence_idx", "read_id", "sequence", "quality", "sequence_length"]


def test_execute_raises_file_not_found(fake_mint, tmp_path):
    """A missing fastq_path raises FileNotFoundError from `execute`
    itself — the framework dispatcher (run_native_job) is responsible
    for mapping that to BackendFailure(BAD_INPUT) one layer up. The
    mint is NOT called (the check fires before phase 1)."""
    missing = tmp_path / "does-not-exist.fastq"

    with pytest.raises(FileNotFoundError, match="FASTQ file not found"):
        _run(
            Inputs(fastq_path=missing, prep_sample_idx=42, work_ticket_idx=1),
            tmp_path / "ws",
        )
    assert fake_mint == []


# --- Mint-side failure mapping (S1-B) -------------------------------------
#
# The mint helper raises typed Python exceptions; the framework dispatcher
# only wraps NotImplementedError / FileNotFoundError / ValueError. So
# execute() itself maps the mint exceptions to typed BackendFailures.
# These tests inject a mint that raises and assert the right
# (kind, step_name, reason) on the resulting BackendFailure.


def _make_failing_mint(monkeypatch, exc):
    """Patch mint_sequence_range with a function that raises `exc`."""

    async def _raise(*, http, prep_sample_idx, count):
        raise exc

    monkeypatch.setattr(fastq_module, "mint_sequence_range", _raise)


def _minimal_fastq(tmp_path):
    """A single-read FASTQ — enough to drive phase 1+2 to a non-zero
    count so phase 3 actually runs."""
    fastq = tmp_path / "in.fastq"
    fastq.write_text("@r1\nACGT\n+\n!!!!\n")
    return fastq


def test_execute_maps_already_exists_to_unknown_permanent(monkeypatch, tmp_path):
    """SequenceRangeAlreadyExists (CP 409, mid-step failure left a
    range on a previous attempt) -> BackendFailure(UNKNOWN_PERMANENT)
    with step_name='fastq' and a reason that points operators at the
    recovery (DELETE prep_sample + resubmit)."""
    from qiita_common.backend_failure import BackendFailure, FailureKind
    from qiita_common.models import WorkTicketFailureStage

    from qiita_compute_orchestrator.sequence_range import SequenceRangeAlreadyExists

    _make_failing_mint(monkeypatch, SequenceRangeAlreadyExists(42, 1))

    with pytest.raises(BackendFailure) as ei:
        _run(
            Inputs(fastq_path=_minimal_fastq(tmp_path), prep_sample_idx=42, work_ticket_idx=1),
            tmp_path / "ws",
        )

    assert ei.value.kind is FailureKind.UNKNOWN_PERMANENT
    assert ei.value.stage is WorkTicketFailureStage.STEP_RUN
    assert ei.value.step_name == "fastq"
    assert "already has a sequence_range" in ei.value.reason
    # Recovery hint is in the exception's str — the operator needs it.
    assert "deleting the prep_sample" in ei.value.reason


def test_execute_maps_not_eligible_to_bad_input(monkeypatch, tmp_path):
    """PrepSampleNotEligibleForSequenceRange (CP 404, prep_sample
    deleted between submission and step execution) -> BAD_INPUT."""
    from qiita_common.backend_failure import BackendFailure, FailureKind

    from qiita_compute_orchestrator.sequence_range import (
        PrepSampleNotEligibleForSequenceRange,
    )

    _make_failing_mint(monkeypatch, PrepSampleNotEligibleForSequenceRange(42))

    with pytest.raises(BackendFailure) as ei:
        _run(
            Inputs(fastq_path=_minimal_fastq(tmp_path), prep_sample_idx=42, work_ticket_idx=1),
            tmp_path / "ws",
        )

    assert ei.value.kind is FailureKind.BAD_INPUT
    assert ei.value.step_name == "fastq"
    assert "not found or not eligible" in ei.value.reason


def test_execute_maps_401_to_contract_violation(monkeypatch, tmp_path):
    """HTTP 401 from CP (bad/missing SA PAT) -> CONTRACT_VIOLATION
    with a reason that points at the SA-provisioning runbook."""
    import httpx
    from qiita_common.backend_failure import BackendFailure, FailureKind

    err = httpx.HTTPStatusError(
        "Unauthorized",
        request=httpx.Request("POST", "http://cp.test/api/v1/sequence-range"),
        response=httpx.Response(401),
    )
    _make_failing_mint(monkeypatch, err)

    with pytest.raises(BackendFailure) as ei:
        _run(
            Inputs(fastq_path=_minimal_fastq(tmp_path), prep_sample_idx=42, work_ticket_idx=1),
            tmp_path / "ws",
        )

    assert ei.value.kind is FailureKind.CONTRACT_VIOLATION
    assert ei.value.step_name == "fastq"
    assert "HTTP 401" in ei.value.reason
    # The reason points operators at the runbook.
    assert "compute-service-account-provisioning" in ei.value.reason


def test_execute_maps_5xx_to_unknown_permanent(monkeypatch, tmp_path):
    """HTTP 5xx from CP (CP DB error, infra blip) -> UNKNOWN_PERMANENT.
    Conservative today; a follow-up could add a retriable
    CP_UNREACHABLE FailureKind."""
    import httpx
    from qiita_common.backend_failure import BackendFailure, FailureKind

    err = httpx.HTTPStatusError(
        "Internal Server Error",
        request=httpx.Request("POST", "http://cp.test/api/v1/sequence-range"),
        response=httpx.Response(503, content=b"service unavailable"),
    )
    _make_failing_mint(monkeypatch, err)

    with pytest.raises(BackendFailure) as ei:
        _run(
            Inputs(fastq_path=_minimal_fastq(tmp_path), prep_sample_idx=42, work_ticket_idx=1),
            tmp_path / "ws",
        )

    assert ei.value.kind is FailureKind.UNKNOWN_PERMANENT
    assert ei.value.step_name == "fastq"
    assert "HTTP 503" in ei.value.reason
