"""Isolated unit tests for `fastq_to_parquet.execute`.

Calls `execute()` directly (not through LocalBackend / run_native_job)
so failures here point at the conversion logic, not framework wiring.
The full-stack happy path lives in
`tests/integration/test_native_step_smoke.py`; this file covers
branches that path doesn't exercise:

  - FASTQ with duplicate sequences (no dedup; sequence_idx still unique).
  - FASTA input (no quality scores) writes NULL into the qual1 column.
  - Empty input is rejected as BAD_INPUT (ValueError → BackendFailure
    one layer up); no Parquet is written and no range is minted.
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
            "SELECT sequence_idx, read_id, sequence1, qual1"
            f" FROM read_parquet('{path}') ORDER BY sequence_idx"
        ).fetchall()


@pytest.fixture
def fake_mint(monkeypatch):
    """Replace mint_sequence_range with a recorder. Returns the list of
    recorded calls; each entry is (prep_sample_idx, count). The fake
    hands back a MintedSequenceRange starting at 1000 (a non-zero base
    so the `sequence_index + start - 1` arithmetic is visible)."""
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
    fastq = _three_read_fastq(tmp_path)

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
    # Duplicate sequences kept; sequence_idx is assigned in miint's
    # file-order via `sequence_index + start - 1` (r1→1000, r2→1001,
    # r3→1002 since the fixture writes them in that order), and r1/r3
    # keep their identical sequence.
    by_read_id = {r[1]: r for r in rows}
    assert by_read_id["r1"][2] == "ACGT"
    assert by_read_id["r3"][2] == "ACGT"
    # qual1 is UTINYINT[] (phred-decoded), not the ASCII string.
    assert by_read_id["r1"][3] == [0, 0, 0, 0]
    assert by_read_id["r2"][3] == [2, 2, 2, 2]


def test_execute_handles_fasta_with_null_quality(fake_mint, tmp_path):
    """FASTA input has no quality line — the Parquet must write NULL
    into the qual1 column for every row. Confirms the FASTA branch
    on miint's read_fastx and that the output's qual1 column is
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
    # Every qual1 value is None — FASTA has no quality scores.
    assert all(r[3] is None for r in rows)
    # Sequences still round-trip.
    by_read_id = {r[1]: r for r in rows}
    assert by_read_id["r1"][2] == "ACGT"
    assert by_read_id["r2"][2] == "TGCA"


def test_execute_writes_paired_reads_with_shared_sequence_idx(fake_mint, tmp_path):
    """Paired-end happy path: R1 + R2 with matching read_ids produce
    ONE row per pair (not per read). The pair shares a single
    sequence_idx — paired reads correspond to one molecular event and
    must not be assigned independent identifiers. sequence2/qual2
    carry the R2 strand."""
    r1 = tmp_path / "r1.fastq"
    r2 = tmp_path / "r2.fastq"
    # Two pairs: (r1,r2) — sequences differ between mates so R1 vs R2
    # columns can be distinguished. Quality chars give phred [0,0,0,0]
    # on R1 and [2,2,2,2] on R2.
    r1.write_text("@r1\nACGT\n+\n!!!!\n@r2\nTGCA\n+\n!!!!\n")
    r2.write_text("@r1\nGGGG\n+\n####\n@r2\nAAAA\n+\n####\n")

    outputs = _run(
        Inputs(
            fastq_path=r1,
            reverse_fastq_path=r2,
            prep_sample_idx=42,
            work_ticket_idx=1,
        ),
        tmp_path / "ws",
    )
    parquet = outputs["reads"]

    # Mint called once with count=2 (pairs), NOT count=4 (individual reads).
    assert fake_mint == [(42, 2)]

    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            "SELECT sequence_idx, read_id, sequence1, qual1,"
            f" sequence2, qual2 FROM read_parquet('{parquet}')"
            " ORDER BY sequence_idx"
        ).fetchall()
    assert len(rows) == 2
    assert [r[0] for r in rows] == [1000, 1001]
    by_read_id = {r[1]: r for r in rows}
    # R1 columns
    assert by_read_id["r1"][2] == "ACGT"
    assert by_read_id["r1"][3] == [0, 0, 0, 0]
    # R2 columns — distinct from R1 to prove they aren't crossed
    assert by_read_id["r1"][4] == "GGGG"
    assert by_read_id["r1"][5] == [2, 2, 2, 2]


def test_execute_unpaired_emits_null_paired_columns(fake_mint, tmp_path):
    """Unpaired (single-end) input still emits the 6-column uniform
    schema; sequence2 and qual2 are NULL. Downstream readers see one
    schema regardless of paired-vs-single — paired-ness is data, not
    schema shape."""
    fastq = _minimal_fastq(tmp_path)
    outputs = _run(
        Inputs(fastq_path=fastq, prep_sample_idx=42, work_ticket_idx=1),
        tmp_path / "ws",
    )
    parquet = outputs["reads"]

    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            "SELECT sequence2, qual2 FROM read_parquet(?) ORDER BY sequence_idx",
            [str(parquet)],
        ).fetchall()
    assert all(r[0] is None for r in rows)
    assert all(r[1] is None for r in rows)


def test_execute_raises_on_missing_reverse_fastq(fake_mint, tmp_path):
    """A reverse_fastq_path that doesn't resolve on the filesystem
    raises FileNotFoundError before any DuckDB work — the framework
    dispatcher maps that to BackendFailure(BAD_INPUT) one layer up."""
    fastq = _minimal_fastq(tmp_path)
    missing = tmp_path / "no-such-r2.fastq"

    with pytest.raises(FileNotFoundError, match="reverse FASTQ file not found"):
        _run(
            Inputs(
                fastq_path=fastq,
                reverse_fastq_path=missing,
                prep_sample_idx=42,
                work_ticket_idx=1,
            ),
            tmp_path / "ws",
        )
    assert fake_mint == []


def test_execute_paired_mate_id_mismatch_surfaces_as_duckdb_error(fake_mint, tmp_path):
    """When R1 and R2 carry different read_ids at the same position,
    miint's SequenceReader::read_pe calls check_ids and throws; that
    surfaces as a duckdb.Error here. (The framework dispatcher does
    not wrap duckdb.Error today — these propagate so the orchestrator
    log carries the full message.) The mint is NOT reached because
    the failure happens in phase 1."""
    r1 = tmp_path / "r1.fastq"
    r2 = tmp_path / "r2.fastq"
    r1.write_text("@r1\nACGT\n+\n!!!!\n")
    r2.write_text("@different\nGGGG\n+\n####\n")

    with pytest.raises(duckdb.Error):
        _run(
            Inputs(
                fastq_path=r1,
                reverse_fastq_path=r2,
                prep_sample_idx=42,
                work_ticket_idx=1,
            ),
            tmp_path / "ws",
        )
    assert fake_mint == []


def test_execute_rejects_empty_input(fake_mint, tmp_path):
    """An empty input file raises ValueError (the framework dispatcher
    maps that to BAD_INPUT) instead of producing a zero-row Parquet.
    No Parquet is written and the mint is NOT called — the CP would
    reject count=0 anyway, and emitting a zero-row artifact masks
    upstream data problems (a sequencing run that produced nothing
    should surface, not silently land as an empty result).

    Detection runs in Python (decompressed-stream peek) before any
    DuckDB work, so it doesn't depend on miint's exception wording —
    see miint.is_empty_sequence_file and issue #39."""
    empty = tmp_path / "empty.fastq"
    empty.write_text("")

    with pytest.raises(ValueError, match="contains no records"):
        _run(
            Inputs(fastq_path=empty, prep_sample_idx=42, work_ticket_idx=1),
            tmp_path / "ws",
        )
    # No mint call for an empty sample (failure happens before phase 3).
    assert fake_mint == []
    # No reads.parquet emitted — workspace contains no Parquet artifact.
    workspace = tmp_path / "ws"
    if workspace.exists():
        assert list(workspace.glob("*.parquet")) == []


def test_execute_rejects_empty_gzipped_input(fake_mint, tmp_path):
    """The decompressed-stream peek catches `.fastq.gz` files that
    decompress to zero bytes (the realistic empty-case — a sequencer
    that produced no reads but still gzipped the output to ~20 bytes
    of framing). File size alone wouldn't catch this."""
    import gzip

    empty_gz = tmp_path / "empty.fastq.gz"
    # `wb` with no writes leaves the file as gzip framing only —
    # decompressed content is 0 bytes.
    with gzip.open(empty_gz, "wb"):
        pass

    with pytest.raises(ValueError, match="contains no records"):
        _run(
            Inputs(fastq_path=empty_gz, prep_sample_idx=42, work_ticket_idx=1),
            tmp_path / "ws",
        )
    assert fake_mint == []


def test_execute_rejects_empty_reverse_fastq(fake_mint, tmp_path):
    """A non-empty R1 with an empty R2 is BAD_INPUT — paired-end input
    requires both mates to carry records (mismatched counts would
    surface from miint downstream anyway, but we raise the clearer
    error up front)."""
    r1 = _minimal_fastq(tmp_path)
    r2 = tmp_path / "r2_empty.fastq"
    r2.write_text("")

    with pytest.raises(ValueError, match="reverse FASTQ file contains no records"):
        _run(
            Inputs(
                fastq_path=r1,
                reverse_fastq_path=r2,
                prep_sample_idx=42,
                work_ticket_idx=1,
            ),
            tmp_path / "ws",
        )
    assert fake_mint == []


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


# --- E-operator recovery path: pre_minted_range short-circuits mint ----


def test_execute_recovery_skips_mint_and_uses_supplied_range(monkeypatch, tmp_path):
    """When Inputs.pre_minted_range is set, phase 3 is skipped entirely:
    no HTTP mint call is made (the patched mint would raise loudly), and
    the output Parquet's sequence_idx column starts at the supplied
    range's `sequence_idx_start`."""
    from qiita_compute_orchestrator.jobs.fastq_to_parquet import PreMintedRange

    _assert_mint_not_called(monkeypatch)

    outputs = _run(
        Inputs(
            fastq_path=_three_read_fastq(tmp_path),
            prep_sample_idx=42,
            work_ticket_idx=1,
            pre_minted_range=PreMintedRange(sequence_idx_start=5000, sequence_idx_stop=5002),
        ),
        tmp_path / "ws",
    )
    parquet = outputs["reads"]
    assert parquet.exists()

    rows = _read_parquet(parquet)
    assert [r[0] for r in rows] == [5000, 5001, 5002]


def test_execute_recovery_rejects_count_mismatch(monkeypatch, tmp_path):
    """A pre_minted_range whose (stop - start + 1) doesn't match the
    FASTQ's read count surfaces as BackendFailure(BAD_INPUT). A stale
    recovery (different mint count) must fail loudly rather than write
    a Parquet that mismatches qiita.sequence_range at registration."""
    from qiita_common.backend_failure import BackendFailure, FailureKind
    from qiita_common.models import WorkTicketFailureStage

    from qiita_compute_orchestrator.jobs.fastq_to_parquet import PreMintedRange

    _assert_mint_not_called(monkeypatch)

    # 3-read FASTQ but recovery declares 5 indices — mismatch.
    with pytest.raises(BackendFailure) as ei:
        _run(
            Inputs(
                fastq_path=_three_read_fastq(tmp_path),
                prep_sample_idx=42,
                work_ticket_idx=1,
                pre_minted_range=PreMintedRange(sequence_idx_start=5000, sequence_idx_stop=5004),
            ),
            tmp_path / "ws",
        )
    assert ei.value.kind is FailureKind.BAD_INPUT
    assert ei.value.stage is WorkTicketFailureStage.STEP_RUN
    assert ei.value.step_name == "fastq"
    assert "5 indices" in ei.value.reason
    assert "3 reads" in ei.value.reason


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


# 3 reads; r1 and r3 share the same sequence intentionally so the
# happy-path test can assert duplicate-sequence preservation. Quality
# strings are calibrated to phred values [0,0,0,0] / [2,2,2,2] / [3,3,3,3]
# so quality-decode assertions read cleanly.
_THREE_READ_FASTQ_CONTENT = "@r1\nACGT\n+\n!!!!\n@r2\nTGCA\n+\n####\n@r3\nACGT\n+\n$$$$\n"


def _three_read_fastq(tmp_path):
    """A 3-read FASTQ — drives phase 2 to count=3 and gives recovery-
    path tests a known size for the pre_minted_range round-trip."""
    fastq = tmp_path / "in.fastq"
    fastq.write_text(_THREE_READ_FASTQ_CONTENT)
    return fastq


def _assert_mint_not_called(monkeypatch):
    """Patch fastq_module.mint_sequence_range to raise loudly if
    invoked. Use in recovery-path tests where the mint MUST be skipped —
    any invocation fails the test with a clear message instead of
    silently consuming a fresh range."""

    async def _should_not_run(*, http, prep_sample_idx, count):
        raise AssertionError("mint must not be called on the recovery path")

    monkeypatch.setattr(fastq_module, "mint_sequence_range", _should_not_run)


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
