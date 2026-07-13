"""Isolated unit tests for `fastq_to_parquet.execute`.

Calls `execute()` directly (not through LocalBackend / run_native_job)
so failures here point at the conversion logic, not framework wiring.
The full-stack happy path lives in
`tests/integration/test_native_step_smoke.py`; this file covers
branches that path doesn't exercise:

  - FASTQ with duplicate sequences (no dedup; sequence_idx still unique).
  - FASTA input (no quality scores) writes NULL into the qual1 column.
  - Empty input is a terminal no-data outcome (StepNoData → the runner's
    NO_DATA transition); no Parquet is written and no range is minted.
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
from qiita_common.backend_failure import StepNoData

from qiita_compute_orchestrator import sequence_range_retry
from qiita_compute_orchestrator.jobs.fastq_to_parquet import (
    YAML_STEP_NAME,
    Inputs,
    execute,
)
from qiita_compute_orchestrator.read_count import ReadCount
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

    async def _fake(*, http, prep_sample_idx, count, work_ticket_idx):
        calls.append((prep_sample_idx, count))
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=1000,
            sequence_idx_stop=1000 + count - 1,
        )

    monkeypatch.setattr(sequence_range_retry, "mint_sequence_range", _fake)
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
    assert parquet.name == "read.parquet"
    assert parquet.exists()
    # The workspace is exposed as read_staging_dir so a register-files step loads
    # read.parquet into the DuckLake `read` table.
    assert outputs["read_staging_dir"] == tmp_path / "ws"

    # mint was called once with the exact count.
    assert fake_mint == [(42, 3)]

    # Every read carries the prep_sample_idx scope/prune column.
    with duckdb.connect(":memory:") as conn:
        ps = conn.execute(
            f"SELECT DISTINCT prep_sample_idx FROM read_parquet('{parquet}')"
        ).fetchall()
    assert ps == [(42,)]

    # Raw read count: 3 single-end reads → 3 reads r1r2 (R1 only),
    # layout 'single'.
    rc = ReadCount.model_validate_json(outputs["raw_read_count"].read_text())
    assert (rc.read_pairs, rc.read_count_r1r2, rc.layout) == (3, 3, "single")

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

    # Raw read count: 2 pairs → count(*)=2 + count(sequence2)=2 = 4
    # reads r1r2 (both mates), layout 'paired'.
    rc = ReadCount.model_validate_json(outputs["raw_read_count"].read_text())
    assert (rc.read_pairs, rc.read_count_r1r2, rc.layout) == (2, 4, "paired")

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


def test_execute_empty_input_is_no_data(fake_mint, tmp_path):
    """An empty input file raises StepNoData — a TERMINAL no-data outcome
    (an empty well: a blank, a no-template control, or a failed-yield well),
    NOT a failure. No range is minted and no read.parquet is written: the CP
    would reject count=0 anyway, and the runner transitions the ticket to
    NO_DATA so the pool can still reach a "done" state.

    Detection runs in Python (decompressed-stream peek) before any DuckDB
    work, so it doesn't depend on miint's exception wording — see
    miint.is_empty_sequence_file. The signal carries the YAML step name."""
    empty = tmp_path / "empty.fastq"
    empty.write_text("")

    with pytest.raises(StepNoData, match="contains no records") as excinfo:
        _run(
            Inputs(fastq_path=empty, prep_sample_idx=42, work_ticket_idx=1),
            tmp_path / "ws",
        )
    assert excinfo.value.step_name == YAML_STEP_NAME
    # No mint call for an empty sample (no_data happens before phase 3).
    assert fake_mint == []
    # No read.parquet emitted — workspace contains no Parquet artifact.
    workspace = tmp_path / "ws"
    if workspace.exists():
        assert list(workspace.glob("*.parquet")) == []


def test_execute_empty_gzipped_input_is_no_data(fake_mint, tmp_path):
    """The decompressed-stream peek catches `.fastq.gz` files that
    decompress to zero bytes (the realistic empty-case — a sequencer
    that produced no reads but still gzipped the output to ~20 bytes
    of framing). File size alone wouldn't catch this. Same terminal
    no-data outcome (StepNoData) as the plain empty case."""
    import gzip

    empty_gz = tmp_path / "empty.fastq.gz"
    # `wb` with no writes leaves the file as gzip framing only —
    # decompressed content is 0 bytes.
    with gzip.open(empty_gz, "wb"):
        pass

    with pytest.raises(StepNoData, match="contains no records"):
        _run(
            Inputs(fastq_path=empty_gz, prep_sample_idx=42, work_ticket_idx=1),
            tmp_path / "ws",
        )
    assert fake_mint == []


def test_execute_empty_reverse_fastq_is_no_data(fake_mint, tmp_path):
    """A non-empty R1 with an empty R2 is also no-data — the pair has no
    reads to write, so the well produced nothing. StepNoData, not a
    failure; no mint, no Parquet."""
    r1 = _minimal_fastq(tmp_path)
    r2 = tmp_path / "r2_empty.fastq"
    r2.write_text("")

    with pytest.raises(StepNoData, match="reverse FASTQ file contains no records"):
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


# --- Mint-side failure mapping (S1-B) -------------------------------------
#
# The mint helper raises typed Python exceptions; the framework dispatcher
# only wraps NotImplementedError / FileNotFoundError / ValueError. So
# execute() itself maps the mint exceptions to typed BackendFailures.
# These tests inject a mint that raises and assert the right
# (kind, step_name, reason) on the resulting BackendFailure.


def _make_failing_mint(monkeypatch, exc):
    """Patch mint_sequence_range with a function that raises `exc`."""

    async def _raise(*, http, prep_sample_idx, count, work_ticket_idx):
        raise exc

    monkeypatch.setattr(sequence_range_retry, "mint_sequence_range", _raise)


def _minimal_fastq(tmp_path):
    """A single-read FASTQ — enough to drive phase 1+2 to a non-zero
    count so phase 3 actually runs."""
    fastq = tmp_path / "in.fastq"
    fastq.write_text("@r1\nACGT\n+\n!!!!\n")
    return fastq


# Read count of `_minimal_fastq`, so a mint/read-back range can be sized to match it.
_MINIMAL_FASTQ_READS = 1


# 3 reads; r1 and r3 share the same sequence intentionally so the
# happy-path test can assert duplicate-sequence preservation. Quality
# strings are calibrated to phred values [0,0,0,0] / [2,2,2,2] / [3,3,3,3]
# so quality-decode assertions read cleanly.
_THREE_READ_FASTQ_CONTENT = "@r1\nACGT\n+\n!!!!\n@r2\nTGCA\n+\n####\n@r3\nACGT\n+\n$$$$\n"


def _three_read_fastq(tmp_path):
    """A 3-read FASTQ — drives phase 2 to count=3."""
    fastq = tmp_path / "in.fastq"
    fastq.write_text(_THREE_READ_FASTQ_CONTENT)
    return fastq


def _assert_mint_not_called(monkeypatch):
    """Patch fastq_module.mint_sequence_range to raise loudly if
    invoked. Use in recovery-path tests where the mint MUST be skipped —
    any invocation fails the test with a clear message instead of
    silently consuming a fresh range."""

    async def _should_not_run(*, http, prep_sample_idx, count, work_ticket_idx):
        raise AssertionError("mint must not be called on the recovery path")

    monkeypatch.setattr(sequence_range_retry, "mint_sequence_range", _should_not_run)


def test_execute_reuses_range_left_by_a_crashed_attempt(monkeypatch, tmp_path):
    """A 409 on mint is RECOVERED, not fatal: the range a prior attempt minted
    before dying is read back and reused, and the step completes.

    This is what makes the step idempotent across runner retries. The prior
    behaviour — 409 -> UNKNOWN_PERMANENT — turned every transient mid-step failure
    (an OOM kill, a walltime kill) into a permanent one on the next attempt, masked
    the real cause behind a mint conflict, and defeated the runner's OOM memory
    escalation (the escalated attempt could never get past the mint to benefit)."""
    from qiita_compute_orchestrator.sequence_range import (
        MintedSequenceRange,
        SequenceRangeAlreadyExists,
    )

    async def _conflict(*, http, prep_sample_idx, count, work_ticket_idx):
        raise SequenceRangeAlreadyExists(prep_sample_idx, count)

    async def _existing(*, http, prep_sample_idx):
        # The range the crashed attempt of THIS ticket minted: same count, and
        # minted_by matches, so it is reusable.
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=1000,
            sequence_idx_stop=1000 + _MINIMAL_FASTQ_READS - 1,
            minted_by_work_ticket_idx=1,
        )

    monkeypatch.setattr(sequence_range_retry, "mint_sequence_range", _conflict)
    monkeypatch.setattr(sequence_range_retry, "get_sequence_range", _existing)

    ws = tmp_path / "ws"
    out = _run(
        Inputs(fastq_path=_minimal_fastq(tmp_path), prep_sample_idx=42, work_ticket_idx=1),
        ws,
    )

    rows = _read_parquet(out["read_staging_dir"] / "read.parquet")
    assert len(rows) == _MINIMAL_FASTQ_READS
    # Reused the existing range's start rather than consuming a fresh one.
    assert [r[0] for r in rows] == list(range(1000, 1000 + _MINIMAL_FASTQ_READS))


def test_execute_range_left_with_a_different_count_is_bad_input(monkeypatch, tmp_path):
    """A read-back range whose size doesn't match this attempt's read count must
    NOT be reused — reusing it would write sequence_idx values that mismatch
    qiita.sequence_range at registration. Unreachable while the input is immutable
    between submit and execution; fail loudly if it ever isn't."""
    from qiita_common.backend_failure import BackendFailure, FailureKind

    from qiita_compute_orchestrator.sequence_range import (
        MintedSequenceRange,
        SequenceRangeAlreadyExists,
    )

    async def _conflict(*, http, prep_sample_idx, count, work_ticket_idx):
        raise SequenceRangeAlreadyExists(prep_sample_idx, count)

    async def _wrong_size(*, http, prep_sample_idx):
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=1000,
            sequence_idx_stop=1000 + _MINIMAL_FASTQ_READS,  # one too many
            minted_by_work_ticket_idx=1,  # ours, so the width check is what fires
        )

    monkeypatch.setattr(sequence_range_retry, "mint_sequence_range", _conflict)
    monkeypatch.setattr(sequence_range_retry, "get_sequence_range", _wrong_size)

    with pytest.raises(BackendFailure) as ei:
        _run(
            Inputs(fastq_path=_minimal_fastq(tmp_path), prep_sample_idx=42, work_ticket_idx=1),
            tmp_path / "ws",
        )

    assert ei.value.kind is FailureKind.BAD_INPUT
    assert ei.value.step_name == YAML_STEP_NAME
    assert "must match the prior mint count exactly" in ei.value.reason


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
    assert ei.value.step_name == YAML_STEP_NAME
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
    assert ei.value.step_name == YAML_STEP_NAME
    assert "HTTP 401" in ei.value.reason
    # The reason points operators at the runbook.
    assert "compute-service-account-provisioning" in ei.value.reason


@pytest.fixture
def no_backoff(monkeypatch):
    """Zero the CP-callback retry backoff so the retry tests don't sleep."""
    monkeypatch.setattr(sequence_range_retry, "CP_RETRY_BACKOFF_BASE_S", 0)


def _status_error(status: int):
    import httpx

    request = httpx.Request("POST", "http://cp.test/api/v1/sequence-range")
    return httpx.HTTPStatusError(
        f"HTTP {status}", request=request, response=httpx.Response(status, request=request)
    )


def test_execute_maps_exhausted_5xx_to_control_plane_unreachable(monkeypatch, no_backoff, tmp_path):
    """A persistent HTTP 5xx on the mint callback is retried in-job and, once
    exhausted, maps to RETRIABLE CONTROL_PLANE_UNREACHABLE — not the old
    permanent classification — so the runner re-dispatches rather than failing
    the ingest over an infra blip."""
    from qiita_common.backend_failure import BackendFailure, FailureKind

    calls: list[int] = []

    async def _raise_503(*, http, prep_sample_idx, count, work_ticket_idx):
        calls.append(prep_sample_idx)
        raise _status_error(503)

    monkeypatch.setattr(sequence_range_retry, "mint_sequence_range", _raise_503)

    with pytest.raises(BackendFailure) as ei:
        _run(
            Inputs(fastq_path=_minimal_fastq(tmp_path), prep_sample_idx=42, work_ticket_idx=1),
            tmp_path / "ws",
        )

    assert (
        len(calls) == sequence_range_retry.CP_RETRY_MAX_ATTEMPTS
    )  # retried, not failed on attempt 1
    assert ei.value.kind is FailureKind.CONTROL_PLANE_UNREACHABLE
    assert ei.value.transient is True
    assert ei.value.step_name == YAML_STEP_NAME
    assert "HTTP 503" in ei.value.reason


def test_execute_maps_transport_error_to_control_plane_unreachable(
    monkeypatch, no_backoff, tmp_path
):
    """A pure transport error (connection reset / read timeout) — which never
    reaches raise_for_status and previously escaped the handler entirely,
    classifying permanent downstream — now maps to retriable
    CONTROL_PLANE_UNREACHABLE."""
    import httpx
    from qiita_common.backend_failure import BackendFailure, FailureKind

    _make_failing_mint(monkeypatch, httpx.ConnectError("connection reset"))

    with pytest.raises(BackendFailure) as ei:
        _run(
            Inputs(fastq_path=_minimal_fastq(tmp_path), prep_sample_idx=42, work_ticket_idx=1),
            tmp_path / "ws",
        )

    assert ei.value.kind is FailureKind.CONTROL_PLANE_UNREACHABLE
    assert ei.value.transient is True
    assert ei.value.step_name == YAML_STEP_NAME
    assert "transport error (ConnectError)" in ei.value.reason


def test_execute_transient_5xx_on_mint_self_heals(monkeypatch, no_backoff, tmp_path):
    """Two transient 5xx blips then success: the in-job retry self-heals and the
    step writes its reads against the eventually-minted range — never failing."""
    calls: list[int] = []

    async def _flaky(*, http, prep_sample_idx, count, work_ticket_idx):
        calls.append(prep_sample_idx)
        if len(calls) <= 2:
            raise _status_error(502)
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=1000,
            sequence_idx_stop=1000 + count - 1,
        )

    monkeypatch.setattr(sequence_range_retry, "mint_sequence_range", _flaky)

    outputs = _run(
        Inputs(fastq_path=_three_read_fastq(tmp_path), prep_sample_idx=42, work_ticket_idx=1),
        tmp_path / "ws",
    )

    assert len(calls) == 3  # two retries + the successful attempt
    rows = _read_parquet(outputs["reads"])
    assert [r[0] for r in rows] == [1000, 1001, 1002]
