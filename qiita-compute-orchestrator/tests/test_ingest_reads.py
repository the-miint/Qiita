"""Isolated unit tests for `ingest_reads.execute` — the pool-level read
storage step of the bcl-convert workflow.

Calls `execute()` directly (not through LocalBackend) so failures point at the
ingest loop, not framework wiring. Covers the branches the split introduced:

  - Happy path: each pool sample's FASTQ is parsed once, a range minted, and the
    full reads written to BOTH the durable staged copy
    (compute_reads_staging_path) and the register part (read/<idx>.parquet,
    hardlinked to the durable copy).
  - Empty well: a zero-record FASTQ is skipped (no mint, no reads), not an error.
  - Missing required R1: collected and the step fails BAD_INPUT.
  - Idempotent re-run: a sample whose durable copy already exists is skipped (no
    re-mint) but its register part is re-linked.
  - Range reuse: a sample whose durable copy is absent but whose range already
    exists (prior attempt minted then crashed) reuses the existing range rather
    than failing on the 409; count mismatch and concurrent-deletion are mapped
    to BAD_INPUT / UNKNOWN_PERMANENT.
  - Transient CP callback errors: a 5xx / transport blip on the sequence-range
    mint (or reuse read-back) self-heals via in-job retry; an exhausted-retry
    transient error classifies retriable (CONTROL_PLANE_UNREACHABLE), while
    401/403 and other 4xx stay permanent and are not retried.
  - All-empty pool: StepNoData (the whole ticket is no-data).

mint_sequence_range / get_sequence_range are monkey-patched so no live CP is
needed. miint must be available (set MIINT_EXTENSION_REPO for the team mirror).
"""

from __future__ import annotations

import asyncio
import gzip

import duckdb
import httpx
import pytest
from qiita_common.api_paths import compute_reads_staging_path
from qiita_common.backend_failure import BackendFailure, FailureKind, StepNoData

import qiita_compute_orchestrator.jobs.ingest_reads as ingest_module
import qiita_compute_orchestrator.sequence_range_retry as retry_module
from qiita_compute_orchestrator.jobs.ingest_reads import Inputs, execute
from qiita_compute_orchestrator.sequence_range import (
    MintedSequenceRange,
    SequenceRangeAlreadyExists,
)

# The ticket every Inputs in this file is built with. A read-back fake claims it so
# the reuse path sees a range minted by ITS OWN ticket — the only reusable case.
_WORK_TICKET_IDX = 1


def _run(inputs: Inputs, workspace) -> dict:
    return asyncio.run(execute(inputs, workspace))


@pytest.fixture
def fake_mint(monkeypatch):
    """Replace mint_sequence_range with a recorder. Returns the list of
    (prep_sample_idx, count) calls; each mint starts at a per-sample base so the
    written sequence_idx values are visible and distinct across samples."""
    calls: list[tuple[int, int]] = []

    async def _fake(*, http, prep_sample_idx, count, work_ticket_idx):
        calls.append((prep_sample_idx, count))
        base = 1000 * prep_sample_idx
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=base,
            sequence_idx_stop=base + count - 1,
        )

    monkeypatch.setattr(retry_module, "mint_sequence_range", _fake)
    return calls


def _write_fastq_gz(path, records: list[tuple[str, str]]) -> None:
    """Write a gzipped FASTQ with the given (read_id, sequence) records (constant
    quality). An empty list writes a valid-but-empty .gz (an empty well)."""
    body = "".join(f"@{rid}\n{seq}\n+\n{'I' * len(seq)}\n" for rid, seq in records)
    path.write_bytes(gzip.compress(body.encode()))


def _seed_convert_dir(tmp_path, samples: dict[str, list[tuple[str, str]]]):
    """Lay out a bcl-convert ConvertJob dir: one R1 .fastq.gz per pool_item_id
    nested under a Sample_Project subdir (mirrors --bcl-sampleproject-subdirectories)."""
    convert_dir = tmp_path / "ConvertJob"
    proj = convert_dir / "MyProject"
    proj.mkdir(parents=True)
    for item_id, records in samples.items():
        _write_fastq_gz(proj / f"{item_id}_S1_L001_R1_001.fastq.gz", records)
    return convert_dir


def _write_sample_map(path, roster: list[tuple[int, str]]) -> None:
    """Write the `(prep_sample_idx, pool_item_id)` roster Parquet the runner
    materializes for the step."""
    rows = ", ".join(f"({idx}, '{item}')" for idx, item in roster)
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "COPY (SELECT * FROM (VALUES " + rows + ") AS t(prep_sample_idx, pool_item_id)) "
            f"TO '{path}' (FORMAT parquet)"
        )


def _durable_rows(staging_root, prep_sample_idx) -> list[tuple]:
    path = compute_reads_staging_path(staging_root, prep_sample_idx)
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            "SELECT prep_sample_idx, sequence_idx, sequence1 "
            f"FROM read_parquet('{path}') ORDER BY sequence_idx"
        ).fetchall()


def _inputs(tmp_path, convert_dir, roster) -> Inputs:
    sample_map = tmp_path / "sample_map.parquet"
    _write_sample_map(sample_map, roster)
    return Inputs(
        convert_dir=convert_dir,
        sample_map=sample_map,
        reads_staging_root=tmp_path / "staging",
        sequenced_pool_idx=5,
        sequencing_run_idx=3,
        work_ticket_idx=1,
    )


def test_ingests_every_sample_once(fake_mint, tmp_path):
    """Two samples → two mints, durable copies under compute_reads_staging_path,
    and register parts hardlinked to them (same inode)."""
    convert_dir = _seed_convert_dir(
        tmp_path, {"10": [("a", "ACGT"), ("b", "TTTT")], "11": [("c", "GGGG")]}
    )
    inputs = _inputs(tmp_path, convert_dir, [(10, "10"), (11, "11")])

    outputs = _run(inputs, tmp_path / "ws")

    # One mint per sample, with the exact read count.
    assert sorted(fake_mint) == [(10, 2), (11, 1)]
    # Durable copies carry the scope column and the minted sequence_idx range.
    assert _durable_rows(inputs.reads_staging_root, 10) == [
        (10, 10000, "ACGT"),
        (10, 10001, "TTTT"),
    ]
    assert _durable_rows(inputs.reads_staging_root, 11) == [(11, 11000, "GGGG")]
    # register part hardlinked to the durable copy (same inode).
    register_dir = outputs["read_staging_dir"] / "read"
    for idx in (10, 11):
        part = register_dir / f"{idx}.parquet"
        durable = compute_reads_staging_path(inputs.reads_staging_root, idx)
        assert part.exists() and part.stat().st_ino == durable.stat().st_ino


def test_concurrent_pool_ingests_every_sample_above_cap(fake_mint, tmp_path):
    """A pool larger than the concurrency cap ingests every sample: the bounded
    asyncio.gather fan-out mints once per sample (with the exact count) and
    writes each durable copy + register part. n = _CONCURRENCY + 2 forces the
    semaphore to actually gate (more samples in flight than slots)."""
    n = ingest_module._CONCURRENCY + 2
    # sample i carries i reads (all non-empty), so its minted count is i.
    samples = {str(i): [(f"r{i}_{j}", "ACGT") for j in range(i)] for i in range(1, n + 1)}
    convert_dir = _seed_convert_dir(tmp_path, samples)
    inputs = _inputs(tmp_path, convert_dir, [(i, str(i)) for i in range(1, n + 1)])

    outputs = _run(inputs, tmp_path / "ws")

    # One mint per sample, each with that sample's exact read count.
    assert sorted(fake_mint) == [(i, i) for i in range(1, n + 1)]
    register_dir = outputs["read_staging_dir"] / "read"
    for i in range(1, n + 1):
        assert compute_reads_staging_path(inputs.reads_staging_root, i).exists()
        assert (register_dir / f"{i}.parquet").exists()


def test_empty_well_is_skipped(fake_mint, tmp_path):
    """A zero-record FASTQ is an empty well: no mint, no reads — but the pool
    still succeeds via its non-empty samples."""
    convert_dir = _seed_convert_dir(tmp_path, {"10": [("a", "ACGT")], "11": []})
    inputs = _inputs(tmp_path, convert_dir, [(10, "10"), (11, "11")])

    outputs = _run(inputs, tmp_path / "ws")

    assert fake_mint == [(10, 1)]  # only the non-empty well minted
    assert compute_reads_staging_path(inputs.reads_staging_root, 10).exists()
    assert not compute_reads_staging_path(inputs.reads_staging_root, 11).exists()
    assert not (outputs["read_staging_dir"] / "read" / "11.parquet").exists()


def test_missing_required_r1_fails_bad_input(fake_mint, tmp_path):
    """A roster sample with no R1 FASTQ on disk is a broken pool: BAD_INPUT,
    naming the offending sample."""
    convert_dir = _seed_convert_dir(tmp_path, {"10": [("a", "ACGT")]})
    inputs = _inputs(tmp_path, convert_dir, [(10, "10"), (99, "99")])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "99" in str(exc.value)


def test_rerun_skips_already_ingested(fake_mint, tmp_path):
    """Idempotent: a second run over a sample whose durable copy exists does NOT
    re-mint, but still re-creates its register part (the workspace is fresh)."""
    convert_dir = _seed_convert_dir(tmp_path, {"10": [("a", "ACGT")]})
    inputs = _inputs(tmp_path, convert_dir, [(10, "10")])

    _run(inputs, tmp_path / "ws1")
    assert fake_mint == [(10, 1)]

    outputs = _run(inputs, tmp_path / "ws2")
    # No second mint — the durable copy already exists.
    assert fake_mint == [(10, 1)]
    # The fresh workspace still gets the register part (re-linked from durable).
    assert (outputs["read_staging_dir"] / "read" / "10.parquet").exists()


def test_stale_partial_does_not_count_as_ingested(fake_mint, tmp_path):
    """A `.partial` left by a crashed prior attempt must NOT satisfy the
    idempotency skip — only the atomically-published durable read.parquet does.
    Otherwise a truncated write would be registered as the full read set."""
    convert_dir = _seed_convert_dir(tmp_path, {"10": [("a", "ACGT")]})
    inputs = _inputs(tmp_path, convert_dir, [(10, "10")])
    # Simulate a crash mid-COPY: a partial sentinel exists, the durable does not.
    durable = compute_reads_staging_path(inputs.reads_staging_root, 10)
    durable.parent.mkdir(parents=True)
    (durable.parent / f"{durable.name}.partial").write_text("truncated")

    _run(inputs, tmp_path / "ws")

    # The sample was (re-)ingested — the partial did not short-circuit it.
    assert fake_mint == [(10, 1)]
    assert durable.exists()
    assert not (durable.parent / f"{durable.name}.partial").exists()


def test_all_empty_pool_is_no_data(fake_mint, tmp_path):
    """Every well empty → StepNoData (no reads to register at all)."""
    convert_dir = _seed_convert_dir(tmp_path, {"10": [], "11": []})
    inputs = _inputs(tmp_path, convert_dir, [(10, "10"), (11, "11")])

    with pytest.raises(StepNoData):
        _run(inputs, tmp_path / "ws")
    assert fake_mint == []


# ---------------------------------------------------------------------------
# Range reuse — a prior attempt minted then crashed before the durable write
# ---------------------------------------------------------------------------


def _patch_conflicting_mint(monkeypatch):
    """Make mint_sequence_range always 409 (a range already exists)."""

    async def _conflict(*, http, prep_sample_idx, count, work_ticket_idx):
        raise SequenceRangeAlreadyExists(prep_sample_idx, count)

    monkeypatch.setattr(retry_module, "mint_sequence_range", _conflict)


def test_reuses_existing_range_on_mint_conflict(monkeypatch, tmp_path):
    """Durable absent + mint 409s ⇒ read the existing range back and reuse its
    start. The reads are written against the reused range (5000..), proving the
    step did NOT fail and did NOT mint a fresh range — the OOM-escalation retry
    completes transparently."""
    convert_dir = _seed_convert_dir(tmp_path, {"10": [("a", "ACGT"), ("b", "TTTT")]})
    inputs = _inputs(tmp_path, convert_dir, [(10, "10")])
    _patch_conflicting_mint(monkeypatch)

    async def _existing(*, http, prep_sample_idx):
        # The range the crashed attempt minted: starts at 5000, covers 2 reads.
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=5000,
            sequence_idx_stop=5001,
            minted_by_work_ticket_idx=_WORK_TICKET_IDX,
            minted_by_work_ticket_state="processing",  # still in flight
        )

    monkeypatch.setattr(retry_module, "get_sequence_range", _existing)

    _run(inputs, tmp_path / "ws")

    assert _durable_rows(inputs.reads_staging_root, 10) == [
        (10, 5000, "ACGT"),
        (10, 5001, "TTTT"),
    ]


def test_reuse_count_mismatch_fails_bad_input(monkeypatch, tmp_path):
    """An existing range whose span doesn't match the FASTQ's read count would
    write sequence_idx values that mismatch qiita.sequence_range at
    registration → BAD_INPUT, not a silent reuse."""
    convert_dir = _seed_convert_dir(tmp_path, {"10": [("a", "ACGT")]})  # 1 read
    inputs = _inputs(tmp_path, convert_dir, [(10, "10")])
    _patch_conflicting_mint(monkeypatch)

    async def _existing(*, http, prep_sample_idx):
        # Covers 5 indices, but the FASTQ has 1 read.
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=5000,
            sequence_idx_stop=5004,
            minted_by_work_ticket_idx=_WORK_TICKET_IDX,
            minted_by_work_ticket_state="processing",  # still in flight
        )

    monkeypatch.setattr(retry_module, "get_sequence_range", _existing)

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "must match the prior mint count" in str(exc.value)


def test_reuse_missing_range_fails_permanent(monkeypatch, tmp_path):
    """409 on mint but 404 on read-back ⇒ the range was deleted mid-retry
    (concurrent deletion): UNKNOWN_PERMANENT — a fresh resubmit re-mints."""
    convert_dir = _seed_convert_dir(tmp_path, {"10": [("a", "ACGT")]})
    inputs = _inputs(tmp_path, convert_dir, [(10, "10")])
    _patch_conflicting_mint(monkeypatch)

    async def _gone(*, http, prep_sample_idx):
        return None

    monkeypatch.setattr(retry_module, "get_sequence_range", _gone)

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.UNKNOWN_PERMANENT
    assert "concurrent deletion" in str(exc.value)


# ---------------------------------------------------------------------------
# Transient CP sequence-range callback errors: a 5xx / transport blip on one
# per-sample callback must self-heal via in-job retry, and an exhausted-retry
# transient error must classify retriable (CONTROL_PLANE_UNREACHABLE) — never
# permanently fail the whole pool over one dropped connection. 401/403 and other
# 4xx stay permanent (no retry).
# ---------------------------------------------------------------------------


def _status_error(status: int) -> httpx.HTTPStatusError:
    """An httpx.HTTPStatusError carrying `status`, shaped like the one
    `raise_for_status()` raises inside mint_sequence_range / get_sequence_range."""
    request = httpx.Request("POST", "http://cp/api/v1/sequence-range")
    response = httpx.Response(status, request=request)
    return httpx.HTTPStatusError(f"HTTP {status}", request=request, response=response)


@pytest.fixture
def no_backoff(monkeypatch):
    """Zero the retry backoff so the retry tests don't actually sleep."""
    monkeypatch.setattr(retry_module, "CP_RETRY_BACKOFF_BASE_S", 0)


def _flaky_mint(monkeypatch, errors):
    """mint raises each exc in `errors` on successive calls, then succeeds with a
    range starting at 7000. Returns the per-call list so the test can count
    attempts."""
    calls: list[int] = []

    async def _mint(*, http, prep_sample_idx, count, work_ticket_idx):
        calls.append(prep_sample_idx)
        if len(calls) <= len(errors):
            raise errors[len(calls) - 1]
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=7000,
            sequence_idx_stop=7000 + count - 1,
        )

    monkeypatch.setattr(retry_module, "mint_sequence_range", _mint)
    return calls


def _always_failing_mint(monkeypatch, exc):
    """mint always raises `exc`. Returns the per-call list."""
    calls: list[int] = []

    async def _mint(*, http, prep_sample_idx, count, work_ticket_idx):
        calls.append(prep_sample_idx)
        raise exc

    monkeypatch.setattr(retry_module, "mint_sequence_range", _mint)
    return calls


@pytest.mark.parametrize(
    "error",
    [_status_error(502), httpx.ConnectError("connection reset")],
    ids=["http_502", "transport_error"],
)
def test_transient_mint_error_self_heals(monkeypatch, no_backoff, tmp_path, error):
    """A transient 5xx / transport blip on the mint callback is retried in-job
    and the next attempt succeeds — the step completes and writes the reads
    against the eventually-minted range, never failing."""
    convert_dir = _seed_convert_dir(tmp_path, {"10": [("a", "ACGT"), ("b", "TTTT")]})
    inputs = _inputs(tmp_path, convert_dir, [(10, "10")])
    # Two transient failures, then success on the third attempt.
    calls = _flaky_mint(monkeypatch, [error, error])

    _run(inputs, tmp_path / "ws")

    assert len(calls) == 3  # two retries + the successful attempt
    assert _durable_rows(inputs.reads_staging_root, 10) == [
        (10, 7000, "ACGT"),
        (10, 7001, "TTTT"),
    ]


@pytest.mark.parametrize(
    "error,reason_substr",
    [
        (_status_error(502), "HTTP 502"),
        (_status_error(503), "HTTP 503"),
        (_status_error(408), "HTTP 408"),  # request timeout — transient
        (_status_error(429), "HTTP 429"),  # rate limit — transient
        (httpx.ConnectError("connection reset"), "transport error (ConnectError)"),
        (httpx.ReadTimeout("read timed out"), "transport error (ReadTimeout)"),
    ],
    ids=["http_502", "http_503", "http_408", "http_429", "connect_error", "read_timeout"],
)
def test_exhausted_transient_mint_error_is_retriable(
    monkeypatch, no_backoff, tmp_path, error, reason_substr
):
    """If every retry attempt hits the same transient error, the step raises a
    *retriable* CONTROL_PLANE_UNREACHABLE — the runner re-dispatches the
    idempotent step instead of discarding the whole pool's demux."""
    convert_dir = _seed_convert_dir(tmp_path, {"10": [("a", "ACGT")]})
    inputs = _inputs(tmp_path, convert_dir, [(10, "10")])
    calls = _always_failing_mint(monkeypatch, error)

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")

    assert len(calls) == retry_module.CP_RETRY_MAX_ATTEMPTS  # all attempts spent
    assert exc.value.kind == FailureKind.CONTROL_PLANE_UNREACHABLE
    assert exc.value.transient is True
    assert reason_substr in str(exc.value)
    assert "prep_sample 10" in str(exc.value)


@pytest.mark.parametrize("status", [401, 403], ids=["unauthorized", "forbidden"])
def test_auth_error_on_mint_is_permanent_no_retry(monkeypatch, no_backoff, tmp_path, status):
    """401/403 is a token/scope misconfig a retry can't fix: CONTRACT_VIOLATION
    (permanent) and *not* retried — the mint is called exactly once."""
    convert_dir = _seed_convert_dir(tmp_path, {"10": [("a", "ACGT")]})
    inputs = _inputs(tmp_path, convert_dir, [(10, "10")])
    calls = _always_failing_mint(monkeypatch, _status_error(status))

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")

    assert len(calls) == 1  # no retry on a permanent client error
    assert exc.value.kind == FailureKind.CONTRACT_VIOLATION
    assert exc.value.transient is False


def test_other_4xx_on_mint_is_permanent_no_retry(monkeypatch, no_backoff, tmp_path):
    """A non-auth 4xx (e.g. 400) is permanent (UNKNOWN_PERMANENT) and not
    retried."""
    convert_dir = _seed_convert_dir(tmp_path, {"10": [("a", "ACGT")]})
    inputs = _inputs(tmp_path, convert_dir, [(10, "10")])
    calls = _always_failing_mint(monkeypatch, _status_error(400))

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")

    assert len(calls) == 1
    assert exc.value.kind == FailureKind.UNKNOWN_PERMANENT
    assert exc.value.transient is False


def test_transient_error_on_reuse_readback_is_retriable(monkeypatch, no_backoff, tmp_path):
    """The reuse read-back (GET after a 409) is wrapped in the same retry; an
    exhausted transient 5xx there also classifies retriable."""
    convert_dir = _seed_convert_dir(tmp_path, {"10": [("a", "ACGT")]})
    inputs = _inputs(tmp_path, convert_dir, [(10, "10")])
    _patch_conflicting_mint(monkeypatch)  # mint always 409 → reuse path

    get_calls: list[int] = []

    async def _flaky_get(*, http, prep_sample_idx):
        get_calls.append(prep_sample_idx)
        raise _status_error(502)

    monkeypatch.setattr(retry_module, "get_sequence_range", _flaky_get)

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")

    assert len(get_calls) == retry_module.CP_RETRY_MAX_ATTEMPTS
    assert exc.value.kind == FailureKind.CONTROL_PLANE_UNREACHABLE
    assert exc.value.transient is True


def test_transient_error_on_reuse_readback_self_heals(monkeypatch, no_backoff, tmp_path):
    """A transient blip on the reuse read-back is retried and the next attempt
    returns the existing range — the reuse path completes (reads written against
    the recovered 5000.. range), proving the GET retry actually self-heals, not
    just that exhaustion classifies right."""
    convert_dir = _seed_convert_dir(tmp_path, {"10": [("a", "ACGT"), ("b", "TTTT")]})
    inputs = _inputs(tmp_path, convert_dir, [(10, "10")])
    _patch_conflicting_mint(monkeypatch)  # mint always 409 → reuse path

    get_calls: list[int] = []

    async def _flaky_get(*, http, prep_sample_idx):
        get_calls.append(prep_sample_idx)
        if len(get_calls) == 1:
            raise _status_error(503)  # one transient blip, then succeed
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=5000,
            sequence_idx_stop=5001,
            minted_by_work_ticket_idx=_WORK_TICKET_IDX,
            minted_by_work_ticket_state="processing",  # still in flight
        )

    monkeypatch.setattr(retry_module, "get_sequence_range", _flaky_get)

    _run(inputs, tmp_path / "ws")

    assert len(get_calls) == 2  # one retry + the successful read-back
    assert _durable_rows(inputs.reads_staging_root, 10) == [
        (10, 5000, "ACGT"),
        (10, 5001, "TTTT"),
    ]
