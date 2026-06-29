"""Bounded retry + failure classification for the CP sequence-range callback.

Both native jobs that mint a per-prep_sample sequence range — `ingest_reads`
(the bcl-convert read-storage step, which fans over every pool sample) and
`fastq_to_parquet` — call back to the control plane's `/sequence-range` routes
over HTTP. A *transient* blip on ONE of a pool's many per-sample callbacks (an
HTTP 5xx from a proxy / a CP restart, or a pure transport error like a
connection reset / read timeout) must not discard the whole multi-hour ingest:
the mint is idempotent on retry (a minted-but-unwritten range is read back and
reused), so re-driving the call is safe.

This module is the single home for that retry + classification, shared by both
jobs so they can't diverge:

  - `cp_call_with_retry` retries a transient call a few times in-job, so a blip
    self-heals without ever failing the step.
  - `cp_call_failure` maps an *exhausted* transient error to a RETRIABLE
    `CONTROL_PLANE_UNREACHABLE` BackendFailure (the runner re-dispatches the
    idempotent step) — the exact mirror of `ORCHESTRATOR_UNREACHABLE` for the
    CO→CP direction. 401/403 → CONTRACT_VIOLATION (a deploy misconfig a retry
    can't fix); any other 4xx → UNKNOWN_PERMANENT.

Kept separate from `sequence_range.py`, which is deliberately transport-agnostic
(it raises typed exceptions / returns None and never reaches for BackendFailure)
— the BackendFailure mapping is a job-level concern and lives here. The typed
409 / 404 mint exceptions (`SequenceRangeAlreadyExists`,
`PrepSampleNotEligibleForSequenceRange`) are NOT httpx errors, so they pass
straight through `cp_call_with_retry` for each job to handle its own way
(`ingest_reads` reuses the orphaned range; `fastq_to_parquet` fails with
operator-recovery instructions).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import httpx
from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models import WorkTicketFailureStage

# Attempts and exponential backoff for a transient CP sequence-range callback.
# 401/403 and any non-5xx 4xx are NOT retried (a token/scope misconfig or a
# missing prep_sample can't self-heal). Tests zero the backoff to avoid sleeps.
CP_RETRY_MAX_ATTEMPTS = 3
CP_RETRY_BACKOFF_BASE_S = 0.5


def _is_transient_status(status: int) -> bool:
    """True for an HTTP status from the CP callback that a retry can self-heal:
    any 5xx (proxy/connection blip, CP restart, or a CP statement-timeout
    surfacing as a 500), plus 408 (request timeout) and 429 (rate limit). 401/403
    and any other 4xx are permanent client-side conditions → False."""
    return status >= 500 or status in (408, 429)


def is_transient_cp_error(exc: httpx.HTTPStatusError | httpx.TransportError) -> bool:
    """True for a CP sequence-range error a retry can self-heal: a transient HTTP
    status (5xx / 408 / 429, per `_is_transient_status`) or a pure transport
    error (connect reset / read timeout — these raise before `raise_for_status`).
    A 401/403 or any other 4xx is a permanent client-side condition → False."""
    if isinstance(exc, httpx.HTTPStatusError):
        return _is_transient_status(exc.response.status_code)
    return isinstance(exc, httpx.TransportError)


async def cp_call_with_retry[T](call: Callable[[], Awaitable[T]]) -> T:
    """Await `call()` (a thunk returning a *fresh* coroutine — a coroutine can't
    be re-awaited), retrying a transient 5xx / transport error up to
    `CP_RETRY_MAX_ATTEMPTS` with exponential backoff. A non-transient httpx
    error (401/403, other 4xx) — and the typed 409/404 mint exceptions, which
    aren't httpx errors — propagate on the first raise. The final transient
    error propagates after the last attempt for the caller to classify via
    `cp_call_failure` (→ CONTROL_PLANE_UNREACHABLE, retriable)."""
    last_exc: httpx.HTTPStatusError | httpx.TransportError | None = None
    for attempt in range(1, CP_RETRY_MAX_ATTEMPTS + 1):
        try:
            return await call()
        except (httpx.HTTPStatusError, httpx.TransportError) as exc:
            if not is_transient_cp_error(exc):
                raise
            last_exc = exc
            if attempt < CP_RETRY_MAX_ATTEMPTS:
                await asyncio.sleep(CP_RETRY_BACKOFF_BASE_S * 2 ** (attempt - 1))
    assert last_exc is not None  # only reached after a recorded transient failure
    raise last_exc


def cp_call_failure(
    prep_sample_idx: int,
    exc: httpx.HTTPStatusError | httpx.TransportError,
    *,
    step_name: str,
) -> BackendFailure:
    """Map a CP sequence-range call (mint or read-back) that failed *after* the
    in-job retries to a BackendFailure carrying the caller's `step_name`:

      - transport error or a transient HTTP status (5xx / 408 / 429) →
        CONTROL_PLANE_UNREACHABLE (retriable): an infra-reachability blip on one
        per-sample callback, never a statement that the step's work is broken.
        The runner re-dispatches the whole (idempotent) step. Mirrors
        ORCHESTRATOR_UNREACHABLE for the reverse hop.
      - 401/403 → CONTRACT_VIOLATION (permanent): the compute SA PAT is missing
        / wrong or its scope ceiling was lowered — a deploy misconfig a retry
        can't fix.
      - any other 4xx → UNKNOWN_PERMANENT (permanent)."""
    if isinstance(exc, httpx.TransportError):
        kind = FailureKind.CONTROL_PLANE_UNREACHABLE
        detail = f"a transport error ({type(exc).__name__})"
    else:
        status = exc.response.status_code
        if status in (401, 403):
            kind = FailureKind.CONTRACT_VIOLATION
            detail = (
                f"HTTP {status} — compute SA PAT misconfigured "
                "(see docs/runbooks/compute-service-account-provisioning.md)"
            )
        elif _is_transient_status(status):
            kind = FailureKind.CONTROL_PLANE_UNREACHABLE
            detail = f"HTTP {status}"
        else:
            kind = FailureKind.UNKNOWN_PERMANENT
            detail = f"HTTP {status}"
    return BackendFailure(
        kind=kind,
        stage=WorkTicketFailureStage.STEP_RUN,
        step_name=step_name,
        reason=f"CP sequence-range call for prep_sample {prep_sample_idx} failed with {detail}",
    )
