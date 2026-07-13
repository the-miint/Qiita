"""Bounded retry + failure classification for the CP sequence-range callback.

Every native job that mints a per-prep_sample sequence range — `ingest_reads` (the
bcl-convert read-storage step, which fans over every pool sample), `fastq_to_parquet`
and `bam_to_parquet` — calls back to the control plane's `/sequence-range` routes
over HTTP. A *transient* blip on ONE of a pool's many per-sample callbacks (an
HTTP 5xx from a proxy / a CP restart, or a pure transport error like a
connection reset / read timeout) must not discard the whole multi-hour ingest:
the mint is idempotent on retry (a minted-but-unwritten range is read back and
reused), so re-driving the call is safe.

This module is the single home for that retry + classification, shared by all three
so they can't diverge:

  - `cp_call_with_retry` retries a transient call a few times in-job, so a blip
    self-heals without ever failing the step.
  - `cp_call_failure` maps an *exhausted* transient error to a RETRIABLE
    `CONTROL_PLANE_UNREACHABLE` BackendFailure (the runner re-dispatches the
    idempotent step) — the exact mirror of `ORCHESTRATOR_UNREACHABLE` for the
    CO→CP direction. 401/403 → CONTRACT_VIOLATION (a deploy misconfig a retry
    can't fix); any other 4xx → UNKNOWN_PERMANENT.

  - `mint_or_reuse_sequence_range` is the one mint entry point every reads job
    calls. It makes the mint idempotent across runner retries by reading an
    orphaned range back instead of dying on the one-shot mint contract.

Kept separate from `sequence_range.py`, which is deliberately transport-agnostic
(it raises typed exceptions / returns None and never reaches for BackendFailure)
— the BackendFailure mapping is a job-level concern and lives here. The typed
409 / 404 mint exceptions (`SequenceRangeAlreadyExists`,
`PrepSampleNotEligibleForSequenceRange`) are NOT httpx errors, so they pass
straight through `cp_call_with_retry` into `mint_or_reuse_sequence_range`, which
handles them uniformly for every job.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import httpx
from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models import (
    NON_TERMINAL_WORK_TICKET_STATES,
    WorkTicketFailureStage,
    WorkTicketState,
)

from .sequence_range import (
    PrepSampleNotEligibleForSequenceRange,
    SequenceRangeAlreadyExists,
    get_sequence_range,
    mint_sequence_range,
)

# Attempts and exponential backoff for a transient CP sequence-range callback.
# 401/403 and any non-5xx 4xx are NOT retried (a token/scope misconfig or a
# missing prep_sample can't self-heal). Tests zero the backoff to avoid sleeps.
CP_RETRY_MAX_ATTEMPTS = 3
CP_RETRY_BACKOFF_BASE_S = 0.5

# The states a minting ticket may be in for its range to be REUSABLE: an ALLOWLIST,
# derived from the canonical split so it cannot drift into a hand-maintained copy of
# it. Reuse is legitimate only while the minting ticket is still IN FLIGHT — a job
# reaching the mint under a ticket that has already terminated is a stale attempt, and
# reusing a range whose reads are registered duplicates them in DuckLake, which has no
# uniqueness and no way to notice afterwards. A denylist ("everything except
# completed") would let a work_ticket_state added later fall through to the permissive
# path by default; with a silent failure mode, the default must be refusal.
_REUSABLE_MINTER_STATES: frozenset[str] = frozenset(NON_TERMINAL_WORK_TICKET_STATES)


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


async def mint_or_reuse_sequence_range(
    http: httpx.AsyncClient,
    prep_sample_idx: int,
    count: int,
    *,
    work_ticket_idx: int,
    step_name: str,
) -> int:
    """Mint a sequence range for one sample, or reuse the one an earlier attempt left.

    Returns the inclusive range start.

    A reads job mints the range and THEN does its heavy durable write, so the
    window between the two is exactly where an OOM / walltime kill lands. The
    runner re-runs the whole step module on such a (transient) failure, which
    re-reaches this call — and the mint is one-shot, so a naive re-mint 409s and
    the retry dies. Worse, it dies *permanently*, masking the real failure (the
    OOM) behind a mint conflict, and it defeats the runner's OOM memory
    escalation: the escalated attempt can never get far enough to benefit.

    So a 409 is not automatically an error — but it is only SAFE to reuse the
    existing range when it belongs to a prior attempt of THIS work_ticket **and that
    ticket has not COMPLETED** (a completed ticket's reads are registered, so even its
    own stale attempt must not re-write the range). The cases the 409 conflates are:

      - a prior ATTEMPT of this ticket minted then crashed → the reads are NOT in
        the lake, the range is orphaned, reuse is correct and is what makes the
        step idempotent across runner retries;
      - a DIFFERENT ticket minted it → the sample's reads ARE already registered,
        and reusing the range would register them a second time. DuckLake has no
        uniqueness, so that duplication is silent and permanent.

    Nothing else in the system can separate them. The submit-time
    disallow-without-delete gate only blocks NON-terminal tickets, so a COMPLETED
    sample can be resubmitted; and the job's output lives in a per-ticket workspace
    it cannot see across tickets. So the range itself records its minting ticket
    (qiita.sequence_range.minted_by_work_ticket_idx) and we compare.

    A NULL `minted_by_work_ticket_idx` (a range the backfill could not attribute) is
    treated as NOT-mine: fail closed. That is the safe reading, and it is exactly
    disallow-without-delete.

    The ownership check does NOT subsume the caller's own precondition: WITHIN one
    ticket, ownership always matches, so a caller that can re-run after its durable
    output already landed must still check for it (`ingest_reads` guards on the
    durable read.parquet). Ownership separates tickets; it cannot separate a
    completed attempt from a crashed one inside the same ticket.

    Maps the typed mint exceptions to BackendFailures (the framework dispatcher
    only wraps bare NotImplementedError / FileNotFoundError / ValueError). Both
    CP calls go through `cp_call_with_retry`, so a transient blip on one of a
    pool's many per-sample callbacks self-heals instead of failing the step.
    """
    try:
        rng = await cp_call_with_retry(
            lambda: mint_sequence_range(
                http=http,
                prep_sample_idx=prep_sample_idx,
                count=count,
                work_ticket_idx=work_ticket_idx,
            )
        )
        return rng.sequence_idx_start
    except SequenceRangeAlreadyExists as exc:
        # Reuse the range a prior crashed attempt left. The GET is gated on the
        # same `sequence_range:mint` scope the SA already holds.
        try:
            existing = await cp_call_with_retry(
                lambda: get_sequence_range(http=http, prep_sample_idx=prep_sample_idx)
            )
        except (httpx.HTTPStatusError, httpx.TransportError) as get_exc:
            raise cp_call_failure(prep_sample_idx, get_exc, step_name=step_name) from get_exc
        if existing is None:
            # 409 on mint but 404 on read-back: the range vanished between the two
            # calls (an operator deleted the prep_sample / range mid-retry). A fresh
            # resubmit will re-mint cleanly, but THIS attempt can't run against a
            # moving target.
            raise BackendFailure(
                kind=FailureKind.UNKNOWN_PERMANENT,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=step_name,
                reason=(
                    f"prep_sample {prep_sample_idx} sequence_range 409'd on mint but "
                    "404'd on read-back — concurrent deletion during retry; resubmit"
                ),
            ) from exc
        if existing.minted_by_work_ticket_idx != work_ticket_idx:
            # A DIFFERENT ticket minted this range (or its provenance is unknown —
            # NULL, which we read as not-mine). Either way the sample's reads are
            # already registered in the lake, so reusing the range would register
            # them a second time. DuckLake has no uniqueness: the duplication would
            # be silent. Refuse, and tell the operator the one thing that fixes it.
            owner = existing.minted_by_work_ticket_idx
            owner_detail = f"work_ticket {owner}" if owner is not None else "an unknown work_ticket"
            raise BackendFailure(
                kind=FailureKind.UNKNOWN_PERMANENT,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=step_name,
                reason=(
                    f"prep_sample {prep_sample_idx} already has a sequence_range minted "
                    f"by {owner_detail}, not by this one (work_ticket {work_ticket_idx}) — "
                    "its reads are already loaded, and re-ingesting would duplicate them "
                    "(DuckLake has no uniqueness). To re-ingest deliberately, DELETE the "
                    "prep_sample (its sequence_range goes with it via ON DELETE CASCADE; "
                    "for a whole pool, `qiita delete-sequenced-pool`) and resubmit"
                ),
            ) from exc
        if existing.minted_by_work_ticket_state not in _REUSABLE_MINTER_STATES:
            # Ownership is necessary but NOT sufficient — the minting ticket must also
            # still be IN FLIGHT. A terminal minter means this attempt is stale: the
            # ticket already finished (an orphaned SLURM job outliving the attempt that
            # completed it), and if it COMPLETED then its reads are registered, so even
            # its own attempt must not re-write the range.
            state = existing.minted_by_work_ticket_state
            # The recovery differs by state, so name it rather than just refusing —
            # and only offer a redrive where the CP will actually accept one. `/run`
            # takes a ticket in PENDING or FAILED; it 409s on `no_data` and 404s on a
            # ticket row that is gone (state=None). So the three-way is not cosmetic:
            # the fall-through arm exists because a fail-closed allowlist must land an
            # UNANTICIPATED state on advice that works, not on advice that bounces.
            if state == WorkTicketState.FAILED.value:
                recovery = (
                    f"re-drive this ticket with `qiita ticket run {work_ticket_idx}`, "
                    "which returns it to flight and makes its own range reusable"
                )
            else:
                # COMPLETED (reads registered), or a state with no in-place redrive.
                recovery = (
                    "there is no in-place recovery from this state — to re-ingest, "
                    "DELETE the prep_sample (its sequence_range goes with it) and "
                    "resubmit"
                )
            raise BackendFailure(
                kind=FailureKind.UNKNOWN_PERMANENT,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=step_name,
                reason=(
                    f"prep_sample {prep_sample_idx}'s sequence_range was minted by "
                    f"work_ticket {work_ticket_idx}, which is no longer in flight "
                    f"(state={state!r}) — this attempt is stale. Refusing to re-write "
                    f"the range, which could duplicate the sample's reads. {recovery}"
                ),
            ) from exc
        recovered_count = existing.sequence_idx_stop - existing.sequence_idx_start + 1
        if recovered_count != count:
            # The existing range was minted against a different read count than this
            # attempt's input — reusing it would write sequence_idx values that
            # mismatch qiita.sequence_range at registration. The input is immutable
            # between submit and execution, so this is unreachable in practice; fail
            # loudly if it isn't.
            raise BackendFailure(
                kind=FailureKind.BAD_INPUT,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=step_name,
                reason=(
                    f"prep_sample {prep_sample_idx} has an existing sequence_range covering "
                    f"{recovered_count} indices "
                    f"({existing.sequence_idx_start}..{existing.sequence_idx_stop}) but its "
                    f"input now has {count} reads — the range must match the prior mint "
                    "count exactly; delete the prep_sample to re-mint"
                ),
            ) from exc
        return existing.sequence_idx_start
    except PrepSampleNotEligibleForSequenceRange as exc:
        raise BackendFailure(
            kind=FailureKind.BAD_INPUT,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=step_name,
            reason=str(exc),
        ) from exc
    except (httpx.HTTPStatusError, httpx.TransportError) as exc:
        raise cp_call_failure(prep_sample_idx, exc, step_name=step_name) from exc
