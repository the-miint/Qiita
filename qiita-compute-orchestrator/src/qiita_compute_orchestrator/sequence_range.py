"""Client for the control plane's sequence-range allocator.

The orchestrator-side caller for `POST /api/v1/sequence-range` (CP
route: qiita-control-plane/src/qiita_control_plane/routes/sequence_range.py).
Native jobs that need a globally-unique bigint range per prep_sample
(today: fastq_to_parquet) go through `mint_sequence_range` here.

**Recovery model.** The CP's mint is idempotent only in that a second
mint for the same prep_sample 409s — there is no GET-or-mint shape on
the wire today. The GET endpoint exists but requires
`prep_sample:read`, which the compute service-account deliberately
does NOT hold (per docs/runbooks/compute-service-account-provisioning.md
the SA is scope-minimal at `sequence_range:mint` only). So:

- First mint for a prep_sample: 201 + the range. Normal.
- Mid-step failure after mint succeeded: the next attempt's mint
  call 409s. This helper raises `SequenceRangeAlreadyExists`; the
  caller (`fastq_to_parquet.execute`) is responsible for mapping it
  to a `BackendFailure` with the right `FailureKind`. The helper
  itself stays transport-agnostic — it doesn't reach for
  BackendFailure or know about runner-level failure classification.

A future improvement (out of scope for this PR) would add a
`GET /sequence-range/{prep_sample_idx}` variant gated on
`sequence_range:mint` so the minter can read back its own range. With
that endpoint, retries become transparent. Track that as a CP-side
follow-up.

The 404 path (prep_sample doesn't exist or isn't eligible) is also
typed: `PrepSampleNotEligibleForSequenceRange`. The submit-route
on the CP already 404s when the work_ticket itself points at a
non-existent prep_sample, so this exception fires only if the
prep_sample was deleted between work_ticket submission and step
execution. Same caller-side mapping pattern as the 409 case.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx
from qiita_common.api_paths import URL_SEQUENCE_RANGE_PREFIX, URL_SEQUENCED_POOL_PREFLIGHT
from qiita_common.models import SequencedPoolPreflightResponse

from .config import Settings, get_settings


def make_cp_client(
    settings: Settings | None = None,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> httpx.AsyncClient:
    """Build an authed AsyncClient pointed at the control plane.

    The compute service-account PAT goes into the Authorization header
    on every request. Caller is responsible for `async with`-style
    lifetime management; one client per execute() invocation is the
    expected pattern (cheap to construct, no connection pooling
    benefit at our call rate).

    `settings` is injectable for tests; production code passes nothing
    and get_settings() (config.py) resolves either the lifespan-installed
    cached value (orchestrator service) or a fresh Settings.from_env()
    (SLURM launcher / CLI). See config.py module header for the
    asymmetric resolution rationale.

    `transport` is injectable for tests so an integration suite can
    swap in an `httpx.ASGITransport(app=cp_app)` and exercise the full
    Settings → headers → httpx → CP route → DB path in-process without
    a uvicorn subprocess. Production code passes nothing; httpx uses
    its default network transport against `settings.cp_url`.
    """
    if settings is None:
        settings = get_settings()
    kwargs: dict[str, Any] = {
        "base_url": settings.cp_url,
        "headers": {"Authorization": f"Bearer {settings.co_to_cp_token}"},
        # 30s caters to a slow nextval/setval/INSERT under contention;
        # the CP route is bounded by the advisory lock + a few ms of
        # plpgsql, so a longer timeout would only mask infra issues.
        "timeout": httpx.Timeout(30.0),
    }
    if transport is not None:
        kwargs["transport"] = transport
    return httpx.AsyncClient(**kwargs)


@dataclass(frozen=True, slots=True)
class MintedSequenceRange:
    """The mint operation's domain-level return value — distinct from
    the wire shape `qiita_common.models.SequenceRange` by name so a
    reader can't conflate them. Carries only the fields downstream
    native jobs actually consume (no `created_at`); if a job ever
    needs the wire's audit fields, parse the response into
    `qiita_common.models.SequenceRange` directly. Inclusive on both
    ends: `count = stop - start + 1`."""

    prep_sample_idx: int
    sequence_idx_start: int
    sequence_idx_stop: int


class SequenceRangeAlreadyExists(Exception):
    """Raised when the CP returns 409 from POST /sequence-range — the
    prep_sample already has a sequence_range from a prior (likely
    failed) attempt. Callers map this to a permanent BackendFailure
    with operator-recovery instructions; the helper itself does not."""

    def __init__(self, prep_sample_idx: int, count: int):
        super().__init__(
            f"prep_sample {prep_sample_idx} already has a sequence_range "
            f"(attempted mint with count={count}, typically from a "
            "previous failed attempt). Recover by resubmitting the "
            "work_ticket with `pre_minted_range` populated from the "
            "existing qiita.sequence_range row — see "
            "docs/runbooks/fastq-to-parquet-retry-recovery.md. "
            "Deleting the prep_sample also works (CASCADE removes the "
            "range) but is destructive; the pre_minted_range path is "
            "preferred."
        )
        self.prep_sample_idx = prep_sample_idx
        self.count = count


class PrepSampleNotEligibleForSequenceRange(Exception):
    """Raised when the CP returns 404 — the prep_sample doesn't exist
    or its processing_kind is not 'sequenced'. The submit route should
    have caught the latter case already; this surfaces only if the
    prep_sample was deleted between work_ticket submission and step
    execution. Callers map this to a permanent BackendFailure
    (typically BAD_INPUT)."""

    def __init__(self, prep_sample_idx: int):
        super().__init__(
            f"prep_sample {prep_sample_idx} not found or not eligible for "
            "sequence-range allocation (deleted between submission and "
            "step execution, or non-sequenced processing_kind)."
        )
        self.prep_sample_idx = prep_sample_idx


async def mint_sequence_range(
    *,
    http: httpx.AsyncClient,
    prep_sample_idx: int,
    count: int,
) -> MintedSequenceRange:
    """POST /api/v1/sequence-range and return the minted range.

    `http` is the authed httpx client (Bearer with the compute SA
    PAT, base_url = the CP). The caller constructs and re-uses one
    per execute() invocation.

    Raises:
      SequenceRangeAlreadyExists: 409 — prep_sample already minted.
      PrepSampleNotEligibleForSequenceRange: 404 — prep_sample missing
        or non-sequenced.
      httpx.HTTPStatusError: anything else (5xx, auth 401/403, etc.).
        The caller maps these to BackendFailure based on the status.
    """
    resp = await http.post(
        URL_SEQUENCE_RANGE_PREFIX,
        json={"prep_sample_idx": prep_sample_idx, "count": count},
    )
    if resp.status_code == 409:
        raise SequenceRangeAlreadyExists(prep_sample_idx, count)
    if resp.status_code == 404:
        raise PrepSampleNotEligibleForSequenceRange(prep_sample_idx)
    resp.raise_for_status()
    body = resp.json()
    return MintedSequenceRange(
        prep_sample_idx=body["prep_sample_idx"],
        sequence_idx_start=body["sequence_idx_start"],
        sequence_idx_stop=body["sequence_idx_stop"],
    )


class SequencedPoolPreflightNotFound(Exception):
    """Raised when GET /sequencing-run/{R}/sequenced-pool/{P}/preflight
    returns 404 — either the pool doesn't exist under the named run, or
    the pool exists but its preflight blob isn't populated.

    The route distinguishes the two cases in the response body's
    ``detail`` field; callers that need to discriminate can read
    ``self.detail``. The bcl_convert_prep step treats both as
    BackendFailure(BAD_INPUT) — neither is a transient infra issue —
    so the runner side maps it to a PERMANENT work_ticket failure.
    """

    def __init__(
        self,
        *,
        sequencing_run_idx: int,
        sequenced_pool_idx: int,
        detail: str | None = None,
    ) -> None:
        msg = (
            f"sequenced_pool {sequenced_pool_idx} preflight not available"
            f" under sequencing_run {sequencing_run_idx}"
        )
        if detail:
            msg = f"{msg}: {detail}"
        super().__init__(msg)
        self.sequencing_run_idx = sequencing_run_idx
        self.sequenced_pool_idx = sequenced_pool_idx
        self.detail = detail


async def fetch_sequenced_pool_preflight(
    *,
    http: httpx.AsyncClient,
    sequencing_run_idx: int,
    sequenced_pool_idx: int,
) -> SequencedPoolPreflightResponse:
    """GET /sequencing-run/{R}/sequenced-pool/{P}/preflight.

    ``http`` is the authed httpx client (Bearer with the compute SA PAT,
    base_url=the CP) returned by ``make_cp_client``. The caller manages
    its lifetime — one client per ``execute()`` invocation matches the
    pattern ``mint_sequence_range`` uses.

    Returns the populated SequencedPoolPreflightResponse (raw bytes after
    Pydantic's Base64Bytes decoding).

    Raises:
      SequencedPoolPreflightNotFound: 404 (pool not in run / no preflight).
      httpx.HTTPStatusError: anything else (401/403, 5xx). The caller
        maps to BackendFailure based on the status.
    """
    url = URL_SEQUENCED_POOL_PREFLIGHT.format(
        sequencing_run_idx=sequencing_run_idx,
        sequenced_pool_idx=sequenced_pool_idx,
    )
    resp = await http.get(url)
    if resp.status_code == 404:
        detail: str | None = None
        try:
            detail = resp.json().get("detail")
        except ValueError, AttributeError:
            pass
        raise SequencedPoolPreflightNotFound(
            sequencing_run_idx=sequencing_run_idx,
            sequenced_pool_idx=sequenced_pool_idx,
            detail=detail if isinstance(detail, str) else None,
        )
    resp.raise_for_status()
    return SequencedPoolPreflightResponse.model_validate_json(resp.content)
