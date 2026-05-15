"""Client for the control plane's sequence-range allocator.

The orchestrator-side caller for `POST /api/v1/sequence-range`
(added by PR #36 to the CP). Native jobs that need a globally-unique
bigint range per prep_sample (today: fastq_to_parquet) go through
`mint_sequence_range` here.

**Recovery model.** The CP's mint is idempotent only in that a second
mint for the same prep_sample 409s — there is no GET-or-mint shape on
the wire today. The GET endpoint exists but requires
`prep_sample:read`, which the compute service-account deliberately
does NOT hold (per docs/runbooks/compute-service-account-provisioning.md
the SA is scope-minimal at `sequence_range:mint` only). So:

- First mint for a prep_sample: 201 + the range. Normal.
- Mid-step failure after mint succeeded: the next attempt's mint
  call 409s. The orchestrator surfaces this as a typed
  `SequenceRangeAlreadyExists` exception, which the caller maps to a
  `BackendFailure(UNKNOWN_PERMANENT)`. Recovery requires operator
  intervention (DELETE the prep_sample, CASCADE removes the range,
  resubmit the work_ticket).

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
execution.
"""

from __future__ import annotations

from dataclasses import dataclass

import httpx
from qiita_common.api_paths import URL_SEQUENCE_RANGE_PREFIX


@dataclass(frozen=True, slots=True)
class SequenceRange:
    """The orchestrator's view of a minted sequence-range row. The
    fields mirror qiita_common.models.SequenceRange but without the
    full Pydantic surface — this is a value type for one helper.
    Inclusive on both ends: `count = stop - start + 1`."""

    prep_sample_idx: int
    sequence_idx_start: int
    sequence_idx_stop: int


class SequenceRangeAlreadyExists(Exception):
    """Raised when the CP returns 409 from POST /sequence-range — the
    prep_sample already has a sequence_range from a prior (likely
    failed) attempt. The orchestrator can't recover today without the
    self-read endpoint discussed in the module docstring; the caller
    should surface this as a permanent failure with operator
    instructions."""

    def __init__(self, prep_sample_idx: int):
        super().__init__(
            f"prep_sample {prep_sample_idx} already has a sequence_range "
            "(typically from a previous failed attempt). Recovery requires "
            "deleting the prep_sample (CASCADE removes the range) and "
            "resubmitting the work_ticket. A future CP-side endpoint will "
            "let the minter re-read its own range; track as follow-up."
        )
        self.prep_sample_idx = prep_sample_idx


class PrepSampleNotEligibleForSequenceRange(Exception):
    """Raised when the CP returns 404 — the prep_sample doesn't exist
    or its processing_kind is not 'sequenced'. The submit route should
    have caught the latter case already; this surfaces only if the
    prep_sample was deleted between work_ticket submission and step
    execution."""

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
) -> SequenceRange:
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
        raise SequenceRangeAlreadyExists(prep_sample_idx)
    if resp.status_code == 404:
        raise PrepSampleNotEligibleForSequenceRange(prep_sample_idx)
    resp.raise_for_status()
    body = resp.json()
    return SequenceRange(
        prep_sample_idx=body["prep_sample_idx"],
        sequence_idx_start=body["sequence_idx_start"],
        sequence_idx_stop=body["sequence_idx_stop"],
    )
