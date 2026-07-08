"""Client for the control plane's sequence-range allocator.

The orchestrator-side caller for the CP's `/api/v1/sequence-range`
routes (qiita-control-plane/src/qiita_control_plane/routes/sequence_range.py).
A native job mints a globally-unique bigint range per prep_sample via
`mint_sequence_range`, and reads an existing one back via
`get_sequence_range`.

**Recovery model.** The CP's mint 409s on a second mint for the same
prep_sample. Two helpers cover the two recovery shapes:

- `mint_sequence_range` — POST; raises `SequenceRangeAlreadyExists` on
  409. The `fastq_to_parquet` job maps that to a permanent
  `BackendFailure` (its recovery is the operator-supplied
  `pre_minted_range`; see docs/runbooks/fastq-to-parquet-retry-recovery.md).
- `get_sequence_range` — GET; reads an existing range back, or None on
  404. The `ingest_reads` job uses it for *transparent* retry: a pool
  step that minted a sample's range then crashed before the durable
  write reuses the existing range on the next attempt instead of
  failing. This is what lets the runner's OOM memory-escalation pay off
  on an oversized sample — the escalated retry reuses the range rather
  than dying on the one-shot mint contract.

The GET endpoint accepts `sequence_range:mint` (as well as
`prep_sample:read`), so the compute service-account — scope-minimal at
`sequence_range:mint` per
docs/runbooks/compute-service-account-provisioning.md — can read back
its own range without holding `prep_sample:read`.

Both helpers stay transport-agnostic: they raise typed exceptions /
return None and never reach for `BackendFailure` or runner-level
failure classification — the calling job owns that mapping.

The 404 path (prep_sample doesn't exist or isn't eligible) is also
typed: `PrepSampleNotEligibleForSequenceRange`. The submit-route
on the CP already 404s when the work_ticket itself points at a
non-existent prep_sample, so this exception fires only if the
prep_sample was deleted between work_ticket submission and step
execution. Same caller-side mapping pattern as the 409 case.
"""

from __future__ import annotations

from dataclasses import dataclass

import httpx
from pydantic import BaseModel, Field
from qiita_common.api_paths import (
    URL_SEQUENCE_RANGE_BY_PREP_SAMPLE,
    URL_SEQUENCE_RANGE_PREFIX,
)


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


class PreMintedRange(BaseModel):
    """Operator-supplied recovery range for a retried read-ingest work_ticket
    (the `fastq_to_parquet` and `bam_to_parquet` native jobs both accept it).

    Set only when a prior attempt failed transiently AFTER it had already minted
    a sequence-range — the prep_sample's `qiita.sequence_range` row exists and a
    fresh mint would 409. The operator (or runner-side automation) reads the
    existing range, resubmits the work_ticket with this field populated, and the
    job skips the mint call.

    The two indices are inclusive on both ends and must match the input's read
    count exactly: `sequence_idx_stop - sequence_idx_start + 1 == count_of_reads`.
    Mismatch → BAD_INPUT.

    Lives here (not in a job module) because it is the recovery twin of
    `MintedSequenceRange` and both read-ingest jobs consume it — neither should
    import it out of the other. See
    docs/runbooks/fastq-to-parquet-retry-recovery.md for the full operator flow.
    """

    sequence_idx_start: int = Field(gt=0)
    sequence_idx_stop: int = Field(gt=0)


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


async def get_sequence_range(
    *,
    http: httpx.AsyncClient,
    prep_sample_idx: int,
) -> MintedSequenceRange | None:
    """GET /api/v1/sequence-range/{prep_sample_idx}; return the existing
    range or None if the prep_sample has none yet (404).

    Used by `ingest_reads` to read back a range a prior, crashed attempt
    already minted, so the retry reuses it instead of re-minting (which
    would 409). The CP route accepts the `sequence_range:mint` scope the
    compute SA holds, so no `prep_sample:read` grant is needed.

    `http` is the authed httpx client (Bearer with the compute SA PAT,
    base_url = the CP), same as `mint_sequence_range`.

    Raises:
      httpx.HTTPStatusError: any non-200, non-404 (5xx, auth 401/403,
        etc.). The caller maps these to BackendFailure based on status.
    """
    resp = await http.get(URL_SEQUENCE_RANGE_BY_PREP_SAMPLE.format(prep_sample_idx=prep_sample_idx))
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    body = resp.json()
    return MintedSequenceRange(
        prep_sample_idx=body["prep_sample_idx"],
        sequence_idx_start=body["sequence_idx_start"],
        sequence_idx_stop=body["sequence_idx_stop"],
    )
