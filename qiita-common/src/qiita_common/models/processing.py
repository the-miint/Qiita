"""Processing-run identity and its lifecycle.

A `processing_idx` is the identity of one processing RUN — the canonical-params
hash over the workflow, its version, and the result-affecting inputs. Today the
only workflow that mints one is `long-read-assembly`, whose contigs
(`qiita.assembly_membership` and the DuckLake assembly tables) are stamped with
it, so it is what a published contig set is attributable to.

The lifecycle models here are the twin of the mask ones in
`qiita_common.models.sequencing`, at the same two granularities.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from qiita_common.models._base import check_withdrawal_reason

AssemblySampleState = Literal["pending", "completed", "no_data", "invalidated"]
"""A sample's per-`(processing_idx, prep_sample)` assembly state.

Mirrors the `qiita.assembly_sample.state` TEXT/CHECK column (NOT a Postgres
ENUM). `'completed'` means the run assembled contigs and registered them;
`'no_data'` means it finished having assembled no contig of any kind;
`'pending'` means it has not finished; `'invalidated'` means it completed and
has since been withdrawn. The canonical statement of the gate contract lives on
`qiita_control_plane.repositories.assembly.fetch_assembly_sample_state`.

Consumers that need contigs act only on `'completed'`. Absence of a state is
never `'assembled'`.

`'invalidated'` is per-RUN and `ProcessingStatus.DEPRECATED` is per-CONFIG; they
are deliberately not the same judgement (the `assembly_lifecycle` migration
header carries why).
"""


class ProcessingStatus(StrEnum):
    """Lifecycle of an assembly run CONFIG (`qiita.processing.status`).

    Mirrors a TEXT + CHECK column, NOT a Postgres `CREATE TYPE ... AS ENUM` —
    per the enum-parity carve-out in CLAUDE.md it therefore has no `ENUM_PAIRS`
    entry and is out of scope for the parity test. Nothing derives this set from
    the CHECK constraint that also declares it, so the two move together by hand.

    `DEPRECATED` is enforced at `qiita.mint_processing`, which refuses to return
    the row; it says nothing about any individual run, which is
    `AssemblySampleState` `'invalidated'` instead.
    """

    ACTIVE = "active"
    DEPRECATED = "deprecated"


class Processing(BaseModel):
    """Returned by GET /api/v1/processing/{processing_idx} and by the two PATCH
    routes.

    `params` carries the canonical params the identity was hashed from — the
    workflow, its version, the `mask_idx` whose pass-set was assembled, and the
    assembler — so a `processing_idx` is self-describing without recomputing the
    hash.

    Scope: `params` covers the result-affecting inputs at the time the run was
    minted. It does not cover the code that consumed them, so two runs with
    identical `params` can have assembled differently if the workflow's
    containers changed between them.
    """

    processing_idx: Annotated[int, Field(gt=0)]
    workflow: str
    version: str
    params: dict[str, Any]
    created_at: AwareDatetime
    # Lifecycle. A deprecated run is still returned by every read route — the
    # record of what assembled published contigs outlives the judgement that the
    # run was wrong. The three provenance fields are set exactly when `status` is
    # DEPRECATED (a CHECK enforces the biconditional).
    status: ProcessingStatus
    deprecated_at: AwareDatetime | None = None
    deprecated_by_idx: Annotated[int | None, Field(default=None, gt=0)] = None
    deprecation_reason: str | None = None
    superseded_by: Annotated[int | None, Field(default=None, gt=0)] = None


class ProcessingSummary(Processing):
    """One row of GET /api/v1/processing (the list view).

    A Processing plus the per-run sample tally, so a pool assembled under several
    identities is separable in one round trip instead of a GET per run. The tally
    is over the same rows the roster read returns — scoped to the filters the
    list was called with, narrowed to the samples the caller may see, and
    excluding entity-retired prep_samples.

    `samples_completed` are assembled and registered — the set a de novo
    alignment can be submitted against. `samples_pending` have not finished.
    `samples_no_data` finished having assembled no contig. `samples_invalidated`
    completed and were withdrawn.
    """

    samples_completed: Annotated[int, Field(ge=0)]
    samples_pending: Annotated[int, Field(ge=0)]
    # Counted separately rather than folded into either bucket above: a run that
    # assembled nothing is neither usable nor still coming.
    samples_no_data: Annotated[int, Field(ge=0)]
    samples_invalidated: Annotated[int, Field(ge=0)]


class ProcessingListResponse(BaseModel):
    """Returned by GET /api/v1/processing.

    `processing` is ordered by descending `processing_idx` (newest first), capped
    at the route's hard limit; `truncated` is True when the underlying set
    exceeded it. `sequenced_pool_idx` and `prep_sample_idx` are echoed back so a
    stored response is self-describing; `status` is not, being a filter over the
    rows rather than over which samples they tally.
    """

    processing: list[ProcessingSummary]
    count: Annotated[int, Field(ge=0)]
    truncated: bool = False
    sequenced_pool_idx: Annotated[int | None, Field(default=None, gt=0)] = None
    prep_sample_idx: Annotated[int | None, Field(default=None, gt=0)] = None


class ProcessingPrepSample(BaseModel):
    """One sample in an assembly run's per-sample roster.

    `assembly_state` answers "may this sample's contigs be read?" — `'completed'`
    if so. Unlike the mask roster there is no second source to reconcile: the
    `assembly_sample` gate row is the only record of a (run, sample), because no
    `work_ticket` column carries a `processing_idx` to attribute a ticket by. So
    a sample with no gate row does not appear here at all.
    """

    prep_sample_idx: Annotated[int, Field(gt=0)]
    biosample_accession: str | None = None
    assembly_state: AssemblySampleState


class ProcessingPrepSampleListResponse(BaseModel):
    """Returned by GET /api/v1/processing/{processing_idx}/prep-sample.

    The roster of samples assembled under one run, ascending by
    `prep_sample_idx`, optionally narrowed to one `sequenced_pool_idx` and always
    narrowed to the samples the caller may see. `truncated` is True when the
    underlying set exceeded the route's hard cap.
    """

    processing_idx: Annotated[int, Field(gt=0)]
    samples: list[ProcessingPrepSample]
    count: Annotated[int, Field(ge=0)]
    truncated: bool = False
    sequenced_pool_idx: Annotated[int | None, Field(default=None, gt=0)] = None


class ProcessingStatusUpdate(BaseModel):
    """Body for PATCH /api/v1/processing/{processing_idx}/status.

    Deprecating requires a `reason` (`check_withdrawal_reason` carries why).
    `superseded_by` names the replacement when a re-mint under a corrected mask or
    assembler produced one; it is accepted only alongside DEPRECATED, matching the
    CHECK on the column.

    The route applies this body as a whole-block replace, so a second PATCH that
    omits `superseded_by` clears a previously-recorded replacement — re-supply it
    when correcting a reason.
    """

    model_config = ConfigDict(extra="forbid")

    status: ProcessingStatus
    reason: Annotated[str | None, Field(default=None, min_length=1, max_length=2000)] = None
    superseded_by: Annotated[int | None, Field(default=None, gt=0)] = None

    @model_validator(mode="after")
    def _reason_required_to_deprecate(self) -> ProcessingStatusUpdate:
        deprecating = self.status is ProcessingStatus.DEPRECATED
        check_withdrawal_reason(
            withdrawing=deprecating,
            reason=self.reason,
            field="status",
            value=ProcessingStatus.DEPRECATED.value,
        )
        if not deprecating and self.superseded_by is not None:
            raise ValueError("superseded_by is only accepted when status is 'deprecated'")
        return self


class AssemblySampleStatusUpdate(BaseModel):
    """Body for PATCH /api/v1/processing/{processing_idx}/sample-status.

    Withdraws (or restores) specific runs of one processing identity. Bulk because
    that is the unit the judgement is made in, and applying it one call per sample
    leaves a partially-withdrawn run for someone to reason about.

    `state` accepts only the two settable values; what happens to a row holding
    either of the other two, and why restoring to `'completed'` is exact rather
    than a guess, is on
    `qiita_control_plane.repositories.processing.set_assembly_sample_states`.
    """

    model_config = ConfigDict(extra="forbid")

    prep_sample_idx: Annotated[list[Annotated[int, Field(gt=0)]], Field(min_length=1)]
    state: Literal["completed", "invalidated"]
    reason: Annotated[str | None, Field(default=None, min_length=1, max_length=2000)] = None

    @model_validator(mode="after")
    def _reason_required_to_invalidate(self) -> AssemblySampleStatusUpdate:
        check_withdrawal_reason(
            withdrawing=self.state == "invalidated",
            reason=self.reason,
            field="state",
            value="invalidated",
        )
        if len(set(self.prep_sample_idx)) != len(self.prep_sample_idx):
            raise ValueError("prep_sample_idx must not repeat")
        return self


class AssemblySampleStatusUpdateResponse(BaseModel):
    """Returned by PATCH /api/v1/processing/{processing_idx}/sample-status.

    `updated` are the pairs whose state changed. `unchanged` already held the
    requested state (the call is idempotent, so re-running a withdrawal is not an
    error). `not_found` had no gate row under this run at all — reported rather
    than silently skipped, because a caller naming a sample that was never
    assembled under this run has almost certainly named the wrong run.

    `skipped_pending` were left `'pending'`: the assembly pipeline flips that
    value to a terminal one when the run lands, so a withdrawal written over it
    would be undone without anyone being told, and there is no contig set to
    withdraw yet. `skipped_no_data` were left `'no_data'`: that run assembled no
    contig, so there is nothing to withdraw and nothing to restore.
    """

    processing_idx: Annotated[int, Field(gt=0)]
    state: Literal["completed", "invalidated"]
    updated: list[int]
    unchanged: list[int]
    not_found: list[int]
    skipped_pending: list[int] = []
    skipped_no_data: list[int] = []
