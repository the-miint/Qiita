"""Wire models for the batch multi-study ENA import driver.

`POST /api/v1/ena-import-batch` accepts a list of accessions
(`BatchImportRequest`) and returns a handle immediately (`BatchImportResponse`);
`GET /api/v1/ena-import-batch/{idx}` (`BatchImportStatus`) polls per-item
progress.

The persisted `BatchItemState` values mirror the `qiita.ena_import_batch_item.state`
CHECK constraint — TEXT/CHECK, not a Postgres ENUM, same carve-out as `UploadStatus` /
`ReferenceStatus`; see CLAUDE.md "Enum parity". Keep both sides in sync by hand.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from qiita_common.models.work_ticket import FailureType

# Upper bound of `run_concurrency` on `download-ena-study`; its YAML `maximum` must match.
MAX_ENA_DOWNLOAD_RUN_CONCURRENCY = 8


class BatchItemState(StrEnum):
    """Per-item lifecycle within one ena_import_batch.

    pending -> resolving -> registered -> downloading -> done, with `failed`
    reachable from any non-terminal step. `done` and `completed_partial` are
    display states `fetch_batch_status` rolls up; the batch driver never writes them.
    """

    PENDING = "pending"
    RESOLVING = "resolving"
    REGISTERED = "registered"
    DOWNLOADING = "downloading"
    DONE = "done"
    COMPLETED_PARTIAL = "completed_partial"
    FAILED = "failed"


# Named beside BatchItemState, mirroring TERMINAL_WORK_TICKET_STATES, so a
# watch loop imports a shared tuple instead of a private per-caller copy.
TERMINAL_BATCH_ITEM_STATES: tuple[str, ...] = (
    BatchItemState.DONE.value,
    BatchItemState.COMPLETED_PARTIAL.value,
    BatchItemState.FAILED.value,
)


class BatchImportRequest(BaseModel):
    """Body for `POST /api/v1/ena-import-batch`.

    `accessions`: ENA study or run accessions, mixed. A study imports whole; runs
    are grouped by their study and import only those runs. The transport each spawned
    ticket uses is the download job's to choose.
    """

    model_config = ConfigDict(extra="forbid")

    accessions: list[str] = Field(min_length=1)


class EnaRunImportOutcome(BaseModel):
    """One ENA run's registration outcome within a batch item, surfaced on
    `GET /api/v1/ena-import-batch/{idx}`.

    `status` is the `EnaRunRegistrationStatus` value (`registered` /
    `skipped_already_present` / `excluded` / `failed` / `flagged_unavailable` /
    `held_not_downloaded` / `download_failed`). `failure_reason` is set on every
    status but the first two; `failure_type` only on `download_failed`.
    `metadata_warnings`: harmonization gaps on the biosample this run created.
    """

    run_accession: str
    status: str
    failure_reason: str | None = None
    failure_type: FailureType | None = None
    metadata_warnings: list[str] = Field(default_factory=list)


class FailedRunOutcome(BaseModel):
    """One run `ingest_ena_reads` could not store, as listed in its `run_outcomes`
    output for `persist-ena-run-outcomes`."""

    prep_sample_idx: int
    ena_run_accession: str
    failure_reason: str
    failure_type: FailureType


class BatchImportItem(BaseModel):
    """One item's current state within a batch: a whole study, or a list of runs.

    `ena_study_accession` is null only on a run-list item not grouped by study;
    `requested_run_accessions` is null for a whole-study item.
    """

    idx: int
    ena_study_accession: str | None
    requested_run_accessions: list[str] | None = None
    state: BatchItemState
    study_idx: int | None = None
    failure_reason: str | None = None
    download_work_ticket_idxs: list[int] = Field(default_factory=list)
    # Per-run registration outcomes for this item's study, populated once
    # register_ena_study runs (so an operator sees per-run failures, not just
    # the rolled-up item state). Empty until then.
    ena_runs: list[EnaRunImportOutcome] = Field(default_factory=list)


class BatchImportResponse(BaseModel):
    """Returned by `POST /api/v1/ena-import-batch` with HTTP 202: the batch handle
    plus every item at its just-created `pending` state."""

    ena_import_batch_idx: int
    items: list[BatchImportItem]


class BatchImportStatus(BaseModel):
    """Returned by `GET /api/v1/ena-import-batch/{idx}` — the current,
    rolled-up per-item state."""

    ena_import_batch_idx: int
    items: list[BatchImportItem]
