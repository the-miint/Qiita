"""Wire models for the batch multi-study ENA import driver.

`POST /api/v1/ena-import-batch` accepts a list of accessions
(`BatchImportRequest`) and returns a handle immediately (`BatchImportResponse`);
`GET /api/v1/ena-import-batch/{idx}` (`BatchImportStatus`) polls per-item
progress.

`BatchItemState` mirrors the `qiita.ena_import_batch_item.state` CHECK
constraint — TEXT/CHECK, not a Postgres ENUM, same carve-out as `UploadStatus` /
`ReferenceStatus`; see CLAUDE.md "Enum parity". Keep both sides in sync by hand.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class BatchItemState(StrEnum):
    """Per-accession lifecycle within one ena_import_batch.

    pending -> resolving -> registered -> downloading -> done, with `failed`
    reachable from any non-terminal step. `done` is a rolled-up display state
    computed on demand from the item's download tickets' states (all
    terminal-success) — the batch driver never writes it.
    """

    PENDING = "pending"
    RESOLVING = "resolving"
    REGISTERED = "registered"
    DOWNLOADING = "downloading"
    DONE = "done"
    FAILED = "failed"


class BatchImportRequest(BaseModel):
    """Body for `POST /api/v1/ena-import-batch`.

    `accessions`: ENA STUDY accessions, one `qiita.study` per entry. The
    transport each spawned ticket uses is the download job's to choose.
    """

    model_config = ConfigDict(extra="forbid")

    accessions: list[str] = Field(min_length=1)


class RunImportOutcome(BaseModel):
    """One ENA run's registration outcome within a batch item, surfaced on
    `GET /api/v1/ena-import-batch/{idx}`.

    `status` is the `RunRegistrationStatus` value
    (`registered` / `skipped_already_present` / `failed`). `failure_reason` is
    set only on `failed`.
    """

    run_accession: str
    status: str
    failure_reason: str | None = None


class BatchImportItem(BaseModel):
    """One accession's current state within a batch."""

    ena_study_accession: str
    state: BatchItemState
    study_idx: int | None = None
    failure_reason: str | None = None
    download_work_ticket_idxs: list[int] = Field(default_factory=list)
    # Per-run registration outcomes for this item's study, populated once
    # register_ena_study runs (so an operator sees per-run failures, not just
    # the rolled-up item state). Empty until then.
    runs: list[RunImportOutcome] = Field(default_factory=list)


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
