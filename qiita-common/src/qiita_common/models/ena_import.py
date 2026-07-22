"""Wire models for the batch multi-study ENA import driver (TASK-06).

`POST /api/v1/ena-import-batch` accepts a *list* of ENA/SRA study
accessions (`BatchImportRequest`) and returns a handle immediately
(`BatchImportResponse`) — the resolve+register+download-submit work runs in
a background task (`qiita_control_plane.ena_import.batch`). `GET
/api/v1/ena-import-batch/{idx}` (`BatchImportStatus`) polls per-item
progress.

`BatchItemState` mirrors the `qiita.ena_import_batch_item.state` CHECK
constraint (db/migrations) — TEXT/CHECK, not a Postgres ENUM, same carve-out
as `UploadStatus` / `ReferenceStatus`; see CLAUDE.md "Enum parity". Keep
both sides in sync by hand.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class BatchItemState(StrEnum):
    """Per-accession lifecycle within one ena_import_batch.

    pending -> resolving -> registered -> downloading -> done, with
    `failed` reachable from any non-terminal step. `done` is a rolled-up
    display state computed on demand from the item's
    `download_work_ticket_idxs`' `qiita.work_ticket.state` (all
    terminal-success) — the batch driver itself never writes it; see
    `qiita_control_plane.routes.ena_import`.
    """

    PENDING = "pending"
    RESOLVING = "resolving"
    REGISTERED = "registered"
    DOWNLOADING = "downloading"
    DONE = "done"
    FAILED = "failed"


class BatchImportRequest(BaseModel):
    """Body for `POST /api/v1/ena-import-batch`.

    `accessions` is the list of ENA/SRA STUDY accessions to import, one
    `qiita.study` per entry (D4). `backend` selects the `EnaResolver`
    implementation (`ena_import.factory.get_resolver` — `'miint'` default,
    `'http'` the experimental fallback); `source` is the archive the reads
    will come from (`SourceArchive` — `'ena'` default); `download_method`
    is the transport pinned into each spawned `download-ena-study` ticket
    (only `'http'` is supported in this compute environment today, mirroring
    `ena_import.submit.DEFAULT_DOWNLOAD_METHOD`).
    """

    accessions: list[str] = Field(min_length=1)
    backend: str = "miint"
    source: str = "ena"
    download_method: str = "http"


class BatchImportItem(BaseModel):
    """One accession's current state within a batch."""

    ena_study_accession: str
    state: BatchItemState
    study_idx: int | None = None
    failure_reason: str | None = None
    download_work_ticket_idxs: list[int] = Field(default_factory=list)


class BatchImportResponse(BaseModel):
    """Returned by `POST /api/v1/ena-import-batch` with HTTP 202.

    The batch handle plus every item at its just-created `pending` state —
    the background task has not yet had a chance to run."""

    ena_import_batch_idx: int
    items: list[BatchImportItem]


class BatchImportStatus(BaseModel):
    """Returned by `GET /api/v1/ena-import-batch/{idx}` — the current,
    rolled-up per-item state."""

    ena_import_batch_idx: int
    items: list[BatchImportItem]
