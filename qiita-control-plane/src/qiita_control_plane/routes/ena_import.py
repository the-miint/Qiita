"""Batch multi-study ENA import routes.

`POST /api/v1/ena-import-batch` accepts a *list* of ENA/SRA study
accessions, validates their shape up front (fail-loud, 422 on anything
malformed), and returns a batch handle immediately (202) — the
resolve/register/download-submit work for every accession runs in a
background task (`ena_import.batch`). ADMIN-only (wet_lab_admin /
system_admin): this is an operator gesture, mirroring the download-ena-study
workflow's own audience and `bcl-convert`'s admin-only submission shape.

`GET /api/v1/ena-import-batch/{idx}` reads the current, rolled-up per-item
status. Also ADMIN-only — the batch surface has no per-originator viewer
path yet (unlike `GET /work-ticket/{idx}`'s originator-or-bypass rule);
every admin sees every batch.
"""

from __future__ import annotations

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request, status
from qiita_common.api_paths import (
    PATH_ENA_IMPORT_BATCH_BY_IDX,
    PATH_ENA_IMPORT_BATCH_PREFIX,
    PATH_ENA_IMPORT_BATCH_ROOT,
)
from qiita_common.auth_constants import SystemRole
from qiita_common.models import (
    BatchImportItem,
    BatchImportRequest,
    BatchImportResponse,
    BatchImportStatus,
    BatchItemState,
)
from qiita_common.models.ena import ResolverKind, SourceArchive

from ..auth.guards import require_human_with_role
from ..auth.principal import HumanUser
from ..deps import get_db_pool
from ..ena_import.batch import (
    create_ena_import_batch,
    fetch_batch_status,
    schedule_ena_import_batch,
)
from ..ena_import.submit import DEFAULT_DOWNLOAD_METHOD

router = APIRouter(prefix=PATH_ENA_IMPORT_BATCH_PREFIX, tags=["ena-import-batch"])


def _require_compute_backend_client(request: Request) -> None:
    """Guard that 503s if the orchestrator dispatch path is not configured.

    A batch's download-ena-study tickets can never run without it; mirrors
    `routes.work_ticket._require_compute_backend_client` (kept as a small,
    local copy — a route-layer guard, not domain logic to share)."""
    if request.app.state.compute_backend_client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="compute orchestrator not configured (COMPUTE_ORCHESTRATOR_URL unset)",
        )


@router.post(
    PATH_ENA_IMPORT_BATCH_ROOT,
    response_model=BatchImportResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def submit_ena_import_batch(
    body: BatchImportRequest,
    request: Request,
    pool: asyncpg.Pool = Depends(get_db_pool),
    principal: HumanUser = Depends(require_human_with_role(SystemRole.WET_LAB_ADMIN)),
    _: None = Depends(_require_compute_backend_client),
) -> BatchImportResponse:
    """Fail loud, before writing anything, on: an unrecognized `source`
    archive, an unsupported `download_method` (only `'http'` today — no
    Aspera key-staging in this compute environment), or ANY malformed
    accession in the list (`ena_import.accession.validate_study_accession`,
    raised inside `create_ena_import_batch` as `InvalidEnaAccessionError`, a
    `ValueError` subclass) — one bad accession 422s the whole submission
    rather than silently dropping it or 500ing.

    On success: INSERT the batch + one `pending` item per accession
    (synchronous), fire the background processing task, and return 202
    with the handle. `principal` (the submitting admin) is threaded through
    to the background task as BOTH the owner/caller identity
    `register_ena_study` uses for every study this batch creates, AND the
    principal `submit_work_ticket_core` enforces the download-ena-study
    action's own audience against for every download ticket this batch
    submits — never bypassed just because this route is itself admin-gated.
    """
    try:
        source_archive = SourceArchive(body.source)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"unknown source archive: {body.source!r}",
        ) from exc

    if body.download_method != DEFAULT_DOWNLOAD_METHOD:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"unsupported download_method {body.download_method!r};"
                f" only {DEFAULT_DOWNLOAD_METHOD!r} is supported in this compute environment"
            ),
        )

    try:
        batch_idx, items = await create_ena_import_batch(
            pool,
            accessions=body.accessions,
            principal=principal,
            resolver_backend=body.backend,
            source_archive=source_archive,
            download_method=body.download_method,
        )
    except ValueError as exc:
        # Covers InvalidEnaAccessionError (a bad accession's shape) and an
        # unrecognized resolver `backend` name (get_resolver) -- both raised
        # by create_ena_import_batch BEFORE any row is written.
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)
        ) from exc

    schedule_ena_import_batch(
        request.app,
        items=items,
        principal=principal,
        resolver_backend=body.backend,
        source_archive=source_archive,
        resolver_kind=ResolverKind(body.backend),
        download_method=body.download_method,
    )

    return BatchImportResponse(
        ena_import_batch_idx=batch_idx,
        items=[
            BatchImportItem(
                ena_study_accession=item.ena_study_accession, state=BatchItemState.PENDING
            )
            for item in items
        ],
    )


@router.get(
    PATH_ENA_IMPORT_BATCH_BY_IDX,
    response_model=BatchImportStatus,
)
async def get_ena_import_batch(
    ena_import_batch_idx: int,
    pool: asyncpg.Pool = Depends(get_db_pool),
    _principal: HumanUser = Depends(require_human_with_role(SystemRole.WET_LAB_ADMIN)),
) -> BatchImportStatus:
    """Read a batch's current, rolled-up per-item status (see
    `ena_import.batch.fetch_batch_status` for the rollup rule)."""
    result = await fetch_batch_status(pool, batch_idx=ena_import_batch_idx)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"ena_import_batch {ena_import_batch_idx} not found",
        )
    return result
