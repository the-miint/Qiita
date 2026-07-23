"""Batch multi-study ENA import routes.

`POST /api/v1/ena-import-batch` accepts a *list* of ENA/SRA study accessions,
validates their shape up front (422 on anything malformed), and returns a batch
handle immediately (202) — the resolve/register/download-submit work runs in a
background task (`ena_import.batch`). `GET /api/v1/ena-import-batch/{idx}` reads
the rolled-up per-item status. Both ADMIN-only (wet_lab_admin / system_admin);
every admin sees every batch (no per-originator viewer path yet).
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
    """503 if the orchestrator dispatch path is not configured -- a batch's
    download-ena-study tickets can never run without it. Local copy of
    `routes.work_ticket._require_compute_backend_client` (a route-layer guard)."""
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
    """Fail loud (422), before writing anything, on an unrecognized `source`, an
    unsupported `download_method`, or any malformed accession — one bad accession
    422s the whole submission.

    On success: INSERT the batch + one `pending` item per accession, fire the
    background task, return 202. `principal` (the submitting admin) is threaded
    into the background task as BOTH the owner/caller identity for every study
    and the principal `submit_work_ticket_core` enforces each download ticket's
    audience against — never bypassed just because this route is admin-gated.
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
        # InvalidEnaAccessionError or an unrecognized resolver `backend` -- both
        # raised by create_ena_import_batch before any row is written.
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
