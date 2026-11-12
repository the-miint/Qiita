"""Batch multi-study ENA import routes.

`POST /api/v1/ena-import-batch` accepts a *list* of INSDC study or run accessions,
validates their shape up front (422 on anything malformed), and returns a batch
handle immediately (202) — the resolve/register/download-submit work runs in a
background task (`ena_import.batch`). `GET /api/v1/ena-import-batch/{idx}` reads
the rolled-up per-item status; `GET /api/v1/ena-import-batch` lists batches. All
ADMIN-only (wet_lab_admin / system_admin); every admin can read every batch.
"""

from __future__ import annotations

from typing import Annotated, Literal

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import AwareDatetime
from qiita_common.api_paths import (
    PATH_ENA_IMPORT_BATCH_BY_IDX,
    PATH_ENA_IMPORT_BATCH_PREFIX,
    PATH_ENA_IMPORT_BATCH_ROOT,
)
from qiita_common.auth_constants import SystemRole
from qiita_common.models import (
    BatchImportItem,
    BatchImportListResponse,
    BatchImportRequest,
    BatchImportResponse,
    BatchImportStatus,
    BatchImportSummary,
    BatchItemState,
)
from qiita_common.models.ena_import import (
    ENA_IMPORT_BATCH_LIST_DEFAULT_LIMIT,
    ENA_IMPORT_BATCH_LIST_MAX_LIMIT,
)

from ..auth.guards import METADATA_BYPASS_ROLE, require_human_with_role
from ..auth.principal import HumanUser
from ..deps import get_db_pool
from ..ena_import.batch import (
    create_ena_import_batch,
    fetch_batch_status,
    schedule_ena_import_batch,
)
from ..repositories.ena_import_batch import fetch_ena_import_batches
from ._helpers import cap_rows
from .work_ticket import _require_compute_backend_client

router = APIRouter(prefix=PATH_ENA_IMPORT_BATCH_PREFIX, tags=["ena-import-batch"])


@router.post(
    PATH_ENA_IMPORT_BATCH_ROOT,
    response_model=BatchImportResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def submit_ena_import_batch(
    body: BatchImportRequest,
    request: Request,
    pool: asyncpg.Pool = Depends(get_db_pool),
    principal: HumanUser = Depends(require_human_with_role(METADATA_BYPASS_ROLE)),
    _: None = Depends(_require_compute_backend_client),
) -> BatchImportResponse:
    """Fail loud (422), before writing anything, on any malformed accession:
    one bad accession 422s the whole submission.

    On success: INSERT the batch + one `pending` item per accession, fire the
    background task, return 202. `principal` (the submitting admin) is threaded
    into the background task as BOTH the owner/caller identity for every study
    and the principal `submit_work_ticket_core` enforces each download ticket's
    audience against — never bypassed just because this route is admin-gated.
    """
    try:
        batch_idx, items = await create_ena_import_batch(
            pool,
            accessions=body.accessions,
            principal=principal,
        )
    except ValueError as exc:
        # InvalidEnaAccessionError, raised before any row is written.
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)
        ) from exc

    schedule_ena_import_batch(
        request.app,
        items=items,
        principal=principal,
    )

    return BatchImportResponse(
        ena_import_batch_idx=batch_idx,
        items=[
            BatchImportItem(
                idx=item.idx,
                ena_study_accession=item.ena_study_accession,
                requested_run_accessions=item.requested_run_accessions,
                state=BatchItemState.PENDING,
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


@router.get(
    PATH_ENA_IMPORT_BATCH_ROOT,
    response_model=BatchImportListResponse,
)
async def list_ena_import_batches(
    submitted_by: Literal["me"] | None = None,
    since: AwareDatetime | None = None,
    limit: Annotated[
        int, Query(ge=1, le=ENA_IMPORT_BATCH_LIST_MAX_LIMIT)
    ] = ENA_IMPORT_BATCH_LIST_DEFAULT_LIMIT,
    pool: asyncpg.Pool = Depends(get_db_pool),
    principal: HumanUser = Depends(require_human_with_role(SystemRole.WET_LAB_ADMIN)),
) -> BatchImportListResponse:
    """List batches newest first, counting each batch's items by rolled-up state, not
    by their stored state."""
    rows = await fetch_ena_import_batches(
        pool,
        submitted_by_principal_idx=principal.principal_idx if submitted_by == "me" else None,
        since=since,
        limit=limit + 1,
    )
    rows, truncated = cap_rows(rows, limit)
    summaries = []
    for row in rows:
        result = await fetch_batch_status(pool, batch_idx=row["ena_import_batch_idx"])
        if result is None:
            raise RuntimeError(
                f"ena_import_batch {row['ena_import_batch_idx']} vanished while being listed"
            )
        counts = dict.fromkeys(BatchItemState, 0)
        for item in result.items:
            counts[item.state] += 1
        summaries.append(
            BatchImportSummary.model_validate({**dict(row), "item_state_counts": counts})
        )
    return BatchImportListResponse(batches=summaries, count=len(summaries), truncated=truncated)
