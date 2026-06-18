"""DELETE /api/v1/reference-artifact/{reference_idx} — on-disk index cleanup.

The control-plane `DELETE /reference/{idx}` flow removes a reference's
Postgres rows and DuckLake data itself, but the persistent on-disk index
artifacts a host reference builds —
`{path_derived}/references/{idx}/{rype,minimap2}/...` — live on the compute
host, reachable only from the orchestrator side. This single synchronous
endpoint deletes that per-reference directory.

It is *not* a SLURM step: there is no job to schedule, just an idempotent
`rmtree` of one directory tree on the shared filesystem the orchestrator
already resolves paths against. Driving it as a `/step/*` submission would
need a `work_ticket` and the poll loop for what is one filesystem call.

Auth reuses the shared CP↔CO bearer guard from `step.py` — same private
two-service path, same constant-time compare.
"""

from __future__ import annotations

import shutil
from typing import Annotated

from fastapi import APIRouter, Depends, Request
from pydantic import Field
from qiita_common.api_paths import (
    PATH_REFERENCE_ARTIFACT_BY_IDX,
    PATH_REFERENCE_ARTIFACT_PREFIX,
)
from qiita_common.models import ReferenceArtifactPurgeResponse

from .config import Settings
from .derived_store import reference_derived_dir
from .step import _require_cp_to_co_token

router = APIRouter(prefix=PATH_REFERENCE_ARTIFACT_PREFIX, tags=["reference-artifact"])


def _get_settings(request: Request) -> Settings:
    return request.app.state.settings


@router.delete(PATH_REFERENCE_ARTIFACT_BY_IDX)
async def purge_reference_artifacts(
    reference_idx: Annotated[int, Field(gt=0)],
    settings: Annotated[Settings, Depends(_get_settings)],
    _: Annotated[None, Depends(_require_cp_to_co_token)],
) -> ReferenceArtifactPurgeResponse:
    """Remove `{path_derived}/references/{reference_idx}` and everything under
    it (the rype `.ryxdi` directory and the minimap2 `.mmi`). Idempotent: a
    reference that never built indexes (non-host, or never reached `indexing`)
    has no directory, so `removed` comes back False and the call still
    succeeds."""
    target = reference_derived_dir(settings.path_derived, reference_idx)
    removed = target.exists()
    if removed:
        # ignore_errors=False so a real failure (permissions, partial FS)
        # surfaces as a 500 rather than silently reporting success — the CP
        # logs it and the operator can re-run the idempotent delete.
        shutil.rmtree(target)
    return ReferenceArtifactPurgeResponse(
        reference_idx=reference_idx,
        path=str(target),
        removed=removed,
    )
