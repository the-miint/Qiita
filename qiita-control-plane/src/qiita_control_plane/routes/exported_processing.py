"""The public exported-processing handle.

`POST /exported-processing` mints (or recovers) the name a published bundle's
manifest cites for the processing it was built from. A control-plane route for the
same reason as its two siblings: the handle lives only in Postgres and minting one
is a write.

There is no cohort authorization, and the asymmetry with `/exported-identifier` is
deliberate. That route names *samples*, which are study-scoped and carry per-study
access tiers. This one names a processing, which is not study-scoped, and the only
thing it discloses is whether an `alignment_idx` exists — precisely what
`/exported-identifier` already discloses at its own first step, under the same
scope, before it looks at a cohort at all. A second answer to that question is how
one surface comes to advertise what another refuses.
"""

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from qiita_common.api_paths import (
    PATH_EXPORTED_PROCESSING_PREFIX,
    PATH_EXPORTED_PROCESSING_ROOT,
)
from qiita_common.auth_constants import Scope
from qiita_common.models import ExportedProcessingRequest, ExportedProcessingResponse

from ..auth.guards import require_human, require_scope
from ..auth.principal import HumanUser, Principal
from ..deps import get_db_pool
from ..repositories.alignment_definition import alignment_definition_exists
from ..repositories.exported_processing import ProcessingVanishedError, mint_exported_processing

router = APIRouter(prefix=PATH_EXPORTED_PROCESSING_PREFIX, tags=["exported-processing"])

_MSG_ALIGNMENT_NOT_FOUND = "alignment not found"

# `require_human` and `prep_sample:read` even though this WRITES — both for the
# reasons routes/exported_identifier.py spells out at its own guard: a route that
# only names things sits at require_human, and the write is a mint of a name that is
# idempotent and creates nothing the caller could not already see.


@router.post(PATH_EXPORTED_PROCESSING_ROOT, status_code=201)
async def mint_exported_processing_handle(
    body: ExportedProcessingRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
) -> ExportedProcessingResponse:
    """Mint the public handle for a processing, so a bundle's manifest can say what
    produced the table without naming our `alignment_idx`.

    A manifest is not optional documentation here: coverage filtering makes a feature
    table a function of the cohort it was built over rather than of the samples in it,
    so a table without a record of its processing cannot be reproduced.

    **Idempotent.** Re-requesting a processing returns the handle it already has, so
    two bundles built from one processing cite it identically.
    """
    if not await alignment_definition_exists(pool, body.alignment_idx):
        raise HTTPException(status_code=404, detail=_MSG_ALIGNMENT_NOT_FOUND)

    try:
        row = await mint_exported_processing(
            pool, alignment_idx=body.alignment_idx, created_by_idx=caller.principal_idx
        )
    except ProcessingVanishedError as exc:
        # A concurrent purge of this alignment between the check above and the mint.
        # 409 rather than 500 because nothing is wrong with the request: the
        # processing it named stopped existing mid-flight.
        raise HTTPException(
            status_code=409,
            detail=(
                f"alignment {body.alignment_idx} was purged while minting its public"
                " handle; there is no processing left to name — retry against the"
                " re-run data"
            ),
        ) from exc

    return ExportedProcessingResponse(
        alignment_idx=row["alignment_idx"],
        export_processing_id=row["export_processing_id"],
    )
