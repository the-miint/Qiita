"""The public exported-processing handle.

`POST /exported-processing` mints (or recovers) the name a published bundle's
manifest cites for the processing it was built from. A control-plane route for the
same reason as its two siblings: the handle lives only in Postgres and minting one
is a write.

Authorization is `/exported-identifier`'s, step for step, over the same
`(alignment_idx, cohort)` pair — and that is the point rather than convenience. A
processing is not study-scoped, so it is tempting to conclude there is nothing here
to authorize; but minting is a **write**, and a route that wrote on behalf of data
the caller cannot read would hand any human a handle for every processing in the
system, one `alignment_idx` at a time. The cohort is what closes that: it is not part
of the handle's identity, it is how a caller shows they could have built the table
this manifest would describe.
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
from ..repositories.exported_processing import ProcessingVanishedError, mint_exported_processing
from ._helpers import authorize_completed_alignment_cohort

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

    `prep_sample_idx` authorizes; it does not appear in the answer. The validation
    order below is `/exported-identifier`'s, in the same order and through the same
    three helpers, and that route's docstring is the single copy of why the order is
    what it is. The one difference: it is the *cohort* that must be readable and
    completed, and the handle that comes back names only the processing — so two
    callers publishing different cohorts of one alignment cite the same handle, which
    is what lets a reader see they share a processing.
    """
    await authorize_completed_alignment_cohort(
        pool,
        caller=caller,
        alignment_idx=body.alignment_idx,
        prep_sample_idx=body.prep_sample_idx,
        nothing_to="; a manifest describes processed data, so there is nothing yet to describe",
    )

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
