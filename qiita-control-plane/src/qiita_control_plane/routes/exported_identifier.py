"""The public exported-identifier map.

`POST /exported-identifier` mints (or recovers) an `export_id` for every
processed sample in a cohort, so a published feature table can name its samples
without carrying ours. A control-plane route rather than anything the data plane
can serve: the identifiers live only in Postgres, and minting one is a write.

Cohort authorization is `authorize_prep_sample_cohort`, shared with the human
alignment mint: minting a ticket for a cohort and minting the public handles for
that same cohort is one workflow, and two answers to "may I read this sample" is
how one surface comes to advertise what the other refuses.
"""

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from qiita_common.api_paths import (
    PATH_EXPORTED_IDENTIFIER_PREFIX,
    PATH_EXPORTED_IDENTIFIER_ROOT,
)
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    ExportedIdentifier,
    ExportedIdentifierRequest,
    ExportedIdentifierResponse,
)

from ..auth.guards import require_human, require_scope
from ..auth.principal import HumanUser, Principal
from ..deps import get_db_pool
from ..repositories.exported_identifier import IncompleteMintError, mint_exported_identifiers
from ._helpers import authorize_completed_alignment_cohort, first_few

router = APIRouter(prefix=PATH_EXPORTED_IDENTIFIER_PREFIX, tags=["exported-identifier"])

_MSG_ALIGNMENT_NOT_FOUND = "alignment not found"

# `require_human`, NOT the alignment mint's `require_complete_profile`, and the
# difference is the rule rather than an oversight: a route that hands out access
# to raw sequence-derived data demands a complete profile, while one that only
# names things sits at require_human (as the read-mask discovery GETs do).
#
# `prep_sample:read` even though this WRITES. The write is a mint of a name, not
# of scientific data: it is idempotent, it creates nothing a caller could not
# already see, and the authority it takes is exactly "may I read this sample".
# A separate write scope would have to be granted to every role that can already
# read a sample, which is the same set.


@router.post(PATH_EXPORTED_IDENTIFIER_ROOT, status_code=201)
async def mint_exported_identifier_map(
    body: ExportedIdentifierRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
) -> ExportedIdentifierResponse:
    """Mint the public handle for each processed sample in a cohort, ascending by
    prep_sample_idx.

    A published artifact cannot carry `prep_sample_idx` — those identifiers are
    ours, they mean nothing outside this system, and they are not a handle we
    promise to keep. Nor can an accession stand in: a biosample sequenced
    repeatedly has several prep_samples, so its accession cannot say which
    sequencing a row came from, and `ena_run_accession` is NULL until the data is
    submitted. `export_id` is what a table names its columns with; this map is the
    only place both it and `prep_sample_idx` appear, so the caller can join it to
    their alignment rows.

    **Idempotent.** Re-requesting a cohort returns the identifiers it already has.
    An export_id is published, so the same processed sample must resolve the same
    way every time.

    The cohort gate is `authorize_completed_alignment_cohort`, shared with the
    alignment ticket mint and the processing mint: the three take the same cohort and
    must never disagree about it, and its docstring is the single copy of why the
    checks run in the order they do.

    There is deliberately no refusal for a missing `biosample_accession`. The
    label this route replaced could not be composed without one; an `export_id`
    always can, so an unaccessioned sample is now labellable rather than a 422.
    """
    cohort = await authorize_completed_alignment_cohort(
        pool,
        caller=caller,
        alignment_idx=body.alignment_idx,
        prep_sample_idx=body.prep_sample_idx,
        nothing_to="; an identifier names processed data, so there is nothing yet to name",
    )

    try:
        rows = await mint_exported_identifiers(
            pool,
            alignment_idx=body.alignment_idx,
            prep_sample_idxs=cohort,
            created_by_idx=caller.principal_idx,
        )
    except IncompleteMintError as exc:
        # The only way to get here is a concurrent purge of this alignment (which
        # retires the identifiers it named) between the checks above and the mint.
        # 409 rather than 500 because nothing is wrong with the request: the
        # alignment it named stopped existing mid-flight, and a retry against the
        # re-aligned data is the correct next move.
        raise HTTPException(
            status_code=409,
            detail=(
                f"alignment {body.alignment_idx} changed while minting; no identifier"
                f" for {len(exc.missing)} prep_sample(s) (e.g."
                f" {first_few(exc.missing)}). Nothing partial was returned — retry"
            ),
        ) from exc
    identifiers = [ExportedIdentifier.model_validate(dict(row)) for row in rows]
    return ExportedIdentifierResponse(
        alignment_idx=body.alignment_idx,
        identifiers=identifiers,
        count=len(identifiers),
    )
