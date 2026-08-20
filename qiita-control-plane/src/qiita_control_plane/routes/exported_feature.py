"""The public exported-feature map.

`POST /exported-feature` mints (or recovers) the label a published artifact uses
for each of its ROWS, the way `/exported-identifier` does for its columns. A
control-plane route rather than anything the data plane can serve: `genome_idx`,
`source_id` and `reference_membership.accession` live only in Postgres, and
minting is a write.

There is no cohort authorization here, and the asymmetry with
`/exported-identifier` is not an omission. That route names *samples*, which are
study-scoped and carry per-study access tiers. This one names reference
entities — genomes and the features of a reference — which are not study-scoped:
the same `reference:read` that already hands a caller the entire
`feature_idx → (genome_idx, source, source_id)` map through
`GET /reference/{idx}/genome-map` is exactly the authority this needs, and a
second answer to "may I read this reference" is how one surface comes to
advertise what another refuses.
"""

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from qiita_common.api_paths import (
    PATH_EXPORTED_FEATURE_PREFIX,
    PATH_EXPORTED_FEATURE_ROOT,
)
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    ExportedFeature,
    ExportedFeatureRequest,
    ExportedFeatureResponse,
)

from ..auth.guards import require_human, require_scope
from ..auth.principal import HumanUser, Principal
from ..deps import get_db_pool
from ..repositories.exported_feature import IncompleteMintError, mint_exported_features
from ._helpers import first_few, require_reference_exists

router = APIRouter(prefix=PATH_EXPORTED_FEATURE_PREFIX, tags=["exported-feature"])

# `require_human` rather than `require_complete_profile`, and `reference:read`
# even though this WRITES — both for the reasons `routes/exported_identifier.py`
# spells out at its own guard: a route that only names things sits at
# require_human, and the write is a mint of a name that is idempotent, creates
# nothing the caller could not already see, and takes exactly the authority "may I
# read this reference".


@router.post(PATH_EXPORTED_FEATURE_ROOT, status_code=201)
async def mint_exported_feature_map(
    body: ExportedFeatureRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.REFERENCE_READ)),
) -> ExportedFeatureResponse:
    """Mint the public label for each named feature-axis entity: genome entries
    first ascending by `genome_idx`, then feature entries ascending by `feature_idx`.

    A published feature table cannot carry `genome_idx` or `feature_idx` — those are
    ours, they mean nothing outside this system, and they are not handles we promise
    to keep. Where a real accession exists it is used instead of anything we mint,
    because an accession is what a reader can actually resolve; `QF<n>` is the
    fallback for an entity with none, or one whose accession another entity already
    publishes. A genome whose `source` is Qiita's own — assembled from a prep_sample
    rather than imported — always takes a minted handle: it carries a `source_id`,
    but no external repository resolves it.

    **The three artifacts of a bundle share this vocabulary.** The table, the
    taxonomy sidecar and the sheared tree all label a row with the
    `export_feature_id` this route returns, which is what lets them be used
    together.

    **Idempotent.** Re-requesting an entity returns the identifier it already has.

    A collided accession is **not** an error: the entity gets a minted handle and the
    response says so, via `accession` (what it wanted) alongside
    `accession_published: false` (that it lost). Only an entity this route cannot
    name at all is fatal — an unknown `genome_idx`, or a `feature_idx` that is not a
    member of the named reference and therefore has no accession from it. Those are
    reported together as a 422, because a map missing an entity would publish a table
    with an unnamed row.

    A `reference_idx` that does not exist is a 404 instead, checked up front the way
    `/exported-identifier` checks its alignment: every feature would otherwise be
    reported as "not in reference 99", which sends the caller to audit a membership
    table that was never the problem.
    """
    if body.reference_idx is not None:
        await require_reference_exists(pool, body.reference_idx)
    try:
        rows = await mint_exported_features(
            pool,
            genome_idx=body.genome_idx,
            reference_idx=body.reference_idx,
            feature_idx=body.feature_idx,
            created_by_idx=caller.principal_idx,
        )
    except IncompleteMintError as exc:
        detail = []
        if exc.genome_idx:
            detail.append(
                f"{len(exc.genome_idx)} unknown genome_idx (e.g. {first_few(exc.genome_idx)})"
            )
        if exc.feature_idx:
            detail.append(
                f"{len(exc.feature_idx)} feature_idx not in reference {body.reference_idx}"
                f" (e.g. {first_few(exc.feature_idx)})"
            )
        raise HTTPException(
            status_code=422,
            detail=(
                f"{'; '.join(detail)}. An identifier names something that exists, and"
                " nothing partial was returned"
            ),
        ) from exc

    identifiers = [ExportedFeature.model_validate(dict(row)) for row in rows]
    return ExportedFeatureResponse(identifiers=identifiers, count=len(identifiers))
