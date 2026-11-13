"""POST /definition-group/resolve: the member of a mask, alignment or processing
group that each named prep_sample resolves to (`definition_resolver` holds the
rules).

Human callers holding `prep_sample:read`, with `Tier.VIEWER` on every study each
requested prep_sample is still linked to. The cohort is refused whole (403) rather
than narrowed, as on every cohort route (`authorize_prep_sample_cohort`). The
resolution reads one read-only snapshot, so the groups, gates and tickets it reads
are of one moment.
"""

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from qiita_common.api_paths import PATH_DEFINITION_GROUP_PREFIX, PATH_DEFINITION_GROUP_RESOLVE
from qiita_common.auth_constants import Scope
from qiita_common.models import DefinitionGroupResolveRequest, DefinitionGroupResolveResponse

from ..auth.guards import COHORT_MIN_TIER, require_human, require_scope
from ..auth.principal import HumanUser, Principal
from ..definition_resolver import resolve_group
from ..definition_similarity import load_groups
from ..deps import get_db_pool
from ._helpers import authorize_prep_sample_cohort

router = APIRouter(prefix=PATH_DEFINITION_GROUP_PREFIX, tags=["definition-group"])


@router.post(PATH_DEFINITION_GROUP_RESOLVE)
async def resolve_definition_group_route(
    body: DefinitionGroupResolveRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
) -> DefinitionGroupResolveResponse:
    """Each requested prep_sample's state in the named definition's group:
    `processed` (and under which member), `incomplete` or `not_processed`, with the
    reason. 404 when the definition does not exist, before anything about the
    cohort is said; 403 when the caller may not read every sample."""
    async with pool.acquire() as conn, conn.transaction(isolation="repeatable_read", readonly=True):
        groups = await load_groups(conn)
        if (body.definition_kind, body.definition_idx) not in groups.definitions:
            raise HTTPException(
                status_code=404,
                detail=f"{body.definition_kind.value} {body.definition_idx} not found",
            )
        cohort = await authorize_prep_sample_cohort(
            conn, caller=caller, prep_sample_idx=body.prep_sample_idx, min_tier=COHORT_MIN_TIER
        )
        resolution = await resolve_group(
            conn,
            kind=body.definition_kind,
            definition_idx=body.definition_idx,
            prep_sample_idxs=cohort,
            groups=groups,
        )
    return DefinitionGroupResolveResponse(
        definition_kind=resolution.kind,
        definition_idx=resolution.definition_idx,
        group=list(resolution.group),
        samples=list(resolution.samples.values()),
    )
