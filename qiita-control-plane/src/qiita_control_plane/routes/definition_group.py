"""POST /definition-group/resolve: the member of a mask, alignment or processing
group that each named prep_sample resolves to (`definition_resolver` holds the
rules).

Human callers holding `prep_sample:read`, with `Tier.VIEWER` on every study each
requested prep_sample is still linked to. The cohort is refused whole (403) rather
than narrowed, as on every cohort route (`authorize_prep_sample_cohort`). The
resolution reads one read-only snapshot, so the groups, gates and tickets it reads
are of one moment.

Each prep_sample's `work_ticket_idx` is returned to a system_admin only
(`sees_resolution_tickets` says why).
"""

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from qiita_common.api_paths import PATH_DEFINITION_GROUP_PREFIX, PATH_DEFINITION_GROUP_RESOLVE
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    DefinitionGroupResolveRequest,
    DefinitionGroupResolveResponse,
    admitted_deprecated_upstream_kinds,
)

from ..auth.guards import COHORT_MIN_TIER, require_human, require_scope
from ..auth.principal import HumanUser, Principal
from ..definition_resolver import DefinitionNotObservable, require_observable, resolve_group
from ..deps import get_db_pool
from ._helpers import (
    authorize_prep_sample_cohort,
    load_groups_as_of,
    resolution_body,
    sees_resolution_tickets,
)

router = APIRouter(prefix=PATH_DEFINITION_GROUP_PREFIX, tags=["definition-group"])


@router.post(PATH_DEFINITION_GROUP_RESOLVE, response_model=DefinitionGroupResolveResponse)
async def resolve_definition_group_route(
    body: DefinitionGroupResolveRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
) -> DefinitionGroupResolveResponse | JSONResponse:
    """Each requested prep_sample's state in the named definition's group:
    `processed` (and under which member), `incomplete` or `not_processed`, with the
    reason. 404 when the definition does not exist and 422 when its group's runs are
    not read per prep_sample (`require_observable`), both before anything about the
    cohort is said; 403 when the caller may not read every prep_sample. Each
    prep_sample's `work_ticket_idx` is omitted unless the caller is a system_admin.

    `decisions` records the equivalence decisions the group was computed under; passing its
    `decision_set` back as `as_of_event` computes the group from those decisions again,
    with runs, gates, tickets and deprecation read as they are now (422 for a token
    that names no decision set). `include_deprecated` and `include_deprecated_upstream`
    resolve past deprecated upstreams of the kinds they admit
    (`admitted_deprecated_upstream_kinds`; `definition_resolver`, "Deprecated
    upstreams")."""
    async with pool.acquire() as conn, conn.transaction(isolation="repeatable_read", readonly=True):
        groups = await load_groups_as_of(conn, body.as_of_event)
        if (body.definition_kind, body.definition_idx) not in groups.definitions:
            raise HTTPException(
                status_code=404,
                detail=f"{body.definition_kind.value} {body.definition_idx} not found",
            )
        try:
            require_observable(groups, body.definition_kind, body.definition_idx)
        except DefinitionNotObservable as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        cohort = await authorize_prep_sample_cohort(
            conn, caller=caller, prep_sample_idx=body.prep_sample_idx, min_tier=COHORT_MIN_TIER
        )
        resolution = await resolve_group(
            conn,
            kind=body.definition_kind,
            definition_idx=body.definition_idx,
            prep_sample_idxs=cohort,
            groups=groups,
            admitted_upstream_kinds=admitted_deprecated_upstream_kinds(
                include_deprecated=body.include_deprecated,
                include_deprecated_upstream=body.include_deprecated_upstream,
            ),
        )
    response = DefinitionGroupResolveResponse(
        definition_kind=resolution.kind,
        definition_idx=resolution.definition_idx,
        group=list(resolution.group),
        samples=list(resolution.samples.values()),
        decisions=resolution.decisions,
    )
    if sees_resolution_tickets(caller):
        return response
    return JSONResponse(
        {
            **response.model_dump(mode="json", exclude={"samples"}),
            "samples": [resolution_body(r, caller) for r in response.samples],
        }
    )
