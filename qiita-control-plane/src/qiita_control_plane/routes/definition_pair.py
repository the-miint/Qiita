"""Admin routes for pair decisions between two definitions of one kind
(`qiita.definition_pair`): list, show, open, decide, revoke.

system_admin only, human callers only. Each pair is gated on its kind's lifecycle
scope (`mask_definition:lifecycle`, `alignment_definition:lifecycle`,
`processing:lifecycle`), the scope that deprecates a definition of that kind; the
route learns the kind from the body or the pair row, so the check is made in the
handler. The list returns only the kinds the caller's token holds.

The rules and every refusal are in `definition_similarity`; a refusal is a 409
whose detail carries a `reason` code and the pairs it names. A decision or revoke
that cannot take the group lock within its bounded retry is a 503 with
Retry-After.
"""

from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import Field
from qiita_common.api_paths import (
    PATH_ADMIN_DEFINITION_PAIR,
    PATH_ADMIN_DEFINITION_PAIR_BY_IDX,
    PATH_ADMIN_DEFINITION_PAIR_DECISION,
    PATH_ADMIN_DEFINITION_PAIR_REVOKE,
    PATH_ADMIN_PREFIX,
)
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    DefinitionKind,
    DefinitionPair,
    DefinitionPairDecisionRequest,
    DefinitionPairDetail,
    DefinitionPairEvent,
    DefinitionPairListResponse,
    DefinitionPairOpenRequest,
    DefinitionPairRevokeRequest,
    DefinitionPairVerdict,
)

from ..auth.guards import check_scope, require_any_scope, require_human
from ..auth.principal import HumanUser, Principal
from ..definition_similarity import (
    DefinitionNotFound,
    GroupLockBusy,
    PairNotFound,
    PairRefused,
    decide_pair,
    open_pair,
    revoke_pair,
)
from ..deps import TxConnFactory, get_db_pool, get_tx_conn_factory
from ..repositories.definition_pair import (
    fetch_pair,
    fetch_pair_events,
    list_pairs,
    pair_record_fields,
)
from ._helpers import cap_rows, pair_refused_http, raise_transient_retry

router = APIRouter(prefix=PATH_ADMIN_PREFIX, tags=["definition-pair"])

# The lifecycle scope that governs each kind's pairs.
_KIND_SCOPE: dict[DefinitionKind, Scope] = {
    DefinitionKind.MASK: Scope.MASK_DEFINITION_LIFECYCLE,
    DefinitionKind.ALIGNMENT: Scope.ALIGNMENT_DEFINITION_LIFECYCLE,
    DefinitionKind.PROCESSING: Scope.PROCESSING_LIFECYCLE,
}

_require_any_lifecycle = require_any_scope(*_KIND_SCOPE.values())

# Hard cap on the list. Pairs are opened by hand, one per decision an admin is asked
# to make, so the table is expected to stay far below it; a caller that hits it
# narrows with `verdict` or `definition_kind`.
_PAIR_LIST_HARD_CAP = 1_000

_MSG_PAIR_NOT_FOUND = "Definition pair not found"


def _pair_response(row: asyncpg.Record) -> DefinitionPair:
    return DefinitionPair.model_validate(pair_record_fields(row))


async def _fetch_pair_for_caller(
    conn: asyncpg.Pool | asyncpg.Connection, definition_pair_idx: int, caller: Principal
) -> asyncpg.Record:
    """The pair, 404 when absent, 403 when the caller lacks its kind's scope."""
    row = await fetch_pair(conn, definition_pair_idx)
    if row is None:
        raise HTTPException(status_code=404, detail=_MSG_PAIR_NOT_FOUND)
    check_scope(caller, _KIND_SCOPE[DefinitionKind(row["definition_kind"])])
    return row


@router.get(PATH_ADMIN_DEFINITION_PAIR)
async def list_definition_pair_route(
    verdict: Annotated[DefinitionPairVerdict | None, Query()] = None,
    definition_kind: Annotated[DefinitionKind | None, Query()] = None,
    pool: asyncpg.Pool = Depends(get_db_pool),
    _caller: HumanUser = Depends(require_human),
    principal: Principal = Depends(_require_any_lifecycle),
) -> DefinitionPairListResponse:
    """Pairs, oldest first, optionally of one verdict and one kind. Without
    `definition_kind`, only the kinds whose lifecycle scope the caller holds."""
    if definition_kind is not None:
        check_scope(principal, _KIND_SCOPE[definition_kind])
        kinds = [definition_kind]
    else:
        kinds = [k for k, scope in _KIND_SCOPE.items() if principal.has_scope(scope)]
    rows, truncated = cap_rows(
        await list_pairs(pool, kinds=kinds, verdict=verdict, limit=_PAIR_LIST_HARD_CAP + 1),
        _PAIR_LIST_HARD_CAP,
    )
    pairs = [_pair_response(r) for r in rows]
    return DefinitionPairListResponse(pairs=pairs, count=len(pairs), truncated=truncated)


@router.post(PATH_ADMIN_DEFINITION_PAIR, status_code=201)
async def open_definition_pair_route(
    body: DefinitionPairOpenRequest,
    response: Response,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(_require_any_lifecycle),
) -> DefinitionPair:
    """Open a pending pair of two definitions of one kind (201), or return the pair
    already recorded for them (200). Refused (409) across comparison scopes and when
    the two have the same params; 404 when a definition does not exist."""
    check_scope(caller, _KIND_SCOPE[body.definition_kind])
    try:
        async with tx() as conn:
            row, created = await open_pair(
                conn,
                kind=body.definition_kind,
                definition_idxs=body.definition_idx,
                principal_idx=caller.principal_idx,
            )
    except DefinitionNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PairRefused as exc:
        raise pair_refused_http(exc) from exc
    if not created:
        response.status_code = 200
    return _pair_response(row)


@router.get(PATH_ADMIN_DEFINITION_PAIR_BY_IDX)
async def get_definition_pair_route(
    definition_pair_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(_require_any_lifecycle),
) -> DefinitionPairDetail:
    """One pair and its history, oldest event first, read from one snapshot so the
    verdict agrees with the last event."""
    async with pool.acquire() as conn, conn.transaction(isolation="repeatable_read", readonly=True):
        row = await _fetch_pair_for_caller(conn, definition_pair_idx, caller)
        events = [
            DefinitionPairEvent.model_validate(dict(e))
            for e in await fetch_pair_events(conn, definition_pair_idx)
        ]
    return DefinitionPairDetail.model_validate({**pair_record_fields(row), "events": events})


@router.post(PATH_ADMIN_DEFINITION_PAIR_DECISION)
async def decide_definition_pair_route(
    definition_pair_idx: Annotated[int, Field(gt=0)],
    body: DefinitionPairDecisionRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(_require_any_lifecycle),
) -> DefinitionPair:
    """Decide a pair `similar` or `not_similar`, with a reason. Re-deciding a decided
    pair replaces its decision; the earlier one stays in the history."""
    await _fetch_pair_for_caller(pool, definition_pair_idx, caller)
    try:
        async with tx() as conn:
            row = await decide_pair(
                conn,
                definition_pair_idx=definition_pair_idx,
                verdict=body.verdict,
                reason=body.reason,
                principal_idx=caller.principal_idx,
            )
    except PairNotFound as exc:
        raise HTTPException(status_code=404, detail=_MSG_PAIR_NOT_FOUND) from exc
    except PairRefused as exc:
        raise pair_refused_http(exc) from exc
    except GroupLockBusy as exc:
        raise_transient_retry(str(exc))
    return _pair_response(row)


@router.post(PATH_ADMIN_DEFINITION_PAIR_REVOKE)
async def revoke_definition_pair_route(
    definition_pair_idx: Annotated[int, Field(gt=0)],
    body: DefinitionPairRevokeRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(_require_any_lifecycle),
) -> DefinitionPair:
    """Return a decided pair to pending, with a reason recorded in its history."""
    await _fetch_pair_for_caller(pool, definition_pair_idx, caller)
    try:
        async with tx() as conn:
            row = await revoke_pair(
                conn,
                definition_pair_idx=definition_pair_idx,
                reason=body.reason,
                principal_idx=caller.principal_idx,
            )
    except PairNotFound as exc:
        raise HTTPException(status_code=404, detail=_MSG_PAIR_NOT_FOUND) from exc
    except PairRefused as exc:
        raise pair_refused_http(exc) from exc
    except GroupLockBusy as exc:
        raise_transient_retry(str(exc))
    return _pair_response(row)
