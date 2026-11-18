"""Admin routes for output equivalences between two definitions of one kind
(`qiita.output_equivalence`): list, show, open, decide, revoke.

system_admin only, human callers only. Each output equivalence is gated on its kind's lifecycle
scope (`mask_definition:lifecycle`, `alignment_definition:lifecycle`,
`processing:lifecycle`), the scope that deprecates a definition of that kind; the
route learns the kind from the body or the output-equivalence row, so the check is made in the
handler. The list returns only the kinds the caller's token holds.

The rules and every refusal are in `definition_similarity`; a refusal is a 409
whose detail carries a `reason` code and the output equivalences it names. A decision or revoke
that cannot take the group lock within its bounded retry is a 503 with
Retry-After.
"""

from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import Field
from qiita_common.api_paths import (
    PATH_ADMIN_OUTPUT_EQUIVALENCE,
    PATH_ADMIN_OUTPUT_EQUIVALENCE_BY_IDX,
    PATH_ADMIN_OUTPUT_EQUIVALENCE_DECISION,
    PATH_ADMIN_OUTPUT_EQUIVALENCE_REVOKE,
    PATH_ADMIN_PREFIX,
)
from qiita_common.models import (
    DefinitionKind,
    OutputEquivalence,
    OutputEquivalenceDecisionRequest,
    OutputEquivalenceDetail,
    OutputEquivalenceEvent,
    OutputEquivalenceListResponse,
    OutputEquivalenceOpenRequest,
    OutputEquivalenceRevokeRequest,
    OutputEquivalenceVerdict,
)

from ..auth.guards import check_scope, require_any_scope, require_human
from ..auth.principal import HumanUser, Principal
from ..auth.scopes import KIND_LIFECYCLE_SCOPE
from ..definition_similarity import (
    DefinitionNotFound,
    EquivalenceNotFound,
    EquivalenceRefused,
    GroupLockBusy,
    decide_equivalence,
    open_equivalence,
    revoke_equivalence,
)
from ..deps import TxConnFactory, get_db_pool, get_tx_conn_factory
from ..repositories.output_equivalence import (
    equivalence_record_fields,
    fetch_equivalence,
    fetch_equivalence_events,
    list_equivalences,
)
from ._helpers import cap_rows, equivalence_refused_http, raise_transient_retry

router = APIRouter(prefix=PATH_ADMIN_PREFIX, tags=["output-equivalence"])

_require_any_lifecycle = require_any_scope(*KIND_LIFECYCLE_SCOPE.values())

# The list's page size: the default, and the most one request may ask for.
_EQUIVALENCE_LIST_DEFAULT_LIMIT = 100
_EQUIVALENCE_LIST_MAX_LIMIT = 1_000

_MSG_EQUIVALENCE_NOT_FOUND = "Output equivalence not found"


def _equivalence_response(row: asyncpg.Record) -> OutputEquivalence:
    return OutputEquivalence.model_validate(equivalence_record_fields(row))


async def _fetch_equivalence_for_caller(
    conn: asyncpg.Pool | asyncpg.Connection, output_equivalence_idx: int, caller: Principal
) -> asyncpg.Record:
    """The equivalence, 404 when absent, 403 when the caller lacks its kind's scope."""
    row = await fetch_equivalence(conn, output_equivalence_idx)
    if row is None:
        raise HTTPException(status_code=404, detail=_MSG_EQUIVALENCE_NOT_FOUND)
    check_scope(caller, KIND_LIFECYCLE_SCOPE[DefinitionKind(row["definition_kind"])])
    return row


@router.get(PATH_ADMIN_OUTPUT_EQUIVALENCE)
async def list_output_equivalence_route(
    verdict: Annotated[OutputEquivalenceVerdict | None, Query()] = None,
    definition_kind: Annotated[DefinitionKind | None, Query()] = None,
    after_output_equivalence_idx: Annotated[
        int | None,
        Query(gt=0, description="Cursor: the previous page's next_after_output_equivalence_idx."),
    ] = None,
    limit: Annotated[
        int, Query(ge=1, le=_EQUIVALENCE_LIST_MAX_LIMIT)
    ] = _EQUIVALENCE_LIST_DEFAULT_LIMIT,
    pool: asyncpg.Pool = Depends(get_db_pool),
    _caller: HumanUser = Depends(require_human),
    principal: Principal = Depends(_require_any_lifecycle),
) -> OutputEquivalenceListResponse:
    """One page of equivalences, newest first, optionally of one verdict and one kind.
    Without `definition_kind`, only the kinds whose lifecycle scope the caller
    holds. `next_after_output_equivalence_idx` is the cursor for the next page, None on
    the last."""
    if definition_kind is not None:
        check_scope(principal, KIND_LIFECYCLE_SCOPE[definition_kind])
        kinds = [definition_kind]
    else:
        kinds = [k for k, scope in KIND_LIFECYCLE_SCOPE.items() if principal.has_scope(scope)]
    rows, more = cap_rows(
        await list_equivalences(
            pool,
            kinds=kinds,
            verdict=verdict,
            before_output_equivalence_idx=after_output_equivalence_idx,
            limit=limit + 1,
        ),
        limit,
    )
    equivalences = [_equivalence_response(r) for r in rows]
    return OutputEquivalenceListResponse(
        output_equivalences=equivalences,
        count=len(equivalences),
        next_after_output_equivalence_idx=equivalences[-1].output_equivalence_idx if more else None,
    )


@router.post(PATH_ADMIN_OUTPUT_EQUIVALENCE, status_code=201)
async def open_output_equivalence_route(
    body: OutputEquivalenceOpenRequest,
    response: Response,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(_require_any_lifecycle),
) -> OutputEquivalence:
    """Open a pending equivalence of two definitions of one kind (201), or return the equivalence
    already recorded for them (200). Refused (409) across comparison scopes and when
    the two have the same params; 404 when a definition does not exist."""
    check_scope(caller, KIND_LIFECYCLE_SCOPE[body.definition_kind])
    try:
        async with tx() as conn:
            row, created = await open_equivalence(
                conn,
                kind=body.definition_kind,
                definition_idxs=body.definition_idx,
                principal_idx=caller.principal_idx,
            )
    except DefinitionNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except EquivalenceRefused as exc:
        raise equivalence_refused_http(exc) from exc
    if not created:
        response.status_code = 200
    return _equivalence_response(row)


@router.get(PATH_ADMIN_OUTPUT_EQUIVALENCE_BY_IDX)
async def get_output_equivalence_route(
    output_equivalence_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(_require_any_lifecycle),
) -> OutputEquivalenceDetail:
    """One equivalence and its history, oldest event first, read from one snapshot so the
    verdict agrees with the last event."""
    async with pool.acquire() as conn, conn.transaction(isolation="repeatable_read", readonly=True):
        row = await _fetch_equivalence_for_caller(conn, output_equivalence_idx, caller)
        events = [
            OutputEquivalenceEvent.model_validate(dict(e))
            for e in await fetch_equivalence_events(conn, output_equivalence_idx)
        ]
    return OutputEquivalenceDetail.model_validate(
        {**equivalence_record_fields(row), "events": events}
    )


@router.post(PATH_ADMIN_OUTPUT_EQUIVALENCE_DECISION)
async def decide_output_equivalence_route(
    output_equivalence_idx: Annotated[int, Field(gt=0)],
    body: OutputEquivalenceDecisionRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(_require_any_lifecycle),
) -> OutputEquivalence:
    """Decide an output equivalence (`same`, `similar`, `different` or
    `not_benchmarked`), with a reason. Re-deciding a decided output equivalence replaces
    its decision; the earlier one stays in the history."""
    await _fetch_equivalence_for_caller(pool, output_equivalence_idx, caller)
    try:
        async with tx() as conn:
            row = await decide_equivalence(
                conn,
                output_equivalence_idx=output_equivalence_idx,
                verdict=body.verdict,
                reason=body.reason,
                principal_idx=caller.principal_idx,
            )
    except EquivalenceNotFound as exc:
        raise HTTPException(status_code=404, detail=_MSG_EQUIVALENCE_NOT_FOUND) from exc
    except EquivalenceRefused as exc:
        raise equivalence_refused_http(exc) from exc
    except GroupLockBusy as exc:
        raise_transient_retry(str(exc))
    return _equivalence_response(row)


@router.post(PATH_ADMIN_OUTPUT_EQUIVALENCE_REVOKE)
async def revoke_output_equivalence_route(
    output_equivalence_idx: Annotated[int, Field(gt=0)],
    body: OutputEquivalenceRevokeRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(_require_any_lifecycle),
) -> OutputEquivalence:
    """Return a decided equivalence to pending, with a reason recorded in its history."""
    await _fetch_equivalence_for_caller(pool, output_equivalence_idx, caller)
    try:
        async with tx() as conn:
            row = await revoke_equivalence(
                conn,
                output_equivalence_idx=output_equivalence_idx,
                reason=body.reason,
                principal_idx=caller.principal_idx,
            )
    except EquivalenceNotFound as exc:
        raise HTTPException(status_code=404, detail=_MSG_EQUIVALENCE_NOT_FOUND) from exc
    except EquivalenceRefused as exc:
        raise equivalence_refused_http(exc) from exc
    except GroupLockBusy as exc:
        raise_transient_retry(str(exc))
    return _equivalence_response(row)
