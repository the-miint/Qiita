"""Per-study access routes: list, grant, change tier, revoke (qiita.study_access).

Who may do what is `auth.study_access_policy`; this module applies it. Every
mutation records an `auth_event` in the same transaction, because a revoke
hard-deletes the row and would otherwise leave no trace of who removed whom.
"""

from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from pydantic import Field
from qiita_common.api_paths import (
    PATH_STUDY_ACCESS,
    PATH_STUDY_ACCESS_BY_PRINCIPAL,
    PATH_STUDY_PREFIX,
)
from qiita_common.auth_constants import AuthEventType, Scope
from qiita_common.models import (
    StudyAccessGrant,
    StudyAccessResponse,
    StudyAccessTierUpdate,
    Tier,
)

from ..auth import study_access_policy as policy
from ..auth.audit import record_event
from ..auth.guards import require_scope
from ..auth.principal import Principal
from ..deps import TxConnFactory, get_db_pool, get_tx_conn_factory
from ..repositories.study_access import (
    delete_study_access,
    fetch_caller_study_access,
    fetch_grantee_by_email,
    fetch_study_access_row,
    insert_study_access,
    list_study_access,
    update_study_access_tier,
)

router = APIRouter(prefix=PATH_STUDY_PREFIX, tags=["study"])

_MSG_NO_ACCOUNT = (
    "no Qiita account uses that email; the person must log in to Qiita once"
    " before they can be granted access"
)
_MSG_ACCOUNT_INACTIVE = "that account is disabled or retired"
_MSG_CANNOT_LIST = "listing study access requires member access or higher on the study"
_MSG_CANNOT_MANAGE = "managing study access requires member access or higher on the study"


def _response(row: asyncpg.Record) -> StudyAccessResponse:
    return StudyAccessResponse.model_validate(dict(row))


async def _standing(
    conn: asyncpg.Connection | asyncpg.Pool, *, caller: Principal, study_idx: int
) -> policy.Standing:
    """The caller's standing on the study; 404 when the study does not exist."""
    row = await fetch_caller_study_access(
        conn, principal_idx=caller.principal_idx, study_idx=study_idx
    )
    if row is None:
        raise HTTPException(status_code=404, detail=f"study {study_idx} not found")
    return policy.standing_of(caller, row)


def _require_can_manage(standing: policy.Standing) -> None:
    """403 before any row lookup for a caller who can change nothing, so a
    viewer cannot learn from 404-vs-403 which principals hold a row."""
    if not policy.can_revoke(standing, Tier.VIEWER):
        raise HTTPException(status_code=403, detail=_MSG_CANNOT_MANAGE)


async def _locked_row(conn: asyncpg.Connection, *, study_idx: int, principal_idx: int):
    row = await fetch_study_access_row(
        conn, study_idx=study_idx, principal_idx=principal_idx, for_update=True
    )
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"principal {principal_idx} has no access row on study {study_idx}",
        )
    return row


@router.get(PATH_STUDY_ACCESS)
async def list_study_access_route(
    study_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    caller: Principal = Depends(require_scope(Scope.STUDY_READ)),
) -> list[StudyAccessResponse]:
    """Every access row on the study, admin first. Requires member or higher
    (or wet_lab_admin+)."""
    standing = await _standing(pool, caller=caller, study_idx=study_idx)
    if not policy.can_list(standing):
        raise HTTPException(status_code=403, detail=_MSG_CANNOT_LIST)
    return [_response(r) for r in await list_study_access(pool, study_idx=study_idx)]


@router.post(PATH_STUDY_ACCESS, status_code=201)
async def grant_study_access(
    study_idx: Annotated[int, Field(gt=0)],
    body: StudyAccessGrant,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    caller: Principal = Depends(require_scope(Scope.STUDY_WRITE)),
) -> StudyAccessResponse:
    """Grant `body.access_tier` to the user whose email is `body.email`.

    403 when the caller may not grant that tier; 422 when no account uses the
    email or it is disabled or retired; 409 when the grantee already has a
    row (change it with PATCH).
    """
    async with tx() as conn:
        standing = await _standing(conn, caller=caller, study_idx=study_idx)
        if not policy.can_grant(standing, body.access_tier):
            raise HTTPException(
                status_code=403,
                detail=f"your access on this study cannot grant {str(body.access_tier)!r}",
            )
        grantee = await fetch_grantee_by_email(conn, email=body.email)
        if grantee is None:
            raise HTTPException(status_code=422, detail=_MSG_NO_ACCOUNT)
        if grantee.disabled or grantee.retired:
            raise HTTPException(status_code=422, detail=_MSG_ACCOUNT_INACTIVE)
        try:
            # A savepoint, so the 409 lookup below runs in a live transaction.
            async with conn.transaction():
                row = await insert_study_access(
                    conn,
                    study_idx=study_idx,
                    principal_idx=grantee.principal_idx,
                    access_tier=body.access_tier,
                    granted_by_idx=caller.principal_idx,
                )
        except asyncpg.UniqueViolationError:
            existing = await fetch_study_access_row(
                conn, study_idx=study_idx, principal_idx=grantee.principal_idx
            )
            current = existing["access_tier"] if existing is not None else "unknown"
            raise HTTPException(
                status_code=409,
                detail=(
                    f"that account already has {current!r} access on study {study_idx};"
                    " change the tier instead"
                ),
            )
        await record_event(
            conn,
            event_type=AuthEventType.STUDY_ACCESS_GRANT,
            principal_idx=grantee.principal_idx,
            actor_principal_idx=caller.principal_idx,
            detail={"study_idx": study_idx, "access_tier": str(body.access_tier)},
        )
    return _response(row)


@router.patch(PATH_STUDY_ACCESS_BY_PRINCIPAL)
async def change_study_access_tier(
    study_idx: Annotated[int, Field(gt=0)],
    principal_idx: Annotated[int, Field(gt=0)],
    body: StudyAccessTierUpdate,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    caller: Principal = Depends(require_scope(Scope.STUDY_WRITE)),
) -> StudyAccessResponse:
    """Change one grantee's tier. Allowed iff the caller may revoke the row's
    current tier and grant the new one. Setting the tier it already has
    returns the row unchanged and records nothing."""
    async with tx() as conn:
        standing = await _standing(conn, caller=caller, study_idx=study_idx)
        _require_can_manage(standing)
        row = await _locked_row(conn, study_idx=study_idx, principal_idx=principal_idx)
        current = Tier(row["access_tier"])
        if current == body.access_tier:
            return _response(row)
        if not policy.can_change_tier(standing, current=current, new=body.access_tier):
            raise HTTPException(
                status_code=403,
                detail=(
                    f"your access on this study cannot change a {str(current)!r} row"
                    f" to {str(body.access_tier)!r}"
                ),
            )
        await update_study_access_tier(
            conn, study_idx=study_idx, principal_idx=principal_idx, access_tier=body.access_tier
        )
        await record_event(
            conn,
            event_type=AuthEventType.STUDY_ACCESS_TIER_CHANGE,
            principal_idx=principal_idx,
            actor_principal_idx=caller.principal_idx,
            detail={"study_idx": study_idx, "from": str(current), "to": str(body.access_tier)},
        )
        updated = await fetch_study_access_row(
            conn, study_idx=study_idx, principal_idx=principal_idx
        )
    if updated is None:
        raise RuntimeError(f"study_access row ({study_idx}, {principal_idx}) vanished under lock")
    return _response(updated)


@router.delete(PATH_STUDY_ACCESS_BY_PRINCIPAL)
async def revoke_study_access(
    study_idx: Annotated[int, Field(gt=0)],
    principal_idx: Annotated[int, Field(gt=0)],
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    caller: Principal = Depends(require_scope(Scope.STUDY_WRITE)),
) -> StudyAccessResponse:
    """Delete one grantee's row and return it as it was. Revoking the owner's row leaves the owner's
    access in place (the owner bypass in `require_study_access`)."""
    async with tx() as conn:
        standing = await _standing(conn, caller=caller, study_idx=study_idx)
        _require_can_manage(standing)
        row = await _locked_row(conn, study_idx=study_idx, principal_idx=principal_idx)
        current = Tier(row["access_tier"])
        if not policy.can_revoke(standing, current):
            raise HTTPException(
                status_code=403,
                detail=f"your access on this study cannot revoke a {str(current)!r} row",
            )
        await delete_study_access(conn, study_idx=study_idx, principal_idx=principal_idx)
        await record_event(
            conn,
            event_type=AuthEventType.STUDY_ACCESS_REVOKE,
            principal_idx=principal_idx,
            actor_principal_idx=caller.principal_idx,
            detail={"study_idx": study_idx, "access_tier": str(current)},
        )
    return _response(row)
