"""Work-ticket lifecycle routes.

`POST /api/v1/work-ticket` — submit a new ticket. Validates the action
exists & is enabled, that the caller is in the action's audience, that
the scope_target.kind matches the action's target_kind, and that no
existing ticket for the same `(scope_target, action_id, action_version)`
is in a non-terminal state (disallow-without-delete). On success: INSERT
the row, fire `schedule_dispatch` to start the workflow in the background,
return 202 with the ticket id and state.

`POST /api/v1/work-ticket/{idx}/run` — operator override. State-aware:

| Current state | Behavior                                          |
|---------------|---------------------------------------------------|
| PENDING       | Start dispatch (recovery from a lost create-task) |
| QUEUED        | 409 — already dispatching                         |
| PROCESSING    | 409 — already running                             |
| COMPLETED     | 409 — terminal                                    |
| FAILED        | Reset → PENDING and dispatch (manual restart)     |

The atomic state transition guard inside `runner._atomic_transition`
prevents double-dispatch even if `/run` races with the implicit dispatch
fired by submission.

Auth: every route here requires the caller to be in the action's
`audience` (humans by `system_role`, or service accounts) AND to hold
all of `action.scopes`. Resource-level ACL beyond that is action-specific
and not enforced here.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request, status
from qiita_common.actions import Audience
from qiita_common.api_paths import (
    PATH_WORK_TICKET_PREFIX,
    PATH_WORK_TICKET_ROOT,
    PATH_WORK_TICKET_RUN,
)
from qiita_common.models import (
    ScopeTargetKind,
    WorkTicketCreateRequest,
    WorkTicketResponse,
    WorkTicketState,
)

from ..actions.context_validator import validate_context
from ..auth.principal import HumanUser, Principal, ServiceAccount, get_current_principal
from ..deps import get_db_pool
from ..dispatch import NON_TERMINAL_WORK_TICKET_STATES, schedule_dispatch

_log = logging.getLogger(__name__)

router = APIRouter(prefix=PATH_WORK_TICKET_PREFIX, tags=["work-ticket"])


# Names of the unique partial indexes that enforce
# disallow-without-delete atomically. Defined in migration
# 20260508000000_work_ticket_disallow_without_delete_indexes.sql; renaming
# either there must light up the catch site below at type-check time.
_DISALLOW_WITHOUT_DELETE_INDEXES = frozenset(
    {
        "work_ticket_one_in_flight_per_reference",
        "work_ticket_one_in_flight_per_study_prep",
    }
)


# =============================================================================
# Helpers
# =============================================================================


async def _fetch_action_for_submission(
    pool: asyncpg.Pool, action_id: str, action_version: str
) -> dict[str, Any] | None:
    """Read just the columns the submission gate needs. Returns None if
    the action does not exist or is disabled.

    `audience` is parsed via the `Audience` Pydantic model — JSONB drift
    (renamed/removed field) becomes a loud ValidationError at submission
    rather than a silent default in the audience check below."""
    row = await pool.fetchrow(
        "SELECT target_kind, scopes, audience, context_schema"
        " FROM qiita.action"
        " WHERE action_id = $1 AND version = $2 AND enabled = true",
        action_id,
        action_version,
    )
    if row is None:
        return None
    return {
        "target_kind": row["target_kind"],
        "scopes": list(row["scopes"]),
        # `audience` and `context_schema` are JSONB; asyncpg returns each
        # as a string by default.
        "audience": Audience.model_validate_json(row["audience"]),
        "context_schema": json.loads(row["context_schema"]),
    }


def _check_audience(principal: Principal, audience: Audience) -> None:
    """403 unless the caller is in the action's audience.

    `audience.service=True` permits service-account principals.
    `audience.human_roles` is the set of system_role values that may
    submit. An action with `service=False` and an empty `human_roles`
    is unsubmittable by anyone — by design (e.g. a workflow that only
    the runner itself invokes via an internal call path)."""
    if isinstance(principal, ServiceAccount):
        if not audience.service:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="this action is not in your audience (service accounts not permitted)",
            )
        return
    if isinstance(principal, HumanUser):
        if principal.system_role not in audience.human_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="this action is not in your audience (system_role not permitted)",
            )
        return
    # Anonymous and any future kinds.
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="authentication required",
    )


def _check_scopes(principal: Principal, required_scopes: list[str]) -> None:
    """403 unless the caller's token carries every required scope.

    AND-composition: every scope in `action.scopes` must be present.
    OIDC-resolved principals carry their role's full ceiling; PAT-resolved
    principals carry the token's own scope set. `Principal.scopes` reads
    from whichever source applies."""
    missing = [s for s in required_scopes if not principal.has_scope(s)]
    if missing:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "reason": "missing required action scopes",
                "missing_scopes": missing,
            },
        )


def _scope_target_columns(
    scope_target: dict[str, Any],
) -> tuple[int | None, int | None, int | None]:
    """Map a ScopeTarget union member to the (study_idx, prep_idx,
    reference_idx) tuple the work_ticket table expects."""
    kind = scope_target["kind"]
    if kind == ScopeTargetKind.REFERENCE.value:
        return (None, None, scope_target["reference_idx"])
    if kind == ScopeTargetKind.STUDY_PREP.value:
        return (scope_target["study_idx"], scope_target["prep_idx"], None)
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"unknown scope_target.kind={kind!r}",
    )


async def _check_disallow_without_delete(
    pool: asyncpg.Pool,
    action_id: str,
    action_version: str,
    scope_target: dict[str, Any],
) -> None:
    """409 if any existing ticket for this `(scope_target, action_id,
    action_version)` triple is in a non-terminal state. COMPLETED tickets
    are tolerated; resubmission is gated until DELETE lands.

    Best-effort fast path. The atomic gate is the unique partial indexes
    `work_ticket_one_in_flight_per_{reference,study_prep}`; we still
    SELECT first so the common (non-racing) case returns a 409 carrying
    the blocking ticket idx, which is more useful to clients than the
    bare unique-violation that fires when two submissions race past
    this check."""
    study_idx, prep_idx, reference_idx = _scope_target_columns(scope_target)
    if scope_target["kind"] == ScopeTargetKind.REFERENCE.value:
        existing = await pool.fetchval(
            "SELECT work_ticket_idx FROM qiita.work_ticket"
            " WHERE action_id = $1 AND action_version = $2"
            "   AND reference_idx = $3"
            "   AND state = ANY($4::qiita.work_ticket_state[])"
            " LIMIT 1",
            action_id,
            action_version,
            reference_idx,
            list(NON_TERMINAL_WORK_TICKET_STATES),
        )
    else:
        existing = await pool.fetchval(
            "SELECT work_ticket_idx FROM qiita.work_ticket"
            " WHERE action_id = $1 AND action_version = $2"
            "   AND study_idx = $3 AND prep_idx = $4"
            "   AND state = ANY($5::qiita.work_ticket_state[])"
            " LIMIT 1",
            action_id,
            action_version,
            study_idx,
            prep_idx,
            list(NON_TERMINAL_WORK_TICKET_STATES),
        )
    if existing is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "reason": "another ticket for this (scope_target, action) is in flight",
                "blocking_work_ticket_idx": existing,
            },
        )


def _require_compute_backend_client(request: Request) -> None:
    """Guard that 503s if the orchestrator dispatch path is not configured.
    Prevents creating tickets that can never run."""
    if request.app.state.compute_backend_client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="compute orchestrator not configured (COMPUTE_ORCHESTRATOR_URL unset)",
        )


# =============================================================================
# Routes
# =============================================================================


@router.post(
    PATH_WORK_TICKET_ROOT,
    response_model=WorkTicketResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def submit_work_ticket(
    body: WorkTicketCreateRequest,
    request: Request,
    pool: asyncpg.Pool = Depends(get_db_pool),
    principal: Principal = Depends(get_current_principal),
    _: None = Depends(_require_compute_backend_client),
) -> WorkTicketResponse:
    action = await _fetch_action_for_submission(pool, body.action_id, body.action_version)
    if action is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"action ({body.action_id!r}, {body.action_version!r}) not found or disabled",
        )

    _check_audience(principal, action["audience"])
    _check_scopes(principal, action["scopes"])

    scope_target = body.scope_target.model_dump(mode="json")
    if scope_target["kind"] != action["target_kind"]:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "reason": "scope_target.kind does not match action.target_kind",
                "scope_target_kind": scope_target["kind"],
                "action_target_kind": action["target_kind"],
            },
        )

    context_errors = validate_context(action["context_schema"], body.action_context)
    if context_errors:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "reason": "action_context does not match action.context_schema",
                "errors": context_errors,
            },
        )

    await _check_disallow_without_delete(pool, body.action_id, body.action_version, scope_target)

    study_idx, prep_idx, reference_idx = _scope_target_columns(scope_target)
    try:
        work_ticket_idx = await pool.fetchval(
            "INSERT INTO qiita.work_ticket ("
            "  action_id, action_version, originator_principal_idx,"
            "  scope_target_kind, study_idx, prep_idx, reference_idx,"
            "  action_context"
            ") VALUES ($1, $2, $3, $4::qiita.scope_target_kind, $5, $6, $7, $8::jsonb)"
            " RETURNING work_ticket_idx",
            body.action_id,
            body.action_version,
            principal.principal_idx,
            scope_target["kind"],
            study_idx,
            prep_idx,
            reference_idx,
            json.dumps(body.action_context),
        )
    except asyncpg.exceptions.UniqueViolationError as exc:
        # Unique partial index fired — a concurrent submission won the
        # race past `_check_disallow_without_delete`. Map to the same
        # 409 shape the SELECT-side check returns; the blocking idx is
        # not cheap to recover here (would require a second query) and
        # the client only needs to know to retry against the in-flight
        # ticket they will discover via /work-ticket listing.
        if exc.constraint_name in _DISALLOW_WITHOUT_DELETE_INDEXES:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "reason": "another ticket for this (scope_target, action) is in flight",
                },
            ) from exc
        raise

    # Fire-and-forget dispatch in the background. The route returns 202
    # immediately; the workflow runs in-process via asyncio.
    schedule_dispatch(request.app, work_ticket_idx)

    _log.info(
        "submitted work_ticket %d for action %s/%s by principal %d",
        work_ticket_idx,
        body.action_id,
        body.action_version,
        principal.principal_idx,
    )
    return WorkTicketResponse(work_ticket_idx=work_ticket_idx, state=WorkTicketState.PENDING)


@router.post(
    PATH_WORK_TICKET_RUN,
    response_model=WorkTicketResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def run_work_ticket(
    work_ticket_idx: int,
    request: Request,
    pool: asyncpg.Pool = Depends(get_db_pool),
    principal: Principal = Depends(get_current_principal),
    _: None = Depends(_require_compute_backend_client),
) -> WorkTicketResponse:
    """Operator override — restart a FAILED ticket or resume a PENDING
    one whose original create-time dispatch was lost. State-aware (see
    table at module top)."""
    row = await pool.fetchrow(
        "SELECT action_id, action_version, state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"work_ticket {work_ticket_idx} not found",
        )

    # Re-apply the action's audience+scopes gate. Without this, anyone
    # who guesses a ticket idx could redrive arbitrary work.
    action = await _fetch_action_for_submission(pool, row["action_id"], row["action_version"])
    if action is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"action {row['action_id']!r}/{row['action_version']!r} "
                "no longer enabled — cannot redrive"
            ),
        )
    _check_audience(principal, action["audience"])
    _check_scopes(principal, action["scopes"])

    current_state = row["state"]
    if current_state in (
        WorkTicketState.QUEUED.value,
        WorkTicketState.PROCESSING.value,
        WorkTicketState.COMPLETED.value,
    ):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "reason": f"work_ticket is in {current_state!r}; /run not applicable",
                "current_state": current_state,
            },
        )

    if current_state == WorkTicketState.FAILED.value:
        # Manual restart: FAILED → PENDING (atomic; refuse if state
        # changed under us between the SELECT and now).
        updated = await pool.fetchval(
            "UPDATE qiita.work_ticket"
            " SET state = $1::qiita.work_ticket_state"
            " WHERE work_ticket_idx = $2"
            "   AND state = $3::qiita.work_ticket_state"
            " RETURNING work_ticket_idx",
            WorkTicketState.PENDING.value,
            work_ticket_idx,
            WorkTicketState.FAILED.value,
        )
        if updated is None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="work_ticket state changed under /run; retry",
            )
        _log.info("manual restart of work_ticket %d (FAILED → PENDING)", work_ticket_idx)
    # PENDING: no state change needed, just dispatch.

    schedule_dispatch(request.app, work_ticket_idx)
    return WorkTicketResponse(work_ticket_idx=work_ticket_idx, state=WorkTicketState.PENDING)
