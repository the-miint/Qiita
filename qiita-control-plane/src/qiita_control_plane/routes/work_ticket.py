"""Work-ticket lifecycle routes.

`POST /api/v1/work-ticket` — submit a new ticket. Validates the action
exists & is enabled, that the caller is in the action's audience, that
the scope_target.kind matches the action's target_kind, and that no
existing ticket for the same `(scope_target, action_id, action_version)`
is in a non-terminal state (disallow-without-delete). On success: INSERT
the row, fire `schedule_dispatch` to start the workflow in the background,
return 202 with the ticket id and state.

`GET /api/v1/work-ticket/{idx}` — read a single ticket. Returns the full
WorkTicket model (state, action info, scope target, action context, retry
accounting, failure surface, timestamps) so a polling CLI can render
everything in one round trip. Auth: the originator passes; wet_lab_admin+
bypasses; everyone else gets 404 (not 403 — see the route docstring for
why non-owners cannot distinguish a missing ticket from one they lack
access to).

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
all of `action.scopes`. prep_sample-scoped submissions additionally
require the caller to have `Tier.ADMIN` on every non-retired study
linked to the prep_sample (wet_lab_admin+ bypass), see
`_check_prep_sample_study_access`. Reference- and study_prep-scoped
submissions carry no per-resource gate at this layer; the action's
own audience / scope choices are the policy.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request, status
from qiita_common.actions import Audience
from qiita_common.api_paths import (
    PATH_WORK_TICKET_BY_IDX,
    PATH_WORK_TICKET_PREFIX,
    PATH_WORK_TICKET_ROOT,
    PATH_WORK_TICKET_RUN,
)
from qiita_common.auth_constants import SystemRole
from qiita_common.models import (
    ScopeTargetKind,
    WorkTicket,
    WorkTicketCreateRequest,
    WorkTicketResponse,
    WorkTicketState,
)

from ..actions.context_validator import validate_context
from ..auth.guards import require_caller_has_admin_on_all_studies
from ..auth.principal import Anonymous, HumanUser, Principal, ServiceAccount, get_current_principal
from ..deps import get_db_pool
from ..dispatch import NON_TERMINAL_WORK_TICKET_STATES, schedule_dispatch
from ..repositories.prep_sample import fetch_active_study_idxs_for_prep_sample

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
        "work_ticket_one_in_flight_per_prep_sample",
    }
)


# =============================================================================
# Helpers
# =============================================================================


async def _fetch_action_for_submission(
    pool: asyncpg.Pool, action_id: str, action_version: str
) -> dict[str, Any] | None:
    """Read just the columns the submission gate needs. Returns None if
    the action does not exist; returns a dict including `enabled` if it
    does (so the caller can distinguish "not found" from "deprecated"
    and respond with the right HTTP status — 404 vs 410).

    `audience` is parsed via the `Audience` Pydantic model — JSONB drift
    (renamed/removed field) becomes a loud ValidationError at submission
    rather than a silent default in the audience check below."""
    row = await pool.fetchrow(
        "SELECT target_kind, target_processing_kinds,"
        "       scopes, audience, context_schema, enabled"
        " FROM qiita.action"
        " WHERE action_id = $1 AND version = $2",
        action_id,
        action_version,
    )
    if row is None:
        return None
    return {
        "target_kind": row["target_kind"],
        # asyncpg yields qiita.processing_kind[] as a list of strings;
        # the route compares them against prep_sample.processing_kind
        # (also a string) so no enum coercion is needed.
        "target_processing_kinds": list(row["target_processing_kinds"]),
        "scopes": list(row["scopes"]),
        # `audience` and `context_schema` are JSONB; asyncpg returns each
        # as a string by default.
        "audience": Audience.model_validate_json(row["audience"]),
        "context_schema": json.loads(row["context_schema"]),
        "enabled": row["enabled"],
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
) -> tuple[int | None, int | None, int | None, int | None]:
    """Map a ScopeTarget union member to the (study_idx, prep_idx,
    reference_idx, prep_sample_idx) tuple the work_ticket table
    expects."""
    kind = scope_target["kind"]
    if kind == ScopeTargetKind.REFERENCE.value:
        return (None, None, scope_target["reference_idx"], None)
    if kind == ScopeTargetKind.STUDY_PREP.value:
        return (scope_target["study_idx"], scope_target["prep_idx"], None, None)
    if kind == ScopeTargetKind.PREP_SAMPLE.value:
        return (None, None, None, scope_target["prep_sample_idx"])
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
    `work_ticket_one_in_flight_per_{reference,study_prep,prep_sample}`;
    we still SELECT first so the common (non-racing) case returns a 409
    carrying the blocking ticket idx, which is more useful to clients
    than the bare unique-violation that fires when two submissions race
    past this check."""
    study_idx, prep_idx, reference_idx, prep_sample_idx = _scope_target_columns(scope_target)
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
    elif scope_target["kind"] == ScopeTargetKind.PREP_SAMPLE.value:
        existing = await pool.fetchval(
            "SELECT work_ticket_idx FROM qiita.work_ticket"
            " WHERE action_id = $1 AND action_version = $2"
            "   AND prep_sample_idx = $3"
            "   AND state = ANY($4::qiita.work_ticket_state[])"
            " LIMIT 1",
            action_id,
            action_version,
            prep_sample_idx,
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


async def _check_prep_sample_study_access(
    pool: asyncpg.Pool, *, prep_sample_idx: int, caller: Principal
) -> None:
    """Require Tier.ADMIN (or owner / wet_lab_admin+) on every non-retired
    study link of this prep_sample. Bypass-role callers skip the DB read.
    An orphaned prep_sample (all links retired) passes — downstream
    lookups fail elsewhere.

    No service-account special-case: a service account holds no
    study_access rows, so `require_caller_has_admin_on_all_studies`
    rejects it on the first linked study with a 403. For every
    prep_sample-scoped action today the action's audience already
    excludes service accounts, so this gate never sees one — but the
    natural rejection means an action that later opens itself to
    service accounts does not silently bypass the per-study check.

    Policy is "caller had access at submit time"; this read runs on the
    pool, not the work_ticket INSERT transaction, so a study_access row
    racing with the gate decides arbitrarily. Future per-resource gates
    on other scope kinds are expected to follow the same shape.
    """
    if caller.has_role_at_least(SystemRole.WET_LAB_ADMIN):
        return
    study_idxs = await fetch_active_study_idxs_for_prep_sample(pool, prep_sample_idx)
    if not study_idxs:
        return
    await require_caller_has_admin_on_all_studies(
        pool,
        caller=caller,
        study_idxs=study_idxs,
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
            detail=f"action ({body.action_id!r}, {body.action_version!r}) not found",
        )
    if not action["enabled"]:
        # 410 Gone — the action_id/version pair exists in the catalog
        # but has been deprecated (sync replaced it with a newer version,
        # or an operator disabled it). Distinct from 404 so clients that
        # auto-retry on 404 don't keep trying a permanently-gone version.
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail=f"action ({body.action_id!r}, {body.action_version!r}) is deprecated",
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

    # Kind-specific gate for prep_sample-scoped actions: when the action
    # declares a nonempty target_processing_kinds list, the prep_sample's
    # actual processing_kind must be in it. Empty list = "any kind"
    # (cross-kind admin actions). For other scope kinds the list must
    # be empty per the DB CHECK; nothing to do here.
    if (
        scope_target["kind"] == ScopeTargetKind.PREP_SAMPLE.value
        and action["target_processing_kinds"]
    ):
        actual_kind = await pool.fetchval(
            "SELECT processing_kind FROM qiita.prep_sample WHERE idx = $1",
            scope_target["prep_sample_idx"],
        )
        if actual_kind is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(f"prep_sample {scope_target['prep_sample_idx']!r} not found"),
            )
        if actual_kind not in action["target_processing_kinds"]:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail={
                    "reason": (
                        "prep_sample.processing_kind does not match action.target_processing_kinds"
                    ),
                    "prep_sample_processing_kind": actual_kind,
                    "action_target_processing_kinds": action["target_processing_kinds"],
                },
            )

    if scope_target["kind"] == ScopeTargetKind.PREP_SAMPLE.value:
        await _check_prep_sample_study_access(
            pool,
            prep_sample_idx=scope_target["prep_sample_idx"],
            caller=principal,
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

    study_idx, prep_idx, reference_idx, prep_sample_idx = _scope_target_columns(scope_target)
    try:
        work_ticket_idx = await pool.fetchval(
            "INSERT INTO qiita.work_ticket ("
            "  action_id, action_version, originator_principal_idx,"
            "  scope_target_kind, study_idx, prep_idx, reference_idx,"
            "  prep_sample_idx, action_context"
            ") VALUES ($1, $2, $3, $4::qiita.scope_target_kind,"
            "          $5, $6, $7, $8, $9::jsonb)"
            " RETURNING work_ticket_idx",
            body.action_id,
            body.action_version,
            principal.principal_idx,
            scope_target["kind"],
            study_idx,
            prep_idx,
            reference_idx,
            prep_sample_idx,
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


_WORK_TICKET_COLUMNS = (
    "work_ticket_idx, action_id, action_version, originator_principal_idx,"
    " scope_target_kind, study_idx, prep_idx, reference_idx, prep_sample_idx,"
    " action_context, state, retry_count, max_retries,"
    " failure_type, failure_stage, failure_step_name, failure_reason,"
    " created_at, updated_at"
)


def _row_to_work_ticket(row: asyncpg.Record) -> WorkTicket:
    """Reconstruct the discriminated `scope_target` from the four nullable
    target columns the work_ticket table stores and assemble a WorkTicket.
    `action_context` is JSONB-as-string from asyncpg and is decoded here."""
    kind = row["scope_target_kind"]
    if kind == ScopeTargetKind.REFERENCE.value:
        scope_target: dict[str, Any] = {"kind": kind, "reference_idx": row["reference_idx"]}
    elif kind == ScopeTargetKind.STUDY_PREP.value:
        scope_target = {
            "kind": kind,
            "study_idx": row["study_idx"],
            "prep_idx": row["prep_idx"],
        }
    else:  # PREP_SAMPLE — DB CHECK enforces one of the three valid kinds.
        scope_target = {"kind": kind, "prep_sample_idx": row["prep_sample_idx"]}
    return WorkTicket.model_validate(
        {
            "work_ticket_idx": row["work_ticket_idx"],
            "action_id": row["action_id"],
            "action_version": row["action_version"],
            "originator_principal_idx": row["originator_principal_idx"],
            "scope_target": scope_target,
            "action_context": json.loads(row["action_context"]),
            "state": row["state"],
            "retry_count": row["retry_count"],
            "max_retries": row["max_retries"],
            "failure_type": row["failure_type"],
            "failure_stage": row["failure_stage"],
            "failure_step_name": row["failure_step_name"],
            "failure_reason": row["failure_reason"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
    )


@router.get(
    PATH_WORK_TICKET_BY_IDX,
    response_model=WorkTicket,
)
async def get_work_ticket(
    work_ticket_idx: int,
    pool: asyncpg.Pool = Depends(get_db_pool),
    principal: Principal = Depends(get_current_principal),
) -> WorkTicket:
    """Read a single ticket. Returns the full WorkTicket record so the
    caller-side CLI can render state, retry accounting, and the failure
    surface from one call. Auth: 401 on Anonymous; the originator passes;
    wet_lab_admin+ bypasses. No per-study / per-resource access path here
    — the originator-bypass already lets the caller who submitted the
    ticket read it, and operator views are served by the role bypass.

    A caller who is neither the originator nor a bypass-role gets 404,
    not 403 — the same response a genuinely missing idx returns — so a
    caller cannot probe work_ticket_idx values to learn which tickets
    exist. Mirrors the enumeration-safe 404 the auth-token routes use
    (see docs/auth.md)."""
    if isinstance(principal, Anonymous):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="authentication required"
        )
    not_found = HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"work_ticket {work_ticket_idx} not found",
    )
    row = await pool.fetchrow(
        f"SELECT {_WORK_TICKET_COLUMNS} FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    if row is None:
        raise not_found
    is_originator = row["originator_principal_idx"] == principal.principal_idx
    is_bypass = principal.has_role_at_least(SystemRole.WET_LAB_ADMIN)
    if not (is_originator or is_bypass):
        raise not_found
    return _row_to_work_ticket(row)


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
    # who guesses a ticket idx could redrive arbitrary work. Treat
    # "row missing" and "row exists but disabled" the same for redrive:
    # both mean the action is unreachable from this ticket. Submission
    # cares about the distinction (404 vs 410); redrive doesn't.
    action = await _fetch_action_for_submission(pool, row["action_id"], row["action_version"])
    if action is None or not action["enabled"]:
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
        # Manual restart: FAILED → PENDING. Per arch.md spec, resets
        # retry_count to 0 (operator override of the auto-retry budget)
        # and clears the failure_* columns so the
        # work_ticket_failure_consistent DB CHECK is honoured (failure_*
        # all NULL when state != failed). Atomic; refuses if state
        # changed under us between the SELECT and now.
        updated = await pool.fetchval(
            "UPDATE qiita.work_ticket"
            " SET state = $1::qiita.work_ticket_state,"
            "     retry_count = 0,"
            "     failure_type = NULL,"
            "     failure_stage = NULL,"
            "     failure_step_name = NULL,"
            "     failure_reason = NULL"
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
        _log.info(
            "manual restart of work_ticket %d (FAILED → PENDING; retry_count reset)",
            work_ticket_idx,
        )
    # PENDING: no state change needed, just dispatch.

    schedule_dispatch(request.app, work_ticket_idx)
    return WorkTicketResponse(work_ticket_idx=work_ticket_idx, state=WorkTicketState.PENDING)
