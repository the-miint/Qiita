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
from pathlib import PurePosixPath
from typing import Any

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from qiita_common.actions import FASTQ_PATH_CONTEXT_KEYS, Audience
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
    WorkTicketSummary,
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
        "work_ticket_one_in_flight_per_sequenced_pool",
    }
)

# Page-size bounds for GET /work-ticket, mirroring the audit-log query's
# ge=1 / le=max shape (routes/admin.py). Local to this route — its only
# consumer; the CLI just passes `--limit N` and the server enforces here.
_WORK_TICKET_LIST_DEFAULT_LIMIT = 50
_WORK_TICKET_LIST_MAX_LIMIT = 500


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
) -> tuple[int | None, int | None, int | None, int | None, int | None]:
    """Map a ScopeTarget union member to the
    (study_idx, prep_idx, reference_idx, prep_sample_idx, sequenced_pool_idx)
    tuple the work_ticket table expects.

    The SEQUENCED_POOL arm carries both sequenced_pool_idx and
    sequencing_run_idx; the run idx is consumed by the orchestrator
    framework (SCOPE_SCALARS_BY_KIND) but is not a work_ticket column —
    it's derivable from the pool row's FK back to qiita.sequencing_run.
    """
    kind = scope_target["kind"]
    if kind == ScopeTargetKind.REFERENCE.value:
        return (None, None, scope_target["reference_idx"], None, None)
    if kind == ScopeTargetKind.STUDY_PREP.value:
        return (scope_target["study_idx"], scope_target["prep_idx"], None, None, None)
    if kind == ScopeTargetKind.PREP_SAMPLE.value:
        return (None, None, None, scope_target["prep_sample_idx"], None)
    if kind == ScopeTargetKind.SEQUENCED_POOL.value:
        return (None, None, None, None, scope_target["sequenced_pool_idx"])
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
    study_idx, prep_idx, reference_idx, prep_sample_idx, sequenced_pool_idx = _scope_target_columns(
        scope_target
    )
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
    elif scope_target["kind"] == ScopeTargetKind.SEQUENCED_POOL.value:
        existing = await pool.fetchval(
            "SELECT work_ticket_idx FROM qiita.work_ticket"
            " WHERE action_id = $1 AND action_version = $2"
            "   AND sequenced_pool_idx = $3"
            "   AND state = ANY($4::qiita.work_ticket_state[])"
            " LIMIT 1",
            action_id,
            action_version,
            sequenced_pool_idx,
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


def _basename_carries_prefix(basename: str, prefix: str) -> bool:
    """True iff `basename` is `prefix` immediately followed by a `_` or
    `.` separator — segment-anchored, not a bare substring match.

    A plain `str.startswith` would admit `ITEM-12_R1.fastq` for a
    sequenced_pool_item_id of `ITEM-1`; requiring the separator pins the
    match to the documented `<pool_item_id>_R1.fastq` (paired-end) and
    `<pool_item_id>.fastq` (single-end) filename convention exactly. A
    basename equal to `prefix` with nothing after it is rejected — a
    fastq file always carries an extension.
    """
    if not basename.startswith(prefix):
        return False
    return basename[len(prefix) : len(prefix) + 1] in ("_", ".")


async def _check_fastq_filename_prefix(
    pool: asyncpg.Pool, *, prep_sample_idx: int, action_context: dict[str, Any]
) -> None:
    """422 when a fastq path in `action_context` has a basename that is
    not the prep_sample's `sequenced_pool_item_id` followed by a `_` or
    `.` separator (see `_basename_carries_prefix`).

    The rule: the `--pool-item-id` chosen at `sequenced-sample create`
    time is the filename prefix of every fastq the work-ticket
    processes, so the R1/R2 pair and the sequenced_sample row stay
    mechanically tied. `action_context` and the sequenced_sample row are
    minted in two separate calls and nothing else couples them, so the
    check lives here. Keyed on the context keys (FASTQ_PATH_CONTEXT_KEYS),
    not the action_id, so the route stays generic over actions.

    Skipped when the resolved `sequenced_pool_item_id` is NULL. Two
    shapes reach that branch:

    - a sequenced_sample row exists but is pool-less — sequenced_pool_idx
      and sequenced_pool_item_id are both NULL — which is legitimate; the
      rule then has nothing to anchor against.
    - no sequenced_sample subtype row exists for this
      processing_kind='sequenced' prep_sample — anomalous, since the
      sequenced-sample create composer writes supertype and subtype
      atomically. Policing that integrity gap is not this gate's job
      (the processing_kind check above and the create composer own it);
      the gate just declines to invent a constraint it cannot evaluate.

    Either way the rule constrains a relationship between two values and
    is vacuous when one does not exist, so the submission proceeds.

    Like `_check_prep_sample_study_access`, the read runs on the pool,
    not the work_ticket INSERT transaction: a sequenced_sample row
    created concurrently with the gate decides arbitrarily.
    `sequenced_pool_item_id` is not PATCH-editable, so a value already
    set cannot change under the gate.
    """
    fastq_paths = {
        key: action_context[key]
        for key in FASTQ_PATH_CONTEXT_KEYS
        # Defense-in-depth: for fastq-to-parquet the context_schema pins
        # each fastq key to a string, so validate_context already 422'd a
        # non-string upstream. The guard still covers actions with a
        # permissive schema — a non-string value is not a fastq path, so
        # it is skipped here rather than rejected.
        if isinstance(action_context.get(key), str)
    }
    if not fastq_paths:
        return
    pool_item_id = await pool.fetchval(
        "SELECT sequenced_pool_item_id FROM qiita.sequenced_sample WHERE prep_sample_idx = $1",
        prep_sample_idx,
    )
    if pool_item_id is None:
        return
    mismatched = [
        {
            "context_key": key,
            "fastq_path": path,
            "basename": PurePosixPath(path).name,
        }
        for key, path in sorted(fastq_paths.items())
        if not _basename_carries_prefix(PurePosixPath(path).name, pool_item_id)
    ]
    if mismatched:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "reason": (
                    "fastq filename must be the prep_sample's sequenced_pool_item_id"
                    " followed by '_' or '.'"
                ),
                "sequenced_pool_item_id": pool_item_id,
                "mismatched": mismatched,
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

    # A fastq path in the (now schema-valid) action_context must carry a
    # basename prefixed by the prep_sample's sequenced_pool_item_id.
    if scope_target["kind"] == ScopeTargetKind.PREP_SAMPLE.value:
        await _check_fastq_filename_prefix(
            pool,
            prep_sample_idx=scope_target["prep_sample_idx"],
            action_context=body.action_context,
        )

    await _check_disallow_without_delete(pool, body.action_id, body.action_version, scope_target)

    study_idx, prep_idx, reference_idx, prep_sample_idx, sequenced_pool_idx = _scope_target_columns(
        scope_target
    )
    try:
        work_ticket_idx = await pool.fetchval(
            "INSERT INTO qiita.work_ticket ("
            "  action_id, action_version, originator_principal_idx,"
            "  scope_target_kind, study_idx, prep_idx, reference_idx,"
            "  prep_sample_idx, sequenced_pool_idx, action_context"
            ") VALUES ($1, $2, $3, $4::qiita.scope_target_kind,"
            "          $5, $6, $7, $8, $9, $10::jsonb)"
            " RETURNING work_ticket_idx",
            body.action_id,
            body.action_version,
            principal.principal_idx,
            scope_target["kind"],
            study_idx,
            prep_idx,
            reference_idx,
            prep_sample_idx,
            sequenced_pool_idx,
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


# Two-row-source SELECT. work_ticket carries the scope_target_kind plus
# the four nullable scope_target idx columns; for the SEQUENCED_POOL arm
# we additionally need the parent sequencing_run_idx to round-trip the
# scope_target shape Pydantic expects. The LEFT JOIN against
# qiita.sequenced_pool is a no-op for the other three kinds (the
# work_ticket's sequenced_pool_idx is NULL, the join produces NULL).
_WORK_TICKET_COLUMNS = (
    "wt.work_ticket_idx, wt.action_id, wt.action_version, wt.originator_principal_idx,"
    " wt.scope_target_kind, wt.study_idx, wt.prep_idx, wt.reference_idx,"
    " wt.prep_sample_idx, wt.sequenced_pool_idx,"
    " sp.sequencing_run_idx,"
    " wt.action_context, wt.state, wt.retry_count, wt.max_retries,"
    " wt.failure_type, wt.failure_stage, wt.failure_step_name, wt.failure_reason,"
    " wt.created_at, wt.updated_at"
)
_WORK_TICKET_FROM = (
    " FROM qiita.work_ticket wt LEFT JOIN qiita.sequenced_pool sp ON sp.idx = wt.sequenced_pool_idx"
)

# Summary read (GET /work-ticket): the work_ticket columns plus the ticket's
# *current* step entry — the highest (step_index, attempt) progress row —
# pulled in via a LATERAL join so the list is one query with no N+1. LEFT
# JOIN LATERAL so a ticket with no progress rows yet (PENDING before its
# first write-ahead) still returns, with the cur.* columns NULL. The compute
# columns are aliased `current_*` to stay distinct from the work_ticket
# columns; `_row_to_work_ticket_summary` re-keys them onto the model fields.
_WORK_TICKET_SUMMARY_FROM = (
    _WORK_TICKET_FROM + " LEFT JOIN LATERAL ("
    "   SELECT step_index, step_name, compute_target, slurm_job_id, state"
    "   FROM qiita.work_ticket_step wts"
    "   WHERE wts.work_ticket_idx = wt.work_ticket_idx"
    "   ORDER BY step_index DESC, attempt DESC"
    "   LIMIT 1"
    " ) cur ON true"
)
_WORK_TICKET_SUMMARY_COLUMNS = (
    _WORK_TICKET_COLUMNS + ","
    " cur.step_index AS current_step_index,"
    " cur.step_name AS current_step_name,"
    " cur.compute_target AS current_compute_target,"
    " cur.slurm_job_id AS current_slurm_job_id,"
    " cur.state AS current_step_state"
)


def _scope_target_from_columns(
    kind: str,
    *,
    study_idx: int | None,
    prep_idx: int | None,
    reference_idx: int | None,
    prep_sample_idx: int | None,
    sequenced_pool_idx: int | None,
    sequencing_run_idx: int | None,
) -> dict[str, Any]:
    """Rebuild the discriminated scope_target dict from the tagged-union
    columns work_ticket stores (scope_target_kind + the five nullable idx
    columns; the SEQUENCED_POOL arm additionally needs the joined
    sequencing_run_idx). Shared by the single-ticket and list-summary row
    shapers so the union mapping lives in exactly one place."""
    if kind == ScopeTargetKind.REFERENCE.value:
        return {"kind": kind, "reference_idx": reference_idx}
    if kind == ScopeTargetKind.STUDY_PREP.value:
        return {"kind": kind, "study_idx": study_idx, "prep_idx": prep_idx}
    if kind == ScopeTargetKind.PREP_SAMPLE.value:
        return {"kind": kind, "prep_sample_idx": prep_sample_idx}
    # SEQUENCED_POOL — DB CHECK enforces one of the four valid kinds.
    return {
        "kind": kind,
        "sequenced_pool_idx": sequenced_pool_idx,
        "sequencing_run_idx": sequencing_run_idx,
    }


def _shape_work_ticket_columns(data: dict[str, Any]) -> dict[str, Any]:
    """In place, fold a work_ticket row's tagged-union scope columns into a
    single `scope_target` and decode `action_context` from asyncpg's
    JSONB-as-string. Every other column maps 1:1 onto a WorkTicket field and
    flows through unchanged — so a field added to both the model and
    `_WORK_TICKET_COLUMNS` needs no edit here. Returns the same dict for
    chaining."""
    data["scope_target"] = _scope_target_from_columns(
        data.pop("scope_target_kind"),
        study_idx=data.pop("study_idx"),
        prep_idx=data.pop("prep_idx"),
        reference_idx=data.pop("reference_idx"),
        prep_sample_idx=data.pop("prep_sample_idx"),
        sequenced_pool_idx=data.pop("sequenced_pool_idx"),
        # Joined sequencing-run idx — only the SEQUENCED_POOL arm carries a
        # value (NULL otherwise) but the column is always selected, so pop
        # defensively with a default.
        sequencing_run_idx=data.pop("sequencing_run_idx", None),
    )
    data["action_context"] = json.loads(data["action_context"])
    return data


def _row_to_work_ticket(row: asyncpg.Record) -> WorkTicket:
    """Assemble a WorkTicket from a work_ticket row (scope_target rebuilt
    from the tagged-union columns, action_context JSON-decoded). The
    SEQUENCED_POOL arm reads sequencing_run_idx from the joined
    sequenced_pool row — selected upstream so the assembly stays a pure
    transformation."""
    return WorkTicket.model_validate(_shape_work_ticket_columns(dict(row)))


def _row_to_work_ticket_summary(row: asyncpg.Record) -> WorkTicketSummary:
    """Assemble a WorkTicketSummary from a summary-query row: the shaped
    work_ticket columns plus the five `current_*` columns the LATERAL join
    over the highest-(step_index, attempt) progress row adds. Re-key the
    aliased compute columns onto the model's field names; they are all NULL
    when the ticket has no progress rows yet (the LEFT JOIN found no match,
    so Pydantic's `| None` defaults apply)."""
    data = dict(row)
    # Pop the LATERAL-join aliases before shaping the work_ticket columns so
    # `_shape_work_ticket_columns` sees only the columns it knows about.
    summary_fields = {
        "current_step_index": data.pop("current_step_index"),
        "current_step_name": data.pop("current_step_name"),
        "compute_target": data.pop("current_compute_target"),
        "slurm_job_id": data.pop("current_slurm_job_id"),
        "step_state": data.pop("current_step_state"),
    }
    shaped = _shape_work_ticket_columns(data)
    return WorkTicketSummary.model_validate({**shaped, **summary_fields})


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
        f"SELECT {_WORK_TICKET_COLUMNS}{_WORK_TICKET_FROM} WHERE wt.work_ticket_idx = $1",
        work_ticket_idx,
    )
    if row is None:
        raise not_found
    is_originator = row["originator_principal_idx"] == principal.principal_idx
    is_bypass = principal.has_role_at_least(SystemRole.WET_LAB_ADMIN)
    if not (is_originator or is_bypass):
        raise not_found
    return _row_to_work_ticket(row)


@router.get(
    PATH_WORK_TICKET_ROOT,
    response_model=list[WorkTicketSummary],
)
async def list_work_tickets(
    pool: asyncpg.Pool = Depends(get_db_pool),
    principal: Principal = Depends(get_current_principal),
    state: WorkTicketState | None = Query(
        default=None, description="Filter to a single lifecycle state."
    ),
    active: bool = Query(
        default=False,
        description="Filter to non-terminal tickets (pending / queued / processing).",
    ),
    all_tickets: bool = Query(
        default=False,
        alias="all",
        description=(
            "Operator view: return tickets from every originator (requires "
            "wet_lab_admin or higher). Default is the caller's own tickets."
        ),
    ),
    limit: int = Query(
        default=_WORK_TICKET_LIST_DEFAULT_LIMIT, ge=1, le=_WORK_TICKET_LIST_MAX_LIMIT
    ),
) -> list[WorkTicketSummary]:
    """List work tickets, each with a snapshot of its *current* compute
    placement (target, SLURM job id, step state) from a single LATERAL join
    against work_ticket_step — no live SLURM hop, so the read is at most one
    poll-interval stale (see `WorkTicketSummary`).

    Scope: by default a caller sees only tickets they originated. `?all=true`
    widens to every originator and is gated to wet_lab_admin+ (mirrors the
    single-ticket GET's role bypass); a non-admin requesting it gets 403.
    Anonymous → 401. Ordered newest-first (work_ticket_idx DESC), capped by
    `limit`.

    `state` and `active` AND-compose (`?state=completed&active=true` is a
    valid — empty — intersection), so a caller can scope to "my active
    tickets" or "all failed tickets" in one query."""
    if isinstance(principal, Anonymous):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="authentication required"
        )
    if all_tickets and not principal.has_role_at_least(SystemRole.WET_LAB_ADMIN):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="listing all tickets (?all=true) requires wet_lab_admin or higher",
        )

    # Build the WHERE incrementally so each filter binds its own $n; the
    # placeholder index is always len(args) right after the append.
    conditions: list[str] = []
    args: list[Any] = []
    if not all_tickets:
        args.append(principal.principal_idx)
        conditions.append(f"wt.originator_principal_idx = ${len(args)}")
    if state is not None:
        args.append(state.value)
        conditions.append(f"wt.state = ${len(args)}::qiita.work_ticket_state")
    if active:
        args.append(list(NON_TERMINAL_WORK_TICKET_STATES))
        conditions.append(f"wt.state = ANY(${len(args)}::qiita.work_ticket_state[])")
    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    args.append(limit)
    rows = await pool.fetch(
        f"SELECT {_WORK_TICKET_SUMMARY_COLUMNS}{_WORK_TICKET_SUMMARY_FROM}{where}"
        f" ORDER BY wt.work_ticket_idx DESC LIMIT ${len(args)}",
        *args,
    )
    return [_row_to_work_ticket_summary(row) for row in rows]


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
