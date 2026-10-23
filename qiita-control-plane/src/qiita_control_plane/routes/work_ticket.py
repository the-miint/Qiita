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
| NO_DATA       | 409 — terminal (empty-well outcome)               |
| FAILED        | Reset → PENDING and dispatch (manual restart)     |
| CANCELLED     | Reset → PENDING and dispatch (redrive after fix)  |

A download-ena-study redrive re-runs every step against the live roster. It is
refused (409) when a newer download ticket exists for the pool, and when a FAILED
ticket still has a live job holding the old roster.

The atomic state transition guard inside `runner._atomic_transition`
prevents double-dispatch even if `/run` races with the implicit dispatch
fired by submission.

Auth: every route here requires the caller to be in the action's
`audience` (humans by `system_role`, or service accounts) AND to hold
all of `action.scopes`. prep_sample-scoped submissions additionally
require the caller to have `Tier.ADMIN` on every non-retired study
linked to the prep_sample (wet_lab_admin+ bypass), see
`_check_prep_sample_study_access` in `work_ticket_submit`. Reference- and study_prep-scoped
submissions carry no per-resource gate at this layer; the action's
own audience / scope choices are the policy.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from qiita_common.api_paths import (
    PATH_WORK_TICKET_BY_IDX,
    PATH_WORK_TICKET_CANCEL,
    PATH_WORK_TICKET_FANOUT,
    PATH_WORK_TICKET_FANOUT_COHORT,
    PATH_WORK_TICKET_FANOUT_COHORT_PUMP,
    PATH_WORK_TICKET_PREFIX,
    PATH_WORK_TICKET_ROOT,
    PATH_WORK_TICKET_RUN,
    PATH_WORK_TICKET_STEP_LOGS,
)
from qiita_common.auth_constants import Scope, SystemRole
from qiita_common.log_tail import read_text_tail
from qiita_common.models import (
    NON_TERMINAL_WORK_TICKET_STATES,
    REDRIVABLE_WORK_TICKET_STATES,
    FailureType,
    FanoutCohortKind,
    FanoutListResponse,
    FanoutOverrideRequest,
    FanoutPumpResponse,
    MaskDefinitionStatus,
    ScopeTargetKind,
    StepProgressState,
    WorkTicket,
    WorkTicketCancelRequest,
    WorkTicketCancelResponse,
    WorkTicketCancelResult,
    WorkTicketCreateRequest,
    WorkTicketListResponse,
    WorkTicketReadOutcome,
    WorkTicketResponse,
    WorkTicketState,
    WorkTicketStepLogs,
    WorkTicketSummary,
)

from ..auth.guards import require_scope
from ..auth.principal import Anonymous, Principal, get_current_principal
from ..deps import get_db_pool
from ..dispatch import schedule_dispatch
from ..ena_import.registration import download_ticket_has_live_job, fetch_pool_download_ticket
from ..ena_import.submit import DOWNLOAD_ENA_STUDY_ACTION_ID
from ..fanout_dispatch import (
    COHORT_BUILDERS,
    FanoutCohort,
    active_cohorts,
    clear_override,
    cohort_status,
    overridden_cohorts,
    set_override,
    top_up_dispatch,
)
from ..runner import _fetch_action
from ..step_progress import StepProgressMismatch, check_step_progress, load_step_progress
from ..work_ticket_cancel import WorkTicketNotFound, cancel_work_ticket
from ..work_ticket_submit import (
    _check_audience,
    _check_scopes,
    _fetch_action_for_submission,
    _reset_failed_reference_scope_for_dispatch,
    require_compute_backend_client,
    submit_work_ticket_core,
)
from ..workspace import step_attempt_dir, step_logs_dir, ticket_workspace
from ._helpers import cap_rows

_log = logging.getLogger(__name__)

# Step-logs tail bounds. Defaults err small so a routine `qiita ticket logs`
# stays readable; the caller can widen `tail_lines` up to the byte ceiling.
_STEP_LOGS_DEFAULT_TAIL_LINES = 200
_STEP_LOGS_MAX_TAIL_LINES = 5000
_STEP_LOGS_MAX_TAIL_BYTES = 256 * 1024

# /run applies to a PENDING ticket that was never dispatched and the redrivable
# terminal states (REDRIVABLE_WORK_TICKET_STATES). Everything else is refused. The
# not-applicable set is the COMPLEMENT of the applicable set, so a new
# WorkTicketState defaults to REFUSED — the safe direction; listing the refused
# states positively would silently make a new state runnable.
# FAILED and CANCELLED both redrive in place (reset → PENDING + dispatch), but they
# differ in what they leave behind. A CANCELLED ticket has no in-flight job (the
# cancel scancelled it). A FAILED one MAY still have one: an OOM-killed step
# escalates and submits attempt N+1, so a ticket that then fails leaves that attempt
# live with a real slurm_job_id. The redrive's step-row cleanup keys off exactly that
# difference — see the DELETE in the redrive branch. PENDING just (re)dispatches a
# lost create-time task.
_RUN_APPLICABLE_STATES = frozenset({WorkTicketState.PENDING.value}) | (
    REDRIVABLE_WORK_TICKET_STATES
)
_RUN_NOT_APPLICABLE_STATES = tuple(
    state.value for state in WorkTicketState if state.value not in _RUN_APPLICABLE_STATES
)

router = APIRouter(prefix=PATH_WORK_TICKET_PREFIX, tags=["work-ticket"])


# Page-size bounds for GET /work-ticket, mirroring the audit-log query's
# ge=1 / le=max shape (routes/admin.py). Local to this route — its only
# consumer; the CLI just passes `--limit N` and the server enforces here.
_WORK_TICKET_LIST_DEFAULT_LIMIT = 50
_WORK_TICKET_LIST_MAX_LIMIT = 500


# =============================================================================
# Helpers
# =============================================================================


def _require_compute_backend_client(request: Request) -> None:
    require_compute_backend_client(request.app)


async def _resolve_cancel_filter(pool: asyncpg.Pool, body: WorkTicketCancelRequest) -> list[int]:
    """Resolve a cancel request's `action_id` (+ optional run/pool) filter to the
    NON-terminal work_ticket idxs it matches. A ticket's pool is either its own
    (`sequenced_pool`-scoped, e.g. bcl-convert) or its prep_sample's
    (`prep_sample`-scoped, e.g. read-mask) — COALESCE bridges both so a
    `sequenced_pool_idx` / `sequencing_run_idx` narrowing covers either shape. Only
    non-terminal tickets: the filter targets in-flight work (cancelling a terminal
    ticket is a no-op), whereas an explicit idx is reaped regardless."""
    rows = await pool.fetch(
        "SELECT DISTINCT wt.work_ticket_idx"
        " FROM qiita.work_ticket wt"
        " LEFT JOIN qiita.sequenced_sample ss ON ss.prep_sample_idx = wt.prep_sample_idx"
        " LEFT JOIN qiita.sequenced_pool sp"
        "   ON sp.idx = COALESCE(wt.sequenced_pool_idx, ss.sequenced_pool_idx)"
        " WHERE wt.action_id = $1"
        "   AND wt.state = ANY($2::qiita.work_ticket_state[])"
        "   AND ($3::bigint IS NULL OR sp.sequencing_run_idx = $3)"
        "   AND ($4::bigint IS NULL OR sp.idx = $4)"
        " ORDER BY wt.work_ticket_idx",
        body.action_id,
        list(NON_TERMINAL_WORK_TICKET_STATES),
        body.sequencing_run_idx,
        body.sequenced_pool_idx,
    )
    return [r["work_ticket_idx"] for r in rows]


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
    principal: Principal = Depends(get_current_principal),
) -> WorkTicketResponse:
    """`submit_work_ticket_core` for the requesting principal."""
    return await submit_work_ticket_core(app=request.app, principal=principal, body=body)


# Two-row-source SELECT. work_ticket carries the scope_target_kind plus
# the four nullable scope_target idx columns; for the SEQUENCED_POOL arm
# we additionally need the parent sequencing_run_idx to round-trip the
# scope_target shape Pydantic expects. The LEFT JOIN against
# qiita.sequenced_pool is a no-op for the other three kinds (the
# work_ticket's sequenced_pool_idx is NULL, the join produces NULL).
_WORK_TICKET_COLUMNS = (
    "wt.work_ticket_idx, wt.action_id, wt.action_version, wt.originator_principal_idx,"
    " wt.scope_target_kind, wt.study_idx, wt.prep_idx, wt.reference_idx,"
    " wt.prep_sample_idx, wt.sequenced_pool_idx, wt.block_idx,"
    " sp.sequencing_run_idx, wt.shard_id, wt.mask_idx,"
    " wt.action_context, wt.state, wt.retry_count, wt.max_retries,"
    " wt.failure_type, wt.failure_stage, wt.failure_step_name, wt.failure_reason,"
    " wt.transient_reason, wt.transient_since,"
    " wt.on_success, wt.follow_on_work_ticket_idx, wt.follow_on_error,"
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
# The summary read also nests the ticket's prep_sample read outcome: a LEFT JOIN
# to the sequenced_sample of wt.prep_sample_idx (1:1 with the
# prep_sample), so a read-mask ticket is assessable without a separate lookup. The
# join is NULL for a non-prep_sample-scoped ticket (prep_sample_idx NULL) or a
# prep_sample with no sequenced_sample; `ro_present` (ss.idx) distinguishes "no
# sequenced_sample" from "processed = has no counts yet".
_WORK_TICKET_SUMMARY_FROM_WITH_READ_OUTCOME = (
    _WORK_TICKET_SUMMARY_FROM
    + " LEFT JOIN qiita.sequenced_sample ss ON ss.prep_sample_idx = wt.prep_sample_idx"
)
_WORK_TICKET_SUMMARY_COLUMNS = (
    _WORK_TICKET_COLUMNS + ","
    " cur.step_index AS current_step_index,"
    " cur.step_name AS current_step_name,"
    " cur.compute_target AS current_compute_target,"
    " cur.slurm_job_id AS current_slurm_job_id,"
    " cur.state AS current_step_state,"
    " ss.idx AS ro_present,"
    " ss.raw_read_count_r1r2 AS ro_raw_read_count_r1r2,"
    " ss.biological_read_count_r1r2 AS ro_biological_read_count_r1r2,"
    " ss.quality_filtered_read_count_r1r2 AS ro_quality_filtered_read_count_r1r2,"
    " ss.spikein_read_count_r1r2 AS ro_spikein_read_count_r1r2"
)


def _scope_target_from_columns(
    kind: str,
    *,
    study_idx: int | None,
    prep_idx: int | None,
    reference_idx: int | None,
    prep_sample_idx: int | None,
    sequenced_pool_idx: int | None,
    block_idx: int | None,
    sequencing_run_idx: int | None,
) -> dict[str, Any]:
    """Rebuild the discriminated scope_target dict from the tagged-union
    columns work_ticket stores (scope_target_kind + the six nullable idx
    columns; the SEQUENCED_POOL arm additionally needs the joined
    sequencing_run_idx). Shared by the single-ticket and list-summary row
    shapers so the union mapping lives in exactly one place."""
    if kind == ScopeTargetKind.REFERENCE.value:
        return {"kind": kind, "reference_idx": reference_idx}
    if kind == ScopeTargetKind.STUDY_PREP.value:
        return {"kind": kind, "study_idx": study_idx, "prep_idx": prep_idx}
    if kind == ScopeTargetKind.PREP_SAMPLE.value:
        return {"kind": kind, "prep_sample_idx": prep_sample_idx}
    if kind == ScopeTargetKind.BLOCK.value:
        return {"kind": kind, "block_idx": block_idx}
    # SEQUENCED_POOL — DB CHECK enforces one of the five valid kinds.
    return {
        "kind": kind,
        "sequenced_pool_idx": sequenced_pool_idx,
        "sequencing_run_idx": sequencing_run_idx,
    }


def _shape_work_ticket_columns(data: dict[str, Any]) -> dict[str, Any]:
    """In place, fold a work_ticket row's tagged-union scope columns into a
    single `scope_target` and decode the JSONB columns (`action_context`,
    `on_success`) from asyncpg's JSONB-as-string. Every other column maps 1:1
    onto a WorkTicket field and flows through unchanged — so a scalar field added
    to both the model and `_WORK_TICKET_COLUMNS` needs no edit here. Returns the
    same dict for chaining."""
    data["scope_target"] = _scope_target_from_columns(
        data.pop("scope_target_kind"),
        study_idx=data.pop("study_idx"),
        prep_idx=data.pop("prep_idx"),
        reference_idx=data.pop("reference_idx"),
        prep_sample_idx=data.pop("prep_sample_idx"),
        sequenced_pool_idx=data.pop("sequenced_pool_idx"),
        block_idx=data.pop("block_idx"),
        # Joined sequencing-run idx — only the SEQUENCED_POOL arm carries a
        # value (NULL otherwise) but the column is always selected, so pop
        # defensively with a default.
        sequencing_run_idx=data.pop("sequencing_run_idx", None),
    )
    data["action_context"] = json.loads(data["action_context"])
    if data.get("on_success") is not None:
        data["on_success"] = json.loads(data["on_success"])
    return data


def _row_to_work_ticket(row: asyncpg.Record) -> WorkTicket:
    """Assemble a WorkTicket from a work_ticket row (scope_target rebuilt
    from the tagged-union columns, action_context JSON-decoded). The
    SEQUENCED_POOL arm reads sequencing_run_idx from the joined
    sequenced_pool row — selected upstream so the assembly stays a pure
    transformation."""
    return WorkTicket.model_validate(_shape_work_ticket_columns(dict(row)))


async def fetch_work_ticket(pool: asyncpg.Pool, work_ticket_idx: int) -> WorkTicket | None:
    """Read one ticket with no caller gate, for in-process callers that already
    own it. Routes gate before calling this."""
    row = await pool.fetchrow(
        f"SELECT {_WORK_TICKET_COLUMNS}{_WORK_TICKET_FROM} WHERE wt.work_ticket_idx = $1",
        work_ticket_idx,
    )
    return None if row is None else _row_to_work_ticket(row)


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
    # Nest the prep_sample read outcome. `ro_present` (ss.idx) is NULL when the
    # ticket has no sequenced_sample (non-prep_sample-scoped, or a prep_sample
    # without one) → read_outcome None; otherwise build it from the counts (each
    # individually None until processed). Pop all ro_* either way so they don't
    # reach `_shape_work_ticket_columns`.
    ro_present = data.pop("ro_present")
    ro_counts = {
        "raw_read_count_r1r2": data.pop("ro_raw_read_count_r1r2"),
        "biological_read_count_r1r2": data.pop("ro_biological_read_count_r1r2"),
        "quality_filtered_read_count_r1r2": data.pop("ro_quality_filtered_read_count_r1r2"),
        "spikein_read_count_r1r2": data.pop("ro_spikein_read_count_r1r2"),
    }
    summary_fields["read_outcome"] = (
        WorkTicketReadOutcome.model_validate(ro_counts) if ro_present is not None else None
    )
    shaped = _shape_work_ticket_columns(data)
    return WorkTicketSummary.model_validate({**shaped, **summary_fields})


# =============================================================================
# Fan-out throttle control
# =============================================================================
# All three routes reuse `work_ticket:cancel` (system_admin) rather than minting a
# scope: both are operator overrides on work-ticket dispatch at the same privilege
# tier, and a separate scope would be granted to exactly the same principals.
#
# DECLARED BEFORE `GET /{work_ticket_idx}` ON PURPOSE. Starlette matches routes in
# definition order, so a later `GET /fanout` is swallowed by the int path param and
# 422s on "fanout". `POST /cancel` has no such problem only because nothing else
# claims POST on a one-segment path. Keep this block above that route.


async def _resolve_cohort(request: Request, kind: FanoutCohortKind, key: int) -> FanoutCohort:
    """Build the cohort and 404 when nothing matches it.

    An unknown `kind` never reaches here — FastAPI rejects it against the enum. A
    key with no tickets is a 404 rather than an empty success so a typo cannot
    silently record an override against a cohort that does not exist.
    """
    cohort = COHORT_BUILDERS[kind](key)
    status_ = await cohort_status(
        request.app.state.pool, cohort, max_inflight=request.app.state.settings.fanout_max_inflight
    )
    if status_.total == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"no fan-out cohort {kind.value}/{key} — it has no work tickets",
        )
    return cohort


async def _pump_and_report(request: Request, cohort: FanoutCohort) -> FanoutPumpResponse:
    """Pump `cohort`, then report what moved and where it now stands.

    Status is read after the pump, so it reflects this call's release rather than
    the state before it. Not a strict snapshot — a concurrent pump or a ticket going
    terminal in between can move it on — which matches `cohort_status` being a read
    for a human, not an input to a release decision.

    `top_up_dispatch` resolves the override itself, so the default passed here only
    applies when the cohort has none.
    """
    released = await top_up_dispatch(
        request.app.state.pool,
        cohort,
        max_inflight=request.app.state.settings.fanout_max_inflight,
        dispatch_cb=lambda idx: schedule_dispatch(request.app, idx),
    )
    return FanoutPumpResponse(
        released=released,
        status=await cohort_status(
            request.app.state.pool,
            cohort,
            max_inflight=request.app.state.settings.fanout_max_inflight,
        ),
    )


@router.get(PATH_WORK_TICKET_FANOUT, response_model=FanoutListResponse)
async def list_fanout_cohorts(
    request: Request,
    _scope: Principal = Depends(require_scope(Scope.WORK_TICKET_CANCEL)),
) -> FanoutListResponse:
    """Every fan-out cohort with held or in-flight children, plus every cohort
    carrying a runtime override, with its throttle state.

    The first call an operator makes when a fan-out looks stuck: it distinguishes
    "fail-stopped", "at its cap", and "nothing held" without a DB session.

    Overrides are unioned in because nothing evicts one — a cohort that has fully
    drained drops out of `active_cohorts` while its override stays set and reapplies
    if that (kind, key) is ever re-run. Listing only active cohorts made a set
    override unenumerable at exactly the moment it became a surprise, so an operator
    could not ask "what have I set?". A drained cohort appears with zero counts and a
    non-null `override`, which reads as "set, nothing to apply it to".
    """
    pool = request.app.state.pool
    default = request.app.state.settings.fanout_max_inflight
    cohorts = await active_cohorts(pool)
    seen = {(c.kind, c.key) for c in cohorts}
    cohorts.extend(c for c in overridden_cohorts() if (c.kind, c.key) not in seen)
    return FanoutListResponse(
        cohorts=[await cohort_status(pool, cohort, max_inflight=default) for cohort in cohorts]
    )


@router.patch(PATH_WORK_TICKET_FANOUT_COHORT, response_model=FanoutPumpResponse)
async def set_fanout_cohort_override(
    kind: FanoutCohortKind,
    key: int,
    body: FanoutOverrideRequest,
    request: Request,
    _scope: Principal = Depends(require_scope(Scope.WORK_TICKET_CANCEL)),
    # 503 without an orchestrator, like every other dispatching route. Not
    # cosmetic: `schedule_dispatch` raises when the client is unset, the pump's
    # per-ticket guard swallows it, and the release is already committed — so
    # without this the route would answer 200 with a `released` list whose tickets
    # are un-held, undispatched, and unreachable by any later pump.
    _: None = Depends(_require_compute_backend_client),
) -> FanoutPumpResponse:
    """Retune one cohort's in-flight cap and pump it in the same call.

    Pumping here is the point, not a convenience: the pump is edge-triggered (a
    child's terminal transition or startup reconcile), so a route that only recorded
    the cap would appear inert until unrelated work finished — which is exactly the
    confusion this surface exists to end.

    `max_inflight: null` clears the override. Raising it releases immediately;
    lowering it releases nothing and lets the excess drain, since the pump only ever
    moves tickets held → released and never recalls one.

    The override is in-memory and process-local, so a CP restart reverts the cohort
    to the FANOUT_MAX_INFLIGHT default. That revert is conservative for a RAISED cap
    and not for a LOWERED one, which reverts upward to more in flight than was asked
    for — `set_override` WARNs on the lowering case, and the `_OVERRIDES` comment
    carries the full asymmetry.
    """
    cohort = await _resolve_cohort(request, kind, key)
    if body.max_inflight is None:
        clear_override(cohort)
    else:
        # Bounded by the request model; the registry re-checks (fail fast at both).
        # `default` is what a restart would revert to, and is what makes a lowering
        # WARN rather than being recorded silently.
        set_override(
            cohort,
            body.max_inflight,
            default=request.app.state.settings.fanout_max_inflight,
        )
    return await _pump_and_report(request, cohort)


@router.post(PATH_WORK_TICKET_FANOUT_COHORT_PUMP, response_model=FanoutPumpResponse)
async def pump_fanout_cohort(
    kind: FanoutCohortKind,
    key: int,
    request: Request,
    _scope: Principal = Depends(require_scope(Scope.WORK_TICKET_CANCEL)),
    # Same reason as the PATCH above: pumping without a dispatch path strands
    # everything it releases.
    _: None = Depends(_require_compute_backend_client),
) -> FanoutPumpResponse:
    """Re-trigger a cohort's pump without changing its cap.

    Recovery for a cohort stranded by a pump that failed on the last in-flight
    child's completion hook: with everything remaining held, no terminal transition
    is left to re-trigger it, and before this route only a CP restart would.
    """
    return await _pump_and_report(request, await _resolve_cohort(request, kind, key))


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
    ticket = await fetch_work_ticket(pool, work_ticket_idx)
    if ticket is None:
        raise not_found
    is_originator = ticket.originator_principal_idx == principal.principal_idx
    is_bypass = principal.has_role_at_least(SystemRole.WET_LAB_ADMIN)
    if not (is_originator or is_bypass):
        raise not_found
    return ticket


@router.get(
    PATH_WORK_TICKET_ROOT,
    response_model=WorkTicketListResponse,
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
    sequenced_pool_idx: int | None = Query(
        default=None,
        gt=0,
        description=(
            "Filter to tickets that touch this sequenced_pool: pool-scoped tickets, "
            "tickets on one of the pool's samples, and block tickets whose block "
            "covers one of them."
        ),
    ),
    prep_sample_idx: int | None = Query(
        default=None, gt=0, description="Filter to tickets scoped to this prep_sample."
    ),
    action_id: str | None = Query(
        default=None,
        min_length=1,
        description="Filter to one action_id (e.g. read-mask), across every action_version.",
    ),
    failure_type: FailureType | None = Query(
        default=None, description="Filter to FAILED tickets whose failure was of this type."
    ),
    limit: int = Query(
        default=_WORK_TICKET_LIST_DEFAULT_LIMIT, ge=1, le=_WORK_TICKET_LIST_MAX_LIMIT
    ),
) -> WorkTicketListResponse:
    """List work tickets, each with a snapshot of its *current* compute
    placement (target, SLURM job id, step state) from a single LATERAL join
    against work_ticket_step — no live SLURM hop, so the read is at most one
    poll-interval stale (see `WorkTicketSummary`).

    Scope: by default a caller sees only tickets they originated. `?all=true`
    widens to every originator and is gated to wet_lab_admin+ (mirrors the
    single-ticket GET's role bypass); a non-admin requesting it gets 403.
    Anonymous → 401. Ordered newest-first (work_ticket_idx DESC), capped by
    `limit`, with `truncated` True when the underlying set exceeded it — a
    pool's ticket set can run to hundreds of rows, so a caller assembling one
    can tell a complete answer from a prefix instead of inferring it from the
    row count.

    Every filter AND-composes (`?state=completed&active=true` is a valid —
    empty — intersection), so a caller can scope to "my active tickets" or
    "this pool's read-mask tickets" in one query. `sequenced_pool_idx`
    composes with the originator scoping rather than replacing it: a caller
    without `?all=true` still sees only the tickets they originated, so the
    pool filter is not a way around that.

    `sequenced_pool_idx` matches a ticket by any of the three ways a ticket
    reaches a pool, so a pool's work is visible whichever submit path
    produced it:
      • pool-scoped   — wt.sequenced_pool_idx (bcl-convert)
      • sample-scoped — the ticket's prep_sample is one of the pool's
        sequenced_samples (read-mask; the join that also feeds read_outcome)
      • block-scoped  — the ticket's block covers one of those samples
        (read-mask-block, whose own prep_sample_idx is NULL)
    A block ticket spans many samples, so it carries no read_outcome; the
    per-sample counts for a block-masked pool come from the pool's
    sequenced-sample roster instead.

    The pool arms read the sample's CURRENT sequenced_pool_idx and do not
    exclude retired prep_samples, so this list keeps a retired sample's
    tickets while the pool roster drops the sample itself — a ticket is a
    record of work that ran, the roster is the pool's live membership.

    An idx that matches nothing (including one that does not exist) returns
    an empty list rather than 404 — these are filters on a list, and the
    route stays enumeration-safe like the single-ticket GET above."""
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
    if prep_sample_idx is not None:
        args.append(prep_sample_idx)
        conditions.append(f"wt.prep_sample_idx = ${len(args)}")
    if action_id is not None:
        args.append(action_id)
        conditions.append(f"wt.action_id = ${len(args)}")
    if failure_type is not None:
        args.append(failure_type.value)
        conditions.append(f"wt.failure_type = ${len(args)}::qiita.failure_type")
    if sequenced_pool_idx is not None:
        args.append(sequenced_pool_idx)
        n = len(args)
        # The three arms of "touches this pool" (see the docstring). The
        # sample arm reads the already-joined `ss` — sequenced_sample is
        # UNIQUE on prep_sample_idx, so it cannot fan a ticket into several
        # rows. The block arm is an EXISTS because a block covers many
        # samples; a join there would.
        conditions.append(
            f"(wt.sequenced_pool_idx = ${n} OR ss.sequenced_pool_idx = ${n}"
            " OR EXISTS (SELECT 1 FROM qiita.block_member bm"
            "            JOIN qiita.sequenced_sample bss"
            "              ON bss.prep_sample_idx = bm.prep_sample_idx"
            f"            WHERE bm.block_idx = wt.block_idx AND bss.sequenced_pool_idx = ${n}))"
        )
    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    # Over-fetch by one so cap_rows can tell a full page from a cut one.
    args.append(limit + 1)
    rows = await pool.fetch(
        f"SELECT {_WORK_TICKET_SUMMARY_COLUMNS}{_WORK_TICKET_SUMMARY_FROM_WITH_READ_OUTCOME}{where}"
        f" ORDER BY wt.work_ticket_idx DESC LIMIT ${len(args)}",
        *args,
    )
    rows, truncated = cap_rows(rows, limit)
    return WorkTicketListResponse(
        tickets=[_row_to_work_ticket_summary(row) for row in rows],
        count=len(rows),
        truncated=truncated,
    )


@router.get(
    PATH_WORK_TICKET_STEP_LOGS,
    response_model=WorkTicketStepLogs,
)
async def get_work_ticket_step_logs(
    work_ticket_idx: int,
    step_index: int,
    request: Request,
    pool: asyncpg.Pool = Depends(get_db_pool),
    principal: Principal = Depends(get_current_principal),
    attempt: int | None = Query(
        default=None,
        ge=0,
        description="Step attempt to read; defaults to the latest recorded attempt.",
    ),
    tail_lines: int = Query(
        default=_STEP_LOGS_DEFAULT_TAIL_LINES,
        ge=1,
        le=_STEP_LOGS_MAX_TAIL_LINES,
        description="Max lines of each stream to return (from the end).",
    ),
) -> WorkTicketStepLogs:
    """Read a bounded tail of a step attempt's stdout/stderr.

    The logs live in the attempt's `logs/` directory (layout: `workspace.py`),
    owned `qiita-orch:qiita-pipeline` (mode 2770). The CP service account is in
    `qiita-pipeline`, so it reads them straight off shared scratch
    and serves the tail here — letting an operator diagnose an OOM / bad input
    / contract violation without a host shell or sudo.

    Auth mirrors `GET /work-ticket/{idx}` exactly: 401 on Anonymous; the
    originator passes; wet_lab_admin+ bypasses; anyone else gets 404 (not 403),
    so a caller cannot probe which tickets exist. A missing ticket, an unknown
    step index, or an unknown attempt all return the same 404.

    `attempt` defaults to the latest recorded attempt for the step — the run an
    operator most likely just watched fail. A stream the job never wrote comes
    back as an empty string, not an error."""
    if isinstance(principal, Anonymous):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="authentication required"
        )
    not_found = HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"work_ticket {work_ticket_idx} step {step_index} logs not found",
    )
    owner_row = await pool.fetchrow(
        "SELECT originator_principal_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    if owner_row is None:
        raise not_found
    is_originator = owner_row["originator_principal_idx"] == principal.principal_idx
    is_bypass = principal.has_role_at_least(SystemRole.WET_LAB_ADMIN)
    if not (is_originator or is_bypass):
        raise not_found

    # Resolve the (step_index, attempt) -> step_name from the write-ahead spine.
    # Without a progress row the step never reached a backend, so there is no
    # log path to read — a 404, indistinguishable from an out-of-range index.
    all_rows = await load_step_progress(pool, work_ticket_idx)
    rows = [r for r in all_rows if r.step_index == step_index]
    if not rows:
        raise not_found
    if attempt is None:
        chosen = max(rows, key=lambda r: r.attempt)
    else:
        chosen = next((r for r in rows if r.attempt == attempt), None)
        if chosen is None:
            raise not_found

    ticket_root = request.app.state.settings.path_scratch_ticket
    if ticket_root is None:
        # PATH_SCRATCH is required at production boot; a None here is a
        # misconfigured deploy, not a client error — fail loud.
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="path_scratch_ticket is not configured on this control plane",
        )
    # Defense-in-depth: step_name comes from the DB, sourced from admin-synced
    # workflow YAML (WorkflowStep.name, no character-class constraint) — not from
    # the request — and the runner already uses it verbatim to build the write
    # path. But this is the first place it's interpolated into a path on a request
    # path, so a separator / parent-ref would let a malformed entry name escape the
    # ticket root. That's a contract violation, never a client error: fail loud.
    if "/" in chosen.step_name or "\\" in chosen.step_name or chosen.step_name in (".", ".."):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"step_name {chosen.step_name!r} is not a valid path segment",
        )
    logs_dir = step_logs_dir(
        step_attempt_dir(
            ticket_workspace(ticket_root, work_ticket_idx), chosen.step_name, chosen.attempt
        )
    )
    stdout, stdout_truncated = read_text_tail(
        logs_dir / "stdout", max_lines=tail_lines, max_bytes=_STEP_LOGS_MAX_TAIL_BYTES
    )
    stderr, stderr_truncated = read_text_tail(
        logs_dir / "stderr", max_lines=tail_lines, max_bytes=_STEP_LOGS_MAX_TAIL_BYTES
    )
    return WorkTicketStepLogs(
        work_ticket_idx=work_ticket_idx,
        step_index=step_index,
        attempt=chosen.attempt,
        step_name=chosen.step_name,
        stdout=stdout,
        stderr=stderr,
        stdout_truncated=stdout_truncated,
        stderr_truncated=stderr_truncated,
    )


async def _refuse_unsafe_download_redrive(
    conn: asyncpg.Connection, *, work_ticket_idx: int, sequenced_pool_idx: int, current_state: str
) -> None:
    """409 when redriving this download ticket would duplicate a newer ticket's
    runs, or adopt a live job that holds a stale roster."""
    latest = await fetch_pool_download_ticket(conn, sequenced_pool_idx=sequenced_pool_idx)
    if latest is not None and latest["work_ticket_idx"] != work_ticket_idx:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"work_ticket {work_ticket_idx} is superseded by download ticket "
                f"{latest['work_ticket_idx']} ({latest['work_ticket_state']}) on the same pool"
            ),
        )
    if current_state == WorkTicketState.FAILED.value and await download_ticket_has_live_job(
        conn, work_ticket_idx=work_ticket_idx
    ):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"work_ticket {work_ticket_idx} still has a live download job that may hold "
                "an outdated run roster; cancel it by idx (work_ticket_idxs) to reap the job, "
                "then re-import the study; the ticket stays FAILED"
            ),
        )


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
        "SELECT action_id, action_version, state, scope_target_kind, reference_idx,"
        "       sequenced_pool_idx"
        " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
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
    disabled = (
        f"action {row['action_id']!r}/{row['action_version']!r} no longer enabled — cannot redrive"
    )
    action = await _fetch_action_for_submission(pool, row["action_id"], row["action_version"])
    if action is None or not action["enabled"]:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=disabled)
    _check_audience(principal, action["audience"])
    _check_scopes(principal, action["scopes"])

    current_state = row["state"]
    if current_state in _RUN_NOT_APPLICABLE_STATES:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "reason": f"work_ticket is in {current_state!r}; /run not applicable",
                "current_state": current_state,
            },
        )

    if current_state in REDRIVABLE_WORK_TICKET_STATES:
        # Manual restart: FAILED / CANCELLED → PENDING. Per arch.md spec, resets
        # retry_count to 0 (operator override of the auto-retry budget)
        # and clears the failure_* columns so the
        # work_ticket_failure_consistent DB CHECK is honoured (failure_*
        # all NULL when state != failed; a CANCELLED ticket already carries
        # NULLs, so the clear is a harmless no-op there). The ticket reset, the
        # dead step-row drop, and the scope-target reset are ONE transaction so
        # a redrive never half-applies. Atomic; refuses if state changed
        # under us between the SELECT and now (the WHERE binds the exact
        # from-state we read, so a concurrent flip loses the race cleanly).
        is_download = row["action_id"] == DOWNLOAD_ENA_STUDY_ACTION_ID
        # Read before the transaction so the request holds one connection. None
        # here means the action was disabled since the check above.
        definition = await _fetch_action(pool, row["action_id"], row["action_version"])
        if definition is None:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=disabled)
        async with pool.acquire() as conn, conn.transaction():
            # A redrive fast-forwards the completed mint, so the mint's own refusal
            # of a deprecated config never runs: the ticket would mask, or build
            # on, a filter that is void.
            mask = await conn.fetchrow(
                "SELECT md.mask_idx, md.status FROM qiita.work_ticket wt"
                " JOIN qiita.mask_definition md ON md.mask_idx = wt.mask_idx"
                " WHERE wt.work_ticket_idx = $1",
                work_ticket_idx,
            )
            if mask is not None and mask["status"] == MaskDefinitionStatus.DEPRECATED.value:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        f"work_ticket {work_ticket_idx} runs under mask_definition"
                        f" {mask['mask_idx']}, which is deprecated; it is not redriven."
                        " Submit the work under a current mask instead"
                    ),
                )
            if is_download:
                await _refuse_unsafe_download_redrive(
                    conn,
                    work_ticket_idx=work_ticket_idx,
                    sequenced_pool_idx=row["sequenced_pool_idx"],
                    current_state=current_state,
                )
            updated = await conn.fetchval(
                "UPDATE qiita.work_ticket"
                " SET state = $1::qiita.work_ticket_state,"
                "     retry_count = 0,"
                "     failure_type = NULL,"
                "     failure_stage = NULL,"
                "     failure_step_name = NULL,"
                "     failure_reason = NULL,"
                # A prior FAILED/CANCELLED outcome may already have been
                # notified; a redrive that later reaches its TRUE terminal state
                # must re-notify, so reset the notification lifecycle here.
                "     notified_at = NULL,"
                "     notify_attempts = 0"
                " WHERE work_ticket_idx = $2"
                "   AND state = $3::qiita.work_ticket_state"
                " RETURNING work_ticket_idx",
                WorkTicketState.PENDING.value,
                work_ticket_idx,
                current_state,
            )
            if updated is None:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="work_ticket state changed under /run; retry",
                )

            # Which prior progress rows survive the redrive. `completed` survives,
            # so the runner still fast-forwards finished entries (a download
            # ticket drops them in the second DELETE below).
            #
            # Of the rest, the question is whether the row still names a job
            # worth adopting — `_adopt_or_submit` re-attaches by the persisted
            # `slurm_job_id`, and the runner may only adopt a LIVE attempt.
            #
            #   * `failed` — always dropped. Re-using that attempt number would
            #     collide (record_submitted rejects anything but submitting→,
            #     and record_failed refuses failed→failed), wedging the redrive
            #     on the dead row. Dropped, the runner re-enters the entry,
            #     finds the orphaned on-disk attempt dir and advances past it to
            #     a fresh one (it can neither reuse the stale read-only output
            #     nor delete the SLURM-job-owned dir — see `_attempt_is_unowned`).
            #   * in-flight WITH a job id — kept when redriving a FAILED ticket.
            #     A FAILED ticket CAN have a live job: when a step OOM-kills, the
            #     runner escalates and submits attempt N+1, so a ticket that then
            #     fails leaves that attempt live with a real id. Dropping it
            #     would strand the job outright — the CP forgets the id, nothing
            #     polls it, and no later redrive can reclaim it.
            #   * in-flight WITHOUT a job id (a `submitting` write-ahead row) —
            #     dropped. Its only handle is the deterministic `job_name`, and
            #     the find-by-name orphan closer that would use it runs solely
            #     under `resume=True`; /run dispatches with resume=False, so a
            #     kept row would fall through to a fresh submit writing into the
            #     same attempt dir a possibly-live orphan is already using.
            #     Dropping it restores the advance-to-a-fresh-dir guard.
            #
            # CANCELLED is the carve-out: cancel is terminal-flip-then-scancel,
            # and the runner's abort path stops without recording the step row
            # terminal — so a cancelled ticket keeps an in-flight row naming a
            # job the reap already killed. Adopting that is the very bug this
            # commit fixes, reached through a live row instead of a dead one, so
            # a post-cancel redrive drops every non-completed row.
            redrive_after_cancel = current_state == WorkTicketState.CANCELLED.value
            await conn.execute(
                "DELETE FROM qiita.work_ticket_step"
                " WHERE work_ticket_idx = $1"
                "   AND state <> $2"
                "   AND ($3::boolean OR state = $4 OR slurm_job_id IS NULL)",
                work_ticket_idx,
                StepProgressState.COMPLETED.value,
                redrive_after_cancel,
                StepProgressState.FAILED.value,
            )
            if is_download:
                # The roster is re-read on dispatch, so a kept completed ingest would be stale.
                await conn.execute(
                    "DELETE FROM qiita.work_ticket_step WHERE work_ticket_idx = $1 AND state = $2",
                    work_ticket_idx,
                    StepProgressState.COMPLETED.value,
                )

            # Rows that no longer match the action's steps would only fail the
            # ticket again at submission; refused here, it stays as it was.
            try:
                check_step_progress(
                    work_ticket_idx,
                    row["action_id"],
                    await load_step_progress(conn, work_ticket_idx),
                    definition.steps,
                )
            except StepProgressMismatch as exc:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

            await _reset_failed_reference_scope_for_dispatch(
                conn,
                scope_target_kind=row["scope_target_kind"],
                reference_idx=row["reference_idx"],
                context="redrive",
            )

        _log.info(
            "manual restart of work_ticket %d (FAILED → PENDING; retry_count reset)",
            work_ticket_idx,
        )
    # PENDING: no state change needed, just dispatch.

    schedule_dispatch(request.app, work_ticket_idx)
    return WorkTicketResponse(work_ticket_idx=work_ticket_idx, state=WorkTicketState.PENDING)


@router.post(PATH_WORK_TICKET_CANCEL, response_model=WorkTicketCancelResponse)
async def cancel_work_tickets(
    body: WorkTicketCancelRequest,
    request: Request,
    pool: asyncpg.Pool = Depends(get_db_pool),
    _scope: Principal = Depends(require_scope(Scope.WORK_TICKET_CANCEL)),
    _: None = Depends(_require_compute_backend_client),
) -> WorkTicketCancelResponse:
    """Operator-cancel of in-flight compute (system_admin, `work_ticket:cancel`):
    for each selected ticket, flip it terminal (`cancelled`) so the runner's poll
    loop aborts and no new attempt is submitted, THEN scancel its SLURM job(s) — the
    terminal-first ordering that avoids re-submitting the job we just reaped.

    Selection is the explicit `work_ticket_idxs` (reaped regardless of state — a
    defensive orphan-reap) UNION the `action_id` (+ optional run/pool) filter
    matches (non-terminal only). Idempotent per ticket: an already-terminal ticket
    is not re-flipped (`cancelled=False`) but its jobs are still reaped. A missing
    explicit idx comes back `not_found`; a reap that fails after the flip lands
    comes back with `reap_error` (the flip stands — re-run cancel to retry)."""
    backend_client = request.app.state.compute_backend_client
    # Explicit idxs first (stable, deduped), then filter matches not already listed.
    selected = list(dict.fromkeys(body.work_ticket_idxs))
    if body.action_id is not None:
        for idx in await _resolve_cancel_filter(pool, body):
            if idx not in selected:
                selected.append(idx)

    results: list[WorkTicketCancelResult] = []
    for idx in selected:
        try:
            outcome = await cancel_work_ticket(pool, backend_client, idx)
        except WorkTicketNotFound:
            results.append(WorkTicketCancelResult(work_ticket_idx=idx, not_found=True))
            continue
        results.append(WorkTicketCancelResult(**outcome))

    return WorkTicketCancelResponse(
        requested=len(selected),
        cancelled=sum(1 for r in results if r.cancelled),
        results=results,
    )
