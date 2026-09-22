"""Runner DB-access helpers — work_ticket / action fetches and guarded state transitions."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import asyncpg
from qiita_common.actions import (
    ActionDefinition,
)
from qiita_common.models import (
    NON_TERMINAL_WORK_TICKET_STATES,
    FailureType,
    WorkTicketFailureStage,
    WorkTicketState,
)

# =============================================================================
# DB access helpers
# =============================================================================


# The runner mints two identities onto the ticket row itself, so the arms that act
# later read a COLUMN rather than the submitter's action_context. The column name is
# looked up here rather than passed as SQL, so a caller cannot reach the statement.
_TICKET_IDX_COLUMNS = {"alignment_idx": "alignment_idx", "mask_idx": "mask_idx"}


async def persist_ticket_idx(
    pool: asyncpg.Pool, *, column: str, work_ticket_idx: int, value: int
) -> None:
    """Write one minted identity onto `qiita.work_ticket`.

    Idempotent by construction: both callers re-resolve to the same value on a resume,
    so re-running writes what is already there. Like every runner DB write it fails
    loud — a PG outage raises and unwinds the run via run_workflow's catch-all."""
    await pool.execute(
        f"UPDATE qiita.work_ticket SET {_TICKET_IDX_COLUMNS[column]} = $1 "
        "WHERE work_ticket_idx = $2",
        value,
        work_ticket_idx,
    )


# LEFT JOIN qiita.sequenced_pool so the SEQUENCED_POOL scope_target arm
# can carry the parent sequencing_run_idx — _build_scope_target reads it
# alongside sequenced_pool_idx to produce the {kind: sequenced_pool, ...}
# dict the orchestrator's SCOPE_SCALARS_BY_KIND injection consumes.
_WORK_TICKET_COLS = (
    "wt.work_ticket_idx, wt.action_id, wt.action_version, wt.originator_principal_idx, "
    "wt.scope_target_kind, wt.study_idx, wt.prep_idx, wt.reference_idx, "
    "wt.prep_sample_idx, wt.sequenced_pool_idx, sp.sequencing_run_idx, "
    "wt.block_idx, wt.mask_idx, wt.alignment_idx, wt.shard_id, "
    "wt.action_context, wt.state, wt.retry_count, wt.max_retries, "
    "wt.resource_override, wt.escalated_resource_floor"
)
_WORK_TICKET_FROM = (
    " FROM qiita.work_ticket wt LEFT JOIN qiita.sequenced_pool sp ON sp.idx = wt.sequenced_pool_idx"
)

_ACTION_COLS = (
    "action_id, version, target_kind, description, "
    "scopes, audience, context_schema, steps, "
    "cpu_ceiling, mem_ceiling_gb, walltime_ceiling, gpu_ceiling, "
    "success_status, failure_status"
)


async def _fetch_work_ticket(pool: asyncpg.Pool, work_ticket_idx: int) -> dict[str, Any]:
    row = await pool.fetchrow(
        f"SELECT {_WORK_TICKET_COLS}{_WORK_TICKET_FROM} WHERE wt.work_ticket_idx = $1",
        work_ticket_idx,
    )
    if row is None:
        raise RuntimeError(f"work_ticket {work_ticket_idx} not found")
    out = dict(row)
    # asyncpg returns JSONB as a JSON *string* (no codec is registered), so
    # every JSONB column the runner indexes into is parsed eagerly here. A SQL
    # NULL stays None: no action_context, no override, nothing escalated yet.
    for jsonb_col in ("action_context", "resource_override", "escalated_resource_floor"):
        if isinstance(out.get(jsonb_col), str):
            out[jsonb_col] = json.loads(out[jsonb_col])
    return out


async def _fetch_action(
    pool: asyncpg.Pool, action_id: str, version: str
) -> ActionDefinition | None:
    """Reconstruct an ActionDefinition from qiita.action — filtered by
    enabled=true so a manually disabled action is unreachable to the
    runner without an explicit operator un-disable."""
    row = await pool.fetchrow(
        f"SELECT {_ACTION_COLS} FROM qiita.action "
        "WHERE action_id = $1 AND version = $2 AND enabled = true",
        action_id,
        version,
    )
    if row is None:
        return None
    return ActionDefinition.model_validate(
        {
            "action_id": row["action_id"],
            "version": row["version"],
            "target_kind": row["target_kind"],
            "description": row["description"],
            "scopes": list(row["scopes"]),
            "audience": json.loads(row["audience"]),
            "context_schema": json.loads(row["context_schema"]),
            "steps": json.loads(row["steps"]),
            "action_ceiling": {
                "cpu": row["cpu_ceiling"],
                "mem_gb": row["mem_ceiling_gb"],
                "walltime": row["walltime_ceiling"],
                "gpu": row["gpu_ceiling"],
            },
            "success_status": row["success_status"],
            "failure_status": row["failure_status"],
        }
    )


# =============================================================================
# Escalated resource floor (qiita.work_ticket.escalated_resource_floor)
# =============================================================================
#
# What the column is for, and how it relates to `resource_override`, is on the
# column itself (`COMMENT ON COLUMN`, reachable from `\d+ qiita.work_ticket`).
# What lives here is the wire shape, which this pair of functions owns in both
# directions:
#
#     {"<step name>": {"mem_gb": 384, "walltime_seconds": 115200}}
#
# `walltime` crosses as whole seconds — JSONB has no interval type, and an int
# round-trips exactly where a float would not.


@dataclass(frozen=True)
class StepResourceFloor:
    """One `step:` entry's persisted escalated floor, decoded.

    Both axes are independently optional: a step that has only ever OOM-killed
    carries `mem_gb` with `walltime=None`, and vice versa. An all-None instance
    (`_NO_STEP_FLOOR`) means "this step has not escalated" and leaves the
    dispatch sizing exactly where it is today."""

    mem_gb: int | None = None
    walltime: timedelta | None = None


# Shared "nothing learned yet" value — every step of a fresh ticket, and any
# entry with no recorded floor.
_NO_STEP_FLOOR = StepResourceFloor()


def _parse_escalated_floor(raw: Any, *, work_ticket_idx: int) -> dict[str, StepResourceFloor]:
    """Decode `work_ticket.escalated_resource_floor` into per-step floors.

    NULL / absent → an empty map (no step has escalated). Anything else must
    match the shape this module writes; a mismatch raises rather than being
    silently dropped, because a floor that decodes to None is indistinguishable
    from "never escalated" and would quietly resurrect the very re-climb this
    column exists to prevent. Callers must invoke this somewhere a raise is
    recorded against the ticket, not before the runner's failure handler is
    armed."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise RuntimeError(
            f"work_ticket {work_ticket_idx} escalated_resource_floor is "
            f"{type(raw).__name__}, expected a JSON object"
        )
    out: dict[str, StepResourceFloor] = {}
    for step_name, axes in raw.items():
        if not isinstance(axes, dict):
            raise RuntimeError(
                f"work_ticket {work_ticket_idx} escalated_resource_floor[{step_name!r}] is "
                f"{type(axes).__name__}, expected a JSON object"
            )
        mem_gb = axes.get("mem_gb")
        walltime_seconds = axes.get("walltime_seconds")
        # bool is an int subclass in Python; reject it explicitly so a stray
        # `true` can't be read as a 1 GB floor.
        for axis, value in (("mem_gb", mem_gb), ("walltime_seconds", walltime_seconds)):
            if value is None:
                continue
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise RuntimeError(
                    f"work_ticket {work_ticket_idx} "
                    f"escalated_resource_floor[{step_name!r}][{axis!r}] is {value!r}, "
                    "expected a positive integer"
                )
        out[step_name] = StepResourceFloor(
            mem_gb=mem_gb,
            walltime=(
                timedelta(seconds=walltime_seconds) if walltime_seconds is not None else None
            ),
        )
    return out


async def _persist_escalated_floor(
    pool: asyncpg.Pool,
    work_ticket_idx: int,
    *,
    step_name: str,
    mem_gb: int | None = None,
    walltime: timedelta | None = None,
) -> None:
    """Record one step's newly-escalated floor on the ticket.

    Merges the given axis (or axes) into that step's existing object rather
    than replacing it, so raising the memory floor never drops a walltime floor
    the same step learned earlier — the two axes escalate independently, from
    separate arms of the retry loop. One `jsonb_set` statement, so the merge
    reads and writes the row within a single UPDATE: no application-side
    read-modify-write window.

    Deliberately NOT best-effort. A lost write silently reintroduces the
    re-climb this column exists to prevent, and every other write in the same
    retry arm already raises on failure. A transient CP-DB error raised here is
    classified RETRIABLE by the runner's catch-all, so a blip leaves the ticket
    redrivable rather than abandoned."""
    patch: dict[str, int] = {}
    if mem_gb is not None:
        patch["mem_gb"] = mem_gb
    if walltime is not None:
        # Whole seconds — see the wire-shape note above. `ceil`, not truncate:
        # a fractional walltime (ISO 8601 admits one) rounded DOWN would make
        # the floor compare below the ceiling it was clamped to, so the
        # ceiling-exhaustion fail-fast would miss once and burn an attempt.
        patch["walltime_seconds"] = math.ceil(walltime.total_seconds())
    if not patch:
        raise ValueError(
            f"_persist_escalated_floor for work_ticket {work_ticket_idx} step "
            f"{step_name!r} was given no axis to record"
        )
    updated = await pool.fetchval(
        "UPDATE qiita.work_ticket"
        "   SET escalated_resource_floor = jsonb_set("
        "           COALESCE(escalated_resource_floor, '{}'::jsonb),"
        "           ARRAY[$2::text],"
        "           COALESCE(escalated_resource_floor -> $2::text, '{}'::jsonb) || $3::jsonb,"
        "           true"
        "       )"
        " WHERE work_ticket_idx = $1"
        " RETURNING work_ticket_idx",
        work_ticket_idx,
        step_name,
        json.dumps(patch),
    )
    if updated is None:
        raise RuntimeError(
            f"could not persist escalated resource floor for work_ticket "
            f"{work_ticket_idx} step {step_name!r}: row not found"
        )


# The non-terminal states a work_ticket may legitimately transition FROM — the
# allowed-source set for every guarded transition. A list because asyncpg binds
# it as a state[] array; the set itself lives beside the enum in qiita_common.
_NON_TERMINAL_STATES = list(NON_TERMINAL_WORK_TICKET_STATES)


async def _guarded_state_update(
    pool: asyncpg.Pool | asyncpg.Connection,
    work_ticket_idx: int,
    *,
    set_clause: str,
    set_params: list[Any],
    allowed_states: list[str],
    action: str,
) -> None:
    """Run a TOCTOU-safe work_ticket state UPDATE.

    Applies `set_clause` only when the row's current state is one of
    `allowed_states`. Coupling the caller MUST honour: `set_clause` references
    exactly $1..$len(set_params); the helper appends the WHERE's $n+1
    (work_ticket_idx) and $n+2 (allowed_states) after them. If nothing matched, reads the
    actual state and raises — surfacing a stuck/racing ticket loudly instead of
    silently overwriting it. `action` names the attempted transition in that
    error. Accepts a pool (transient connection) or a live Connection, so the
    finalize block can run its transition inside the same transaction as
    `_consume_upload_handles` and the status PATCH."""
    n = len(set_params)
    updated = await pool.fetchval(
        f"UPDATE qiita.work_ticket SET {set_clause}"
        f" WHERE work_ticket_idx = ${n + 1}"
        f"   AND state = ANY(${n + 2}::qiita.work_ticket_state[])"
        " RETURNING work_ticket_idx",
        *set_params,
        work_ticket_idx,
        allowed_states,
    )
    if updated is None:
        actual = await pool.fetchval(
            "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
            work_ticket_idx,
        )
        raise RuntimeError(
            f"could not {action} work_ticket {work_ticket_idx}: "
            f"expected state in {allowed_states}, got {actual!r}"
        )


async def _atomic_transition(
    pool: asyncpg.Pool | asyncpg.Connection,
    work_ticket_idx: int,
    *,
    expected: WorkTicketState,
    new: WorkTicketState,
) -> None:
    """Guarded single-state transition (expected → new). Raises if the row isn't
    in `expected` — surfacing a stuck ticket instead of overwriting it. Accepts a
    pool or a live Connection (the finalize block fires this in its transaction)."""
    await _guarded_state_update(
        pool,
        work_ticket_idx,
        set_clause="state = $1::qiita.work_ticket_state",
        set_params=[new.value],
        allowed_states=[expected.value],
        action=f"transition to {new.value!r}",
    )


async def _transition_to_processing_for_resume(pool: asyncpg.Pool, work_ticket_idx: int) -> None:
    """Move a ticket to PROCESSING from any non-terminal state, for startup
    recovery re-driving an in-flight ticket. Unlike `_atomic_transition`
    (single expected state), this accepts PENDING / QUEUED / PROCESSING so
    recovery doesn't need to know exactly where the crash left it; a
    PROCESSING → PROCESSING is a harmless no-op. Raises on a terminal ticket
    — recovery should never be handed one."""
    await _guarded_state_update(
        pool,
        work_ticket_idx,
        set_clause="state = $1::qiita.work_ticket_state",
        set_params=[WorkTicketState.PROCESSING.value],
        allowed_states=_NON_TERMINAL_STATES,
        action="resume to processing",
    )


async def _retry_count(pool: asyncpg.Pool, work_ticket_idx: int) -> int:
    """Read the current retry_count. Used by the retry loop to compare
    against max_retries before requeuing."""
    return await pool.fetchval(
        "SELECT retry_count FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )


async def _bump_retry_and_requeue(pool: asyncpg.Pool, work_ticket_idx: int) -> None:
    """Atomic PROCESSING → QUEUED transition with retry_count + 1. Single
    UPDATE so monitoring queries always see a coherent (state, count)
    pair; an observer that reads after this commit sees QUEUED with the
    bumped count, never PROCESSING with the bumped count or QUEUED with
    the old count."""
    await _guarded_state_update(
        pool,
        work_ticket_idx,
        set_clause="state = $1::qiita.work_ticket_state, retry_count = retry_count + 1",
        set_params=[WorkTicketState.QUEUED.value],
        allowed_states=[WorkTicketState.PROCESSING.value],
        action="bump retry on",
    )


async def _transition_to_failed(
    pool: asyncpg.Pool,
    work_ticket_idx: int,
    *,
    failure_type: FailureType,
    failure_stage: WorkTicketFailureStage,
    failure_step_name: str | None,
    failure_reason: str,
) -> None:
    """Atomic transition into FAILED with all four failure_* columns
    populated in one UPDATE. The DB's `work_ticket_failure_consistent`
    CHECK enforces all-or-nothing; doing it in one statement keeps that
    invariant honoured.

    Accepts transition from any non-terminal state — the runner may be
    in PROCESSING (most common) or QUEUED (if a retry's QUEUED → PROCESSING
    transition raced with shutdown). Refuses already-terminal tickets so
    a buggy second call doesn't overwrite a COMPLETED state.

    A genuine failure ends any in-place-retry episode: the transient marker is
    cleared so the FAILED ticket shows only its real failure surface, not a
    stale "stuck retrying" reason."""
    await _guarded_state_update(
        pool,
        work_ticket_idx,
        set_clause=(
            "state = $1::qiita.work_ticket_state,"
            " failure_type = $2::qiita.failure_type,"
            " failure_stage = $3::qiita.work_ticket_failure_stage,"
            " failure_step_name = $4,"
            " failure_reason = $5,"
            " transient_reason = NULL,"
            " transient_since = NULL"
        ),
        set_params=[
            WorkTicketState.FAILED.value,
            failure_type.value,
            failure_stage.value,
            failure_step_name,
            failure_reason,
        ],
        allowed_states=_NON_TERMINAL_STATES,
        action="mark FAILED",
    )


async def _transition_to_no_data(pool: asyncpg.Pool, work_ticket_idx: int) -> None:
    """Atomic transition into NO_DATA — the terminal outcome for a step that
    legitimately produced no data (an empty FASTQ well).

    Distinct from `_transition_to_failed`: NO_DATA is not a failure, so all four
    failure_* columns are explicitly written NULL (honouring the DB's
    `work_ticket_failure_consistent` all-or-nothing CHECK from the
    none-populated side) and the transient-retry marker is cleared. Accepts a
    transition from any non-terminal state (PROCESSING most commonly, or QUEUED
    if a retry's requeue raced shutdown); refuses an already-terminal ticket so
    a buggy second call can't overwrite a COMPLETED/FAILED state."""
    await _guarded_state_update(
        pool,
        work_ticket_idx,
        set_clause=(
            "state = $1::qiita.work_ticket_state,"
            " failure_type = NULL,"
            " failure_stage = NULL,"
            " failure_step_name = NULL,"
            " failure_reason = NULL,"
            " transient_reason = NULL,"
            " transient_since = NULL"
        ),
        set_params=[WorkTicketState.NO_DATA.value],
        allowed_states=_NON_TERMINAL_STATES,
        action="mark NO_DATA",
    )


def _safe_entry_name(action: ActionDefinition | None, index: int | None) -> str | None:
    """Best-effort lookup of the entry name at `index`. Returns None if
    `action` is unresolved (a pre-loop failure never fetched it) or the index
    is out of range (e.g. action.steps is empty so the loop never iterated).
    When the loop body has executed at least once, `index` is the most recent
    entry — the natural name to record on failure."""
    if action is None or index is None:
        return None
    if 0 <= index < len(action.steps):
        return action.steps[index].name
    return None
