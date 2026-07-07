"""Runner DB-access helpers — work_ticket / action fetches and guarded state transitions."""

from __future__ import annotations

import json
from typing import Any

import asyncpg
from qiita_common.actions import (
    ActionDefinition,
)
from qiita_common.models import (
    FailureType,
    WorkTicketFailureStage,
    WorkTicketState,
)

# =============================================================================
# DB access helpers
# =============================================================================


# LEFT JOIN qiita.sequenced_pool so the SEQUENCED_POOL scope_target arm
# can carry the parent sequencing_run_idx — _build_scope_target reads it
# alongside sequenced_pool_idx to produce the {kind: sequenced_pool, ...}
# dict the orchestrator's SCOPE_SCALARS_BY_KIND injection consumes.
_WORK_TICKET_COLS = (
    "wt.work_ticket_idx, wt.action_id, wt.action_version, wt.originator_principal_idx, "
    "wt.scope_target_kind, wt.study_idx, wt.prep_idx, wt.reference_idx, "
    "wt.prep_sample_idx, wt.sequenced_pool_idx, sp.sequencing_run_idx, "
    "wt.block_idx, wt.mask_idx, "
    "wt.action_context, wt.state, wt.retry_count, wt.max_retries, "
    "wt.resource_override"
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
    # action_context is JSONB — asyncpg returns it as a JSON string by
    # default; parse it eagerly so the runner can index into it.
    if out.get("action_context") is not None and isinstance(out["action_context"], str):
        out["action_context"] = json.loads(out["action_context"])
    # resource_override is JSONB (nullable) — decode the same way so the runner
    # can read its mem_gb. NULL stays None (no override).
    if out.get("resource_override") is not None and isinstance(out["resource_override"], str):
        out["resource_override"] = json.loads(out["resource_override"])
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


# The non-terminal states a work_ticket may legitimately transition FROM.
# Shared by every guarded transition so the allowed-source set is defined once.
_NON_TERMINAL_STATES = [
    WorkTicketState.PENDING.value,
    WorkTicketState.QUEUED.value,
    WorkTicketState.PROCESSING.value,
]


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
