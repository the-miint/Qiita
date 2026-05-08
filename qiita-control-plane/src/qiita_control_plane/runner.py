"""Workflow runner — walks an action's `steps` list for one work ticket.

`step:` entries dispatch to the orchestrator via ComputeBackendClient
(HTTP). `action:` entries dispatch to LIBRARY in-process — no HTTP hop.
Status transitions declared in YAML are PATCHed before each entry that
declares one. Workflow-level success/failure transitions wrap the run.

Lives in the control plane: direct DB access for work_ticket / action /
reference rows is legitimate here. The orchestrator is reduced to its
SLURM-driver role behind `POST /step/run`.

Workspace contract: every entry that needs a file consumes a path in
`<workspace_root>/<work_ticket_idx>/`. LIBRARY primitives write their
outputs (e.g. `feature_map.parquet`) into the same workspace, so later
entries see them via the runner's binding map.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import asyncpg
from qiita_common.actions import ActionDefinition, WorkflowAction, WorkflowStep
from qiita_common.api_paths import LibraryPrimitive
from qiita_common.backend_failure import BackendFailure
from qiita_common.compute_backend_client import ComputeBackendClient
from qiita_common.models import (
    FailureType,
    ReferenceStatus,
    ScopeTargetKind,
    WorkTicketFailureStage,
    WorkTicketState,
)

from .actions.library import LIBRARY
from .actions.reference import transition_reference_status

_log = logging.getLogger(__name__)

DEFAULT_WORKSPACE_ROOT = Path("/data/workspace")


async def run_workflow(
    work_ticket_idx: int,
    pool: asyncpg.Pool,
    backend_client: ComputeBackendClient,
    *,
    hmac_secret: bytes,
    data_plane_url: str,
    workspace_root: Path = DEFAULT_WORKSPACE_ROOT,
) -> None:
    """Execute the workflow attached to one work ticket.

    Reads the ticket and its action from the DB, transitions PENDING →
    PROCESSING, walks each entry in ``action.steps``, and finishes by
    transitioning PROCESSING → COMPLETED. Any unhandled exception
    transitions the ticket to FAILED, best-effort PATCHes the resource
    to ``action.failure_status``, and re-raises.

    Pre-conditions:
        * Ticket must be in 'pending' state. A leftover PROCESSING
          (runner crashed mid-run) requires operator recovery — the
          runner refuses to silently re-run.
        * Action ``(action_id, version)`` must exist in qiita.action
          with ``enabled=true``.
    """
    work_ticket = await _fetch_work_ticket(pool, work_ticket_idx)
    if work_ticket["state"] != WorkTicketState.PENDING.value:
        raise RuntimeError(
            f"work_ticket {work_ticket_idx} is in state {work_ticket['state']!r}, "
            f"must be {WorkTicketState.PENDING.value!r}; manual recovery required"
        )

    action = await _fetch_action(pool, work_ticket["action_id"], work_ticket["action_version"])
    if action is None:
        raise RuntimeError(
            f"action ({work_ticket['action_id']!r}, "
            f"{work_ticket['action_version']!r}) not found or disabled"
        )

    await _atomic_transition(
        pool,
        work_ticket_idx,
        expected=WorkTicketState.PENDING,
        new=WorkTicketState.PROCESSING,
    )

    workspace = workspace_root / str(work_ticket_idx)
    workspace.mkdir(parents=True, exist_ok=True)

    bound: dict[str, Any] = dict(work_ticket["action_context"] or {})
    scope_target = _build_scope_target(work_ticket)
    current_status: str | None = None
    max_retries: int = work_ticket["max_retries"]

    _log.info(
        "running workflow %s/%s for work_ticket %d (max_retries=%d)",
        action.action_id,
        action.version,
        work_ticket_idx,
        max_retries,
    )

    try:
        for index, entry in enumerate(action.steps):
            if entry.target_status and entry.target_status != current_status:
                await _patch_resource_status(pool, scope_target, entry.target_status)
                current_status = entry.target_status

            outputs = await _run_entry_with_retry(
                pool=pool,
                work_ticket_idx=work_ticket_idx,
                index=index,
                entry=entry,
                bound=bound,
                workspace=workspace,
                scope_target=scope_target,
                backend_client=backend_client,
                hmac_secret=hmac_secret,
                data_plane_url=data_plane_url,
                max_retries=max_retries,
            )
            bound.update(outputs)

        if action.success_status:
            await _patch_resource_status(pool, scope_target, action.success_status)
        await _atomic_transition(
            pool,
            work_ticket_idx,
            expected=WorkTicketState.PROCESSING,
            new=WorkTicketState.COMPLETED,
        )
        _log.info("workflow %d completed", work_ticket_idx)
    except BackendFailure as exc:
        # Retry-loop already exhausted retries (transient) or this was a
        # permanent failure. The retry loop has not yet transitioned the
        # ticket — we own that transition here so failure_status PATCH
        # and the FAILED row insert happen together.
        _log.warning("workflow %d failed: %s", work_ticket_idx, exc)
        if action.failure_status:
            try:
                await _patch_resource_status(pool, scope_target, action.failure_status)
            except Exception:
                _log.exception(
                    "best-effort failure_status PATCH for work_ticket %d failed",
                    work_ticket_idx,
                )
        await _transition_to_failed(
            pool,
            work_ticket_idx,
            failure_type=(FailureType.RETRIABLE if exc.transient else FailureType.PERMANENT),
            failure_stage=exc.stage,
            failure_step_name=exc.step_name,
            failure_reason=exc.reason,
        )
        raise
    except Exception as exc:
        # Anything not a BackendFailure is either a programming bug or
        # an in-process LIBRARY primitive raising plain Python. We treat
        # both as permanent UNKNOWN_PERMANENT — there's no classification
        # to retry against, and re-running an in-process Python failure
        # produces the same error.
        _log.exception("workflow %d failed (unwrapped exception)", work_ticket_idx)
        if action.failure_status:
            try:
                await _patch_resource_status(pool, scope_target, action.failure_status)
            except Exception:
                _log.exception(
                    "best-effort failure_status PATCH for work_ticket %d failed",
                    work_ticket_idx,
                )
        await _transition_to_failed(
            pool,
            work_ticket_idx,
            failure_type=FailureType.PERMANENT,
            failure_stage=WorkTicketFailureStage.STEP_RUN,
            # Best-effort: if the exception fires before the loop body,
            # 'index' isn't defined; guard by name lookup.
            failure_step_name=_safe_entry_name(action, locals().get("index")),
            failure_reason=f"{type(exc).__name__}: {exc!s}"[:2000],
        )
        raise


async def _run_entry_with_retry(
    *,
    pool: asyncpg.Pool,
    work_ticket_idx: int,
    index: int,
    entry: WorkflowStep | WorkflowAction,
    bound: dict[str, Any],
    workspace: Path,
    scope_target: dict[str, Any],
    backend_client: ComputeBackendClient,
    hmac_secret: bytes,
    data_plane_url: str,
    max_retries: int,
) -> dict[str, Any]:
    """Dispatch one workflow entry, with auto-retry on transient
    `BackendFailure`. Returns the entry's output map on success; raises
    `BackendFailure` on permanent failure or once retry budget is
    exhausted.

    Retry semantics:
      * On `BackendFailure(transient=True)` and retry_count < max_retries:
        increment retry_count, transition PROCESSING → QUEUED → PROCESSING
        atomically, retry the same step. Earlier successful entries are
        not re-run — `bound` carries their outputs forward.
      * On permanent failure or retry_count >= max_retries: re-raise so
        the outer handler in `run_workflow` writes the failure_* columns
        and transitions to FAILED.

    The state churn (PROCESSING → QUEUED → PROCESSING) is observable to
    monitoring queries: a ticket bouncing through QUEUED indicates a
    retry attempt.
    """
    while True:
        try:
            if isinstance(entry, WorkflowStep):
                return await _dispatch_step(backend_client, entry, bound, workspace, scope_target)
            if isinstance(entry, WorkflowAction):
                return await _dispatch_action(
                    pool,
                    entry,
                    bound,
                    workspace,
                    scope_target,
                    hmac_secret=hmac_secret,
                    data_plane_url=data_plane_url,
                )
            # WorkflowEntry is a closed union; the discriminator on
            # ActionDefinition guarantees one of the two arms above.
            raise TypeError(f"unexpected entry type at index {index}: {type(entry)!r}")
        except BackendFailure as exc:
            if not exc.transient:
                raise
            current_retry = await _retry_count(pool, work_ticket_idx)
            if current_retry >= max_retries:
                _log.warning(
                    "work_ticket %d step %r exhausted retries (%d/%d); failing",
                    work_ticket_idx,
                    entry.name,
                    current_retry,
                    max_retries,
                )
                raise
            _log.warning(
                "work_ticket %d step %r transient failure (%s); retrying %d/%d",
                work_ticket_idx,
                entry.name,
                exc.kind.value,
                current_retry + 1,
                max_retries,
            )
            await _bump_retry_and_requeue(pool, work_ticket_idx)
            await _atomic_transition(
                pool,
                work_ticket_idx,
                expected=WorkTicketState.QUEUED,
                new=WorkTicketState.PROCESSING,
            )


# =============================================================================
# DB access helpers
# =============================================================================


_WORK_TICKET_COLS = (
    "work_ticket_idx, action_id, action_version, originator_principal_idx, "
    "scope_target_kind, study_idx, prep_idx, reference_idx, "
    "action_context, state, retry_count, max_retries"
)

_ACTION_COLS = (
    "action_id, version, target_kind, description, "
    "scopes, audience, context_schema, steps, "
    "cpu_ceiling, mem_ceiling_gb, walltime_ceiling, gpu_ceiling, "
    "success_status, failure_status"
)


async def _fetch_work_ticket(pool: asyncpg.Pool, work_ticket_idx: int) -> dict[str, Any]:
    row = await pool.fetchrow(
        f"SELECT {_WORK_TICKET_COLS} FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    if row is None:
        raise RuntimeError(f"work_ticket {work_ticket_idx} not found")
    out = dict(row)
    # action_context is JSONB — asyncpg returns it as a JSON string by
    # default; parse it eagerly so the runner can index into it.
    if out.get("action_context") is not None and isinstance(out["action_context"], str):
        out["action_context"] = json.loads(out["action_context"])
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


async def _atomic_transition(
    pool: asyncpg.Pool,
    work_ticket_idx: int,
    *,
    expected: WorkTicketState,
    new: WorkTicketState,
) -> None:
    """UPDATE state with a TOCTOU-safe WHERE clause. Raises if the row
    isn't in the expected state — surfacing a stuck PROCESSING ticket
    instead of silently overwriting it."""
    updated = await pool.fetchval(
        "UPDATE qiita.work_ticket SET state = $1::qiita.work_ticket_state "
        "WHERE work_ticket_idx = $2 AND state = $3::qiita.work_ticket_state "
        "RETURNING work_ticket_idx",
        new.value,
        work_ticket_idx,
        expected.value,
    )
    if updated is None:
        actual = await pool.fetchval(
            "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
            work_ticket_idx,
        )
        raise RuntimeError(
            f"could not transition work_ticket {work_ticket_idx} "
            f"from {expected.value!r} to {new.value!r}; actual state {actual!r}"
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
    updated = await pool.fetchval(
        "UPDATE qiita.work_ticket"
        " SET state = $1::qiita.work_ticket_state,"
        "     retry_count = retry_count + 1"
        " WHERE work_ticket_idx = $2"
        "   AND state = $3::qiita.work_ticket_state"
        " RETURNING work_ticket_idx",
        WorkTicketState.QUEUED.value,
        work_ticket_idx,
        WorkTicketState.PROCESSING.value,
    )
    if updated is None:
        actual = await pool.fetchval(
            "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
            work_ticket_idx,
        )
        raise RuntimeError(
            f"could not bump retry on work_ticket {work_ticket_idx}: "
            f"expected processing, got {actual!r}"
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
    a buggy second call doesn't overwrite a COMPLETED state."""
    updated = await pool.fetchval(
        "UPDATE qiita.work_ticket"
        " SET state = $1::qiita.work_ticket_state,"
        "     failure_type = $2::qiita.failure_type,"
        "     failure_stage = $3::qiita.work_ticket_failure_stage,"
        "     failure_step_name = $4,"
        "     failure_reason = $5"
        " WHERE work_ticket_idx = $6"
        "   AND state = ANY($7::qiita.work_ticket_state[])"
        " RETURNING work_ticket_idx",
        WorkTicketState.FAILED.value,
        failure_type.value,
        failure_stage.value,
        failure_step_name,
        failure_reason,
        work_ticket_idx,
        [
            WorkTicketState.PENDING.value,
            WorkTicketState.QUEUED.value,
            WorkTicketState.PROCESSING.value,
        ],
    )
    if updated is None:
        actual = await pool.fetchval(
            "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
            work_ticket_idx,
        )
        raise RuntimeError(
            f"could not mark work_ticket {work_ticket_idx} FAILED: "
            f"expected non-terminal, got {actual!r}"
        )


def _safe_entry_name(action: ActionDefinition, index: int | None) -> str | None:
    """Best-effort lookup of the entry name at `index`. Returns None if
    the index is out of range (e.g. an exception fired before the loop
    body started). The DB CHECK requires step_name to be NULL when
    failure_stage is not STEP_RUN, but we always pass STEP_RUN here —
    keep the column populated when we can."""
    if index is None:
        return None
    if 0 <= index < len(action.steps):
        return action.steps[index].name
    return None


# =============================================================================
# Dispatch helpers
# =============================================================================


def _build_scope_target(work_ticket: dict[str, Any]) -> dict[str, Any]:
    """Reconstruct a {kind, ...idx fields} dict matching qiita_common's
    ScopeTarget tagged-union shape from the work_ticket row."""
    kind = work_ticket["scope_target_kind"]
    if kind == ScopeTargetKind.REFERENCE.value:
        return {
            "kind": ScopeTargetKind.REFERENCE.value,
            "reference_idx": work_ticket["reference_idx"],
        }
    if kind == ScopeTargetKind.STUDY_PREP.value:
        return {
            "kind": ScopeTargetKind.STUDY_PREP.value,
            "study_idx": work_ticket["study_idx"],
            "prep_idx": work_ticket["prep_idx"],
        }
    raise RuntimeError(f"unknown scope_target_kind: {kind!r}")


async def _patch_resource_status(
    pool: asyncpg.Pool, scope_target: dict[str, Any], target_status: str
) -> None:
    """Drive the appropriate resource-status transition for the scope_target.
    Today only `reference` is wired."""
    if scope_target["kind"] == ScopeTargetKind.REFERENCE.value:
        await transition_reference_status(
            pool, scope_target["reference_idx"], ReferenceStatus(target_status)
        )
        return
    raise NotImplementedError(
        f"status transition for scope_target.kind={scope_target['kind']!r} not yet wired"
    )


async def _dispatch_step(
    backend_client: ComputeBackendClient,
    entry: WorkflowStep,
    bound: dict[str, Any],
    workspace: Path,
    scope_target: dict[str, Any],
) -> dict[str, Any]:
    """Translate the YAML-declared input names into Path arguments and
    call the orchestrator's /step/run endpoint; record outputs under the
    YAML's declared names so subsequent entries can reference them.

    `optional_inputs` flow through if present in the binding map; missing
    ones are simply omitted from the dispatch payload (the backend's
    step handler decides what to do without them)."""
    inputs = {name: Path(bound[name]) for name in entry.inputs}
    inputs.update({name: Path(bound[name]) for name in entry.optional_inputs if name in bound})
    # Steps that need a reference_idx (today: hash, load) only run under
    # reference-scoped tickets. Refuse to silently substitute a 0 for
    # non-reference scope_targets — fail-fast tells the operator the
    # workflow YAML and the ticket scope are mismatched.
    if scope_target["kind"] != ScopeTargetKind.REFERENCE.value:
        raise RuntimeError(
            f"backend step {entry.name!r} requires a reference scope_target; "
            f"got kind={scope_target['kind']!r}"
        )
    raw_outputs = await backend_client.run_step(
        step_name=entry.name,
        inputs=inputs,
        workspace=workspace,
        reference_idx=scope_target["reference_idx"],
    )
    # Convention: the orchestrator's output dict keys match the YAML's
    # `outputs:` names exactly. A mismatch is a workflow authoring
    # error and surfaces here as a KeyError.
    return {name: raw_outputs[name] for name in entry.outputs}


async def _dispatch_action(
    pool: asyncpg.Pool,
    entry: WorkflowAction,
    bound: dict[str, Any],
    workspace: Path,
    scope_target: dict[str, Any],
    *,
    hmac_secret: bytes,
    data_plane_url: str,
) -> dict[str, Any]:
    """Translate a workflow `action:` entry into the matching LIBRARY call.
    Per-primitive logic lives here because each primitive has its own
    input/output shape — a generic dispatcher would just push the same
    `if name == ...` ladder somewhere else."""
    if entry.name == LibraryPrimitive.MINT_FEATURES:
        manifest_path = Path(bound[entry.inputs[0]])
        # `genome_map_path` is a workflow-context optional, not an entry
        # input — the YAML's mint-features `inputs:` stays single-valued.
        # Pulled directly from `bound` so a ticket whose action_context
        # carries it picks up genome-association writes for free.
        genome_map = bound.get("genome_map_path")
        feature_map_path, _, _ = await LIBRARY[LibraryPrimitive.MINT_FEATURES](
            pool,
            manifest_path,
            workspace,
            genome_map_path=Path(genome_map) if genome_map else None,
        )
        # YAML declares one output (typically "feature_map"); bind it.
        return {entry.outputs[0]: feature_map_path}

    if entry.name == LibraryPrimitive.WRITE_MEMBERSHIP:
        feature_map_path = Path(bound[entry.inputs[0]])
        await LIBRARY[LibraryPrimitive.WRITE_MEMBERSHIP](
            pool, scope_target["reference_idx"], feature_map_path
        )
        return {}

    if entry.name == LibraryPrimitive.REGISTER_FILES:
        staging_dir = Path(bound[entry.inputs[0]])
        # Filename → DuckLake table mapping derived from the staging dir.
        # Convention: every *.parquet file in the dir gets registered to
        # a table whose name matches the filename stem. Workflows that
        # want a different mapping should declare it in YAML; today the
        # only caller (reference-add) follows the convention.
        files = {p.name: p.stem for p in sorted(staging_dir.glob("*.parquet"))}
        if not files:
            raise RuntimeError(
                f"register-files: staging_dir {staging_dir} contains no Parquet files"
            )
        await LIBRARY[LibraryPrimitive.REGISTER_FILES](
            staging_dir=str(staging_dir),
            files=files,
            hmac_secret=hmac_secret,
            data_plane_url=data_plane_url,
        )
        return {}

    raise RuntimeError(f"runner has no adapter for action {entry.name!r}")
