"""Workflow runner — walks an action's `steps` list for one work ticket.

Step entries dispatch to the local compute backend; action entries
dispatch via the ControlPlaneClient (HTTP). Status transitions are
declared per-entry in the YAML and PATCHed before each entry that
declares one. Workflow-level success/failure transitions wrap the run.

Trigger surface for v1 is a plain async function. Tests invoke it
directly.

Workspace contract: every entry that needs a file consumes a path in
`<workspace_root>/<work_ticket_idx>/`. Action library functions write
their outputs (e.g. `feature_map.parquet`) into the same workspace, so
later entries see them via the runner's binding map.

DB access: direct asyncpg pool. The orchestrator and control-plane
share the qiita database in dev / current production layouts.
"""

import json
import logging
from pathlib import Path
from typing import Any

import asyncpg
from qiita_common.actions import ActionDefinition, WorkflowAction, WorkflowStep
from qiita_common.api_paths import LibraryPrimitive
from qiita_common.client import ControlPlaneClient
from qiita_common.models import ScopeTargetKind, WorkTicketState

from .backend import ComputeBackend

_log = logging.getLogger(__name__)

DEFAULT_WORKSPACE_ROOT = Path("/data/workspace")


async def run_workflow(
    work_ticket_idx: int,
    backend: ComputeBackend,
    client: ControlPlaneClient,
    pool: asyncpg.Pool,
    *,
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
          (orchestrator crashed mid-run) requires operator recovery —
          the runner refuses to silently re-run.
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
        expected=WorkTicketState.PENDING.value,
        new=WorkTicketState.PROCESSING.value,
    )

    workspace = workspace_root / str(work_ticket_idx)
    workspace.mkdir(parents=True, exist_ok=True)

    bound: dict[str, Any] = dict(work_ticket["action_context"] or {})
    scope_target = _build_scope_target(work_ticket)
    current_status: str | None = None

    _log.info(
        "running workflow %s/%s for work_ticket %d",
        action.action_id,
        action.version,
        work_ticket_idx,
    )

    try:
        for index, entry in enumerate(action.steps):
            if entry.target_status and entry.target_status != current_status:
                await _patch_resource_status(client, scope_target, entry.target_status)
                current_status = entry.target_status

            if isinstance(entry, WorkflowStep):
                outputs = await _dispatch_step(backend, entry, bound, workspace, scope_target)
            elif isinstance(entry, WorkflowAction):
                outputs = await _dispatch_action(client, entry, bound, workspace, scope_target)
            else:
                # WorkflowEntry is a closed union; the discriminator on
                # ActionDefinition guarantees one of the two arms above.
                raise TypeError(f"unexpected entry type at index {index}: {type(entry)!r}")
            bound.update(outputs)

        if action.success_status:
            await _patch_resource_status(client, scope_target, action.success_status)
        await _atomic_transition(
            pool,
            work_ticket_idx,
            expected=WorkTicketState.PROCESSING.value,
            new=WorkTicketState.COMPLETED.value,
        )
        _log.info("workflow %d completed", work_ticket_idx)
    except Exception as exc:
        _log.exception("workflow %d failed", work_ticket_idx)
        if action.failure_status:
            try:
                await _patch_resource_status(client, scope_target, action.failure_status)
            except Exception:
                _log.exception(
                    "best-effort failure_status PATCH for work_ticket %d failed",
                    work_ticket_idx,
                )
        await _atomic_transition(
            pool,
            work_ticket_idx,
            expected=WorkTicketState.PROCESSING.value,
            new=WorkTicketState.FAILED.value,
        )
        raise exc


# =============================================================================
# DB access helpers
# =============================================================================


_WORK_TICKET_COLS = (
    "work_ticket_idx, action_id, action_version, originator_principal_idx, "
    "scope_target_kind, study_idx, prep_idx, reference_idx, "
    "action_context, state"
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
    expected: str,
    new: str,
) -> None:
    """UPDATE state with a TOCTOU-safe WHERE clause. Raises if the row
    isn't in the expected state — surfacing a stuck PROCESSING ticket
    instead of silently overwriting it."""
    updated = await pool.fetchval(
        "UPDATE qiita.work_ticket SET state = $1::qiita.work_ticket_state "
        "WHERE work_ticket_idx = $2 AND state = $3::qiita.work_ticket_state "
        "RETURNING work_ticket_idx",
        new,
        work_ticket_idx,
        expected,
    )
    if updated is None:
        actual = await pool.fetchval(
            "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
            work_ticket_idx,
        )
        raise RuntimeError(
            f"could not transition work_ticket {work_ticket_idx} "
            f"from {expected!r} to {new!r}; actual state {actual!r}"
        )


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
    client: ControlPlaneClient, scope_target: dict[str, Any], target_status: str
) -> None:
    """Drive the appropriate resource-status PATCH for the scope_target.
    Today only `reference` is wired."""
    if scope_target["kind"] == ScopeTargetKind.REFERENCE.value:
        await client.update_reference_status(scope_target["reference_idx"], target_status)
        return
    raise NotImplementedError(
        f"status PATCH for scope_target.kind={scope_target['kind']!r} not yet wired"
    )


async def _dispatch_step(
    backend: ComputeBackend,
    entry: WorkflowStep,
    bound: dict[str, Any],
    workspace: Path,
    scope_target: dict[str, Any],
) -> dict[str, Any]:
    """Translate the YAML-declared input names into Path arguments and
    call backend.run_step; record outputs under the YAML's declared
    names so subsequent entries can reference them."""
    inputs = {name: Path(bound[name]) for name in entry.inputs}
    # Backend steps that need a reference_idx (today: hash, load) only run
    # under reference-scoped tickets. Refuse to silently substitute a 0
    # for non-reference scope_targets — fail-fast tells the operator the
    # workflow YAML and the ticket scope are mismatched.
    if scope_target["kind"] != ScopeTargetKind.REFERENCE.value:
        raise RuntimeError(
            f"backend step {entry.name!r} requires a reference scope_target; "
            f"got kind={scope_target['kind']!r}"
        )
    reference_idx = scope_target["reference_idx"]
    raw_outputs = await backend.run_step(entry.name, inputs, workspace, reference_idx=reference_idx)
    # Convention: the backend's output dict keys match the YAML's
    # `outputs:` names exactly. A mismatch is a workflow authoring
    # error and surfaces here as a KeyError.
    return {name: raw_outputs[name] for name in entry.outputs}


async def _dispatch_action(
    client: ControlPlaneClient,
    entry: WorkflowAction,
    bound: dict[str, Any],
    workspace: Path,
    scope_target: dict[str, Any],
) -> dict[str, Any]:
    """Translate a workflow `action:` entry into the matching
    ControlPlaneClient call. Per-primitive logic lives here because each
    primitive has its own input/output shape — a generic dispatcher
    would just push the same `if name == ...` ladder somewhere else."""
    if entry.name == LibraryPrimitive.MINT_FEATURES:
        manifest_path = Path(bound[entry.inputs[0]])
        # `genome_map_path` is a workflow-context optional, not an entry
        # input — the YAML's mint-features `inputs:` stays single-valued.
        # Pulled directly from `bound` so a ticket whose action_context
        # carries it picks up genome-association writes for free.
        genome_map = bound.get("genome_map_path")
        resp = await client.mint_features(
            reference_idx=scope_target["reference_idx"],
            manifest_path=manifest_path,
            output_dir=workspace,
            genome_map_path=Path(genome_map) if genome_map else None,
        )
        # YAML declares one output (typically "feature_map"); bind it.
        return {entry.outputs[0]: Path(resp.feature_map_path)}

    if entry.name == LibraryPrimitive.WRITE_MEMBERSHIP:
        feature_map_path = Path(bound[entry.inputs[0]])
        await client.write_membership(
            reference_idx=scope_target["reference_idx"],
            feature_map_path=feature_map_path,
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
        await client.register_files(
            reference_idx=scope_target["reference_idx"],
            staging_dir=str(staging_dir),
            files=files,
        )
        return {}

    raise RuntimeError(f"runner has no adapter for action {entry.name!r}")
