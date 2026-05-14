"""Integration smoke test: drive a native-step workflow through the
control-plane runner with a real LocalBackend.

What's exercised end-to-end:
  - Inline ActionDefinition with `module:` set → sync into qiita.action
  - Runner reads the action row, walks the single step entry
  - Runner forwards module= to LocalComputeBackendClient.run_step
  - LocalBackend delegates to run_native_job
  - run_native_job imports qiita_compute_orchestrator.jobs.fastq_to_parquet
  - The skeleton's execute() raises NotImplementedError
  - run_native_job translates that to BackendFailure(UNKNOWN_PERMANENT)
  - Runner transitions the work_ticket to FAILED with PERMANENT
    failure_type and the step's name on failure_step_name

The skeleton's value here is the dispatch path, not the work it'll
eventually do. When fastq_to_parquet gets a real implementation, this
test gets renamed and the assertions move from "failure mentions not
implemented" to whatever success shape the new implementation provides.

The ActionDefinition is constructed inline rather than loaded from
workflows/ — the skeleton would be premature to ship as a deployable
YAML (it'd accept submissions that always fail). When the conversion
lands, a workflow YAML returns to workflows/ and this test can switch
to loading from there.
"""

from __future__ import annotations

import json
import uuid
from datetime import timedelta

import pytest
from qiita_common.actions import (
    ActionCeiling,
    ActionDefinition,
    Audience,
)
from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models import (
    FailureType,
    ScopeTargetKind,
    StepType,
    WorkTicketFailureStage,
    WorkTicketState,
)

from _runner_helpers import LocalComputeBackendClient


def _build_native_skeleton_action(*, action_id: str, version: str) -> ActionDefinition:
    """Construct an ActionDefinition whose single step targets the
    fastq_to_parquet native skeleton. Mirrors the shape a deployable
    YAML would carry once the conversion is implemented."""
    return ActionDefinition(
        action_id=action_id,
        version=version,
        target_kind=ScopeTargetKind.REFERENCE,
        scopes=[],
        audience=Audience(service=False, human_roles=["system_admin"]),
        context_schema={
            "type": "object",
            "required": ["fastq_path"],
            "properties": {"fastq_path": {"type": "string"}},
        },
        steps=[
            {
                "kind": "step",
                "name": "fastq",
                "step_type": StepType.SINGLETON,
                "module": "qiita_compute_orchestrator.jobs.fastq_to_parquet",
                "inputs": ["fastq_path"],
                "outputs": [],
                "baseline_resources": {
                    "cpu": 2,
                    "mem_gb": 4,
                    "walltime": timedelta(minutes=30),
                },
            }
        ],
        action_ceiling=ActionCeiling(
            cpu=8, mem_gb=16, walltime=timedelta(hours=2), gpu=0
        ),
    )


@pytest.fixture
async def native_skeleton_action(postgres_pool):
    """Sync an inline ActionDefinition for the skeleton. Each test run
    uses a unique version suffix so parallel pytest-xdist workers
    don't collide on the same (action_id, version) primary key."""
    from qiita_control_plane.actions import sync_actions

    action_id = "fastq-to-parquet"
    version = f"smoke-{uuid.uuid4()}"
    action = _build_native_skeleton_action(action_id=action_id, version=version)

    async with postgres_pool.acquire() as conn:
        await sync_actions(conn, [action])

    yield (action_id, version)

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        action_id,
        version,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
        action_id,
        version,
    )


@pytest.fixture
async def smoke_reference(postgres_pool, human_admin_session):
    """A bare reference for the smoke run. The skeleton fails before it
    consumes any reference attributes, so the row carries the minimum
    columns required by the schema."""
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', 'pending', $2)"
        " RETURNING reference_idx",
        f"native-smoke-{uuid.uuid4()}",
        human_admin_session["principal_idx"],
    )
    yield idx
    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE reference_idx = $1", idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = $1", idx
    )


async def test_native_step_skeleton_fails_through_runner(
    postgres_pool,
    native_skeleton_action,
    smoke_reference,
    human_admin_session,
    tmp_path,
):
    """End-to-end: a native-step ticket reaches FAILED with a typed,
    permanent failure when the skeleton's execute() raises
    NotImplementedError.

    Asserts every layer of the dispatch path actually ran:
    - Runner walked into the single step entry.
    - LocalComputeBackendClient.run_step received module=.
    - LocalBackend delegated to run_native_job.
    - run_native_job imported the skeleton and translated
      NotImplementedError → BackendFailure(UNKNOWN_PERMANENT).
    - Runner transitioned the ticket to FAILED + PERMANENT and
      stamped the failure_* columns.
    """
    from qiita_control_plane.runner import run_workflow

    action_id, action_version = native_skeleton_action
    reference_idx = smoke_reference

    fastq = tmp_path / "input.fastq"
    fastq.write_bytes(b"@read1\nACGT\n+\n!!!!\n")

    work_ticket_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, action_context"
        ") VALUES ($1, $2, $3, 'reference', $4, $5::jsonb)"
        " RETURNING work_ticket_idx",
        action_id,
        action_version,
        human_admin_session["principal_idx"],
        reference_idx,
        json.dumps({"fastq_path": str(fastq)}),
    )

    workspace_root = tmp_path / "workspace"
    backend_client = LocalComputeBackendClient()

    # The runner re-raises BackendFailure after transitioning the ticket
    # to FAILED; assert both the exception type and the DB transition.
    with pytest.raises(BackendFailure) as ei:
        await run_workflow(
            work_ticket_idx,
            postgres_pool,
            backend_client,  # type: ignore[arg-type]  # protocol-shaped duck
            hmac_secret=b"unused-in-smoke",
            data_plane_url="grpc://unused:0",
            workspace_root=workspace_root,
        )
    assert ei.value.kind is FailureKind.UNKNOWN_PERMANENT
    assert "fastq_to_parquet" in ei.value.reason
    assert "not implemented" in ei.value.reason.lower()

    row = await postgres_pool.fetchrow(
        "SELECT state, failure_type, failure_stage, failure_step_name, failure_reason"
        " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert row["state"] == WorkTicketState.FAILED.value
    assert row["failure_type"] == FailureType.PERMANENT.value
    assert row["failure_stage"] == WorkTicketFailureStage.STEP_RUN.value
    # `failure_step_name` carries the YAML step name (per the migration's
    # documented contract). run_native_job plumbs it through; container
    # and native dispatch are uniform on this column.
    assert row["failure_step_name"] == "fastq"
    # The module path stays in the reason text for operator-side debugging.
    assert "fastq_to_parquet" in row["failure_reason"]
    assert "not implemented" in row["failure_reason"].lower()
