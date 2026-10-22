"""DB tests for the per-step progress spine (qiita.work_ticket_step).

`step_progress.py` records one row per (work_ticket_idx, step_index,
attempt) as the runner executes an action's `steps:` list. These tests
exercise the write-ahead writers and the recovery query helpers in
isolation against a real Postgres, without driving the full runner.
"""

import json
import uuid

import asyncpg
import pytest
from qiita_common.models import ComputeTarget, StepProgressState

from qiita_control_plane import step_progress

pytestmark = pytest.mark.db


async def _seed_work_ticket(pool) -> tuple[int, str, str, int]:
    """Insert the minimal reference + action + work_ticket needed to satisfy
    the work_ticket_step FK. Returns (work_ticket_idx, action_id, version,
    reference_idx) so the caller can tear them down."""
    ref_idx = await pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, is_host, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', true,"
        "         (SELECT MIN(idx) FROM qiita.principal)) RETURNING reference_idx",
        f"step-progress-{uuid.uuid4()}",
    )
    action_id = "step-progress-test-action"
    version = f"v-{uuid.uuid4()}"
    await pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, scopes, audience, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling"
        ") VALUES ($1, $2, 'reference', $3::text[], $4::jsonb, $5::jsonb,"
        "          1, 1, '1 minute')",
        action_id,
        version,
        ["reference:write"],
        json.dumps({"service": False, "human_roles": ["system_admin"]}),
        json.dumps([]),
    )
    wt_idx = await pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, state)"
        " VALUES ($1, $2, (SELECT MIN(idx) FROM qiita.principal),"
        "         'reference', $3, 'processing'::qiita.work_ticket_state)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        ref_idx,
    )
    return wt_idx, action_id, version, ref_idx


async def _cleanup(pool, wt_idx, action_id, version, ref_idx) -> None:
    # work_ticket_step rows cascade from the work_ticket delete.
    await pool.execute("DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", wt_idx)
    await pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, version
    )
    await pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", ref_idx)


async def _state_of(pool, wt_idx, step_index, attempt=0) -> str | None:
    return await pool.fetchval(
        "SELECT state FROM qiita.work_ticket_step"
        " WHERE work_ticket_idx = $1 AND step_index = $2 AND attempt = $3",
        wt_idx,
        step_index,
        attempt,
    )


# ---------------------------------------------------------------------------
# Write-ahead intent
# ---------------------------------------------------------------------------


async def test_record_submitting_creates_write_ahead_row(postgres_pool):
    """The progress row exists in 'submitting' *before* any submit fires,
    carrying the deterministic job name and no job id yet."""
    wt_idx, *rest = await _seed_work_ticket(postgres_pool)
    try:
        await step_progress.record_submitting(
            postgres_pool,
            work_ticket_idx=wt_idx,
            step_index=0,
            attempt=0,
            step_name="hash",
            compute_target=ComputeTarget.SLURM,
            job_name=f"qiita-wt{wt_idx}-hash-a0",
        )
        rows = await step_progress.load_step_progress(postgres_pool, wt_idx)
        assert len(rows) == 1
        row = rows[0]
        assert row.state is StepProgressState.SUBMITTING
        assert row.compute_target is ComputeTarget.SLURM
        assert row.slurm_job_id is None
        assert row.job_name == f"qiita-wt{wt_idx}-hash-a0"
        assert row.step_name == "hash"
    finally:
        await _cleanup(postgres_pool, wt_idx, *rest)


async def test_record_submitting_is_idempotent(postgres_pool):
    """Re-entry (recovery, retry of the same attempt) must not reset a row
    that already advanced past 'submitting'."""
    wt_idx, *rest = await _seed_work_ticket(postgres_pool)
    try:
        await step_progress.record_submitting(
            postgres_pool,
            work_ticket_idx=wt_idx,
            step_index=0,
            attempt=0,
            step_name="hash",
            compute_target=ComputeTarget.SLURM,
            job_name=f"qiita-wt{wt_idx}-hash-a0",
        )
        await step_progress.record_submitted(
            postgres_pool, work_ticket_idx=wt_idx, step_index=0, attempt=0, slurm_job_id=4242
        )
        # A second write-ahead for the same (idx, step, attempt) is a no-op.
        await step_progress.record_submitting(
            postgres_pool,
            work_ticket_idx=wt_idx,
            step_index=0,
            attempt=0,
            step_name="hash",
            compute_target=ComputeTarget.SLURM,
            job_name=f"qiita-wt{wt_idx}-hash-a0",
        )
        rows = await step_progress.load_step_progress(postgres_pool, wt_idx)
        assert len(rows) == 1
        assert rows[0].state is StepProgressState.SUBMITTED
        assert rows[0].slurm_job_id == 4242
    finally:
        await _cleanup(postgres_pool, wt_idx, *rest)


# ---------------------------------------------------------------------------
# Lifecycle transitions
# ---------------------------------------------------------------------------


async def test_slurm_lifecycle_to_completed(postgres_pool):
    """submitting → submitted(job_id) → running → completed."""
    wt_idx, *rest = await _seed_work_ticket(postgres_pool)
    try:
        await step_progress.record_submitting(
            postgres_pool,
            work_ticket_idx=wt_idx,
            step_index=0,
            attempt=0,
            step_name="align",
            compute_target=ComputeTarget.SLURM,
            job_name=f"qiita-wt{wt_idx}-align-a0",
        )
        await step_progress.record_submitted(
            postgres_pool, work_ticket_idx=wt_idx, step_index=0, attempt=0, slurm_job_id=99
        )
        assert await _state_of(postgres_pool, wt_idx, 0) == "submitted"

        await step_progress.record_running(
            postgres_pool, work_ticket_idx=wt_idx, step_index=0, attempt=0
        )
        assert await _state_of(postgres_pool, wt_idx, 0) == "running"
        # Repeated running polls are idempotent, not a guard violation.
        await step_progress.record_running(
            postgres_pool, work_ticket_idx=wt_idx, step_index=0, attempt=0
        )

        await step_progress.record_completed(
            postgres_pool, work_ticket_idx=wt_idx, step_index=0, attempt=0
        )
        rows = await step_progress.load_step_progress(postgres_pool, wt_idx)
        assert rows[0].state is StepProgressState.COMPLETED
        assert rows[0].slurm_job_id == 99
    finally:
        await _cleanup(postgres_pool, wt_idx, *rest)


async def test_control_plane_entry_has_no_job_fields(postgres_pool):
    """An in-process action: entry runs on the control plane: no job id, no
    job name, and it can go submitting → completed directly."""
    wt_idx, *rest = await _seed_work_ticket(postgres_pool)
    try:
        await step_progress.record_submitting(
            postgres_pool,
            work_ticket_idx=wt_idx,
            step_index=0,
            attempt=0,
            step_name="register-index",
            compute_target=ComputeTarget.CONTROL_PLANE,
        )
        await step_progress.record_completed(
            postgres_pool, work_ticket_idx=wt_idx, step_index=0, attempt=0
        )
        row = (await step_progress.load_step_progress(postgres_pool, wt_idx))[0]
        assert row.compute_target is ComputeTarget.CONTROL_PLANE
        assert row.slurm_job_id is None
        assert row.job_name is None
        assert row.state is StepProgressState.COMPLETED
    finally:
        await _cleanup(postgres_pool, wt_idx, *rest)


async def test_record_synchronous_completion_corrects_target(postgres_pool):
    """A step write-ahead'd optimistically as slurm that turns out to run on
    the local backend (synchronous) is corrected to compute_target=local with
    the SLURM job fields cleared, in one move to completed."""
    wt_idx, *rest = await _seed_work_ticket(postgres_pool)
    try:
        await step_progress.record_submitting(
            postgres_pool,
            work_ticket_idx=wt_idx,
            step_index=0,
            attempt=0,
            step_name="hash",
            compute_target=ComputeTarget.SLURM,
            job_name=f"qiita-wt{wt_idx}-hash-a0",
        )
        await step_progress.record_synchronous_completion(
            postgres_pool,
            work_ticket_idx=wt_idx,
            step_index=0,
            attempt=0,
            compute_target=ComputeTarget.LOCAL,
        )
        row = (await step_progress.load_step_progress(postgres_pool, wt_idx))[0]
        assert row.state is StepProgressState.COMPLETED
        assert row.compute_target is ComputeTarget.LOCAL
        assert row.slurm_job_id is None
        assert row.job_name is None
    finally:
        await _cleanup(postgres_pool, wt_idx, *rest)


async def test_record_failed_sets_failure_columns(postgres_pool):
    wt_idx, *rest = await _seed_work_ticket(postgres_pool)
    try:
        await step_progress.record_submitting(
            postgres_pool,
            work_ticket_idx=wt_idx,
            step_index=0,
            attempt=0,
            step_name="align",
            compute_target=ComputeTarget.SLURM,
            job_name=f"qiita-wt{wt_idx}-align-a0",
        )
        await step_progress.record_submitted(
            postgres_pool, work_ticket_idx=wt_idx, step_index=0, attempt=0, slurm_job_id=7
        )
        await step_progress.record_failed(
            postgres_pool,
            work_ticket_idx=wt_idx,
            step_index=0,
            attempt=0,
            failure_kind="oom_killed",
            failure_reason="step align exceeded mem ceiling",
        )
        row = (await step_progress.load_step_progress(postgres_pool, wt_idx))[0]
        assert row.state is StepProgressState.FAILED
        assert row.failure_kind == "oom_killed"
        assert row.failure_reason == "step align exceeded mem ceiling"
    finally:
        await _cleanup(postgres_pool, wt_idx, *rest)


async def test_record_failed_is_not_idempotent(postgres_pool):
    """Unlike completion, a failure carries mutable payload (kind/reason), so
    re-failing the same attempt raises rather than silently overwriting the
    original diagnostic. A retry is a new attempt row, not a re-fail."""
    wt_idx, *rest = await _seed_work_ticket(postgres_pool)
    try:
        await step_progress.record_submitting(
            postgres_pool,
            work_ticket_idx=wt_idx,
            step_index=0,
            attempt=0,
            step_name="align",
            compute_target=ComputeTarget.SLURM,
            job_name=f"qiita-wt{wt_idx}-align-a0",
        )
        await step_progress.record_failed(
            postgres_pool,
            work_ticket_idx=wt_idx,
            step_index=0,
            attempt=0,
            failure_kind="node_fail",
            failure_reason="node died",
        )
        with pytest.raises(RuntimeError):
            await step_progress.record_failed(
                postgres_pool,
                work_ticket_idx=wt_idx,
                step_index=0,
                attempt=0,
                failure_kind="oom_killed",
                failure_reason="stray late signal",
            )
        # Original diagnostic is preserved.
        row = (await step_progress.load_step_progress(postgres_pool, wt_idx))[0]
        assert row.failure_kind == "node_fail"
        assert row.failure_reason == "node died"
    finally:
        await _cleanup(postgres_pool, wt_idx, *rest)


async def test_record_completed_is_idempotent(postgres_pool):
    """Re-completing a completed entry is a safe no-op (re-run-after-crash
    finalize relies on it) — no payload to clobber."""
    wt_idx, *rest = await _seed_work_ticket(postgres_pool)
    try:
        await step_progress.record_submitting(
            postgres_pool,
            work_ticket_idx=wt_idx,
            step_index=0,
            attempt=0,
            step_name="x",
            compute_target=ComputeTarget.CONTROL_PLANE,
        )
        await step_progress.record_completed(
            postgres_pool, work_ticket_idx=wt_idx, step_index=0, attempt=0
        )
        await step_progress.record_completed(
            postgres_pool, work_ticket_idx=wt_idx, step_index=0, attempt=0
        )
        assert await _state_of(postgres_pool, wt_idx, 0) == "completed"
    finally:
        await _cleanup(postgres_pool, wt_idx, *rest)


async def test_in_process_entry_rejects_job_id_at_db(postgres_pool):
    """Defence-in-depth: handing a job id to an in-process (control_plane)
    entry trips the slurm-fields CHECK loudly instead of recording a
    nonsensical row."""
    wt_idx, *rest = await _seed_work_ticket(postgres_pool)
    try:
        await step_progress.record_submitting(
            postgres_pool,
            work_ticket_idx=wt_idx,
            step_index=0,
            attempt=0,
            step_name="x",
            compute_target=ComputeTarget.CONTROL_PLANE,
        )
        with pytest.raises(asyncpg.exceptions.CheckViolationError):
            await step_progress.record_submitted(
                postgres_pool, work_ticket_idx=wt_idx, step_index=0, attempt=0, slurm_job_id=1
            )
    finally:
        await _cleanup(postgres_pool, wt_idx, *rest)


async def test_out_of_order_transition_raises(postgres_pool):
    """Writers are WHERE-guarded: a transition from a state that can't
    legally precede the target raises loudly instead of silently
    overwriting (mirrors runner._atomic_transition)."""
    wt_idx, *rest = await _seed_work_ticket(postgres_pool)
    try:
        await step_progress.record_submitting(
            postgres_pool,
            work_ticket_idx=wt_idx,
            step_index=0,
            attempt=0,
            step_name="align",
            compute_target=ComputeTarget.SLURM,
            job_name=f"qiita-wt{wt_idx}-align-a0",
        )
        # 'running' requires a prior 'submitted'; from 'submitting' it raises.
        with pytest.raises(RuntimeError):
            await step_progress.record_running(
                postgres_pool, work_ticket_idx=wt_idx, step_index=0, attempt=0
            )
    finally:
        await _cleanup(postgres_pool, wt_idx, *rest)


async def test_completed_cannot_be_failed(postgres_pool):
    """A completed entry can't be re-stamped failed — terminal-success is
    sticky so a late stray failure never corrupts a finished step."""
    wt_idx, *rest = await _seed_work_ticket(postgres_pool)
    try:
        await step_progress.record_submitting(
            postgres_pool,
            work_ticket_idx=wt_idx,
            step_index=0,
            attempt=0,
            step_name="x",
            compute_target=ComputeTarget.CONTROL_PLANE,
        )
        await step_progress.record_completed(
            postgres_pool, work_ticket_idx=wt_idx, step_index=0, attempt=0
        )
        with pytest.raises(RuntimeError):
            await step_progress.record_failed(
                postgres_pool,
                work_ticket_idx=wt_idx,
                step_index=0,
                attempt=0,
                failure_kind="unknown_permanent",
                failure_reason="should not apply",
            )
    finally:
        await _cleanup(postgres_pool, wt_idx, *rest)


# ---------------------------------------------------------------------------
# Recovery query helpers
# ---------------------------------------------------------------------------


async def test_load_step_progress_returns_per_step_view(postgres_pool):
    """The query helper returns each entry's compute_target, job id, and
    state, ordered by (step_index, attempt) — the shape recovery and the
    summary read consume."""
    wt_idx, *rest = await _seed_work_ticket(postgres_pool)
    try:
        # Step 0: a completed control-plane action.
        await step_progress.record_submitting(
            postgres_pool,
            work_ticket_idx=wt_idx,
            step_index=0,
            attempt=0,
            step_name="prep",
            compute_target=ComputeTarget.CONTROL_PLANE,
        )
        await step_progress.record_completed(
            postgres_pool, work_ticket_idx=wt_idx, step_index=0, attempt=0
        )
        # Step 1: a running SLURM job.
        await step_progress.record_submitting(
            postgres_pool,
            work_ticket_idx=wt_idx,
            step_index=1,
            attempt=0,
            step_name="align",
            compute_target=ComputeTarget.SLURM,
            job_name=f"qiita-wt{wt_idx}-align-a0",
        )
        await step_progress.record_submitted(
            postgres_pool, work_ticket_idx=wt_idx, step_index=1, attempt=0, slurm_job_id=555
        )
        await step_progress.record_running(
            postgres_pool, work_ticket_idx=wt_idx, step_index=1, attempt=0
        )

        rows = await step_progress.load_step_progress(postgres_pool, wt_idx)
        assert [r.step_index for r in rows] == [0, 1]
        assert rows[0].compute_target is ComputeTarget.CONTROL_PLANE
        assert rows[0].state is StepProgressState.COMPLETED
        assert rows[0].slurm_job_id is None
        assert rows[1].compute_target is ComputeTarget.SLURM
        assert rows[1].state is StepProgressState.RUNNING
        assert rows[1].slurm_job_id == 555
    finally:
        await _cleanup(postgres_pool, wt_idx, *rest)


async def test_first_incomplete_step_index(postgres_pool):
    """'current entry' = first step index lacking a completed row across any
    attempt; once all steps complete it points one past the end."""
    wt_idx, *rest = await _seed_work_ticket(postgres_pool)
    try:
        total_steps = 3
        # Nothing recorded → current entry is 0.
        rows = await step_progress.load_step_progress(postgres_pool, wt_idx)
        assert step_progress.first_incomplete_step_index(rows, total_steps) == 0

        # Step 0 completed, step 1 running → current entry is 1.
        for step_index, target, name in (
            (0, ComputeTarget.CONTROL_PLANE, "prep"),
            (1, ComputeTarget.CONTROL_PLANE, "mid"),
        ):
            await step_progress.record_submitting(
                postgres_pool,
                work_ticket_idx=wt_idx,
                step_index=step_index,
                attempt=0,
                step_name=name,
                compute_target=target,
            )
        await step_progress.record_completed(
            postgres_pool, work_ticket_idx=wt_idx, step_index=0, attempt=0
        )
        rows = await step_progress.load_step_progress(postgres_pool, wt_idx)
        assert step_progress.first_incomplete_step_index(rows, total_steps) == 1

        # Complete 1 and 2 → current entry is one past the last step.
        await step_progress.record_completed(
            postgres_pool, work_ticket_idx=wt_idx, step_index=1, attempt=0
        )
        await step_progress.record_submitting(
            postgres_pool,
            work_ticket_idx=wt_idx,
            step_index=2,
            attempt=0,
            step_name="tail",
            compute_target=ComputeTarget.CONTROL_PLANE,
        )
        await step_progress.record_completed(
            postgres_pool, work_ticket_idx=wt_idx, step_index=2, attempt=0
        )
        rows = await step_progress.load_step_progress(postgres_pool, wt_idx)
        assert step_progress.first_incomplete_step_index(rows, total_steps) == total_steps
    finally:
        await _cleanup(postgres_pool, wt_idx, *rest)


async def test_retry_attempt_completion_counts_for_current_entry(postgres_pool):
    """A step that failed on attempt 0 but completed on attempt 1 counts as
    complete — the current entry advances past it."""
    wt_idx, *rest = await _seed_work_ticket(postgres_pool)
    try:
        # attempt 0 fails.
        await step_progress.record_submitting(
            postgres_pool,
            work_ticket_idx=wt_idx,
            step_index=0,
            attempt=0,
            step_name="align",
            compute_target=ComputeTarget.SLURM,
            job_name=f"qiita-wt{wt_idx}-align-a0",
        )
        await step_progress.record_failed(
            postgres_pool,
            work_ticket_idx=wt_idx,
            step_index=0,
            attempt=0,
            failure_kind="node_fail",
            failure_reason="node died",
        )
        # attempt 1 completes.
        await step_progress.record_submitting(
            postgres_pool,
            work_ticket_idx=wt_idx,
            step_index=0,
            attempt=1,
            step_name="align",
            compute_target=ComputeTarget.SLURM,
            job_name=f"qiita-wt{wt_idx}-align-a1",
        )
        await step_progress.record_submitted(
            postgres_pool, work_ticket_idx=wt_idx, step_index=0, attempt=1, slurm_job_id=812
        )
        await step_progress.record_completed(
            postgres_pool, work_ticket_idx=wt_idx, step_index=0, attempt=1
        )
        rows = await step_progress.load_step_progress(postgres_pool, wt_idx)
        # Both attempts present, ordered.
        assert [(r.step_index, r.attempt) for r in rows] == [(0, 0), (0, 1)]
        assert step_progress.first_incomplete_step_index(rows, 1) == 1
    finally:
        await _cleanup(postgres_pool, wt_idx, *rest)
