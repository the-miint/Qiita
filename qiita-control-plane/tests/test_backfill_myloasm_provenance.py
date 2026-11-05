"""DB tests for `qiita-admin backfill myloasm-provenance`'s planner.

The planner resolves every myloasm run of a long-read-assembly version that predates
the provenance tables to the `assemble` attempt whose output a recovery ticket reads
(completed ticket for the run's prep_sample, mask and assembler -> its newest
completed `assemble` attempt -> that attempt's output on scratch), or to why it
cannot. The source version here is a per-test string, so the plan sees only this
test's runs.
"""

import json
import secrets
import uuid

import pytest
import pytest_asyncio
from qiita_common.actions import LONG_READ_ASSEMBLY_ACTION_ID
from qiita_common.backend_failure import FailureKind
from qiita_common.models import (
    ComputeTarget,
    FailureType,
    ProcessingStatus,
    ScopeTargetKind,
    StepProgressState,
    WorkTicketFailureStage,
    WorkTicketState,
)

from qiita_control_plane.backfill.myloasm_provenance import (
    RECOVER_ACTION_ID,
    ResidueKind,
    RunStatus,
    plan_backfill,
)
from qiita_control_plane.repositories.assembly import (
    ASSEMBLY_SAMPLE_COMPLETED,
    ASSEMBLY_SAMPLE_NO_DATA,
)
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)
from qiita_control_plane.testing.db_teardown import teardown_entity_graph

pytestmark = pytest.mark.db


async def _seed_action(pool, action_id: str, version: str, context_schema: dict) -> None:
    await pool.execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status)"
        " VALUES ($1, $2, $5, '{}'::text[], $3::jsonb,"
        "         $4::jsonb, '[]'::jsonb, 1, 1, '1 minute', 'active', 'failed')",
        action_id,
        version,
        '{"service": false, "human_roles": ["system_admin"]}',
        json.dumps(context_schema),
        ScopeTargetKind.PREP_SAMPLE.value,
    )


@pytest_asyncio.fixture
async def world(postgres_pool, tmp_path):
    pool = postgres_pool
    suffix = secrets.token_hex(4)
    source_version = f"bf-myl-{suffix}"
    other_version = f"bf-myl-other-{suffix}"
    recover_version = f"bf-rec-{suffix}"
    # The default the runner resolves an omitted `assembler` to, as 1.0.1 declares it.
    schema = {"properties": {"assembler": {"type": "string", "default": "hifiasm_meta"}}}
    for version in (source_version, other_version):
        await _seed_action(pool, LONG_READ_ASSEMBLY_ACTION_ID, version, schema)
    await _seed_action(pool, RECOVER_ACTION_ID, recover_version, {})
    owner = await seed_user_principal(pool, prefix="myl-bf", suffix=suffix)
    mask_idx = await pool.fetchval(
        "INSERT INTO qiita.mask_definition"
        " (params_hash, filter_workflow, filter_version, params, created_by_idx)"
        " VALUES ($1, 'read-mask', '1.0.0', '{}'::jsonb, $2) RETURNING mask_idx",
        uuid.uuid4().bytes + uuid.uuid4().bytes,
        owner,
    )
    samples: list[tuple[int, int]] = []
    processing: list[int] = []
    tickets: list[int] = []

    async def prep_sample() -> int:
        biosample_idx, ps = await seed_biosample_with_sequenced_prep_sample(pool, owner_idx=owner)
        samples.append((biosample_idx, ps))
        return ps

    async def run(
        ps: int,
        *,
        assembler: str = "myloasm",
        version: str = source_version,
        state: str = ASSEMBLY_SAMPLE_COMPLETED,
    ) -> int:
        """An assembly run of `ps` and its `assembly_sample` row; returns processing_idx."""
        params = {
            "workflow": LONG_READ_ASSEMBLY_ACTION_ID,
            "version": version,
            "mask_idx": mask_idx,
            "assembler": assembler,
        }
        p = await pool.fetchval(
            "INSERT INTO qiita.processing (params_hash, workflow, version, params)"
            " VALUES ($1, $2, $3, $4::jsonb) RETURNING processing_idx",
            uuid.uuid4().bytes + uuid.uuid4().bytes,
            LONG_READ_ASSEMBLY_ACTION_ID,
            version,
            json.dumps(params),
        )
        processing.append(p)
        await pool.execute(
            "INSERT INTO qiita.assembly_sample (processing_idx, prep_sample_idx, state)"
            " VALUES ($1, $2, $3)",
            p,
            ps,
            state,
        )
        return p

    async def ticket(
        ps: int,
        *,
        context: dict | None = None,
        state: WorkTicketState = WorkTicketState.COMPLETED,
        version: str = source_version,
        action_id: str = LONG_READ_ASSEMBLY_ACTION_ID,
        mask: int | None = mask_idx,
        failure_reason: str | None = None,
    ) -> int:
        failed = state is WorkTicketState.FAILED
        idx = await pool.fetchval(
            "INSERT INTO qiita.work_ticket"
            " (action_id, action_version, originator_principal_idx, scope_target_kind,"
            "  prep_sample_idx, action_context, mask_idx, state,"
            "  failure_type, failure_stage, failure_reason)"
            " VALUES ($1, $2, $3, $11, $4, $5::jsonb, $6,"
            "         $7::qiita.work_ticket_state, $8::qiita.failure_type,"
            "         $9::qiita.work_ticket_failure_stage, $10) RETURNING work_ticket_idx",
            action_id,
            version,
            owner,
            ps,
            json.dumps({"assembler": "myloasm"} if context is None else context),
            mask,
            state.value,
            FailureType.PERMANENT.value if failed else None,
            WorkTicketFailureStage.SUBMISSION.value if failed else None,
            (failure_reason or "failed") if failed else None,
            ScopeTargetKind.PREP_SAMPLE.value,
        )
        tickets.append(idx)
        return idx

    async def step(
        wt: int,
        attempt: int,
        *,
        state: StepProgressState = StepProgressState.COMPLETED,
        tree: bool = True,
    ):
        """An `assemble` step row, and (for a completed one with `tree`) its output on
        scratch: the manifest and the assembler tree. The path is spelled out rather
        than built with `qiita_common.workspace`, so the layout of trees already on
        disk is pinned independently of the helpers that the planner uses."""
        failed = state is StepProgressState.FAILED
        await pool.execute(
            "INSERT INTO qiita.work_ticket_step"
            " (work_ticket_idx, step_index, attempt, step_name, compute_target, state,"
            "  failure_kind, failure_reason)"
            " VALUES ($1, 0, $2, 'assemble', $6, $3, $4, $5)",
            wt,
            attempt,
            state.value,
            FailureKind.OOM_KILLED.value if failed else None,
            "out of memory" if failed else None,
            ComputeTarget.LOCAL.value,
        )
        out = tmp_path / str(wt) / "assemble" / f"attempt-{attempt}" / "output"
        if tree:
            (out / "assembler").mkdir(parents=True)
            (out / "manifest.json").write_text('{"files": [], "outputs": {}}')
        return out

    async def recover(
        ps: int, processing_idx: int, *, state: WorkTicketState, failure_reason=None
    ) -> int:
        return await ticket(
            ps,
            action_id=RECOVER_ACTION_ID,
            version=recover_version,
            context={"assembly_processing_idx": processing_idx},
            state=state,
            mask=None,
            failure_reason=failure_reason,
        )

    async def plan(*, source_versions=(source_version,), **kwargs):
        p = await plan_backfill(
            pool, ticket_root=tmp_path, source_versions=source_versions, **kwargs
        )
        return {(r.prep_sample_idx, r.processing_idx): r for r in p.runs}

    yield {
        "pool": pool,
        "owner": owner,
        "mask_idx": mask_idx,
        "source_version": source_version,
        "other_version": other_version,
        "prep_sample": prep_sample,
        "run": run,
        "ticket": ticket,
        "step": step,
        "recover": recover,
        "plan": plan,
    }

    await pool.execute(
        "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = ANY($1::bigint[])", tickets
    )
    await teardown_entity_graph(
        pool,
        study_idxs=[],
        biosample_idxs=[bs for bs, _ps in samples],
        prep_sample_idxs=[ps for _bs, ps in samples],
    )
    await pool.execute(
        "DELETE FROM qiita.processing WHERE processing_idx = ANY($1::bigint[])", processing
    )
    await pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)
    await pool.execute(
        "DELETE FROM qiita.action WHERE version = ANY($1::text[])",
        [source_version, other_version, recover_version],
    )


async def test_a_finished_run_is_writable_from_its_completed_attempt(world):
    w = world
    ps = await w["prep_sample"]()
    p = await w["run"](ps)
    wt = await w["ticket"](ps)
    await w["step"](wt, 0, state=StepProgressState.FAILED, tree=False)
    await w["step"](wt, 1)

    run = (await w["plan"]())[(ps, p)]
    assert run.status is RunStatus.WRITABLE, run.reason
    assert (run.source_work_ticket_idx, run.assemble_attempt) == (wt, 1)
    assert run.submit_body() == {
        "action_id": RECOVER_ACTION_ID,
        "action_version": "1.0.0",
        "scope_target": {"kind": ScopeTargetKind.PREP_SAMPLE, "prep_sample_idx": ps},
        "action_context": {
            "assembly_processing_idx": p,
            "assembly_mask_idx": w["mask_idx"],
            "source_work_ticket_idx": wt,
            "assemble_attempt": 1,
        },
    }


async def test_only_myloasm_runs_of_a_source_version_are_planned(world):
    w = world
    ps = await w["prep_sample"]()
    hifiasm = await w["run"](ps, assembler="hifiasm_meta")
    later = await w["run"](ps, version=w["other_version"])
    planned = await w["plan"]()
    assert (ps, hifiasm) not in planned
    assert (ps, later) not in planned


async def test_the_newest_completed_ticket_of_the_runs_assembler_is_the_source(world):
    """Tickets are matched on prep_sample, mask and assembler. An omitted assembler
    is the action's default (hifiasm_meta), so a newer ticket that omitted it is a
    different run's, and a newer failed one never finished."""
    w = world
    ps = await w["prep_sample"]()
    p = await w["run"](ps)
    old = await w["ticket"](ps)
    await w["step"](old, 0)
    newer = await w["ticket"](ps)
    await w["step"](newer, 0)
    await w["step"](await w["ticket"](ps, context={}), 0)
    await w["ticket"](ps, state=WorkTicketState.FAILED)

    run = (await w["plan"]())[(ps, p)]
    assert run.status is RunStatus.WRITABLE, run.reason
    assert (run.source_work_ticket_idx, run.assemble_attempt) == (newer, 0)


@pytest.mark.parametrize(
    ("setup", "kind", "reason"),
    [
        pytest.param("no-ticket", ResidueKind.NO_SOURCE_TICKET, "no completed", id="no-ticket"),
        pytest.param(
            "no-step", ResidueKind.NO_ASSEMBLE_STEP, "no completed 'assemble' step", id="no-step"
        ),
        pytest.param("no-tree", ResidueKind.NO_MANIFEST, "no manifest", id="output-gone"),
        pytest.param(
            "no-assembler", ResidueKind.NO_ASSEMBLER_TREE, "no assembler tree", id="assembler-gone"
        ),
    ],
)
async def test_a_run_whose_source_cannot_be_found_is_residue(world, setup, kind, reason):
    w = world
    ps = await w["prep_sample"]()
    p = await w["run"](ps)
    if setup != "no-ticket":
        wt = await w["ticket"](ps)
        if setup == "no-tree":
            await w["step"](wt, 0, tree=False)
        elif setup == "no-assembler":
            out = await w["step"](wt, 0)
            (out / "assembler").rmdir()

    run = (await w["plan"]())[(ps, p)]
    assert run.status is RunStatus.RESIDUE
    assert run.residue_kind is kind
    assert reason in run.reason


async def test_runs_with_nothing_to_recover_are_skipped(world):
    w = world
    ps = await w["prep_sample"]()
    empty = await w["run"](ps, state=ASSEMBLY_SAMPLE_NO_DATA)
    deprecated = await w["run"](ps)
    await w["pool"].execute(
        "UPDATE qiita.processing SET status = $3, deprecated_at = now(),"
        " deprecated_by_idx = $2, deprecation_reason = 'test' WHERE processing_idx = $1",
        deprecated,
        w["owner"],
        ProcessingStatus.DEPRECATED.value,
    )
    planned = await w["plan"]()
    assert planned[(ps, empty)].status is RunStatus.SKIPPED
    assert ASSEMBLY_SAMPLE_NO_DATA in planned[(ps, empty)].reason
    assert planned[(ps, deprecated)].status is RunStatus.SKIPPED
    assert ProcessingStatus.DEPRECATED in planned[(ps, deprecated)].reason


@pytest.mark.parametrize(
    ("state", "status"),
    [
        (WorkTicketState.COMPLETED, RunStatus.DONE),
        (WorkTicketState.PROCESSING, RunStatus.IN_FLIGHT),
        (WorkTicketState.FAILED, RunStatus.RESIDUE),
        (WorkTicketState.CANCELLED, RunStatus.RESIDUE),
        (WorkTicketState.NO_DATA, RunStatus.RESIDUE),
    ],
)
async def test_a_runs_own_recovery_ticket_decides_its_status(world, state, status):
    w = world
    ps = await w["prep_sample"]()
    p = await w["run"](ps)
    wt = await w["ticket"](ps)
    await w["step"](wt, 0)
    rec = await w["recover"](
        ps, p, state=state, failure_reason="boom" if state is WorkTicketState.FAILED else None
    )

    run = (await w["plan"]())[(ps, p)]
    assert run.status is status
    if status is not RunStatus.DONE:
        assert str(rec) in run.reason
    if status is RunStatus.RESIDUE:
        assert (
            run.residue_kind
            is {
                WorkTicketState.FAILED: ResidueKind.RECOVERY_FAILED,
                WorkTicketState.CANCELLED: ResidueKind.RECOVERY_CANCELLED,
                WorkTicketState.NO_DATA: ResidueKind.RECOVERY_NO_DATA,
            }[state]
        )
        if state is WorkTicketState.FAILED:
            assert run.reason.endswith(": boom")
        else:
            # No failure columns outside FAILED, so no dangling ": None".
            assert run.reason == f"recovery ticket {rec} {state}"
        retried = (await w["plan"](retry_failed=True))[(ps, p)]
        assert retried.status is RunStatus.WRITABLE
        assert retried.source_work_ticket_idx == wt


async def test_another_runs_recovery_ticket_does_not_count_as_this_runs(world):
    """One prep_sample assembled with myloasm on both source versions: two runs."""
    w = world
    ps = await w["prep_sample"]()
    a, b = await w["run"](ps), await w["run"](ps, version=w["other_version"])
    await w["step"](await w["ticket"](ps), 0)
    await w["step"](await w["ticket"](ps, version=w["other_version"]), 0)
    await w["recover"](ps, a, state=WorkTicketState.COMPLETED)

    planned = await w["plan"](source_versions=(w["source_version"], w["other_version"]))
    assert planned[(ps, a)].status is RunStatus.DONE
    assert planned[(ps, b)].status is RunStatus.WRITABLE, planned[(ps, b)].reason
