"""DB tests for `qiita-admin backfill assembly-gene-call`'s planner.

The planner resolves every long-read-assembly run of a source version to the attempts
whose outputs a recovery ticket reads (completed ticket for the run's prep_sample,
mask and assembler -> its newest completed attempt of each source entry -> those
outputs on scratch), or to why it cannot. The source version here is a per-test
string, so the plan sees only this test's runs.
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

from qiita_control_plane.backfill.assembly_gene_call import (
    DONE_BY_RECOVERY,
    DONE_BY_SOURCE,
    RECOVER_ACTION_ID,
    RECOVER_ACTION_VERSION,
    SOURCE_ENTRIES,
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

_STEPS = ("assemble", "bin_refine", "checkm", "assembly_hash")


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
    source_version = f"bf-gc-{suffix}"
    other_version = f"bf-gc-other-{suffix}"
    recover_version = f"bf-gcr-{suffix}"
    # The default the runner resolves an omitted `assembler` to, as every source
    # version declares it.
    schema = {"properties": {"assembler": {"type": "string", "default": "hifiasm_meta"}}}
    for version in (source_version, other_version):
        await _seed_action(pool, LONG_READ_ASSEMBLY_ACTION_ID, version, schema)
    await _seed_action(pool, RECOVER_ACTION_ID, recover_version, {})
    owner = await seed_user_principal(pool, prefix="gc-bf", suffix=suffix)
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
    features: list[int] = []

    async def prep_sample() -> int:
        biosample_idx, ps = await seed_biosample_with_sequenced_prep_sample(pool, owner_idx=owner)
        samples.append((biosample_idx, ps))
        return ps

    async def membership(ps: int, p: int, *, genome: bool = True) -> None:
        feature_idx = await pool.fetchval(
            "INSERT INTO qiita.feature (sequence_hash) VALUES ($1) RETURNING feature_idx",
            uuid.uuid4(),
        )
        features.append(feature_idx)
        genome_idx = None
        if genome:
            genome_idx = await pool.fetchval(
                "INSERT INTO qiita.genome (source, source_id, prep_sample_idx)"
                " VALUES ('qiita', $1, $2) RETURNING genome_idx",
                str(uuid.uuid4()),
                ps,
            )
        await pool.execute(
            "INSERT INTO qiita.assembly_membership"
            " (prep_sample_idx, processing_idx, kind, bin_id, feature_idx, genome_idx)"
            " VALUES ($1, $2, 'MAG', $3, $4, $5)",
            ps,
            p,
            f"bin.{feature_idx}",
            feature_idx,
            genome_idx,
        )

    async def run(
        ps: int,
        *,
        assembler: str = "myloasm",
        version: str = source_version,
        state: str = ASSEMBLY_SAMPLE_COMPLETED,
        genome: bool | None = True,
    ) -> int:
        """An assembly run of `ps`, its `assembly_sample` row and (unless `genome` is
        None) one membership row, with a genome when `genome`; returns processing_idx."""
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
        if genome is not None:
            await membership(ps, p, genome=genome)
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
        name: str,
        attempt: int,
        *,
        state: StepProgressState = StepProgressState.COMPLETED,
        on_disk: bool = True,
    ) -> None:
        """A step row for `name`, and (for a completed one with `on_disk`) what the
        planner stats: a step's manifest, or mint-features' map. The paths are spelled
        out rather than built with `qiita_common.workspace`, so the layout of
        workspaces already on disk is pinned independently of the planner's helpers."""
        failed = state is StepProgressState.FAILED
        await pool.execute(
            "INSERT INTO qiita.work_ticket_step"
            " (work_ticket_idx, step_index, attempt, step_name, compute_target, state,"
            "  failure_kind, failure_reason)"
            " VALUES ($1, $7, $2, $8, $6, $3, $4, $5)",
            wt,
            attempt,
            state.value,
            FailureKind.OOM_KILLED.value if failed else None,
            "out of memory" if failed else None,
            ComputeTarget.LOCAL.value,
            [*SOURCE_ENTRIES, "gene_call"].index(name),
            name,
        )
        if not on_disk or state is not StepProgressState.COMPLETED:
            return
        attempt_dir = tmp_path / str(wt) / name / f"attempt-{attempt}"
        if name == "mint-features":
            attempt_dir.mkdir(parents=True)
            (attempt_dir / "feature_map.parquet").write_bytes(b"x")
        else:
            (attempt_dir / "output").mkdir(parents=True)
            (attempt_dir / "output" / "manifest.json").write_text('{"files": [], "outputs": {}}')

    async def source(ps: int, *, skip: tuple[str, ...] = (), **ticket_kwargs) -> int:
        """A completed source ticket with attempt 0 of every source entry but `skip`."""
        wt = await ticket(ps, **ticket_kwargs)
        for name in SOURCE_ENTRIES:
            if name not in skip:
                await step(wt, name, 0)
        return wt

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
        "source_version": source_version,
        "other_version": other_version,
        "prep_sample": prep_sample,
        "membership": membership,
        "run": run,
        "ticket": ticket,
        "step": step,
        "source": source,
        "recover": recover,
        "plan": plan,
        "tmp_path": tmp_path,
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
    await pool.execute("DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])", features)
    await pool.execute(
        "DELETE FROM qiita.processing WHERE processing_idx = ANY($1::bigint[])", processing
    )
    await pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)
    await pool.execute(
        "DELETE FROM qiita.action WHERE version = ANY($1::text[])",
        [source_version, other_version, recover_version],
    )


async def test_a_finished_run_is_writable_from_each_entrys_completed_attempt(world):
    w = world
    ps = await w["prep_sample"]()
    p = await w["run"](ps)
    wt = await w["ticket"](ps)
    await w["step"](wt, "assemble", 0, state=StepProgressState.FAILED)
    await w["step"](wt, "assemble", 1)
    await w["step"](wt, "bin_refine", 0)
    await w["step"](wt, "checkm", 0, state=StepProgressState.FAILED)
    await w["step"](wt, "checkm", 1, state=StepProgressState.FAILED)
    await w["step"](wt, "checkm", 2)
    await w["step"](wt, "assembly_hash", 0)
    await w["step"](wt, "mint-features", 0)

    run = (await w["plan"]())[(ps, p)]
    assert run.status is RunStatus.WRITABLE, run.reason
    assert run.submit_body() == {
        "action_id": RECOVER_ACTION_ID,
        "action_version": RECOVER_ACTION_VERSION,
        "scope_target": {"kind": ScopeTargetKind.PREP_SAMPLE, "prep_sample_idx": ps},
        "action_context": {
            "assembly_processing_idx": p,
            "source_work_ticket_idx": wt,
            "assemble_attempt": 1,
            "bin_refine_attempt": 0,
            "checkm_attempt": 2,
            "assembly_hash_attempt": 0,
            "mint_features_attempt": 0,
        },
    }


async def test_every_assembler_and_only_source_versions_are_planned(world):
    w = world
    ps = await w["prep_sample"]()
    hifiasm = await w["run"](ps, assembler="hifiasm_meta")
    later = await w["run"](ps, version=w["other_version"])
    planned = await w["plan"]()
    assert (ps, hifiasm) in planned
    assert (ps, later) not in planned


async def test_the_newest_completed_ticket_of_the_runs_assembler_is_the_source(world):
    """Tickets are matched on prep_sample, mask and assembler. An omitted assembler
    is the action's default (hifiasm_meta), so a newer ticket that omitted it is a
    different run's, and a newer failed one never finished."""
    w = world
    ps = await w["prep_sample"]()
    p = await w["run"](ps)
    await w["source"](ps)
    newer = await w["source"](ps)
    await w["source"](ps, context={})
    await w["ticket"](ps, state=WorkTicketState.FAILED)

    run = (await w["plan"]())[(ps, p)]
    assert run.status is RunStatus.WRITABLE, run.reason
    assert run.source_work_ticket_idx == newer


async def test_an_omitted_assembler_matches_a_default_assembler_run(world):
    w = world
    ps = await w["prep_sample"]()
    p = await w["run"](ps, assembler="hifiasm_meta")
    wt = await w["source"](ps, context={})
    run = (await w["plan"]())[(ps, p)]
    assert run.status is RunStatus.WRITABLE, run.reason
    assert run.source_work_ticket_idx == wt


async def test_a_source_ticket_that_called_genes_is_done(world):
    w = world
    ps = await w["prep_sample"]()
    p = await w["run"](ps)
    wt = await w["source"](ps)
    await w["step"](wt, "gene_call", 0)
    run = (await w["plan"]())[(ps, p)]
    assert run.status is RunStatus.DONE
    assert run.reason == DONE_BY_SOURCE


@pytest.mark.parametrize(
    ("setup", "kind", "reason"),
    [
        pytest.param("no-ticket", ResidueKind.NO_SOURCE_TICKET, "no completed", id="no-ticket"),
        pytest.param(
            "no-checkm",
            ResidueKind.NO_COMPLETED_ENTRY,
            "no completed attempt of checkm",
            id="no-checkm",
        ),
        pytest.param(
            "no-mint",
            ResidueKind.NO_COMPLETED_ENTRY,
            "no completed attempt of mint-features",
            id="no-mint",
        ),
        pytest.param("no-manifest", ResidueKind.NO_MANIFEST, "no step manifest", id="no-manifest"),
        pytest.param("no-map", ResidueKind.NO_FEATURE_MAP, "no mint-features map", id="no-map"),
        pytest.param(
            "no-membership", ResidueKind.NO_MEMBERSHIP, "no assembly_membership", id="no-membership"
        ),
        pytest.param(
            "no-genome",
            ResidueKind.MEMBERSHIP_WITHOUT_GENOME,
            "1 of 2 assembly_membership rows without a genome",
            id="no-genome",
        ),
    ],
)
async def test_a_run_that_cannot_be_recovered_is_residue(world, setup, kind, reason):
    w = world
    ps = await w["prep_sample"]()
    p = await w["run"](ps, genome=None if setup == "no-membership" else True)
    if setup == "no-genome":
        # A second subject without a genome beside the first one's.
        await w["membership"](ps, p, genome=False)
    if setup != "no-ticket":
        skip = {"no-checkm": ("checkm",), "no-mint": ("mint-features",)}.get(setup, ())
        wt = await w["source"](ps, skip=skip)
        ticket_dir = w["tmp_path"] / str(wt)
        if setup == "no-manifest":
            (ticket_dir / "checkm" / "attempt-0" / "output" / "manifest.json").unlink()
        elif setup == "no-map":
            (ticket_dir / "mint-features" / "attempt-0" / "feature_map.parquet").unlink()

    run = (await w["plan"]())[(ps, p)]
    assert run.status is RunStatus.RESIDUE
    assert run.residue_kind is kind
    assert reason in run.reason


async def test_runs_with_nothing_to_recover_are_skipped(world):
    w = world
    ps = await w["prep_sample"]()
    empty = await w["run"](ps, state=ASSEMBLY_SAMPLE_NO_DATA, genome=None)
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
    wt = await w["source"](ps)
    rec = await w["recover"](
        ps, p, state=state, failure_reason="boom" if state is WorkTicketState.FAILED else None
    )

    run = (await w["plan"]())[(ps, p)]
    assert run.status is status
    if status is RunStatus.DONE:
        assert run.reason == DONE_BY_RECOVERY
    else:
        assert str(rec) in run.reason
    if status is RunStatus.IN_FLIGHT:
        # What scratch reclaim reads to keep the workspace the recovery is reading.
        assert run.source_work_ticket_idx == wt
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
            assert run.reason == f"recovery ticket {rec} {state}"
        retried = (await w["plan"](retry_failed=True))[(ps, p)]
        assert retried.status is RunStatus.WRITABLE
        assert retried.source_work_ticket_idx == wt


async def test_another_runs_recovery_ticket_does_not_count_as_this_runs(world):
    """One prep_sample assembled on two source versions: two runs."""
    w = world
    ps = await w["prep_sample"]()
    a, b = await w["run"](ps), await w["run"](ps, version=w["other_version"])
    await w["source"](ps)
    await w["source"](ps, version=w["other_version"])
    await w["recover"](ps, a, state=WorkTicketState.COMPLETED)

    planned = await w["plan"](source_versions=(w["source_version"], w["other_version"]))
    assert planned[(ps, a)].status is RunStatus.DONE
    assert planned[(ps, b)].status is RunStatus.WRITABLE, planned[(ps, b)].reason
