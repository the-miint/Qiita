"""DB tests for POST /definition-group/resolve: authorization (VIEWER on every study
of every sample, refused whole), a cohort spanning pools, and the run reads the
resolver makes against Postgres (gate rows, block and per-sample tickets, cancelled
and abandoned ones, assembly tickets matched by params).

The rules are unit-tested in tests/test_definition_resolver.py.
"""

import json
import uuid

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.actions import (
    ALIGN_ACTION_ID,
    LONG_READ_ASSEMBLY_ACTION_ID,
    READ_MASK_ACTION_ID,
)
from qiita_common.api_paths import URL_DEFINITION_GROUP_RESOLVE
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    DefinitionKind,
    FailureType,
    SampleResolutionReason,
    SampleResolutionState,
    ScopeTargetKind,
    WorkTicketFailureStage,
    WorkTicketState,
)

from qiita_control_plane.definition_resolver import resolve_group
from qiita_control_plane.repositories.alignment_definition import (
    ALIGNMENT_SAMPLE_COMPLETED,
    ALIGNMENT_SAMPLE_PENDING,
)
from qiita_control_plane.repositories.assembly import ASSEMBLY_SAMPLE_COMPLETED
from qiita_control_plane.repositories.block import (
    BLOCK_ABANDONED,
    BLOCK_PENDING,
    MASK_SAMPLE_COMPLETED,
    add_block_members,
    create_block,
)
from qiita_control_plane.repositories.processing import mint_processing
from qiita_control_plane.testing.db_seeds import (
    delete_action_if_created,
    seed_action_if_absent,
    seed_biosample_to_study_link,
    seed_biosample_with_sequenced_prep_sample,
    seed_prep_sample_to_study_link,
    seed_sequenced_sample_subtype,
)
from qiita_control_plane.testing.db_teardown import teardown_entity_graph

pytestmark = pytest.mark.db

PROCESSED = SampleResolutionState.PROCESSED
INCOMPLETE = SampleResolutionState.INCOMPLETE
NOT_PROCESSED = SampleResolutionState.NOT_PROCESSED
R = SampleResolutionReason

# A version no workflow ships, so the action rows these tests seed never collide with a
# synced one (whose context_schema would change the assembly params).
_TEST_VERSION = "0.0.0-resolver-test"


@pytest.fixture
def ctx(role_keyed_clients):
    return dict(role_keyed_clients)


def _body(kind: DefinitionKind, idx: int, samples: list[int]) -> dict:
    return {"definition_kind": kind, "definition_idx": idx, "prep_sample_idx": samples}


def _by_sample(resp) -> dict[int, dict]:
    return {s["prep_sample_idx"]: s for s in resp.json()["samples"]}


@pytest_asyncio.fixture
async def other_pool_sample(ctx, alignment_group_world):
    """ps_x: a sample in a second sequenced pool, linked to study_1 (readable by the
    regular user), masked under mask_2 and aligned under align_3."""
    w = alignment_group_world
    db = ctx["pool"]
    owner = w["owner_idx"]
    bs_x, ps_x = await seed_biosample_with_sequenced_prep_sample(db, owner_idx=owner)
    run_idx, pool_idx, _ = await seed_sequenced_sample_subtype(
        db, prep_sample_idx=ps_x, owner_idx=owner, sequenced_pool_item_id="other-pool-x"
    )
    assert pool_idx != w["pool_idx"]
    await seed_biosample_to_study_link(
        db, biosample_idx=bs_x, study_idx=w["study_1"], created_by_idx=owner
    )
    await seed_prep_sample_to_study_link(
        db, prep_sample_idx=ps_x, study_idx=w["study_1"], created_by_idx=owner
    )
    await w["gate"]("mask_sample", w["mask_2"], ps_x, MASK_SAMPLE_COMPLETED)
    await w["gate"]("alignment_sample", w["align_3"], ps_x, ALIGNMENT_SAMPLE_COMPLETED)
    yield ps_x
    await teardown_entity_graph(db, study_idxs=[], biosample_idxs=[bs_x], prep_sample_idxs=[ps_x])
    await db.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await db.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)


@pytest_asyncio.fixture
async def tickets(ctx):
    """`block(action_id, ...)` and `sample(action_id, ...)` insert work tickets (and a
    block covering the samples); every ticket is deleted afterwards, its block with it."""
    db = ctx["pool"]
    principal = ctx["wet_session"]["principal_idx"]
    made: list[int] = []
    actions: dict[str, bool] = {}

    async def _action(action_id: str, target_kind: str) -> None:
        if action_id not in actions:
            actions[action_id] = await seed_action_if_absent(
                db, action_id=action_id, version=_TEST_VERSION, target_kind=target_kind
            )

    def _failure(state: str) -> tuple:
        if state != WorkTicketState.FAILED:
            return (None, None, None, None)
        return (
            FailureType.PERMANENT.value,
            WorkTicketFailureStage.STEP_RUN.value,
            "align_sharded",
            "step failed in a test",
        )

    async def block(
        action_id: str,
        *,
        samples: list[int],
        state: str,
        mask_idx: int,
        alignment_idx: int | None = None,
        block_state: str = BLOCK_PENDING,
    ) -> int:
        await _action(action_id, ScopeTargetKind.BLOCK.value)
        async with db.acquire() as conn, conn.transaction():
            block_idx = await create_block(conn)
            await add_block_members(
                conn, block_idx=block_idx, members=[(ps, 1, 10) for ps in samples]
            )
        ticket = await db.fetchval(
            "INSERT INTO qiita.work_ticket ("
            "  action_id, action_version, originator_principal_idx, scope_target_kind,"
            "  block_idx, mask_idx, alignment_idx, state,"
            "  failure_type, failure_stage, failure_step_name, failure_reason"
            ") VALUES ($1, $2, $3, $12::qiita.scope_target_kind, $4, $5, $6,"
            "          $7::qiita.work_ticket_state,"
            "          $8::qiita.failure_type, $9::qiita.work_ticket_failure_stage, $10, $11)"
            " RETURNING work_ticket_idx",
            action_id,
            _TEST_VERSION,
            principal,
            block_idx,
            mask_idx,
            alignment_idx,
            state,
            *_failure(state),
            ScopeTargetKind.BLOCK.value,
        )
        await db.execute(
            "UPDATE qiita.block SET work_ticket_idx = $2, state = $3 WHERE block_idx = $1",
            block_idx,
            ticket,
            block_state,
        )
        made.append(ticket)
        return ticket

    async def sample(
        action_id: str,
        *,
        prep_sample_idx: int,
        state: str,
        mask_idx: int,
        action_context: dict | None = None,
    ) -> int:
        await _action(action_id, ScopeTargetKind.PREP_SAMPLE.value)
        ticket = await db.fetchval(
            "INSERT INTO qiita.work_ticket ("
            "  action_id, action_version, originator_principal_idx, scope_target_kind,"
            "  prep_sample_idx, mask_idx, state, action_context,"
            "  failure_type, failure_stage, failure_step_name, failure_reason"
            ") VALUES ($1, $2, $3, $12::qiita.scope_target_kind, $4, $5,"
            "          $6::qiita.work_ticket_state,"
            "          $7::jsonb, $8::qiita.failure_type,"
            "          $9::qiita.work_ticket_failure_stage, $10, $11)"
            " RETURNING work_ticket_idx",
            action_id,
            _TEST_VERSION,
            principal,
            prep_sample_idx,
            mask_idx,
            state,
            json.dumps(action_context or {}),
            *_failure(state),
            ScopeTargetKind.PREP_SAMPLE.value,
        )
        made.append(ticket)
        return ticket

    yield {"block": block, "sample": sample}

    await db.execute(
        "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = ANY($1::bigint[])", made
    )
    for action_id, created in actions.items():
        await delete_action_if_created(
            db, action_id=action_id, version=_TEST_VERSION, created=created
        )


# ---------------------------------------------------------------------------
# Resolution through the route
# ---------------------------------------------------------------------------


async def test_resolve_across_pools_names_each_samples_member(
    ctx, alignment_group_world, other_pool_sample
):
    """ps_a (seed pool) is aligned on align_1; ps_x (another pool) on align_3. Both
    answer for the group, each naming its own member and mask; ps_c's pending gate
    has no ticket behind it, so it is not a run."""
    w = alignment_group_world
    resp = await ctx["user"].post(
        URL_DEFINITION_GROUP_RESOLVE,
        json=_body(
            DefinitionKind.ALIGNMENT, w["align_1"], [other_pool_sample, w["ps_c"], w["ps_a"]]
        ),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["group"] == sorted([w["align_1"], w["align_3"]])
    assert [s["prep_sample_idx"] for s in body["samples"]] == sorted(
        [w["ps_a"], w["ps_c"], other_pool_sample]
    )
    samples = _by_sample(resp)
    assert (samples[w["ps_a"]]["state"], samples[w["ps_a"]]["definition_idx"]) == (
        PROCESSED,
        w["align_1"],
    )
    assert samples[w["ps_a"]]["mask_idx"] == w["mask_idx"]
    x = samples[other_pool_sample]
    assert (x["state"], x["definition_idx"], x["mask_idx"]) == (
        PROCESSED,
        w["align_3"],
        w["mask_2"],
    )
    assert (samples[w["ps_c"]]["state"], samples[w["ps_c"]]["reason"]) == (
        NOT_PROCESSED,
        R.NO_RUN,
    )


async def test_resolve_a_mask_group(ctx, alignment_group_world, other_pool_sample):
    w = alignment_group_world
    resp = await ctx["user"].post(
        URL_DEFINITION_GROUP_RESOLVE,
        json=_body(DefinitionKind.MASK, w["mask_idx"], [w["ps_a"], other_pool_sample]),
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["group"] == sorted([w["mask_idx"], w["mask_2"]])
    samples = _by_sample(resp)
    assert samples[w["ps_a"]]["definition_idx"] == w["mask_idx"]
    assert samples[other_pool_sample]["definition_idx"] == w["mask_2"]
    assert samples[other_pool_sample]["mask_idx"] is None


# ---------------------------------------------------------------------------
# Tickets
# ---------------------------------------------------------------------------


async def test_a_covering_align_block_that_is_running_makes_the_sample_incomplete(
    ctx, alignment_group_world, tickets
):
    """A completed gate whose block was re-armed: the ticket is waiting again."""
    w = alignment_group_world
    ticket = await tickets["block"](
        ALIGN_ACTION_ID,
        samples=[w["ps_a"]],
        state=WorkTicketState.PENDING,
        mask_idx=w["mask_idx"],
        alignment_idx=w["align_1"],
    )
    resp = await ctx["user"].post(
        URL_DEFINITION_GROUP_RESOLVE,
        json=_body(DefinitionKind.ALIGNMENT, w["align_1"], [w["ps_a"]]),
    )
    assert resp.status_code == 200, resp.text
    s = _by_sample(resp)[w["ps_a"]]
    assert (s["state"], s["reason"], s["work_ticket_idx"], s["definition_idx"]) == (
        INCOMPLETE,
        R.IN_PROGRESS,
        ticket,
        w["align_1"],
    )


@pytest.mark.parametrize(
    ("ticket_state", "block_state"),
    [(WorkTicketState.CANCELLED, BLOCK_PENDING), (WorkTicketState.FAILED, BLOCK_ABANDONED)],
)
async def test_cancelled_tickets_and_abandoned_blocks_are_ignored(
    ctx, alignment_group_world, tickets, ticket_state, block_state
):
    """A block ticket under align_3 that was cancelled, or whose block was abandoned,
    leaves a pending gate nothing will finish: not a run, so ps_a resolves to
    align_1."""
    w = alignment_group_world
    await w["gate"]("alignment_sample", w["align_3"], w["ps_a"], ALIGNMENT_SAMPLE_PENDING)
    await tickets["block"](
        ALIGN_ACTION_ID,
        samples=[w["ps_a"]],
        state=ticket_state,
        mask_idx=w["mask_2"],
        alignment_idx=w["align_3"],
        block_state=block_state,
    )
    resp = await ctx["user"].post(
        URL_DEFINITION_GROUP_RESOLVE,
        json=_body(DefinitionKind.ALIGNMENT, w["align_1"], [w["ps_a"]]),
    )
    assert resp.status_code == 200, resp.text
    s = _by_sample(resp)[w["ps_a"]]
    assert (s["state"], s["definition_idx"]) == (PROCESSED, w["align_1"])


async def test_a_failed_per_sample_mask_ticket_under_the_newer_mask_is_incomplete(
    ctx, alignment_group_world, tickets
):
    """Only the sample's newest per-sample ticket counts: a failed ticket a later
    completed one superseded does not."""
    w = alignment_group_world
    newer = await tickets["sample"](
        READ_MASK_ACTION_ID,
        prep_sample_idx=w["ps_a"],
        state=WorkTicketState.FAILED,
        mask_idx=w["mask_2"],
    )
    body = _body(DefinitionKind.MASK, w["mask_idx"], [w["ps_a"]])
    resp = await ctx["user"].post(URL_DEFINITION_GROUP_RESOLVE, json=body)
    assert resp.status_code == 200, resp.text
    s = _by_sample(resp)[w["ps_a"]]
    assert (s["state"], s["reason"], s["work_ticket_idx"], s["definition_idx"]) == (
        INCOMPLETE,
        R.FAILED,
        newer,
        w["mask_2"],
    )

    await tickets["sample"](
        READ_MASK_ACTION_ID,
        prep_sample_idx=w["ps_a"],
        state=WorkTicketState.COMPLETED,
        mask_idx=w["mask_2"],
    )
    await w["gate"]("mask_sample", w["mask_2"], w["ps_a"], MASK_SAMPLE_COMPLETED)
    s = _by_sample(await ctx["user"].post(URL_DEFINITION_GROUP_RESOLVE, json=body))[w["ps_a"]]
    assert (s["state"], s["definition_idx"]) == (PROCESSED, w["mask_2"])


async def test_an_assembly_ticket_is_matched_to_the_processing_it_mints(
    ctx, alignment_group_world, tickets
):
    """A long-read-assembly ticket records no processing_idx; it counts for the
    processing whose params it mints, here over a completed gate (a same-version
    re-run)."""
    w = alignment_group_world
    db = ctx["pool"]
    params = {
        "workflow": LONG_READ_ASSEMBLY_ACTION_ID,
        "version": _TEST_VERSION,
        "mask_idx": w["mask_idx"],
        "probe": str(uuid.uuid4()),
    }
    async with db.acquire() as conn:
        row = await mint_processing(
            conn, workflow=LONG_READ_ASSEMBLY_ACTION_ID, version=_TEST_VERSION, params=params
        )
    processing_idx = row["processing_idx"]
    try:
        await db.execute(
            "INSERT INTO qiita.assembly_sample (processing_idx, prep_sample_idx, state)"
            " VALUES ($1, $2, $3)",
            processing_idx,
            w["ps_a"],
            ASSEMBLY_SAMPLE_COMPLETED,
        )
        body = _body(DefinitionKind.PROCESSING, processing_idx, [w["ps_a"]])
        s = _by_sample(await ctx["user"].post(URL_DEFINITION_GROUP_RESOLVE, json=body))[w["ps_a"]]
        assert (s["state"], s["definition_idx"], s["mask_idx"]) == (
            PROCESSED,
            processing_idx,
            w["mask_idx"],
        )

        # The minted params carry no `probe`, so this ticket mints a different
        # processing and leaves the sample processed.
        await tickets["sample"](
            LONG_READ_ASSEMBLY_ACTION_ID,
            prep_sample_idx=w["ps_a"],
            state=WorkTicketState.PROCESSING,
            mask_idx=w["mask_idx"],
            action_context={"mask_idx": w["mask_idx"]},
        )
        s = _by_sample(await ctx["user"].post(URL_DEFINITION_GROUP_RESOLVE, json=body))[w["ps_a"]]
        assert s["state"] == PROCESSED

        await db.execute(
            "UPDATE qiita.processing SET params = params - 'probe' WHERE processing_idx = $1",
            processing_idx,
        )
        s = _by_sample(await ctx["user"].post(URL_DEFINITION_GROUP_RESOLVE, json=body))[w["ps_a"]]
        assert (s["state"], s["reason"]) == (INCOMPLETE, R.IN_PROGRESS)
    finally:
        # Ahead of the entity sweep: assembly_sample references this processing with
        # no ON DELETE action, so the processing row cannot go while it exists.
        await db.execute(
            "DELETE FROM qiita.assembly_sample WHERE processing_idx = $1", processing_idx
        )
        await db.execute("DELETE FROM qiita.processing WHERE processing_idx = $1", processing_idx)


# ---------------------------------------------------------------------------
# Authorization
# ---------------------------------------------------------------------------


async def test_resolve_refuses_a_cohort_with_an_unreadable_study_403(ctx, alignment_group_world):
    """ps_b is in study_2 only; ps_e is in study_1 and study_2. Either refuses the
    whole request rather than narrowing it."""
    w = alignment_group_world
    for samples in ([w["ps_a"], w["ps_b"]], [w["ps_e"]]):
        resp = await ctx["user"].post(
            URL_DEFINITION_GROUP_RESOLVE,
            json=_body(DefinitionKind.ALIGNMENT, w["align_1"], samples),
        )
        assert resp.status_code == 403, resp.text
        assert str(w["study_2"]) in resp.json()["detail"]


async def test_resolve_refuses_an_orphaned_sample_403(ctx, alignment_group_world):
    w = alignment_group_world
    resp = await ctx["user"].post(
        URL_DEFINITION_GROUP_RESOLVE,
        json=_body(DefinitionKind.ALIGNMENT, w["align_1"], [w["ps_d"]]),
    )
    assert resp.status_code == 403, resp.text


async def test_resolve_unknown_definition_404_before_access(ctx, alignment_group_world):
    w = alignment_group_world
    resp = await ctx["user"].post(
        URL_DEFINITION_GROUP_RESOLVE, json=_body(DefinitionKind.ALIGNMENT, 999_999_999, [w["ps_b"]])
    )
    assert resp.status_code == 404, resp.text


async def test_resolve_without_prep_sample_read_403(ctx, alignment_group_world, make_pat_client):
    w = alignment_group_world
    client = await make_pat_client(
        label="no-prep-sample-read", scopes=[Scope.SELF_PROFILE, Scope.ALIGNMENT_DOGET]
    )
    resp = await client.post(
        URL_DEFINITION_GROUP_RESOLVE,
        json=_body(DefinitionKind.ALIGNMENT, w["align_1"], [w["ps_a"]]),
    )
    assert resp.status_code == 403, resp.text


async def test_resolve_refuses_a_service_account(
    ctx, alignment_group_world, compute_worker_service_account
):
    from qiita_control_plane.main import app

    w = alignment_group_world
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {compute_worker_service_account['token']}"},
    ) as sa:
        resp = await sa.post(
            URL_DEFINITION_GROUP_RESOLVE,
            json=_body(DefinitionKind.ALIGNMENT, w["align_1"], [w["ps_a"]]),
        )
    assert resp.status_code == 403, resp.text


async def test_resolve_rejects_an_empty_cohort_422(ctx, alignment_group_world):
    resp = await ctx["user"].post(
        URL_DEFINITION_GROUP_RESOLVE,
        json=_body(DefinitionKind.ALIGNMENT, alignment_group_world["align_1"], []),
    )
    assert resp.status_code == 422, resp.text


async def test_resolve_group_refuses_a_read_committed_transaction(postgres_pool):
    """The resolver reads gates, tickets and groups in several statements, so it
    refuses a transaction in which each statement takes its own snapshot."""
    async with postgres_pool.acquire() as conn, conn.transaction():
        with pytest.raises(RuntimeError, match="REPEATABLE READ"):
            await resolve_group(
                conn, kind=DefinitionKind.MASK, definition_idx=1, prep_sample_idxs=[1]
            )
