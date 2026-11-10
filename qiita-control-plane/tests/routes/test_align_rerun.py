"""DB tests for re-running a finished align block (`rerun-block`) and for deleting the
rows of a prep_sample no gate tracks (`purge-sample-rows`).

The data plane and the compute backend are absent at this tier. Dispatch is recorded,
the align action's step list is stubbed (the seeded action has none), and the
per-sample delete is replaced by a recording stub. What is checked is what the
control plane owns: the refusals, and the Postgres rows each call changes or leaves.
"""

import asyncio
import secrets
import time
import types

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.actions import (
    ALIGN_ACTION_ID,
    BLOCK_MASK_ACTION_ID,
    ESTIMATE_FEATURE_TABLE_ACTION_ID,
)
from qiita_common.api_paths import (
    URL_ALIGNMENT_DEFINITION_ABANDON_PLAN,
    URL_ALIGNMENT_DEFINITION_PURGE_SAMPLE_ROWS,
    URL_ALIGNMENT_DEFINITION_RERUN_BLOCK,
)
from qiita_common.models import WorkTicketState

from qiita_control_plane.actions import align_rerun
from qiita_control_plane.actions.library import reconcile_alignment_block
from qiita_control_plane.align_planner import ALIGN_ACTION_VERSION
from qiita_control_plane.repositories.alignment_definition import mint_alignment_definition
from qiita_control_plane.repositories.block import (
    add_block_members,
    create_block,
    set_block_work_ticket,
)
from qiita_control_plane.repositories.mask_definition import mint_mask_definition
from qiita_control_plane.testing.db_seeds import (
    delete_action_if_created,
    seed_action_if_absent,
    seed_biosample_with_sequenced_prep_sample,
)
from qiita_control_plane.testing.db_teardown import teardown_entity_graph

pytestmark = pytest.mark.db

_REASON = "rows zeroed in job memory before they were written"
_ROUTES = "qiita_control_plane.routes.alignment"
_ACTION = "qiita_control_plane.actions.align_rerun"
_STEPS = ["align_sharded", "delete-alignment-block", "register-files", "reconcile-alignment-block"]
# The shipped YAML's wiring, as far as `registered_output_step` reads it.
_STEP_IO = {
    "align_sharded": ([], ["alignment", "alignment_staging_dir"]),
    "register-files": (["alignment_staging_dir"], []),
}
_OTHER_VERSION = "rerun-test"


def _step_stubs():
    return [
        types.SimpleNamespace(
            name=n, inputs=_STEP_IO.get(n, ([], []))[0], outputs=_STEP_IO.get(n, ([], []))[1]
        )
        for n in _STEPS
    ]


@pytest.fixture
def dispatched(monkeypatch):
    """Record dispatches, stub the align action's steps and the step-progress check
    the seeded action cannot satisfy. Returns the dispatched work_ticket_idxs."""
    calls: list[int] = []

    async def _definition(*_a, **_k):
        return types.SimpleNamespace(steps=_step_stubs())

    monkeypatch.setattr(f"{_ROUTES}._fetch_action", _definition)
    monkeypatch.setattr(f"{_ACTION}.check_step_progress", lambda *_a, **_k: None)
    monkeypatch.setattr(f"{_ROUTES}.schedule_dispatch", lambda _app, idx: calls.append(idx))
    return calls


@pytest_asyncio.fixture
async def client(postgres_pool, human_admin_session, tmp_path):
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    saved_settings = getattr(app.state, "settings", None)
    saved_backend = getattr(app.state, "compute_backend_client", None)
    app.state.settings = Settings(
        database_url="unused",
        flight_signing_key=b"\x00" * 32,
        data_plane_url="unused",
        path_scratch_ticket=tmp_path,
    )
    app.state.compute_backend_client = object()
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={"Authorization": f"Bearer {human_admin_session['token']}"},
        ) as ac:
            yield ac
    finally:
        app.state.settings = saved_settings
        app.state.compute_backend_client = saved_backend


@pytest_asyncio.fixture
async def rr(postgres_pool, human_admin_session, tmp_path):
    """Three sequenced prep_samples, an active alignment and an active mask, and
    helpers to mint align block tickets, gates and step rows."""
    owner = human_admin_session["principal_idx"]
    suffix = secrets.token_hex(4)
    seeded = [
        await seed_biosample_with_sequenced_prep_sample(postgres_pool, owner_idx=owner)
        for _ in range(3)
    ]
    biosamples = [b for b, _p in seeded]
    samples = [p for _b, p in seeded]
    reference_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, created_by_idx)"
        " VALUES ($1, '1', 'sequence_reference', $2) RETURNING reference_idx",
        f"align-rerun-{suffix}",
        owner,
    )
    async with postgres_pool.acquire() as conn:
        mask_idx = (
            await mint_mask_definition(
                conn,
                filter_workflow="read-mask",
                filter_version="1.0.0",
                params={"workflow": "read-mask", "s": suffix},
                principal_idx=owner,
            )
        )["mask_idx"]
        alignment_idx = (
            await mint_alignment_definition(
                conn,
                params={
                    "reference_idx": reference_idx,
                    "aligner": "minimap2",
                    "mask_idx": mask_idx,
                    "shard_ids": [0],
                    "s": suffix,
                },
                principal_idx=owner,
            )
        )["alignment_idx"]
    created = [
        (
            aid,
            ver,
            await seed_action_if_absent(
                postgres_pool, action_id=aid, version=ver, target_kind=kind
            ),
        )
        for aid, ver, kind in [
            (ALIGN_ACTION_ID, ALIGN_ACTION_VERSION, "block"),
            (BLOCK_MASK_ACTION_ID, _OTHER_VERSION, "block"),
            (ESTIMATE_FEATURE_TABLE_ACTION_ID, _OTHER_VERSION, "reference"),
        ]
    ]
    blocks: list[int] = []
    tickets: list[int] = []

    async def make_block(
        members,
        *,
        block_state="completed",
        ticket_state=WorkTicketState.COMPLETED,
        action_id=ALIGN_ACTION_ID,
        alignment=None,
        context_alignment=None,
    ) -> tuple[int, int]:
        async with postgres_pool.acquire() as conn, conn.transaction():
            block_idx = await create_block(conn)
            await add_block_members(conn, block_idx=block_idx, members=members)
        blocks.append(block_idx)
        failed = ticket_state == WorkTicketState.FAILED
        aln = alignment or alignment_idx
        wt_idx = await postgres_pool.fetchval(
            "INSERT INTO qiita.work_ticket ("
            "  action_id, action_version, originator_principal_idx, scope_target_kind,"
            "  block_idx, alignment_idx, mask_idx, action_context, state,"
            "  failure_type, failure_stage, failure_step_name, failure_reason, notified_at"
            ") VALUES ($1, $2, $3, 'block', $4, $5, $6, $7::jsonb, $8::qiita.work_ticket_state,"
            "          $9::qiita.failure_type, $10::qiita.work_ticket_failure_stage, $11, $12,"
            "          now())"
            " RETURNING work_ticket_idx",
            action_id,
            ALIGN_ACTION_VERSION if action_id == ALIGN_ACTION_ID else _OTHER_VERSION,
            owner,
            block_idx,
            aln,
            mask_idx,
            f'{{"alignment_idx": {context_alignment or aln}}}',
            ticket_state,
            "permanent" if failed else None,
            "step_run" if failed else None,
            "align_sharded" if failed else None,
            "step timed out" if failed else None,
        )
        tickets.append(wt_idx)
        async with postgres_pool.acquire() as conn, conn.transaction():
            await set_block_work_ticket(conn, block_idx=block_idx, work_ticket_idx=wt_idx)
        await postgres_pool.execute(
            "UPDATE qiita.block SET state = $2 WHERE block_idx = $1", block_idx, block_state
        )
        return block_idx, wt_idx

    async def set_gate(prep_sample_idx, state):
        invalidated = state == "invalidated"
        await postgres_pool.execute(
            "INSERT INTO qiita.alignment_sample (alignment_idx, prep_sample_idx, state,"
            "  invalidated_at, invalidated_by_idx, invalidation_reason)"
            " VALUES ($1, $2, $3, CASE WHEN $4 THEN now() END, CASE WHEN $4 THEN $5::bigint END,"
            "         CASE WHEN $4 THEN 'r' END)"
            " ON CONFLICT (alignment_idx, prep_sample_idx) DO UPDATE SET state = $3",
            alignment_idx,
            prep_sample_idx,
            state,
            invalidated,
            owner,
        )

    async def add_step(wt, step_index, *, attempt=0, state="completed", job_id=None, mkdir=True):
        """A slurm step row. Every state but `submitting` and `failed` needs a job id,
        and `failed` needs its failure fields (the table's CHECKs)."""
        name = _STEPS[step_index]
        if job_id is None and state not in ("submitting", "failed"):
            job_id = 900_000 + 10 * step_index + attempt
        failed = state == "failed"
        await postgres_pool.execute(
            "INSERT INTO qiita.work_ticket_step (work_ticket_idx, step_index, attempt,"
            "  step_name, compute_target, state, slurm_job_id, job_name,"
            "  failure_kind, failure_reason)"
            " VALUES ($1, $2, $3, $4, 'slurm', $5, $6, $7, $8, $9)",
            wt,
            step_index,
            attempt,
            name,
            state,
            job_id,
            f"qiita-wt{wt}-{name}-a{attempt}",
            "timeout" if failed else None,
            "step timed out" if failed else None,
        )
        if mkdir:
            (tmp_path / str(wt) / name / f"attempt-{attempt}").mkdir(parents=True)

    async def full_steps(wt, *, align_attempt=0, mkdir=True):
        await add_step(wt, 0, attempt=align_attempt, mkdir=mkdir)
        for i in (1, 2, 3):
            await add_step(wt, i, mkdir=False)

    async def deprecate_mask():
        await postgres_pool.execute(
            "UPDATE qiita.mask_definition SET status = 'deprecated', deprecated_at = now(),"
            "       deprecated_by_idx = $2, deprecation_reason = 'superseded'"
            " WHERE mask_idx = $1",
            mask_idx,
            owner,
        )

    yield types.SimpleNamespace(
        pool=postgres_pool,
        owner=owner,
        samples=samples,
        alignment_idx=alignment_idx,
        mask_idx=mask_idx,
        reference_idx=reference_idx,
        ticket_root=tmp_path,
        make_block=make_block,
        set_gate=set_gate,
        add_step=add_step,
        full_steps=full_steps,
        deprecate_mask=deprecate_mask,
        tickets=tickets,
    )

    await teardown_entity_graph(
        postgres_pool, study_idxs=[], biosample_idxs=biosamples, prep_sample_idxs=samples
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = ANY($1::bigint[])", tickets
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.block WHERE block_idx = ANY($1::bigint[])", blocks
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1", alignment_idx
    )
    await postgres_pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)
    await postgres_pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    for aid, ver, was_created in created:
        await delete_action_if_created(
            postgres_pool, action_id=aid, version=ver, created=was_created
        )


async def _rerun(client, alignment_idx, wt, **extra):
    return await client.post(
        URL_ALIGNMENT_DEFINITION_RERUN_BLOCK.format(alignment_idx=alignment_idx),
        json={"work_ticket_idx": wt, "reason": _REASON, **extra},
    )


async def _snapshot(rr, wt, block_idx):
    """Everything a refused call must leave as it was."""
    pool = rr.pool
    return {
        "ticket": await pool.fetchval(
            "SELECT state::text FROM qiita.work_ticket WHERE work_ticket_idx = $1", wt
        ),
        "block": await pool.fetchval(
            "SELECT state FROM qiita.block WHERE block_idx = $1", block_idx
        ),
        "steps": await pool.fetchval(
            "SELECT count(*) FROM qiita.work_ticket_step WHERE work_ticket_idx = $1", wt
        ),
        "gates": dict(
            await pool.fetch(
                "SELECT prep_sample_idx, state FROM qiita.alignment_sample"
                " WHERE alignment_idx = $1",
                rr.alignment_idx,
            )
        ),
    }


async def _assert_refused(client, rr, wt, block_idx, reason, *, status=409, **extra):
    before = await _snapshot(rr, wt, block_idx)
    resp = await _rerun(client, rr.alignment_idx, wt, **extra)
    assert resp.status_code == status, resp.text
    assert resp.json()["detail"]["reason"] == reason
    assert await _snapshot(rr, wt, block_idx) == before
    return resp


# ---------------------------------------------------------------------------
# Re-arming a completed block
# ---------------------------------------------------------------------------


async def test_rerun_rearms_a_completed_block_and_reconcile_completes_it(client, rr, dispatched):
    """Sample A is wholly in the block; sample B is split with a completed sibling.
    The re-arm deletes every step row, sets block and both gates pending, and
    dispatches. Reconcile then completes the block and both gates again."""
    a, b, _c = rr.samples
    block, wt = await rr.make_block([(a, 1, 100), (b, 1, 50)])
    sibling, _ = await rr.make_block([(b, 51, 100)])
    await rr.full_steps(wt)
    await rr.set_gate(a, "completed")
    await rr.set_gate(b, "completed")

    resp = await _rerun(client, rr.alignment_idx, wt)
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "alignment_idx": rr.alignment_idx,
        "work_ticket_idx": wt,
        "block_idx": block,
        "rerun_of": "completed",
        "mask_override": False,
        "pending_prep_sample_idx": sorted([a, b]),
        "step_rows_deleted": 4,
    }
    assert dispatched == [wt]
    after = await _snapshot(rr, wt, block)
    assert after["ticket"] == "pending"
    assert after["block"] == "pending"
    assert after["steps"] == 0
    assert after["gates"] == {a: "pending", b: "pending"}
    row = await rr.pool.fetchrow(
        "SELECT notified_at, retry_count, dispatch_held FROM qiita.work_ticket"
        " WHERE work_ticket_idx = $1",
        wt,
    )
    assert dict(row) == {"notified_at": None, "retry_count": 0, "dispatch_held": False}
    assert (
        await rr.pool.fetchval("SELECT state FROM qiita.block WHERE block_idx = $1", sibling)
        == "completed"
    )

    finalized = await reconcile_alignment_block(
        rr.pool, block_idx=block, alignment_idx=rr.alignment_idx
    )
    assert sorted(finalized["finalized_samples"]) == sorted([a, b])
    assert (await _snapshot(rr, wt, block))["gates"] == {a: "completed", b: "completed"}


async def test_rerun_needs_the_lifecycle_scope(
    postgres_pool, rr, wet_lab_admin_session, dispatched
):
    from qiita_control_plane.main import app

    _block, wt = await rr.make_block([(rr.samples[0], 1, 100)])
    app.state.pool = postgres_pool
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {wet_lab_admin_session['token']}"},
    ) as ac:
        resp = await _rerun(ac, rr.alignment_idx, wt)
    assert resp.status_code == 403
    assert dispatched == []


async def test_rerun_unknown_ticket_is_404(client, rr, dispatched):
    resp = await _rerun(client, rr.alignment_idx, 2_000_000_000)
    assert resp.status_code == 404
    assert resp.json()["detail"]["reason"] == "not_found"


async def test_rerun_refuses_a_ticket_that_is_not_align(client, rr, dispatched):
    a = rr.samples[0]
    block, wt = await rr.make_block([(a, 1, 100)], action_id=BLOCK_MASK_ACTION_ID)
    await rr.full_steps(wt)
    await rr.set_gate(a, "completed")
    await _assert_refused(client, rr, wt, block, "wrong_action")


@pytest.mark.parametrize("where", ["column", "context"])
async def test_rerun_refuses_another_alignments_ticket(client, rr, dispatched, where):
    a = rr.samples[0]
    other = 2_000_000_000
    kwargs = {"context_alignment": other} if where == "context" else {}
    block, wt = await rr.make_block([(a, 1, 100)], **kwargs)
    await rr.full_steps(wt)
    await rr.set_gate(a, "completed")
    if where == "column":
        # The context names this alignment, the column does not.
        await rr.pool.execute(
            "UPDATE qiita.work_ticket SET alignment_idx = NULL WHERE work_ticket_idx = $1", wt
        )
    await _assert_refused(client, rr, wt, block, "wrong_alignment")


@pytest.mark.parametrize(
    ("ticket_state", "block_state"),
    [
        (WorkTicketState.PROCESSING, "pending"),
        (WorkTicketState.CANCELLED, "pending"),
        (WorkTicketState.COMPLETED, "pending"),
        (WorkTicketState.COMPLETED, "abandoned"),
    ],
)
async def test_rerun_refuses_other_states(client, rr, dispatched, ticket_state, block_state):
    a = rr.samples[0]
    block, wt = await rr.make_block(
        [(a, 1, 100)], ticket_state=ticket_state, block_state=block_state
    )
    await rr.full_steps(wt)
    await rr.set_gate(a, "completed")
    await _assert_refused(client, rr, wt, block, "wrong_state")


async def test_rerun_refuses_a_deprecated_alignment(client, rr, dispatched):
    a = rr.samples[0]
    block, wt = await rr.make_block([(a, 1, 100)])
    await rr.full_steps(wt)
    await rr.set_gate(a, "completed")
    await rr.pool.execute(
        "UPDATE qiita.alignment_definition SET status = 'deprecated', deprecated_at = now(),"
        "       deprecated_by_idx = $2, deprecation_reason = 'void'"
        " WHERE alignment_idx = $1",
        rr.alignment_idx,
        rr.owner,
    )
    await _assert_refused(client, rr, wt, block, "alignment_deprecated")
    await _assert_refused(client, rr, wt, block, "mask_not_deprecated", allow_deprecated_mask=True)


async def test_the_mask_override_is_needed_on_a_deprecated_mask_and_refused_on_an_active_one(
    client, rr, dispatched
):
    a = rr.samples[0]
    block, wt = await rr.make_block([(a, 1, 100)])
    await rr.full_steps(wt)
    await rr.set_gate(a, "completed")
    await _assert_refused(client, rr, wt, block, "mask_not_deprecated", allow_deprecated_mask=True)
    await rr.deprecate_mask()
    await _assert_refused(client, rr, wt, block, "mask_deprecated")
    resp = await _rerun(client, rr.alignment_idx, wt, allow_deprecated_mask=True)
    assert resp.status_code == 200, resp.text
    assert resp.json()["mask_override"] is True


@pytest.mark.parametrize("gate", ["invalidated", None])
async def test_rerun_refuses_an_invalidated_or_missing_gate(client, rr, dispatched, gate):
    a, b, _c = rr.samples
    block, wt = await rr.make_block([(a, 1, 100), (b, 1, 100)])
    await rr.full_steps(wt)
    await rr.set_gate(a, "completed")
    if gate is not None:
        await rr.set_gate(b, gate)
    resp = await _assert_refused(
        client, rr, wt, block, "gate_invalidated" if gate else "gate_missing"
    )
    assert resp.json()["detail"]["prep_sample_idx"] == [b]


async def test_rerun_waits_for_a_sibling_block_and_ignores_an_abandoned_one(client, rr, dispatched):
    """A sibling covering the same sample that is not completed refuses the re-arm;
    once it completes the re-arm goes through. An abandoned sibling owes nothing."""
    a, b, _c = rr.samples
    block, wt = await rr.make_block([(a, 1, 50)])
    sibling, _ = await rr.make_block([(a, 51, 100)], block_state="pending")
    await rr.make_block([(a, 101, 150)], block_state="abandoned")
    await rr.full_steps(wt)
    await rr.set_gate(a, "completed")
    resp = await _assert_refused(client, rr, wt, block, "sibling_incomplete")
    assert resp.json()["detail"]["prep_sample_idx"] == [a]

    await rr.pool.execute(
        "UPDATE qiita.block SET state = 'completed' WHERE block_idx = $1", sibling
    )
    assert (await _rerun(client, rr.alignment_idx, wt)).status_code == 200


@pytest.mark.parametrize("key", ["alignment_idx", "denovo_alignment_idx"])
async def test_rerun_waits_for_an_unfinished_feature_table(client, rr, dispatched, key):
    a = rr.samples[0]
    block, wt = await rr.make_block([(a, 1, 100)])
    await rr.full_steps(wt)
    await rr.set_gate(a, "completed")
    ft = await rr.pool.fetchval(
        "INSERT INTO qiita.work_ticket (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, action_context, state)"
        " VALUES ($1, $2, $3, 'reference', $5, $4::jsonb, 'processing')"
        " RETURNING work_ticket_idx",
        ESTIMATE_FEATURE_TABLE_ACTION_ID,
        _OTHER_VERSION,
        rr.owner,
        f'{{"{key}": {rr.alignment_idx}}}',
        rr.reference_idx,
    )
    rr.tickets.append(ft)
    resp = await _assert_refused(client, rr, wt, block, "feature_table_in_flight")
    assert resp.json()["detail"]["work_ticket_idx"] == [ft]


@pytest.mark.parametrize("count", [0, 2])
async def test_rerun_needs_exactly_one_completed_producing_row(client, rr, dispatched, count):
    a = rr.samples[0]
    block, wt = await rr.make_block([(a, 1, 100)])
    for attempt in range(count):
        await rr.add_step(wt, 0, attempt=attempt)
    await rr.set_gate(a, "completed")
    await _assert_refused(client, rr, wt, block, "completed_step_rows")


async def test_rerun_refuses_when_the_registered_attempt_dir_is_gone(client, rr, dispatched):
    """Attempts 0 and 1 exist on disk but the registered attempt 2 does not: the runner
    would pick attempt 2 again, and with it the registered file's lake name."""
    a = rr.samples[0]
    block, wt = await rr.make_block([(a, 1, 100)])
    await rr.full_steps(wt, align_attempt=2, mkdir=False)
    for attempt in (0, 1):
        (rr.ticket_root / str(wt) / "align_sharded" / f"attempt-{attempt}").mkdir(parents=True)
    await rr.set_gate(a, "completed")
    await _assert_refused(client, rr, wt, block, "registered_attempt_missing")


# ---------------------------------------------------------------------------
# Retrying a failed re-run under a deprecated mask
# ---------------------------------------------------------------------------


async def test_a_failed_rerun_is_retried_only_with_the_override(client, rr, dispatched):
    """A failed re-run's block and gates are pending. Under an active mask it goes
    through /run, so this route refuses it; under a deprecated mask it needs the
    override. The retry keeps completed rows and an in-flight row with a job, and
    drops the failed one, as /run does."""
    a = rr.samples[0]
    block, wt = await rr.make_block(
        [(a, 1, 100)], ticket_state=WorkTicketState.FAILED, block_state="pending"
    )
    await rr.add_step(wt, 0, attempt=1)
    await rr.add_step(wt, 1, attempt=0, state="failed", mkdir=False)
    await rr.add_step(wt, 1, attempt=1, state="running", job_id=4242, mkdir=False)
    await rr.set_gate(a, "pending")
    await _assert_refused(client, rr, wt, block, "wrong_state")
    await rr.deprecate_mask()
    await _assert_refused(client, rr, wt, block, "mask_deprecated")

    resp = await _rerun(client, rr.alignment_idx, wt, allow_deprecated_mask=True)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["rerun_of"] == "failed"
    assert body["step_rows_deleted"] == 1
    assert body["pending_prep_sample_idx"] == []
    kept = await rr.pool.fetch(
        "SELECT step_index, attempt, state FROM qiita.work_ticket_step"
        " WHERE work_ticket_idx = $1 ORDER BY step_index, attempt",
        wt,
    )
    assert [tuple(r) for r in kept] == [(0, 1, "completed"), (1, 1, "running")]
    state = await _snapshot(rr, wt, block)
    assert (state["ticket"], state["block"]) == ("pending", "pending")
    row = await rr.pool.fetchrow(
        "SELECT failure_type, failure_reason FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        wt,
    )
    assert dict(row) == {"failure_type": None, "failure_reason": None}
    assert dispatched == [wt]


async def test_a_failed_rerun_with_no_producing_row_skips_the_attempt_check(client, rr, dispatched):
    """A re-run that failed inside align_sharded has no completed row for it, and the
    runner moves past the existing attempts, so no directory has to exist."""
    a = rr.samples[0]
    _block, wt = await rr.make_block(
        [(a, 1, 100)], ticket_state=WorkTicketState.FAILED, block_state="pending"
    )
    await rr.add_step(wt, 0, attempt=1, state="failed", mkdir=False)
    await rr.set_gate(a, "pending")
    await rr.deprecate_mask()
    resp = await _rerun(client, rr.alignment_idx, wt, allow_deprecated_mask=True)
    assert resp.status_code == 200, resp.text


# ---------------------------------------------------------------------------
# abandon-plan and a re-armed block
# ---------------------------------------------------------------------------


async def test_a_rerun_stamps_the_block_and_abandon_plan_refuses_it_until_allowed(
    client, rr, dispatched, monkeypatch
):
    """A failed re-run's block is pending with pending gates, like a block that never
    finished; `rearmed_at` is what abandon-plan tells them apart by. A block never
    re-armed is not stamped."""
    purged: list[dict] = []

    async def _purge(*, alignment_idx, members, signing_key, data_plane_url):
        purged.append({"alignment_idx": alignment_idx, "members": members})
        return 0

    monkeypatch.setattr(
        "qiita_control_plane.actions.align_abandon.delete_alignment_block_data", _purge
    )
    a, b, c = rr.samples
    block, wt = await rr.make_block([(a, 1, 100)])
    untouched, _ = await rr.make_block([(b, 1, 100)])
    # Never re-armed and unfinished: abandon-plan collects it, and must not list it.
    never_finished, _ = await rr.make_block(
        [(c, 1, 100)], ticket_state=WorkTicketState.FAILED, block_state="pending"
    )
    await rr.full_steps(wt)
    await rr.set_gate(a, "completed")
    await rr.set_gate(b, "completed")
    await rr.set_gate(c, "pending")
    assert (await _rerun(client, rr.alignment_idx, wt)).status_code == 200
    url = URL_ALIGNMENT_DEFINITION_ABANDON_PLAN.format(alignment_idx=rr.alignment_idx)
    # While the re-run is unfinished the in-flight refusal comes first; the override
    # does not lift it.
    resp = await client.post(url, json={"allow_rearmed": True})
    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"]["work_ticket_idx"] == [wt]
    stamps = dict(
        await rr.pool.fetch(
            "SELECT block_idx, rearmed_at IS NOT NULL FROM qiita.block"
            " WHERE block_idx = ANY($1::bigint[])",
            [block, untouched],
        )
    )
    assert stamps == {block: True, untouched: False}
    assert (
        await rr.pool.fetchval(
            "SELECT rearmed_at IS NULL FROM qiita.block WHERE block_idx = $1", never_finished
        )
        is True
    )

    # The re-run failed.
    await rr.pool.execute(
        "UPDATE qiita.work_ticket SET state = 'failed', failure_type = 'permanent',"
        " failure_stage = 'step_run', failure_step_name = 'align_sharded',"
        " failure_reason = 'x' WHERE work_ticket_idx = $1",
        wt,
    )
    resp = await client.post(url)
    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"]["block_idx"] == [block]
    assert purged == []
    assert (await _snapshot(rr, wt, block))["block"] == "pending"

    resp = await client.post(url, json={"allow_rearmed": True})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["abandoned_block_idx"] == sorted([block, never_finished])
    assert body["cleared_prep_sample_idx"] == sorted([a, c])
    gates = (await _snapshot(rr, wt, block))["gates"]
    assert a not in gates and c not in gates and gates[b] == "completed"
    assert purged


# ---------------------------------------------------------------------------
# The time bound
# ---------------------------------------------------------------------------


class _CountingConn:
    """Forwards to a real connection and counts the statements sent through it."""

    _STATEMENT_METHODS = frozenset({"execute", "executemany", "fetch", "fetchrow", "fetchval"})

    def __init__(self, conn):
        self._conn = conn
        self.statements = 0

    def __getattr__(self, name):
        attr = getattr(self._conn, name)
        if name not in self._STATEMENT_METHODS:
            return attr

        async def counted(*args, **kwargs):
            self.statements += 1
            return await attr(*args, **kwargs)

        return counted


class _CountingPool:
    def __init__(self, pool):
        self._pool = pool
        self.conns: list[_CountingConn] = []

    async def acquire(self, *, timeout=None):
        conn = _CountingConn(await self._pool.acquire(timeout=timeout))
        self.conns.append(conn)
        return conn

    async def release(self, conn):
        await self._pool.release(conn._conn)


async def _rerun_direct(rr, wt, pool=None, **kw):
    kw.setdefault("deadline", align_rerun.pre_txn_deadline())
    kw.setdefault("allow_deprecated_mask", False)
    return await align_rerun.rerun_align_block(
        pool or rr.pool,
        alignment_idx=rr.alignment_idx,
        work_ticket_idx=wt,
        reason=_REASON,
        principal_idx=rr.owner,
        ticket_root=rr.ticket_root,
        action_steps=_step_stubs(),
        **kw,
    )


async def test_each_path_runs_the_statements_the_bound_counts(rr, dispatched):
    """`RERUN_REQUEST_BOUND_S` assumes `_MAX_STATEMENTS` statements under the timeout:
    a statement added to either path fails here instead of silently stretching it."""
    a, b, _c = rr.samples
    _block, wt = await rr.make_block([(a, 1, 100)])
    await rr.full_steps(wt)
    await rr.set_gate(a, "completed")
    pool = _CountingPool(rr.pool)
    assert (await _rerun_direct(rr, wt, pool))["rerun_of"] == "completed"
    assert pool.conns[0].statements == align_rerun._MAX_STATEMENTS

    _block, wt = await rr.make_block(
        [(b, 1, 100)], ticket_state=WorkTicketState.FAILED, block_state="pending"
    )
    await rr.add_step(wt, 0, attempt=1)
    await rr.set_gate(b, "pending")
    await rr.deprecate_mask()
    pool = _CountingPool(rr.pool)
    outcome = await _rerun_direct(rr, wt, pool, allow_deprecated_mask=True)
    assert outcome["rerun_of"] == "failed"
    assert pool.conns[0].statements == align_rerun._MAX_STATEMENTS


async def test_a_call_past_its_deadline_changes_nothing(rr, dispatched):
    a = rr.samples[0]
    block, wt = await rr.make_block([(a, 1, 100)])
    await rr.full_steps(wt)
    await rr.set_gate(a, "completed")
    before = await _snapshot(rr, wt, block)
    with pytest.raises(align_rerun.AlignRerunRefused) as exc:
        await _rerun_direct(rr, wt, deadline=time.monotonic() - 1)
    assert (exc.value.reason, exc.value.status) == ("deadline_passed", 503)
    assert await _snapshot(rr, wt, block) == before


class _ExhaustedPool:
    async def acquire(self, *, timeout=None):
        raise TimeoutError


async def test_no_connection_within_the_budget_changes_nothing(rr, dispatched):
    a = rr.samples[0]
    block, wt = await rr.make_block([(a, 1, 100)])
    await rr.full_steps(wt)
    await rr.set_gate(a, "completed")
    before = await _snapshot(rr, wt, block)
    with pytest.raises(align_rerun.AlignRerunRefused) as exc:
        await _rerun_direct(rr, wt, _ExhaustedPool())
    assert (exc.value.reason, exc.value.status) == ("deadline_passed", 503)
    assert await _snapshot(rr, wt, block) == before


async def test_a_stalled_attempt_stat_is_a_503_that_changes_nothing(
    client, rr, dispatched, monkeypatch
):
    a = rr.samples[0]
    block, wt = await rr.make_block([(a, 1, 100)])
    await rr.full_steps(wt)
    await rr.set_gate(a, "completed")

    def _hung(_path):
        return asyncio.get_running_loop().create_future()  # never settles

    monkeypatch.setattr(f"{_ACTION}._stat_off_loop", _hung)
    monkeypatch.setattr(f"{_ACTION}._STAT_TIMEOUT_S", 0.05)
    await _assert_refused(client, rr, wt, block, "scratch_timeout", status=503)
    assert dispatched == []


async def test_an_unreadable_scratch_path_is_refused_not_read_as_gone(client, rr, dispatched):
    """A permission error is not a missing directory: refused with its own reason."""
    a = rr.samples[0]
    block, wt = await rr.make_block([(a, 1, 100)])
    await rr.full_steps(wt)
    await rr.set_gate(a, "completed")
    ticket_dir = rr.ticket_root / str(wt)
    ticket_dir.chmod(0)
    try:
        await _assert_refused(client, rr, wt, block, "registered_attempt_unreadable")
    finally:
        ticket_dir.chmod(0o755)


async def test_a_lock_wait_past_the_statement_timeout_is_a_503_that_changes_nothing(
    client, rr, dispatched
):
    """Another transaction holds the block row: the re-arm's block lock waits until
    `statement_timeout` cancels it, and the rollback leaves everything as it was."""
    a = rr.samples[0]
    block, wt = await rr.make_block([(a, 1, 100)])
    await rr.full_steps(wt)
    await rr.set_gate(a, "completed")
    async with rr.pool.acquire() as holder, holder.transaction():
        await holder.execute("SELECT 1 FROM qiita.block WHERE block_idx = $1 FOR UPDATE", block)
        resp = await _rerun(client, rr.alignment_idx, wt)
    assert resp.status_code == 503, resp.text
    assert resp.json()["detail"]["reason"] == "statement_timeout"
    after = await _snapshot(rr, wt, block)
    assert (after["ticket"], after["block"], after["steps"]) == ("completed", "completed", 4)
    assert dispatched == []


# ---------------------------------------------------------------------------
# purge-sample-rows
# ---------------------------------------------------------------------------


class _RecordingDelete:
    def __init__(self, rows_deleted=14_647, error=None):
        self.rows_deleted = rows_deleted
        self.error = error
        self.calls: list[tuple[int, int]] = []

    async def __call__(self, *, alignment_idx, prep_sample_idx, signing_key, data_plane_url):
        self.calls.append((alignment_idx, prep_sample_idx))
        if self.error is not None:
            raise self.error
        return self.rows_deleted


@pytest.fixture
def delete(monkeypatch):
    stub = _RecordingDelete()
    monkeypatch.setattr(f"{_ACTION}.delete_alignment_sample_data", stub)
    return stub


async def _purge(client, alignment_idx, prep_sample_idx):
    return await client.post(
        URL_ALIGNMENT_DEFINITION_PURGE_SAMPLE_ROWS.format(alignment_idx=alignment_idx),
        json={"prep_sample_idx": prep_sample_idx, "reason": _REASON},
    )


async def test_purge_deletes_an_ungated_prep_sample_including_zero(client, rr, delete):
    resp = await _purge(client, rr.alignment_idx, 0)
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "alignment_idx": rr.alignment_idx,
        "prep_sample_idx": 0,
        "rows_deleted": 14_647,
    }
    assert delete.calls == [(rr.alignment_idx, 0)]


@pytest.mark.parametrize("state", ["completed", "pending", "invalidated"])
async def test_purge_refuses_a_gated_prep_sample(client, rr, delete, state):
    a = rr.samples[0]
    await rr.set_gate(a, state)
    resp = await _purge(client, rr.alignment_idx, a)
    assert resp.status_code == 409
    assert delete.calls == []


async def test_purge_unknown_alignment_is_404_and_negative_idx_is_422(client, rr, delete):
    assert (await _purge(client, 2_000_000_000, 0)).status_code == 404
    assert (await _purge(client, rr.alignment_idx, -1)).status_code == 422
    assert delete.calls == []


async def test_a_failed_purge_is_502(client, rr, delete):
    import pyarrow.flight as flight

    delete.error = flight.FlightUnavailableError("data plane down")
    resp = await _purge(client, rr.alignment_idx, 0)
    assert resp.status_code == 502
    assert "again" in resp.json()["detail"]
