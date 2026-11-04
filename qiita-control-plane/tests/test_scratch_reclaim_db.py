"""Assertions are about this test's own tickets; the DB is shared with other tests."""

import asyncio
import os
import secrets
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from qiita_common.models import WorkTicketState

from qiita_control_plane import scratch_reclaim
from qiita_control_plane.backfill.syndna_read_count import BackfillPlan, Pair
from qiita_control_plane.cli import admin as cli
from qiita_control_plane.ena_import import (
    DOWNLOAD_ENA_STUDY_ACTION_ID,
    DOWNLOAD_ENA_STUDY_ACTION_VERSION,
)
from qiita_control_plane.scratch_reclaim import (
    RECLAIM_LOCK_KEY,
    ReclaimLockHeld,
    apply_reclaim,
    plan_reclaim,
    reclaim_lock,
)
from qiita_control_plane.testing.db_seeds import (
    delete_action_if_created,
    seed_action_if_absent,
    seed_user_principal,
)
from qiita_control_plane.testing.db_teardown import delete_principal

pytestmark = pytest.mark.db

_VERSION = "1.0.0"
_CUTOFF = datetime(2026, 6, 1, tzinfo=UTC)
_OLD = _CUTOFF - timedelta(days=30)
_LONG_AGO = datetime.now(UTC) - timedelta(days=30)


@pytest_asyncio.fixture
async def world(postgres_pool, tmp_path):
    pool = postgres_pool
    action_id = f"reclaim-{secrets.token_hex(4)}"
    created = [
        (
            action_id,
            _VERSION,
            await seed_action_if_absent(
                pool, action_id=action_id, version=_VERSION, target_kind="sequenced_pool"
            ),
        ),
        (
            DOWNLOAD_ENA_STUDY_ACTION_ID,
            DOWNLOAD_ENA_STUDY_ACTION_VERSION,
            await seed_action_if_absent(
                pool,
                action_id=DOWNLOAD_ENA_STUDY_ACTION_ID,
                version=DOWNLOAD_ENA_STUDY_ACTION_VERSION,
                target_kind="sequenced_pool",
            ),
        ),
    ]
    owner = await seed_user_principal(pool, prefix="reclaim", suffix="owner")
    root = tmp_path / "ticket"
    root.mkdir()
    tickets: list[int] = []
    pools: list[tuple[int, int]] = []

    async def new_pool() -> int:
        run_idx = await pool.fetchval(
            "INSERT INTO qiita.sequencing_run (instrument_run_id, platform, created_by_idx)"
            " VALUES ($1, 'illumina'::qiita.platform, $2) RETURNING idx",
            f"reclaim-{secrets.token_hex(4)}",
            owner,
        )
        pool_idx = await pool.fetchval(
            "INSERT INTO qiita.sequenced_pool (sequencing_run_idx, created_by_idx)"
            " VALUES ($1, $2) RETURNING idx",
            run_idx,
            owner,
        )
        pools.append((run_idx, pool_idx))
        return pool_idx

    async def ticket(
        state="completed",
        *,
        action=action_id,
        version=_VERSION,
        updated_at=_OLD,
        pool_idx=None,
        steps=(),
        workspace=True,
    ) -> int:
        if pool_idx is None:
            pool_idx = await new_pool()
        failed = state == "failed"
        idx = await pool.fetchval(
            "INSERT INTO qiita.work_ticket"
            " (action_id, action_version, originator_principal_idx, scope_target_kind,"
            "  sequenced_pool_idx, state, updated_at, failure_type, failure_stage, failure_reason)"
            " VALUES ($1, $2, $3, 'sequenced_pool', $4, $5::qiita.work_ticket_state, $6,"
            "  $7::qiita.failure_type, $8::qiita.work_ticket_failure_stage, $9)"
            " RETURNING work_ticket_idx",
            action,
            version,
            owner,
            pool_idx,
            state,
            updated_at,
            "permanent" if failed else None,
            "submission" if failed else None,
            "boom" if failed else None,
        )
        tickets.append(idx)
        for n, step_state in enumerate(steps):
            await pool.execute(
                "INSERT INTO qiita.work_ticket_step"
                " (work_ticket_idx, step_index, attempt, step_name, compute_target, state,"
                "  failure_kind, failure_reason)"
                " VALUES ($1, $2, 1, $3, 'local', $4, $5, $6)",
                idx,
                n,
                f"s{n}",
                step_state,
                "x" if step_state == "failed" else None,
                "boom" if step_state == "failed" else None,
            )
        if workspace:
            make_workspace(root, idx)
        return idx

    yield {
        "pool": pool,
        "root": root,
        "action_id": action_id,
        "ticket": ticket,
        "new_pool": new_pool,
        "owner": owner,
    }

    await pool.execute(
        "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = ANY($1::bigint[])", tickets
    )
    for run_idx, pool_idx in pools:
        await pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
        await pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    for action, version, was_created in created:
        await delete_action_if_created(pool, action_id=action, version=version, created=was_created)
    await delete_principal(pool, owner)


def make_workspace(root, idx):
    attempt = root / str(idx) / "step" / "attempt-1"
    for sub, name in [("input", "params.json"), ("output", "data.bin"), ("logs", "stdout")]:
        (attempt / sub).mkdir(parents=True, exist_ok=True)
        (attempt / sub / name).write_bytes(b"x" * 10)
    return attempt


def tree(root) -> list[tuple[str, int]]:
    return sorted(
        (str(p.relative_to(root)), p.stat().st_size if p.is_file() else -1) for p in root.rglob("*")
    )


def kept(root, idx) -> bool:
    return (root / str(idx) / "step" / "attempt-1" / "output" / "data.bin").exists()


async def _plan(world, **kwargs):
    return await plan_reclaim(
        world["pool"],
        ticket_root=world["root"],
        cutoff=kwargs.pop("cutoff", _CUTOFF),
        action_id=kwargs.pop("action_id", world["action_id"]),
    )


def _idxs(plan):
    return {t.work_ticket_idx for t in plan.tickets}


async def test_only_completed_and_no_data_tickets_past_the_cutoff_are_selected(world):
    ticket = world["ticket"]
    completed = await ticket("completed")
    no_data = await ticket("no_data")
    at_cutoff = await ticket("completed", updated_at=_CUTOFF)
    newer = await ticket("completed", updated_at=_CUTOFF + timedelta(days=1))
    others = [
        await ticket(state) for state in ("pending", "queued", "processing", "failed", "cancelled")
    ]
    # A new state has to be classified for reclaim before this passes again.
    assert {s.value for s in WorkTicketState} == {
        "pending",
        "queued",
        "processing",
        "completed",
        "failed",
        "cancelled",
        "no_data",
    }

    other_action = await ticket(
        "completed", action=DOWNLOAD_ENA_STUDY_ACTION_ID, version=DOWNLOAD_ENA_STUDY_ACTION_VERSION
    )

    assert _idxs(await _plan(world)) == {completed, no_data}
    assert not {at_cutoff, newer, other_action, *others} & _idxs(await _plan(world))
    assert other_action in _idxs(await _plan(world, action_id=DOWNLOAD_ENA_STUDY_ACTION_ID))


async def test_a_notified_at_bump_makes_an_old_ticket_ineligible(world):
    idx = await world["ticket"]("completed")
    assert idx in _idxs(await _plan(world))

    await world["pool"].execute(
        "UPDATE qiita.work_ticket SET notified_at = now() WHERE work_ticket_idx = $1", idx
    )

    assert idx not in _idxs(await _plan(world, cutoff=datetime.now(UTC) - timedelta(days=1)))


@pytest.mark.parametrize("live", ["submitting", "submitted", "running"])
async def test_a_ticket_with_a_live_step_row_is_excluded(world, live):
    done = await world["ticket"]("completed", steps=["completed", "failed"])
    busy = await world["ticket"]("completed", steps=["completed", live])

    assert _idxs(await _plan(world)) == {done}
    assert busy not in _idxs(await _plan(world))


async def test_a_ticket_that_becomes_ineligible_before_apply_is_not_deleted(world):
    pool = world["pool"]
    stays = await world["ticket"]("completed")
    busy = await world["ticket"]("completed")
    plan = await _plan(world)
    assert {stays, busy} <= _idxs(plan)

    await pool.execute(
        "INSERT INTO qiita.work_ticket_step"
        " (work_ticket_idx, step_index, attempt, step_name, compute_target, state)"
        " VALUES ($1, 0, 1, 'late', 'local', 'running')",
        busy,
    )
    result = await apply_reclaim(pool, plan)

    assert kept(world["root"], busy)
    assert not kept(world["root"], stays)
    assert [i for i, _ in result.skipped] == [busy]
    assert [t.work_ticket_idx for t in result.reclaimed] == [stays]


async def test_a_download_ticket_is_swept_only_once_a_newer_one_supersedes_it(world):
    ticket = world["ticket"]
    pool_idx = await world["new_pool"]()
    download = {
        "action": DOWNLOAD_ENA_STUDY_ACTION_ID,
        "version": DOWNLOAD_ENA_STUDY_ACTION_VERSION,
    }
    older = await ticket("failed", pool_idx=pool_idx, **download)
    plan = await _plan(world, action_id=DOWNLOAD_ENA_STUDY_ACTION_ID)
    assert older not in _idxs(plan)

    newer = await ticket("failed", pool_idx=pool_idx, **download)
    latest = await ticket("cancelled", pool_idx=pool_idx, **download)
    plan = await _plan(world, action_id=DOWNLOAD_ENA_STUDY_ACTION_ID)
    assert {older, newer, latest} & _idxs(plan) == {older, newer}

    other_pool = await world["new_pool"]()
    lone = await ticket("failed", pool_idx=other_pool, **download)
    assert lone not in _idxs(await _plan(world, action_id=DOWNLOAD_ENA_STUDY_ACTION_ID))

    await apply_reclaim(world["pool"], plan)
    assert not kept(world["root"], older)
    assert kept(world["root"], lone)
    assert kept(world["root"], latest)
    await world["pool"].execute(
        "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = ANY($1::bigint[])", [older, newer]
    )
    assert latest not in _idxs(await _plan(world, action_id=DOWNLOAD_ENA_STUDY_ACTION_ID))


async def test_a_failed_ticket_of_another_action_is_never_swept(world):
    pool_idx = await world["new_pool"]()
    older = await world["ticket"]("failed", pool_idx=pool_idx)
    newer = await world["ticket"]("failed", pool_idx=pool_idx)

    assert not {older, newer} & _idxs(await _plan(world))


async def test_a_ticket_the_syndna_backfill_would_still_write_is_skipped_and_reported(
    world, monkeypatch
):
    pool = world["pool"]
    needed = await world["ticket"]("completed")
    free = await world["ticket"]("completed")

    async def fake_plan(_pool, *, ticket_root):
        return BackfillPlan(pairs=[Pair(1, 1, ticket_root / "x", None, work_ticket_idx=needed)])

    monkeypatch.setattr(scratch_reclaim, "plan_syndna_backfill", fake_plan)
    plan = await _plan(world)
    result = await apply_reclaim(pool, plan)

    assert plan.syndna_skipped == [needed]
    assert _idxs(plan) == {free}
    assert kept(world["root"], needed)
    assert not kept(world["root"], free)
    assert [t.work_ticket_idx for t in result.reclaimed] == [free]


def _deny_chmod_on_locked(m):
    real_chmod = os.chmod

    def chmod(path, mode, *args, **kwargs):
        if os.path.basename(path) == "locked":
            raise PermissionError(1, "not permitted", path)
        real_chmod(path, mode, *args, **kwargs)

    m.setattr(os, "chmod", chmod)


async def test_a_workspace_that_cannot_be_deleted_is_a_failure_and_the_rest_still_goes(
    world, monkeypatch
):
    if os.geteuid() == 0:
        pytest.skip("root ignores directory permissions")
    pool = world["pool"]
    stuck = await world["ticket"]("completed")
    fine = await world["ticket"]("completed")
    locked = world["root"] / str(stuck) / "step" / "attempt-1" / "output" / "locked"
    locked.mkdir()
    (locked / "f").write_text("x")
    locked.chmod(0o500)
    try:
        plan = await _plan(world)
        with monkeypatch.context() as m:
            _deny_chmod_on_locked(m)
            result = await apply_reclaim(pool, plan)
    finally:
        locked.chmod(0o700)

    assert [i for i, _ in result.failures] == [stuck]
    assert [t.work_ticket_idx for t in result.reclaimed] == [fine]
    assert not kept(world["root"], fine)


async def test_a_workspace_that_cannot_be_measured_is_reported_and_the_rest_still_planned(world):
    if os.geteuid() == 0:
        pytest.skip("root ignores directory permissions")
    stuck = await world["ticket"]("completed")
    fine = await world["ticket"]("completed")
    sealed = world["root"] / str(stuck) / "step" / "attempt-1" / "output"
    sealed.chmod(0o000)
    try:
        plan = await _plan(world)
    finally:
        sealed.chmod(0o700)

    by_idx = {t.work_ticket_idx: t for t in plan.tickets}
    assert by_idx[stuck].error and by_idx[fine].error is None
    assert by_idx[fine].usage.total_bytes == 30


async def _cli(monkeypatch, postgres_url, root, *args):
    monkeypatch.setenv("DATABASE_URL", postgres_url)
    monkeypatch.setenv("PATH_SCRATCH", str(root.parent))
    return await asyncio.to_thread(
        cli.main,
        ["scratch", "reclaim", "--older-than", "1", *args],
    )


async def test_a_dry_run_changes_nothing_and_execute_deletes(
    world, monkeypatch, postgres_url, capsys
):
    idx = await world["ticket"]("completed", updated_at=_LONG_AGO)
    base = ["--action-id", world["action_id"]]
    before = tree(world["root"])

    assert await _cli(monkeypatch, postgres_url, world["root"], *base) == 0
    assert tree(world["root"]) == before
    assert "DRY RUN" in capsys.readouterr().out

    assert await _cli(monkeypatch, postgres_url, world["root"], *base, "--execute") == 0
    assert not kept(world["root"], idx)
    assert (world["root"] / str(idx) / "step" / "attempt-1" / "logs" / "stdout").exists()


async def test_execute_exits_1_when_a_workspace_fails_and_still_reclaims_the_rest(
    world, monkeypatch, postgres_url
):
    if os.geteuid() == 0:
        pytest.skip("root ignores directory permissions")
    stuck = await world["ticket"]("completed", updated_at=_LONG_AGO)
    fine = await world["ticket"]("completed", updated_at=_LONG_AGO)
    locked = world["root"] / str(stuck) / "step" / "attempt-1" / "output" / "locked"
    locked.mkdir()
    (locked / "f").write_text("x")
    locked.chmod(0o500)
    try:
        with monkeypatch.context() as m:
            _deny_chmod_on_locked(m)
            rc = await _cli(
                m,
                postgres_url,
                world["root"],
                "--action-id",
                world["action_id"],
                "--execute",
            )
    finally:
        locked.chmod(0o700)

    assert rc == 1
    assert not kept(world["root"], fine)


async def test_execute_exits_3_and_touches_nothing_while_the_lock_is_held(
    world, monkeypatch, postgres_url
):
    idx = await world["ticket"]("completed", updated_at=_LONG_AGO)
    async with world["pool"].acquire() as holder:
        assert await holder.fetchval("SELECT pg_try_advisory_lock($1)", RECLAIM_LOCK_KEY)
        try:
            rc = await _cli(
                monkeypatch,
                postgres_url,
                world["root"],
                "--action-id",
                world["action_id"],
                "--execute",
            )
            with pytest.raises(ReclaimLockHeld):
                async with reclaim_lock(world["pool"]):
                    pass
        finally:
            await holder.fetchval("SELECT pg_advisory_unlock($1)", RECLAIM_LOCK_KEY)

    assert rc == 3
    assert kept(world["root"], idx)


async def _orphan(world, idx):
    make_workspace(world["root"], idx)
    return idx


async def _free_idx(world) -> int:
    """A work_ticket_idx that was issued and then deleted, so no row holds it."""
    idx = await world["ticket"]("completed", workspace=False)
    await world["pool"].execute("DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", idx)
    return idx


async def test_an_orphan_is_reported_but_untouched_without_the_flag(world):
    orphan = await _orphan(world, await _free_idx(world))
    (world["root"] / "not-a-ticket").mkdir()
    (world["root"] / "12x").mkdir()
    (world["root"] / "007").mkdir()

    plan = await _plan(world)
    result = await apply_reclaim(world["pool"], plan)

    assert [o.work_ticket_idx for o in plan.orphans] == [orphan]
    assert plan.orphans[0].usage.total_bytes == 30
    assert plan.orphans[0].usage.inodes > 0
    assert result.reclaimed == []
    assert kept(world["root"], orphan)


async def test_an_orphan_is_reclaimed_with_the_flag_and_keeps_its_logs(world):
    orphan = await _orphan(world, await _free_idx(world))
    (world["root"] / "not-a-ticket").mkdir()

    result = await apply_reclaim(world["pool"], await _plan(world), include_orphans=True)

    assert [o.work_ticket_idx for o in result.reclaimed] == [orphan]
    assert not kept(world["root"], orphan)
    assert (world["root"] / str(orphan) / "step" / "attempt-1" / "logs" / "stdout").exists()
    assert (world["root"] / "not-a-ticket").is_dir()


async def test_an_orphan_whose_row_appears_before_apply_is_not_deleted(world):
    pool = world["pool"]
    idx = await _free_idx(world)
    await _orphan(world, idx)
    plan = await _plan(world)
    assert [o.work_ticket_idx for o in plan.orphans] == [idx]

    pool_idx = await world["new_pool"]()
    await pool.execute(
        "INSERT INTO qiita.work_ticket (work_ticket_idx, action_id, action_version,"
        " originator_principal_idx, scope_target_kind, sequenced_pool_idx, state)"
        " OVERRIDING SYSTEM VALUE"
        " VALUES ($1, $2, $3, $4, 'sequenced_pool', $5, 'processing')",
        idx,
        world["action_id"],
        _VERSION,
        world["owner"],
        pool_idx,
    )
    try:
        result = await apply_reclaim(pool, plan, include_orphans=True)
    finally:
        await pool.execute("DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", idx)

    assert kept(world["root"], idx)
    assert [i for i, _ in result.skipped] == [idx]
