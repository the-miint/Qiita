"""The krepp build lifecycle in Postgres: the reference_index columns and constraints
20261117000000_krepp_index_lifecycle.sql adds, `register_index`'s krepp refusals, the
state changes in `repositories.reference_index`, and the reference-delete gate."""

import uuid

import asyncpg
import pytest
from qiita_common.models import (
    INDEX_TYPE_KREPP,
    IndexPathRoot,
    ReferenceIndexState,
)

from qiita_control_plane.actions.library import register_index
from qiita_control_plane.actions.reference import (
    ReferenceDeleteBlocked,
    assert_reference_deletable,
)
from qiita_control_plane.repositories.reference_index import (
    KreppBuildTransitionRefused,
    activate_krepp_build,
    remove_krepp_build,
    removed_build_of_ticket,
)
from qiita_control_plane.runner import _resolve_reference_index_path

pytestmark = pytest.mark.db

_ACTION_ID = "krepp-lifecycle-test"
_VERSION = "1.0.0"


@pytest.fixture
async def world(postgres_pool):
    pool = postgres_pool
    principal_idx = await pool.fetchval("SELECT MIN(idx) FROM qiita.principal")
    reference_idx = await pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1', 'sequence_reference', 'active', $2) RETURNING reference_idx",
        f"krepp-lifecycle-{uuid.uuid4().hex[:8]}",
        principal_idx,
    )
    await pool.execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status)"
        " VALUES ($1, $2, 'reference', '{}'::text[], $3::jsonb, '{}'::jsonb, '[]'::jsonb,"
        "         1, 1, '1 minute', NULL, NULL)"
        " ON CONFLICT (action_id, version) DO NOTHING",
        _ACTION_ID,
        _VERSION,
        '{"service": false, "human_roles": ["system_admin"]}',
    )
    yield {"pool": pool, "principal_idx": principal_idx, "reference_idx": reference_idx}
    await pool.execute("DELETE FROM qiita.work_ticket WHERE reference_idx = $1", reference_idx)
    await pool.execute("DELETE FROM qiita.reference_index WHERE reference_idx = $1", reference_idx)
    await pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx)


def _params(sums: str) -> dict:
    return {"sha256sums_sha256": sums, "partitions": [0, 1]}


async def _register(world, label: str, sums: str = "a" * 64) -> int:
    return await register_index(
        world["pool"],
        reference_idx=world["reference_idx"],
        index_type=INDEX_TYPE_KREPP,
        fs_path=f"references/{world['reference_idx']}/krepp/{label}",
        params=_params(sums),
        state=ReferenceIndexState.REGISTERED,
        fs_path_root=IndexPathRoot.NODE_LOCAL,
    )


async def _state(pool, reference_index_idx: int) -> str:
    return await pool.fetchval(
        "SELECT state FROM qiita.reference_index WHERE reference_index_idx = $1",
        reference_index_idx,
    )


async def _activate(pool, reference_index_idx: int):
    async with pool.acquire() as conn, conn.transaction():
        return await activate_krepp_build(conn, reference_index_idx)


async def _remove(pool, reference_index_idx: int):
    async with pool.acquire() as conn, conn.transaction():
        return await remove_krepp_build(conn, reference_index_idx)


async def _ticket(world, reference_index_idx: int, state: str) -> int:
    # A failed ticket carries its failure columns (work_ticket_failure_consistent).
    failed = state == "failed"
    return await world["pool"].fetchval(
        "INSERT INTO qiita.work_ticket (action_id, action_version, originator_principal_idx,"
        " scope_target_kind, reference_idx, action_context, state, reference_index_idx,"
        " failure_type, failure_stage, failure_reason)"
        " VALUES ($1, $2, $3, 'reference', $4, '{}'::jsonb, $5, $6,"
        " $7::qiita.failure_type, $8::qiita.work_ticket_failure_stage, $9)"
        " RETURNING work_ticket_idx",
        _ACTION_ID,
        _VERSION,
        world["principal_idx"],
        world["reference_idx"],
        state,
        reference_index_idx,
        "permanent" if failed else None,
        "submission" if failed else None,
        "test" if failed else None,
    )


# --- schema -----------------------------------------------------------------


async def test_existing_index_types_default_to_active_absolute(world):
    pool = world["pool"]
    idx = await pool.fetchval(
        "INSERT INTO qiita.reference_index (reference_idx, index_type, fs_path, params)"
        " VALUES ($1, 'rype', '/srv/x.ryxdi', '{}'::jsonb) RETURNING reference_index_idx",
        world["reference_idx"],
    )
    row = await pool.fetchrow(
        "SELECT state, fs_path_root FROM qiita.reference_index WHERE reference_index_idx = $1",
        idx,
    )
    assert (row["state"], row["fs_path_root"]) == ("active", "absolute")


@pytest.mark.parametrize(
    ("index_type", "root"),
    [("krepp", "absolute"), ("rype", "node_local")],
)
async def test_node_local_root_is_krepp_only(world, index_type, root):
    with pytest.raises(asyncpg.CheckViolationError):
        await world["pool"].execute(
            "INSERT INTO qiita.reference_index"
            " (reference_idx, index_type, fs_path, params, fs_path_root)"
            " VALUES ($1, $2, 'x', '{}'::jsonb, $3)",
            world["reference_idx"],
            index_type,
            root,
        )


async def test_one_active_krepp_build_per_reference(world):
    a = await _register(world, "a", "a" * 64)
    b = await _register(world, "b", "b" * 64)
    await world["pool"].execute(
        "UPDATE qiita.reference_index SET state = 'active' WHERE reference_index_idx = $1", a
    )
    with pytest.raises(asyncpg.UniqueViolationError):
        await world["pool"].execute(
            "UPDATE qiita.reference_index SET state = 'active' WHERE reference_index_idx = $1", b
        )


async def test_deleting_a_build_row_detaches_its_tickets(world):
    pool = world["pool"]
    build = await _register(world, "a")
    ticket = await _ticket(world, build, "completed")
    await pool.execute("DELETE FROM qiita.reference_index WHERE reference_index_idx = $1", build)
    assert (
        await pool.fetchval(
            "SELECT reference_index_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1", ticket
        )
        is None
    )


# --- register_index -----------------------------------------------------------


async def test_register_krepp_writes_registered_and_is_idempotent(world):
    first = await _register(world, "a")
    again = await _register(world, "a")
    assert first == again
    assert await _state(world["pool"], first) == ReferenceIndexState.REGISTERED


async def test_register_krepp_refuses_changed_params(world):
    await _register(world, "a", "a" * 64)
    with pytest.raises(ValueError, match="different params"):
        await _register(world, "a", "c" * 64)


@pytest.mark.parametrize("state", ["retired", "removed"])
async def test_register_krepp_refuses_a_retired_or_removed_build(world, state):
    build = await _register(world, "a")
    await world["pool"].execute(
        "UPDATE qiita.reference_index SET state = $2 WHERE reference_index_idx = $1",
        build,
        state,
    )
    with pytest.raises(ValueError, match=state):
        await _register(world, "a")


async def test_register_non_krepp_ignores_params_on_replay(world):
    """The krepp refusal does not reach other types: a host minimap2 build re-registers
    a fixed path with whatever params its replay carries."""
    kwargs = {
        "reference_idx": world["reference_idx"],
        "index_type": "minimap2",
        "fs_path": "/derived/x.mmi",
    }
    first = await register_index(world["pool"], params={"preset": "a"}, **kwargs)
    again = await register_index(world["pool"], params={"preset": "b"}, **kwargs)
    assert first == again


async def test_generic_resolver_refuses_krepp(world):
    with pytest.raises(ValueError, match="krepp"):
        await _resolve_reference_index_path(world["pool"], world["reference_idx"], INDEX_TYPE_KREPP)


# --- activate -----------------------------------------------------------------


async def test_activate_retires_the_previous_build(world):
    pool = world["pool"]
    a = await _register(world, "a", "a" * 64)
    b = await _register(world, "b", "b" * 64)
    first = await _activate(pool, a)
    assert first.retired_reference_index_idx is None
    second = await _activate(pool, b)
    assert second.retired_reference_index_idx == a
    assert (await _state(pool, a), await _state(pool, b)) == ("retired", "active")


async def test_activate_is_a_no_op_on_the_active_build(world):
    pool = world["pool"]
    a = await _register(world, "a")
    await _activate(pool, a)
    again = await _activate(pool, a)
    assert again.state is ReferenceIndexState.ACTIVE
    assert again.retired_reference_index_idx is None


@pytest.mark.parametrize("state", ["retired", "removed"])
async def test_activate_refuses_a_retired_or_removed_build(world, state):
    pool = world["pool"]
    a = await _register(world, "a")
    await pool.execute(
        "UPDATE qiita.reference_index SET state = $2 WHERE reference_index_idx = $1", a, state
    )
    with pytest.raises(KreppBuildTransitionRefused, match="only a registered build"):
        await _activate(pool, a)


# --- remove -------------------------------------------------------------------


async def test_remove_refuses_the_active_build(world):
    a = await _register(world, "a")
    await _activate(world["pool"], a)
    with pytest.raises(KreppBuildTransitionRefused, match="active build"):
        await _remove(world["pool"], a)


@pytest.mark.parametrize("ticket_state", ["pending", "queued", "processing"])
async def test_remove_refuses_while_a_ticket_runs_on_the_build(world, ticket_state):
    pool = world["pool"]
    a = await _register(world, "a", "a" * 64)
    b = await _register(world, "b", "b" * 64)
    await _activate(pool, a)
    await _activate(pool, b)
    ticket = await _ticket(world, a, ticket_state)
    with pytest.raises(KreppBuildTransitionRefused, match=str(ticket)):
        await _remove(pool, a)
    assert await _state(pool, a) == "retired"


@pytest.mark.parametrize("ticket_state", ["completed", "failed", "cancelled", "no_data"])
async def test_remove_allows_finished_tickets_and_keeps_their_build(world, ticket_state):
    pool = world["pool"]
    a = await _register(world, "a", "a" * 64)
    b = await _register(world, "b", "b" * 64)
    await _activate(pool, a)
    await _activate(pool, b)
    ticket = await _ticket(world, a, ticket_state)
    change = await _remove(pool, a)
    assert change.state is ReferenceIndexState.REMOVED
    assert (
        await pool.fetchval(
            "SELECT reference_index_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1", ticket
        )
        == a
    )
    async with pool.acquire() as conn, conn.transaction():
        assert await removed_build_of_ticket(conn, ticket) == a


async def test_remove_a_registered_build_that_was_never_activated(world):
    a = await _register(world, "a")
    change = await _remove(world["pool"], a)
    assert change.state is ReferenceIndexState.REMOVED


async def test_removed_build_of_ticket_is_none_for_a_live_build(world):
    pool = world["pool"]
    a = await _register(world, "a")
    await _activate(pool, a)
    ticket = await _ticket(world, a, "failed")
    async with pool.acquire() as conn, conn.transaction():
        assert await removed_build_of_ticket(conn, ticket) is None


# --- reference delete ---------------------------------------------------------


async def test_reference_delete_blocked_by_a_build_not_removed_even_with_force(world):
    await _register(world, "a")
    with pytest.raises(ReferenceDeleteBlocked) as exc:
        await assert_reference_deletable(world["pool"], world["reference_idx"], force=True)
    assert exc.value.krepp_builds == 1
    assert "krepp-index remove" in str(exc.value)


async def test_reference_delete_allowed_once_every_build_is_removed(world):
    a = await _register(world, "a")
    await _remove(world["pool"], a)
    await assert_reference_deletable(world["pool"], world["reference_idx"], force=True)
