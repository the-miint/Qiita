"""DB-bound tests: the database refuses a field exception on a
unique_in_study field, and refuses declaring unique_in_study over a field that
exceptions key on, whichever of the two commits first. The migration that adds
the two triggers says why.
"""

import asyncio

import asyncpg
import pytest
from qiita_common.models import Tier

from qiita_control_plane.testing.db_seeds import seed_field_exception

from .conftest import (
    METADATA_WRITE_LOCK_TIMEOUT,
    SPECS,
    _create_linked_entity_for_spec,
    _create_plain_field,
    _set_unique_in_study,
    _spec_id,
)

pytestmark = pytest.mark.db

# Bounds every wait on the other transaction, so a lock that never blocks, or
# one that never releases, fails the test rather than hanging it.
_WAIT_S = 10


async def _seed_exception(ctx, spec, *, entity_idx, field_idx) -> int:
    return await seed_field_exception(
        ctx["pool"],
        spec=spec,
        entity_idx=entity_idx,
        study_field_idx=field_idx,
        tier_override=Tier.ADMIN,
        created_by_idx=ctx["principal_idx"],
    )


async def _insert_exception(conn, spec, *, entity_idx, field_idx, created_by_idx) -> None:
    await conn.execute(
        f"INSERT INTO {spec.field_exception_table} ("
        f"    {spec.entity_key_column}, {spec.study_field_idx_column},"
        f"    tier_override, created_by_idx"
        f") VALUES ($1, $2, $3::qiita.tier, $4)",
        entity_idx,
        field_idx,
        Tier.ADMIN,
        created_by_idx,
    )


async def _declare_unique(conn, spec, field_idx) -> None:
    await conn.execute(f"SET LOCAL lock_timeout = '{METADATA_WRITE_LOCK_TIMEOUT}'")
    await conn.execute(
        f"UPDATE {spec.study_field_table} SET unique_in_study = true WHERE idx = $1", field_idx
    )


async def _until_blocked_by(pool, blocker: asyncpg.Connection, task: asyncio.Task) -> None:
    """Return once some backend waits on a lock that `blocker` holds."""
    blocker_pid = blocker.get_server_pid()
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _WAIT_S
    while loop.time() < deadline:
        assert not task.done(), "the write finished without waiting on the held lock"
        if await pool.fetchval(
            "SELECT count(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))",
            blocker_pid,
        ):
            return
        await asyncio.sleep(0.02)
    raise AssertionError("the write never waited on the held lock")


async def _exception_count(ctx, spec, field_idx) -> int:
    return await ctx["pool"].fetchval(
        f"SELECT count(*) FROM {spec.field_exception_table}"
        f" WHERE {spec.study_field_idx_column} = $1",
        field_idx,
    )


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_exception_refused_on_a_unique_in_study_field(ctx, spec):
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    field_idx = await _create_plain_field(ctx, spec, suffix="exc_unique")
    await _set_unique_in_study(ctx, spec, field_idx, True)

    with pytest.raises(asyncpg.CheckViolationError) as exc_info:
        await _seed_exception(ctx, spec, entity_idx=entity_idx, field_idx=field_idx)

    assert exc_info.value.constraint_name == spec.exception_on_unique_field_constraint
    assert await _exception_count(ctx, spec, field_idx) == 0


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_unique_in_study_refused_while_an_exception_keys_on_the_field(ctx, spec):
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    field_idx = await _create_plain_field(ctx, spec, suffix="exc_then_unique")
    await _seed_exception(ctx, spec, entity_idx=entity_idx, field_idx=field_idx)

    with pytest.raises(asyncpg.CheckViolationError) as exc_info:
        await _set_unique_in_study(ctx, spec, field_idx, True)

    assert exc_info.value.constraint_name == spec.unique_over_exceptions_constraint
    assert (
        await ctx["pool"].fetchval(
            f"SELECT unique_in_study FROM {spec.study_field_table} WHERE idx = $1", field_idx
        )
        is False
    )


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_an_exception_on_another_field_does_not_block_unique_in_study(ctx, spec):
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    excepted_field_idx = await _create_plain_field(ctx, spec, suffix="exc_other")
    await _seed_exception(ctx, spec, entity_idx=entity_idx, field_idx=excepted_field_idx)
    field_idx = await _create_plain_field(ctx, spec, suffix="exc_unique_other")

    await _set_unique_in_study(ctx, spec, field_idx, True)

    assert (
        await ctx["pool"].fetchval(
            f"SELECT unique_in_study FROM {spec.study_field_table} WHERE idx = $1", field_idx
        )
        is True
    )


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_unique_in_study_declared_once_the_exceptions_are_cleared(ctx, spec):
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    field_idx = await _create_plain_field(ctx, spec, suffix="exc_cleared")
    exception_idx = await _seed_exception(ctx, spec, entity_idx=entity_idx, field_idx=field_idx)
    await ctx["pool"].execute(
        f"DELETE FROM {spec.field_exception_table} WHERE idx = $1", exception_idx
    )

    await _set_unique_in_study(ctx, spec, field_idx, True)

    assert (
        await ctx["pool"].fetchval(
            f"SELECT unique_in_study FROM {spec.study_field_table} WHERE idx = $1", field_idx
        )
        is True
    )


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_exception_waits_behind_an_uncommitted_unique_declaration(ctx, spec):
    """The declaration commits first, so the exception that waited on it is
    refused rather than landing on a field now unique."""
    pool = ctx["pool"]
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    field_idx = await _create_plain_field(ctx, spec, suffix="exc_race_a")

    async def _insert() -> None:
        async with pool.acquire() as conn, conn.transaction():
            await _insert_exception(
                conn,
                spec,
                entity_idx=entity_idx,
                field_idx=field_idx,
                created_by_idx=ctx["principal_idx"],
            )

    async with pool.acquire() as declarer:
        tx = declarer.transaction()
        await tx.start()
        try:
            await _declare_unique(declarer, spec, field_idx)
            task = asyncio.create_task(_insert())
            await _until_blocked_by(pool, declarer, task)
        finally:
            await tx.commit()

    with pytest.raises(asyncpg.CheckViolationError) as exc_info:
        await asyncio.wait_for(task, _WAIT_S)
    assert exc_info.value.constraint_name == spec.exception_on_unique_field_constraint
    assert await _exception_count(ctx, spec, field_idx) == 0


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_unique_declaration_waits_behind_an_uncommitted_exception(ctx, spec):
    """The exception commits first, so the declaration that waited on it is
    refused rather than making a field unique over a restricted value."""
    pool = ctx["pool"]
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    field_idx = await _create_plain_field(ctx, spec, suffix="exc_race_b")

    async def _declare() -> None:
        async with pool.acquire() as conn, conn.transaction():
            await _declare_unique(conn, spec, field_idx)

    async with pool.acquire() as setter:
        tx = setter.transaction()
        await tx.start()
        try:
            await _insert_exception(
                setter,
                spec,
                entity_idx=entity_idx,
                field_idx=field_idx,
                created_by_idx=ctx["principal_idx"],
            )
            task = asyncio.create_task(_declare())
            await _until_blocked_by(pool, setter, task)
        finally:
            await tx.commit()

    with pytest.raises(asyncpg.CheckViolationError) as exc_info:
        await asyncio.wait_for(task, _WAIT_S)
    assert exc_info.value.constraint_name == spec.unique_over_exceptions_constraint
