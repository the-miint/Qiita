"""Tests for the role-typed FK triggers introduced by 20260429000000.

Two triggers are exercised together:

- `tg_principal_must_be_user`: attached to qiita.study(owner_idx) and
  qiita.study(principal_investigator_idx). Raises if either column is
  set to a principal that has no qiita.user row.

- `tg_user_role_ref_blocks_delete`: attached to qiita.user. Raises if a
  qiita.user row is deleted while any registered consumer column still
  references its principal_idx.

Each test runs in its own transaction and rolls back, so seeded
principals and studies don't leak across cases.
"""

import secrets

import asyncpg
import pytest
from qiita_common.auth_constants import SystemRole

pytestmark = pytest.mark.db


SYSTEM_PRINCIPAL_IDX = 1


def _suffix(label: str) -> str:
    return f"{label}-{secrets.token_hex(4)}"


async def _create_user(conn) -> int:
    """Return principal_idx of a freshly-minted user-kind principal."""
    name = _suffix("user")
    pidx = await conn.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, $3) RETURNING idx",
        name,
        SystemRole.USER,
        SYSTEM_PRINCIPAL_IDX,
    )
    await conn.execute(
        "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
        pidx,
        f"{name}@example.com",
    )
    return pidx


async def _create_service_account(conn) -> int:
    name = _suffix("svc")
    pidx = await conn.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, $3) RETURNING idx",
        name,
        SystemRole.USER,
        SYSTEM_PRINCIPAL_IDX,
    )
    await conn.execute(
        "INSERT INTO qiita.service_account (principal_idx, name) VALUES ($1, $2)",
        pidx,
        name,
    )
    return pidx


async def _create_bare_principal(conn) -> int:
    """Principal with no subtype row — represents an actor that cannot
    authenticate (e.g., a PI imported from an external system before
    they've logged in)."""
    return await conn.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, $3) RETURNING idx",
        _suffix("bare"),
        SystemRole.USER,
        SYSTEM_PRINCIPAL_IDX,
    )


async def _insert_study(
    conn,
    *,
    owner_idx: int,
    pi_idx: int | None = None,
    created_by_idx: int = SYSTEM_PRINCIPAL_IDX,
) -> int:
    return await conn.fetchval(
        "INSERT INTO qiita.study"
        "  (owner_idx, principal_investigator_idx, title, created_by_idx)"
        " VALUES ($1, $2, $3, $4) RETURNING idx",
        owner_idx,
        pi_idx,
        _suffix("study"),
        created_by_idx,
    )


# ---------------------------------------------------------------------------
# tg_principal_must_be_user — happy paths
# ---------------------------------------------------------------------------


async def test_study_insert_with_user_owner_and_pi_succeeds(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            pi = await _create_user(conn)
            study_idx = await _insert_study(conn, owner_idx=owner, pi_idx=pi)
            assert study_idx is not None
        finally:
            await tr.rollback()


async def test_study_insert_with_null_pi_succeeds(postgres_pool):
    """principal_investigator_idx is nullable; the trigger short-circuits on NULL."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            study_idx = await _insert_study(conn, owner_idx=owner, pi_idx=None)
            assert study_idx is not None
        finally:
            await tr.rollback()


# ---------------------------------------------------------------------------
# tg_principal_must_be_user — rejection paths
# ---------------------------------------------------------------------------


async def test_study_insert_with_service_account_owner_raises(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            svc = await _create_service_account(conn)
            with pytest.raises(asyncpg.RaiseError, match="must reference a user-kind principal"):
                await _insert_study(conn, owner_idx=svc)
        finally:
            await tr.rollback()


async def test_study_insert_with_bare_principal_owner_raises(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            bare = await _create_bare_principal(conn)
            with pytest.raises(asyncpg.RaiseError, match="must reference a user-kind principal"):
                await _insert_study(conn, owner_idx=bare)
        finally:
            await tr.rollback()


async def test_study_insert_with_service_account_pi_raises(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            svc = await _create_service_account(conn)
            with pytest.raises(asyncpg.RaiseError, match="must reference a user-kind principal"):
                await _insert_study(conn, owner_idx=owner, pi_idx=svc)
        finally:
            await tr.rollback()


async def test_study_update_owner_to_service_account_raises(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            svc = await _create_service_account(conn)
            study_idx = await _insert_study(conn, owner_idx=owner)
            with pytest.raises(asyncpg.RaiseError, match="must reference a user-kind principal"):
                await conn.execute(
                    "UPDATE qiita.study SET owner_idx = $1 WHERE idx = $2",
                    svc,
                    study_idx,
                )
        finally:
            await tr.rollback()


# ---------------------------------------------------------------------------
# tg_user_role_ref_blocks_delete — happy and rejection paths
# ---------------------------------------------------------------------------


async def test_user_delete_blocked_when_referenced_as_owner(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            await _insert_study(conn, owner_idx=owner)
            with pytest.raises(asyncpg.RaiseError, match="cannot delete qiita.user"):
                await conn.execute("DELETE FROM qiita.user WHERE principal_idx = $1", owner)
        finally:
            await tr.rollback()


async def test_user_delete_blocked_when_referenced_as_pi(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            pi = await _create_user(conn)
            await _insert_study(conn, owner_idx=owner, pi_idx=pi)
            with pytest.raises(asyncpg.RaiseError, match="cannot delete qiita.user"):
                await conn.execute("DELETE FROM qiita.user WHERE principal_idx = $1", pi)
        finally:
            await tr.rollback()


async def test_user_delete_succeeds_when_no_inbound_refs(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            unused = await _create_user(conn)
            await conn.execute("DELETE FROM qiita.user WHERE principal_idx = $1", unused)
            still_there = await conn.fetchval(
                "SELECT 1 FROM qiita.user WHERE principal_idx = $1", unused
            )
            assert still_there is None
        finally:
            await tr.rollback()
