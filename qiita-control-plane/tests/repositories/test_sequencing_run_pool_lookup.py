"""Tests for repositories.sequencing_run.fetch_sequenced_pool_idxs_for_run
(T02, ena_import.registration's get-or-create-pool-for-run step).

Pattern 1 (transaction-rollback per test), matching test_study.py: no
committed fixture needed since every assertion runs inside the same
open transaction as the seed writes.
"""

import secrets

import pytest
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX, SystemRole

from qiita_control_plane.repositories.sequencing_run import (
    fetch_sequenced_pool_idxs_for_run,
    insert_sequenced_pool,
    insert_sequencing_run,
)

pytestmark = pytest.mark.db


def _suffix(label: str) -> str:
    return f"{label}-{secrets.token_hex(4)}"


async def _create_user(conn) -> int:
    pidx = await conn.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, $3) RETURNING idx",
        _suffix("user"),
        SystemRole.USER,
        SYSTEM_PRINCIPAL_IDX,
    )
    await conn.execute(
        "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
        pidx,
        f"{_suffix('u')}@example.com",
    )
    return pidx


async def test_fetch_sequenced_pool_idxs_for_run_empty_for_fresh_run(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            run_idx, _ = await insert_sequencing_run(
                conn,
                instrument_run_id=_suffix("RUN"),
                platform="illumina",
                created_by_idx=owner,
            )

            assert await fetch_sequenced_pool_idxs_for_run(conn, run_idx) == []
        finally:
            await tr.rollback()


async def test_fetch_sequenced_pool_idxs_for_run_returns_oldest_first(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            run_idx, _ = await insert_sequencing_run(
                conn,
                instrument_run_id=_suffix("RUN"),
                platform="illumina",
                created_by_idx=owner,
            )

            pool_idx_1, created_1 = await insert_sequenced_pool(
                conn, sequencing_run_idx=run_idx, created_by_idx=owner
            )
            pool_idx_2, created_2 = await insert_sequenced_pool(
                conn, sequencing_run_idx=run_idx, created_by_idx=owner
            )
            # No-preflight inserts always create a fresh row (no natural
            # content key to ON-CONFLICT against) -- this is exactly why
            # the registration composer checks this fetch first.
            assert created_1 is True
            assert created_2 is True
            assert pool_idx_1 != pool_idx_2

            idxs = await fetch_sequenced_pool_idxs_for_run(conn, run_idx)
            assert idxs == sorted(idxs)
            assert set(idxs) == {pool_idx_1, pool_idx_2}
        finally:
            await tr.rollback()


async def test_fetch_sequenced_pool_idxs_for_run_scoped_to_its_own_run(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            run_a_idx, _ = await insert_sequencing_run(
                conn,
                instrument_run_id=_suffix("RUN-A"),
                platform="illumina",
                created_by_idx=owner,
            )
            run_b_idx, _ = await insert_sequencing_run(
                conn,
                instrument_run_id=_suffix("RUN-B"),
                platform="oxford_nanopore",
                created_by_idx=owner,
            )
            pool_a_idx, _ = await insert_sequenced_pool(
                conn, sequencing_run_idx=run_a_idx, created_by_idx=owner
            )
            await insert_sequenced_pool(conn, sequencing_run_idx=run_b_idx, created_by_idx=owner)

            assert await fetch_sequenced_pool_idxs_for_run(conn, run_a_idx) == [pool_a_idx]
        finally:
            await tr.rollback()
