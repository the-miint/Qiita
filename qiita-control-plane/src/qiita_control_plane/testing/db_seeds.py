"""Pytest seed and state-change helpers for DB-row fixtures.

Plain async functions (not pytest fixtures) so callers can pass test-local
arguments. Helpers fall into two groups: seeders that insert rows and
return the new idx, and state-changers that update existing rows
(disabling, retiring, etc.). Cleanup is the caller's responsibility
(route tests do FK-reverse cleanup against a per-test `created` tracker;
integration tests may rely on a session-scoped truncate). Helpers are
pool-based and commit their writes — for repository-layer trigger tests
that roll back, build the SQL inline against the open connection instead.
"""

import secrets

import asyncpg
from qiita_common.auth_constants import SystemRole

# Bare-principal idx the schema's first migration installs as the root
# created_by parent. Centralised here so route tests do not redefine it.
SYSTEM_PRINCIPAL_IDX = 1


async def seed_user_principal(
    pool: asyncpg.Pool,
    *,
    prefix: str,
    suffix: str,
    profile_complete: bool = True,
) -> int:
    """Insert a principal + qiita.user row; return the principal_idx.

    `prefix` and `suffix` form the display_name as f"{prefix}-{suffix}-{token}";
    the token defends against name collisions across re-runs. With
    profile_complete=True the user row carries email + affiliation + address
    + phone, which the schema's profile_complete computed column treats as a
    complete profile. With profile_complete=False only email is populated, so
    the flag stays false.
    """
    name = f"{prefix}-{suffix}-{secrets.token_hex(4)}"
    async with pool.acquire() as conn:
        async with conn.transaction():
            pidx = await conn.fetchval(
                "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
                " VALUES ($1, $2, $3) RETURNING idx",
                name,
                SystemRole.USER,
                SYSTEM_PRINCIPAL_IDX,
            )
            if profile_complete:
                await conn.execute(
                    "INSERT INTO qiita.user"
                    "  (principal_idx, email, affiliation, address, phone)"
                    " VALUES ($1, $2, 'UCSD', 'X', 'Y')",
                    pidx,
                    f"{name}@test.local",
                )
            else:
                await conn.execute(
                    "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
                    pidx,
                    f"{name}@test.local",
                )
    return pidx


async def seed_service_principal(
    pool: asyncpg.Pool,
    *,
    prefix: str,
    suffix: str,
) -> int:
    """Insert a principal + qiita.service_account row; return the principal_idx.

    `prefix` and `suffix` form the display_name as f"{prefix}-{suffix}-{token}";
    the token defends against name collisions across re-runs. The service
    account row uses the principal's display_name verbatim as its `name`.
    """
    name = f"{prefix}-{suffix}-{secrets.token_hex(4)}"
    async with pool.acquire() as conn:
        async with conn.transaction():
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


async def disable_principal(pool: asyncpg.Pool, principal_idx: int) -> None:
    """Mark a principal disabled, populating the audit columns the
    qiita.principal disabled-consistency CHECK requires."""
    await pool.execute(
        "UPDATE qiita.principal SET"
        "  disabled = true, disabled_at = now(), disabled_by_idx = $2"
        " WHERE idx = $1",
        principal_idx,
        SYSTEM_PRINCIPAL_IDX,
    )


async def retire_principal(pool: asyncpg.Pool, principal_idx: int) -> None:
    """Mark a principal retired, populating the audit columns the
    qiita.principal retired-consistency CHECK requires."""
    await pool.execute(
        "UPDATE qiita.principal SET"
        "  retired = true, retired_at = now(), retired_by_idx = $2"
        " WHERE idx = $1",
        principal_idx,
        SYSTEM_PRINCIPAL_IDX,
    )


async def seed_biosample(
    pool: asyncpg.Pool,
    *,
    owner_idx: int,
    created_by_idx: int,
) -> int:
    """Insert a minimal qiita.biosample row; return its idx.

    Only the two NOT-NULL principal references are populated; every
    other column carries its schema default. Sufficient for tests that
    need a biosample idx without exercising accessions, metadata
    checklists, or the import composer.
    """
    return await pool.fetchval(
        "INSERT INTO qiita.biosample (owner_idx, created_by_idx)"
        " VALUES ($1, $2) RETURNING idx",
        owner_idx,
        created_by_idx,
    )


async def seed_biosample_to_study_link(
    pool: asyncpg.Pool,
    *,
    biosample_idx: int,
    study_idx: int,
    created_by_idx: int,
) -> None:
    """Insert a qiita.biosample_to_study link row at the active retirement state.

    The four retirement columns are CHECK-pinned to NULL/false on a
    fresh row, so they have no place in a create call; created_at
    defaults to now().
    """
    await pool.execute(
        "INSERT INTO qiita.biosample_to_study"
        "  (biosample_idx, study_idx, created_by_idx)"
        " VALUES ($1, $2, $3)",
        biosample_idx,
        study_idx,
        created_by_idx,
    )


async def retire_biosample_to_study_link(
    pool: asyncpg.Pool,
    *,
    biosample_idx: int,
    study_idx: int,
    retired_by_idx: int,
) -> None:
    """UPDATE qiita.biosample_to_study to retire the (biosample, study) link.

    Populates retired, retired_at, and retired_by_idx together so the
    biosample_to_study_retirement_consistent CHECK passes; retire_reason
    is left NULL (the CHECK allows it). Caller supplies retired_by_idx
    explicitly so the helper does not need to know which test fixture
    owns the action.
    """
    await pool.execute(
        "UPDATE qiita.biosample_to_study"
        " SET retired = true, retired_at = now(), retired_by_idx = $3"
        " WHERE biosample_idx = $1 AND study_idx = $2",
        biosample_idx,
        study_idx,
        retired_by_idx,
    )


async def retire_biosample(
    pool: asyncpg.Pool,
    *,
    biosample_idx: int,
    retired_by_idx: int,
) -> None:
    """UPDATE qiita.biosample to retire the biosample entity-wide.

    Populates retired, retired_at, and retired_by_idx together so the
    biosample_retirement_consistent CHECK passes; retire_reason is left
    NULL (the CHECK allows it). Distinct from retiring a single
    biosample_to_study link — this withdraws the sample everywhere.
    """
    await pool.execute(
        "UPDATE qiita.biosample"
        " SET retired = true, retired_at = now(), retired_by_idx = $2"
        " WHERE idx = $1",
        biosample_idx,
        retired_by_idx,
    )
