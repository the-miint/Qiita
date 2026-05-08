"""DB-bound tests for fetch_user_eligibility.

Exercises the LEFT JOIN behavior: nonexistent principal, complete user,
non-user-kind principals (service account, bare), and various
state-modifier branches (disabled, retired, incomplete profile).
"""

import secrets

import pytest
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX, SystemRole

from qiita_control_plane.repositories.user_eligibility import (
    UserEligibility,
    fetch_user_eligibility,
)
from qiita_control_plane.testing.db_seeds import (
    disable_principal,
    retire_principal,
)

pytestmark = pytest.mark.db


async def _seed_principal(pool, *, suffix: str) -> int:
    """Insert a bare principal row; return principal_idx."""
    return await pool.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, $3) RETURNING idx",
        f"ue-{suffix}-{secrets.token_hex(4)}",
        SystemRole.USER,
        SYSTEM_PRINCIPAL_IDX,
    )


async def _attach_user_complete(pool, principal_idx: int) -> None:
    """Attach a qiita.user row with all required profile fields populated.

    profile_complete is a generated column over (affiliation <> '' AND
    address <> '' AND phone <> ''); supplying non-empty values makes
    the generated column True.
    """
    await pool.execute(
        "INSERT INTO qiita.user"
        "  (principal_idx, email, affiliation, address, phone)"
        " VALUES ($1, $2, $3, $4, $5)",
        principal_idx,
        f"ue-{principal_idx}@test.local",
        "Test University",
        "1 Lab Way",
        "555-0100",
    )


async def _attach_user_minimal(pool, principal_idx: int) -> None:
    """Attach a qiita.user row with only email; profile fields default to ''
    so the generated profile_complete column resolves to False."""
    await pool.execute(
        "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
        principal_idx,
        f"ue-{principal_idx}@test.local",
    )


async def _attach_service_account(pool, principal_idx: int) -> None:
    await pool.execute(
        "INSERT INTO qiita.service_account (principal_idx, name) VALUES ($1, $2)",
        principal_idx,
        f"ue-svc-{principal_idx}",
    )


async def _delete_principal(pool, principal_idx: int) -> None:
    """FK-reverse cleanup helper for a principal seeded by these tests."""
    # Subtype rows first (FK to principal is RESTRICT).
    await pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await pool.execute("DELETE FROM qiita.service_account WHERE principal_idx = $1", principal_idx)
    await pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


async def test_fetch_user_eligibility_returns_none_for_nonexistent_principal(
    postgres_pool,
):
    row = await fetch_user_eligibility(postgres_pool, principal_idx=-1)
    assert row is None


async def test_fetch_user_eligibility_returns_full_eligibility_for_complete_user(
    postgres_pool,
):
    pidx = await _seed_principal(postgres_pool, suffix="complete")
    await _attach_user_complete(postgres_pool, pidx)
    try:
        row = await fetch_user_eligibility(postgres_pool, principal_idx=pidx)
        assert row == UserEligibility(
            is_user=True, disabled=False, retired=False, profile_complete=True
        )
    finally:
        await _delete_principal(postgres_pool, pidx)


async def test_fetch_user_eligibility_returns_is_user_false_for_service_account(
    postgres_pool,
):
    pidx = await _seed_principal(postgres_pool, suffix="svc")
    await _attach_service_account(postgres_pool, pidx)
    try:
        row = await fetch_user_eligibility(postgres_pool, principal_idx=pidx)
        # No qiita.user row → is_user False, profile_complete defaults to False.
        assert row == UserEligibility(
            is_user=False, disabled=False, retired=False, profile_complete=False
        )
    finally:
        await _delete_principal(postgres_pool, pidx)


async def test_fetch_user_eligibility_returns_is_user_false_for_bare_principal(
    postgres_pool,
):
    pidx = await _seed_principal(postgres_pool, suffix="bare")
    try:
        row = await fetch_user_eligibility(postgres_pool, principal_idx=pidx)
        assert row == UserEligibility(
            is_user=False, disabled=False, retired=False, profile_complete=False
        )
    finally:
        await _delete_principal(postgres_pool, pidx)


async def test_fetch_user_eligibility_returns_disabled_true_for_disabled_user(
    postgres_pool,
):
    pidx = await _seed_principal(postgres_pool, suffix="disabled")
    await _attach_user_complete(postgres_pool, pidx)
    await disable_principal(postgres_pool, pidx)
    try:
        row = await fetch_user_eligibility(postgres_pool, principal_idx=pidx)
        assert row == UserEligibility(
            is_user=True, disabled=True, retired=False, profile_complete=True
        )
    finally:
        await _delete_principal(postgres_pool, pidx)


async def test_fetch_user_eligibility_returns_retired_true_for_retired_user(
    postgres_pool,
):
    pidx = await _seed_principal(postgres_pool, suffix="retired")
    await _attach_user_complete(postgres_pool, pidx)
    await retire_principal(postgres_pool, pidx)
    try:
        row = await fetch_user_eligibility(postgres_pool, principal_idx=pidx)
        assert row == UserEligibility(
            is_user=True, disabled=False, retired=True, profile_complete=True
        )
    finally:
        await _delete_principal(postgres_pool, pidx)


async def test_fetch_user_eligibility_returns_profile_complete_false_for_incomplete_user(
    postgres_pool,
):
    pidx = await _seed_principal(postgres_pool, suffix="incomplete")
    await _attach_user_minimal(postgres_pool, pidx)
    try:
        row = await fetch_user_eligibility(postgres_pool, principal_idx=pidx)
        assert row == UserEligibility(
            is_user=True, disabled=False, retired=False, profile_complete=False
        )
    finally:
        await _delete_principal(postgres_pool, pidx)
