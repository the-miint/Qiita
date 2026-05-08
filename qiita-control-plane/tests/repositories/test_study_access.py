"""DB-bound tests for fetch_caller_study_access.

Exercises the SQL JOIN paths: nonexistent study, study with no
study_access row for the caller, study with an access row at a given
tier, and the owner-as-caller case (where the JOIN's owner_idx column
identifies the caller themselves).
"""

import secrets

import pytest
import pytest_asyncio
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX, SystemRole
from qiita_common.models import Tier

from qiita_control_plane.repositories.study_access import (
    CallerStudyAccessRow,
    fetch_caller_study_access,
)

pytestmark = pytest.mark.db


async def _seed_user(pool, *, suffix: str) -> int:
    """Insert a principal + qiita.user row, return the principal_idx."""
    name = f"sa-{suffix}-{secrets.token_hex(4)}"
    pidx = await pool.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, $3) RETURNING idx",
        name,
        SystemRole.USER,
        SYSTEM_PRINCIPAL_IDX,
    )
    await pool.execute(
        "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
        pidx,
        f"{name}@test.local",
    )
    return pidx


async def _seed_study(pool, *, owner_idx: int) -> int:
    """Insert a minimal qiita.study row owned by owner_idx, return its idx."""
    return await pool.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        owner_idx,
        f"sa-{secrets.token_hex(4)}",
    )


@pytest_asyncio.fixture
async def ctx(postgres_pool):
    """Seed a caller-user, an owner-user, and a study owned by the owner."""
    caller_idx = await _seed_user(postgres_pool, suffix="caller")
    owner_idx = await _seed_user(postgres_pool, suffix="owner")
    study_idx = await _seed_study(postgres_pool, owner_idx=owner_idx)

    yield {
        "pool": postgres_pool,
        "caller_idx": caller_idx,
        "owner_idx": owner_idx,
        "study_idx": study_idx,
    }

    # FK-reverse cleanup: study_access → study → user → principal.
    await postgres_pool.execute("DELETE FROM qiita.study_access WHERE study_idx = $1", study_idx)
    await postgres_pool.execute("DELETE FROM qiita.study WHERE idx = $1", study_idx)
    await postgres_pool.execute(
        "DELETE FROM qiita.user WHERE principal_idx = ANY($1::bigint[])",
        [caller_idx, owner_idx],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.principal WHERE idx = ANY($1::bigint[])",
        [caller_idx, owner_idx],
    )


async def test_fetch_caller_study_access_returns_none_for_nonexistent_study(ctx):
    row = await fetch_caller_study_access(
        ctx["pool"], principal_idx=ctx["caller_idx"], study_idx=-1
    )
    assert row is None


async def test_fetch_caller_study_access_returns_owner_with_null_tier_when_caller_has_no_access_row(
    ctx,
):
    # Caller has no study_access row; LEFT JOIN yields a row with
    # access_tier as NULL → mapped to None. Schema-default default_tier
    # ('member') applies because the seed insert omits the column.
    row = await fetch_caller_study_access(
        ctx["pool"], principal_idx=ctx["caller_idx"], study_idx=ctx["study_idx"]
    )
    assert row == CallerStudyAccessRow(
        owner_idx=ctx["owner_idx"],
        access_tier=None,
        default_tier=Tier.MEMBER,
    )


async def test_fetch_caller_study_access_returns_caller_tier_when_access_row_exists(ctx):
    # Grant the caller a member-tier access row, then verify the JOIN
    # surfaces it as a Tier enum member.
    await ctx["pool"].execute(
        "INSERT INTO qiita.study_access"
        "  (study_idx, principal_idx, access_tier)"
        " VALUES ($1, $2, $3)",
        ctx["study_idx"],
        ctx["caller_idx"],
        Tier.MEMBER,
    )

    row = await fetch_caller_study_access(
        ctx["pool"], principal_idx=ctx["caller_idx"], study_idx=ctx["study_idx"]
    )
    assert row == CallerStudyAccessRow(
        owner_idx=ctx["owner_idx"],
        access_tier=Tier.MEMBER,
        default_tier=Tier.MEMBER,
    )


async def test_fetch_caller_study_access_returns_owner_idx_for_owner_caller(ctx):
    # The owner has no study_access row of its own (owner-bypass is
    # policy-layer, not data-layer), so access_tier is None. owner_idx
    # in the row matches the caller — letting the predicate apply the
    # owner bypass.
    row = await fetch_caller_study_access(
        ctx["pool"], principal_idx=ctx["owner_idx"], study_idx=ctx["study_idx"]
    )
    assert row == CallerStudyAccessRow(
        owner_idx=ctx["owner_idx"],
        access_tier=None,
        default_tier=Tier.MEMBER,
    )


async def test_fetch_caller_study_access_surfaces_non_default_default_tier(ctx):
    # Set the study's default_tier to a non-default value and verify
    # the JOIN returns it on the row alongside the caller's access_tier.
    await ctx["pool"].execute(
        "UPDATE qiita.study SET default_tier = $1 WHERE idx = $2",
        Tier.VIEWER,
        ctx["study_idx"],
    )

    row = await fetch_caller_study_access(
        ctx["pool"], principal_idx=ctx["caller_idx"], study_idx=ctx["study_idx"]
    )
    assert row == CallerStudyAccessRow(
        owner_idx=ctx["owner_idx"],
        access_tier=None,
        default_tier=Tier.VIEWER,
    )
