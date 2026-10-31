"""DB-bound tests for fetch_caller_study_access and fetch_caller_study_access_all.

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
    fetch_caller_study_access_all,
)
from qiita_control_plane.testing.db_seeds import seed_study
from qiita_control_plane.testing.db_teardown import delete_principal, teardown_entity_graph

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


@pytest_asyncio.fixture
async def access_ctx(postgres_pool):
    """Seed a caller-user, an owner-user, and a study owned by the owner."""
    caller_idx = await _seed_user(postgres_pool, suffix="caller")
    owner_idx = await _seed_user(postgres_pool, suffix="owner")
    study_idx = await seed_study(
        postgres_pool, owner_idx=owner_idx, title=f"sa-{secrets.token_hex(4)}"
    )

    yield {
        "pool": postgres_pool,
        "caller_idx": caller_idx,
        "owner_idx": owner_idx,
        "study_idx": study_idx,
    }

    await teardown_entity_graph(
        postgres_pool, study_idxs=[study_idx], biosample_idxs=[], prep_sample_idxs=[]
    )
    await delete_principal(postgres_pool, [caller_idx, owner_idx])


async def test_fetch_caller_study_access_returns_none_for_nonexistent_study(access_ctx):
    row = await fetch_caller_study_access(
        access_ctx["pool"], principal_idx=access_ctx["caller_idx"], study_idx=-1
    )
    assert row is None


async def test_fetch_caller_study_access_returns_owner_with_null_tier_when_caller_has_no_access_row(
    access_ctx,
):
    # Caller has no study_access row; LEFT JOIN yields a row with
    # access_tier as NULL → mapped to None. default_tier is seed_study's
    # default, 'member', the column's own.
    row = await fetch_caller_study_access(
        access_ctx["pool"],
        principal_idx=access_ctx["caller_idx"],
        study_idx=access_ctx["study_idx"],
    )
    assert row == CallerStudyAccessRow(
        owner_idx=access_ctx["owner_idx"],
        access_tier=None,
        default_tier=Tier.MEMBER,
    )


async def test_fetch_caller_study_access_returns_caller_tier_when_access_row_exists(access_ctx):
    # Grant the caller a member-tier access row, then verify the JOIN
    # surfaces it as a Tier enum member.
    await access_ctx["pool"].execute(
        "INSERT INTO qiita.study_access"
        "  (study_idx, principal_idx, access_tier)"
        " VALUES ($1, $2, $3)",
        access_ctx["study_idx"],
        access_ctx["caller_idx"],
        Tier.MEMBER,
    )

    row = await fetch_caller_study_access(
        access_ctx["pool"],
        principal_idx=access_ctx["caller_idx"],
        study_idx=access_ctx["study_idx"],
    )
    assert row == CallerStudyAccessRow(
        owner_idx=access_ctx["owner_idx"],
        access_tier=Tier.MEMBER,
        default_tier=Tier.MEMBER,
    )


async def test_fetch_caller_study_access_returns_owner_idx_for_owner_caller(access_ctx):
    # The owner has no study_access row of its own (owner-bypass is
    # policy-layer, not data-layer), so access_tier is None. owner_idx
    # in the row matches the caller — letting the predicate apply the
    # owner bypass.
    row = await fetch_caller_study_access(
        access_ctx["pool"], principal_idx=access_ctx["owner_idx"], study_idx=access_ctx["study_idx"]
    )
    assert row == CallerStudyAccessRow(
        owner_idx=access_ctx["owner_idx"],
        access_tier=None,
        default_tier=Tier.MEMBER,
    )


async def test_fetch_caller_study_access_surfaces_non_default_default_tier(access_ctx):
    # Set the study's default_tier to a non-default value and verify
    # the JOIN returns it on the row alongside the caller's access_tier.
    await access_ctx["pool"].execute(
        "UPDATE qiita.study SET default_tier = $1 WHERE idx = $2",
        Tier.VIEWER,
        access_ctx["study_idx"],
    )

    row = await fetch_caller_study_access(
        access_ctx["pool"],
        principal_idx=access_ctx["caller_idx"],
        study_idx=access_ctx["study_idx"],
    )
    assert row == CallerStudyAccessRow(
        owner_idx=access_ctx["owner_idx"],
        access_tier=None,
        default_tier=Tier.VIEWER,
    )


async def _grant(pool, *, study_idx, principal_idx, tier):
    await pool.execute(
        "INSERT INTO qiita.study_access (study_idx, principal_idx, access_tier)"
        " VALUES ($1, $2, $3)",
        study_idx,
        principal_idx,
        tier,
    )


async def test_fetch_caller_study_access_all_returns_every_owned_or_granted_study(access_ctx):
    """One row per study the caller owns or holds a grant on, and no other.

    The caller owns one study with no row of its own, holds a grant on the
    fixture's study, and both owns and holds a row on a third (one entry, not
    two). A study the caller has nothing to do with is absent, as is another
    principal's grant.
    """
    pool = access_ctx["pool"]
    caller_idx = access_ctx["caller_idx"]
    owner_idx = access_ctx["owner_idx"]
    owned = await seed_study(pool, owner_idx=caller_idx, title=f"sa-own-{secrets.token_hex(4)}")
    owned_and_granted = await seed_study(
        pool, owner_idx=caller_idx, title=f"sa-both-{secrets.token_hex(4)}"
    )
    unrelated = await seed_study(pool, owner_idx=owner_idx, title=f"sa-none-{secrets.token_hex(4)}")
    extra_studies = [owned, owned_and_granted, unrelated]
    try:
        await _grant(
            pool, study_idx=access_ctx["study_idx"], principal_idx=caller_idx, tier=Tier.VIEWER
        )
        await _grant(pool, study_idx=owned_and_granted, principal_idx=caller_idx, tier=Tier.MEMBER)
        await _grant(pool, study_idx=unrelated, principal_idx=owner_idx, tier=Tier.ADMIN)

        rows = await fetch_caller_study_access_all(pool, principal_idx=caller_idx)

        assert rows == {
            owned: CallerStudyAccessRow(
                owner_idx=caller_idx, access_tier=None, default_tier=Tier.MEMBER
            ),
            access_ctx["study_idx"]: CallerStudyAccessRow(
                owner_idx=owner_idx, access_tier=Tier.VIEWER, default_tier=Tier.MEMBER
            ),
            owned_and_granted: CallerStudyAccessRow(
                owner_idx=caller_idx, access_tier=Tier.MEMBER, default_tier=Tier.MEMBER
            ),
        }
    finally:
        await teardown_entity_graph(
            pool, study_idxs=extra_studies, biosample_idxs=[], prep_sample_idxs=[]
        )


async def test_fetch_caller_study_access_all_is_empty_for_a_caller_with_nothing(access_ctx):
    rows = await fetch_caller_study_access_all(
        access_ctx["pool"], principal_idx=access_ctx["caller_idx"]
    )
    assert rows == {}
