"""Unit tests for guards. Guards are FastAPI deps; we exercise them by
synthesising the dep input directly (no FastAPI request needed).

Most guards in this module operate purely on a passed-in Principal and
require no DB. `require_study_access` is the exception — it takes the
study_idx from the path, looks up the caller's access row, and applies
the tier policy. Its tests carry `pytest.mark.db` and seed real rows
via `postgres_pool`.
"""

import secrets

import pytest
import pytest_asyncio
from fastapi import HTTPException
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX, Scope, SystemRole
from qiita_common.models import Tier


def _human(*, role=SystemRole.USER, scopes=frozenset(), profile_complete=True):
    from qiita_control_plane.auth.principal import HumanUser

    return HumanUser(
        principal_idx=2,
        email="alice@example.com",
        system_role=role,
        scopes=scopes,
        profile_complete=profile_complete,
        disabled=False,
        retired=False,
    )


def _service(scopes=frozenset()):
    from qiita_control_plane.auth.principal import ServiceAccount

    return ServiceAccount(
        principal_idx=3,
        name="orchestrator",
        scopes=scopes,
        disabled=False,
        retired=False,
    )


def _anon():
    from qiita_control_plane.auth.principal import Anonymous

    return Anonymous()


# ---------------------------------------------------------------------------
# require_human / require_service
# ---------------------------------------------------------------------------


def test_require_human_returns_human():
    from qiita_control_plane.auth.guards import require_human

    h = _human()
    assert require_human(h) is h


def test_require_human_401_on_anonymous():
    from qiita_control_plane.auth.guards import require_human

    with pytest.raises(HTTPException) as exc:
        require_human(_anon())
    assert exc.value.status_code == 401


def test_require_human_403_on_service_account():
    from qiita_control_plane.auth.guards import require_human

    with pytest.raises(HTTPException) as exc:
        require_human(_service())
    assert exc.value.status_code == 403


def test_require_service_returns_service():
    from qiita_control_plane.auth.guards import require_service

    s = _service()
    assert require_service(s) is s


def test_require_service_401_on_anonymous():
    from qiita_control_plane.auth.guards import require_service

    with pytest.raises(HTTPException) as exc:
        require_service(_anon())
    assert exc.value.status_code == 401


def test_require_service_403_on_human():
    from qiita_control_plane.auth.guards import require_service

    with pytest.raises(HTTPException) as exc:
        require_service(_human())
    assert exc.value.status_code == 403


# ---------------------------------------------------------------------------
# require_role_at_least
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "user_role,required,should_pass",
    [
        (SystemRole.USER, SystemRole.USER, True),
        (SystemRole.USER, SystemRole.WET_LAB_ADMIN, False),
        (SystemRole.USER, SystemRole.SYSTEM_ADMIN, False),
        (SystemRole.WET_LAB_ADMIN, SystemRole.USER, True),
        (SystemRole.WET_LAB_ADMIN, SystemRole.WET_LAB_ADMIN, True),
        (SystemRole.WET_LAB_ADMIN, SystemRole.SYSTEM_ADMIN, False),
        (SystemRole.SYSTEM_ADMIN, SystemRole.USER, True),
        (SystemRole.SYSTEM_ADMIN, SystemRole.WET_LAB_ADMIN, True),
        (SystemRole.SYSTEM_ADMIN, SystemRole.SYSTEM_ADMIN, True),
    ],
)
def test_require_role_at_least_matrix(user_role, required, should_pass):
    from qiita_control_plane.auth.guards import require_role_at_least

    dep = require_role_at_least(required)
    if should_pass:
        assert dep(_human(role=user_role))
    else:
        with pytest.raises(HTTPException) as exc:
            dep(_human(role=user_role))
        assert exc.value.status_code == 403


def test_require_role_at_least_401_on_anonymous():
    from qiita_control_plane.auth.guards import require_role_at_least

    with pytest.raises(HTTPException) as exc:
        require_role_at_least(SystemRole.USER)(_anon())
    assert exc.value.status_code == 401


def test_require_role_at_least_returns_403_for_service_account():
    """Service accounts don't fit the human hierarchy — every role check
    against them is a 403 (not a 401)."""
    from qiita_control_plane.auth.guards import require_role_at_least

    with pytest.raises(HTTPException) as exc:
        require_role_at_least(SystemRole.USER)(_service())
    assert exc.value.status_code == 403


# ---------------------------------------------------------------------------
# require_scope
# ---------------------------------------------------------------------------


def test_require_scope_pass_when_scope_present():
    from qiita_control_plane.auth.guards import require_scope

    dep = require_scope(Scope.REFERENCE_READ)
    h = _human(scopes=frozenset({Scope.SELF_PROFILE, Scope.REFERENCE_READ}))
    assert dep(h) is h


def test_require_scope_403_when_scope_missing():
    from qiita_control_plane.auth.guards import require_scope

    dep = require_scope(Scope.ADMIN_USER)
    with pytest.raises(HTTPException) as exc:
        dep(_human(scopes=frozenset({Scope.SELF_PROFILE})))
    assert exc.value.status_code == 403


def test_require_scope_401_on_anonymous():
    from qiita_control_plane.auth.guards import require_scope

    with pytest.raises(HTTPException) as exc:
        require_scope(Scope.SELF_PROFILE)(_anon())
    assert exc.value.status_code == 401


def test_require_scope_works_for_service_account_with_matching_scope():
    """Service accounts pass scope checks identically to humans — the only
    role constraint is from require_role_at_least, not require_scope."""
    from qiita_control_plane.auth.guards import require_scope

    s = _service(scopes=frozenset({Scope.FEATURE_MINT}))
    dep = require_scope(Scope.FEATURE_MINT)
    assert dep(s) is s


# ---------------------------------------------------------------------------
# require_any_scope — passes on ANY one of the listed scopes
# ---------------------------------------------------------------------------


def test_require_any_scope_pass_when_first_present():
    from qiita_control_plane.auth.guards import require_any_scope

    dep = require_any_scope(Scope.PREP_SAMPLE_READ, Scope.SEQUENCE_RANGE_MINT)
    h = _human(scopes=frozenset({Scope.PREP_SAMPLE_READ}))
    assert dep(h) is h


def test_require_any_scope_pass_when_second_present():
    """The motivating case: the compute SA holds sequence_range:mint but not
    prep_sample:read and must still pass the GET /sequence-range guard."""
    from qiita_control_plane.auth.guards import require_any_scope

    s = _service(scopes=frozenset({Scope.SEQUENCE_RANGE_MINT}))
    dep = require_any_scope(Scope.PREP_SAMPLE_READ, Scope.SEQUENCE_RANGE_MINT)
    assert dep(s) is s


def test_require_any_scope_403_when_none_present():
    from qiita_control_plane.auth.guards import require_any_scope

    dep = require_any_scope(Scope.PREP_SAMPLE_READ, Scope.SEQUENCE_RANGE_MINT)
    with pytest.raises(HTTPException) as exc:
        dep(_service(scopes=frozenset({Scope.FEATURE_MINT})))
    assert exc.value.status_code == 403


def test_require_any_scope_401_on_anonymous():
    from qiita_control_plane.auth.guards import require_any_scope

    with pytest.raises(HTTPException) as exc:
        require_any_scope(Scope.PREP_SAMPLE_READ, Scope.SEQUENCE_RANGE_MINT)(_anon())
    assert exc.value.status_code == 401


# ---------------------------------------------------------------------------
# stale-token hint: a scope-403 whose real cause is a PAT minted before the
# scope was added to the caller's role ceiling should tell them to re-login.
# ---------------------------------------------------------------------------


def test_require_scope_403_stale_token_hints_relogin():
    """Role grants the scope (it's in the live ceiling) but the token predates
    the grant — the 403 detail carries an actionable re-login hint."""
    from qiita_control_plane.auth.guards import require_scope
    from qiita_control_plane.auth.scopes import role_ceiling

    granted = next(iter(role_ceiling(SystemRole.SYSTEM_ADMIN)))
    dep = require_scope(granted)
    # admin role, but the token itself carries none of its ceiling scopes.
    h = _human(role=SystemRole.SYSTEM_ADMIN, scopes=frozenset())
    with pytest.raises(HTTPException) as exc:
        dep(h)
    assert exc.value.status_code == 403
    assert "qiita login" in exc.value.detail


def test_require_scope_403_no_hint_when_scope_outside_ceiling():
    """A genuinely-unentitled caller (scope not in their role ceiling) gets the
    plain 403 with no re-login hint — re-minting wouldn't help them."""
    from qiita_control_plane.auth.guards import require_scope
    from qiita_control_plane.auth.scopes import role_ceiling

    assert Scope.ADMIN_USER not in role_ceiling(SystemRole.USER)
    dep = require_scope(Scope.ADMIN_USER)
    with pytest.raises(HTTPException) as exc:
        dep(_human(role=SystemRole.USER, scopes=frozenset({Scope.SELF_PROFILE})))
    assert exc.value.status_code == 403
    assert "qiita login" not in exc.value.detail


def test_require_scope_403_no_hint_for_service_account():
    """Service accounts don't use the role ceiling, so no re-login hint."""
    from qiita_control_plane.auth.guards import require_scope

    dep = require_scope(Scope.FEATURE_MINT)
    with pytest.raises(HTTPException) as exc:
        dep(_service(scopes=frozenset()))
    assert exc.value.status_code == 403
    assert "qiita login" not in exc.value.detail


def test_require_any_scope_403_stale_token_hints_relogin():
    from qiita_control_plane.auth.guards import require_any_scope
    from qiita_control_plane.auth.scopes import role_ceiling

    granted = next(iter(role_ceiling(SystemRole.SYSTEM_ADMIN)))
    dep = require_any_scope(granted, Scope.ADMIN_USER)
    h = _human(role=SystemRole.SYSTEM_ADMIN, scopes=frozenset())
    with pytest.raises(HTTPException) as exc:
        dep(h)
    assert exc.value.status_code == 403
    assert "qiita login" in exc.value.detail


# ---------------------------------------------------------------------------
# require_human_with_role — composes require_human + role check
# ---------------------------------------------------------------------------
# These tests exercise the role-check body of the helper directly.
# Anonymous/Service rejection is delegated to require_human (tested above)
# via the FastAPI dep chain; calling the body with a non-human input is
# not a real production code path.


def test_require_human_with_role_returns_human_user_with_sufficient_role():
    from qiita_control_plane.auth.guards import require_human_with_role

    dep = require_human_with_role(SystemRole.USER)
    h = _human(role=SystemRole.SYSTEM_ADMIN)
    assert dep(h) is h


def test_require_human_with_role_403_on_insufficient_role():
    from qiita_control_plane.auth.guards import require_human_with_role

    dep = require_human_with_role(SystemRole.SYSTEM_ADMIN)
    with pytest.raises(HTTPException) as exc:
        dep(_human(role=SystemRole.WET_LAB_ADMIN))
    assert exc.value.status_code == 403


# ---------------------------------------------------------------------------
# require_complete_profile
# ---------------------------------------------------------------------------


def test_require_complete_profile_passes_when_complete():
    from qiita_control_plane.auth.guards import require_complete_profile

    h = _human(profile_complete=True)
    assert require_complete_profile(h) is h


def test_require_complete_profile_422_when_incomplete():
    from qiita_control_plane.auth.guards import require_complete_profile

    h = _human(profile_complete=False)
    with pytest.raises(HTTPException) as exc:
        require_complete_profile(h)
    assert exc.value.status_code == 422
    assert exc.value.detail["reason"] == "profile_incomplete"
    # The guard does NOT carry per-field missing list — the route layer
    # (POST /auth/pat) owns that, since it can pull the actual field
    # values from the DB.
    assert "missing_fields" not in exc.value.detail


# ---------------------------------------------------------------------------
# require_study_access (DB-bound)
# ---------------------------------------------------------------------------
# Resource-access guards consult the DB; tests below seed real rows. Each
# test calls the dep directly with a synthesised Principal and the
# postgres_pool fixture instead of going through a FastAPI request.


async def _seed_user_for_study(pool, *, suffix: str, role: SystemRole = SystemRole.USER) -> int:
    name = f"req-sa-{suffix}-{secrets.token_hex(4)}"
    pidx = await pool.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, $3) RETURNING idx",
        name,
        role,
        SYSTEM_PRINCIPAL_IDX,
    )
    await pool.execute(
        "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
        pidx,
        f"{name}@test.local",
    )
    return pidx


async def _seed_study_for_test(pool, *, owner_idx: int) -> int:
    return await pool.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        owner_idx,
        f"req-sa-{secrets.token_hex(4)}",
    )


def _human_with_idx(principal_idx: int, role: SystemRole = SystemRole.USER):
    """HumanUser pinned to a given principal_idx. Required for DB tests
    where the synthesised principal must match a real seeded user."""
    from qiita_control_plane.auth.principal import HumanUser

    return HumanUser(
        principal_idx=principal_idx,
        email=f"u{principal_idx}@test.local",
        system_role=role,
        scopes=frozenset(),
        profile_complete=True,
        disabled=False,
        retired=False,
    )


@pytest_asyncio.fixture
async def study_access_ctx(postgres_pool):
    """Seed a caller-user, an owner-user, and a study owned by the owner."""
    caller_idx = await _seed_user_for_study(postgres_pool, suffix="caller")
    owner_idx = await _seed_user_for_study(postgres_pool, suffix="owner")
    study_idx = await _seed_study_for_test(postgres_pool, owner_idx=owner_idx)

    yield {
        "pool": postgres_pool,
        "caller_idx": caller_idx,
        "owner_idx": owner_idx,
        "study_idx": study_idx,
    }

    # FK-reverse cleanup.
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


@pytest.mark.db
async def test_require_study_access_anonymous_raises_401(study_access_ctx):
    from qiita_control_plane.auth.guards import require_study_access

    dep = require_study_access(Tier.MEMBER)
    with pytest.raises(HTTPException) as exc:
        await dep(
            study_idx=study_access_ctx["study_idx"],
            p=_anon(),
            pool=study_access_ctx["pool"],
        )
    assert exc.value.status_code == 401


@pytest.mark.db
async def test_require_study_access_system_admin_bypasses(study_access_ctx):
    # System admin bypasses without needing a study_access row.
    from qiita_control_plane.auth.guards import require_study_access

    dep = require_study_access(Tier.ADMIN)
    admin = _human_with_idx(study_access_ctx["caller_idx"], role=SystemRole.SYSTEM_ADMIN)
    result = await dep(
        study_idx=study_access_ctx["study_idx"],
        p=admin,
        pool=study_access_ctx["pool"],
    )
    assert result is None


@pytest.mark.db
async def test_require_study_access_owner_bypasses(study_access_ctx):
    # Study owner authorized at every tier without needing a study_access row.
    from qiita_control_plane.auth.guards import require_study_access

    dep = require_study_access(Tier.ADMIN)
    owner = _human_with_idx(study_access_ctx["owner_idx"])
    result = await dep(
        study_idx=study_access_ctx["study_idx"],
        p=owner,
        pool=study_access_ctx["pool"],
    )
    assert result is None


@pytest.mark.db
async def test_require_study_access_admin_tier_meets_member_min(study_access_ctx):
    from qiita_control_plane.auth.guards import require_study_access

    await study_access_ctx["pool"].execute(
        "INSERT INTO qiita.study_access (study_idx, principal_idx, access_tier)"
        " VALUES ($1, $2, $3)",
        study_access_ctx["study_idx"],
        study_access_ctx["caller_idx"],
        Tier.ADMIN,
    )
    dep = require_study_access(Tier.MEMBER)
    caller = _human_with_idx(study_access_ctx["caller_idx"])
    result = await dep(
        study_idx=study_access_ctx["study_idx"],
        p=caller,
        pool=study_access_ctx["pool"],
    )
    assert result is None


@pytest.mark.db
async def test_require_study_access_viewer_tier_below_member_min_raises_403(
    study_access_ctx,
):
    from qiita_control_plane.auth.guards import require_study_access

    await study_access_ctx["pool"].execute(
        "INSERT INTO qiita.study_access (study_idx, principal_idx, access_tier)"
        " VALUES ($1, $2, $3)",
        study_access_ctx["study_idx"],
        study_access_ctx["caller_idx"],
        Tier.VIEWER,
    )
    dep = require_study_access(Tier.MEMBER)
    caller = _human_with_idx(study_access_ctx["caller_idx"])
    with pytest.raises(HTTPException) as exc:
        await dep(
            study_idx=study_access_ctx["study_idx"],
            p=caller,
            pool=study_access_ctx["pool"],
        )
    assert exc.value.status_code == 403


@pytest.mark.db
async def test_require_study_access_no_access_row_below_min_raises_403(
    study_access_ctx,
):
    # Caller has no study_access row → effective tier PUBLIC, fails MEMBER.
    from qiita_control_plane.auth.guards import require_study_access

    dep = require_study_access(Tier.MEMBER)
    caller = _human_with_idx(study_access_ctx["caller_idx"])
    with pytest.raises(HTTPException) as exc:
        await dep(
            study_idx=study_access_ctx["study_idx"],
            p=caller,
            pool=study_access_ctx["pool"],
        )
    assert exc.value.status_code == 403


@pytest.mark.db
async def test_require_study_access_no_access_row_with_public_min_passes(
    study_access_ctx,
):
    # Caller has no study_access row → effective tier PUBLIC, meets PUBLIC.
    from qiita_control_plane.auth.guards import require_study_access

    dep = require_study_access(Tier.PUBLIC)
    caller = _human_with_idx(study_access_ctx["caller_idx"])
    result = await dep(
        study_idx=study_access_ctx["study_idx"],
        p=caller,
        pool=study_access_ctx["pool"],
    )
    assert result is None


@pytest.mark.db
async def test_require_study_access_nonexistent_study_raises_404(study_access_ctx):
    from qiita_control_plane.auth.guards import require_study_access

    dep = require_study_access(Tier.PUBLIC)
    caller = _human_with_idx(study_access_ctx["caller_idx"])
    with pytest.raises(HTTPException) as exc:
        await dep(
            study_idx=-1,
            p=caller,
            pool=study_access_ctx["pool"],
        )
    assert exc.value.status_code == 404


# ---------------------------------------------------------------------------
# require_study_access — min_tier=None resolves to study.default_tier
# ---------------------------------------------------------------------------
# When the factory is called without an explicit min_tier, the inner _dep
# uses the study's own default_tier as the per-request floor.


@pytest.mark.db
async def test_require_study_access_min_tier_none_passes_caller_at_default_tier(
    study_access_ctx,
):
    # Study's default_tier is the schema-default 'member'; granting the
    # caller a member access row meets that floor.
    from qiita_control_plane.auth.guards import require_study_access

    await study_access_ctx["pool"].execute(
        "INSERT INTO qiita.study_access (study_idx, principal_idx, access_tier)"
        " VALUES ($1, $2, $3)",
        study_access_ctx["study_idx"],
        study_access_ctx["caller_idx"],
        Tier.MEMBER,
    )
    dep = require_study_access()
    caller = _human_with_idx(study_access_ctx["caller_idx"])
    result = await dep(
        study_idx=study_access_ctx["study_idx"],
        p=caller,
        pool=study_access_ctx["pool"],
    )
    assert result is None


@pytest.mark.db
async def test_require_study_access_min_tier_none_403_when_below_default_tier(
    study_access_ctx,
):
    # Study default_tier=member; caller has only viewer → 403.
    from qiita_control_plane.auth.guards import require_study_access

    await study_access_ctx["pool"].execute(
        "INSERT INTO qiita.study_access (study_idx, principal_idx, access_tier)"
        " VALUES ($1, $2, $3)",
        study_access_ctx["study_idx"],
        study_access_ctx["caller_idx"],
        Tier.VIEWER,
    )
    dep = require_study_access()
    caller = _human_with_idx(study_access_ctx["caller_idx"])
    with pytest.raises(HTTPException) as exc:
        await dep(
            study_idx=study_access_ctx["study_idx"],
            p=caller,
            pool=study_access_ctx["pool"],
        )
    assert exc.value.status_code == 403
    # The 403 detail names the resolved minimum, not the literal None.
    assert "'member'" in exc.value.detail


@pytest.mark.db
async def test_require_study_access_min_tier_none_passes_when_default_is_public(
    study_access_ctx,
):
    # Study default_tier=public; caller with no access row has effective
    # tier public-by-absence which meets the floor.
    from qiita_control_plane.auth.guards import require_study_access

    await study_access_ctx["pool"].execute(
        "UPDATE qiita.study SET default_tier = $1 WHERE idx = $2",
        Tier.PUBLIC,
        study_access_ctx["study_idx"],
    )
    dep = require_study_access()
    caller = _human_with_idx(study_access_ctx["caller_idx"])
    result = await dep(
        study_idx=study_access_ctx["study_idx"],
        p=caller,
        pool=study_access_ctx["pool"],
    )
    assert result is None


# ---------------------------------------------------------------------------
# require_study_access — bypass_role parameterization
# ---------------------------------------------------------------------------


@pytest.mark.db
async def test_require_study_access_bypass_role_wet_lab_admin_bypasses(
    study_access_ctx,
):
    # bypass_role=WET_LAB_ADMIN lets a wet_lab_admin caller through
    # without a study_access row, even when the study's default_tier
    # would otherwise require member.
    from qiita_control_plane.auth.guards import require_study_access

    dep = require_study_access(bypass_role=SystemRole.WET_LAB_ADMIN)
    caller = _human_with_idx(study_access_ctx["caller_idx"], role=SystemRole.WET_LAB_ADMIN)
    result = await dep(
        study_idx=study_access_ctx["study_idx"],
        p=caller,
        pool=study_access_ctx["pool"],
    )
    assert result is None


@pytest.mark.db
async def test_require_study_access_bypass_role_wet_lab_admin_does_not_bypass_regular_user(
    study_access_ctx,
):
    # Regular user is below WET_LAB_ADMIN threshold → falls through to
    # the tier comparison; no access row → public-by-absence vs the
    # study's default_tier=member yields 403.
    from qiita_control_plane.auth.guards import require_study_access

    dep = require_study_access(bypass_role=SystemRole.WET_LAB_ADMIN)
    caller = _human_with_idx(study_access_ctx["caller_idx"])
    with pytest.raises(HTTPException) as exc:
        await dep(
            study_idx=study_access_ctx["study_idx"],
            p=caller,
            pool=study_access_ctx["pool"],
        )
    assert exc.value.status_code == 403


@pytest.mark.db
async def test_require_study_access_bypass_role_wet_lab_admin_admits_system_admin(
    study_access_ctx,
):
    # has_role_at_least is monotonic, so a system_admin caller passes
    # a WET_LAB_ADMIN bypass threshold trivially.
    from qiita_control_plane.auth.guards import require_study_access

    dep = require_study_access(bypass_role=SystemRole.WET_LAB_ADMIN)
    caller = _human_with_idx(study_access_ctx["caller_idx"], role=SystemRole.SYSTEM_ADMIN)
    result = await dep(
        study_idx=study_access_ctx["study_idx"],
        p=caller,
        pool=study_access_ctx["pool"],
    )
    assert result is None


# ---------------------------------------------------------------------------
# require_study_exists (DB-bound)
# ---------------------------------------------------------------------------
# Existence-only sibling of require_study_access; no Principal in scope, so
# tests synthesize the call with just study_idx and pool.


@pytest.mark.db
async def test_require_study_exists_passes_for_existing_study(study_access_ctx):
    from qiita_control_plane.auth.guards import require_study_exists

    # The seeded study's idx must pass the guard with no return value.
    result = await require_study_exists(
        study_idx=study_access_ctx["study_idx"],
        pool=study_access_ctx["pool"],
    )
    assert result is None


@pytest.mark.db
async def test_require_study_exists_raises_404_for_missing_study(study_access_ctx):
    from qiita_control_plane.auth.guards import require_study_exists

    # A negative idx never matches because the IDENTITY column only
    # issues positive values.
    with pytest.raises(HTTPException) as exc:
        await require_study_exists(
            study_idx=-1,
            pool=study_access_ctx["pool"],
        )
    assert exc.value.status_code == 404
    assert "study -1 not found" in exc.value.detail


# ---------------------------------------------------------------------------
# require_caller_owns_run / require_caller_owns_pool (DB-bound)
# ---------------------------------------------------------------------------
# Caller-creator predicates over sequencing_run and sequenced_pool. Both
# guards share the same shape: anonymous → 401; role-bypass at or above
# bypass_role; otherwise 404 on missing resource and 403 on a non-creator
# caller. Tests seed real rows so we exercise the asyncpg.Record path
# (created_by_idx column read) end-to-end.


async def _seed_sequencing_run_for_test(pool, *, created_by_idx: int) -> int:
    return await pool.fetchval(
        "INSERT INTO qiita.sequencing_run ("
        "  instrument_run_id, platform, created_by_idx"
        ") VALUES ($1, 'illumina', $2) RETURNING idx",
        f"run-{secrets.token_hex(4)}",
        created_by_idx,
    )


async def _seed_sequenced_pool_for_test(
    pool, *, sequencing_run_idx: int, created_by_idx: int
) -> int:
    return await pool.fetchval(
        "INSERT INTO qiita.sequenced_pool ("
        "  sequencing_run_idx, created_by_idx"
        ") VALUES ($1, $2) RETURNING idx",
        sequencing_run_idx,
        created_by_idx,
    )


@pytest_asyncio.fixture
async def run_and_pool_ctx(postgres_pool):
    """Seed a creator-user, a stranger-user, a sequencing_run owned by the
    creator, and a sequenced_pool attached to that run, also owned by the
    creator. Cleanup walks the FK chain in reverse."""
    creator_idx = await _seed_user_for_study(postgres_pool, suffix="creator")
    stranger_idx = await _seed_user_for_study(postgres_pool, suffix="stranger")
    run_idx = await _seed_sequencing_run_for_test(postgres_pool, created_by_idx=creator_idx)
    pool_idx = await _seed_sequenced_pool_for_test(
        postgres_pool, sequencing_run_idx=run_idx, created_by_idx=creator_idx
    )

    yield {
        "pool": postgres_pool,
        "creator_idx": creator_idx,
        "stranger_idx": stranger_idx,
        "run_idx": run_idx,
        "pool_idx": pool_idx,
    }

    await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await postgres_pool.execute(
        "DELETE FROM qiita.user WHERE principal_idx = ANY($1::bigint[])",
        [creator_idx, stranger_idx],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.principal WHERE idx = ANY($1::bigint[])",
        [creator_idx, stranger_idx],
    )


@pytest.mark.db
async def test_require_caller_owns_run_creator_passes(run_and_pool_ctx):
    from qiita_control_plane.auth.guards import require_caller_owns_run

    dep = require_caller_owns_run()
    caller = _human_with_idx(run_and_pool_ctx["creator_idx"])
    result = await dep(
        sequencing_run_idx=run_and_pool_ctx["run_idx"],
        p=caller,
        pool=run_and_pool_ctx["pool"],
    )
    assert result is None


@pytest.mark.db
async def test_require_caller_owns_run_stranger_raises_403(run_and_pool_ctx):
    from qiita_control_plane.auth.guards import require_caller_owns_run

    dep = require_caller_owns_run()
    caller = _human_with_idx(run_and_pool_ctx["stranger_idx"])
    with pytest.raises(HTTPException) as exc:
        await dep(
            sequencing_run_idx=run_and_pool_ctx["run_idx"],
            p=caller,
            pool=run_and_pool_ctx["pool"],
        )
    assert exc.value.status_code == 403


@pytest.mark.db
async def test_require_caller_owns_run_wet_lab_admin_bypasses(run_and_pool_ctx):
    from qiita_control_plane.auth.guards import require_caller_owns_run

    dep = require_caller_owns_run()
    caller = _human_with_idx(run_and_pool_ctx["stranger_idx"], role=SystemRole.WET_LAB_ADMIN)
    result = await dep(
        sequencing_run_idx=run_and_pool_ctx["run_idx"],
        p=caller,
        pool=run_and_pool_ctx["pool"],
    )
    assert result is None


@pytest.mark.db
async def test_require_caller_owns_run_system_admin_bypasses(run_and_pool_ctx):
    from qiita_control_plane.auth.guards import require_caller_owns_run

    dep = require_caller_owns_run()
    caller = _human_with_idx(run_and_pool_ctx["stranger_idx"], role=SystemRole.SYSTEM_ADMIN)
    result = await dep(
        sequencing_run_idx=run_and_pool_ctx["run_idx"],
        p=caller,
        pool=run_and_pool_ctx["pool"],
    )
    assert result is None


@pytest.mark.db
async def test_require_caller_owns_run_anonymous_raises_401(run_and_pool_ctx):
    from qiita_control_plane.auth.guards import require_caller_owns_run

    dep = require_caller_owns_run()
    with pytest.raises(HTTPException) as exc:
        await dep(
            sequencing_run_idx=run_and_pool_ctx["run_idx"],
            p=_anon(),
            pool=run_and_pool_ctx["pool"],
        )
    assert exc.value.status_code == 401


@pytest.mark.db
async def test_require_caller_owns_run_missing_run_raises_404(run_and_pool_ctx):
    from qiita_control_plane.auth.guards import require_caller_owns_run

    dep = require_caller_owns_run()
    caller = _human_with_idx(run_and_pool_ctx["creator_idx"])
    with pytest.raises(HTTPException) as exc:
        await dep(
            sequencing_run_idx=-1,
            p=caller,
            pool=run_and_pool_ctx["pool"],
        )
    assert exc.value.status_code == 404


@pytest.mark.db
async def test_require_caller_owns_pool_creator_passes(run_and_pool_ctx):
    from qiita_control_plane.auth.guards import require_caller_owns_pool

    dep = require_caller_owns_pool()
    caller = _human_with_idx(run_and_pool_ctx["creator_idx"])
    result = await dep(
        sequenced_pool_idx=run_and_pool_ctx["pool_idx"],
        p=caller,
        pool=run_and_pool_ctx["pool"],
    )
    assert result is None


@pytest.mark.db
async def test_require_caller_owns_pool_stranger_raises_403(run_and_pool_ctx):
    from qiita_control_plane.auth.guards import require_caller_owns_pool

    dep = require_caller_owns_pool()
    caller = _human_with_idx(run_and_pool_ctx["stranger_idx"])
    with pytest.raises(HTTPException) as exc:
        await dep(
            sequenced_pool_idx=run_and_pool_ctx["pool_idx"],
            p=caller,
            pool=run_and_pool_ctx["pool"],
        )
    assert exc.value.status_code == 403


@pytest.mark.db
async def test_require_caller_owns_pool_wet_lab_admin_bypasses(run_and_pool_ctx):
    from qiita_control_plane.auth.guards import require_caller_owns_pool

    dep = require_caller_owns_pool()
    caller = _human_with_idx(run_and_pool_ctx["stranger_idx"], role=SystemRole.WET_LAB_ADMIN)
    result = await dep(
        sequenced_pool_idx=run_and_pool_ctx["pool_idx"],
        p=caller,
        pool=run_and_pool_ctx["pool"],
    )
    assert result is None


@pytest.mark.db
async def test_require_caller_owns_pool_anonymous_raises_401(run_and_pool_ctx):
    from qiita_control_plane.auth.guards import require_caller_owns_pool

    dep = require_caller_owns_pool()
    with pytest.raises(HTTPException) as exc:
        await dep(
            sequenced_pool_idx=run_and_pool_ctx["pool_idx"],
            p=_anon(),
            pool=run_and_pool_ctx["pool"],
        )
    assert exc.value.status_code == 401


@pytest.mark.db
async def test_require_caller_owns_pool_missing_pool_raises_404(run_and_pool_ctx):
    from qiita_control_plane.auth.guards import require_caller_owns_pool

    dep = require_caller_owns_pool()
    caller = _human_with_idx(run_and_pool_ctx["creator_idx"])
    with pytest.raises(HTTPException) as exc:
        await dep(
            sequenced_pool_idx=-1,
            p=caller,
            pool=run_and_pool_ctx["pool"],
        )
    assert exc.value.status_code == 404
