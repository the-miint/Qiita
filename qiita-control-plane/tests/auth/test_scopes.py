"""Unit tests for the scope catalog and role ceilings."""

import pytest
from qiita_common.auth_constants import Scope, SystemRole


def test_role_implied_scopes_keys_match_system_role_enum():
    """ROLE_IMPLIED_SCOPES must cover exactly the qiita.system_role enum
    values. Drift would mean either an unhandled role at the API layer or
    a dead key in the map.
    """
    from qiita_control_plane.auth.scopes import ROLE_IMPLIED_SCOPES

    assert set(ROLE_IMPLIED_SCOPES.keys()) == {
        SystemRole.USER,
        SystemRole.WET_LAB_ADMIN,
        SystemRole.SYSTEM_ADMIN,
    }


def test_role_ceilings_are_hierarchical():
    """system_admin ⊇ wet_lab_admin ⊇ user (strictly inclusive)."""
    from qiita_control_plane.auth.scopes import ROLE_IMPLIED_SCOPES

    user = ROLE_IMPLIED_SCOPES[SystemRole.USER]
    wla = ROLE_IMPLIED_SCOPES[SystemRole.WET_LAB_ADMIN]
    sa = ROLE_IMPLIED_SCOPES[SystemRole.SYSTEM_ADMIN]

    assert user.issubset(wla)
    assert wla.issubset(sa)
    assert user.issubset(sa)
    # Strict superset: each higher tier adds at least one new scope.
    assert wla > user
    assert sa > wla


def test_all_role_scopes_are_in_valid_scopes():
    from qiita_control_plane.auth.scopes import (
        ROLE_IMPLIED_SCOPES,
        VALID_SCOPES,
    )

    for role, scopes in ROLE_IMPLIED_SCOPES.items():
        unknown = scopes - VALID_SCOPES
        assert not unknown, f"{role} ceiling has scopes not in VALID_SCOPES: {unknown}"


def test_service_account_ceiling_is_in_valid_scopes():
    from qiita_control_plane.auth.scopes import (
        SERVICE_ACCOUNT_SCOPE_CEILING,
        VALID_SCOPES,
    )

    assert SERVICE_ACCOUNT_SCOPE_CEILING.issubset(VALID_SCOPES)


def test_service_account_ceiling_does_not_include_admin_or_self_scopes():
    """Workers don't get admin or self-service scopes."""
    from qiita_control_plane.auth.scopes import SERVICE_ACCOUNT_SCOPE_CEILING

    forbidden = {
        Scope.ADMIN_USER,
        Scope.ADMIN_SERVICE_ACCOUNT,
        Scope.ADMIN_AUDIT_READ,
        Scope.SELF_PROFILE,
        Scope.SELF_TOKEN,
    }
    assert SERVICE_ACCOUNT_SCOPE_CEILING.isdisjoint(forbidden)


def test_role_ceiling_helper_returns_correct_set():
    from qiita_control_plane.auth.scopes import (
        ROLE_IMPLIED_SCOPES,
        role_ceiling,
    )

    for role, scopes in ROLE_IMPLIED_SCOPES.items():
        assert role_ceiling(role) == scopes


def test_role_ceiling_helper_raises_on_unknown_role():
    from qiita_control_plane.auth.scopes import role_ceiling

    # ValueError comes from the SystemRole StrEnum constructor when the
    # input doesn't match any enum value.
    with pytest.raises(ValueError, match="super-duper-admin"):
        role_ceiling("super-duper-admin")


def test_compute_worker_fixture_scopes_subset_of_ceiling():
    """The `compute_worker_service_account` test fixture must grant only
    scopes that production deploys can mint via the admin path —
    SERVICE_ACCOUNT_SCOPE_CEILING gates the admin route, so any
    fixture-only scope creates an SA shape production can't replicate.
    Catches drift in either direction: fixture gains an over-ceiling
    scope, or the ceiling shrinks below what the fixture needs."""
    from qiita_control_plane.auth.scopes import SERVICE_ACCOUNT_SCOPE_CEILING
    from qiita_control_plane.testing.sessions import COMPUTE_WORKER_FIXTURE_SCOPES

    over_ceiling = COMPUTE_WORKER_FIXTURE_SCOPES - SERVICE_ACCOUNT_SCOPE_CEILING
    assert not over_ceiling, (
        f"compute_worker_service_account fixture grants scopes outside the "
        f"SA ceiling — production admin path will reject these: "
        f"{sorted(s.value for s in over_ceiling)}"
    )


def test_sequence_range_mint_is_workers_only():
    """sequence-range:mint is allocated only to compute service accounts
    — humans never mint sequence ranges, so the scope must be on the
    service-account ceiling and absent from every role ceiling."""
    from qiita_control_plane.auth.scopes import (
        ROLE_IMPLIED_SCOPES,
        SERVICE_ACCOUNT_SCOPE_CEILING,
    )

    assert Scope.SEQUENCE_RANGE_MINT in SERVICE_ACCOUNT_SCOPE_CEILING
    for role, ceiling in ROLE_IMPLIED_SCOPES.items():
        assert Scope.SEQUENCE_RANGE_MINT not in ceiling, (
            f"sequence-range:mint must not be on role {role!r}'s ceiling — compute workers only"
        )


def test_reject_scopes_outside_ceiling():
    from qiita_control_plane.auth.scopes import (
        ROLE_IMPLIED_SCOPES,
        reject_scopes_outside_ceiling,
    )

    user_ceiling = ROLE_IMPLIED_SCOPES[SystemRole.USER]
    # Subset → no rejections.
    assert reject_scopes_outside_ceiling([Scope.SELF_PROFILE], user_ceiling) == []
    # Superset → rejections include the over-scope.
    rejected = reject_scopes_outside_ceiling(
        [Scope.SELF_PROFILE, Scope.ADMIN_USER, Scope.REFERENCE_WRITE], user_ceiling
    )
    assert set(rejected) == {Scope.ADMIN_USER, Scope.REFERENCE_WRITE}
    # Sorted, so the API can echo them deterministically.
    assert rejected == sorted(rejected)
