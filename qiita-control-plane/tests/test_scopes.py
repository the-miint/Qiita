"""Unit tests for the scope catalog and role ceilings."""

import pytest


def test_role_implied_scopes_keys_match_system_role_enum():
    """ROLE_IMPLIED_SCOPES must cover exactly the qiita.system_role enum
    values. Drift would mean either an unhandled role at the API layer or
    a dead key in the map.
    """
    from qiita_control_plane.auth.scopes import ROLE_IMPLIED_SCOPES

    assert set(ROLE_IMPLIED_SCOPES.keys()) == {
        "user",
        "wet_lab_admin",
        "system_admin",
    }


def test_role_ceilings_are_hierarchical():
    """system_admin ⊇ wet_lab_admin ⊇ user (strictly inclusive)."""
    from qiita_control_plane.auth.scopes import ROLE_IMPLIED_SCOPES

    user = ROLE_IMPLIED_SCOPES["user"]
    wla = ROLE_IMPLIED_SCOPES["wet_lab_admin"]
    sa = ROLE_IMPLIED_SCOPES["system_admin"]

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
        "admin:users",
        "admin:service_accounts",
        "admin:audit_read",
        "self:profile",
        "self:tokens",
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

    with pytest.raises(KeyError):
        role_ceiling("super-duper-admin")


def test_reject_scopes_outside_ceiling():
    from qiita_control_plane.auth.scopes import (
        ROLE_IMPLIED_SCOPES,
        reject_scopes_outside_ceiling,
    )

    user_ceiling = ROLE_IMPLIED_SCOPES["user"]
    # Subset → no rejections.
    assert reject_scopes_outside_ceiling(["self:profile"], user_ceiling) == []
    # Superset → rejections include the over-scope.
    rejected = reject_scopes_outside_ceiling(
        ["self:profile", "admin:users", "references:write"], user_ceiling
    )
    assert set(rejected) == {"admin:users", "references:write"}
    # Sorted, so the API can echo them deterministically.
    assert rejected == sorted(rejected)
