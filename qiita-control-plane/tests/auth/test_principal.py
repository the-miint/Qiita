"""Unit tests for Principal types (no DB, no resolver)."""

import pytest
from qiita_common.auth_constants import Scope, SystemRole


def test_principal_base_class_capabilities_default_false():
    from qiita_control_plane.auth.principal import Principal

    p = Principal()
    assert p.has_role("anything") is False
    assert p.has_role_at_least(SystemRole.USER) is False
    assert p.has_scope("anything") is False


def test_anonymous_inherits_default_falses():
    from qiita_control_plane.auth.principal import Anonymous, Principal

    a = Anonymous()
    assert isinstance(a, Principal)
    assert a.has_role(SystemRole.USER) is False
    assert a.has_role_at_least(SystemRole.USER) is False
    assert a.has_scope(Scope.REFERENCE_READ) is False


def test_human_user_isinstance_principal():
    """Slot inheritance: subclass dataclasses are Principal instances."""
    from qiita_control_plane.auth.principal import HumanUser, Principal

    h = HumanUser(
        principal_idx=2,
        email="x@x.com",
        system_role=SystemRole.USER,
        scopes=frozenset({Scope.REFERENCE_READ}),
        profile_complete=False,
        disabled=False,
        retired=False,
    )
    assert isinstance(h, Principal)


def test_human_user_has_role_exact_match():
    from qiita_control_plane.auth.principal import HumanUser

    h = HumanUser(
        principal_idx=2,
        email="x@x.com",
        system_role=SystemRole.WET_LAB_ADMIN,
        scopes=frozenset(),
        profile_complete=True,
        disabled=False,
        retired=False,
    )
    assert h.has_role(SystemRole.WET_LAB_ADMIN) is True
    assert h.has_role(SystemRole.SYSTEM_ADMIN) is False
    assert h.has_role(SystemRole.USER) is False  # exact match, not hierarchical


def test_human_user_has_role_at_least_hierarchy():
    from qiita_control_plane.auth.principal import HumanUser

    args = dict(
        principal_idx=2,
        email="x@x.com",
        scopes=frozenset(),
        profile_complete=True,
        disabled=False,
        retired=False,
    )
    user = HumanUser(system_role=SystemRole.USER, **args)
    wla = HumanUser(system_role=SystemRole.WET_LAB_ADMIN, **args)
    sa = HumanUser(system_role=SystemRole.SYSTEM_ADMIN, **args)

    # user
    assert user.has_role_at_least(SystemRole.USER)
    assert not user.has_role_at_least(SystemRole.WET_LAB_ADMIN)
    assert not user.has_role_at_least(SystemRole.SYSTEM_ADMIN)
    # wet_lab_admin
    assert wla.has_role_at_least(SystemRole.USER)
    assert wla.has_role_at_least(SystemRole.WET_LAB_ADMIN)
    assert not wla.has_role_at_least(SystemRole.SYSTEM_ADMIN)
    # system_admin
    assert sa.has_role_at_least(SystemRole.USER)
    assert sa.has_role_at_least(SystemRole.WET_LAB_ADMIN)
    assert sa.has_role_at_least(SystemRole.SYSTEM_ADMIN)


def test_service_account_role_checks_always_false():
    """ServiceAccounts don't fit the human hierarchy."""
    from qiita_control_plane.auth.principal import ServiceAccount

    s = ServiceAccount(
        principal_idx=3,
        name="orchestrator",
        scopes=frozenset({Scope.FEATURE_MINT}),
        disabled=False,
        retired=False,
    )
    assert s.has_role(SystemRole.USER) is False
    assert s.has_role_at_least(SystemRole.USER) is False
    assert s.has_role_at_least(SystemRole.SYSTEM_ADMIN) is False
    # But scopes work.
    assert s.has_scope(Scope.FEATURE_MINT) is True
    assert s.has_scope(Scope.ADMIN_USER) is False


def test_human_user_has_scope():
    from qiita_control_plane.auth.principal import HumanUser

    h = HumanUser(
        principal_idx=2,
        email="x@x.com",
        system_role=SystemRole.USER,
        scopes=frozenset({Scope.SELF_PROFILE, Scope.REFERENCE_READ}),
        profile_complete=True,
        disabled=False,
        retired=False,
    )
    assert h.has_scope(Scope.SELF_PROFILE)
    assert not h.has_scope(Scope.ADMIN_USER)


def test_principal_subclasses_are_frozen():
    from qiita_control_plane.auth.principal import (
        Anonymous,
        HumanUser,
        ServiceAccount,
    )

    h = HumanUser(
        principal_idx=2,
        email="x@x.com",
        system_role=SystemRole.USER,
        scopes=frozenset(),
        profile_complete=False,
        disabled=False,
        retired=False,
    )
    s = ServiceAccount(
        principal_idx=3,
        name="x",
        scopes=frozenset(),
        disabled=False,
        retired=False,
    )
    a = Anonymous()

    for instance in (h, s, a):
        with pytest.raises(Exception):
            instance.principal_idx = 999  # type: ignore[attr-defined]
