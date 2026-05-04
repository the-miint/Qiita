"""Unit tests for Principal types (no DB, no resolver)."""

import pytest


def test_principal_base_class_capabilities_default_false():
    from qiita_control_plane.auth.principal import Principal

    p = Principal()
    assert p.has_role("anything") is False
    assert p.has_role_at_least("user") is False
    assert p.has_scope("anything") is False


def test_anonymous_inherits_default_falses():
    from qiita_control_plane.auth.principal import Anonymous, Principal

    a = Anonymous()
    assert isinstance(a, Principal)
    assert a.has_role("user") is False
    assert a.has_role_at_least("user") is False
    assert a.has_scope("reference:read") is False


def test_human_user_isinstance_principal():
    """Slot inheritance: subclass dataclasses are Principal instances."""
    from qiita_control_plane.auth.principal import HumanUser, Principal

    h = HumanUser(
        principal_idx=2,
        email="x@x.com",
        system_role="user",
        scopes=frozenset({"reference:read"}),
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
        system_role="wet_lab_admin",
        scopes=frozenset(),
        profile_complete=True,
        disabled=False,
        retired=False,
    )
    assert h.has_role("wet_lab_admin") is True
    assert h.has_role("system_admin") is False
    assert h.has_role("user") is False  # exact match, not hierarchical


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
    user = HumanUser(system_role="user", **args)
    wla = HumanUser(system_role="wet_lab_admin", **args)
    sa = HumanUser(system_role="system_admin", **args)

    # user
    assert user.has_role_at_least("user")
    assert not user.has_role_at_least("wet_lab_admin")
    assert not user.has_role_at_least("system_admin")
    # wet_lab_admin
    assert wla.has_role_at_least("user")
    assert wla.has_role_at_least("wet_lab_admin")
    assert not wla.has_role_at_least("system_admin")
    # system_admin
    assert sa.has_role_at_least("user")
    assert sa.has_role_at_least("wet_lab_admin")
    assert sa.has_role_at_least("system_admin")


def test_service_account_role_checks_always_false():
    """ServiceAccounts don't fit the human hierarchy."""
    from qiita_control_plane.auth.principal import ServiceAccount

    s = ServiceAccount(
        principal_idx=3,
        name="orchestrator",
        scopes=frozenset({"feature:mint"}),
        disabled=False,
        retired=False,
    )
    assert s.has_role("user") is False
    assert s.has_role_at_least("user") is False
    assert s.has_role_at_least("system_admin") is False
    # But scopes work.
    assert s.has_scope("feature:mint") is True
    assert s.has_scope("admin:user") is False


def test_human_user_has_scope():
    from qiita_control_plane.auth.principal import HumanUser

    h = HumanUser(
        principal_idx=2,
        email="x@x.com",
        system_role="user",
        scopes=frozenset({"self:profile", "reference:read"}),
        profile_complete=True,
        disabled=False,
        retired=False,
    )
    assert h.has_scope("self:profile")
    assert not h.has_scope("admin:user")


def test_principal_subclasses_are_frozen():
    from qiita_control_plane.auth.principal import (
        Anonymous,
        HumanUser,
        ServiceAccount,
    )

    h = HumanUser(
        principal_idx=2,
        email="x@x.com",
        system_role="user",
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
