"""The study-access management policy, checked against its full table.

Each expectation below is written out rather than derived, so the test states
the rules independently of `study_access_policy`'s own tables.
"""

import pytest
from qiita_common.auth_constants import SystemRole
from qiita_common.models import Tier

from qiita_control_plane.auth.principal import HumanUser
from qiita_control_plane.auth.study_access_policy import (
    Standing,
    can_change_tier,
    can_grant,
    can_list,
    can_revoke,
    standing_of,
)
from qiita_control_plane.repositories.study_access import CallerStudyAccessRow

_CALLER = 10
_OTHER = 20
_GRANTABLE = (Tier.ADMIN, Tier.MEMBER, Tier.VIEWER)

_LISTS = {Standing.STAFF, Standing.ADMIN, Standing.MEMBER}
_GRANTS = {
    Standing.STAFF: {Tier.ADMIN, Tier.MEMBER, Tier.VIEWER},
    Standing.ADMIN: {Tier.ADMIN, Tier.MEMBER, Tier.VIEWER},
    Standing.MEMBER: {Tier.MEMBER, Tier.VIEWER},
    Standing.VIEWER: set(),
    Standing.NONE: set(),
}
_REVOKES = {
    Standing.STAFF: {Tier.ADMIN, Tier.MEMBER, Tier.VIEWER},
    Standing.ADMIN: {Tier.MEMBER, Tier.VIEWER},
    Standing.MEMBER: {Tier.MEMBER, Tier.VIEWER},
    Standing.VIEWER: set(),
    Standing.NONE: set(),
}


def _caller(role: SystemRole = SystemRole.USER) -> HumanUser:
    return HumanUser(
        principal_idx=_CALLER,
        email="caller@test.local",
        system_role=role,
        scopes=frozenset(),
        profile_complete=True,
        disabled=False,
        retired=False,
    )


def _row(*, owner_idx: int = _OTHER, tier: Tier | None) -> CallerStudyAccessRow:
    return CallerStudyAccessRow(owner_idx=owner_idx, access_tier=tier, default_tier=Tier.MEMBER)


@pytest.mark.parametrize(
    ("role", "row", "expected"),
    [
        (SystemRole.WET_LAB_ADMIN, _row(tier=None), Standing.STAFF),
        (SystemRole.SYSTEM_ADMIN, _row(tier=Tier.VIEWER), Standing.STAFF),
        (SystemRole.USER, _row(owner_idx=_CALLER, tier=None), Standing.ADMIN),
        (SystemRole.USER, _row(owner_idx=_CALLER, tier=Tier.VIEWER), Standing.ADMIN),
        (SystemRole.USER, _row(tier=Tier.ADMIN), Standing.ADMIN),
        (SystemRole.USER, _row(tier=Tier.MEMBER), Standing.MEMBER),
        (SystemRole.USER, _row(tier=Tier.VIEWER), Standing.VIEWER),
        (SystemRole.USER, _row(tier=None), Standing.NONE),
    ],
)
def test_standing_of(role, row, expected):
    assert standing_of(_caller(role), row) == expected


@pytest.mark.parametrize("standing", list(Standing))
def test_can_list(standing):
    assert can_list(standing) is (standing in _LISTS)


@pytest.mark.parametrize("standing", list(Standing))
@pytest.mark.parametrize("tier", [*_GRANTABLE, Tier.PUBLIC])
def test_can_grant(standing, tier):
    assert can_grant(standing, tier) is (tier in _GRANTS[standing])


@pytest.mark.parametrize("standing", list(Standing))
@pytest.mark.parametrize("tier", _GRANTABLE)
def test_can_revoke(standing, tier):
    assert can_revoke(standing, tier) is (tier in _REVOKES[standing])


@pytest.mark.parametrize("standing", list(Standing))
@pytest.mark.parametrize("current", _GRANTABLE)
@pytest.mark.parametrize("new", _GRANTABLE)
def test_can_change_tier(standing, current, new):
    expected = current in _REVOKES[standing] and new in _GRANTS[standing]
    assert can_change_tier(standing, current=current, new=new) is expected


def test_member_moves_rows_only_between_member_and_viewer():
    assert can_change_tier(Standing.MEMBER, current=Tier.VIEWER, new=Tier.MEMBER)
    assert not can_change_tier(Standing.MEMBER, current=Tier.MEMBER, new=Tier.ADMIN)


def test_only_staff_demotes_an_admin_row():
    assert can_change_tier(Standing.STAFF, current=Tier.ADMIN, new=Tier.VIEWER)
    assert not can_change_tier(Standing.ADMIN, current=Tier.ADMIN, new=Tier.VIEWER)
