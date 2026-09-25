"""Who may list, grant, change, and revoke rows in qiita.study_access.

The rules are tables, not a tier comparison: each caller standing maps to the
set of tiers it may grant and the set of row tiers it may revoke. Changing a
row's tier is allowed iff the caller may revoke the row's current tier and
grant the new one.

| Caller on the study                  | List | Grant                    | Revoke rows      |
|--------------------------------------|------|--------------------------|------------------|
| `wet_lab_admin` / `system_admin`     | yes  | admin, member, viewer    | any              |
| study `admin` (incl. the owner)      | yes  | admin, member, viewer    | member, viewer   |
| study `member`                       | yes  | member, viewer           | member, viewer   |
| study `viewer`                       | no   | none                     | none             |
| no row (`public`)                    | no   | none                     | none             |

Only `wet_lab_admin`+ revokes or demotes an `admin` row, which covers the
owner's auto-granted row; removing it does not remove the owner's access,
because `require_study_access` lets the owner through without one.
"""

from enum import StrEnum

from qiita_common.auth_constants import SystemRole
from qiita_common.models import STORABLE_ACCESS_TIERS, Tier

from ..repositories.study_access import CallerStudyAccessRow
from .principal import Principal

# The role at or above which a caller manages any study's access rows.
STUDY_ACCESS_BYPASS_ROLE = SystemRole.WET_LAB_ADMIN


class Standing(StrEnum):
    """A caller's position on one study for the purpose of managing access."""

    STAFF = "staff"  # STUDY_ACCESS_BYPASS_ROLE or above
    ADMIN = "admin"  # admin row, or the study owner
    MEMBER = "member"
    VIEWER = "viewer"
    NONE = "none"  # no row


_ALL_GRANTABLE = STORABLE_ACCESS_TIERS
_MEMBER_AND_VIEWER = frozenset({Tier.MEMBER, Tier.VIEWER})

_CAN_LIST = frozenset({Standing.STAFF, Standing.ADMIN, Standing.MEMBER})
_GRANTABLE: dict[Standing, frozenset[Tier]] = {
    Standing.STAFF: _ALL_GRANTABLE,
    Standing.ADMIN: _ALL_GRANTABLE,
    Standing.MEMBER: _MEMBER_AND_VIEWER,
}
_REVOCABLE: dict[Standing, frozenset[Tier]] = {
    Standing.STAFF: _ALL_GRANTABLE,
    Standing.ADMIN: _MEMBER_AND_VIEWER,
    Standing.MEMBER: _MEMBER_AND_VIEWER,
}

_ROW_TIER_STANDING = {
    Tier.ADMIN: Standing.ADMIN,
    Tier.MEMBER: Standing.MEMBER,
    Tier.VIEWER: Standing.VIEWER,
}


def standing_of(caller: Principal, row: CallerStudyAccessRow) -> Standing:
    """The caller's standing on the study `row` describes."""
    if caller.has_role_at_least(STUDY_ACCESS_BYPASS_ROLE):
        return Standing.STAFF
    if row.owner_idx == caller.principal_idx:
        return Standing.ADMIN
    if row.access_tier is None:
        return Standing.NONE
    return _ROW_TIER_STANDING[row.access_tier]


def can_list(standing: Standing) -> bool:
    return standing in _CAN_LIST


def can_grant(standing: Standing, tier: Tier) -> bool:
    return tier in _GRANTABLE.get(standing, frozenset())


def can_revoke(standing: Standing, row_tier: Tier) -> bool:
    return row_tier in _REVOCABLE.get(standing, frozenset())


def can_change_tier(standing: Standing, *, current: Tier, new: Tier) -> bool:
    return can_revoke(standing, current) and can_grant(standing, new)
