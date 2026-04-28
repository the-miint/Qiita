"""Scope catalog and role-implied ceilings.

`VALID_SCOPES` is the closed set of scope strings the mint path validates
against (Phase C). `ROLE_IMPLIED_SCOPES` is the *hierarchical* per-role
ceiling: each entry is the **full** set, not the increment, with
system_admin ⊇ wet_lab_admin ⊇ user. Service accounts have a separate
non-inherited ceiling because workers don't fit the human hierarchy.
"""

from collections.abc import Mapping

from qiita_common.auth_constants import Scope, SystemRole

VALID_SCOPES: frozenset[Scope] = frozenset(Scope)


# Hierarchical scope ceiling for the `qiita.system_role` enum on principal.
# Each entry is the full ceiling (not the increment) — explicit, no hidden
# unions; future readers don't have to chase inheritance to know what role X
# can do.
ROLE_IMPLIED_SCOPES: Mapping[SystemRole, frozenset[Scope]] = {
    SystemRole.USER: frozenset(
        {
            Scope.SELF_PROFILE,
            Scope.SELF_TOKENS,
            Scope.REFERENCES_READ,
        }
    ),
    SystemRole.WET_LAB_ADMIN: frozenset(
        {
            Scope.SELF_PROFILE,
            Scope.SELF_TOKENS,
            Scope.REFERENCES_READ,
            Scope.REFERENCES_WRITE,
        }
    ),
    SystemRole.SYSTEM_ADMIN: frozenset(
        {
            Scope.SELF_PROFILE,
            Scope.SELF_TOKENS,
            Scope.REFERENCES_READ,
            Scope.REFERENCES_WRITE,
            Scope.ADMIN_USERS,
            Scope.ADMIN_SERVICE_ACCOUNTS,
            Scope.ADMIN_AUDIT_READ,
        }
    ),
}


# Worker / cron scope ceiling. Non-inherited — workers don't fit the human
# hierarchy. Admin-mint of a service-account token must spell out the scopes
# explicitly; requested scopes must fall within this set.
SERVICE_ACCOUNT_SCOPE_CEILING: frozenset[Scope] = frozenset(
    {
        Scope.FEATURES_MINT,
        Scope.REFERENCES_REGISTER_FILES,
        Scope.REFERENCES_READ,
        Scope.TICKETS_DOGET,
    }
)


def role_ceiling(system_role: str) -> frozenset[Scope]:
    """Return the scope ceiling for a human's system_role.

    Raises ValueError on unknown role (via the SystemRole StrEnum constructor) —
    callers should already have validated the role string against the
    qiita.system_role enum.
    """
    return ROLE_IMPLIED_SCOPES[SystemRole(system_role)]


def reject_scopes_outside_ceiling(requested: list[str], ceiling: frozenset[Scope]) -> list[str]:
    """Return the subset of requested scopes that fall outside the ceiling.
    Empty list means the request is fully within the ceiling."""
    return sorted(set(requested) - ceiling)
