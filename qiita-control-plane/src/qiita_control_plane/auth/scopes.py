"""Scope catalog and role-implied ceilings.

`VALID_SCOPES` is the closed set of scope strings the mint path validates
against (Phase C). `ROLE_IMPLIED_SCOPES` is the *hierarchical* per-role
ceiling: each entry is the **full** set, not the increment, with
system_admin ⊇ wet_lab_admin ⊇ user. Service accounts have a separate
non-inherited ceiling because workers don't fit the human hierarchy.
"""

from collections.abc import Mapping

VALID_SCOPES: frozenset[str] = frozenset(
    {
        # Reference data
        "references:read",
        "references:write",
        "references:register_files",
        "features:mint",
        "tickets:doget",
        # Admin operations
        "admin:users",
        "admin:service_accounts",
        "admin:audit_read",
        # Self-service (humans only)
        "self:profile",
        "self:tokens",
    }
)


# Hierarchical scope ceiling for the `qiita.system_role` enum on principal.
# Each entry is the full ceiling (not the increment) — explicit, no hidden
# unions; future readers don't have to chase inheritance to know what role X
# can do.
ROLE_IMPLIED_SCOPES: Mapping[str, frozenset[str]] = {
    "user": frozenset(
        {
            "self:profile",
            "self:tokens",
            "references:read",
        }
    ),
    "wet_lab_admin": frozenset(
        {
            "self:profile",
            "self:tokens",
            "references:read",
            "references:write",
        }
    ),
    "system_admin": frozenset(
        {
            "self:profile",
            "self:tokens",
            "references:read",
            "references:write",
            "admin:users",
            "admin:service_accounts",
            "admin:audit_read",
        }
    ),
}


# Worker / cron scope ceiling. Non-inherited — workers don't fit the human
# hierarchy. Admin-mint of a service-account token must spell out the scopes
# explicitly; requested scopes must fall within this set.
SERVICE_ACCOUNT_SCOPE_CEILING: frozenset[str] = frozenset(
    {
        "features:mint",
        "references:register_files",
        "references:read",
        "tickets:doget",
    }
)


def role_ceiling(system_role: str) -> frozenset[str]:
    """Return the scope ceiling for a human's system_role.

    Raises KeyError on unknown role — callers should already have validated
    the role string against the qiita.system_role enum.
    """
    return ROLE_IMPLIED_SCOPES[system_role]


def reject_scopes_outside_ceiling(requested: list[str], ceiling: frozenset[str]) -> list[str]:
    """Return the subset of requested scopes that fall outside the ceiling.
    Empty list means the request is fully within the ceiling."""
    return sorted(set(requested) - ceiling)
