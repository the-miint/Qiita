"""Scope catalog.

Phase C ships only `VALID_SCOPES` — the closed set of scope strings the
mint path validates against. The hierarchical `ROLE_IMPLIED_SCOPES` map
(role-based ceilings for human PATs) and `SERVICE_ACCOUNT_SCOPE_CEILING`
(non-inherited worker ceiling) are added in Phase E when scope-based
authorization is wired into route guards.
"""

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
