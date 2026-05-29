"""Scope catalog and role-implied ceilings.

`VALID_SCOPES` is the closed set of scope strings the mint path validates
against. `ROLE_IMPLIED_SCOPES` is the *hierarchical* per-role ceiling:
each entry is the **full** set, not the increment, with
system_admin ⊇ wet_lab_admin ⊇ user. Service accounts have a separate
non-inherited ceiling because workers don't fit the human hierarchy.
"""

from collections.abc import Mapping

from fastapi.responses import JSONResponse
from qiita_common.auth_constants import Scope, SystemRole

VALID_SCOPES: frozenset[Scope] = frozenset(Scope)


# Hierarchical scope ceiling for the `qiita.system_role` enum on principal.
# Each entry is the full ceiling (not the increment) — explicit, no hidden
# unions: `grep WET_LAB_ADMIN` (or any role) returns the complete answer
# inline, so neither human readers nor AI tools have to chase inheritance
# to know what role X can do. `test_role_ceilings_are_hierarchical` locks
# in `USER ⊊ WET_LAB_ADMIN ⊊ SYSTEM_ADMIN` so any drift in either entry
# trips.
ROLE_IMPLIED_SCOPES: Mapping[SystemRole, frozenset[Scope]] = {
    SystemRole.USER: frozenset(
        {
            Scope.SELF_PROFILE,
            Scope.SELF_TOKEN,
            Scope.REFERENCE_READ,
            Scope.BIOSAMPLE_READ,
            Scope.BIOSAMPLE_WRITE,
            Scope.PREP_SAMPLE_READ,
            Scope.PREP_SAMPLE_WRITE,
            Scope.STUDY_READ,
            Scope.STUDY_WRITE,
        }
    ),
    SystemRole.WET_LAB_ADMIN: frozenset(
        {
            Scope.SELF_PROFILE,
            Scope.SELF_TOKEN,
            Scope.REFERENCE_READ,
            Scope.REFERENCE_WRITE,
            Scope.BIOSAMPLE_READ,
            Scope.BIOSAMPLE_WRITE,
            Scope.PREP_SAMPLE_READ,
            Scope.PREP_SAMPLE_WRITE,
            Scope.STUDY_READ,
            Scope.STUDY_WRITE,
            # Upload slots — needed to drive reference data ingest via the
            # qiita-admin CLI, whose reference-add audience includes
            # wet_lab_admin.
            Scope.TICKET_DOPUT,
        }
    ),
    SystemRole.SYSTEM_ADMIN: frozenset(
        {
            Scope.SELF_PROFILE,
            Scope.SELF_TOKEN,
            Scope.REFERENCE_READ,
            Scope.REFERENCE_WRITE,
            Scope.BIOSAMPLE_READ,
            Scope.BIOSAMPLE_WRITE,
            Scope.PREP_SAMPLE_READ,
            Scope.PREP_SAMPLE_WRITE,
            Scope.STUDY_READ,
            Scope.STUDY_WRITE,
            Scope.ADMIN_USER,
            Scope.ADMIN_SERVICE_ACCOUNT,
            Scope.ADMIN_AUDIT_READ,
            Scope.TICKET_DOPUT,
        }
    ),
}


# Worker / cron scope ceiling. Non-inherited — workers don't fit the human
# hierarchy. Admin-mint of a service-account token must spell out the scopes
# explicitly; requested scopes must fall within this set.
SERVICE_ACCOUNT_SCOPE_CEILING: frozenset[Scope] = frozenset(
    {
        Scope.FEATURE_MINT,
        Scope.REFERENCE_REGISTER_FILES,
        Scope.REFERENCE_READ,
        Scope.TICKET_DOGET,
        Scope.TICKET_DOPUT,
        Scope.SEQUENCE_RANGE_MINT,
        Scope.SEQUENCED_POOL_PREFLIGHT_READ,
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


def validate_scopes_against_ceiling(
    requested: list[str],
    ceiling: frozenset[Scope],
    *,
    ceiling_violation_detail: str,
) -> JSONResponse | None:
    """Validate `requested` against `VALID_SCOPES` and the supplied ceiling.

    Returns `None` when the request passes both checks. Otherwise returns the
    422 JSONResponse the route should hand back — body shape is the same on
    both rejection paths (`{"detail": ..., "rejected_scopes": [...]}`); only
    the ceiling-violation detail varies between callers (PAT mint vs.
    service-account create), which is why it's a required keyword argument.
    """
    unknown = [s for s in requested if s not in VALID_SCOPES]
    if unknown:
        return JSONResponse(
            status_code=422,
            content={"detail": "unknown scopes", "rejected_scopes": sorted(unknown)},
        )
    rejected = reject_scopes_outside_ceiling(requested, ceiling)
    if rejected:
        return JSONResponse(
            status_code=422,
            content={"detail": ceiling_violation_detail, "rejected_scopes": rejected},
        )
    return None
