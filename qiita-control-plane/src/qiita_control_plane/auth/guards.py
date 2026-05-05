"""FastAPI dependency factories that gate routes on Principal capability.

Compose freely:
    @router.post(...)
    def handler(
        user: HumanUser = Depends(require_complete_profile),
        _=Depends(require_role_at_least("system_admin")),
        _=Depends(require_scope("admin:user")),
    ): ...

Each guard depends on `get_current_principal`, so FastAPI dedupes the
underlying resolution per-request even when many guards compose.
"""

from collections.abc import Callable

import asyncpg
from fastapi import Depends, HTTPException
from qiita_common.auth_constants import SystemRole
from qiita_common.models import Tier

from ..deps import get_db_pool
from ..repositories.study_access import fetch_caller_study_access
from .principal import (
    Anonymous,
    HumanUser,
    Principal,
    ServiceAccount,
    get_current_principal,
)

_MSG_AUTH_REQUIRED = "authentication required"


def _msg_requires_role(role: str) -> str:
    return f"requires system_role at least {role!r}"


def _kind_guard_failure(p: Principal, allowed_kind_label: str) -> HTTPException:
    """Build the HTTPException for a rejected kind guard: 401 if anonymous,
    403 with a kind-specific detail otherwise."""
    if isinstance(p, Anonymous):
        return HTTPException(status_code=401, detail=_MSG_AUTH_REQUIRED)
    return HTTPException(
        status_code=403,
        detail=f"this route is restricted to {allowed_kind_label}",
    )


# ---------------------------------------------------------------------------
# Kind guards
# ---------------------------------------------------------------------------
# Positive-assertion style: each guard's only return-branch is the explicit
# isinstance check; every other case funnels through `_kind_guard_failure`.
# Deny-by-default is structural — a future Principal subclass is denied
# automatically without any change to these guards.


def require_human(
    p: Principal = Depends(get_current_principal),
) -> HumanUser:
    """Returns the principal if it's a HumanUser; 401 if Anonymous,
    403 otherwise (e.g., a ServiceAccount tried to use a humans-only route)."""
    if isinstance(p, HumanUser):
        return p
    raise _kind_guard_failure(p, "human users")


def require_service(
    p: Principal = Depends(get_current_principal),
) -> ServiceAccount:
    """Returns the principal if it's a ServiceAccount; 401 if Anonymous,
    403 otherwise (e.g., a HumanUser tried to use a workers-only route)."""
    if isinstance(p, ServiceAccount):
        return p
    raise _kind_guard_failure(p, "service accounts")


# ---------------------------------------------------------------------------
# Role / scope guards (factory style — return a FastAPI dep)
# ---------------------------------------------------------------------------
# Negative-logic style here on purpose: the positive case is a capability
# check (not an isinstance), so the kind-guard inversion would be less
# readable for the same defense. The `Principal` base class returns False
# from both `has_role_at_least` and `has_scope`, which gives the same
# deny-by-default property without inverting.


def require_role_at_least(role: str) -> Callable[..., Principal]:
    """Factory: returns a dep that 401s on Anonymous, 403s on insufficient
    role (including ServiceAccounts, which always fail role checks because
    they don't fit the human hierarchy).

    `role` accepts either a SystemRole member or its bare string value.
    Normalised to `str(role)` at factory time so the 403 detail renders
    `'system_admin'` not `<SystemRole.SYSTEM_ADMIN: 'system_admin'>`.
    """
    role_str = str(role)

    def _dep(p: Principal = Depends(get_current_principal)) -> Principal:
        if isinstance(p, Anonymous):
            raise HTTPException(status_code=401, detail=_MSG_AUTH_REQUIRED)
        if not p.has_role_at_least(role_str):
            raise HTTPException(
                status_code=403,
                detail=_msg_requires_role(role_str),
            )
        return p

    return _dep


def require_human_with_role(role: str) -> Callable[..., HumanUser]:
    """Factory: composes `require_human` with a hierarchical role check.

    Returns the resolved `HumanUser` so callers can use `.principal_idx`,
    `.email`, etc. without runtime narrowing — `require_role_at_least`
    alone returns `Principal` because it accepts service accounts at the
    type level (they always 403 at runtime). For routes that need both
    role authority AND human context (most admin endpoints + admin-side
    POST /user), this is the cleaner combinator.
    """
    role_str = str(role)

    def _dep(user: HumanUser = Depends(require_human)) -> HumanUser:
        if not user.has_role_at_least(role_str):
            raise HTTPException(
                status_code=403,
                detail=_msg_requires_role(role_str),
            )
        return user

    return _dep


def require_scope(scope: str) -> Callable[..., Principal]:
    """Factory: returns a dep that 401s on Anonymous, 403s if the principal's
    token scope set does not include `scope`.

    Accepts a Scope member or bare string; normalised so the 403 detail
    renders `'self:token'` not `<Scope.SELF_TOKEN: 'self:token'>`.
    """
    scope_str = str(scope)

    def _dep(p: Principal = Depends(get_current_principal)) -> Principal:
        if isinstance(p, Anonymous):
            raise HTTPException(status_code=401, detail=_MSG_AUTH_REQUIRED)
        if not p.has_scope(scope_str):
            raise HTTPException(
                status_code=403,
                detail=f"missing required scope {scope_str!r}",
            )
        return p

    return _dep


# ---------------------------------------------------------------------------
# Profile completeness
# ---------------------------------------------------------------------------


def require_complete_profile(
    user: HumanUser = Depends(require_human),
) -> HumanUser:
    """Chain: must be a HumanUser AND profile_complete is True.

    Returns 422 with a flat-detail body. The DB-driven missing-fields list
    (which fields are actually empty for this user) is not computed here —
    `HumanUser` only carries the `profile_complete` boolean, not the raw
    fields. Routes that want to surface the per-field missing list to the
    user (e.g. `POST /auth/pat`) skip this guard and check `profile_complete`
    inline so they can issue a single SQL query that pulls both the boolean
    and the field values.
    """
    if not user.profile_complete:
        raise HTTPException(
            status_code=422,
            detail={
                "detail": "profile incomplete",
                "reason": "profile_incomplete",
            },
        )
    return user


# ---------------------------------------------------------------------------
# Resource-access guards
# ---------------------------------------------------------------------------
# Unlike the kind/role/scope guards above, resource-access guards consult
# the DB to evaluate the caller's relationship to a specific resource
# named in the path. system_admin bypasses the resource check entirely;
# the resource owner bypasses the tier comparison; otherwise the caller's
# tier (or 'public' by absence of a row) is compared against min_tier.


# Strict ordering by privilege; mirrors the qiita.tier enum order. The
# integer ranks let the guard compare tiers with a plain >= rather than
# relying on enum-string ordering.
_TIER_ORDER = {
    Tier.PUBLIC: 0,
    Tier.VIEWER: 1,
    Tier.MEMBER: 2,
    Tier.ADMIN: 3,
}


def require_study_access(min_tier: Tier) -> Callable[..., None]:
    """Factory: returns a dep that gates the route on the caller's tier
    of access to the path's `study_idx`.

    Behavior, in order: 401 on Anonymous; system_admin bypass (skips the
    DB lookup); 404 if the study does not exist; owner bypass (caller is
    the study's owner); 403 if the caller's effective tier is below
    `min_tier`. A caller with no qiita.study_access row has effective
    tier `Tier.PUBLIC` by absence — meets `min_tier=Tier.PUBLIC`, fails
    everything higher.
    """

    async def _dep(
        study_idx: int,
        p: Principal = Depends(get_current_principal),
        pool: asyncpg.Pool = Depends(get_db_pool),
    ) -> None:
        # 401 on Anonymous.
        if isinstance(p, Anonymous):
            raise HTTPException(status_code=401, detail=_MSG_AUTH_REQUIRED)

        # System-admin bypass — no DB lookup needed.
        if p.has_role_at_least(SystemRole.SYSTEM_ADMIN):
            return

        # Fetch the study + caller's access row in one round trip.
        row = await fetch_caller_study_access(
            pool, principal_idx=p.principal_idx, study_idx=study_idx
        )
        if row is None:
            raise HTTPException(
                status_code=404,
                detail=f"study {study_idx} not found",
            )

        # Owner bypass — study owner authorized at every tier.
        if row.owner_idx == p.principal_idx:
            return

        # Tier comparison — public-by-absence when no study_access row.
        effective_tier = row.access_tier if row.access_tier is not None else Tier.PUBLIC
        if _TIER_ORDER[effective_tier] < _TIER_ORDER[min_tier]:
            raise HTTPException(
                status_code=403,
                detail=f"requires study access at tier {str(min_tier)!r} or higher",
            )

    return _dep
