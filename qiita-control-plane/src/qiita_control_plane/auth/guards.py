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
from ..repositories.sequencing_run import fetch_sequenced_pool, fetch_sequencing_run_exists
from ..repositories.study import fetch_study_exists
from ..repositories.study_access import fetch_caller_study_access
from ..repositories.user_eligibility import fetch_user_eligibility
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


def require_service_with_scope(scope: str) -> Callable[..., ServiceAccount]:
    """Factory: bundle `require_service` + a scope check into one dep,
    with the ordering wired as a *data-flow* edge in the dep graph.

    The inner `_dep` takes `Depends(require_service)` as input, so the
    kind guard's 403 ("service accounts only") fires before the scope
    check would ever run. This removes the implicit-ordering question
    a route would otherwise have when listing both guards as siblings:

        sa: ServiceAccount = Depends(require_service)
        _scope: Principal = Depends(require_scope(...))

    FastAPI does evaluate sibling deps in declaration order in practice,
    but the structural form makes the kind-first ordering a property of
    the dep graph rather than a property of FastAPI's evaluation policy.
    """
    scope_str = str(scope)

    def _dep(sa: ServiceAccount = Depends(require_service)) -> ServiceAccount:
        if not sa.has_scope(scope_str):
            raise HTTPException(
                status_code=403,
                detail=f"missing required scope {scope_str!r}",
            )
        return sa

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
# relying on enum-string ordering. Lives next to its sole consumer
# (require_study_access). Unlike Roles, Tiers are not a property of
# principals.
_TIER_ORDER = {
    Tier.PUBLIC: 0,
    Tier.VIEWER: 1,
    Tier.MEMBER: 2,
    Tier.ADMIN: 3,
}


async def require_eligible_owner(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    candidate_idx: int,
    detail: str,
) -> None:
    """Body-time helper (not a Depends() dep): enforce that a body-supplied
    owner_idx names a profile-complete, non-disabled, non-retired user.

    Accepts either a pool or a connection so the helper composes inside an
    open transaction or stands alone (mirrors fetch_user_eligibility, which
    is what does the work). The full eligibility lookup runs on every call
    — there is no caller-state short-circuit. Routes whose caller is
    role-gated only (no require_complete_profile) would otherwise be able
    to set owner_idx to a profile-incomplete caller's own principal_idx
    and pass; running the lookup unconditionally closes that path without
    requiring each call site to assert its own caller has been validated.

    All ineligibility cases (no principal, non-user-kind, disabled,
    retired, profile incomplete) collapse to one 422 with the
    caller-supplied detail to avoid leaking principal-state to callers
    probing arbitrary owner_idx values.
    """
    # One round trip; the policy combination is checked here.
    eligibility = await fetch_user_eligibility(pool_or_conn, principal_idx=candidate_idx)
    if eligibility is not None and (
        eligibility.is_user
        and not eligibility.disabled
        and not eligibility.retired
        and eligibility.profile_complete
    ):
        return
    raise HTTPException(status_code=422, detail=detail)


async def require_study_exists(
    study_idx: int,
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> None:
    """Existence-only guard: 404 if no qiita.study row matches the path's
    study_idx. No access-tier check, no role gate. Use on routes that gate
    write access via require_role_at_least (e.g., the biosample import
    route, where wet_lab_admin replaces the tier comparison) but still
    need to surface a 404 on a nonexistent study rather than letting an
    FK violation surface as a 422 from the composer.
    """
    if not await fetch_study_exists(pool, study_idx):
        raise HTTPException(
            status_code=404,
            detail=f"study {study_idx} not found",
        )


async def require_sequencing_run_exists(
    sequencing_run_idx: int,
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> None:
    """Existence-only guard: 404 if no qiita.sequencing_run row matches
    the path's sequencing_run_idx. Direct mirror of require_study_exists
    for routes that mount on a path containing {sequencing_run_idx} and
    need a clean 404 before opening their write transaction.
    """
    if not await fetch_sequencing_run_exists(pool, sequencing_run_idx):
        raise HTTPException(
            status_code=404,
            detail=f"sequencing_run {sequencing_run_idx} not found",
        )


async def require_sequenced_pool_in_run(
    sequencing_run_idx: int,
    sequenced_pool_idx: int,
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> None:
    """Existence + parent-run consistency guard: 404 if no sequenced_pool
    row matches the path's sequenced_pool_idx; 422 if the pool exists but
    its sequencing_run_idx does not match the path's sequencing_run_idx.

    Folded into one DB round-trip: the pool row's stored sequencing_run_idx
    is checked directly, so a parent-run-exists pre-check is unnecessary
    (the FK invariant guarantees the run exists if the pool's
    sequencing_run_idx matches the path).
    """
    pool_row = await fetch_sequenced_pool(pool, sequenced_pool_idx)
    if pool_row is None:
        raise HTTPException(
            status_code=404,
            detail=f"sequenced_pool {sequenced_pool_idx} not found",
        )
    if pool_row["sequencing_run_idx"] != sequencing_run_idx:
        raise HTTPException(
            status_code=422,
            detail=(
                f"sequenced_pool {sequenced_pool_idx} does not belong to"
                f" sequencing_run {sequencing_run_idx}"
            ),
        )


def require_study_access(
    min_tier: Tier | None = None,
    *,
    bypass_role: SystemRole = SystemRole.SYSTEM_ADMIN,
) -> Callable[..., None]:
    """Factory: returns a dep that gates the route on the caller's tier
    of access to the path's `study_idx`.

    Behavior, in order: 401 on Anonymous; role-bypass for callers at or
    above `bypass_role` (returns without any DB lookup — the bypass
    path runs neither the existence check nor the tier comparison);
    otherwise 404 if the study does not exist, owner bypass (caller is
    the study's owner), or 403 if the caller's effective tier is below
    the resolved minimum tier. Routes that must surface 404 on a
    missing study for bypass-role callers should compose
    `require_study_exists` alongside this guard (see
    `list_biosample_idxs_in_study` and `get_study_route`).

    `min_tier=None` resolves the minimum to the study's own
    `default_tier` at request time (per-study policy). Pass an
    explicit `Tier` member to lock the minimum at factory call time
    (per-route policy).

    `bypass_role` defaults to `SYSTEM_ADMIN` (existing behavior). Pass
    `WET_LAB_ADMIN` for routes whose policy admits any wet_lab_admin
    or higher regardless of tier.

    A caller with no qiita.study_access row has effective tier
    `Tier.PUBLIC` by absence — meets a resolved minimum of
    `Tier.PUBLIC`, fails everything higher.
    """

    async def _dep(
        study_idx: int,
        p: Principal = Depends(get_current_principal),
        pool: asyncpg.Pool = Depends(get_db_pool),
    ) -> None:
        # 401 on Anonymous.
        if isinstance(p, Anonymous):
            raise HTTPException(status_code=401, detail=_MSG_AUTH_REQUIRED)

        # Role bypass — no DB lookup needed.
        if p.has_role_at_least(bypass_role):
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

        # Resolve the minimum tier per call: explicit factory arg wins,
        # otherwise fall back to the study's own default_tier.
        resolved_min_tier = min_tier if min_tier is not None else row.default_tier

        # Tier comparison — public-by-absence when no study_access row.
        effective_tier = row.access_tier if row.access_tier is not None else Tier.PUBLIC
        if _TIER_ORDER[effective_tier] >= _TIER_ORDER[resolved_min_tier]:
            return
        raise HTTPException(
            status_code=403,
            detail=f"requires study access at tier {str(resolved_min_tier)!r} or higher",
        )

    return _dep
