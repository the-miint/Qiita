"""FastAPI dependency factories that gate routes on Principal capability.

Compose freely:
    @router.post(...)
    def handler(
        user: HumanUser = Depends(require_complete_profile),
        _=Depends(require_role_at_least("system_admin")),
        _=Depends(require_scope("admin:users")),
    ): ...

Each guard depends on `get_current_principal`, so FastAPI dedupes the
underlying resolution per-request even when many guards compose.
"""

from __future__ import annotations

from typing import Callable

from fastapi import Depends, HTTPException

from .principal import (
    Anonymous,
    HumanUser,
    Principal,
    ServiceAccount,
    get_current_principal,
)


# ---------------------------------------------------------------------------
# Kind guards
# ---------------------------------------------------------------------------


def require_human(
    p: Principal = Depends(get_current_principal),
) -> HumanUser:
    """Returns the principal if it's a HumanUser; 401 if Anonymous,
    403 if a ServiceAccount tried to use a humans-only route."""
    if isinstance(p, Anonymous):
        raise HTTPException(status_code=401, detail="authentication required")
    if not isinstance(p, HumanUser):
        raise HTTPException(
            status_code=403,
            detail="this route is restricted to human users",
        )
    return p


def require_service(
    p: Principal = Depends(get_current_principal),
) -> ServiceAccount:
    """Returns the principal if it's a ServiceAccount; 401 if Anonymous,
    403 if a HumanUser tried to use a workers-only route."""
    if isinstance(p, Anonymous):
        raise HTTPException(status_code=401, detail="authentication required")
    if not isinstance(p, ServiceAccount):
        raise HTTPException(
            status_code=403,
            detail="this route is restricted to service accounts",
        )
    return p


# ---------------------------------------------------------------------------
# Role / scope guards (factory style — return a FastAPI dep)
# ---------------------------------------------------------------------------


def require_role_at_least(role: str) -> Callable[..., Principal]:
    """Factory: returns a dep that 401s on Anonymous, 403s on insufficient
    role (including ServiceAccounts, which always fail role checks because
    they don't fit the human hierarchy)."""

    def _dep(p: Principal = Depends(get_current_principal)) -> Principal:
        if isinstance(p, Anonymous):
            raise HTTPException(
                status_code=401, detail="authentication required"
            )
        if not p.has_role_at_least(role):
            raise HTTPException(
                status_code=403,
                detail=f"requires system_role at least {role!r}",
            )
        return p

    return _dep


def require_scope(scope: str) -> Callable[..., Principal]:
    """Factory: returns a dep that 401s on Anonymous, 403s if the principal's
    token scope set does not include `scope`."""

    def _dep(p: Principal = Depends(get_current_principal)) -> Principal:
        if isinstance(p, Anonymous):
            raise HTTPException(
                status_code=401, detail="authentication required"
            )
        if not p.has_scope(scope):
            raise HTTPException(
                status_code=403,
                detail=f"missing required scope {scope!r}",
            )
        return p

    return _dep


# ---------------------------------------------------------------------------
# Profile completeness
# ---------------------------------------------------------------------------


def require_complete_profile(
    user: HumanUser = Depends(require_human),
) -> HumanUser:
    """Chain: must be a HumanUser AND profile_complete is True. Returns 422
    when incomplete with a body listing the missing fields. The route handler
    uses the returned user without re-resolution."""
    if not user.profile_complete:
        raise HTTPException(
            status_code=422,
            detail={
                "detail": "profile incomplete",
                "reason": "profile_incomplete",
                "missing_fields": _missing_profile_fields(user),
            },
        )
    return user


def _missing_profile_fields(user: HumanUser) -> list[str]:
    """Names of fields whose emptiness causes profile_complete=False.

    The DB's GENERATED column logic is `affiliation <> '' AND address <> ''
    AND phone <> ''`. We can't see the raw values from a HumanUser (we only
    carry the boolean), so this returns the canonical list. Phase F's PAT
    route may upgrade this to a DB-driven list if a more informative error
    is needed.
    """
    return ["affiliation", "address", "phone"]
