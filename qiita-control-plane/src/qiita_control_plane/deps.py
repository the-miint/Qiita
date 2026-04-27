"""Shared FastAPI dependencies."""

from uuid import UUID

import asyncpg
from fastapi import HTTPException, Request


def get_db_pool(request: Request) -> asyncpg.Pool:
    """Typed accessor for the database pool — use as a FastAPI dependency."""
    pool = getattr(request.app.state, "pool", None)
    if pool is None:
        raise RuntimeError("Database pool not initialised — lifespan may not have run")
    return pool


# DEPRECATED: removed in Phase H.c.
# Both `get_current_user` and `get_current_principal_idx` are mock auth
# helpers from Phases B and earlier. Phase H.b flipped every route to the
# real `get_current_principal` resolver from `auth.principal`; nothing
# imports these any more. They survive this single commit so reverting
# H.b mid-rollout doesn't strand the legacy callers.


def get_current_user(request: Request) -> UUID:
    """DEPRECATED — removed in Phase H.c. Returns a mock UUID."""
    return UUID("a0000000-0000-0000-0000-000000000001")


_MOCK_PRINCIPAL_DISPLAY_NAME = "mock-admin"


async def get_current_principal_idx(request: Request) -> int:
    """DEPRECATED — removed in Phase H.c. Looks up a mock principal by
    display_name."""
    pool = get_db_pool(request)
    idx = await pool.fetchval(
        "SELECT idx FROM qiita.principal WHERE display_name = $1",
        _MOCK_PRINCIPAL_DISPLAY_NAME,
    )
    if idx is None:
        raise HTTPException(
            status_code=503,
            detail=(
                f"Mock principal '{_MOCK_PRINCIPAL_DISPLAY_NAME}' not seeded — "
                "deprecated mock auth path"
            ),
        )
    return idx


def get_hmac_secret(request: Request) -> bytes:
    """Return the HMAC secret key from app settings."""
    settings = getattr(request.app.state, "settings", None)
    if settings is None:
        raise RuntimeError("Settings not initialised — lifespan may not have run")
    return settings.hmac_secret_key


def get_data_plane_url(request: Request) -> str:
    """Return the data plane gRPC URL from app settings."""
    settings = getattr(request.app.state, "settings", None)
    if settings is None:
        raise RuntimeError("Settings not initialised — lifespan may not have run")
    return settings.data_plane_url
