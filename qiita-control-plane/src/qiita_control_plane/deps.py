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


def get_current_user(request: Request) -> UUID:
    """Return the authenticated user ID.

    Currently returns a mock user ID. Will be replaced with JWT extraction
    in Phase E and removed entirely in Phase H.c.
    """
    return UUID("a0000000-0000-0000-0000-000000000001")


# Mock principal_idx resolver used by Phase B routes/users.py until real
# auth lands in Phase E. Looks up a fixture-seeded principal by display_name.
# In tests the integration conftest seeds it; in dev/prod it must be created
# out-of-band (or the request fails 503).
_MOCK_PRINCIPAL_DISPLAY_NAME = "mock-admin"


async def get_current_principal_idx(request: Request) -> int:
    """Return the authenticated principal's idx (mock).

    Resolves a fixture-seeded principal via display_name lookup. Replaced
    by real OIDC/PAT-driven resolution in Phase E.
    """
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
                "create one before calling auth-aware routes (Phase B mock-auth requirement)"
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
