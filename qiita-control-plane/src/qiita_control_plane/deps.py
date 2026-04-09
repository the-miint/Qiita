"""Shared FastAPI dependencies."""

from uuid import UUID

import asyncpg
from fastapi import Request


def get_db_pool(request: Request) -> asyncpg.Pool:
    """Typed accessor for the database pool — use as a FastAPI dependency."""
    pool = getattr(request.app.state, "pool", None)
    if pool is None:
        raise RuntimeError("Database pool not initialised — lifespan may not have run")
    return pool


def get_current_user(request: Request) -> UUID:
    """Return the authenticated user ID.

    Currently returns a mock user ID. Will be replaced with JWT extraction.
    """
    return UUID("a0000000-0000-0000-0000-000000000001")


def get_hmac_secret(request: Request) -> bytes:
    """Return the HMAC secret key from app settings."""
    settings = getattr(request.app.state, "settings", None)
    if settings is None:
        raise RuntimeError("Settings not initialised — lifespan may not have run")
    return settings.hmac_secret_key
