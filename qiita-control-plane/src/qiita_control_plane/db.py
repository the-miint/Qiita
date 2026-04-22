"""Postgres connection pool for the control plane."""

import asyncpg


async def get_pool(
    database_url: str,
    *,
    min_size: int = 2,
    max_size: int = 10,
    command_timeout: float = 10.0,
    connect_timeout: float = 5.0,
) -> asyncpg.Pool:
    """Create and return an asyncpg connection pool.

    Raises on connection failure — fail fast, fail loud.
    """
    return await asyncpg.create_pool(
        database_url,
        min_size=min_size,
        max_size=max_size,
        command_timeout=command_timeout,
        timeout=connect_timeout,
    )


async def close_pool(pool: asyncpg.Pool) -> None:
    """Gracefully close the connection pool."""
    await pool.close()
