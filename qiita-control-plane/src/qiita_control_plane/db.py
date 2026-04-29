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
    """Create a fresh asyncpg connection pool.

    Lifecycle: call this once at app startup (typically inside FastAPI's
    `lifespan`) and store the result on `app.state.pool`. Routes do NOT
    call this — they retrieve the already-created pool via
    `qiita_control_plane.deps.get_db_pool`, which is the request-scoped
    FastAPI dependency accessor. Pair with `close_pool` at shutdown.

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
