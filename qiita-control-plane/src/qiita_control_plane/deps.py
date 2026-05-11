"""Shared FastAPI dependencies."""

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

import asyncpg
from fastapi import Request

# Type alias for the lazy-transaction factory returned by
# get_tx_conn_factory. A handler that takes
# `tx: TxConnFactory = Depends(get_tx_conn_factory)` opens the
# transaction explicitly with `async with tx() as conn:` instead of
# at dependency-resolution time.
TxConnFactory = Callable[[], AbstractAsyncContextManager[asyncpg.Connection]]


def get_db_pool(request: Request) -> asyncpg.Pool:
    """FastAPI dependency: return the pool that `lifespan` created via
    `qiita_control_plane.db.get_pool` and stashed on `app.state.pool`.

    Use this from route handlers and request-scoped deps:
        async def my_route(pool: asyncpg.Pool = Depends(get_db_pool)): ...

    This accessor only retrieves the pool — it never creates one. Raises
    RuntimeError if called before lifespan has run (e.g. from a unit test
    that builds the app without lifespan)."""
    pool = getattr(request.app.state, "pool", None)
    if pool is None:
        raise RuntimeError("Database pool not initialised — lifespan may not have run")
    return pool


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


async def get_tx_conn(request: Request) -> AsyncIterator[asyncpg.Connection]:
    """Acquire an asyncpg.Connection from the pool wrapped in a transaction.

    Use as a FastAPI dependency on write endpoints (POST/PUT/PATCH/DELETE).
    The transaction commits on normal handler return and rolls back on any
    raised exception, including HTTPException.
    """
    pool = get_db_pool(request)
    async with pool.acquire() as conn:
        async with conn.transaction():
            yield conn


def get_tx_conn_factory(request: Request) -> TxConnFactory:
    """Return a callable that, when invoked, yields a transactional
    connection context manager.

    Use on write endpoints whose handlers perform non-trivial work
    (JWT verification, freshness checks, in-handler validation) before
    any DB writes. The handler opens the transaction explicitly:

        async def my_handler(tx: TxConnFactory = Depends(get_tx_conn_factory)):
            # ... pure validation that may 4xx ...
            async with tx() as conn:
                # ... atomic DB work ...

    Prefer get_tx_conn for handlers that go to the DB immediately;
    this dep only earns its keep when there is meaningful pre-DB
    work whose connection cost matters under high 4xx rates.
    """
    pool = get_db_pool(request)

    @asynccontextmanager
    async def _factory() -> AsyncIterator[asyncpg.Connection]:
        async with pool.acquire() as conn:
            async with conn.transaction():
                yield conn

    return _factory
