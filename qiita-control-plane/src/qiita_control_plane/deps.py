"""Shared FastAPI dependencies."""

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from pathlib import Path

import asyncpg
from fastapi import Request

from .config import Settings

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


def get_settings(request: Request) -> Settings:
    """Return the Settings instance stashed on `app.state.settings` by
    lifespan. Single source of truth for the runtime-not-initialised
    guard; field-projection helpers (`get_flight_signing_key`,
    `get_data_plane_url`) delegate here so the check lives in one place.
    """
    settings = getattr(request.app.state, "settings", None)
    if settings is None:
        raise RuntimeError("Settings not initialised — lifespan may not have run")
    return settings


def get_flight_signing_key(request: Request) -> bytes:
    """Return the Ed25519 Flight-ticket signing key from app settings."""
    return get_settings(request).flight_signing_key


def get_data_plane_url(request: Request) -> str:
    """Return the data plane gRPC URL from app settings."""
    return get_settings(request).data_plane_url


def get_scratch_staging(request: Request) -> Path | None:
    """Return the shared-scratch staging root (`PATH_SCRATCH/staging`), or None
    in CP-only/dev where no scratch is configured. This is the root under which
    the bcl-convert `ingest_reads` step writes durable per-sample read copies
    (`reads/{prep_sample_idx}/read.parquet`); the pool-delete reaper removes
    them from here."""
    return get_settings(request).path_scratch_staging


def get_tx_conn_factory(request: Request) -> TxConnFactory:
    """Return a callable that, when invoked, yields a transactional
    connection context manager. The dep itself acquires nothing — the
    pool connection and `conn.transaction()` are deferred to the handler's
    explicit `async with tx() as conn:` block, so any pre-DB validation
    (JWT verification, freshness checks, scope guards) runs without
    holding a pool slot.

    Use as the standard write-endpoint dep:

        async def my_handler(tx: TxConnFactory = Depends(get_tx_conn_factory)):
            # ... pure validation that may 4xx ...
            async with tx() as conn:
                # ... atomic DB work ...
    """
    pool = get_db_pool(request)

    @asynccontextmanager
    async def _factory() -> AsyncIterator[asyncpg.Connection]:
        async with pool.acquire() as conn:
            async with conn.transaction():
                yield conn

    return _factory


def get_snapshot_conn_factory(request: Request) -> TxConnFactory:
    """Return a factory yielding a read-only transaction at REPEATABLE READ.

    Use when a handler issues multiple SELECTs that must observe the same
    point-in-time view. The project default isolation is READ COMMITTED,
    under which each statement takes its own snapshot — two SELECTs in
    the same transaction can disagree if a concurrent writer commits
    between them. REPEATABLE READ takes one snapshot at the transaction's
    first statement and reuses it for every later statement; readonly=True
    documents intent and lets PostgreSQL reject accidental writes.

    Acquisition is deferred to the handler's explicit
    `async with snapshot() as conn:` block so pre-DB validation runs
    without holding a pool slot, mirroring get_tx_conn_factory.

        async def my_handler(snapshot: TxConnFactory = Depends(get_snapshot_conn_factory)):
            async with snapshot() as conn:
                # ... two or more SELECTs that share one snapshot ...
    """
    pool = get_db_pool(request)

    @asynccontextmanager
    async def _factory() -> AsyncIterator[asyncpg.Connection]:
        async with pool.acquire() as conn:
            async with conn.transaction(isolation="repeatable_read", readonly=True):
                yield conn

    return _factory
