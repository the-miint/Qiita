"""Integration tests for the get_tx_conn FastAPI dependency.

Exercises the dependency against the real test postgres via a tiny in-test
FastAPI app. Verifies that get_tx_conn commits on normal handler return
and rolls back on any raised exception.
"""

from collections.abc import AsyncIterator

import asyncpg
import httpx
import pytest_asyncio
from fastapi import Depends, FastAPI

from qiita_control_plane.deps import get_tx_conn


@pytest_asyncio.fixture(scope="module")
async def _kv_table(postgres_pool: asyncpg.Pool) -> AsyncIterator[None]:
    """Create a dedicated test table for these tests; drop at module end."""
    await postgres_pool.execute(
        "CREATE TABLE IF NOT EXISTS _test_db_deps_kv (key text primary key, value text)"
    )
    try:
        yield
    finally:
        await postgres_pool.execute("DROP TABLE IF EXISTS _test_db_deps_kv")


@pytest_asyncio.fixture
async def clean_kv(_kv_table: None, postgres_pool: asyncpg.Pool) -> None:
    """Truncate the test KV table before each test."""
    await postgres_pool.execute("TRUNCATE TABLE _test_db_deps_kv")


@pytest_asyncio.fixture(scope="module")
async def client(
    postgres_pool: asyncpg.Pool, _kv_table: None
) -> AsyncIterator[httpx.AsyncClient]:
    """Tiny FastAPI app with two test routes exercising get_tx_conn."""
    app = FastAPI()
    app.state.pool = postgres_pool

    @app.post("/insert/{key}")
    async def _insert(
        key: str, conn: asyncpg.Connection = Depends(get_tx_conn)
    ) -> dict:
        await conn.execute(
            "INSERT INTO _test_db_deps_kv (key, value) VALUES ($1, $2)",
            key,
            "x",
        )
        return {"key": key}

    @app.post("/insert-fail/{key}")
    async def _insert_fail(
        key: str, conn: asyncpg.Connection = Depends(get_tx_conn)
    ) -> dict:
        # Insert a row, then raise — get_tx_conn must roll back the insert.
        await conn.execute(
            "INSERT INTO _test_db_deps_kv (key, value) VALUES ($1, $2)",
            key,
            "x",
        )
        raise RuntimeError("intentional rollback trigger")

    # raise_app_exceptions=False lets FastAPI's ServerErrorMiddleware translate
    # the deliberate RuntimeError in /insert-fail into a 500 response, instead
    # of httpx re-raising it in the test process.
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_get_tx_conn_commits_on_normal_return(
    client: httpx.AsyncClient,
    postgres_pool: asyncpg.Pool,
    clean_kv: None,
) -> None:
    response = await client.post("/insert/commit-test")
    persisted = await postgres_pool.fetchval(
        "SELECT value FROM _test_db_deps_kv WHERE key = $1",
        "commit-test",
    )
    actual = (response.status_code, persisted)
    expected = (200, "x")
    assert actual == expected


async def test_get_tx_conn_rolls_back_on_exception(
    client: httpx.AsyncClient,
    postgres_pool: asyncpg.Pool,
    clean_kv: None,
) -> None:
    response = await client.post("/insert-fail/rollback-test")
    persisted = await postgres_pool.fetchval(
        "SELECT value FROM _test_db_deps_kv WHERE key = $1",
        "rollback-test",
    )
    actual = (response.status_code, persisted)
    expected = (500, None)
    assert actual == expected
