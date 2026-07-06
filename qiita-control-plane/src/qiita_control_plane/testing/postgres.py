"""Postgres fixtures for control-plane tests.

Exposes the connection-string resolver (with QIITA_TEST_POSTGRES_URL override)
and the session-scoped fixtures `postgres_url`, `_run_db_migrations`, and
`postgres_pool`. Migrations are run once per session via dbmate against the
directory provided by the consumer's `migrations_dir` fixture — the consumer
is responsible for declaring that fixture (each conftest.py walks the
filesystem from its own location, which works regardless of how this package
is installed).
"""

import asyncio
import os
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import asyncpg
import pytest
import pytest_asyncio

# Pin sslmode=disable on every connection: the test postgres container has
# SSL off, and on environments that inherit `PGSSLMODE=require` from the
# surrounding shell (e.g. GitHub Actions ubuntu-latest's pre-installed
# PostgreSQL setup) the dbmate, asyncpg, and DuckLake postgres clients would
# otherwise fail with "SSL is not enabled on the server". CI override values
# (QIITA_TEST_POSTGRES_URL) must include it too.
POSTGRES_URL_DEFAULT = "postgresql://qiita:qiita@localhost:5433/qiita_test?sslmode=disable"


def resolve_postgres_url() -> str:
    """Return the test database URL, honoring QIITA_TEST_POSTGRES_URL."""
    return os.environ.get("QIITA_TEST_POSTGRES_URL", POSTGRES_URL_DEFAULT)


def _provision_worker_database(base_url: str, worker: str) -> str:
    """Return `base_url` with a per-xdist-worker database, created fresh.

    Under `pytest -n` each worker is a separate process that would otherwise
    share the single `qiita_test` database — and the session-scoped fixtures
    (fixed-name principals in sessions.py, the dbmate migration run below) are
    idempotent across *sequential* sessions but race when N workers hit one DB
    at once. So give each worker its own database (`qiita_test_gw0`, …): the
    connection string is fully injectable, and `_run_db_migrations` /
    `postgres_pool` already derive everything from this fixture's return value,
    so the rest of the suite needs no per-worker awareness.

    DROP+CREATE (FORCE, PG13+) guarantees a clean catalog per session so a
    prior run's rows can't leak in. Requires the connecting role to have
    CREATEDB — the test `qiita` role is a superuser in both harnesses (docker
    `POSTGRES_USER` and the macOS host-postgres fixture's `WITH SUPERUSER`).
    """
    parsed = urlparse(base_url)
    base_db = parsed.path.lstrip("/")
    worker_db = f"{base_db}_{worker}"
    # Connect to the always-present `postgres` maintenance DB to (re)create the
    # worker DB; keep host/port/user/password/query (sslmode) from the base URL.
    admin_url = urlunparse(parsed._replace(path="/postgres"))

    async def _recreate() -> None:
        conn = await asyncpg.connect(admin_url)
        try:
            await conn.execute(f'DROP DATABASE IF EXISTS "{worker_db}" WITH (FORCE)')
            await conn.execute(f'CREATE DATABASE "{worker_db}"')
        finally:
            await conn.close()

    # Own, short-lived loop so this sync fixture doesn't disturb the
    # session-scoped loop pytest-asyncio manages for the async fixtures/tests.
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(_recreate())
    finally:
        loop.close()

    return urlunparse(parsed._replace(path=f"/{worker_db}"))


def run_migrations(postgres_url: str, migrations_dir: Path) -> None:
    """Run dbmate migrations against the test database."""
    dbmate = shutil.which("dbmate")
    if dbmate is None:
        raise RuntimeError(
            "dbmate not on PATH — run 'make test-control-plane-with-db' or 'make migrate' "
            "to auto-install"
        )

    # dbmate expects the URL with the postgres:// scheme; the sslmode=disable
    # query param flows through from POSTGRES_URL_DEFAULT.
    dbmate_url = postgres_url.replace("postgresql://", "postgres://")
    result = subprocess.run(
        [
            dbmate,
            "--url",
            dbmate_url,
            "--migrations-dir",
            str(migrations_dir),
            "--migrations-table",
            "public.schema_migrations",
            "--no-dump-schema",
            "up",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"dbmate migration failed:\n{result.stderr}")


@pytest.fixture(scope="session")
def postgres_url() -> str:
    base = resolve_postgres_url()
    # `pytest-xdist` sets PYTEST_XDIST_WORKER (gw0, gw1, …) in each worker
    # process; give each its own database so parallel workers never share
    # catalog state. Unset (serial run) → the shared base DB, unchanged.
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    if worker:
        return _provision_worker_database(base, worker)
    return base


@pytest.fixture(scope="session")
def _run_db_migrations(postgres_url, migrations_dir):
    """Run migrations once per test session.

    `migrations_dir` is provided by the consumer's conftest.py."""
    run_migrations(postgres_url, migrations_dir)


@pytest_asyncio.fixture(scope="session")
async def postgres_pool(_run_db_migrations, postgres_url):
    """Session-scoped asyncpg pool connected to the test database."""
    pool = await asyncpg.create_pool(postgres_url, min_size=1, max_size=5, timeout=5)
    yield pool
    await pool.close()
