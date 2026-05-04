"""Postgres fixtures for control-plane tests.

Exposes the connection-string resolver (with QIITA_TEST_POSTGRES_URL override)
and the session-scoped fixtures `postgres_url`, `_run_db_migrations`, and
`postgres_pool`. Migrations are run once per session via dbmate against the
directory provided by the consumer's `migrations_dir` fixture — the consumer
is responsible for declaring that fixture (each conftest.py walks the
filesystem from its own location, which works regardless of how this package
is installed).
"""

import os
import shutil
import subprocess
from pathlib import Path

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
    return resolve_postgres_url()


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
