"""Integration test fixtures.

Postgres must be running on :5433 (via docker compose up -d --wait).
"""

import shutil
import subprocess
from pathlib import Path

import asyncpg
import httpx
import pytest
import pytest_asyncio

POSTGRES_URL = "postgresql://qiita:qiita@localhost:5433/qiita_test"
MIGRATIONS_DIR = str(Path(__file__).parent.parent.parent / "qiita-control-plane" / "db" / "migrations")


def _run_migrations(postgres_url: str) -> None:
    """Run dbmate migrations against the test database."""
    dbmate = shutil.which("dbmate")
    if dbmate is None:
        pytest.skip("dbmate not installed — run 'make migrate' to auto-install")

    # dbmate expects the URL with the scheme prefix
    dbmate_url = postgres_url.replace("postgresql://", "postgres://")
    result = subprocess.run(
        [
            dbmate,
            "--url", dbmate_url,
            "--migrations-dir", MIGRATIONS_DIR,
            "--migrations-table", "public.schema_migrations",
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
    return POSTGRES_URL


@pytest.fixture(scope="session")
def _run_db_migrations(postgres_url):
    """Run migrations once per test session."""
    _run_migrations(postgres_url)


@pytest_asyncio.fixture(scope="session")
async def postgres_pool(_run_db_migrations, postgres_url):
    """Session-scoped asyncpg pool connected to the test database."""
    pool = await asyncpg.create_pool(postgres_url, min_size=1, max_size=5, timeout=5)
    yield pool
    await pool.close()


@pytest.fixture(scope="session")
def control_plane_url() -> str:
    return "http://localhost:8080"


@pytest.fixture(scope="session")
def compute_orchestrator_url() -> str:
    return "http://localhost:8081"


@pytest.fixture(scope="session")
def data_plane_location() -> str:
    return "grpc://localhost:50051"


@pytest.fixture(scope="session")
def http() -> httpx.Client:
    """Shared HTTP client for the full test session."""
    with httpx.Client(timeout=10) as client:
        yield client
