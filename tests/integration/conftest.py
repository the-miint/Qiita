"""Integration test fixtures.

Postgres must be running on :5433 (via docker compose up -d --wait).
The qiita-data-plane debug binary must be pre-built before running any test
that uses the `data_plane` fixture (see `make build-data-plane-debug`).
"""

import asyncio
import base64
import os
import secrets
import shutil
import signal
import socket
import subprocess
import time
from pathlib import Path

import asyncpg
import httpx
import pytest
import pytest_asyncio

POSTGRES_URL = "postgresql://qiita:qiita@localhost:5433/qiita_test"
REPO_ROOT = Path(__file__).parent.parent.parent
MIGRATIONS_DIR = str(REPO_ROOT / "qiita-control-plane" / "db" / "migrations")
DATA_PLANE_DIR = REPO_ROOT / "qiita-data-plane"
DATA_PLANE_BINARY = DATA_PLANE_DIR / "target" / "debug" / "qiita-data-plane"
DUCKDB_LIB_DIR = (
    DATA_PLANE_DIR / "target" / "duckdb-download" / "x86_64-unknown-linux-gnu" / "1.5.2"
)
DUCKLAKE_CATALOG_CONNSTR = (
    "dbname=qiita_ducklake host=localhost port=5433 user=qiita password=qiita"
)


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


TEST_SEQUENCES = {
    "seq1": "ATCGATCGATCG",
    "seq2": "GCTAGCTAGCTA",
    "seq3": "AAATTTTCCCGGG",
    "seq4": "TTTTAAAACCCC",
    "seq5": "GGGGCCCCAAAA",
}


@pytest.fixture
def fasta_file(tmp_path):
    """Create a 5-sequence FASTA file. Shared across test modules."""
    path = tmp_path / "test.fasta"
    with open(path, "w") as f:
        for name, seq in TEST_SEQUENCES.items():
            f.write(f">{name}\n{seq}\n")
    return path, TEST_SEQUENCES


# ---------------------------------------------------------------------------
# Data plane fixture (shared by any test that needs a live data plane process)
# ---------------------------------------------------------------------------


def _reset_ducklake_catalog() -> None:
    """Drop and recreate the DuckLake catalog database for a clean run."""

    async def _do():
        conn = await asyncpg.connect(POSTGRES_URL)
        await conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = 'qiita_ducklake' AND pid != pg_backend_pid()"
        )
        await conn.execute("DROP DATABASE IF EXISTS qiita_ducklake")
        await conn.execute("CREATE DATABASE qiita_ducklake OWNER qiita")
        await conn.close()

    asyncio.run(_do())


def _wait_for_grpc(host: str, port: int, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def _alloc_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def ducklake_connect(data_path: str):
    """Open a short-lived Python DuckDB connection attached to DuckLake at data_path.

    The caller is responsible for closing it. Use for seeding or inspecting test
    data — the data plane owns schema creation on startup.
    """
    import duckdb

    conn = duckdb.connect(":memory:")
    conn.execute("LOAD ducklake; LOAD postgres;")
    conn.execute(
        f"ATTACH 'ducklake:postgres:{DUCKLAKE_CATALOG_CONNSTR}' AS qiita_lake"
        f" (DATA_PATH '{data_path}');"
    )
    return conn


@pytest.fixture(scope="module")
def hmac_secret() -> bytes:
    """HMAC secret shared between the data plane under test and its clients."""
    return secrets.token_bytes(32)


@pytest.fixture(scope="module")
def data_plane(hmac_secret, tmp_path_factory):
    """Start the qiita-data-plane binary for the duration of a test module.

    Yields a dict with: process, secret, port, data_path, ducklake_connstr.

    The binary must already be built (see Makefile: build-data-plane-debug).
    DuckLake tables are created by the data plane itself on startup — tests
    must not duplicate the DDL. Tests that need to seed rows should do so via
    `ducklake_connect(data_plane["data_path"])` after the fixture yields.
    """
    if not DATA_PLANE_BINARY.exists():
        pytest.fail(
            f"data plane binary not found at {DATA_PLANE_BINARY}. "
            f"Run 'make build-data-plane-debug' (or 'make test-integration', "
            f"which builds it first)."
        )

    _reset_ducklake_catalog()

    data_path = str(tmp_path_factory.mktemp("ducklake-data"))
    port = _alloc_free_port()

    ld_path = os.environ.get("LD_LIBRARY_PATH", "")
    if DUCKDB_LIB_DIR.is_dir():
        ld_path = f"{DUCKDB_LIB_DIR}:{ld_path}" if ld_path else str(DUCKDB_LIB_DIR)

    env = {
        **os.environ,
        "LISTEN_ADDR": f"127.0.0.1:{port}",
        "HMAC_SECRET_KEY": base64.b64encode(hmac_secret).decode(),
        "DUCKLAKE_CATALOG_CONNSTR": DUCKLAKE_CATALOG_CONNSTR,
        "DUCKLAKE_DATA_PATH": data_path,
        "LD_LIBRARY_PATH": ld_path,
    }

    proc = subprocess.Popen(
        [str(DATA_PLANE_BINARY)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    time.sleep(1)
    rc = proc.poll()
    if rc is not None:
        stdout, stderr = proc.communicate(timeout=5)
        pytest.fail(
            f"data plane exited with code {rc}.\n"
            f"stdout: {stdout.decode()[:1000]}\nstderr: {stderr.decode()[:1000]}"
        )

    if not _wait_for_grpc("127.0.0.1", port):
        rc = proc.poll()
        if rc is not None:
            stdout, stderr = proc.communicate(timeout=5)
            pytest.fail(
                f"data plane exited during startup with code {rc}.\n"
                f"stdout: {stdout.decode()[:1000]}\nstderr: {stderr.decode()[:1000]}"
            )
        proc.kill()
        stdout, stderr = proc.communicate(timeout=5)
        pytest.fail(
            f"data plane did not start within 10s.\n"
            f"stdout: {stdout.decode()[:1000]}\nstderr: {stderr.decode()[:1000]}"
        )

    yield {
        "process": proc,
        "secret": hmac_secret,
        "port": port,
        "data_path": data_path,
        "ducklake_connstr": DUCKLAKE_CATALOG_CONNSTR,
    }

    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
