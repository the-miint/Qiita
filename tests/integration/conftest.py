"""Integration test fixtures for cross-component flows.

Postgres must be running on :5433 (via docker compose up -d --wait).
The qiita-data-plane debug binary must be pre-built before running any test
that uses the `data_plane` fixture (see `make build-data-plane-debug`).

Postgres / session / JWKS / FASTA fixtures are extracted into the
qiita_control_plane.testing subpackage and re-exported here so that both
the control-plane and integration suites resolve the same fixture by name.
"""

import asyncio
import base64
import os
import secrets
import signal
import socket
import subprocess
import time
from pathlib import Path

import asyncpg
import pytest
from qiita_common.api_paths import LOOPBACK_HOST

from _pg_env import (
    LIB_PATH_ENV,
    ducklake_catalog_connstr,
    find_duckdb_lib_dir,
)
from qiita_control_plane.testing.jwks import jwks_harness  # noqa: F401
from qiita_control_plane.testing.postgres import (  # noqa: F401
    _run_db_migrations,
    postgres_pool,
    postgres_url,
    resolve_postgres_url,
)
from qiita_control_plane.testing.sessions import (  # noqa: F401
    compute_worker_service_account,
    human_admin_session,
    regular_user_session,
)

POSTGRES_URL = resolve_postgres_url()
REPO_ROOT = Path(__file__).parent.parent.parent
DATA_PLANE_DIR = REPO_ROOT / "qiita-data-plane"
DATA_PLANE_BINARY = DATA_PLANE_DIR / "target" / "debug" / "qiita-data-plane"
DUCKDB_LIB_DIR = find_duckdb_lib_dir(DATA_PLANE_DIR)
DUCKLAKE_CATALOG_CONNSTR = ducklake_catalog_connstr()


@pytest.fixture(scope="session")
def migrations_dir() -> Path:
    """Path to qiita-control-plane/db/migrations from the integration conftest.

    Resolved from `__file__` because conftest is loaded directly from disk by
    pytest, never copied into site-packages."""
    return REPO_ROOT / "qiita-control-plane" / "db" / "migrations"


TEST_SEQUENCES = {
    "seq1": "ATCGATCGATCG",
    "seq2": "GCTAGCTAGCTA",
    "seq3": "AAATTTTCCCGGG",
    "seq4": "TTTTAAAACCCC",
    "seq5": "GGGGCCCCAAAA",
}


@pytest.fixture
def fasta_file(tmp_path):
    """Create a 5-sequence FASTA file. Used by the hash/mint pipeline test."""
    path = tmp_path / "test.fasta"
    with open(path, "w") as f:
        for name, seq in TEST_SEQUENCES.items():
            f.write(f">{name}\n{seq}\n")
    return path, TEST_SEQUENCES


@pytest.fixture(scope="session")
def control_plane_url() -> str:
    return "http://localhost:8080"


@pytest.fixture(scope="session")
def compute_orchestrator_url() -> str:
    return "http://localhost:8081"


@pytest.fixture(scope="session")
def data_plane_location() -> str:
    return "grpc://localhost:50051"


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
        s.bind((LOOPBACK_HOST, 0))
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

    lib_path = os.environ.get(LIB_PATH_ENV, "")
    if DUCKDB_LIB_DIR is not None and DUCKDB_LIB_DIR.is_dir():
        lib_path = f"{DUCKDB_LIB_DIR}:{lib_path}" if lib_path else str(DUCKDB_LIB_DIR)

    env = {
        **os.environ,
        "LISTEN_ADDR": f"{LOOPBACK_HOST}:{port}",
        "HMAC_SECRET_KEY": base64.b64encode(hmac_secret).decode(),
        "DUCKLAKE_CATALOG_CONNSTR": DUCKLAKE_CATALOG_CONNSTR,
        "DUCKLAKE_DATA_PATH": data_path,
        LIB_PATH_ENV: lib_path,
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

    if not _wait_for_grpc(LOOPBACK_HOST, port):
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
