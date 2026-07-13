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

# Set token env vars BEFORE any qiita_compute_orchestrator import so
# Settings.from_env() — eagerly called by `make_cp_client` in the
# fastq_to_parquet pipeline — finds the env-var fallback. Same pattern
# used by qiita-compute-orchestrator/tests/conftest.py for its
# in-package tests.
os.environ.setdefault("QIITA_ALLOW_TOKEN_ENV", "true")
os.environ.setdefault("CP_TO_CO_TOKEN", "test-cp-to-co-token")
os.environ.setdefault("CO_TO_CP_TOKEN", "test-co-to-cp-token")

import asyncpg  # noqa: E402
import pytest  # noqa: E402
from _pg_env import (
    LIB_PATH_ENV,
    ducklake_catalog_connstr,
    find_duckdb_lib_dir,
)
from qiita_common.api_paths import LOOPBACK_HOST
from qiita_common.duckdb_miint import setup_miint_test_env  # noqa: E402
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

# Integration tests run native orchestrator jobs (hash_sequences,
# stage_local_fasta, reference_load) in-process via LocalBackend, which LOAD the
# pre-staged miint extension (open_miint_conn — LOAD-only, like production).
# Point the install at the team mirror and a per-suite private extension
# directory — same as the control-plane and orchestrator conftests — then stage
# once into it (`_stage_miint_extension` below), mirroring the deploy. Set at
# module load (before any test/fixture connects); the env is read at DuckDB
# connect() time, so placement after the import block is correct.
setup_miint_test_env("integration")


@pytest.fixture(scope="session", autouse=True)
def _stage_miint_extension():
    """Stage miint once into the per-suite extension dir so the LOAD-only job
    paths (`open_miint_conn`) work — the integration mirror of the deploy stage.
    Plain INSTALL (not the deploy's FORCE) so the stable temp dir caches across
    runs. Also installs the GPL-boundary tool host once, mirroring the deploy's
    `stage_miint_extension` (bowtie2 alignment runs behind it), so the sharded-
    alignment smoke finds it pre-installed as a native job does. Kept in step with
    qiita-compute-orchestrator/tests/conftest.py's identical fixture."""
    import duckdb
    from qiita_common.duckdb_miint import (
        miint_connect_config,
        miint_install_sql,
        miint_load_sql,
    )

    with duckdb.connect(":memory:", config=miint_connect_config()) as conn:
        conn.execute(miint_install_sql())
        conn.execute(miint_load_sql())
        conn.execute("SELECT install_gpl_boundary()")


POSTGRES_URL = resolve_postgres_url()
REPO_ROOT = Path(__file__).parent.parent.parent
DATA_PLANE_DIR = REPO_ROOT / "qiita-data-plane"
DATA_PLANE_BINARY = DATA_PLANE_DIR / "target" / "debug" / "qiita-data-plane"
DUCKDB_LIB_DIR = find_duckdb_lib_dir(DATA_PLANE_DIR)
DUCKLAKE_CATALOG_CONNSTR = ducklake_catalog_connstr()

# Cold-start ceiling (seconds) for the data-plane gRPC port to open. The FIRST
# module to use the module-scoped `data_plane` fixture pays the coldest start:
# right after `_reset_ducklake_catalog()` drops/recreates the catalog DB, the
# binary must boot, load DuckDB + the miint extension, connect to the catalog,
# and create the DuckLake tables before its first TCP accept — which on a loaded
# CI runner can exceed a tight ceiling (later modules reuse warm caches and come
# up well under it). The poll returns the instant the port opens, so a generous
# ceiling costs nothing on success and only lengthens the wait on a genuine hang.
# Override via QIITA_DP_START_TIMEOUT_S (e.g. CI can set it higher than a local
# box); a malformed value fails loudly here at import rather than mid-run.
_DATA_PLANE_START_TIMEOUT_S = float(os.environ.get("QIITA_DP_START_TIMEOUT_S", "30"))


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


def _wait_for_grpc(
    host: str, port: int, timeout: float = _DATA_PLANE_START_TIMEOUT_S
) -> bool:
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
def signing_key() -> bytes:
    """Ed25519 PRIVATE seed (32 bytes) the signing clients / control plane use to
    sign Flight tickets. The data plane under test gets the matching PUBLIC key
    (see the `data_plane` fixture)."""
    return secrets.token_bytes(32)


@pytest.fixture(scope="module")
def data_plane(signing_key, tmp_path_factory):
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

    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    _reset_ducklake_catalog()

    # Two base roots; the data plane derives PATH_SCRATCH/staging and
    # PATH_PERSISTENT/ducklake from them (matching production). The CP
    # runner's per-ticket workspace is the sibling PATH_SCRATCH/ticket.
    # mkdir the derived leaves since the DP/DuckLake expect them to exist.
    scratch_base = tmp_path_factory.mktemp("scratch")
    persistent_base = tmp_path_factory.mktemp("persistent")
    data_path = str(persistent_base / "ducklake")
    staging_root = str(scratch_base / "staging")
    workspace_root = str(scratch_base / "ticket")
    os.makedirs(data_path, exist_ok=True)
    os.makedirs(staging_root, exist_ok=True)
    os.makedirs(workspace_root, exist_ok=True)
    port = _alloc_free_port()

    lib_path = os.environ.get(LIB_PATH_ENV, "")
    if DUCKDB_LIB_DIR is not None and DUCKDB_LIB_DIR.is_dir():
        lib_path = f"{DUCKDB_LIB_DIR}:{lib_path}" if lib_path else str(DUCKDB_LIB_DIR)

    env = {
        **os.environ,
        "LISTEN_ADDR": f"{LOOPBACK_HOST}:{port}",
        # The data plane verifies with the PUBLIC key derived from the signing seed.
        "FLIGHT_TICKET_PUBLIC_KEY": base64.b64encode(
            Ed25519PrivateKey.from_private_bytes(signing_key)
            .public_key()
            .public_bytes_raw()
        ).decode(),
        "DUCKLAKE_CATALOG_CONNSTR": DUCKLAKE_CATALOG_CONNSTR,
        "PATH_PERSISTENT": str(persistent_base),
        "PATH_SCRATCH": str(scratch_base),
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
        # The process is still alive (rc is None) but never opened the port
        # within the window — a slow cold start that exceeded the ceiling, or a
        # genuine hang. Say which, explicitly: name the pid and the port that
        # never accepted (communicate() yields little here precisely because the
        # process did NOT crash), and point at the override knob so a slow runner
        # is a config bump, not a code change.
        pid = proc.pid
        proc.kill()
        stdout, stderr = proc.communicate(timeout=5)
        pytest.fail(
            f"data plane (pid {pid}) is alive but did not accept a connection on "
            f"{LOOPBACK_HOST}:{port} within {_DATA_PLANE_START_TIMEOUT_S:g}s "
            f"(raise QIITA_DP_START_TIMEOUT_S if this is a slow cold start, not a hang).\n"
            f"stdout: {stdout.decode()[:1000]}\nstderr: {stderr.decode()[:1000]}"
        )

    yield {
        "process": proc,
        "secret": signing_key,
        "port": port,
        "data_path": data_path,
        "ducklake_connstr": DUCKLAKE_CATALOG_CONNSTR,
        "upload_staging_root": staging_root,
        "workspace_root": workspace_root,
    }

    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
