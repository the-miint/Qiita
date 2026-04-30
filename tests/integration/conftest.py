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

# Pin sslmode=disable on every connection: the test postgres container
# has SSL off, and on environments that inherit `PGSSLMODE=require` from
# the surrounding shell (e.g. GitHub Actions ubuntu-latest's pre-installed
# PostgreSQL setup) the dbmate, asyncpg, and DuckLake postgres clients
# would otherwise fail with "SSL is not enabled on the server".
POSTGRES_URL = "postgresql://qiita:qiita@localhost:5433/qiita_test?sslmode=disable"
REPO_ROOT = Path(__file__).parent.parent.parent
MIGRATIONS_DIR = str(REPO_ROOT / "qiita-control-plane" / "db" / "migrations")
DATA_PLANE_DIR = REPO_ROOT / "qiita-data-plane"
DATA_PLANE_BINARY = DATA_PLANE_DIR / "target" / "debug" / "qiita-data-plane"
DUCKDB_LIB_DIR = (
    DATA_PLANE_DIR / "target" / "duckdb-download" / "x86_64-unknown-linux-gnu" / "1.5.2"
)
DUCKLAKE_CATALOG_CONNSTR = (
    "dbname=qiita_ducklake host=localhost port=5433 user=qiita password=qiita sslmode=disable"
)


def _run_migrations(postgres_url: str) -> None:
    """Run dbmate migrations against the test database."""
    dbmate = shutil.which("dbmate")
    if dbmate is None:
        raise RuntimeError(
            "dbmate not on PATH — run 'make test-integration' or 'make migrate' to auto-install"
        )

    # dbmate expects the URL with the scheme prefix. The sslmode=disable
    # query param comes from POSTGRES_URL (see comment there).
    dbmate_url = postgres_url.replace("postgresql://", "postgres://")
    result = subprocess.run(
        [
            dbmate,
            "--url",
            dbmate_url,
            "--migrations-dir",
            MIGRATIONS_DIR,
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


class JwksHarness:
    """Local HTTP server serving a JWKS document and signing JWTs.

    Duplicated from qiita-control-plane/tests/test_oidc.py so integration tests
    can use it without cross-package imports. Counts JWKS fetches so callers
    can assert caching/refresh behavior.
    """

    def __init__(self) -> None:
        import http.server
        import json
        import threading

        from cryptography.hazmat.primitives.asymmetric import rsa
        from jwt.algorithms import RSAAlgorithm

        self._json = json
        self._RSAAlgorithm = RSAAlgorithm
        self.fetch_count = 0
        self._lock = threading.Lock()
        self._private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self._kid = f"kid-{secrets.token_hex(4)}"
        self._jwks = self._build_jwks(self._private_key, self._kid)
        self._server = http.server.HTTPServer(("127.0.0.1", 0), self._make_handler())
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def issuer(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def jwks_url(self) -> str:
        return f"{self.issuer}/connect/jwks"

    @property
    def current_kid(self) -> str:
        return self._kid

    def sign(self, claims: dict, *, kid: str | None = None, key=None) -> str:
        import jwt as _jwt

        return _jwt.encode(
            claims,
            key or self._private_key,
            algorithm="RS256",
            headers={"kid": kid or self._kid},
        )

    def rotate_key(self) -> str:
        from cryptography.hazmat.primitives.asymmetric import rsa

        with self._lock:
            self._private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            self._kid = f"kid-{secrets.token_hex(4)}"
            self._jwks = self._build_jwks(self._private_key, self._kid)
        return self._kid

    def shutdown(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)

    def _build_jwks(self, private_key, kid: str) -> dict:
        public_jwk = self._json.loads(self._RSAAlgorithm.to_jwk(private_key.public_key()))
        return {"keys": [{**public_jwk, "kid": kid, "alg": "RS256", "use": "sig"}]}

    def _make_handler(self):
        from http.server import BaseHTTPRequestHandler

        harness = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/connect/jwks":
                    with harness._lock:
                        harness.fetch_count += 1
                        body = harness._json.dumps(harness._jwks).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                else:
                    self.send_error(404)

            def log_message(self, *args, **kwargs):
                pass

        return Handler


@pytest.fixture
def jwks_harness():
    h = JwksHarness()
    yield h
    h.shutdown()


@pytest_asyncio.fixture(scope="session")
async def human_admin_session(postgres_pool):
    """A session-scoped system_admin human with a complete profile and a
    PAT carrying the full admin scope ceiling. Tests use this token to
    drive routes that require human + admin authority (POST /references,
    POST /admin/*, PATCH /users/me).

    The principal persists across sessions because qiita.auth_events
    references it via FK and is append-only — by design. Reusing the same
    display_name is idempotent: the fixture looks it up and only creates
    if absent.
    """
    from qiita_control_plane.auth.tokens import mint_api_token

    display_name = "test-human-admin"
    idx = await postgres_pool.fetchval(
        "SELECT idx FROM qiita.principal WHERE display_name = $1",
        display_name,
    )
    if idx is None:
        async with postgres_pool.acquire() as conn:
            async with conn.transaction():
                idx = await conn.fetchval(
                    "INSERT INTO qiita.principal"
                    "  (display_name, system_role, created_by_idx)"
                    " VALUES ($1, 'system_admin', 1) RETURNING idx",
                    display_name,
                )
                await conn.execute(
                    "INSERT INTO qiita.user"
                    "  (principal_idx, email, affiliation, address, phone)"
                    " VALUES ($1, $2, 'UCSD', '9500 Gilman', '555-0001')",
                    idx,
                    f"{display_name}@example.com",
                )
    # Make sure existing rows have a complete profile (idempotent).
    await postgres_pool.execute(
        "UPDATE qiita.user SET affiliation = 'UCSD', address = '9500 Gilman',"
        " phone = '555-0001'"
        " WHERE principal_idx = $1",
        idx,
    )
    # Make sure principal isn't disabled / retired from a prior partial run.
    await postgres_pool.execute(
        "UPDATE qiita.principal SET"
        "  disabled = false, disabled_at = NULL, disabled_by_idx = NULL,"
        "  disable_reason = NULL"
        " WHERE idx = $1 AND retired = false",
        idx,
    )
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=idx,
        label="session-admin",
        scopes=[
            "self:profile",
            "self:tokens",
            "references:read",
            "references:write",
            "admin:users",
            "admin:service_accounts",
            "admin:audit_read",
        ],
    )
    return {
        "principal_idx": idx,
        "token": plaintext,
        "email": f"{display_name}@example.com",
        "display_name": display_name,
    }


@pytest_asyncio.fixture(scope="session")
async def regular_user_session(postgres_pool):
    """A session-scoped 'user'-role human with a complete profile and a
    PAT scoped to the user ceiling. Used for negative-case tests that
    need a non-admin caller (e.g., 403 on admin endpoints)."""
    from qiita_control_plane.auth.tokens import mint_api_token

    display_name = "test-regular-user"
    idx = await postgres_pool.fetchval(
        "SELECT idx FROM qiita.principal WHERE display_name = $1",
        display_name,
    )
    if idx is None:
        async with postgres_pool.acquire() as conn:
            async with conn.transaction():
                idx = await conn.fetchval(
                    "INSERT INTO qiita.principal"
                    "  (display_name, system_role, created_by_idx)"
                    " VALUES ($1, 'user', 1) RETURNING idx",
                    display_name,
                )
                await conn.execute(
                    "INSERT INTO qiita.user"
                    "  (principal_idx, email, affiliation, address, phone)"
                    " VALUES ($1, $2, 'UCSD', 'X', 'Y')",
                    idx,
                    f"{display_name}@example.com",
                )
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=idx,
        label="session-user",
        scopes=["self:profile", "self:tokens", "references:read"],
    )
    return {
        "principal_idx": idx,
        "token": plaintext,
        "email": f"{display_name}@example.com",
        "display_name": display_name,
    }


@pytest_asyncio.fixture(scope="session")
async def compute_worker_service_account(postgres_pool, tmp_path_factory):
    """Provision a service-account-kind principal with worker scopes and
    write its token to a tmp file. Reused by the orchestrator-auth tests;
    the file path is the canonical drop-in for the production
    `/etc/qiita/orchestrator.token` location.

    Idempotent across pytest sessions: if a previous run created the
    service_account row (auth_events FK keeps principals around), look it
    up by name instead of re-creating. Always mints a fresh token so each
    session starts with a known-good credential.

    Returns a dict with `principal_idx`, `token_path` (Path), `token` (str).
    """
    from qiita_control_plane.auth.tokens import mint_api_token

    SVC_NAME = "compute-worker-fixture"
    pidx = await postgres_pool.fetchval(
        "SELECT principal_idx FROM qiita.service_account WHERE name = $1",
        SVC_NAME,
    )
    if pidx is None:
        async with postgres_pool.acquire() as conn:
            async with conn.transaction():
                pidx = await conn.fetchval(
                    "INSERT INTO qiita.principal"
                    "  (display_name, system_role, created_by_idx)"
                    " VALUES ($1, 'user', 1) RETURNING idx",
                    SVC_NAME,
                )
                await conn.execute(
                    "INSERT INTO qiita.service_account"
                    "  (principal_idx, name, description)"
                    " VALUES ($1, $2, 'orchestrator service-account fixture')",
                    pidx,
                    SVC_NAME,
                )
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=pidx,
        label="orchestrator-fixture",
        scopes=[
            "features:mint",
            "references:register_files",
            "references:read",
            "tickets:doget",
        ],
    )
    token_path = tmp_path_factory.mktemp("orchestrator-token") / "token"
    token_path.write_text(plaintext)
    token_path.chmod(0o400)
    return {"principal_idx": pidx, "token_path": token_path, "token": plaintext}


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
