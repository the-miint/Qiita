"""Integration test: sign ticket in Python → DoGet via pyarrow.flight → verify Arrow data.

Starts the data plane binary as a subprocess, inserts test data into DuckLake,
signs a ticket, and verifies DoGet returns correct Arrow RecordBatches.
"""

import asyncio
import base64
import os
import secrets
import signal
import subprocess
import time

import asyncpg
import pyarrow.flight as flight
import pytest

DUCKLAKE_DATA_PATH = "/tmp/qiita-integration-ducklake-data"
DUCKLAKE_CONNSTR = "dbname=qiita_ducklake host=localhost port=5433 user=qiita password=qiita"


def _reset_ducklake_catalog():
    """Drop and recreate the DuckLake catalog database for a clean test session."""

    async def _do_reset():
        conn = await asyncpg.connect("postgresql://qiita:qiita@localhost:5433/qiita_test")
        await conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = 'qiita_ducklake' AND pid != pg_backend_pid()"
        )
        await conn.execute("DROP DATABASE IF EXISTS qiita_ducklake")
        await conn.execute("CREATE DATABASE qiita_ducklake OWNER qiita")
        await conn.close()

    asyncio.run(_do_reset())


def _ducklake_conn():
    """Open a Python DuckDB connection attached to DuckLake."""
    import duckdb

    os.makedirs(DUCKLAKE_DATA_PATH, exist_ok=True)
    conn = duckdb.connect(":memory:")
    conn.execute("LOAD ducklake; LOAD postgres;")
    conn.execute(
        f"ATTACH 'ducklake:postgres:{DUCKLAKE_CONNSTR}' AS qiita_lake"
        f" (DATA_PATH '{DUCKLAKE_DATA_PATH}');"
    )
    return conn


def _wait_for_grpc(host: str, port: int, timeout: float = 10.0) -> bool:
    """Wait for the gRPC server to accept TCP connections."""
    import socket

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return True
        except OSError:
            time.sleep(0.2)
    return False


@pytest.fixture(scope="module")
def data_plane_process():
    """Start the data plane binary as a subprocess and stop it after tests."""
    _reset_ducklake_catalog()

    # Pre-create DuckLake tables from Python so the catalog is initialized
    # with the correct DATA_PATH before the data plane starts.
    conn = _ducklake_conn()
    conn.execute(
        "CREATE TABLE IF NOT EXISTS qiita_lake.reference_sequences ("
        "feature_idx BIGINT NOT NULL, sequence VARCHAR NOT NULL, "
        "sequence_hash UUID NOT NULL, sequence_length_bp BIGINT NOT NULL);"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS qiita_lake.reference_taxonomy ("
        "reference_idx BIGINT NOT NULL, feature_idx BIGINT NOT NULL, "
        "domain VARCHAR, phylum VARCHAR, class VARCHAR, "
        '\"order\" VARCHAR, family VARCHAR, genus VARCHAR, '
        "species VARCHAR, strain VARCHAR, ncbi_taxon_id BIGINT);"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS qiita_lake.reference_phylogeny ("
        "reference_idx BIGINT NOT NULL, node_index BIGINT NOT NULL, "
        "name VARCHAR, branch_length DOUBLE, edge_id BIGINT, "
        "parent_index BIGINT, is_tip BOOLEAN NOT NULL);"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS qiita_lake.reference_membership ("
        "reference_idx BIGINT NOT NULL, feature_idx BIGINT NOT NULL);"
    )
    # Insert test data before starting the data plane — avoids cross-connection
    # DuckLake snapshot conflicts during tests.
    conn.execute(
        "INSERT INTO qiita_lake.reference_sequences VALUES "
        "(700001, 'ATCGATCG', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID, 8), "
        "(700002, 'GCTAGCTA', 'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22'::UUID, 8);"
    )
    conn.close()

    # Build the binary
    build_result = subprocess.run(
        ["cargo", "build"],
        cwd=os.path.join(os.path.dirname(__file__), "..", "..", "qiita-data-plane"),
        capture_output=True,
        text=True,
        env={**os.environ, "DUCKDB_DOWNLOAD_LIB": "1"},
        timeout=300,
    )
    if build_result.returncode != 0:
        pytest.skip(f"cargo build failed: {build_result.stderr[:500]}")

    binary = os.path.join(
        os.path.dirname(__file__), "..", "..", "qiita-data-plane", "target", "debug", "qiita-data-plane"
    )
    if not os.path.exists(binary):
        pytest.skip(f"data plane binary not found at {binary}")

    secret_bytes = secrets.token_bytes(32)
    secret_b64 = base64.b64encode(secret_bytes).decode()

    # The data plane binary links against libduckdb.so dynamically (not bundled
    # in test builds). Set LD_LIBRARY_PATH to the download location.
    duckdb_lib_dir = os.path.join(
        os.path.dirname(__file__), "..", "..", "qiita-data-plane",
        "target", "duckdb-download", "x86_64-unknown-linux-gnu", "1.5.1",
    )
    ld_path = os.environ.get("LD_LIBRARY_PATH", "")
    if os.path.isdir(duckdb_lib_dir):
        ld_path = f"{duckdb_lib_dir}:{ld_path}" if ld_path else duckdb_lib_dir

    env = {
        **os.environ,
        "LISTEN_ADDR": "127.0.0.1:50099",
        "HMAC_SECRET_KEY": secret_b64,
        "DUCKLAKE_CATALOG_CONNSTR": DUCKLAKE_CONNSTR,
        "DUCKLAKE_DATA_PATH": DUCKLAKE_DATA_PATH,
        "LD_LIBRARY_PATH": ld_path,
    }

    proc = subprocess.Popen(
        [binary],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait a moment then check if it exited immediately
    time.sleep(1)
    rc = proc.poll()
    if rc is not None:
        stdout, stderr = proc.communicate(timeout=5)
        pytest.fail(
            f"data plane exited with code {rc}.\n"
            f"stdout: {stdout.decode()[:1000]}\nstderr: {stderr.decode()[:1000]}"
        )

    if not _wait_for_grpc("127.0.0.1", 50099):
        rc = proc.poll()
        if rc is not None:
            stdout, stderr = proc.communicate(timeout=5)
            pytest.fail(
                f"data plane exited with code {rc} during startup.\n"
                f"stdout: {stdout.decode()[:1000]}\nstderr: {stderr.decode()[:1000]}"
            )
        proc.kill()
        stdout, stderr = proc.communicate(timeout=5)
        pytest.fail(
            f"data plane did not start within 10s (still running).\n"
            f"stdout: {stdout.decode()[:1000]}\nstderr: {stderr.decode()[:1000]}"
        )

    yield {"process": proc, "secret": secret_bytes, "port": 50099}

    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


@pytest.fixture
def flight_client(data_plane_process):
    """pyarrow FlightClient connected to the test data plane."""
    client = flight.FlightClient(f"grpc://127.0.0.1:{data_plane_process['port']}")
    yield client
    client.close()


def _sign_ticket(table, filter_dict, secret_bytes):
    """Sign a ticket using the Python signing module."""
    from qiita_control_plane.auth.tickets import sign_ticket

    return sign_ticket(table=table, filter=filter_dict, secret=secret_bytes)


def test_doget_reference_sequences(data_plane_process, flight_client):
    """DoGet with a valid ticket returns Arrow data from reference_sequences."""
    secret = data_plane_process["secret"]

    # Test data was pre-inserted by the data_plane_process fixture.
    ticket_bytes = _sign_ticket(
        "reference_sequences",
        {"feature_idx": [700001, 700002]},
        secret,
    )

    ticket = flight.Ticket(ticket_bytes)
    reader = flight_client.do_get(ticket)
    table = reader.read_all()

    assert table.num_rows == 2
    assert "feature_idx" in table.column_names
    assert "sequence" in table.column_names
    feature_idxs = sorted(table.column("feature_idx").to_pylist())
    assert feature_idxs == [700001, 700002]


def test_doget_tampered_ticket(data_plane_process, flight_client):
    """DoGet with a tampered ticket must fail with Unauthenticated."""
    secret = data_plane_process["secret"]
    ticket_bytes = bytearray(
        _sign_ticket("reference_sequences", {"feature_idx": [1]}, secret)
    )
    ticket_bytes[10] ^= 0xFF

    ticket = flight.Ticket(bytes(ticket_bytes))
    with pytest.raises(flight.FlightUnauthenticatedError):
        flight_client.do_get(ticket).read_all()


def test_doget_empty_result(data_plane_process, flight_client):
    """DoGet for non-existent feature_idx returns empty table, not error."""
    secret = data_plane_process["secret"]
    ticket_bytes = _sign_ticket(
        "reference_sequences",
        {"feature_idx": [999999]},
        secret,
    )
    ticket = flight.Ticket(ticket_bytes)
    reader = flight_client.do_get(ticket)
    table = reader.read_all()
    assert table.num_rows == 0
