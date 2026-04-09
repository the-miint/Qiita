"""End-to-end integration test: create → hash → mint → load → register → DoGet.

Exercises every component: control plane (REST), orchestrator (LocalBackend),
DuckLake (Parquet registration), data plane (Arrow Flight DoGet).
Requires Docker Postgres on :5433 and a compiled data plane binary.
"""

import asyncio
import base64
import json
import os
import secrets
import signal
import socket
import subprocess
import time
import uuid

import asyncpg
import pyarrow.flight as flight
import pytest
from httpx import ASGITransport, AsyncClient

DUCKLAKE_DATA_PATH = "/tmp/qiita-integration-ducklake-data"
DUCKLAKE_CONNSTR = (
    "dbname=qiita_ducklake host=localhost port=5433 user=qiita password=qiita"
)
DATA_PLANE_PORT = 50098  # different from test_doget to avoid collision


def _reset_ducklake_catalog():
    """Drop and recreate the DuckLake catalog database."""

    async def _do_reset():
        conn = await asyncpg.connect(
            "postgresql://qiita:qiita@localhost:5433/qiita_test"
        )
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


def _ensure_ducklake_tables(conn):
    """Create DuckLake tables if they don't exist."""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS qiita_lake.reference_sequences ("
        "feature_idx BIGINT NOT NULL, sequence VARCHAR NOT NULL, "
        "sequence_hash UUID NOT NULL, sequence_length_bp BIGINT NOT NULL);"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS qiita_lake.reference_taxonomy ("
        "reference_idx BIGINT NOT NULL, feature_idx BIGINT NOT NULL, "
        "domain VARCHAR, phylum VARCHAR, class VARCHAR, "
        '"order" VARCHAR, family VARCHAR, genus VARCHAR, '
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


def _wait_for_grpc(host: str, port: int, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return True
        except OSError:
            time.sleep(0.2)
    return False


@pytest.fixture(scope="module")
def hmac_secret():
    return secrets.token_bytes(32)


@pytest.fixture(scope="module")
def data_plane_process(hmac_secret):
    """Start the data plane, pre-create DuckLake tables."""
    _reset_ducklake_catalog()

    conn = _ducklake_conn()
    _ensure_ducklake_tables(conn)
    conn.close()

    # Build binary
    build_result = subprocess.run(
        ["cargo", "build"],
        cwd=os.path.join(
            os.path.dirname(__file__), "..", "..", "qiita-data-plane"
        ),
        capture_output=True,
        text=True,
        env={**os.environ, "DUCKDB_DOWNLOAD_LIB": "1"},
        timeout=300,
    )
    if build_result.returncode != 0:
        pytest.skip(f"cargo build failed: {build_result.stderr[:500]}")

    binary = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "qiita-data-plane",
        "target",
        "debug",
        "qiita-data-plane",
    )
    if not os.path.exists(binary):
        pytest.skip(f"data plane binary not found at {binary}")

    secret_b64 = base64.b64encode(hmac_secret).decode()

    duckdb_lib_dir = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "qiita-data-plane",
        "target",
        "duckdb-download",
        "x86_64-unknown-linux-gnu",
        "1.5.1",
    )
    ld_path = os.environ.get("LD_LIBRARY_PATH", "")
    if os.path.isdir(duckdb_lib_dir):
        ld_path = f"{duckdb_lib_dir}:{ld_path}" if ld_path else duckdb_lib_dir

    env = {
        **os.environ,
        "LISTEN_ADDR": f"127.0.0.1:{DATA_PLANE_PORT}",
        "HMAC_SECRET_KEY": secret_b64,
        "DUCKLAKE_CATALOG_CONNSTR": DUCKLAKE_CONNSTR,
        "DUCKLAKE_DATA_PATH": DUCKLAKE_DATA_PATH,
        "LD_LIBRARY_PATH": ld_path,
    }

    proc = subprocess.Popen(
        [binary], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    time.sleep(1)
    rc = proc.poll()
    if rc is not None:
        stdout, stderr = proc.communicate(timeout=5)
        pytest.fail(
            f"data plane exited with code {rc}.\n"
            f"stdout: {stdout.decode()[:1000]}\nstderr: {stderr.decode()[:1000]}"
        )

    if not _wait_for_grpc("127.0.0.1", DATA_PLANE_PORT):
        rc = proc.poll()
        if rc is not None:
            stdout, stderr = proc.communicate(timeout=5)
            pytest.fail(
                f"data plane exited during startup with code {rc}.\n"
                f"stdout: {stdout.decode()[:500]}\nstderr: {stderr.decode()[:500]}"
            )
        proc.kill()
        proc.communicate(timeout=5)
        pytest.fail("data plane did not start within 10s")

    yield proc

    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


@pytest.fixture
def flight_client(data_plane_process):
    client = flight.FlightClient(f"grpc://127.0.0.1:{DATA_PLANE_PORT}")
    yield client
    client.close()


@pytest.fixture
async def client(postgres_pool, hmac_secret):
    """AsyncClient with HMAC secret injected into app state."""
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.settings = Settings(
        database_url="unused-in-test",
        hmac_secret_key=hmac_secret,
    )
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac


@pytest.fixture
async def ref_for_e2e(client, postgres_pool):
    """Create a reference and clean up after."""
    resp = await client.post(
        "/api/v1/references",
        json={
            "name": f"e2e-{uuid.uuid4()}",
            "version": "1.0",
            "kind": "sequence_reference",
        },
    )
    idx = resp.json()["reference_idx"]
    yield idx
    await postgres_pool.execute(
        "DELETE FROM qiita.phylogeny_tip_feature WHERE reference_idx = $1", idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.references WHERE reference_idx = $1", idx
    )


TEST_SEQUENCES = {
    "seq1": "ATCGATCGATCG",
    "seq2": "GCTAGCTAGCTA",
    "seq3": "AAATTTTCCCGGG",
}


@pytest.fixture
def fasta_e2e(tmp_path):
    path = tmp_path / "test.fasta"
    with open(path, "w") as f:
        for name, seq in TEST_SEQUENCES.items():
            f.write(f">{name}\n{seq}\n")
    return path


@pytest.fixture
def taxonomy_e2e(tmp_path):
    path = tmp_path / "taxonomy.tsv"
    path.write_text(
        "Feature ID\tTaxon\n"
        "seq1\td__Bacteria; p__Bacillota; c__Bacilli; o__; f__; g__; s__\n"
        "seq2\td__Bacteria; p__Pseudomonadota; c__; o__; f__; g__; s__\n"
        "seq3\td__Archaea; p__Euryarchaeota; c__; o__; f__; g__; s__\n"
    )
    return path


@pytest.fixture
def tree_e2e(tmp_path):
    path = tmp_path / "tree.nwk"
    path.write_text("((seq1:0.1,seq2:0.2):0.3,seq3:0.4);")
    return path


async def test_e2e_create_to_doget(
    client,
    data_plane_process,
    flight_client,
    hmac_secret,
    ref_for_e2e,
    fasta_e2e,
    taxonomy_e2e,
    tree_e2e,
    tmp_path,
):
    """Full E2E: create → hash → mint → load → register → ticket → DoGet."""
    from qiita_compute_orchestrator.backends.local import LocalBackend
    from qiita_compute_orchestrator.registration import register_staged_parquet

    ref_idx = ref_for_e2e
    backend = LocalBackend()

    # --- Hash ---
    await client.patch(
        f"/api/v1/references/{ref_idx}/status", json={"status": "hashing"}
    )
    hash_dir = tmp_path / "hash"
    manifest_path = await backend.run_hash_job(
        fasta_path=fasta_e2e, output_dir=hash_dir, reference_idx=ref_idx
    )
    manifest = json.loads(manifest_path.read_text())

    # --- Mint ---
    entries = [{"sequence_hash": e["sequence_hash"]} for e in manifest["entries"]]
    mint_resp = await client.post(
        f"/api/v1/references/{ref_idx}/features/mint", json={"entries": entries}
    )
    assert mint_resp.status_code == 200
    feature_map = {
        uuid.UUID(k): v for k, v in mint_resp.json()["mapping"].items()
    }

    # --- Load (write Parquet to staging) ---
    await client.patch(
        f"/api/v1/references/{ref_idx}/status", json={"status": "loading"}
    )
    staging_dir = tmp_path / "staging"
    await backend.run_load_job(
        manifest_path=manifest_path,
        fasta_path=fasta_e2e,
        feature_map=feature_map,
        output_dir=staging_dir,
        reference_idx=ref_idx,
        taxonomy_path=taxonomy_e2e,
        tree_path=tree_e2e,
    )

    # --- Register in DuckLake (move staging → permanent, zero-copy) ---
    register_staged_parquet(
        staging_dir=staging_dir,
        ducklake_connstr=DUCKLAKE_CONNSTR,
        ducklake_data_path=DUCKLAKE_DATA_PATH,
        table_file_map={
            "reference_sequences.parquet": "reference_sequences",
            "reference_membership.parquet": "reference_membership",
            "reference_taxonomy.parquet": "reference_taxonomy",
            "reference_phylogeny.parquet": "reference_phylogeny",
        },
    )

    # --- Post tip features ---
    tips = json.loads((staging_dir / "tip_features.json").read_text())
    tip_resp = await client.post(
        f"/api/v1/references/{ref_idx}/phylogeny-tips",
        json={"entries": tips},
    )
    assert tip_resp.status_code == 201

    # --- Transition to active ---
    active_resp = await client.patch(
        f"/api/v1/references/{ref_idx}/status", json={"status": "active"}
    )
    assert active_resp.status_code == 200

    # --- Sign ticket for reference_sequences ---
    ticket_resp = await client.post(
        f"/api/v1/references/{ref_idx}/tickets/doget",
        json={"table": "reference_sequences"},
    )
    assert ticket_resp.status_code == 201
    ticket_bytes = base64.b64decode(ticket_resp.json()["ticket"])

    # --- DoGet via Arrow Flight ---
    ticket = flight.Ticket(ticket_bytes)
    reader = flight_client.do_get(ticket)
    table = reader.read_all()

    assert table.num_rows == 3
    assert "feature_idx" in table.column_names
    assert "sequence" in table.column_names
    returned_fidxs = set(table.column("feature_idx").to_pylist())
    assert returned_fidxs == set(feature_map.values())

    # Verify actual sequence content
    sequences = set(table.column("sequence").to_pylist())
    assert sequences == set(TEST_SEQUENCES.values())


async def test_e2e_doget_taxonomy(
    client,
    data_plane_process,
    flight_client,
    hmac_secret,
    ref_for_e2e,
    fasta_e2e,
    taxonomy_e2e,
    tmp_path,
):
    """Verify DoGet for reference_taxonomy returns correct parsed ranks."""
    from qiita_compute_orchestrator.backends.local import LocalBackend
    from qiita_compute_orchestrator.registration import register_staged_parquet

    ref_idx = ref_for_e2e
    backend = LocalBackend()

    # Run pipeline (hash → mint → load → register → active)
    await client.patch(
        f"/api/v1/references/{ref_idx}/status", json={"status": "hashing"}
    )
    manifest_path = await backend.run_hash_job(
        fasta_path=fasta_e2e,
        output_dir=tmp_path / "h",
        reference_idx=ref_idx,
    )
    manifest = json.loads(manifest_path.read_text())
    entries = [{"sequence_hash": e["sequence_hash"]} for e in manifest["entries"]]
    mint_resp = await client.post(
        f"/api/v1/references/{ref_idx}/features/mint", json={"entries": entries}
    )
    feature_map = {
        uuid.UUID(k): v for k, v in mint_resp.json()["mapping"].items()
    }

    await client.patch(
        f"/api/v1/references/{ref_idx}/status", json={"status": "loading"}
    )
    staging = tmp_path / "s"
    await backend.run_load_job(
        manifest_path=manifest_path,
        fasta_path=fasta_e2e,
        feature_map=feature_map,
        output_dir=staging,
        reference_idx=ref_idx,
        taxonomy_path=taxonomy_e2e,
    )
    register_staged_parquet(
        staging_dir=staging,
        ducklake_connstr=DUCKLAKE_CONNSTR,
        ducklake_data_path=DUCKLAKE_DATA_PATH,
        table_file_map={
            "reference_membership.parquet": "reference_membership",
            "reference_taxonomy.parquet": "reference_taxonomy",
        },
    )
    await client.patch(
        f"/api/v1/references/{ref_idx}/status", json={"status": "active"}
    )

    # Sign ticket for taxonomy, scoped by feature_idx
    ticket_resp = await client.post(
        f"/api/v1/references/{ref_idx}/tickets/doget",
        json={"table": "reference_taxonomy"},
    )
    assert ticket_resp.status_code == 201
    ticket_bytes = base64.b64decode(ticket_resp.json()["ticket"])

    reader = flight_client.do_get(flight.Ticket(ticket_bytes))
    table = reader.read_all()

    assert table.num_rows == 3
    assert "domain" in table.column_names
    assert "phylum" in table.column_names
    domains = set(table.column("domain").to_pylist())
    assert domains == {"Bacteria", "Archaea"}


async def test_ticket_rejects_non_active_reference(client, ref_for_e2e):
    """Ticket endpoint must reject references not in 'active' status."""
    resp = await client.post(
        f"/api/v1/references/{ref_for_e2e}/tickets/doget",
        json={"table": "reference_sequences"},
    )
    assert resp.status_code == 409


async def test_ticket_rejects_unknown_table(client, ref_for_e2e):
    """Ticket endpoint must reject unknown table names."""
    resp = await client.post(
        f"/api/v1/references/{ref_for_e2e}/tickets/doget",
        json={"table": "nonexistent_table"},
    )
    assert resp.status_code == 422
