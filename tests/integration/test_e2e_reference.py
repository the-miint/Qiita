"""End-to-end integration test: create → hash → mint → load → register → DoGet.

Exercises every component: control plane (REST), orchestrator (LocalBackend),
DuckLake (Parquet registration), data plane (Arrow Flight DoGet).

Relies on the shared `data_plane`, `hmac_secret`, and `postgres_pool` fixtures
in conftest.py — no per-module process/secret/schema plumbing lives here.
"""

import base64
import json
import uuid

import pyarrow.flight as flight
import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture
def flight_client(data_plane):
    client = flight.FlightClient(f"grpc://127.0.0.1:{data_plane['port']}")
    yield client
    client.close()


@pytest.fixture
async def client(postgres_pool, hmac_secret, data_plane, human_admin_session):
    """AsyncClient with HMAC secret and data plane URL injected into app state.

    Default Authorization is the session admin (so POST /references and
    PATCH /references/{id}/status work). Service-only routes (mint, register,
    doget tickets) override per-request via `headers=worker_headers`.
    """
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.settings = Settings(
        database_url="unused-in-test",
        hmac_secret_key=hmac_secret,
        data_plane_url=f"grpc://127.0.0.1:{data_plane['port']}",
    )
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {human_admin_session['token']}"},
    ) as ac:
        yield ac


@pytest.fixture
def worker_headers(compute_worker_service_account):
    """Authorization header for the compute worker service account — required
    by mint, register_files, and tickets:doget endpoints."""
    return {"Authorization": f"Bearer {compute_worker_service_account['token']}"}


@pytest.fixture
async def ref_for_e2e(client, postgres_pool):
    """Create a reference and clean up after."""
    resp = await client.post(
        "/api/v1/reference",
        json={
            "name": f"e2e-{uuid.uuid4()}",
            "version": "1.0",
            "kind": "sequence_reference",
        },
    )
    idx = resp.json()["reference_idx"]
    yield idx
    await postgres_pool.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = $1", idx
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
    import duckdb as _ddb

    path = tmp_path / "taxonomy.parquet"
    with _ddb.connect(":memory:") as conn:
        conn.execute("CREATE TABLE t (feature_id VARCHAR, taxonomy VARCHAR)")
        conn.executemany(
            "INSERT INTO t VALUES (?, ?)",
            [
                ("seq1", "d__Bacteria; p__Bacillota; c__Bacilli; o__; f__; g__; s__"),
                ("seq2", "d__Bacteria; p__Pseudomonadota; c__; o__; f__; g__; s__"),
                ("seq3", "d__Archaea; p__Euryarchaeota; c__; o__; f__; g__; s__"),
            ],
        )
        conn.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    return path


@pytest.fixture
def tree_e2e(tmp_path):
    path = tmp_path / "tree.nwk"
    path.write_text("((seq1:0.1,seq2:0.2):0.3,seq3:0.4);")
    return path


async def test_e2e_create_to_doget(
    client,
    data_plane,
    flight_client,
    ref_for_e2e,
    fasta_e2e,
    taxonomy_e2e,
    tree_e2e,
    tmp_path,
    worker_headers,
):
    """Full E2E: create → hash → mint → load → register (via data plane) → ticket → DoGet."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    ref_idx = ref_for_e2e
    backend = LocalBackend()

    # --- Hash ---
    await client.patch(
        f"/api/v1/reference/{ref_idx}/status", json={"status": "hashing"}
    )
    hash_dir = tmp_path / "hash"
    manifest_path = await backend.run_hash_job(
        fasta_path=fasta_e2e, output_dir=hash_dir, reference_idx=ref_idx
    )
    manifest = json.loads(manifest_path.read_text())

    # --- Mint ---
    entries = [{"sequence_hash": e["sequence_hash"]} for e in manifest["entries"]]
    mint_resp = await client.post(
        f"/api/v1/reference/{ref_idx}/feature/mint",
        json={"entries": entries},
        headers=worker_headers,
    )
    assert mint_resp.status_code == 200
    fm_path = tmp_path / "feature_map.ndjson"
    with open(fm_path, "w") as f:
        for k, v in mint_resp.json()["mapping"].items():
            f.write(json.dumps({"sequence_hash": k, "feature_idx": v}) + "\n")

    # --- Load (write Parquet to staging) ---
    await client.patch(
        f"/api/v1/reference/{ref_idx}/status", json={"status": "loading"}
    )
    staging_dir = tmp_path / "staging"
    await backend.run_load_job(
        manifest_path=manifest_path,
        fasta_path=fasta_e2e,
        feature_map_path=fm_path,
        output_dir=staging_dir,
        reference_idx=ref_idx,
        taxonomy_path=taxonomy_e2e,
        tree_path=tree_e2e,
    )

    # --- Register via control plane → data plane DoAction ---
    reg_resp = await client.post(
        f"/api/v1/reference/{ref_idx}/register",
        json={
            "staging_dir": str(staging_dir),
            "files": {
                "reference_sequences.parquet": "reference_sequences",
                "reference_sequence_chunks.parquet": "reference_sequence_chunks",
                "reference_membership.parquet": "reference_membership",
                "reference_taxonomy.parquet": "reference_taxonomy",
                "reference_phylogeny.parquet": "reference_phylogeny",
            },
        },
        headers=worker_headers,
    )
    assert reg_resp.status_code == 201

    # --- Transition to active ---
    active_resp = await client.patch(
        f"/api/v1/reference/{ref_idx}/status", json={"status": "active"}
    )
    assert active_resp.status_code == 200

    # --- Sign ticket for reference_sequences (metadata-only: hash + length) ---
    ticket_resp = await client.post(
        f"/api/v1/reference/{ref_idx}/ticket/doget",
        json={"table": "reference_sequences"},
        headers=worker_headers,
    )
    assert ticket_resp.status_code == 201
    ticket_bytes = base64.b64decode(ticket_resp.json()["ticket"])

    # --- DoGet via Arrow Flight ---
    reader = flight_client.do_get(flight.Ticket(ticket_bytes))
    table = reader.read_all()

    assert table.num_rows == 3
    assert "feature_idx" in table.column_names
    assert "sequence_hash" in table.column_names
    assert "sequence_length_bp" in table.column_names
    returned_fidxs = set(table.column("feature_idx").to_pylist())
    assert len(returned_fidxs) == 3

    # --- Verify sequence data via reference_sequence_chunks ---
    chunks_ticket_resp = await client.post(
        f"/api/v1/reference/{ref_idx}/ticket/doget",
        json={"table": "reference_sequence_chunks"},
        headers=worker_headers,
    )
    assert chunks_ticket_resp.status_code == 201
    chunks_ticket_bytes = base64.b64decode(chunks_ticket_resp.json()["ticket"])

    chunks_reader = flight_client.do_get(flight.Ticket(chunks_ticket_bytes))
    chunks_table = chunks_reader.read_all()

    # 3 short sequences = 3 rows (one chunk each)
    assert chunks_table.num_rows == 3
    assert "chunk_data" in chunks_table.column_names
    sequences = set(chunks_table.column("chunk_data").to_pylist())
    assert sequences == set(TEST_SEQUENCES.values())


async def test_e2e_doget_taxonomy(
    client,
    data_plane,
    flight_client,
    ref_for_e2e,
    fasta_e2e,
    taxonomy_e2e,
    tmp_path,
    worker_headers,
):
    """Verify DoGet for reference_taxonomy returns correct parsed ranks."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    ref_idx = ref_for_e2e
    backend = LocalBackend()

    # Run pipeline (hash → mint → load → register via data plane → active)
    await client.patch(
        f"/api/v1/reference/{ref_idx}/status", json={"status": "hashing"}
    )
    manifest_path = await backend.run_hash_job(
        fasta_path=fasta_e2e,
        output_dir=tmp_path / "h",
        reference_idx=ref_idx,
    )
    manifest = json.loads(manifest_path.read_text())
    entries = [{"sequence_hash": e["sequence_hash"]} for e in manifest["entries"]]
    mint_resp = await client.post(
        f"/api/v1/reference/{ref_idx}/feature/mint",
        json={"entries": entries},
        headers=worker_headers,
    )
    fm_path = tmp_path / "fm.ndjson"
    with open(fm_path, "w") as f:
        for k, v in mint_resp.json()["mapping"].items():
            f.write(json.dumps({"sequence_hash": k, "feature_idx": v}) + "\n")

    await client.patch(
        f"/api/v1/reference/{ref_idx}/status", json={"status": "loading"}
    )
    staging = tmp_path / "s"
    await backend.run_load_job(
        manifest_path=manifest_path,
        fasta_path=fasta_e2e,
        feature_map_path=fm_path,
        output_dir=staging,
        reference_idx=ref_idx,
        taxonomy_path=taxonomy_e2e,
    )
    reg_resp = await client.post(
        f"/api/v1/reference/{ref_idx}/register",
        json={
            "staging_dir": str(staging),
            "files": {
                "reference_sequences.parquet": "reference_sequences",
                "reference_sequence_chunks.parquet": "reference_sequence_chunks",
                "reference_membership.parquet": "reference_membership",
                "reference_taxonomy.parquet": "reference_taxonomy",
            },
        },
        headers=worker_headers,
    )
    assert reg_resp.status_code == 201
    await client.patch(
        f"/api/v1/reference/{ref_idx}/status", json={"status": "active"}
    )

    # Sign ticket for taxonomy, scoped by feature_idx
    ticket_resp = await client.post(
        f"/api/v1/reference/{ref_idx}/ticket/doget",
        json={"table": "reference_taxonomy"},
        headers=worker_headers,
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


async def test_ticket_rejects_non_active_reference(client, ref_for_e2e, worker_headers):
    """Ticket endpoint must reject references not in 'active' status."""
    resp = await client.post(
        f"/api/v1/reference/{ref_for_e2e}/ticket/doget",
        json={"table": "reference_sequences"},
        headers=worker_headers,
    )
    assert resp.status_code == 409


async def test_ticket_rejects_unknown_table(client, ref_for_e2e, worker_headers):
    """Ticket endpoint must reject unknown table names."""
    resp = await client.post(
        f"/api/v1/reference/{ref_for_e2e}/ticket/doget",
        json={"table": "nonexistent_table"},
        headers=worker_headers,
    )
    assert resp.status_code == 422
