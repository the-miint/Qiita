"""Integration test: full reference load pipeline.

create → hash → mint → load → active.
Requires Docker Postgres on :5433.
"""

import json
import uuid

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture
async def client(postgres_pool, human_admin_session):
    """AsyncClient authenticated as the session admin. Service-only routes
    (mint) take a worker token via per-request `headers=worker_headers`."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {human_admin_session['token']}"},
    ) as ac:
        yield ac


@pytest.fixture
def worker_headers(compute_worker_service_account):
    """Authorization header for the compute worker service account."""
    return {"Authorization": f"Bearer {compute_worker_service_account['token']}"}


@pytest.fixture
async def ref_for_load(client, postgres_pool):
    """Create a reference in pending status and clean up after."""
    resp = await client.post(
        "/api/v1/references",
        json={
            "name": f"load-pipeline-{uuid.uuid4()}",
            "version": "1.0",
            "kind": "sequence_reference",
        },
    )
    idx = resp.json()["reference_idx"]
    yield idx
    # Cleanup in FK dependency order
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
def fasta_3seq(tmp_path):
    """Create a 3-sequence FASTA file."""
    path = tmp_path / "test.fasta"
    with open(path, "w") as f:
        for name, seq in TEST_SEQUENCES.items():
            f.write(f">{name}\n{seq}\n")
    return path


@pytest.fixture
def taxonomy_3seq(tmp_path):
    """Create a 3-entry taxonomy Parquet."""
    import duckdb

    path = tmp_path / "taxonomy.parquet"
    with duckdb.connect(":memory:") as conn:
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
def tree_3seq(tmp_path):
    """Create a newick tree with 3 tips."""
    path = tmp_path / "tree.nwk"
    path.write_text("((seq1:0.1,seq2:0.2):0.3,seq3:0.4);")
    return path


async def test_full_load_pipeline(
    client,
    fasta_3seq,
    taxonomy_3seq,
    tree_3seq,
    ref_for_load,
    postgres_pool,
    tmp_path,
    worker_headers,
):
    """Full round-trip: create → hash → mint → load → active → verify."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    ref_idx = ref_for_load
    backend = LocalBackend()

    # --- Hash phase ---
    await client.patch(
        f"/api/v1/references/{ref_idx}/status",
        json={"status": "hashing"},
    )
    hash_dir = tmp_path / "hash_output"
    manifest_path = await backend.run_hash_job(
        fasta_path=fasta_3seq,
        output_dir=hash_dir,
        reference_idx=ref_idx,
    )
    manifest = json.loads(manifest_path.read_text())
    assert len(manifest["entries"]) == 3

    # --- Mint phase ---
    entries = [{"sequence_hash": e["sequence_hash"]} for e in manifest["entries"]]
    mint_resp = await client.post(
        f"/api/v1/references/{ref_idx}/features/mint",
        json={"entries": entries},
        headers=worker_headers,
    )
    assert mint_resp.status_code == 200
    mint_body = mint_resp.json()
    fm_path = tmp_path / "feature_map.ndjson"
    with open(fm_path, "w") as f:
        for k, v in mint_body["mapping"].items():
            f.write(json.dumps({"sequence_hash": k, "feature_idx": v}) + "\n")

    # --- Load phase (status transition + load job) ---
    load_resp = await client.patch(
        f"/api/v1/references/{ref_idx}/status",
        json={"status": "loading"},
    )
    assert load_resp.status_code == 200

    load_dir = tmp_path / "load_output"
    await backend.run_load_job(
        manifest_path=manifest_path,
        fasta_path=fasta_3seq,
        feature_map_path=fm_path,
        output_dir=load_dir,
        reference_idx=ref_idx,
        taxonomy_path=taxonomy_3seq,
        tree_path=tree_3seq,
    )

    # Verify Parquet files exist
    assert (load_dir / "reference_sequences.parquet").exists()
    assert (load_dir / "reference_sequence_chunks.parquet").exists()
    assert (load_dir / "reference_membership.parquet").exists()
    assert (load_dir / "reference_taxonomy.parquet").exists()
    assert (load_dir / "reference_phylogeny.parquet").exists()

    # --- Transition to active ---
    active_resp = await client.patch(
        f"/api/v1/references/{ref_idx}/status",
        json={"status": "active"},
    )
    assert active_resp.status_code == 200
    assert active_resp.json()["status"] == "active"

    # --- Verify DB state ---
    # Reference is active
    status = await postgres_pool.fetchval(
        "SELECT status FROM qiita.references WHERE reference_idx = $1", ref_idx
    )
    assert status == "active"

    # 3 membership rows
    membership_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.reference_membership WHERE reference_idx = $1",
        ref_idx,
    )
    assert membership_count == 3
