"""Integration test: full reference load pipeline.

create → hash → mint → membership → load → active.
Requires Docker Postgres on :5433.
"""

import json
import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_LIBRARY_NAME, URL_REFERENCE_PREFIX, URL_REFERENCE_STATUS


@pytest.fixture
async def client(postgres_pool, hmac_secret, human_admin_session):
    """AsyncClient authenticated as the session admin. Service-only routes
    take a worker token via per-request `headers=worker_headers`. Settings
    are initialised so the library dispatch endpoint resolves; the
    data_plane_url is a stub since this module never dispatches
    register-files."""
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.settings = Settings(
        database_url="unused-in-test",
        hmac_secret_key=hmac_secret,
        data_plane_url="grpc://127.0.0.1:0",
    )
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
        URL_REFERENCE_PREFIX,
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
    await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", idx)


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
    """Full round-trip: hash → minting → mint-features → write-membership →
    loading → load step → active. Status transitions are explicit (the
    orchestrator drives them between primitives — there is no implicit
    transition baked into any single primitive any more)."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    ref_idx = ref_for_load
    backend = LocalBackend()
    status_url = URL_REFERENCE_STATUS.format(reference_idx=ref_idx)

    # --- Hash phase ---
    await client.patch(status_url, json={"status": "hashing"})
    hash_dir = tmp_path / "hash_output"
    hash_result = await backend.run_step(
        "hash", {"fasta_path": fasta_3seq}, hash_dir, reference_idx=ref_idx
    )
    manifest_path = hash_result["manifest"]
    manifest = json.loads(manifest_path.read_text())
    assert len(manifest["entries"]) == 3

    # --- Mint phase ---
    await client.patch(status_url, json={"status": "minting"})
    entries = [{"sequence_hash": e["sequence_hash"]} for e in manifest["entries"]]
    mint_resp = await client.post(
        URL_LIBRARY_NAME.format(name="mint-features"),
        json={
            "scope_target": {"kind": "reference", "reference_idx": ref_idx},
            "inputs": {"entries": entries},
        },
        headers=worker_headers,
    )
    assert mint_resp.status_code == 200, mint_resp.text
    mint_outputs = mint_resp.json()["outputs"]

    # --- Membership phase ---
    feature_idxs = list(mint_outputs["mapping"].values())
    membership_resp = await client.post(
        URL_LIBRARY_NAME.format(name="write-membership"),
        json={
            "scope_target": {"kind": "reference", "reference_idx": ref_idx},
            "inputs": {"feature_idxs": feature_idxs},
        },
        headers=worker_headers,
    )
    assert membership_resp.status_code == 200, membership_resp.text

    # --- Load phase (status transition + load step) ---
    fm_path = tmp_path / "feature_map.ndjson"
    with open(fm_path, "w") as f:
        for k, v in mint_outputs["mapping"].items():
            f.write(json.dumps({"sequence_hash": k, "feature_idx": v}) + "\n")

    load_resp = await client.patch(status_url, json={"status": "loading"})
    assert load_resp.status_code == 200

    load_dir = tmp_path / "load_output"
    await backend.run_step(
        "load",
        {
            "manifest": manifest_path,
            "fasta_path": fasta_3seq,
            "feature_map": fm_path,
            "taxonomy_path": taxonomy_3seq,
            "tree_path": tree_3seq,
        },
        load_dir,
        reference_idx=ref_idx,
    )

    # Verify Parquet files exist
    assert (load_dir / "reference_sequences.parquet").exists()
    assert (load_dir / "reference_sequence_chunks.parquet").exists()
    assert (load_dir / "reference_membership.parquet").exists()
    assert (load_dir / "reference_taxonomy.parquet").exists()
    assert (load_dir / "reference_phylogeny.parquet").exists()

    # --- Transition to active ---
    active_resp = await client.patch(status_url, json={"status": "active"})
    assert active_resp.status_code == 200
    assert active_resp.json()["status"] == "active"

    # --- Verify DB state ---
    status = await postgres_pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1", ref_idx
    )
    assert status == "active"
    membership_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.reference_membership WHERE reference_idx = $1",
        ref_idx,
    )
    assert membership_count == 3
