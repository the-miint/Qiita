"""Integration test: full reference load pipeline.

create → hash → mint → load → register DuckLake → phylogeny tips → active.
Requires Docker Postgres on :5433.
"""

import hashlib
import json
import uuid

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture
async def client(postgres_pool):
    """AsyncClient backed by the integration test pool."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        yield ac


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
def fasta_3seq(tmp_path):
    """Create a 3-sequence FASTA file."""
    path = tmp_path / "test.fasta"
    with open(path, "w") as f:
        for name, seq in TEST_SEQUENCES.items():
            f.write(f">{name}\n{seq}\n")
    return path


@pytest.fixture
def taxonomy_3seq(tmp_path):
    """Create a 3-entry taxonomy TSV."""
    path = tmp_path / "taxonomy.tsv"
    path.write_text(
        "Feature ID\tTaxon\n"
        "seq1\td__Bacteria; p__Bacillota; c__Bacilli; o__; f__; g__; s__\n"
        "seq2\td__Bacteria; p__Pseudomonadota; c__; o__; f__; g__; s__\n"
        "seq3\td__Archaea; p__Euryarchaeota; c__; o__; f__; g__; s__\n"
    )
    return path


@pytest.fixture
def tree_3seq(tmp_path):
    """Create a newick tree with 3 tips."""
    path = tmp_path / "tree.nwk"
    path.write_text("((seq1:0.1,seq2:0.2):0.3,seq3:0.4);")
    return path


async def test_full_load_pipeline(
    client, fasta_3seq, taxonomy_3seq, tree_3seq, ref_for_load, postgres_pool, tmp_path
):
    """Full round-trip: create → hash → mint → load → tips → active → verify."""
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
    )
    assert mint_resp.status_code == 200
    mint_body = mint_resp.json()
    feature_map = {uuid.UUID(k): v for k, v in mint_body["mapping"].items()}

    # --- Load phase (status transition + load job + tips) ---
    load_resp = await client.patch(
        f"/api/v1/references/{ref_idx}/status",
        json={"status": "loading"},
    )
    assert load_resp.status_code == 200

    load_dir = tmp_path / "load_output"
    await backend.run_load_job(
        manifest_path=manifest_path,
        fasta_path=fasta_3seq,
        feature_map=feature_map,
        output_dir=load_dir,
        reference_idx=ref_idx,
        taxonomy_path=taxonomy_3seq,
        tree_path=tree_3seq,
    )

    # Verify Parquet files exist
    assert (load_dir / "reference_sequences.parquet").exists()
    assert (load_dir / "reference_taxonomy.parquet").exists()
    assert (load_dir / "reference_phylogeny.parquet").exists()
    assert (load_dir / "tip_features.json").exists()

    # --- Post tip features ---
    tips = json.loads((load_dir / "tip_features.json").read_text())
    tip_resp = await client.post(
        f"/api/v1/references/{ref_idx}/phylogeny-tips",
        json={"entries": tips},
    )
    assert tip_resp.status_code == 201
    assert tip_resp.json()["inserted"] == 3

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

    # 3 tip-feature rows
    tip_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.phylogeny_tip_feature WHERE reference_idx = $1",
        ref_idx,
    )
    assert tip_count == 3

    # Tip feature_idx values must be valid features
    tip_features = await postgres_pool.fetch(
        "SELECT feature_idx FROM qiita.phylogeny_tip_feature WHERE reference_idx = $1",
        ref_idx,
    )
    tip_fidxs = {r["feature_idx"] for r in tip_features}
    assert tip_fidxs == set(feature_map.values())


async def test_phylogeny_tips_rejects_wrong_status(client, ref_for_load):
    """POST phylogeny-tips must reject references not in 'loading' status."""
    ref_idx = ref_for_load
    # Reference is in 'pending' — should reject
    resp = await client.post(
        f"/api/v1/references/{ref_idx}/phylogeny-tips",
        json={
            "entries": [
                {"reference_idx": ref_idx, "node_index": 0, "feature_idx": 1},
            ]
        },
    )
    assert resp.status_code == 409


async def test_phylogeny_tips_idempotent(
    client, ref_for_load, postgres_pool, fasta_3seq, tree_3seq, tmp_path
):
    """Resubmitting the same tips must succeed (ON CONFLICT DO NOTHING)."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    ref_idx = ref_for_load
    backend = LocalBackend()

    # Hash + mint
    await client.patch(
        f"/api/v1/references/{ref_idx}/status", json={"status": "hashing"}
    )
    manifest_path = await backend.run_hash_job(
        fasta_path=fasta_3seq, output_dir=tmp_path / "h", reference_idx=ref_idx
    )
    manifest = json.loads(manifest_path.read_text())
    entries = [{"sequence_hash": e["sequence_hash"]} for e in manifest["entries"]]
    mint_resp = await client.post(
        f"/api/v1/references/{ref_idx}/features/mint", json={"entries": entries}
    )
    feature_map = {uuid.UUID(k): v for k, v in mint_resp.json()["mapping"].items()}

    # Load + tips
    await client.patch(
        f"/api/v1/references/{ref_idx}/status", json={"status": "loading"}
    )
    load_dir = tmp_path / "l"
    await backend.run_load_job(
        manifest_path=manifest_path,
        fasta_path=fasta_3seq,
        feature_map=feature_map,
        output_dir=load_dir,
        reference_idx=ref_idx,
        tree_path=tree_3seq,
    )
    tips = json.loads((load_dir / "tip_features.json").read_text())

    # Post tips twice — second should succeed with 0 inserted
    resp1 = await client.post(
        f"/api/v1/references/{ref_idx}/phylogeny-tips", json={"entries": tips}
    )
    assert resp1.status_code == 201
    assert resp1.json()["inserted"] == 3

    resp2 = await client.post(
        f"/api/v1/references/{ref_idx}/phylogeny-tips", json={"entries": tips}
    )
    assert resp2.status_code == 201
    assert resp2.json()["inserted"] == 0
