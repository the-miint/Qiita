"""System test: GG2 2024.09 backbone — full reference-add pipeline on real data.

Exercises the path-based primitives at scale:
  - 331K sequences hashed → manifest.parquet
  - mint-features with genome_map_path (~72K genome associations on top
    of the 331K feature mints)
  - write-membership over the full feature set
  - load step writes reference_sequences, reference_sequence_chunks,
    reference_membership, reference_taxonomy, reference_phylogeny Parquet
  - register-files via real Arrow Flight DoAction (data plane subprocess)
  - DoGet round-trip verifying expected row counts: 331269 sequences,
    331240 taxonomy entries, 662537 phylogeny nodes (tips + internals)

Run via: ``make test-system``. Gated behind ``pytest.mark.system`` (not
in CI's integration matrix) and a ``FASTA.exists()`` skip so it no-ops
gracefully when GG2 isn't on the host.

Expected runtime: ~10 minutes (FASTA hashing dominates).
"""

import base64
import uuid
from pathlib import Path

import duckdb
import pyarrow.flight as flight
import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import LOOPBACK_HOST, URL_LIBRARY_NAME, LibraryPrimitive

DATA_DIR = Path(__file__).parent.parent.parent / "localdocs" / "scratch"
FASTA = DATA_DIR / "2024.09.backbone.sequence.fna.gz"
TREE = DATA_DIR / "2024.09.backbone.nwk.gz"
TAXONOMY = DATA_DIR / "2024.09.backbone.taxonomy.parquet"
GG2_GENOME_MAP = DATA_DIR / "2024.09.backbone.feature-to-genome.parquet"

# Locked-in counts for the GG2 2024.09 backbone — drift in any of these
# is either real data corruption upstream or a regression in our
# pipeline; either way it should fail loudly.
_EXPECTED_FEATURES = 331269
_EXPECTED_TAXONOMY = 331240
_EXPECTED_PHYLOGENY = 662537

pytestmark = [
    pytest.mark.system,
    pytest.mark.skipif(not FASTA.exists(), reason="GG2 data not in localdocs/scratch/"),
]


@pytest.fixture
def gg2_genome_map(tmp_path):
    """Convert GG2's `(feature_id, genome_id)` Parquet to the path-based
    schema `(read_id, genome_source, genome_source_id)` that the new
    `mint_features` JOINs against the manifest's read_id.

    GG2 doesn't carry a genome_source column (every entry is implicitly
    sourced from GG2 itself), so we hardcode it here. Other corpora that
    write a self-describing genome_source column won't need this fixture.
    """
    out = tmp_path / "gg2_genome_map.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "COPY ("
            "  SELECT feature_id AS read_id,"
            "         'gg2' AS genome_source,"
            "         genome_id AS genome_source_id"
            "  FROM read_parquet(?) WHERE genome_id IS NOT NULL"
            f") TO '{out}' (FORMAT PARQUET)",
            [str(GG2_GENOME_MAP)],
        )
    return out


@pytest.fixture
def flight_client(data_plane):
    client = flight.FlightClient(f"grpc://{LOOPBACK_HOST}:{data_plane['port']}")
    yield client
    client.close()


@pytest.fixture
async def client(postgres_pool, hmac_secret, data_plane, human_admin_session):
    """In-process control-plane app via ASGITransport, wired to the real
    data_plane subprocess. Default Authorization is the session admin;
    service-only routes use ``worker_headers`` to override."""
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.settings = Settings(
        database_url="unused-in-test",
        hmac_secret_key=hmac_secret,
        data_plane_url=f"grpc://{LOOPBACK_HOST}:{data_plane['port']}",
    )
    # 600s timeout — mint of 331K features serialises ~33 chunked Postgres
    # transactions; full pipeline runs minutes.
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        timeout=600,
        headers={"Authorization": f"Bearer {human_admin_session['token']}"},
    ) as ac:
        yield ac


@pytest.fixture
def worker_headers(compute_worker_service_account):
    return {"Authorization": f"Bearer {compute_worker_service_account['token']}"}


@pytest.fixture
async def gg2_reference(client, postgres_pool):
    """Fresh reference for the system run; cleans up everything pointing
    at it before dropping the row."""
    resp = await client.post(
        "/api/v1/reference",
        json={
            "name": f"gg2-backbone-{uuid.uuid4()}",
            "version": "2024.09",
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


async def test_gg2_backbone_full_pipeline(
    client,
    data_plane,
    flight_client,
    gg2_reference,
    gg2_genome_map,
    worker_headers,
    postgres_pool,
    tmp_path,
):
    """Full reference-add pipeline on GG2 2024.09: create → hash → mint
    (with genome associations) → write_membership → load → register →
    DoGet. Asserts expected row counts at each verifiable stage and
    confirms genome_map_path produced the expected feature_genome rows.
    """
    from qiita_compute_orchestrator.backends.local import LocalBackend

    ref_idx = gg2_reference
    backend = LocalBackend()
    workspace = tmp_path / "workspace"
    scope_target = {"kind": "reference", "reference_idx": ref_idx}

    # --- Hash (reads 11 GB FASTA, ~7 min) ---
    await client.patch(f"/api/v1/reference/{ref_idx}/status", json={"status": "hashing"})
    hash_result = await backend.run_step(
        "hash", {"fasta_path": FASTA}, workspace, reference_idx=ref_idx
    )
    manifest_path = hash_result["manifest"]

    # --- Mint with genome_map_path ---
    await client.patch(f"/api/v1/reference/{ref_idx}/status", json={"status": "minting"})
    mint_resp = await client.post(
        URL_LIBRARY_NAME.format(name=LibraryPrimitive.MINT_FEATURES),
        json={
            "scope_target": scope_target,
            "inputs": {
                "manifest_path": str(manifest_path),
                "output_dir": str(workspace),
                "genome_map_path": str(gg2_genome_map),
            },
        },
        headers=worker_headers,
    )
    assert mint_resp.status_code == 200, mint_resp.text
    mint_outputs = mint_resp.json()["outputs"]
    assert mint_outputs["minted"] == _EXPECTED_FEATURES
    feature_map_path = Path(mint_outputs["feature_map_path"])

    # --- Membership ---
    membership_resp = await client.post(
        URL_LIBRARY_NAME.format(name=LibraryPrimitive.WRITE_MEMBERSHIP),
        json={
            "scope_target": scope_target,
            "inputs": {"feature_map_path": str(feature_map_path)},
        },
        headers=worker_headers,
    )
    assert membership_resp.status_code == 200, membership_resp.text
    assert membership_resp.json()["outputs"]["linked"] == _EXPECTED_FEATURES

    # --- Genome associations: every row in the converted genome map
    # should have produced exactly one feature_genome row scoped to this
    # reference's feature set.
    expected_genome_count = (
        duckdb.connect(":memory:")
        .execute("SELECT count(*) FROM read_parquet(?)", [str(gg2_genome_map)])
        .fetchone()[0]
    )
    actual_genome_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.feature_genome fg"
        " JOIN qiita.genome g USING (genome_idx)"
        " JOIN qiita.reference_membership m ON m.feature_idx = fg.feature_idx"
        " WHERE m.reference_idx = $1 AND g.source = 'gg2'",
        ref_idx,
    )
    assert actual_genome_count == expected_genome_count

    # --- Load (writes sequences + chunks + membership + taxonomy + phylogeny) ---
    await client.patch(f"/api/v1/reference/{ref_idx}/status", json={"status": "loading"})
    staging = tmp_path / "staging"
    await backend.run_step(
        "load",
        {
            "manifest": manifest_path,
            "fasta_path": FASTA,
            "feature_map": feature_map_path,
            "taxonomy_path": TAXONOMY,
            "tree_path": TREE,
        },
        staging,
        reference_idx=ref_idx,
    )

    # --- Register via real Flight DoAction ---
    reg_resp = await client.post(
        URL_LIBRARY_NAME.format(name=LibraryPrimitive.REGISTER_FILES),
        json={
            "scope_target": scope_target,
            "inputs": {
                "staging_dir": str(staging),
                "files": {
                    "reference_sequences.parquet": "reference_sequences",
                    "reference_sequence_chunks.parquet": "reference_sequence_chunks",
                    "reference_membership.parquet": "reference_membership",
                    "reference_taxonomy.parquet": "reference_taxonomy",
                    "reference_phylogeny.parquet": "reference_phylogeny",
                },
            },
        },
        headers=worker_headers,
    )
    assert reg_resp.status_code == 200, reg_resp.text
    assert len(reg_resp.json()["outputs"]["registered"]) == 5

    await client.patch(f"/api/v1/reference/{ref_idx}/status", json={"status": "active"})

    # --- DoGet: sequence metadata ---
    seq_table = await _doget(client, flight_client, ref_idx, "reference_sequences", worker_headers)
    assert seq_table.num_rows == _EXPECTED_FEATURES
    assert {"feature_idx", "sequence_hash", "sequence_length_bp"} <= set(seq_table.column_names)

    # --- DoGet: taxonomy ---
    tax_table = await _doget(client, flight_client, ref_idx, "reference_taxonomy", worker_headers)
    assert tax_table.num_rows == _EXPECTED_TAXONOMY
    assert "domain" in tax_table.column_names

    # --- DoGet: phylogeny (tips + internal nodes) ---
    phylo_table = await _doget(
        client, flight_client, ref_idx, "reference_phylogeny", worker_headers
    )
    assert phylo_table.num_rows == _EXPECTED_PHYLOGENY
    assert "feature_idx" in phylo_table.column_names


async def _doget(client, flight_client, ref_idx, table, headers):
    resp = await client.post(
        f"/api/v1/reference/{ref_idx}/ticket/doget",
        json={"table": table},
        headers=headers,
    )
    assert resp.status_code == 201, resp.text
    ticket_bytes = base64.b64decode(resp.json()["ticket"])
    return flight_client.do_get(flight.Ticket(ticket_bytes)).read_all()
