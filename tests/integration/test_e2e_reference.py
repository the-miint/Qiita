"""End-to-end integration test: drive workflows/reference-add via the
runner with real LocalBackend + real LIBRARY + real data plane Flight,
then DoGet the registered Parquet rows back via Arrow Flight.

Exercises every component:
  * control plane (runner, LIBRARY primitives, status transitions)
  * orchestrator-equivalent (LocalBackend in-process)
  * data plane (Arrow Flight DoAction for register, DoGet for read)
  * DuckLake (Parquet registration via the data plane)

Relies on the shared `data_plane`, `hmac_secret`, and `postgres_pool`
fixtures in conftest.py — no per-module process/secret/schema plumbing
lives here.
"""

import uuid
from pathlib import Path

import pyarrow.flight as flight
import pytest

from _runner_helpers import LocalComputeBackendClient

_REFERENCE_ADD_YAML_PATH = (
    Path(__file__).parent.parent.parent / "workflows" / "reference-add" / "1.0.0.yaml"
)


@pytest.fixture
def flight_client(data_plane):
    from qiita_common.api_paths import LOOPBACK_HOST

    client = flight.FlightClient(f"grpc://{LOOPBACK_HOST}:{data_plane['port']}")
    yield client
    client.close()


@pytest.fixture
async def synced_reference_add_action(postgres_pool, tmp_path):
    """Materialize workflows/reference-add/1.0.0.yaml under tmp_path/workflows/
    so the loader's directory walk picks it up, sync it into qiita.action,
    and clean the row up after."""
    from qiita_control_plane.actions import load_actions, sync_actions

    workflows_dir = tmp_path / "workflows" / "reference-add"
    workflows_dir.mkdir(parents=True)
    yaml_text = _REFERENCE_ADD_YAML_PATH.read_text()
    test_version = f"e2e-{uuid.uuid4()}"
    yaml_text = yaml_text.replace("version: 1.0.0", f"version: {test_version}")
    (workflows_dir / "1.0.0.yaml").write_text(yaml_text)

    actions = load_actions(tmp_path / "workflows")
    async with postgres_pool.acquire() as conn:
        await sync_actions(conn, actions)

    yield ("reference-add", test_version)

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        "reference-add",
        test_version,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
        "reference-add",
        test_version,
    )


@pytest.fixture
async def fresh_reference(postgres_pool, human_admin_session):
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', 'pending', $2)"
        " RETURNING reference_idx",
        f"e2e-{uuid.uuid4()}",
        human_admin_session["principal_idx"],
    )
    yield idx
    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE reference_idx = $1", idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = $1", idx
    )


_TEST_SEQUENCES = {
    "seq1": "ATCGATCGATCG",
    "seq2": "GCTAGCTAGCTA",
    "seq3": "AAATTTTCCCGGG",
}


@pytest.fixture
def fasta_e2e(tmp_path):
    path = tmp_path / "test.fasta"
    with open(path, "w") as f:
        for name, seq in _TEST_SEQUENCES.items():
            f.write(f">{name}\n{seq}\n")
    return path


@pytest.fixture
def taxonomy_e2e(tmp_path):
    """Parquet with (feature_id, taxonomy) — feature_id matches FASTA read_ids."""
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
    """Newick tree whose tip names match the FASTA read_ids — load step
    populates feature_idx on tip nodes via the read_id → feature_idx join."""
    path = tmp_path / "tree.nwk"
    path.write_text("((seq1:0.1,seq2:0.2):0.3,seq3:0.4);")
    return path


async def test_e2e_create_to_doget(
    postgres_pool,
    data_plane,
    hmac_secret,
    flight_client,
    synced_reference_add_action,
    fresh_reference,
    fasta_e2e,
    taxonomy_e2e,
    tree_e2e,
    human_admin_session,
    tmp_path,
):
    """Full E2E: runner walks reference-add (driving optional taxonomy +
    tree inputs through action_context) → register Parquet via Flight →
    DoGet round-trips sequences, chunks, taxonomy, and phylogeny.
    """
    import json as _json

    from qiita_common.api_paths import LOOPBACK_HOST
    from qiita_control_plane.auth.tickets import sign_ticket
    from qiita_control_plane.runner import run_workflow

    action_id, action_version = synced_reference_add_action
    action_context = _json.dumps(
        {
            "fasta_path": str(fasta_e2e),
            "taxonomy_path": str(taxonomy_e2e),
            "tree_path": str(tree_e2e),
        }
    )
    work_ticket_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, action_context"
        ") VALUES ($1, $2, $3, 'reference', $4, $5::jsonb)"
        " RETURNING work_ticket_idx",
        action_id,
        action_version,
        human_admin_session["principal_idx"],
        fresh_reference,
        action_context,
    )

    await run_workflow(
        work_ticket_idx,
        postgres_pool,
        LocalComputeBackendClient(),  # type: ignore[arg-type]
        hmac_secret=hmac_secret,
        data_plane_url=f"grpc://{LOOPBACK_HOST}:{data_plane['port']}",
        workspace_root=tmp_path / "workspace",
    )

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert state == "completed"

    ref_status = await postgres_pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1",
        fresh_reference,
    )
    assert ref_status == "active"

    def _doget(table_name: str):
        ticket_bytes = sign_ticket(
            table=table_name,
            filter={"reference_idx": [fresh_reference]},
            secret=hmac_secret,
        )
        return flight_client.do_get(flight.Ticket(ticket_bytes)).read_all()

    # reference_sequences round-trip via Flight.
    table = _doget("reference_sequences")
    assert table.num_rows == 3
    assert {"feature_idx", "sequence_hash", "sequence_length_bp"}.issubset(
        set(table.column_names)
    )

    # reference_sequence_chunks — sequences come back intact.
    chunks = _doget("reference_sequence_chunks")
    assert chunks.num_rows == 3
    assert set(chunks.column("chunk_data").to_pylist()) == set(_TEST_SEQUENCES.values())

    # reference_taxonomy — domains parsed correctly from the optional input.
    tax = _doget("reference_taxonomy")
    assert tax.num_rows == 3
    assert set(tax.column("domain").to_pylist()) == {"Bacteria", "Archaea"}

    # reference_phylogeny — Newick decomposed into nodes; the 3 tips carry
    # the feature_idx values minted from the matching FASTA read_ids.
    phylo = _doget("reference_phylogeny")
    tip_rows = [r for r in phylo.to_pylist() if r["is_tip"]]
    assert len(tip_rows) == 3
    assert all(r["feature_idx"] is not None for r in tip_rows)


async def test_ticket_endpoint_rejects_non_active_reference(
    postgres_pool, hmac_secret, fresh_reference, compute_worker_service_account
):
    """Ticket route guard still works — reference at status='pending' refuses."""
    from httpx import ASGITransport, AsyncClient
    from qiita_common.api_paths import LOOPBACK_HOST, URL_REFERENCE_DOGET
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.settings = Settings(
        database_url="unused-in-test",
        hmac_secret_key=hmac_secret,
        data_plane_url=f"grpc://{LOOPBACK_HOST}:0",
    )

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {compute_worker_service_account['token']}"},
    ) as ac:
        resp = await ac.post(
            URL_REFERENCE_DOGET.format(reference_idx=fresh_reference),
            json={"table": "reference_sequences"},
        )
    assert resp.status_code == 409
