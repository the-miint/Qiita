"""End-to-end integration test: drive workflows/reference-add via the CLI's
`qiita reference load` programmatic entry point against real CP +
real data-plane Flight + DuckLake, then DoGet the registered Parquet
rows back.

Exercises every layer of the production path:
  * CLI (`cli.reference_load.do_reference_load`) — Arrow conversion,
    POST /upload + Flight DoPut, POST /upload/{idx}/done, POST /work-ticket.
  * Control plane (route layer + runner upload resolution +
    `_consume_upload_handles` + LIBRARY primitives + status transitions).
  * Compute orchestrator (LocalBackend in-process via
    LocalComputeBackendClient).
  * Data plane (Flight DoPut for upload, DoAction for register, DoGet
    for verification).
  * DuckLake (Parquet registration via the data plane).

Differs from the legacy direct-INSERT version: the work_ticket flows
through POST /work-ticket → schedule_dispatch → background asyncio task
running `run_workflow`. The test awaits completion via the CLI's
work_ticket-poll loop (short poll interval).

Shared fixtures: `data_plane`, `signing_key`, `postgres_pool`,
`human_admin_session` live in conftest.py.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import duckdb
import pyarrow.flight as flight
import pytest
from httpx import ASGITransport, AsyncClient

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
    and clean the row up after.

    Tests don't share the on-disk version_pin (1.0.0) because parallel
    sessions would step on each other's action row; each run synthesizes a
    unique version suffix and the CLI submits against that. But the CLI
    hard-codes `action_version="1.0.0"` — so we keep that pin and accept
    the parallel-session collision risk (the integration suite serializes
    in pytest by default)."""
    from qiita_control_plane.actions import load_actions, sync_actions

    workflows_dir = tmp_path / "workflows" / "reference-add"
    workflows_dir.mkdir(parents=True)
    (workflows_dir / "1.0.0.yaml").write_text(_REFERENCE_ADD_YAML_PATH.read_text())

    actions = load_actions(tmp_path / "workflows")
    async with postgres_pool.acquire() as conn:
        await sync_actions(conn, actions)

    yield ("reference-add", "1.0.0")

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        "reference-add",
        "1.0.0",
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
        "reference-add",
        "1.0.0",
    )


@pytest.fixture
async def fresh_reference(postgres_pool, human_admin_session):
    """Create a reference at status='pending'. The CLI's `--reference-idx`
    binds to this row instead of creating its own — keeps the test's
    cleanup scoped to a single row."""
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
def tree_e2e(tmp_path):
    """Newick tree whose tip names match the FASTA read_ids — load step
    populates feature_idx on tip nodes via the read_id → feature_idx join."""
    path = tmp_path / "tree.nwk"
    path.write_text("((seq1:0.1,seq2:0.2):0.3,seq3:0.4);")
    return path


@pytest.fixture
async def cli_cp_client(postgres_pool, signing_key, human_admin_session, data_plane):
    """Configure cp_app.state for dispatch — pool + settings (with the
    data plane's actual gRPC URL and the spawned PATH_SCRATCH/staging) +
    LocalComputeBackendClient + dispatch task tracking. Yield an
    httpx.AsyncClient over ASGITransport with the admin PAT header."""
    from qiita_common.api_paths import LOOPBACK_HOST
    from qiita_control_plane.config import Settings as CPSettings
    from qiita_control_plane.main import app as cp_app

    cp_app.state.pool = postgres_pool
    cp_app.state.settings = CPSettings(
        database_url="unused-in-test",
        flight_signing_key=signing_key,
        data_plane_url=f"grpc://{LOOPBACK_HOST}:{data_plane['port']}",
        path_scratch_staging=Path(data_plane["upload_staging_root"]),
        path_scratch_ticket=Path(data_plane["workspace_root"]),
    )
    cp_app.state.compute_backend_client = LocalComputeBackendClient()
    cp_app.state.running_dispatches = set()

    async with AsyncClient(
        transport=ASGITransport(app=cp_app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {human_admin_session['token']}"},
    ) as client:
        yield client

    # Drain any in-flight dispatch tasks so a flaky workflow can't leak
    # across tests.
    import asyncio

    pending = list(cp_app.state.running_dispatches)
    if pending:
        _, leftover = await asyncio.wait(pending, timeout=5)
        for task in leftover:
            task.cancel()


async def test_e2e_create_to_doget(
    postgres_pool,
    data_plane,
    signing_key,
    flight_client,
    synced_reference_add_action,
    fresh_reference,
    fasta_e2e,
    taxonomy_e2e,
    tree_e2e,
    human_admin_session,
    cli_cp_client,
    tmp_path,
):
    """Drive the full production path via `do_reference_load`:
    POST /reference (skipped — we bind to fresh_reference) →
    POST /upload + Flight DoPut + POST /done for each input file →
    POST /work-ticket → schedule_dispatch fires runner in background →
    runner resolves upload handles, walks workflow, registers files →
    --watch polls /work-ticket/{idx} until completed →
    DoGet round-trips sequences / chunks / taxonomy / phylogeny.
    """
    from qiita_control_plane.auth.tickets import sign_ticket
    from qiita_control_plane.cli.reference_load import do_reference_load

    result = await do_reference_load(
        http=cli_cp_client,
        token=human_admin_session["token"],
        flight_client=flight_client,
        fasta_path=fasta_e2e,
        taxonomy_path=taxonomy_e2e,
        tree_path=tree_e2e,
        reference_idx=fresh_reference,
        watch=True,
        poll_interval_seconds=0.1,
        timeout_seconds=60,
    )
    assert result["work_ticket"]["state"] == "completed", result["work_ticket"]

    # Both terminal checks — same as the legacy assertions, but reached
    # via the production HTTP path instead of direct DB INSERT.
    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        result["work_ticket_idx"],
    )
    assert state == "completed"
    ref_status = await postgres_pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1",
        fresh_reference,
    )
    assert ref_status == "active"

    # The three uploads transitioned ready → consumed inside the runner's
    # finalize transaction.
    consumed = await postgres_pool.fetch(
        "SELECT upload_idx, status FROM qiita.upload WHERE upload_idx = ANY($1::bigint[])",
        list(result["upload_idxs"].values()),
    )
    assert all(row["status"] == "consumed" for row in consumed), consumed

    # DoGet round-trip via the data plane — confirms register-files ran
    # and the DuckLake catalog carries the new reference's rows.
    def _doget(table_name: str):
        ticket_bytes = sign_ticket(
            table=table_name,
            filter={"reference_idx": [fresh_reference]},
            secret=signing_key,
        )
        return flight_client.do_get(flight.Ticket(ticket_bytes)).read_all()

    table = _doget("reference_sequences")
    assert table.num_rows == 3
    assert {"feature_idx", "sequence_hash", "sequence_length_bp"}.issubset(
        set(table.column_names)
    )

    chunks = _doget("reference_sequence_chunks")
    assert chunks.num_rows == 3
    # Sequences come back in canonical form (LEAST of strand + revcomp)
    # — for the three short fixture sequences each is already canonical.
    canon_seqs = set()
    for seq in _TEST_SEQUENCES.values():
        rc = seq.translate(str.maketrans("ACGTRYSWKMBDHVN", "TGCAYRSWMKVHDBN"))[
            ::-1
        ].upper()
        canon_seqs.add(min(seq.upper(), rc))
    assert set(chunks.column("chunk_data").to_pylist()) == canon_seqs

    tax = _doget("reference_taxonomy")
    assert tax.num_rows == 3
    assert set(tax.column("domain").to_pylist()) == {"Bacteria", "Archaea"}

    phylo = _doget("reference_phylogeny")
    tip_rows = [r for r in phylo.to_pylist() if r["is_tip"]]
    assert len(tip_rows) == 3
    assert all(r["feature_idx"] is not None for r in tip_rows)


async def test_ticket_endpoint_rejects_non_active_reference(
    postgres_pool, signing_key, fresh_reference, compute_worker_service_account
):
    """Ticket route guard still works — reference at status='pending' refuses."""
    from qiita_common.api_paths import LOOPBACK_HOST, URL_REFERENCE_DOGET
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.settings = Settings(
        database_url="unused-in-test",
        flight_signing_key=signing_key,
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
