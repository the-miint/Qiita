"""Integration smoke test: drive the real fastq_to_parquet native step
through the control-plane runner end-to-end.

What's exercised:
  - Deployable YAML at workflows/fastq-to-parquet/1.0.0.yaml is
    materialized under a tmp dir with a unique version stamp, loaded
    via qiita_control_plane.actions.load_actions, and synced into
    qiita.action — the same path `make sync-actions` uses in prod.
  - Submission of a prep_sample-scoped work_ticket (the seeded
    prep_sample has processing_kind='sequenced').
  - Runner reads the action row, walks the single step entry.
  - Runner forwards module= and scope_target= to LocalComputeBackendClient.
  - LocalBackend delegates to run_native_job.
  - flatten_native_inputs merges prep_sample_idx into raw_inputs.
  - run_native_job validates Inputs(fastq_path, prep_sample_idx,
    work_ticket_idx) and invokes the real execute().
  - DuckDB+miint reads the FASTQ fixture and writes reads.parquet.
  - Runner transitions the work_ticket to COMPLETED.

Asserts the Parquet's shape (column names + dtypes + row count), the
duplicate-sequence preservation guarantee, and the FASTQ quality-bytes
round-trip.

The fixture sample.fastq lives at tests/integration/fixtures/sample.fastq
and contains four 20-bp reads, with read_001 and read_003 carrying the
same sequence (ACGTACGTACGTACGTACGT) so the test can prove duplicates
are kept (this is a sample-side ingest, not a reference-side dedup).
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import duckdb
import pytest
from qiita_common.models import WorkTicketState

from _runner_helpers import LocalComputeBackendClient

FIXTURE_FASTQ = Path(__file__).resolve().parent / "fixtures" / "sample.fastq"
_FASTQ_TO_PARQUET_YAML_PATH = (
    Path(__file__).resolve().parents[2]
    / "workflows"
    / "fastq-to-parquet"
    / "1.0.0.yaml"
)


@pytest.fixture
async def fastq_to_parquet_action(postgres_pool, tmp_path):
    """Materialize workflows/fastq-to-parquet/1.0.0.yaml under
    tmp_path/workflows/ with a unique version stamp so parallel
    pytest-xdist workers don't collide on (action_id, version), then
    load it via the same loader prod's `make sync-actions` uses and
    sync into qiita.action."""
    from qiita_control_plane.actions import load_actions, sync_actions

    workflows_dir = tmp_path / "workflows" / "fastq-to-parquet"
    workflows_dir.mkdir(parents=True)
    yaml_text = _FASTQ_TO_PARQUET_YAML_PATH.read_text()
    test_version = f"smoke-{uuid.uuid4()}"
    yaml_text = yaml_text.replace("version: 1.0.0", f"version: {test_version}")
    (workflows_dir / "1.0.0.yaml").write_text(yaml_text)

    actions = load_actions(tmp_path / "workflows")
    async with postgres_pool.acquire() as conn:
        await sync_actions(conn, actions)

    yield ("fastq-to-parquet", test_version)

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        "fastq-to-parquet",
        test_version,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
        "fastq-to-parquet",
        test_version,
    )


@pytest.fixture
async def smoke_prep_sample(postgres_pool, human_admin_session):
    """A minimal qiita.prep_sample row (the supertype introduced by #35)
    with processing_kind='sequenced', to scope the smoke ticket against.
    Seeds the FK chain (biosample + prep_protocol); the sequenced_sample
    1:1 subtype row is intentionally NOT created — fastq_to_parquet
    never reads sequencing-specific columns. Reverse-FK cleanup runs
    on yield exit."""
    admin_idx = human_admin_session["principal_idx"]

    biosample_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.biosample (owner_idx, created_by_idx)"
        " VALUES ($1, $1) RETURNING idx",
        admin_idx,
    )
    prep_protocol_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.prep_protocol (name, created_by_idx)"
        " VALUES ($1, $2) RETURNING idx",
        # prep_protocol_name_format CHECK requires ^[a-z][a-z0-9_]*$
        f"p_{uuid.uuid4().hex[:8]}",
        admin_idx,
    )
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.prep_sample ("
        "  biosample_idx, owner_idx, prep_protocol_idx,"
        "  processing_kind, created_by_idx"
        ") VALUES ($1, $2, $3, 'sequenced', $2) RETURNING idx",
        biosample_idx,
        admin_idx,
        prep_protocol_idx,
    )
    yield idx
    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE prep_sample_idx = $1", idx
    )
    await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", idx)
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_protocol WHERE idx = $1", prep_protocol_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx
    )


async def test_fastq_to_parquet_through_runner(
    postgres_pool,
    fastq_to_parquet_action,
    smoke_prep_sample,
    human_admin_session,
    tmp_path,
):
    """End-to-end: a prep_sample-scoped ticket (processing_kind='sequenced')
    runs fastq_to_parquet against the checked-in FASTQ fixture and
    completes. Asserts the Parquet's column schema and the duplicate-
    sequence preservation guarantee."""
    from qiita_control_plane.runner import run_workflow

    action_id, action_version = fastq_to_parquet_action
    prep_sample_idx = smoke_prep_sample

    work_ticket_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, prep_sample_idx, action_context"
        ") VALUES ($1, $2, $3, 'prep_sample', $4, $5::jsonb)"
        " RETURNING work_ticket_idx",
        action_id,
        action_version,
        human_admin_session["principal_idx"],
        prep_sample_idx,
        json.dumps({"fastq_path": str(FIXTURE_FASTQ)}),
    )

    workspace_root = tmp_path / "workspace"
    backend_client = LocalComputeBackendClient()

    await run_workflow(
        work_ticket_idx,
        postgres_pool,
        backend_client,  # type: ignore[arg-type]  # protocol-shaped duck
        hmac_secret=b"unused-in-smoke",
        data_plane_url="grpc://unused:0",
        workspace_root=workspace_root,
    )

    row = await postgres_pool.fetchrow(
        "SELECT state, failure_type, failure_stage, failure_reason"
        " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert row["state"] == WorkTicketState.COMPLETED.value
    # COMPLETED tickets carry no failure_* (DB CHECK enforces).
    assert row["failure_type"] is None
    assert row["failure_stage"] is None
    assert row["failure_reason"] is None

    # The runner places each step's outputs in
    # <workspace_root>/<work_ticket_idx>/<step_name>/attempt-0/. fastq is
    # the YAML step name; this is the SINGLETON-attempt-0 path.
    reads_parquet = (
        workspace_root / str(work_ticket_idx) / "fastq" / "attempt-0" / "reads.parquet"
    )
    assert reads_parquet.exists(), f"expected reads.parquet at {reads_parquet}"

    # Verify the Parquet's schema and content.
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            "SELECT column_name, column_type FROM ("
            f" DESCRIBE SELECT * FROM '{reads_parquet}')"
        ).fetchall()
        # DuckDB DESCRIBE column order matches the Parquet's physical order.
        # `quality` is miint's native UTINYINT[] (phred-decoded scores), not
        # the FASTQ ASCII string — see fastq_to_parquet.py module docstring.
        # Schema is deliberately pre-DuckLake: no CP-minted identifier
        # columns, no content hash. When sample_reads registration lands
        # the schema and sort change together.
        assert rows == [
            ("read_id", "VARCHAR"),
            ("sequence", "VARCHAR"),
            ("quality", "UTINYINT[]"),
            ("sequence_length", "BIGINT"),
        ]

        # Fixture has 4 reads — no dedup, each read produces a row even
        # when sequences match (read_001 and read_003 carry the same
        # sequence; both appear).
        records = conn.execute(
            "SELECT read_id, sequence, quality, sequence_length"
            f" FROM '{reads_parquet}' ORDER BY read_id"
        ).fetchall()
    assert len(records) == 4

    by_read_id = {r[0]: r for r in records}
    # read_001 and read_003 share the sequence ACGTACGTACGTACGTACGT —
    # confirms duplicates are preserved (this is a sample-side ingest,
    # not a reference-side dedup).
    assert by_read_id["read_001"][1] == "ACGTACGTACGTACGTACGT"
    assert by_read_id["read_003"][1] == "ACGTACGTACGTACGTACGT"
    # All sequence_lengths are 20 (each read in the fixture is 20 bp).
    assert {r[3] for r in records} == {20}

    # Quality bytes round-trip from the FASTQ — read_001's quality is
    # twenty `!` characters (ASCII 33), and miint returns phred-decoded
    # scores so each entry is 33 - 33 = 0. Confirms the FASTA-vs-FASTQ
    # branch on read_fastx populates the qual1 column for FASTQ inputs
    # and that the decoding offset is applied.
    assert by_read_id["read_001"][2] == [0] * 20
