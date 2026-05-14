"""Integration smoke test: drive the real fastq_to_parquet native step
through the control-plane runner end-to-end.

What's exercised:
  - Inline ActionDefinition for `fastq-to-parquet`, target_kind
    `sequenced_sample`, single step `module:` set to the real native
    job → synced into qiita.action.
  - Submission of a sequenced_sample-scoped work_ticket.
  - Runner reads the action row, walks the single step entry.
  - Runner forwards module= and scope_target= to LocalComputeBackendClient.
  - LocalBackend delegates to run_native_job.
  - flatten_native_inputs merges sequenced_sample_idx into raw_inputs.
  - run_native_job validates Inputs(fastq_path, sequenced_sample_idx,
    work_ticket_idx) and invokes the real execute().
  - DuckDB+miint reads the FASTQ fixture and writes reads.parquet.
  - Runner transitions the work_ticket to COMPLETED.

Asserts the Parquet's shape (column names + dtypes + row count) plus a
known-sequence sequence_hash so the md5→UUID path is covered for a
specific deterministic value — not just "some hash appeared".

The fixture sample.fastq lives at tests/integration/fixtures/sample.fastq
and contains four 20-bp reads, with read_001 and read_003 carrying the
same sequence (ACGTACGTACGTACGTACGT). The expected sequence_hash is
computed via Python's hashlib so a future miint version that returns a
different md5 implementation surfaces as a mismatch.

When commit 7 lands the deployable workflows/fastq-to-parquet/1.0.0.yaml,
this test switches off the inline ActionDefinition and loads from disk
via sync_actions.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import timedelta
from pathlib import Path
from uuid import UUID

import duckdb
import pytest
from qiita_common.actions import (
    ActionCeiling,
    ActionDefinition,
    Audience,
)
from qiita_common.models import (
    ScopeTargetKind,
    StepType,
    WorkTicketState,
)
from qiita_common.testing.native_steps import FASTQ_TO_PARQUET_MODULE

from _runner_helpers import LocalComputeBackendClient

FIXTURE_FASTQ = Path(__file__).resolve().parent / "fixtures" / "sample.fastq"


def _build_fastq_to_parquet_action(*, action_id: str, version: str) -> ActionDefinition:
    """Construct an ActionDefinition matching the deployable YAML that
    commit 7 lands. Single step, native module, sequenced_sample-scoped.
    The action_ceiling matches the YAML's; the step's baseline_resources
    are intentionally small so the test doesn't depend on real
    scheduler limits."""
    return ActionDefinition(
        action_id=action_id,
        version=version,
        target_kind=ScopeTargetKind.SEQUENCED_SAMPLE,
        scopes=[],
        audience=Audience(service=False, human_roles=["system_admin"]),
        context_schema={
            "type": "object",
            "required": ["fastq_path"],
            "properties": {"fastq_path": {"type": "string"}},
        },
        steps=[
            {
                "kind": "step",
                "name": "fastq",
                "step_type": StepType.SINGLETON,
                "module": FASTQ_TO_PARQUET_MODULE,
                "inputs": ["fastq_path"],
                "outputs": ["reads"],
                "baseline_resources": {
                    "cpu": 2,
                    "mem_gb": 4,
                    "walltime": timedelta(minutes=30),
                },
            }
        ],
        action_ceiling=ActionCeiling(
            cpu=8, mem_gb=16, walltime=timedelta(hours=2), gpu=0
        ),
    )


@pytest.fixture
async def fastq_to_parquet_action(postgres_pool):
    """Sync the action for this smoke run. Each test gets a unique
    version suffix so parallel pytest-xdist workers don't collide on
    the (action_id, version) primary key."""
    from qiita_control_plane.actions import sync_actions

    action_id = "fastq-to-parquet"
    version = f"smoke-{uuid.uuid4()}"
    action = _build_fastq_to_parquet_action(action_id=action_id, version=version)

    async with postgres_pool.acquire() as conn:
        await sync_actions(conn, [action])

    yield (action_id, version)

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        action_id,
        version,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
        action_id,
        version,
    )


@pytest.fixture
async def smoke_sequenced_sample(postgres_pool, human_admin_session):
    """A minimal qiita.sequenced_sample row to scope the smoke ticket
    against. Seeds the full FK chain (metadata_checklist, biosample,
    prep_protocol) so the row satisfies every NOT NULL FK constraint;
    reverse-FK cleanup runs on yield exit."""
    admin_idx = human_admin_session["principal_idx"]

    checklist_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.metadata_checklist (name) VALUES ($1) RETURNING idx",
        f"smoke-chk-{uuid.uuid4()}",
    )
    biosample_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.biosample (owner_idx, metadata_checklist_idx, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        admin_idx,
        checklist_idx,
    )
    prep_protocol_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.prep_protocol (name, created_by_idx)"
        " VALUES ($1, $2) RETURNING idx",
        # prep_protocol_name_format CHECK requires ^[a-z][a-z0-9_]*$
        f"p_{uuid.uuid4().hex[:8]}",
        admin_idx,
    )
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequenced_sample ("
        "  biosample_idx, owner_idx, prep_protocol_idx, metadata_checklist_idx,"
        "  created_by_idx"
        ") VALUES ($1, $2, $3, $4, $2) RETURNING idx",
        biosample_idx,
        admin_idx,
        prep_protocol_idx,
        checklist_idx,
    )
    yield idx
    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE sequenced_sample_idx = $1", idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.sequenced_sample WHERE idx = $1", idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_protocol WHERE idx = $1", prep_protocol_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.metadata_checklist WHERE idx = $1", checklist_idx
    )


async def test_fastq_to_parquet_through_runner(
    postgres_pool,
    fastq_to_parquet_action,
    smoke_sequenced_sample,
    human_admin_session,
    tmp_path,
):
    """End-to-end: a sequenced_sample-scoped ticket runs fastq_to_parquet
    against the checked-in FASTQ fixture and completes. The Parquet's
    schema and a known-sequence sequence_hash are both asserted so the
    md5 → UUID path is covered for a deterministic value."""
    from qiita_control_plane.runner import run_workflow

    action_id, action_version = fastq_to_parquet_action
    sequenced_sample_idx = smoke_sequenced_sample

    work_ticket_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, sequenced_sample_idx, action_context"
        ") VALUES ($1, $2, $3, 'sequenced_sample', $4, $5::jsonb)"
        " RETURNING work_ticket_idx",
        action_id,
        action_version,
        human_admin_session["principal_idx"],
        sequenced_sample_idx,
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
        # `quality` is miint's native UTINYINT[] (raw phred bytes), not the
        # FASTQ ASCII string — see fastq_to_parquet.py module docstring.
        assert rows == [
            ("read_id", "VARCHAR"),
            ("sequence", "VARCHAR"),
            ("quality", "UTINYINT[]"),
            ("sequence_length", "BIGINT"),
            ("sequence_hash", "UUID"),
        ]

        # Fixture has 4 reads — no dedup at this stage (each read produces
        # a row, including duplicate sequences). Sort by sequence_hash
        # matches what execute() ORDER BYs.
        records = conn.execute(
            "SELECT read_id, sequence, quality, sequence_length, sequence_hash"
            f" FROM '{reads_parquet}' ORDER BY read_id"
        ).fetchall()
    assert len(records) == 4

    by_read_id = {r[0]: r for r in records}
    # read_001 and read_003 share the sequence ACGTACGTACGTACGTACGT, so
    # their sequence_hash values must match — proof the md5+UUID path is
    # deterministic over identical bytes.
    assert by_read_id["read_001"][1] == "ACGTACGTACGTACGTACGT"
    assert by_read_id["read_003"][1] == "ACGTACGTACGTACGTACGT"
    assert by_read_id["read_001"][4] == by_read_id["read_003"][4]
    # All sequence_lengths are 20 (each read in the fixture is 20 bp).
    assert {r[3] for r in records} == {20}

    # Concrete sequence_hash for ACGTACGTACGTACGTACGT — md5 cast to UUID.
    # Computed via Python's hashlib (the canonical md5) so a future miint
    # version that returns a different md5 result surfaces as a mismatch.
    expected_hash = UUID(hashlib.md5(b"ACGTACGTACGTACGTACGT").hexdigest())
    assert by_read_id["read_001"][4] == expected_hash

    # Quality bytes round-trip from the FASTQ — read_001's quality is
    # twenty `!` characters (ASCII 33), and miint returns phred-decoded
    # scores so each entry is 33 - 33 = 0. Confirms the FASTA-vs-FASTQ
    # branch on read_fastx populates the qual1 column for FASTQ inputs
    # and that the decoding offset is applied.
    assert by_read_id["read_001"][2] == [0] * 20
