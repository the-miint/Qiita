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

    Uses the shared db_seeds composer so this fixture stays in sync with
    every other "I need a sequenced prep_sample" site (route tests,
    repository tests). The sequenced_sample 1:1 subtype row is
    intentionally NOT created — fastq_to_parquet never reads
    sequencing-specific columns. Reverse-FK cleanup runs on yield exit."""
    from qiita_control_plane.testing.db_seeds import (
        seed_biosample_with_sequenced_prep_sample,
    )

    admin_idx = human_admin_session["principal_idx"]
    biosample_idx, idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=admin_idx
    )
    yield idx
    # The composer used the seeded `short_read_metagenomics` prep_protocol
    # (system-owned), so we don't delete the protocol here.
    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE prep_sample_idx = $1", idx
    )
    await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", idx)
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx
    )


async def test_fastq_to_parquet_through_runner(
    postgres_pool,
    fastq_to_parquet_action,
    smoke_prep_sample,
    human_admin_session,
    tmp_path,
    monkeypatch,
):
    """End-to-end: a prep_sample-scoped ticket (processing_kind='sequenced')
    runs fastq_to_parquet against the checked-in FASTQ fixture and
    completes. Asserts the Parquet's column schema, the duplicate-
    sequence preservation guarantee, and the CP-minted sequence_idx
    values.

    The mint helper is monkey-patched to bypass HTTP and call
    qiita.mint_sequence_range() directly via the postgres_pool. The
    HTTP path is exercised by the orchestrator's isolated unit tests
    (tests/test_sequence_range.py); spinning up the CP app in-process
    here would add infrastructure for one assertion that the unit
    tests already cover."""
    from qiita_compute_orchestrator.jobs import fastq_to_parquet as fastq_module
    from qiita_compute_orchestrator.sequence_range import MintedSequenceRange
    from qiita_control_plane.runner import run_workflow

    action_id, action_version = fastq_to_parquet_action
    prep_sample_idx = smoke_prep_sample
    admin_idx = human_admin_session["principal_idx"]

    # In-process replacement for mint_sequence_range that calls the CP's
    # mint function directly through the existing postgres_pool. The
    # real signature is `(http, prep_sample_idx, count) -> MintedSequenceRange`;
    # the fake ignores http (no client is constructed at all).
    mint_calls: list[tuple[int, int]] = []

    async def _local_mint(*, http, prep_sample_idx, count):
        mint_calls.append((prep_sample_idx, count))
        row = await postgres_pool.fetchrow(
            "SELECT * FROM qiita.mint_sequence_range($1, $2, $3)",
            prep_sample_idx,
            count,
            admin_idx,
        )
        return MintedSequenceRange(
            prep_sample_idx=row["prep_sample_idx"],
            sequence_idx_start=row["sequence_idx_start"],
            sequence_idx_stop=row["sequence_idx_stop"],
        )

    monkeypatch.setattr(fastq_module, "mint_sequence_range", _local_mint)

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
    upload_staging_root = tmp_path / "upload-staging"
    backend_client = LocalComputeBackendClient()

    await run_workflow(
        work_ticket_idx,
        postgres_pool,
        backend_client,  # type: ignore[arg-type]  # protocol-shaped duck
        hmac_secret=b"unused-in-smoke",
        data_plane_url="grpc://unused:0",
        work_ticket_workspace_root=workspace_root,
        upload_staging_root=upload_staging_root,
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
        workspace_root / str(work_ticket_idx) / "fastq" / "attempt-0" / "read.parquet"
    )
    assert reads_parquet.exists(), f"expected read.parquet at {reads_parquet}"

    # Mint helper called exactly once with the fixture's read count.
    assert mint_calls == [(prep_sample_idx, 4)]

    # The minted range is persisted on the CP side too.
    range_row = await postgres_pool.fetchrow(
        "SELECT sequence_idx_start, sequence_idx_stop "
        "FROM qiita.sequence_range WHERE prep_sample_idx = $1",
        prep_sample_idx,
    )
    assert range_row is not None
    first_idx = range_row["sequence_idx_start"]
    assert range_row["sequence_idx_stop"] == first_idx + 3  # count=4, inclusive

    # Verify the Parquet's schema and content.
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            "SELECT column_name, column_type FROM ("
            f" DESCRIBE SELECT * FROM '{reads_parquet}')"
        ).fetchall()
        # DuckDB DESCRIBE column order matches the Parquet's physical order.
        # prep_sample_idx is the DuckLake `read` table's scope/prune column
        # (leading, the file is sorted by (prep_sample_idx, sequence_idx));
        # sequence_idx is the CP-minted join key; read_id stays as a label.
        # `qual1` is miint's native UTINYINT[] (phred-decoded scores).
        # sequence2/qual2 are always emitted for schema uniformity — they're
        # NULL for the unpaired smoke fixture. No sequence_length column:
        # Parquet row-group metadata + a length(sequence1) call covers it.
        assert rows == [
            ("prep_sample_idx", "BIGINT"),
            ("sequence_idx", "BIGINT"),
            ("read_id", "VARCHAR"),
            ("sequence1", "VARCHAR"),
            ("qual1", "UTINYINT[]"),
            ("sequence2", "VARCHAR"),
            ("qual2", "UTINYINT[]"),
        ]

        # Fixture has 4 reads — no dedup, each read produces a row even
        # when sequences match (read_001 and read_003 carry the same
        # sequence; both appear).
        records = conn.execute(
            "SELECT sequence_idx, read_id, sequence1, qual1,"
            f" length(sequence1) AS seq_len FROM '{reads_parquet}'"
            " ORDER BY sequence_idx"
        ).fetchall()
    assert len(records) == 4

    # sequence_idx values are exactly the contiguous minted range.
    assert [r[0] for r in records] == [first_idx + i for i in range(4)]

    by_read_id = {r[1]: r for r in records}
    # read_001 and read_003 share the sequence ACGTACGTACGTACGTACGT —
    # confirms duplicates are preserved (this is a sample-side ingest,
    # not a reference-side dedup). Both still get distinct sequence_idx
    # values from the minted range.
    assert by_read_id["read_001"][2] == "ACGTACGTACGTACGTACGT"
    assert by_read_id["read_003"][2] == "ACGTACGTACGTACGTACGT"
    assert by_read_id["read_001"][0] != by_read_id["read_003"][0]
    # All read lengths are 20 (each read in the fixture is 20 bp);
    # length() is computed at query time from sequence1 rather than
    # stored as a column.
    assert {r[4] for r in records} == {20}

    # Quality bytes round-trip from the FASTQ — read_001's quality is
    # twenty `!` characters (ASCII 33), and miint returns phred-decoded
    # scores so each entry is 33 - 33 = 0. Confirms the FASTA-vs-FASTQ
    # branch on read_fastx populates the qual1 column for FASTQ inputs
    # and that the decoding offset is applied.
    assert by_read_id["read_001"][3] == [0] * 20
