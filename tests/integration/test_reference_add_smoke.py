"""End-to-end smoke test: drive workflows/reference-add/1.0.0.yaml
through the control-plane runner with a real LocalBackend.

What's exercised end-to-end:
  - YAML loader → sync into qiita.action
  - Runner reads the action row, walks every entry
  - Real LocalBackend hashes a tiny FASTA into manifest.parquet
  - In-process LIBRARY[mint-features] → qiita.feature rows + feature_map.parquet
  - In-process LIBRARY[write-membership] → qiita.reference_membership rows
  - Real LocalBackend load step writes reference_*.parquet
  - register-files monkeypatched at the LIBRARY entry: data-plane Flight
    needs cargo / a running data plane, which this test deliberately
    avoids — covered by test_e2e_reference instead.

The assertion surface is the post-conditions that prove every entry
ran: reference reaches `active`, feature/membership rows exist,
work_ticket reaches COMPLETED.
"""

import uuid
from pathlib import Path

import pytest

from _runner_helpers import LocalComputeBackendClient

_REFERENCE_ADD_YAML_PATH = (
    Path(__file__).parent.parent.parent / "workflows" / "reference-add" / "1.0.0.yaml"
)


@pytest.fixture
async def synced_reference_add_action(postgres_pool, tmp_path):
    """Materialize workflows/reference-add/1.0.0.yaml under tmp_path/workflows/
    so the loader's directory walk picks it up, sync it into qiita.action,
    and clean the row up after."""
    from qiita_control_plane.actions import load_actions, sync_actions

    workflows_dir = tmp_path / "workflows" / "reference-add"
    workflows_dir.mkdir(parents=True)
    yaml_text = _REFERENCE_ADD_YAML_PATH.read_text()
    test_version = f"smoke-{uuid.uuid4()}"
    yaml_text = yaml_text.replace("version: 1.0.0", f"version: {test_version}")
    (workflows_dir / "1.0.0.yaml").write_text(yaml_text)

    actions = load_actions(tmp_path / "workflows")
    assert len(actions) == 1
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
async def smoke_reference(postgres_pool, human_admin_session):
    """A fresh reference for the smoke run; cleans up everything pointing
    at it (work_ticket via FK, then reference_membership) before
    dropping the reference itself."""
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', 'pending', $2)"
        " RETURNING reference_idx",
        f"smoke-{uuid.uuid4()}",
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


async def test_reference_add_workflow_end_to_end(
    monkeypatch,
    postgres_pool,
    synced_reference_add_action,
    smoke_reference,
    human_admin_session,
    tmp_path,
):
    """Drive the full reference-add workflow via the in-process runner +
    LocalBackend. After the run: ticket COMPLETED, reference 'active',
    feature/membership rows present, register-files invoked exactly once
    with a sensible filename → table mapping."""
    from qiita_common.api_paths import LibraryPrimitive
    from qiita_control_plane.actions import library as _lib
    from qiita_control_plane.runner import run_workflow

    import json

    import duckdb
    from qiita_common.api_paths import compute_upload_staging_path

    action_id, action_version = synced_reference_add_action
    reference_idx = smoke_reference

    # Stage an upload row + chunked-FASTA Parquet at the path the
    # runner will resolve from `fasta_upload_idx`. The workflow consumes
    # the Parquet via hash_sequences' read_parquet(?); the shape is the
    # CLI's wire form `(read_id, chunk_index, chunk_data)` — short
    # sequences fit in one chunk each.
    upload_staging_root = tmp_path / "upload-staging"
    upload_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.upload (status, created_by_idx, completed_at,"
        "  sha256, row_count, bytes_received)"
        " VALUES ('ready', $1, now(), $2, 3, 0) RETURNING upload_idx",
        human_admin_session["principal_idx"],
        "0" * 64,
    )
    upload_parquet = compute_upload_staging_path(upload_staging_root, upload_idx)
    upload_parquet.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "COPY (SELECT * FROM (VALUES"
            "  ('seq1', 0, 'ACGTACGTACGTACGT'),"
            "  ('seq2', 0, 'TTTTAAAACCCCGGGG'),"
            "  ('seq3', 0, 'GCATGCATGCATGCAT')"
            ") AS upload(read_id, chunk_index, chunk_data))"
            f" TO '{upload_parquet}' (FORMAT PARQUET)"
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
        reference_idx,
        json.dumps({"fasta_upload_idx": upload_idx}),
    )

    # Stub register-files: the real path requires a running data plane
    # (Arrow Flight DoAction), which test_e2e_reference covers separately.
    register_calls: list[tuple] = []

    async def _stub_register_files(
        *, staging_dir, files, work_ticket_idx, signing_key, data_plane_url
    ):
        # work_ticket_idx is keyword-required: the runner must thread it so the
        # data plane can mint unique, ticket-traceable lake filenames.
        register_calls.append((staging_dir, dict(files)))
        return [f"{staging_dir}/{name}" for name in files]

    monkeypatch.setitem(
        _lib.LIBRARY, LibraryPrimitive.REGISTER_FILES, _stub_register_files
    )

    workspace_root = tmp_path / "workspace"
    backend_client = LocalComputeBackendClient()

    await run_workflow(
        work_ticket_idx,
        postgres_pool,
        backend_client,  # type: ignore[arg-type]  # protocol-shaped duck
        signing_key=b"unused-in-smoke",
        data_plane_url="grpc://unused:0",
        work_ticket_workspace_root=workspace_root,
        upload_staging_root=upload_staging_root,
    )

    # work_ticket transitioned to COMPLETED.
    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert state == "completed"

    # Reference walked through hashing → minting → loading → active.
    ref_status = await postgres_pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    assert ref_status == "active"

    # mint-features inserted three feature rows (the FASTA has 3 sequences).
    feature_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.feature f"
        " JOIN qiita.reference_membership m ON m.feature_idx = f.feature_idx"
        " WHERE m.reference_idx = $1",
        reference_idx,
    )
    assert feature_count == 3

    # register-files invoked once with the staging-dir Parquet files
    # mapped by the runner's convention: flat files → stem; subdir of
    # part files → directory name. `reference_sequence_chunks` is the
    # multi-file form (see reference_load.py).
    assert len(register_calls) == 1
    staging_dir, files = register_calls[0]
    assert "reference_sequences.parquet" in files
    assert files["reference_sequences.parquet"] == "reference_sequences"
    assert "reference_membership.parquet" in files
    chunk_parts = [
        name for name in files if name.startswith("reference_sequence_chunks/")
    ]
    assert chunk_parts, "expected reference_sequence_chunks subdir parts"
    for name in chunk_parts:
        assert files[name] == "reference_sequence_chunks"

    # Per-entry / per-attempt workspaces materialised the expected files.
    # Layout is `<workspace_root>/<work_ticket_idx>/<entry-name>/attempt-<N>/`
    # (see qiita_control_plane/runner.py:251). Happy-path smoke = attempt-0.
    workspace = workspace_root / str(work_ticket_idx)
    assert (workspace / "hash_sequences" / "attempt-0" / "manifest.parquet").exists()
    assert (workspace / "mint-features" / "attempt-0" / "feature_map.parquet").exists()
