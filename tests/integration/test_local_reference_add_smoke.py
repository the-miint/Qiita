"""End-to-end smoke test: drive workflows/local-reference-add/1.0.0.yaml
through the control-plane runner with a real LocalBackend.

This is the by-path (`--local`) twin of test_reference_add_smoke.py. Instead of
staging a DoPut upload Parquet and passing a `fasta_upload_idx`, it writes a
*manifest* of absolute FASTA paths plus the FASTA files themselves on disk and
passes a raw `fasta_manifest_path` in action_context. The new first step
(`stage_local_fasta`, a real native job) reads the manifest, parses every file
with miint's read_fastx, and produces the SAME chunked `(read_id, chunk_index,
chunk_data)` Parquet the remote path uploads — so the rest of the pipeline
(hash_sequences → mint-features → write-membership → load → register-files) runs
byte-for-byte identically. The whole point: the runner needs zero special-casing
for the local path; the raw `*_path` keys flow through untouched.

What's exercised end-to-end:
  - YAML loader → sync into qiita.action
  - Runner reads the action row, walks every entry, leaving raw `*_path`
    action_context keys untouched (no `*_upload_idx` to resolve)
  - Real LocalBackend stage_local_fasta: manifest of 2 FASTA files → one
    fasta.parquet (3 reads across the files)
  - Real LocalBackend hash_sequences consumes that fasta.parquet
  - In-process LIBRARY[mint-features] → qiita.feature rows + feature_map.parquet
  - In-process LIBRARY[write-membership] → qiita.reference_membership rows
  - Real LocalBackend load step writes reference_*.parquet
  - register-files monkeypatched at the LIBRARY entry (data-plane Flight needs a
    running data plane; covered by test_e2e_reference separately)

The assertion surface mirrors the remote smoke: reference reaches `active`, the
right number of feature/membership rows exist, the work_ticket reaches
COMPLETED, and the local stager's fasta.parquet materialised in its workspace.
"""

import uuid
from pathlib import Path

import pytest

from _runner_helpers import LocalComputeBackendClient

_LOCAL_REFERENCE_ADD_YAML_PATH = (
    Path(__file__).parent.parent.parent
    / "workflows"
    / "local-reference-add"
    / "1.0.0.yaml"
)


@pytest.fixture
async def synced_local_reference_add_action(postgres_pool, tmp_path):
    """Materialize workflows/local-reference-add/1.0.0.yaml under
    tmp_path/workflows/ so the loader's directory walk picks it up, sync it into
    qiita.action, and clean the row up after."""
    from qiita_control_plane.actions import load_actions, sync_actions

    workflows_dir = tmp_path / "workflows" / "local-reference-add"
    workflows_dir.mkdir(parents=True)
    yaml_text = _LOCAL_REFERENCE_ADD_YAML_PATH.read_text()
    test_version = f"smoke-{uuid.uuid4()}"
    yaml_text = yaml_text.replace("version: 1.0.0", f"version: {test_version}")
    (workflows_dir / "1.0.0.yaml").write_text(yaml_text)

    actions = load_actions(tmp_path / "workflows")
    assert len(actions) == 1
    async with postgres_pool.acquire() as conn:
        await sync_actions(conn, actions)

    yield ("local-reference-add", test_version)

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        "local-reference-add",
        test_version,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
        "local-reference-add",
        test_version,
    )


@pytest.fixture
async def smoke_reference(postgres_pool, human_admin_session):
    """A fresh reference for the smoke run; cleans up everything pointing at it
    (work_ticket via FK, then reference_membership) before dropping it."""
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', 'pending', $2)"
        " RETURNING reference_idx",
        f"local-smoke-{uuid.uuid4()}",
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


async def test_local_reference_add_workflow_end_to_end(
    monkeypatch,
    postgres_pool,
    synced_local_reference_add_action,
    smoke_reference,
    human_admin_session,
    tmp_path,
):
    """Drive the full local-reference-add workflow via the in-process runner +
    LocalBackend. After the run: ticket COMPLETED, reference 'active', three
    feature/membership rows (the manifest's two FASTA files hold three reads),
    register-files invoked once, and the stage_local_fasta workspace holds the
    combined fasta.parquet."""
    import json

    from qiita_common.api_paths import LibraryPrimitive
    from qiita_control_plane.actions import library as _lib
    from qiita_control_plane.runner import run_workflow

    action_id, action_version = synced_local_reference_add_action
    reference_idx = smoke_reference

    # A small multi-FASTA set on disk plus a manifest of their absolute paths.
    # Three distinct sequences across two files (matching the remote smoke's
    # three features). read_ids are globally unique — the genome_map join key.
    fasta_a = tmp_path / "refs" / "a.fa"
    fasta_b = tmp_path / "refs" / "b.fa"
    fasta_a.parent.mkdir(parents=True, exist_ok=True)
    fasta_a.write_text(">g1\nACGTACGTACGTACGT\n>g2\nTTTTAAAACCCCGGGG\n")
    fasta_b.write_text(">g3\nGCATGCATGCATGCAT\n")
    manifest = tmp_path / "refs" / "manifest.txt"
    manifest.write_text(f"{fasta_a}\n{fasta_b}\n")

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
        json.dumps({"fasta_manifest_path": str(manifest)}),
    )

    # Stub register-files: the real path requires a running data plane (Arrow
    # Flight DoAction), which test_e2e_reference covers separately.
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

    # Stub the post-load exclusion sync too (real path signs a DoAction to a
    # running data plane). Capturing the call proves the hook fires on the local
    # path exactly as on the remote one.
    sync_calls: list[dict] = []

    async def _stub_sync_exclusion(pool, *, dest, signing_key, data_plane_url):
        sync_calls.append({"dest": dest})
        return {"synced_feature_count": 0}

    monkeypatch.setitem(
        _lib.LIBRARY, LibraryPrimitive.SYNC_REFERENCE_EXCLUSION, _stub_sync_exclusion
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
        # No uploads on the local path, but the kwarg is required; an empty
        # staging root is never read because action_context has no *_upload_idx.
        upload_staging_root=tmp_path / "upload-staging",
    )

    # work_ticket transitioned to COMPLETED.
    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert state == "completed"

    # Reference walked pending → (stage) → hashing → minting → loading → active.
    ref_status = await postgres_pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    assert ref_status == "active"

    # mint-features inserted three feature rows (3 reads across the 2 files).
    feature_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.feature f"
        " JOIN qiita.reference_membership m ON m.feature_idx = f.feature_idx"
        " WHERE m.reference_idx = $1",
        reference_idx,
    )
    assert feature_count == 3

    # register-files invoked once with the staging-dir Parquet files mapped by
    # the runner's convention — identical to the remote path's load output.
    assert len(register_calls) == 1
    # The post-load exclusion sync fired exactly once (local path), staging its
    # Parquet into this ticket's workspace.
    assert len(sync_calls) == 1
    assert sync_calls[0]["dest"].name == "reference_exclusion.parquet"
    _staging_dir, files = register_calls[0]
    assert files.get("reference_sequences.parquet") == "reference_sequences"
    assert "reference_membership.parquet" in files
    chunk_parts = [
        name for name in files if name.startswith("reference_sequence_chunks/")
    ]
    assert chunk_parts, "expected reference_sequence_chunks subdir parts"
    for name in chunk_parts:
        assert files[name] == "reference_sequence_chunks"

    # The local stager's combined Parquet materialised in its per-entry
    # workspace (attempt-0), and hash_sequences consumed it into manifest.parquet.
    workspace = workspace_root / str(work_ticket_idx)
    assert (workspace / "stage_local_fasta" / "attempt-0" / "fasta.parquet").exists()
    assert (workspace / "hash_sequences" / "attempt-0" / "manifest.parquet").exists()
