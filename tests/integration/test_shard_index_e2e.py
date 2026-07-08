"""End-to-end: drive workflows/build-shard-index/1.0.0.yaml through the runner.

Closes the loop the per-commit tests cover only in pieces — it runs the FULL
sharded-index build for ONE shard through `run_workflow` against a LIVE data
plane + real miint, exercising every new B5 runner seam:

  - `_stage_shard_roster` (runner pre-step) reads the shard's feature set from
    Postgres `reference_membership.shard_id`, signs a REAL feature_idx-scoped
    `reference_sequences` DoGet against the live DP, and stages
    `shard_roster.parquet` (binding `shard_features` + `shard_id`)
  - `build_rype_index` in shard mode streams that shard's chunks from the live DP
    and runs the REAL miint `rype_index_create` to write the per-shard `.ryxdi`
  - the runner's `register-index` arm writes a per-shard `reference_index` row
    (shard_id set)
  - the terminal `finalize-shard` counts registered shards == N and does the
    guarded `indexing -> active`

`build_rype_index.open_reference_chunk_stream` (which would hop to the CP for a
ticket) is monkeypatched to sign the chunk ticket DIRECTLY with the fixture DP's
secret — the CO->CP ticket hop has its own tests; what's exercised here is the
runner drive + DP DoGet + real streaming build. The roster DoGet in
`_stage_shard_roster` is NOT stubbed: run_workflow gets the DP secret as
`hmac_secret`, so it hits the live DP for real.

Only `build_rype` is enabled (minimap2/bowtie2 off) to keep the real build tiny;
finalize-shard then expects exactly {rype} and flips the reference active. The
per-shard minimap2/bowtie2 stream builds are covered at the job level by
test_reference_stream_build.
"""

import json
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from _runner_helpers import LocalComputeBackendClient
from conftest import ducklake_connect
from qiita_common.api_paths import LOOPBACK_HOST

_BUILD_SHARD_INDEX_YAML_PATH = (
    Path(__file__).parent.parent.parent
    / "workflows"
    / "build-shard-index"
    / "1.0.0.yaml"
)

# STRUCTURED contigs (distinct motifs tiled) so miint sees real, reproducible
# content and builds a non-empty .ryxdi. ~3.6 kb each — small enough for a fast
# real build under load.
_CONTIGS = {
    0: "ACGTACGTGGCCTTAAACGTTGCA" * 150,
    1: "TTGGCCAATTGGCCAAGTGTGTGT" * 150,
}
_SHARD_ID = 0


@pytest.fixture
async def synced_build_shard_index_action(postgres_pool, tmp_path):
    """Materialize workflows/build-shard-index/1.0.0.yaml under tmp_path/workflows/
    so the loader picks it up, sync it into qiita.action under a unique version,
    and clean it up after (so it can't clash with a real synced action)."""
    from qiita_control_plane.actions import load_actions, sync_actions

    workflows_dir = tmp_path / "workflows" / "build-shard-index"
    workflows_dir.mkdir(parents=True)
    yaml_text = _BUILD_SHARD_INDEX_YAML_PATH.read_text()
    test_version = f"e2e-{uuid.uuid4()}"
    yaml_text = yaml_text.replace("version: 1.0.0", f"version: {test_version}")
    (workflows_dir / "1.0.0.yaml").write_text(yaml_text)

    actions = load_actions(tmp_path / "workflows")
    assert len(actions) == 1
    async with postgres_pool.acquire() as conn:
        await sync_actions(conn, actions)

    yield ("build-shard-index", test_version)

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        "build-shard-index",
        test_version,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
        "build-shard-index",
        test_version,
    )


@pytest.fixture
async def sharded_reference(postgres_pool, human_admin_session, data_plane):
    """Seed a reference in `indexing` with one shard (shard_id=0) of two features,
    across BOTH stores: Postgres (qiita.feature + reference_membership.shard_id, the
    cover-map _stage_shard_roster + finalize-shard read) and DuckLake
    (reference_sequences + reference_membership for the roster DoGet, and
    reference_sequence_chunks for the build stream). Yields (reference_idx,
    feature_idxs). Cleans up all Postgres rows after (DuckLake is dropped by the
    next module's data_plane reset)."""
    reference_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', 'indexing', $2)"
        " RETURNING reference_idx",
        f"shard-e2e-{uuid.uuid4()}",
        human_admin_session["principal_idx"],
    )

    # Postgres feature + membership rows (shard_id=0). Feature_idx is assigned
    # here and reused verbatim to seed the DuckLake side so the two agree.
    feature_idxs: list[int] = []
    for _ in _CONTIGS:
        fidx = await postgres_pool.fetchval(
            "INSERT INTO qiita.feature (sequence_hash) VALUES (gen_random_uuid())"
            " RETURNING feature_idx"
        )
        feature_idxs.append(fidx)
        await postgres_pool.execute(
            "INSERT INTO qiita.reference_membership (reference_idx, feature_idx, shard_id)"
            " VALUES ($1, $2, $3)",
            reference_idx,
            fidx,
            _SHARD_ID,
        )

    # DuckLake side (tables created by the data plane on startup).
    conn = ducklake_connect(data_plane["data_path"])
    try:
        seqs = list(_CONTIGS.values())
        seq_rows = ", ".join(
            f"({fidx}, gen_random_uuid(), {len(seq)})"
            for fidx, seq in zip(feature_idxs, seqs)
        )
        conn.execute(f"INSERT INTO qiita_lake.reference_sequences VALUES {seq_rows}")
        member_rows = ", ".join(f"({reference_idx}, {fidx})" for fidx in feature_idxs)
        conn.execute(
            f"INSERT INTO qiita_lake.reference_membership VALUES {member_rows}"
        )
        chunk_rows = []
        for fidx, seq in zip(feature_idxs, seqs):
            mid = len(seq) // 2  # two chunks each, to exercise reassembly
            chunk_rows.append(f"({fidx}, 0, '{seq[:mid]}')")
            chunk_rows.append(f"({fidx}, 1, '{seq[mid:]}')")
        conn.execute(
            f"INSERT INTO qiita_lake.reference_sequence_chunks VALUES {', '.join(chunk_rows)}"
        )
    finally:
        conn.close()

    yield reference_idx, feature_idxs

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE reference_idx = $1", reference_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference_index WHERE reference_idx = $1", reference_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", reference_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])", feature_idxs
    )


def _fake_open_stream(data_plane):
    """A drop-in `open_reference_chunk_stream` that signs a feature_idx-scoped
    chunk ticket with the fixture DP secret and streams via the real
    `stream_reference_chunks` — bypassing the CP hop (which needs a running CP)."""
    from qiita_compute_orchestrator.data_plane_client import stream_reference_chunks

    from qiita_control_plane.auth.tickets import sign_ticket

    @asynccontextmanager
    async def fake(conn, *, reference_idx, feature_idx, relation="reference_chunks"):
        ticket = sign_ticket(
            table="reference_sequence_chunks",
            filter={"reference_idx": [reference_idx], "feature_idx": feature_idx},
            secret=data_plane["secret"],
        )
        url = f"grpc://{LOOPBACK_HOST}:{data_plane['port']}"
        with stream_reference_chunks(
            conn, data_plane_url=url, ticket_bytes=ticket, relation=relation
        ) as rel:
            yield rel

    return fake


async def test_build_shard_index_workflow_end_to_end(
    monkeypatch,
    postgres_pool,
    data_plane,
    synced_build_shard_index_action,
    sharded_reference,
    human_admin_session,
    tmp_path,
):
    """Drive build-shard-index for one shard via the in-process runner +
    LocalBackend against the live data plane. After the run: ticket COMPLETED, a
    rype reference_index row for shard 0, and the reference flipped
    `indexing -> active` by finalize-shard."""
    from qiita_compute_orchestrator.jobs import build_rype_index

    from qiita_control_plane.runner import run_workflow

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "derived"))
    monkeypatch.setattr(
        build_rype_index, "open_reference_chunk_stream", _fake_open_stream(data_plane)
    )

    action_id, action_version = synced_build_shard_index_action
    reference_idx, _feature_idxs = sharded_reference

    work_ticket_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, shard_id, action_context"
        ") VALUES ($1, $2, $3, 'reference', $4, $5, $6::jsonb)"
        " RETURNING work_ticket_idx",
        action_id,
        action_version,
        human_admin_session["principal_idx"],
        reference_idx,
        _SHARD_ID,
        # rype only — keeps the real build tiny; finalize then expects exactly {rype}.
        json.dumps(
            {"build_rype": True, "build_minimap2": False, "build_bowtie2": False}
        ),
    )

    await run_workflow(
        work_ticket_idx,
        postgres_pool,
        LocalComputeBackendClient(),  # type: ignore[arg-type]  # protocol-shaped duck
        # The DP fixture's secret, so _stage_shard_roster's REAL roster DoGet
        # verifies at the live DP.
        hmac_secret=data_plane["secret"],
        data_plane_url=f"grpc://{LOOPBACK_HOST}:{data_plane['port']}",
        work_ticket_workspace_root=tmp_path / "workspace",
        upload_staging_root=tmp_path / "upload-staging",
    )

    # Ticket ran every entry to completion.
    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert state == "completed"

    # register-index wrote exactly one per-shard rype row for shard 0.
    index_rows = await postgres_pool.fetch(
        "SELECT index_type, shard_id FROM qiita.reference_index WHERE reference_idx = $1",
        reference_idx,
    )
    assert [(r["index_type"], r["shard_id"]) for r in index_rows] == [
        ("rype", _SHARD_ID)
    ]

    # finalize-shard saw rype complete for all N (=1) shards and flipped active.
    ref_status = await postgres_pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    assert ref_status == "active"

    # The runner pre-step staged the shard roster (proves _stage_shard_roster ran).
    roster = tmp_path / "workspace" / str(work_ticket_idx) / "shard_roster.parquet"
    assert roster.exists(), "runner did not stage shard_roster.parquet"
