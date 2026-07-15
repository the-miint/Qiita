"""End-to-end: drive workflows/estimate-feature-table/1.0.0.yaml through the runner.

Runs the FULL feature-table (OGU) estimation for one alignment + cohort through
`run_workflow` against a LIVE data plane + real miint, exercising the seams no
narrower test covers together:

  - the runner pre-step resolver (`_resolve_feature_table_bindings`) derives the
    reference from the alignment, gates cohort completeness against
    `alignment_sample`, and stages the `feature_idx -> genome_idx` map Parquet from
    Postgres — binding `genome_map_path` the job then reads (the resolver->job
    handoff);
  - `estimate_feature_table` streams the `alignment` slice (the Phase-1 DP
    `alignment` DoGet, over Arrow Flight) + the reference `sequence_length_bp`
    (`reference_sequences` DoGet) from the live DP, and runs the REAL miint
    `genome_coverage` + `woltka_ogu` to write `ogu_table.parquet`.

The job's `open_alignment_stream` / `open_reference_sequences_stream` (which would
hop to the CP to mint their tickets) are monkeypatched to sign the DoGet tickets
DIRECTLY with the fixture DP's secret — the CP mint route + the resolver have
their own DB-tier tests; what's exercised here is the runner drive + the DP DoGets
+ the real streaming compute end to end.

The seeded cohort proves the load-bearing semantics through the whole stack:
genome A survives on POOLED coverage (0.6% in each of two samples, extending to
1.2% pooled >= 1%), while genome B is dropped (0.5% < 1%) — so the table carries
A for both samples and never B.

Real miint required: passes native BIGINT id columns with NO ::VARCHAR casts, so
it needs the woltka_ogu id-type fix in the installed miint build (local override
via MIINT_EXTENSION_REPO until the team mirror carries it).
"""

import json
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import duckdb
import pytest
from _runner_helpers import LocalComputeBackendClient
from conftest import ducklake_connect
from qiita_common.api_paths import LOOPBACK_HOST

from qiita_control_plane.repositories.alignment_definition import (
    mint_alignment_definition,
)
from qiita_control_plane.repositories.block import (
    create_alignment_sample_pending,
    finalize_alignment_sample,
)
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)

_YAML_PATH = (
    Path(__file__).parent.parent.parent
    / "workflows"
    / "estimate-feature-table"
    / "1.0.0.yaml"
)

# Genome A: 10 kb, covered 0.6% + 0.6% across the two samples in EXTENDING regions
# -> 1.2% pooled >= 1% (RETAINED, and only via pooling). Genome B: 1 kb, covered
# 0.5% -> DROPPED. Threshold 1%.
_LEN_A = 10000
_LEN_B = 1000
_THRESHOLD = 0.01


@pytest.fixture
async def synced_estimate_feature_table_action(postgres_pool, tmp_path):
    """Materialize the workflow YAML under a unique version, sync it into
    qiita.action, and clean it up after (so it never clashes with a real one)."""
    from qiita_control_plane.actions import load_actions, sync_actions

    wf_dir = tmp_path / "workflows" / "estimate-feature-table"
    wf_dir.mkdir(parents=True)
    version = f"e2e-{uuid.uuid4()}"
    (wf_dir / "1.0.0.yaml").write_text(
        _YAML_PATH.read_text().replace("version: 1.0.0", f"version: {version}")
    )
    actions = load_actions(tmp_path / "workflows")
    assert len(actions) == 1
    async with postgres_pool.acquire() as conn:
        await sync_actions(conn, actions)

    yield ("estimate-feature-table", version)

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        "estimate-feature-table",
        version,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
        "estimate-feature-table",
        version,
    )


@pytest.fixture
async def feature_table_scenario(postgres_pool, data_plane):
    """Seed the reference + alignment across BOTH stores with coordinated ids:
    Postgres (feature/genome/feature_genome/reference_membership + an
    alignment_definition + two `completed` alignment_sample gates — the resolver
    inputs) and DuckLake (reference_sequences lengths + the alignment slice — the
    DP-DoGet inputs). Yields the ids the test needs; cleans up Postgres (DuckLake
    is dropped by the next module's data_plane reset)."""
    principal_idx = await seed_user_principal(
        postgres_pool, prefix="ft-e2e", suffix=uuid.uuid4().hex[:8]
    )
    reference_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, is_host, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', false, $2) RETURNING reference_idx",
        f"ft-e2e-{uuid.uuid4()}",
        principal_idx,
    )
    # Two features, each its own genome (A first, B second).
    pairs: list[tuple[int, int]] = []
    for _ in range(2):
        feature_idx = await postgres_pool.fetchval(
            "INSERT INTO qiita.feature (sequence_hash) VALUES (gen_random_uuid())"
            " RETURNING feature_idx"
        )
        genome_idx = await postgres_pool.fetchval(
            "INSERT INTO qiita.genome (source, source_id) VALUES ('refseq', $1)"
            " RETURNING genome_idx",
            str(uuid.uuid4()),
        )
        await postgres_pool.execute(
            "INSERT INTO qiita.feature_genome (feature_idx, genome_idx) VALUES ($1, $2)",
            feature_idx,
            genome_idx,
        )
        await postgres_pool.execute(
            "INSERT INTO qiita.reference_membership (reference_idx, feature_idx) VALUES ($1, $2)",
            reference_idx,
            feature_idx,
        )
        pairs.append((feature_idx, genome_idx))

    async with postgres_pool.acquire() as conn:
        row = await mint_alignment_definition(
            conn,
            params={
                "reference_idx": reference_idx,
                "aligner": "minimap2",
                "mask_idx": 1,
                "shard_ids": [0],
            },
            principal_idx=principal_idx,
        )
    alignment_idx = row["alignment_idx"]

    biosample_idxs: list[int] = []
    prep_sample_idxs: list[int] = []
    for _ in range(2):
        bs_idx, ps_idx = await seed_biosample_with_sequenced_prep_sample(
            postgres_pool, owner_idx=principal_idx
        )
        biosample_idxs.append(bs_idx)
        prep_sample_idxs.append(ps_idx)

    async with postgres_pool.acquire() as conn, conn.transaction():
        await create_alignment_sample_pending(
            conn, alignment_idx=alignment_idx, prep_sample_idxs=prep_sample_idxs
        )
        for ps_idx in prep_sample_idxs:
            await finalize_alignment_sample(
                conn, alignment_idx=alignment_idx, prep_sample_idx=ps_idx
            )

    (feat_a, genome_a), (feat_b, genome_b) = pairs
    ps0, ps1 = prep_sample_idxs

    conn = ducklake_connect(data_plane["data_path"])
    try:
        conn.execute(
            "INSERT INTO qiita_lake.reference_sequences"
            " (feature_idx, sequence_hash, sequence_length_bp) VALUES"
            f" ({feat_a}, gen_random_uuid(), {_LEN_A}),"
            f" ({feat_b}, gen_random_uuid(), {_LEN_B})"
        )
        conn.execute(
            "INSERT INTO qiita_lake.reference_membership VALUES"
            f" ({reference_idx}, {feat_a}), ({reference_idx}, {feat_b})"
        )
        # (alignment_idx, prep_sample_idx, sequence_idx, feature_idx, flags,
        #  position, stop_position). A: 60bp in sample0 + 60bp (extending) in
        #  sample1 -> 1.2% pooled. B: 5bp in sample0 -> 0.5%.
        conn.execute(
            "INSERT INTO qiita_lake.alignment"
            " (alignment_idx, prep_sample_idx, sequence_idx, feature_idx, flags,"
            " position, stop_position) VALUES"
            f" ({alignment_idx}, {ps0}, 1, {feat_a}, 0, 0, 60),"
            f" ({alignment_idx}, {ps1}, 2, {feat_a}, 0, 60, 120),"
            f" ({alignment_idx}, {ps0}, 3, {feat_b}, 0, 0, 5)"
        )
    finally:
        conn.close()

    yield {
        "reference_idx": reference_idx,
        "alignment_idx": alignment_idx,
        "prep_sample_idxs": prep_sample_idxs,
        "principal_idx": principal_idx,
        "genome_a": genome_a,
        "genome_b": genome_b,
        "feature_idxs": [feat_a, feat_b],
        "genome_idxs": [genome_a, genome_b],
        "biosample_idxs": biosample_idxs,
    }

    # Postgres cleanup (FK order); the work_ticket may reference the reference.
    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE reference_idx = $1", reference_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1", alignment_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", prep_sample_idxs
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", biosample_idxs
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", reference_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.feature_genome WHERE feature_idx = ANY($1::bigint[])",
        [feat_a, feat_b],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])",
        [feat_a, feat_b],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.genome WHERE genome_idx = ANY($1::bigint[])",
        [genome_a, genome_b],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.principal WHERE idx = $1", principal_idx
    )


def _fake_alignment_stream(data_plane, *, alignment_idx, prep_sample_idx):
    """Drop-in `open_alignment_stream` that signs the `alignment` DoGet ticket
    directly with the fixture DP secret (the scope the CP route would derive from
    action_context) and streams via the real `stream_reference_chunks`."""
    from qiita_compute_orchestrator.data_plane_client import stream_reference_chunks

    from qiita_control_plane.auth.tickets import sign_ticket

    @asynccontextmanager
    async def fake(conn, *, work_ticket_idx, relation="alignment"):
        ticket = sign_ticket(
            table="alignment",
            filter={
                "alignment_idx": [alignment_idx],
                "prep_sample_idx": prep_sample_idx,
            },
            secret=data_plane["secret"],
        )
        url = f"grpc://{LOOPBACK_HOST}:{data_plane['port']}"
        with stream_reference_chunks(
            conn, data_plane_url=url, ticket_bytes=ticket, relation=relation
        ) as rel:
            yield rel

    return fake


def _fake_lengths_stream(data_plane):
    """Drop-in `open_reference_sequences_stream` that signs a whole-reference
    `reference_sequences` DoGet ticket directly with the fixture DP secret."""
    from qiita_compute_orchestrator.data_plane_client import stream_reference_chunks

    from qiita_control_plane.auth.tickets import sign_ticket

    @asynccontextmanager
    async def fake(conn, *, reference_idx, relation="reference_lengths"):
        ticket = sign_ticket(
            table="reference_sequences",
            filter={"reference_idx": [reference_idx]},
            secret=data_plane["secret"],
        )
        url = f"grpc://{LOOPBACK_HOST}:{data_plane['port']}"
        with stream_reference_chunks(
            conn, data_plane_url=url, ticket_bytes=ticket, relation=relation
        ) as rel:
            yield rel

    return fake


def _read_ogu(path: Path) -> list[tuple]:
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            f"SELECT prep_sample_idx, genome_idx, value FROM read_parquet('{path}') "
            "ORDER BY prep_sample_idx, genome_idx"
        ).fetchall()


async def test_estimate_feature_table_end_to_end(
    monkeypatch,
    postgres_pool,
    data_plane,
    synced_estimate_feature_table_action,
    feature_table_scenario,
    tmp_path,
):
    """Drive estimate-feature-table via the in-process runner + LocalBackend
    against the live data plane + real miint. After the run: ticket COMPLETED, and
    the emitted `ogu_table.parquet` carries genome A (pooled coverage) for both
    samples and NOT genome B (below-threshold)."""
    from qiita_compute_orchestrator.jobs import estimate_feature_table

    from qiita_control_plane.runner import run_workflow

    action_id, action_version = synced_estimate_feature_table_action
    s = feature_table_scenario

    monkeypatch.setattr(
        estimate_feature_table,
        "open_alignment_stream",
        _fake_alignment_stream(
            data_plane,
            alignment_idx=s["alignment_idx"],
            prep_sample_idx=s["prep_sample_idxs"],
        ),
    )
    monkeypatch.setattr(
        estimate_feature_table,
        "open_reference_sequences_stream",
        _fake_lengths_stream(data_plane),
    )

    work_ticket_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, action_context"
        ") VALUES ($1, $2, $3, 'reference', $4, $5::jsonb) RETURNING work_ticket_idx",
        action_id,
        action_version,
        s["principal_idx"],
        s["reference_idx"],
        json.dumps(
            {
                "alignment_idx": s["alignment_idx"],
                "prep_sample_idx": s["prep_sample_idxs"],
                "coverage_threshold": _THRESHOLD,
            }
        ),
    )

    workspace_root = tmp_path / "workspace"
    await run_workflow(
        work_ticket_idx,
        postgres_pool,
        LocalComputeBackendClient(),  # type: ignore[arg-type]  # protocol-shaped duck
        signing_key=data_plane["secret"],
        data_plane_url=f"grpc://{LOOPBACK_HOST}:{data_plane['port']}",
        work_ticket_workspace_root=workspace_root,
        upload_staging_root=tmp_path / "upload-staging",
    )

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert state == "completed"

    # The resolver staged the genome map into the ticket workspace (proves the
    # resolver->job handoff ran).
    assert (
        workspace_root / str(work_ticket_idx) / "feature_genome_map.parquet"
    ).exists()

    # The single job output — not registered into DuckLake (compute-on-demand), so
    # it stays in the workspace; locate it robustly.
    ogu_tables = list(workspace_root.rglob("ogu_table.parquet"))
    assert len(ogu_tables) == 1, f"expected one ogu_table.parquet, got {ogu_tables}"

    ps0, ps1 = s["prep_sample_idxs"]
    assert _read_ogu(ogu_tables[0]) == sorted(
        [(ps0, s["genome_a"], 1.0), (ps1, s["genome_a"], 1.0)]
    )
