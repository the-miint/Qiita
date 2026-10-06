"""End-to-end: golay-demux -> amplicon over a seeded sequenced_pool.

Drives the Rapid 16S chain through the real in-process runner + LocalBackend +
a real qiita-data-plane subprocess + the in-process control-plane app (reached
over an ASGI transport for the two CO->CP callbacks the steps make):

  golay-demux  demultiplexes a tiny multiplexed EMP 16S run (I1 Golay barcode +
               R1/R2 FASTQ) into per-sample reads, mints a `qiita.sequence_range`
               per prep_sample (CO->CP callback), and register-files loads the
               reads into the DuckLake `read` table.

The shipped golay-demux workflow runs bcl-convert (a container step) before the
demux, which LocalBackend cannot dispatch. So — exactly as test_read_mask_e2e
substitutes host_filter's output — this drives a trimmed golay-demux (the native
tail: golay_demux -> register-files) against a synthesized `convert_dir` holding
the Undetermined I1/R1/R2 FASTQs bcl_convert would have written. bcl-convert
itself runs only under SLURM in production.
  amplicon     streams the pool's reads back from `read` at runtime (denoise
               issues a pool-scoped read-block DoGet ticket via a CO->CP
               callback), materializes the seeded SortMeRNA reference to a FASTA,
               runs trim -> derep -> SortMeRNA rRNA filter -> UCHIME -> MAFFT ->
               deblur, mint-features mints a feature_idx per ASV, and
               register-files loads `amplicon_membership`.

Why this is not purely in-process like test_reference_add_smoke: both steps call
back to the control plane (`golay_demux` -> POST /sequence-range,
`denoise` -> POST /read/ticket/doget). `make_cp_client(transport=...)` is
injectable for exactly this reason, so the callbacks route to the in-process
`cp_app` over `httpx.ASGITransport` — no uvicorn, no real port. The compute
service-account PAT authenticates them, carrying the same scopes the production
SA holds.

The fixture (tests/data/amplicon_e2e/) is a de-identified 2000-read subset of a
real run (4 EMP barcodes x 500 reads) plus a 200-OTU SortMeRNA reference; the
pipeline runs in-process on macOS via the staged miint extension, so the ASV
counts asserted below are reproducible without a Linux/SLURM stack.
"""

from __future__ import annotations

import json
import secrets
import shutil
import uuid
from pathlib import Path

import pytest
from httpx import ASGITransport
from qiita_common.api_paths import LOOPBACK_HOST

from _runner_helpers import LocalComputeBackendClient
from conftest import ducklake_connect

_REPO_ROOT = Path(__file__).parent.parent.parent
_WORKFLOWS = _REPO_ROOT / "workflows"
_FIXTURE = Path(__file__).parent.parent / "data" / "amplicon_e2e"

# The four EMP Golay barcodes as they appear in I1 (first 12 nt); each RC is a
# clean Golay codeword. Stored `barcodes_are_rc=True`: golay_demux RCs the map
# barcode to compare against the RC of the I1 index, so the map carries the
# I1-orientation sequence. 500 reads carry each.
_BARCODES = (
    "CGTTTATCCGTT",
    "ATGTTAGGGAAT",
    "AGCTATGTATGG",
    "AGCCCGCAAAGG",
)
_READS_PER_SAMPLE = 500
_TOTAL_READS = _READS_PER_SAMPLE * len(_BARCODES)

# Deblur is deterministic over this fixture + reference; pinned from the run.
_EXPECTED_MEMBERSHIP_ROWS = 213
_EXPECTED_FEATURES = 124

_TRIM = 150


def _read_fasta_sequences(path: Path) -> list[str]:
    """Return the sequence strings from a FASTA via miint's read_fastx (which
    reads .gz natively), rather than a hand-rolled parser."""
    from qiita_compute_orchestrator.miint import open_miint_conn

    with open_miint_conn() as conn:
        return [
            row[0]
            for row in conn.execute(
                "SELECT sequence1 FROM read_fastx(?)", [str(path)]
            ).fetchall()
        ]


# The native tail of the shipped golay-demux workflow (golay_demux ->
# register-files) with the two bcl-convert container steps dropped and
# convert_dir promoted to a context input, so it runs under LocalBackend. The
# shipped 4-step YAML's validity is covered by test_actions_loader; here we only
# need to exercise the demux + ingest + register chain the container feeds.
_GOLAY_DEMUX_E2E_YAML = """\
action_id: golay-demux
version: 1.0.0
target_kind: sequenced_pool
description: >
  Test-only trimmed golay-demux (native tail) driven against a synthesized
  bcl_convert convert_dir. Not the shipped workflow.
scopes: []
audience:
  service: false
  human_roles: [wet_lab_admin, system_admin]
context_schema:
  type: object
  required: [convert_dir, barcode_map]
  properties:
    convert_dir:
      type: string
      minLength: 1
      pattern: "^/"
    barcode_map:
      type: array
      minItems: 1
      items:
        type: object
        required: [prep_sample_idx, barcode, barcodes_are_rc]
        properties:
          prep_sample_idx: {type: integer, minimum: 1}
          barcode: {type: string, minLength: 1}
          barcodes_are_rc: {type: boolean}
    golay_error_threshold:
      type: number
      minimum: 0
      default: 1.5
steps:
  - step: golay_demux
    step_type: singleton
    module: qiita_compute_orchestrator.jobs.golay_demux
    inputs: [convert_dir, barcode_map, reads_staging_root]
    params: {golay_error_threshold: golay_error_threshold}
    outputs: [read_staging_dir]
    baseline_resources:
      cpu: 8
      mem_gb: 32
      walltime: PT4H
  - action: register-files
    inputs: [read_staging_dir]
    outputs: []
action_ceiling:
  cpu: 8
  mem_gb: 56
  walltime: PT8H
"""


async def _sync_workflow_yaml(
    postgres_pool, tmp_dir: Path, name: str, content: str
) -> None:
    """Write `content` as workflows/<name>/1.0.0.yaml under a temp tree and sync it
    into qiita.action."""
    from qiita_control_plane.actions import load_actions, sync_actions

    dest = tmp_dir / "workflows" / name
    dest.mkdir(parents=True)
    (dest / "1.0.0.yaml").write_text(content)
    actions = load_actions(tmp_dir / "workflows")
    async with postgres_pool.acquire() as conn:
        await sync_actions(conn, actions)


@pytest.fixture
async def synced_amplicon_actions(postgres_pool, tmp_path):
    """Sync the trimmed golay-demux (native tail) + the real amplicon workflow into
    qiita.action; clean both up after."""
    await _sync_workflow_yaml(
        postgres_pool, tmp_path / "golay", "golay-demux", _GOLAY_DEMUX_E2E_YAML
    )
    await _sync_workflow_yaml(
        postgres_pool,
        tmp_path / "amp",
        "amplicon",
        (_WORKFLOWS / "amplicon" / "1.0.0.yaml").read_text(),
    )
    yield
    for action_id in ("golay-demux", "amplicon"):
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE action_id = $1", action_id
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.action WHERE action_id = $1", action_id
        )


@pytest.fixture
async def seeded_pool(postgres_pool, human_admin_session):
    """A sequenced_pool with one prep_sample per barcode; reverse-FK cleanup."""
    from qiita_control_plane.testing.db_seeds import (
        seed_biosample_with_sequenced_prep_sample,
        seed_sequenced_sample_subtype,
    )

    owner = human_admin_session["principal_idx"]
    samples: list[tuple[int, int, int]] = []
    run_idx = pool_idx = None
    for _ in _BARCODES:
        (
            biosample_idx,
            prep_sample_idx,
        ) = await seed_biosample_with_sequenced_prep_sample(
            postgres_pool, owner_idx=owner
        )
        run_idx, pool_idx, ss_idx = await seed_sequenced_sample_subtype(
            postgres_pool,
            prep_sample_idx=prep_sample_idx,
            owner_idx=owner,
            sequenced_pool_item_id=f"amplicon-e2e-{secrets.token_hex(4)}",
            sequencing_run_idx=run_idx,
            sequenced_pool_idx=pool_idx,
        )
        samples.append((biosample_idx, prep_sample_idx, ss_idx))

    yield {"pool_idx": pool_idx, "run_idx": run_idx, "samples": samples}

    prep_idxs = [p for _, p, _ in samples]
    bio_idxs = [b for b, _, _ in samples]
    # Work tickets FK the pool; drop them before the pool they scope.
    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE sequenced_pool_idx = $1", pool_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.sequence_range WHERE prep_sample_idx = ANY($1::bigint[])",
        prep_idxs,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.sequenced_sample WHERE sequenced_pool_idx = $1", pool_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", prep_idxs
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", bio_idxs
    )


@pytest.fixture
async def sortmerna_reference(postgres_pool, human_admin_session, data_plane):
    """An ACTIVE `sequence_reference` whose 200 OTU sequences live in the lake.

    The denoise resolver DoGets `reference_sequence_chunks` (joined to
    `reference_membership` by feature_idx) and reassembles a FASTA, so both lake
    tables must carry the real bytes — unlike seed_reference_with_sequences,
    which seeds Postgres only. `reference_sequences` is not read on this path, so
    it is left out.
    """
    owner = human_admin_session["principal_idx"]
    sequences = _read_fasta_sequences(_FIXTURE / "sortmerna_16s_ref.fasta.gz")

    reference_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', 'active', $2) RETURNING reference_idx",
        f"sortmerna-16s-e2e-{uuid.uuid4()}",
        owner,
    )
    feature_idxs: list[int] = []
    async with postgres_pool.acquire() as conn, conn.transaction():
        for _ in sequences:
            feature_idxs.append(
                await conn.fetchval(
                    "INSERT INTO qiita.feature (sequence_hash) VALUES (gen_random_uuid())"
                    " RETURNING feature_idx"
                )
            )

    lake = ducklake_connect(data_plane["data_path"])
    try:
        lake.executemany(
            "INSERT INTO qiita_lake.reference_membership VALUES (?, ?)",
            [(reference_idx, fi) for fi in feature_idxs],
        )
        lake.executemany(
            "INSERT INTO qiita_lake.reference_sequence_chunks"
            " (feature_idx, chunk_index, chunk_data) VALUES (?, 0, ?)",
            [(fi, seq) for fi, seq in zip(feature_idxs, sequences, strict=True)],
        )
    finally:
        lake.close()

    yield reference_idx

    lake = ducklake_connect(data_plane["data_path"])
    try:
        lake.execute(
            "DELETE FROM qiita_lake.reference_sequence_chunks WHERE feature_idx = ANY(?)",
            [feature_idxs],
        )
        lake.execute(
            "DELETE FROM qiita_lake.reference_membership WHERE reference_idx = ?",
            [reference_idx],
        )
    finally:
        lake.close()
    await postgres_pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )


@pytest.fixture
def co_cp_bridge(
    monkeypatch, postgres_pool, signing_key, data_plane, compute_worker_service_account
):
    """Wire the two CO->CP callbacks the steps make to the in-process cp_app.

    Installs CO Settings (so get_settings() in the job resolves the CP url +
    service-account PAT + the data-plane origin), configures cp_app.state the way
    the dispatch route would, and patches make_cp_client at the two job import
    sites to inject an ASGITransport over cp_app — no uvicorn.
    """
    from qiita_compute_orchestrator import cp_client as _cp_client
    from qiita_compute_orchestrator.config import Settings as COSettings
    from qiita_compute_orchestrator.config import _settings_ctx
    from qiita_control_plane.config import Settings as CPSettings
    from qiita_control_plane.main import app as cp_app

    dp_url = f"grpc://{LOOPBACK_HOST}:{data_plane['port']}"

    cp_app.state.pool = postgres_pool
    cp_app.state.settings = CPSettings(
        database_url="unused-in-test",
        flight_signing_key=signing_key,
        data_plane_url=dp_url,
        path_scratch_staging=Path(data_plane["upload_staging_root"]),
        path_scratch_ticket=Path(data_plane["workspace_root"]),
    )
    cp_app.state.compute_backend_client = LocalComputeBackendClient()
    cp_app.state.running_dispatches = set()

    # install_settings has no public "uninstall"; set/reset the ContextVar
    # directly so a get_settings() in a later test doesn't see these test values.
    ctx_token = _settings_ctx.set(
        COSettings(
            backend_type="local",
            path_scratch=str(data_plane["workspace_root"]),
            path_derived=str(data_plane["workspace_root"]),
            cp_to_co_token="unused-in-test",
            cp_url="http://test",
            co_to_cp_token=compute_worker_service_account["token"],
            data_plane_url=dp_url,
        )
    )

    real_make_cp_client = _cp_client.make_cp_client

    def _bridged(*, transport=None):
        return real_make_cp_client(transport=ASGITransport(app=cp_app))

    for module in ("jobs.golay_demux", "data_plane_client"):
        monkeypatch.setattr(
            f"qiita_compute_orchestrator.{module}.make_cp_client", _bridged
        )

    yield {"data_plane_url": dp_url, "cp_app": cp_app}

    _settings_ctx.reset(ctx_token)


def _build_convert_dir(dest: Path) -> Path:
    """Synthesize bcl_convert's output: the pool's Undetermined I1/R1/R2 FASTQs
    under `dest`, named as bcl-convert writes them. golay_demux's `_find_undetermined`
    globs these; read_fastx reads .gz directly, so no decompression is needed."""
    dest.mkdir(parents=True, exist_ok=True)
    for tag, src in (
        ("I1", "I1.fastq.gz"),
        ("R1", "R1.fastq.gz"),
        ("R2", "R2.fastq.gz"),
    ):
        shutil.copy(_FIXTURE / src, dest / f"Undetermined_S0_L001_{tag}_001.fastq.gz")
    return dest


async def _run(
    postgres_pool, data_plane, *, action_id, pool_idx, owner, action_context
) -> int:
    """Seed a sequenced_pool work_ticket and drive it through the runner."""
    from qiita_control_plane.runner import run_workflow

    work_ticket_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, sequenced_pool_idx, action_context"
        ") VALUES ($1, '1.0.0', $2, 'sequenced_pool', $3, $4::jsonb)"
        " RETURNING work_ticket_idx",
        action_id,
        owner,
        pool_idx,
        json.dumps(action_context),
    )
    await run_workflow(
        work_ticket_idx,
        postgres_pool,
        LocalComputeBackendClient(),  # type: ignore[arg-type]  # protocol-shaped duck
        signing_key=data_plane["secret"],
        data_plane_url=f"grpc://{LOOPBACK_HOST}:{data_plane['port']}",
        work_ticket_workspace_root=Path(data_plane["workspace_root"]),
        upload_staging_root=Path(data_plane["upload_staging_root"]),
    )
    return work_ticket_idx


async def test_golay_demux_then_amplicon(
    postgres_pool,
    data_plane,
    synced_amplicon_actions,
    seeded_pool,
    sortmerna_reference,
    co_cp_bridge,
    human_admin_session,
    tmp_path,
):
    owner = human_admin_session["principal_idx"]
    pool_idx = seeded_pool["pool_idx"]
    prep_idxs = [p for _, p, _ in seeded_pool["samples"]]

    # Synthesize bcl_convert's Undetermined I1/R1/R2 output (the container step
    # LocalBackend can't run) so the trimmed golay-demux consumes a real convert_dir.
    convert_dir = _build_convert_dir(tmp_path / "convert")

    barcode_map = [
        {"prep_sample_idx": prep_idx, "barcode": barcode, "barcodes_are_rc": True}
        for prep_idx, barcode in zip(prep_idxs, _BARCODES, strict=True)
    ]

    # 1. golay-demux -> per-sample reads land in the DuckLake `read` table.
    golay_ticket = await _run(
        postgres_pool,
        data_plane,
        action_id="golay-demux",
        pool_idx=pool_idx,
        owner=owner,
        action_context={
            "convert_dir": str(convert_dir),
            "barcode_map": barcode_map,
        },
    )
    assert (
        await postgres_pool.fetchval(
            "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
            golay_ticket,
        )
        == "completed"
    )

    # Every read matched a barcode (the fixture is 4 clean codewords x 500), so
    # the pool carries one sequence_range per sample and 2000 reads total.
    ranges = await postgres_pool.fetch(
        "SELECT prep_sample_idx FROM qiita.sequence_range WHERE prep_sample_idx = ANY($1::bigint[])",
        prep_idxs,
    )
    assert {r["prep_sample_idx"] for r in ranges} == set(prep_idxs)

    lake = ducklake_connect(data_plane["data_path"])
    try:
        (n_reads,) = lake.execute(
            "SELECT count(*) FROM qiita_lake.read WHERE prep_sample_idx = ANY(?)",
            [prep_idxs],
        ).fetchone()
        assert n_reads == _TOTAL_READS
        per_sample = dict(
            lake.execute(
                "SELECT prep_sample_idx, count(*) FROM qiita_lake.read"
                " WHERE prep_sample_idx = ANY(?) GROUP BY prep_sample_idx",
                [prep_idxs],
            ).fetchall()
        )
        assert per_sample == {p: _READS_PER_SAMPLE for p in prep_idxs}
    finally:
        lake.close()

    # 2. amplicon -> denoise streams the pool's reads, deblur -> amplicon_membership.
    amplicon_ticket = await _run(
        postgres_pool,
        data_plane,
        action_id="amplicon",
        pool_idx=pool_idx,
        owner=owner,
        action_context={
            "sortmerna_reference_idx": sortmerna_reference,
            "trim": _TRIM,
        },
    )
    assert (
        await postgres_pool.fetchval(
            "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
            amplicon_ticket,
        )
        == "completed"
    )

    # amplicon_membership carries the runner-minted processing_idx; scope the
    # ASV-yield assertions by this pool's prep_samples (unique to this test) and
    # confirm every row shares the one processing_idx the run minted.
    lake = ducklake_connect(data_plane["data_path"])
    try:
        rows = lake.execute(
            "SELECT prep_sample_idx, processing_idx, feature_idx, count"
            " FROM qiita_lake.amplicon_membership WHERE prep_sample_idx = ANY(?)",
            [prep_idxs],
        ).fetchall()
    finally:
        lake.close()

    assert len(rows) == _EXPECTED_MEMBERSHIP_ROWS
    assert len({r[2] for r in rows}) == _EXPECTED_FEATURES
    assert {r[0] for r in rows} == set(prep_idxs)
    assert len({r[1] for r in rows}) == 1  # one processing_idx for the whole run
    assert all(r[3] > 0 for r in rows)
