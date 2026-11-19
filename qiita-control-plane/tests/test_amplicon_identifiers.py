"""Amplicon processed samples get their public QM identifiers at load time.

`mint-amplicon-identifiers` runs after the amplicon workflow's register-files and
mints one `qiita.exported_identifier` per (processing_idx, prep_sample) denoise
counted, so a feature table never needs a mint on read.
"""

import uuid
from pathlib import Path

import duckdb
import pytest
from qiita_common.actions import PROCESSING_IDX_BINDING, WorkflowAction

from qiita_control_plane.actions.library import mint_amplicon_identifiers
from qiita_control_plane.repositories.exported_identifier import (
    mint_processing_exported_identifiers,
)
from qiita_control_plane.repositories.processing import mint_processing
from qiita_control_plane.testing.db_seeds import (
    delete_action_if_created,
    seed_action_if_absent,
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)
from qiita_control_plane.testing.db_teardown import delete_principal, teardown_entity_graph

pytestmark = pytest.mark.db


@pytest.fixture
async def run(postgres_pool):
    """A user, two sequenced prep_samples, and an amplicon processing run."""
    principal_idx = await seed_user_principal(
        postgres_pool, prefix="amp-qm", suffix=str(uuid.uuid4())[:8]
    )
    samples = [
        await seed_biosample_with_sequenced_prep_sample(postgres_pool, owner_idx=principal_idx)
        for _ in range(2)
    ]
    async with postgres_pool.acquire() as conn:
        processing = await mint_processing(
            conn,
            workflow="amplicon",
            version="1.1.0",
            params={"workflow": "amplicon", "version": "1.1.0", "probe": str(uuid.uuid4())},
        )
    yield {
        "principal_idx": principal_idx,
        "prep_sample_idxs": sorted(ps for _, ps in samples),
        "processing_idx": processing["processing_idx"],
    }
    await postgres_pool.execute(
        "DELETE FROM qiita.exported_identifier WHERE processing_idx = $1",
        processing["processing_idx"],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.processing WHERE processing_idx = $1", processing["processing_idx"]
    )
    await teardown_entity_graph(
        postgres_pool,
        study_idxs=[],
        biosample_idxs=[b for b, _ in samples],
        prep_sample_idxs=[ps for _, ps in samples],
    )
    await delete_principal(postgres_pool, [principal_idx])


def _write_asv_counts(dir_: Path, prep_sample_idxs: list[int]) -> Path:
    """The asv_counts parquet denoise writes: two ASVs per sample."""
    dir_.mkdir(parents=True, exist_ok=True)
    out = dir_ / "asv_counts.parquet"
    rows = ", ".join(
        f"({ps}::BIGINT, uuid(), 3::BIGINT)" for ps in prep_sample_idxs for _ in range(2)
    )
    duckdb.sql(
        f"COPY (SELECT * FROM (VALUES {rows}) t(prep_sample_idx, sequence_hash, count))"
        f" TO '{out}' (FORMAT parquet)"
    )
    return out


async def test_repository_mint_is_idempotent(postgres_pool, run):
    async with postgres_pool.acquire() as conn:
        first = await mint_processing_exported_identifiers(
            conn,
            processing_idx=run["processing_idx"],
            prep_sample_idxs=run["prep_sample_idxs"],
            created_by_idx=run["principal_idx"],
        )
        second = await mint_processing_exported_identifiers(
            conn,
            processing_idx=run["processing_idx"],
            prep_sample_idxs=run["prep_sample_idxs"],
            created_by_idx=run["principal_idx"],
        )
    assert [r["prep_sample_idx"] for r in first] == run["prep_sample_idxs"]
    assert [r["export_id"] for r in first] == [r["export_id"] for r in second]
    assert all(r["export_id"].startswith("QM") for r in first)


async def test_repository_mint_reissues_a_retired_tuple(postgres_pool, run):
    async with postgres_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO qiita.exported_identifier"
            " (processing_idx, prep_sample_idx, created_by_idx)"
            " VALUES ($1, $2, $3)",
            run["processing_idx"],
            run["prep_sample_idxs"][0],
            run["principal_idx"],
        )
        await conn.execute(
            "UPDATE qiita.exported_identifier SET retired = true, retired_at = now(),"
            " retire_reason = 'probe' WHERE processing_idx = $1",
            run["processing_idx"],
        )
        rows = await mint_processing_exported_identifiers(
            conn,
            processing_idx=run["processing_idx"],
            prep_sample_idxs=run["prep_sample_idxs"],
            created_by_idx=run["principal_idx"],
        )
    # A retired tuple is re-minted with a fresh identifier rather than refused.
    assert len(rows) == 2


async def test_mint_amplicon_identifiers_from_asv_counts(postgres_pool, run, tmp_path):
    counts = _write_asv_counts(tmp_path / "denoise", run["prep_sample_idxs"])
    minted = await mint_amplicon_identifiers(
        postgres_pool,
        processing_idx=run["processing_idx"],
        asv_counts_path=counts,
        created_by_idx=run["principal_idx"],
    )
    assert minted == 2
    rows = await postgres_pool.fetch(
        "SELECT prep_sample_idx FROM qiita.exported_identifier"
        " WHERE processing_idx = $1 AND NOT retired ORDER BY prep_sample_idx",
        run["processing_idx"],
    )
    assert [r["prep_sample_idx"] for r in rows] == run["prep_sample_idxs"]


async def test_runner_arm_mints_as_the_ticket_originator(postgres_pool, run, tmp_path):
    from qiita_control_plane.runner import _run_action_primitive
    from qiita_control_plane.testing.run_context import run_context

    created = await seed_action_if_absent(
        postgres_pool, action_id="amplicon", version="1.1.0", target_kind="sequenced_pool"
    )
    run_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequencing_run (instrument_run_id, platform, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, $2) RETURNING idx",
        f"amp-qm-{uuid.uuid4()}",
        run["principal_idx"],
    )
    pool_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequenced_pool (sequencing_run_idx, created_by_idx)"
        " VALUES ($1, $2) RETURNING idx",
        run_idx,
        run["principal_idx"],
    )
    ticket_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, sequenced_pool_idx, action_context)"
        " VALUES ('amplicon', '1.1.0', $1, 'sequenced_pool', $2, '{}'::jsonb)"
        " RETURNING work_ticket_idx",
        run["principal_idx"],
        pool_idx,
    )
    try:
        counts = _write_asv_counts(tmp_path / "denoise", run["prep_sample_idxs"])
        ctx = run_context(
            pool=postgres_pool,
            scope_target={
                "kind": "sequenced_pool",
                "sequenced_pool_idx": pool_idx,
                "sequencing_run_idx": run_idx,
            },
            work_ticket_idx=ticket_idx,
            signing_key=b"\x00" * 32,
            data_plane_url="grpc://unused:50051",
        )
        out = await _run_action_primitive(
            ctx,
            WorkflowAction(
                kind="action", name="mint-amplicon-identifiers", inputs=["asv_counts"], outputs=[]
            ),
            {"asv_counts": str(counts), PROCESSING_IDX_BINDING: run["processing_idx"]},
            tmp_path,
        )
        assert out == {}
        creators = await postgres_pool.fetch(
            "SELECT DISTINCT created_by_idx FROM qiita.exported_identifier"
            " WHERE processing_idx = $1",
            run["processing_idx"],
        )
        assert [r["created_by_idx"] for r in creators] == [run["principal_idx"]]
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", ticket_idx
        )
        await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
        await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
        await delete_action_if_created(
            postgres_pool, action_id="amplicon", version="1.1.0", created=created
        )
