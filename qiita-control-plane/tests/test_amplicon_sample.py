"""The amplicon completion gate, `qiita.amplicon_sample`.

The runner materializes 'pending' for the pool's population at the run-identity
mint; `finalize-amplicon-sample` closes it from denoise's `asv_counts` —
'completed' with an ASV count for each counted sample, 'no_data' for the rest.
"""

import uuid
from pathlib import Path

import duckdb
import pytest

from qiita_control_plane.actions.library import finalize_amplicon_sample
from qiita_control_plane.repositories.amplicon import (
    close_amplicon_samples,
    create_amplicon_samples_pending,
)
from qiita_control_plane.repositories.processing import mint_processing
from qiita_control_plane.runner._processing import _create_amplicon_gate_pending
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
    seed_user_principal,
)
from qiita_control_plane.testing.db_teardown import delete_principal, teardown_entity_graph

pytestmark = pytest.mark.db


@pytest.fixture
async def run(postgres_pool):
    """Three sequenced prep_samples on one pool, and an amplicon processing run."""
    owner = await seed_user_principal(
        postgres_pool, prefix="amp-gate", suffix=str(uuid.uuid4())[:8]
    )
    samples, run_idx, pool_idx = [], None, None
    for i in range(3):
        bs, ps = await seed_biosample_with_sequenced_prep_sample(postgres_pool, owner_idx=owner)
        run_idx, pool_idx, _ = await seed_sequenced_sample_subtype(
            postgres_pool,
            prep_sample_idx=ps,
            owner_idx=owner,
            sequenced_pool_item_id=f"amp-gate-{i}",
            sequencing_run_idx=run_idx,
            sequenced_pool_idx=pool_idx,
        )
        samples.append((bs, ps))
    async with postgres_pool.acquire() as conn:
        processing = await mint_processing(
            conn,
            workflow="amplicon",
            version="1.1.0",
            params={"workflow": "amplicon", "version": "1.1.0", "probe": str(uuid.uuid4())},
        )
    yield {
        "processing_idx": processing["processing_idx"],
        "pool_idx": pool_idx,
        "prep_sample_idxs": sorted(ps for _, ps in samples),
    }
    await postgres_pool.execute(
        "DELETE FROM qiita.amplicon_sample WHERE processing_idx = $1",
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
    await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await delete_principal(postgres_pool, [owner])


async def _states(pool, processing_idx):
    rows = await pool.fetch(
        "SELECT prep_sample_idx, state, asv_count FROM qiita.amplicon_sample"
        " WHERE processing_idx = $1 ORDER BY prep_sample_idx",
        processing_idx,
    )
    return {r["prep_sample_idx"]: (r["state"], r["asv_count"]) for r in rows}


def _write_asv_counts(dir_: Path, asvs_per_sample: dict[int, int]) -> Path:
    """The asv_counts parquet denoise writes: one row per (sample, ASV)."""
    out = dir_ / "asv_counts.parquet"
    rows = ", ".join(
        f"({ps}::BIGINT, uuid(), 3::BIGINT)"
        for ps, n in sorted(asvs_per_sample.items())
        for _ in range(n)
    )
    duckdb.sql(
        f"COPY (SELECT * FROM (VALUES {rows}) t(prep_sample_idx, sequence_hash, count))"
        f" TO '{out}' (FORMAT parquet)"
    )
    return out


async def test_the_runner_marks_the_pools_population_pending(postgres_pool, run):
    await _create_amplicon_gate_pending(
        postgres_pool, processing_idx=run["processing_idx"], sequenced_pool_idx=run["pool_idx"]
    )
    assert await _states(postgres_pool, run["processing_idx"]) == {
        ps: ("pending", None) for ps in run["prep_sample_idxs"]
    }


async def test_finalize_completes_counted_samples_and_closes_the_rest(postgres_pool, run, tmp_path):
    a, b, c = run["prep_sample_idxs"]
    await _create_amplicon_gate_pending(
        postgres_pool, processing_idx=run["processing_idx"], sequenced_pool_idx=run["pool_idx"]
    )
    written = await finalize_amplicon_sample(
        postgres_pool,
        processing_idx=run["processing_idx"],
        prep_sample_idxs=run["prep_sample_idxs"],
        asv_counts_path=_write_asv_counts(tmp_path, {a: 2, c: 5}),
    )
    assert written == (2, 1)
    assert await _states(postgres_pool, run["processing_idx"]) == {
        a: ("completed", 2),
        b: ("no_data", None),
        c: ("completed", 5),
    }


async def test_a_rerun_reopens_no_data_but_keeps_completed(postgres_pool, run):
    a, b, _ = run["prep_sample_idxs"]
    async with postgres_pool.acquire() as conn, conn.transaction():
        await create_amplicon_samples_pending(
            conn, processing_idx=run["processing_idx"], prep_sample_idxs=[a, b]
        )
        await close_amplicon_samples(
            conn, processing_idx=run["processing_idx"], prep_sample_idxs=[a, b], asv_counts={a: 4}
        )
        await create_amplicon_samples_pending(
            conn, processing_idx=run["processing_idx"], prep_sample_idxs=[a, b]
        )
    assert await _states(postgres_pool, run["processing_idx"]) == {
        a: ("completed", 4),
        b: ("pending", None),
    }


async def test_a_zero_count_is_refused(postgres_pool, run):
    a = run["prep_sample_idxs"][0]
    async with postgres_pool.acquire() as conn, conn.transaction():
        with pytest.raises(ValueError, match="at least one ASV"):
            await close_amplicon_samples(
                conn, processing_idx=run["processing_idx"], prep_sample_idxs=[a], asv_counts={a: 0}
            )


async def test_closing_leaves_another_pools_pending_rows_alone(postgres_pool, run):
    """Two pools denoised alike share the processing_idx; one ticket's close must
    not mark the other pool's still-running samples 'no_data'."""
    a, b, other = run["prep_sample_idxs"]
    async with postgres_pool.acquire() as conn, conn.transaction():
        await create_amplicon_samples_pending(
            conn, processing_idx=run["processing_idx"], prep_sample_idxs=[a, b, other]
        )
        written = await close_amplicon_samples(
            conn, processing_idx=run["processing_idx"], prep_sample_idxs=[a, b], asv_counts={a: 1}
        )
    assert written == (1, 1)
    assert await _states(postgres_pool, run["processing_idx"]) == {
        a: ("completed", 1),
        b: ("no_data", None),
        other: ("pending", None),
    }
