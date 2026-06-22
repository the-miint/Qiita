"""DB tests for the persist-qc-report library primitive.

`persist_qc_report` writes the two fastqc-equivalent QC reports (raw / filtered
qc_report.json) onto the 1:1 sequenced_sample for a prep_sample as JSONB. It is
the in-process action fastq-to-parquet/1.2.0 runs after persist-read-metrics,
consuming the qc_report.json sidecars from the qc_report_raw / qc_report_filtered
steps.

Each test seeds its own principal -> biosample -> prep_sample chain (and, where
needed, the sequenced_sample subtype) so cleanup is FK-reverse and order-stable
on the shared postgres_pool fixture.
"""

import json
import secrets

import pytest
import pytest_asyncio

from qiita_control_plane.actions.library import persist_qc_report
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
    seed_user_principal,
)

pytestmark = pytest.mark.db


_RAW_REPORT = {
    "point": "raw",
    "layout": "paired",
    "read_pairs": 100,
    "mates": {
        "r1": {
            "reads": 100,
            "total_bases": 10000,
            "mean_quality": 35.0,
            "gc_content": 0.5,
            "n_content": 0.0,
            "min_length": 100,
            "max_length": 100,
            "mean_length": 100.0,
            "quality_histogram": {"35": 100},
            "gc_histogram": {"50": 100},
            "length_histogram": {"100": 100},
        },
        "r2": None,
    },
}
_FILTERED_REPORT = {**_RAW_REPORT, "point": "filtered", "read_pairs": 90}


@pytest_asyncio.fixture
async def chain(postgres_pool):
    """Seed one principal + biosample + sequenced prep_sample; yield a context
    with a `seed_subtype()` helper that attaches the run -> pool ->
    sequenced_sample subtype on demand and records idxs for FK-reverse cleanup."""
    principal_idx = await seed_user_principal(postgres_pool, prefix="pqc-test", suffix="owner")
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    subtypes: list[tuple[int, int, int]] = []

    async def seed_subtype() -> int:
        run_idx, pool_idx, ss_idx = await seed_sequenced_sample_subtype(
            postgres_pool,
            prep_sample_idx=prep_sample_idx,
            owner_idx=principal_idx,
            sequenced_pool_item_id=f"item-{secrets.token_hex(4)}",
        )
        subtypes.append((run_idx, pool_idx, ss_idx))
        return ss_idx

    yield {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "prep_sample_idx": prep_sample_idx,
        "seed_subtype": seed_subtype,
    }

    for run_idx, pool_idx, ss_idx in subtypes:
        await postgres_pool.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
        await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
        await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", prep_sample_idx)
    await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx)
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


async def _reports(pool, ss_idx):
    row = await pool.fetchrow(
        "SELECT raw_qc_report, filtered_qc_report FROM qiita.sequenced_sample WHERE idx = $1",
        ss_idx,
    )
    # asyncpg returns JSONB as text — decode for comparison.
    return (
        json.loads(row["raw_qc_report"]) if row["raw_qc_report"] else None,
        json.loads(row["filtered_qc_report"]) if row["filtered_qc_report"] else None,
    )


async def test_persist_writes_both_reports(chain):
    ss_idx = await chain["seed_subtype"]()
    returned = await persist_qc_report(
        chain["pool"], chain["prep_sample_idx"], _RAW_REPORT, _FILTERED_REPORT
    )
    assert returned == ss_idx
    raw, filtered = await _reports(chain["pool"], ss_idx)
    assert raw == _RAW_REPORT
    assert filtered == _FILTERED_REPORT


async def test_persist_is_idempotent(chain):
    """A workflow retried from the start re-runs the primitive; the second write
    overwrites with the same reports and returns the same idx."""
    ss_idx = await chain["seed_subtype"]()
    first = await persist_qc_report(
        chain["pool"], chain["prep_sample_idx"], _RAW_REPORT, _FILTERED_REPORT
    )
    second = await persist_qc_report(
        chain["pool"], chain["prep_sample_idx"], _RAW_REPORT, _FILTERED_REPORT
    )
    assert first == second == ss_idx
    raw, _ = await _reports(chain["pool"], ss_idx)
    assert raw["read_pairs"] == 100


async def test_persist_fails_fast_without_sequenced_sample(chain):
    """A sequenced prep_sample with no sequenced_sample subtype is an ordering
    bug, not a benign skip — the primitive raises rather than silently no-op'ing."""
    with pytest.raises(RuntimeError, match="no sequenced_sample row"):
        await persist_qc_report(
            chain["pool"], chain["prep_sample_idx"], _RAW_REPORT, _FILTERED_REPORT
        )
