"""DB tests for the persist-read-metrics library primitive (#142).

`persist_read_metrics` writes the three per-stage read counts (raw / biological /
quality-filtered, both-mates r1r2) onto the 1:1 sequenced_sample for a
prep_sample. It is the in-process action fastq-to-parquet/1.2.0 runs after
host_filter, consuming the read_count.json sidecars #141 emits.

Each test seeds its own principal -> biosample -> prep_sample chain (and, where
needed, the sequenced_sample subtype) so cleanup is FK-reverse and order-stable
on the shared postgres_pool fixture.
"""

import secrets

import asyncpg
import pytest
import pytest_asyncio

from qiita_control_plane.actions.library import persist_read_metrics
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
    seed_user_principal,
)

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def chain(postgres_pool):
    """Seed one principal + biosample + sequenced prep_sample; yield a context
    with a `seed_subtype()` helper that attaches the run -> pool ->
    sequenced_sample subtype on demand and records idxs for FK-reverse cleanup."""
    principal_idx = await seed_user_principal(postgres_pool, prefix="prm-test", suffix="owner")
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

    # FK-reverse cleanup: sequenced_sample -> sequenced_pool -> sequencing_run,
    # then prep_sample (cascades sequence_range), biosample, user, principal.
    for run_idx, pool_idx, ss_idx in subtypes:
        await postgres_pool.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
        await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
        await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", prep_sample_idx)
    await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx)
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


async def _read_counts(pool, ss_idx):
    return await pool.fetchrow(
        "SELECT raw_read_count_r1r2, biological_read_count_r1r2,"
        " quality_filtered_read_count_r1r2 FROM qiita.sequenced_sample WHERE idx = $1",
        ss_idx,
    )


async def test_persist_writes_three_counts(chain):
    ss_idx = await chain["seed_subtype"]()
    returned = await persist_read_metrics(
        chain["pool"],
        chain["prep_sample_idx"],
        raw_read_count_r1r2=1000,
        biological_read_count_r1r2=900,
        quality_filtered_read_count_r1r2=850,
    )
    assert returned == ss_idx
    row = await _read_counts(chain["pool"], ss_idx)
    assert row["raw_read_count_r1r2"] == 1000
    assert row["biological_read_count_r1r2"] == 900
    assert row["quality_filtered_read_count_r1r2"] == 850


async def test_persist_is_idempotent(chain):
    """A workflow retried from the start re-runs the primitive; the second write
    overwrites with the same counts and returns the same idx."""
    ss_idx = await chain["seed_subtype"]()
    first = await persist_read_metrics(chain["pool"], chain["prep_sample_idx"], 1000, 900, 850)
    second = await persist_read_metrics(chain["pool"], chain["prep_sample_idx"], 1000, 900, 850)
    assert first == second == ss_idx
    row = await _read_counts(chain["pool"], ss_idx)
    assert row["quality_filtered_read_count_r1r2"] == 850


async def test_persist_pass_through_equal_counts(chain):
    """Host filtering disabled → quality_filtered == biological; the monotonic
    CHECK allows equality."""
    ss_idx = await chain["seed_subtype"]()
    await persist_read_metrics(chain["pool"], chain["prep_sample_idx"], 1000, 800, 800)
    row = await _read_counts(chain["pool"], ss_idx)
    assert row["biological_read_count_r1r2"] == row["quality_filtered_read_count_r1r2"] == 800


async def test_persist_fails_fast_without_sequenced_sample(chain):
    """A sequenced prep_sample with no sequenced_sample subtype is an ordering
    bug, not a benign skip — the primitive raises rather than silently no-op'ing."""
    with pytest.raises(RuntimeError, match="no sequenced_sample row"):
        await persist_read_metrics(chain["pool"], chain["prep_sample_idx"], 1000, 900, 850)


async def test_persist_rejects_non_monotonic_counts(chain):
    """biological > raw violates the DB CHECK (each stage only drops reads), so
    a swapped/garbled count fails loudly at write time."""
    await chain["seed_subtype"]()
    with pytest.raises(asyncpg.exceptions.CheckViolationError):
        await persist_read_metrics(chain["pool"], chain["prep_sample_idx"], 900, 1000, 850)
