"""DB test for fetch_sequenced_pool_sample_exceptions' retired/flagged exclusion.

The flag-derivation logic itself (`_sequenced_sample_exception_flags`) is pinned
without a DB in `tests/test_exception_flags.py`; this covers only the SQL's
sample-scope predicate, which that pure-unit suite cannot reach.
"""

import secrets

import pytest
import pytest_asyncio

from qiita_control_plane.repositories.sequencing_run import (
    fetch_sequenced_pool_sample_exceptions,
)
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def pool_ctx(postgres_pool):
    owner_idx = await seed_user_principal(postgres_pool, prefix="poolexc", suffix="owner")
    run_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequencing_run (instrument_run_id, platform, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, $2) RETURNING idx",
        f"pe-run-{secrets.token_hex(4)}",
        owner_idx,
    )
    pool_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequenced_pool (sequencing_run_idx, created_by_idx)"
        " VALUES ($1, $2) RETURNING idx",
        run_idx,
        owner_idx,
    )
    samples: list[tuple[int, int, int]] = []

    async def add_sample(*, retired=False, ena_status=None):
        """A sample with no usable reads (raw NULL) -- always anomalous unless
        excluded by scope -- so a returned row means "not excluded"."""
        bs_idx, ps_idx = await seed_biosample_with_sequenced_prep_sample(
            postgres_pool, owner_idx=owner_idx
        )
        ss_idx = await postgres_pool.fetchval(
            "INSERT INTO qiita.sequenced_sample"
            "  (prep_sample_idx, sequenced_pool_idx, sequenced_pool_item_id, created_by_idx)"
            " VALUES ($1, $2, $3, $4) RETURNING idx",
            ps_idx,
            pool_idx,
            f"item-{secrets.token_hex(4)}",
            owner_idx,
        )
        if retired:
            await postgres_pool.execute(
                "UPDATE qiita.prep_sample SET retired = true, retired_by_idx = $2,"
                " retired_at = now(), retire_reason = 'test' WHERE idx = $1",
                ps_idx,
                owner_idx,
            )
        if ena_status is not None:
            await postgres_pool.execute(
                "UPDATE qiita.sequenced_sample SET ena_status = $2,"
                " ena_availability_checked_at = now() WHERE idx = $1",
                ss_idx,
                ena_status,
            )
        samples.append((bs_idx, ps_idx, ss_idx))
        return ps_idx

    yield {"pool": postgres_pool, "pool_idx": pool_idx, "add_sample": add_sample}

    for _bs, _ps, ss_idx in samples:
        await postgres_pool.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    for _bs, ps_idx, _ss in samples:
        await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", ps_idx)
    for bs_idx, _ps, _ss in samples:
        await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", bs_idx)
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", owner_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", owner_idx)


async def test_retired_sample_excluded(pool_ctx):
    ps_anomalous = await pool_ctx["add_sample"]()
    await pool_ctx["add_sample"](retired=True)
    rows = await fetch_sequenced_pool_sample_exceptions(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert [r["prep_sample_idx"] for r in rows] == [ps_anomalous]


async def test_flagged_sample_excluded(pool_ctx):
    ps_anomalous = await pool_ctx["add_sample"]()
    await pool_ctx["add_sample"](ena_status="suppressed")
    rows = await fetch_sequenced_pool_sample_exceptions(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert [r["prep_sample_idx"] for r in rows] == [ps_anomalous]
