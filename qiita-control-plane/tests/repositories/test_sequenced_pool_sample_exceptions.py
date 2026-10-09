"""DB test for fetch_sequenced_pool_sample_exceptions' retired/flagged exclusion.

The flag-derivation logic itself (`_sequenced_sample_exception_flags`) is pinned
without a DB in `tests/test_exception_flags.py`; this covers only the SQL's
sample-scope predicate, which that pure-unit suite cannot reach.
"""

import pytest

from qiita_control_plane.repositories.sequencing_run import (
    fetch_sequenced_pool_sample_exceptions,
)

pytestmark = pytest.mark.db


async def test_retired_sample_excluded(pool_ctx):
    ps_anomalous = (await pool_ctx["add_sample"]()).prep_sample_idx
    await pool_ctx["add_sample"](retired=True)
    rows = await fetch_sequenced_pool_sample_exceptions(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert [r["prep_sample_idx"] for r in rows] == [ps_anomalous]


async def test_flagged_sample_excluded(pool_ctx):
    ps_anomalous = (await pool_ctx["add_sample"]()).prep_sample_idx
    await pool_ctx["add_sample"](ena_status="suppressed")
    rows = await fetch_sequenced_pool_sample_exceptions(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert [r["prep_sample_idx"] for r in rows] == [ps_anomalous]
