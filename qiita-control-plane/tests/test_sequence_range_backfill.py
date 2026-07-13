"""The migration's backfill: attribute each existing sequence_range to the ticket
that MINTED it — and to nothing else.

The guard this feeds (`mint_or_reuse_sequence_range`) reuses an orphaned range only
when it was minted by the SAME work_ticket. So a mis-attribution is not a cosmetic
bug: stamping a range with a ticket that merely COLLIDED with it (mint → 409 →
FAILED) makes that ticket "recognise" the range as its own on a later `ticket run`,
reuse it, and register the sample's reads a SECOND time. DuckLake has no uniqueness,
so the duplication is silent and permanent.

The tell is TIME: a range a ticket minted is created AFTER the ticket. A range the
ticket collided with predates it. These tests pin that, by re-running the migration's
UPDATE statements against seeded scenarios.
"""

import secrets
from datetime import UTC, datetime

import pytest
import pytest_asyncio

from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
    seed_user_principal,
)

pytestmark = pytest.mark.db

# The migration's per-sample backfill, verbatim in shape. Kept here (rather than
# re-running the migration file) so the test pins the LOGIC, not dbmate's plumbing.
_BACKFILL = """
WITH candidate AS (
    SELECT sr.idx AS sequence_range_idx, wt.work_ticket_idx
      FROM qiita.sequence_range sr
      JOIN qiita.work_ticket wt
        ON wt.prep_sample_idx = sr.prep_sample_idx
       AND wt.action_id IN ('bam-to-parquet', 'fastq-to-parquet')
     WHERE sr.created_at >= wt.created_at
    UNION ALL
    SELECT sr.idx AS sequence_range_idx, wt.work_ticket_idx
      FROM qiita.sequence_range sr
      JOIN qiita.sequenced_sample ss
        ON ss.prep_sample_idx = sr.prep_sample_idx
      JOIN qiita.work_ticket wt
        ON wt.sequenced_pool_idx = ss.sequenced_pool_idx
       AND wt.action_id = 'bcl-convert'
     WHERE sr.created_at >= wt.created_at
),
unambiguous AS (
    SELECT sequence_range_idx, min(work_ticket_idx) AS work_ticket_idx
      FROM candidate
     GROUP BY sequence_range_idx
    HAVING count(*) = 1
)
UPDATE qiita.sequence_range sr
   SET minted_by_work_ticket_idx = u.work_ticket_idx
  FROM unambiguous u
 WHERE u.sequence_range_idx = sr.idx
"""


def _at(hour: int, minute: int = 0) -> datetime:
    """A timezone-aware instant. asyncpg binds timestamptz from datetime, not str."""
    return datetime(2026, 7, 13, hour, minute, tzinfo=UTC)


@pytest_asyncio.fixture
async def sample(postgres_pool):
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="srbf", suffix=suffix)
    _bs, ps_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    # work_ticket FKs (action_id, action_version) → qiita.action. The test DB has no
    # synced workflows, so register the loader actions the backfill keys on. The
    # VERSION is unique to this fixture: these are stepless stubs, and parking one on
    # a real (action_id, '1.0.0') would both shadow that action for anyone who later
    # submits against it and make the teardown below ambiguous — under ON CONFLICT the
    # fixture cannot know whether the row it would delete is the one it inserted.
    version = f"0.0.0-{suffix}"
    for action_id in ("bam-to-parquet", "fastq-to-parquet"):
        await _seed_action(postgres_pool, action_id, version, "prep_sample")
    yield {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "prep_sample_idx": ps_idx,
        "version": version,
    }

    # Everything this fixture seeds that another test could SEE. The tickets are the
    # load-bearing half: they are seeded terminal with `notified_at IS NULL`, which is
    # exactly the notify sweeper's owed-set predicate, and the sweeper scans the whole
    # database — so a leaked one gets emailed about, failing an innocent test in
    # another file. The DB is isolated per xdist WORKER, not per test.
    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE originator_principal_idx = $1", principal_idx
    )
    # sequence_range.minted_by_work_ticket_idx has no FK, so the DELETE above would
    # otherwise leave ranges pointing at tickets that no longer exist.
    await postgres_pool.execute(
        "DELETE FROM qiita.sequence_range WHERE prep_sample_idx = $1", ps_idx
    )
    await postgres_pool.execute("DELETE FROM qiita.action WHERE version = $1", version)


async def _seed_action(pool, action_id: str, version: str, target_kind: str) -> None:
    """A stepless `qiita.action` row, purely to satisfy work_ticket's FK."""
    await pool.execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling)"
        " VALUES ($1, $2, $3, '{}', '{}'::jsonb, '[]'::jsonb, 1, 1, '1 hour'::interval)",
        action_id,
        version,
        target_kind,
    )


async def _loader_ticket(
    pool, *, prep_sample_idx, principal_idx, action_id, created_at, version, state="completed"
):
    """A prep_sample-scoped loader ticket with an explicit created_at.

    State is irrelevant to the backfill (it keys on action_id + created_at). 'completed'
    is the default because `work_ticket_one_in_flight_per_prep_sample` allows only ONE
    non-terminal ticket per sample, and the ambiguity test needs two."""
    return await pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx, scope_target_kind,"
        "  prep_sample_idx, state, created_at)"
        " VALUES ($1, $2, $3, 'prep_sample', $4,"
        "         $5::qiita.work_ticket_state, $6)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        principal_idx,
        prep_sample_idx,
        state,
        created_at,
    )


async def _pool_ingest(pool, *, prep_sample_idx, principal_idx, created_at, version):
    """Put the sample in a sequenced_pool and give that pool a bcl-convert ticket —
    the POOL-shaped minter (ingest_reads mints one range per sample in the pool).
    Returns (work_ticket_idx, sequenced_pool_idx)."""
    _run, pool_idx, _ss = await seed_sequenced_sample_subtype(
        pool,
        prep_sample_idx=prep_sample_idx,
        owner_idx=principal_idx,
        sequenced_pool_item_id=f"item-{secrets.token_hex(3)}",
    )
    await _seed_action(pool, "bcl-convert", version, "sequenced_pool")
    wt = await pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx, scope_target_kind,"
        "  sequenced_pool_idx, state, created_at)"
        " VALUES ('bcl-convert', $1, $2, 'sequenced_pool', $3,"
        "         'completed'::qiita.work_ticket_state, $4)"
        " RETURNING work_ticket_idx",
        version,
        principal_idx,
        pool_idx,
        created_at,
    )
    return wt, pool_idx


async def _range_at(pool, *, prep_sample_idx, principal_idx, created_at):
    """A sequence_range with an explicit created_at (bypasses the mint fn on purpose:
    these tests are about attribution, not allocation)."""
    return await pool.fetchval(
        "INSERT INTO qiita.sequence_range"
        " (prep_sample_idx, sequence_idx_start, sequence_idx_stop, created_by_idx, created_at)"
        " VALUES ($1, 1, 10, $2, $3)"
        " RETURNING idx",
        prep_sample_idx,
        principal_idx,
        created_at,
    )


async def test_backfill_attributes_a_range_its_ticket_minted(sample):
    """The ordinary case: the ticket was created, then its step minted the range."""
    pool, ps, pr = sample["pool"], sample["prep_sample_idx"], sample["principal_idx"]
    version = sample["version"]
    wt = await _loader_ticket(
        pool,
        version=version,
        prep_sample_idx=ps,
        principal_idx=pr,
        action_id="bam-to-parquet",
        created_at=_at(10, 0),
    )
    await _range_at(pool, prep_sample_idx=ps, principal_idx=pr, created_at=_at(10, 5))

    await pool.execute(_BACKFILL)

    owner = await pool.fetchval(
        "SELECT minted_by_work_ticket_idx FROM qiita.sequence_range WHERE prep_sample_idx = $1",
        ps,
    )
    assert owner == wt


async def test_backfill_refuses_a_ticket_that_only_collided_with_the_range(sample):
    """THE dangerous case, and the reason for the created_at guard.

    The sample's reads were already loaded (a pool ingest minted the range). Someone
    later submitted a per-sample loader for it; that ticket minted → 409 → FAILED. It
    is now the ONLY per-sample loader ticket, so a naive "exactly one loader" backfill
    would stamp the range with it — and a later `ticket run` would then reuse the
    range and duplicate every read.

    The range PREDATES the ticket, so it must stay unattributed.
    """
    pool, ps, pr = sample["pool"], sample["prep_sample_idx"], sample["principal_idx"]
    version = sample["version"]
    # The range was minted first (by the pool ingest, which we don't model here).
    await _range_at(pool, prep_sample_idx=ps, principal_idx=pr, created_at=_at(9, 0))
    # ... and the stray per-sample loader came later and 409'd.
    await _loader_ticket(
        pool,
        version=version,
        prep_sample_idx=ps,
        principal_idx=pr,
        action_id="fastq-to-parquet",
        created_at=_at(11, 0),
    )

    await pool.execute(_BACKFILL)

    owner = await pool.fetchval(
        "SELECT minted_by_work_ticket_idx FROM qiita.sequence_range WHERE prep_sample_idx = $1",
        ps,
    )
    assert owner is None, (
        "a ticket that only COLLIDED with the range must not be recorded as its minter "
        "— reusing the range on its retry would duplicate the sample's reads"
    )


async def test_backfill_leaves_ambiguous_attribution_null(sample):
    """Two candidate minters → cannot attribute → NULL → fails closed."""
    pool, ps, pr = sample["pool"], sample["prep_sample_idx"], sample["principal_idx"]
    version = sample["version"]
    for created in (_at(10, 0), _at(10, 1)):
        await _loader_ticket(
            pool,
            version=version,
            prep_sample_idx=ps,
            principal_idx=pr,
            action_id="bam-to-parquet",
            created_at=created,
        )
    await _range_at(pool, prep_sample_idx=ps, principal_idx=pr, created_at=_at(10, 5))

    await pool.execute(_BACKFILL)

    owner = await pool.fetchval(
        "SELECT minted_by_work_ticket_idx FROM qiita.sequence_range WHERE prep_sample_idx = $1",
        ps,
    )
    assert owner is None


async def test_backfill_refuses_when_a_pool_ingest_also_could_have_minted(sample):
    """The cross-shape case, and the one a per-sample-only count fails OPEN on.

    The sample's reads were loaded by its POOL ingest (bcl-convert -> ingest_reads),
    which mints a range per sample. It also carries a stray per-sample loader ticket
    created BEFORE that — so the range postdates the stray ticket, and a count that
    only looked at per-sample loaders would see exactly one candidate and credit it.
    A later `ticket run` on that stray ticket would then "recognise" the range as its
    own, reuse it, and register the sample's reads a SECOND time.

    Both shapes are candidates, so the count is 2 → ambiguous → NULL → fails closed.
    """
    pool, ps, pr = sample["pool"], sample["prep_sample_idx"], sample["principal_idx"]
    version = sample["version"]

    # A stray per-sample loader, created first (it 409'd and failed, or never ran).
    await _loader_ticket(
        pool,
        version=version,
        prep_sample_idx=ps,
        principal_idx=pr,
        action_id="fastq-to-parquet",
        created_at=_at(8),
    )
    # The pool ticket that actually minted the range, and the sample's membership in
    # that pool (the join the pool-shape candidate walks).
    pool_ticket, _pool_idx = await _pool_ingest(
        pool, version=version, prep_sample_idx=ps, principal_idx=pr, created_at=_at(9)
    )
    # The range was minted by the POOL ingest — after both tickets exist.
    await _range_at(pool, prep_sample_idx=ps, principal_idx=pr, created_at=_at(10))

    await pool.execute(_BACKFILL)

    owner = await pool.fetchval(
        "SELECT minted_by_work_ticket_idx FROM qiita.sequence_range WHERE prep_sample_idx = $1",
        ps,
    )
    assert owner is None, (
        "two candidate minters (a stray per-sample loader and the pool ingest) must "
        f"leave the range unattributed; got {owner} (pool ticket was {pool_ticket})"
    )
