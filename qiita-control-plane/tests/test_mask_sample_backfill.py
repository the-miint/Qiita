"""DB test for the per-sample mask_sample backfill migration.

The backfill (db/migrations/20260724000000_backfill_mask_sample_per_sample.sql)
populates a 'completed' qiita.mask_sample gate row for every COMPLETED per-sample
read-mask work_ticket, so the tightened first-class-completion readers don't 409
already-masked samples that predate the per-sample writer. This test runs the
migration's actual up-SQL (read from the file so it can't drift) against seeded
tickets and asserts: a completed read-mask ticket is backfilled, a non-completed
one is not, and re-running is idempotent.
"""

import secrets
from pathlib import Path

import pytest
import pytest_asyncio

from qiita_control_plane.repositories.mask_definition import mint_mask_definition
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)

pytestmark = pytest.mark.db


def _backfill_up_sql() -> str:
    """The migration's `migrate:up` body, read from the file so the test tracks the
    real migration rather than a hand-copied duplicate."""
    path = (
        Path(__file__).resolve().parents[1]
        / "db"
        / "migrations"
        / "20260724000000_backfill_mask_sample_per_sample.sql"
    )
    text = path.read_text()
    return text.split("-- migrate:up", 1)[1].split("-- migrate:down", 1)[0].strip()


@pytest_asyncio.fixture
async def bf(postgres_pool):
    """Seed a principal, two prep_samples, one mask_definition, a 'read-mask'
    prep_sample action, and two completed/processing read-mask work_tickets."""
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="bf-test", suffix=suffix)
    _bs1, ps_done = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    _bs2, ps_inflight = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    async with postgres_pool.acquire() as conn:
        mask = await mint_mask_definition(
            conn,
            filter_workflow="read-mask",
            filter_version="1.0.0",
            params={"workflow": "read-mask", "s": suffix},
            principal_idx=principal_idx,
        )
    mask_idx = mask["mask_idx"]

    # A 'read-mask' prep_sample action (the backfill keys on action_id='read-mask';
    # a unique version keeps it independent of any real synced action).
    version = f"bf-{suffix}"
    await postgres_pool.execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status)"
        " VALUES ('read-mask', $1, 'prep_sample', '{}'::text[], $2::jsonb, '{}'::jsonb,"
        "         '[]'::jsonb, 1, 1, '1 minute', 'active', 'failed')",
        version,
        '{"service": false, "human_roles": ["system_admin"]}',
    )

    async def _ticket(prep_sample_idx, state):
        await postgres_pool.execute(
            "INSERT INTO qiita.work_ticket"
            " (action_id, action_version, originator_principal_idx, scope_target_kind,"
            "  prep_sample_idx, mask_idx, state)"
            " VALUES ('read-mask', $1, $2, 'prep_sample', $3, $4, $5)",
            version,
            principal_idx,
            prep_sample_idx,
            mask_idx,
            state,
        )

    await _ticket(ps_done, "completed")
    await _ticket(ps_inflight, "processing")

    yield {
        "pool": postgres_pool,
        "mask_idx": mask_idx,
        "ps_done": ps_done,
        "ps_inflight": ps_inflight,
    }

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = 'read-mask' AND action_version = $1",
        version,
    )
    await postgres_pool.execute("DELETE FROM qiita.mask_sample WHERE mask_idx = $1", mask_idx)
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = 'read-mask' AND version = $1", version
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", [ps_done, ps_inflight]
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", [_bs1, _bs2]
    )
    await postgres_pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


async def _states(pool, mask_idx):
    rows = await pool.fetch(
        "SELECT prep_sample_idx, state FROM qiita.mask_sample WHERE mask_idx = $1", mask_idx
    )
    return {r["prep_sample_idx"]: r["state"] for r in rows}


async def test_backfill_populates_completed_tickets_only(bf):
    """The completed per-sample read-mask ticket gets a 'completed' gate row; the
    still-processing one does not (the predicate filters on state='completed')."""
    pool, mask_idx = bf["pool"], bf["mask_idx"]
    await pool.execute(_backfill_up_sql())
    states = await _states(pool, mask_idx)
    assert states == {bf["ps_done"]: "completed"}


async def test_backfill_is_idempotent(bf):
    """Re-running the backfill leaves exactly one 'completed' row (ON CONFLICT DO
    NOTHING) — safe to re-run after the deploy to catch the deploy-window."""
    pool, mask_idx = bf["pool"], bf["mask_idx"]
    await pool.execute(_backfill_up_sql())
    await pool.execute(_backfill_up_sql())
    states = await _states(pool, mask_idx)
    assert states == {bf["ps_done"]: "completed"}
