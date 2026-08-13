"""DB test for the work_ticket.mask_idx backfill migration.

The backfill (db/migrations/20260812000000_backfill_work_ticket_mask_idx.sql)
populates `qiita.work_ticket.mask_idx` from `action_context->>'mask_idx'` for
tickets that name a mask in their context but not in the column — the state every
long-read-assembly ticket was left in, because it CONSUMES a mask rather than
minting one and the runner only persisted the minting path's value.

This test runs the migration's actual up-SQL (read from the file so it can't drift
from a hand-copied duplicate) against seeded tickets and asserts the four cases the
predicate distinguishes: backfilled, left alone because the column is already set,
left NULL because the named mask is gone, and left NULL because the context names
no usable mask.
"""

import json
import secrets
from pathlib import Path

import pytest
import pytest_asyncio
from qiita_common.actions import LONG_READ_ASSEMBLY_ACTION_ID
from qiita_common.models import WorkTicketState

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
        / "20260812000000_backfill_work_ticket_mask_idx.sql"
    )
    text = path.read_text()
    return text.split("-- migrate:up", 1)[1].split("-- migrate:down", 1)[0].strip()


@pytest_asyncio.fixture
async def bf(postgres_pool):
    """Seed a principal, a prep_sample, two masks, a long-read-assembly action, and
    one ticket per case the backfill predicate distinguishes."""
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="wtmask", suffix=suffix)
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    async with postgres_pool.acquire() as conn:
        consumed = await mint_mask_definition(
            conn,
            filter_workflow="read-mask",
            filter_version="1.0.0",
            params={"workflow": "read-mask", "s": suffix},
            principal_idx=principal_idx,
        )
        already = await mint_mask_definition(
            conn,
            filter_workflow="read-mask",
            filter_version="1.0.0",
            params={"workflow": "read-mask", "s": f"{suffix}-other"},
            principal_idx=principal_idx,
        )
    consumed_idx, already_idx = consumed["mask_idx"], already["mask_idx"]
    # A mask_idx that resolves to no row: what action_context still names after the
    # mask itself was deleted (`qiita-admin mask purge-failed` does exactly that).
    gone_idx = (
        await postgres_pool.fetchval("SELECT COALESCE(MAX(mask_idx), 0) FROM qiita.mask_definition")
    ) + 1000

    version = f"wtmask-{suffix}"
    await postgres_pool.execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status)"
        " VALUES ($1, $2, 'prep_sample', '{}'::text[], $3::jsonb,"
        "         '{}'::jsonb, '[]'::jsonb, 1, 1, '1 minute', 'active', 'failed')",
        LONG_READ_ASSEMBLY_ACTION_ID,
        version,
        '{"service": false, "human_roles": ["system_admin"]}',
    )

    async def _ticket(action_context: dict, mask_idx: int | None) -> int:
        return await postgres_pool.fetchval(
            "INSERT INTO qiita.work_ticket"
            " (action_id, action_version, originator_principal_idx, scope_target_kind,"
            "  prep_sample_idx, action_context, mask_idx, state)"
            " VALUES ($1, $2, $3, 'prep_sample', $4, $5::jsonb, $6,"
            "         $7::qiita.work_ticket_state) RETURNING work_ticket_idx",
            LONG_READ_ASSEMBLY_ACTION_ID,
            version,
            principal_idx,
            prep_sample_idx,
            json.dumps(action_context),
            mask_idx,
            WorkTicketState.COMPLETED.value,
        )

    tickets = {
        "backfilled": await _ticket({"mask_idx": consumed_idx}, None),
        # Column already set, and to a DIFFERENT mask than the context names — the
        # predicate must leave it, not overwrite it.
        "already_set": await _ticket({"mask_idx": consumed_idx}, already_idx),
        "mask_deleted": await _ticket({"mask_idx": gone_idx}, None),
        "no_mask_in_context": await _ticket({"assembler": "hifiasm_meta"}, None),
        # A non-integer value: no action writes one, and the text join means it
        # matches nothing rather than reaching a cast.
        "non_numeric": await _ticket({"mask_idx": "not-a-number"}, None),
    }

    yield {
        "pool": postgres_pool,
        "tickets": tickets,
        "consumed_idx": consumed_idx,
        "already_idx": already_idx,
    }

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        LONG_READ_ASSEMBLY_ACTION_ID,
        version,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
        LONG_READ_ASSEMBLY_ACTION_ID,
        version,
    )
    await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", prep_sample_idx)
    await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx)
    await postgres_pool.execute(
        "DELETE FROM qiita.mask_definition WHERE mask_idx = ANY($1::bigint[])",
        [consumed_idx, already_idx],
    )
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


async def _mask_idx_of(pool, work_ticket_idx: int) -> int | None:
    return await pool.fetchval(
        "SELECT mask_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1", work_ticket_idx
    )


async def test_backfill_populates_from_action_context(bf):
    """The consuming ticket's NULL column takes the mask its action_context names."""
    pool, tickets = bf["pool"], bf["tickets"]
    assert await _mask_idx_of(pool, tickets["backfilled"]) is None
    await pool.execute(_backfill_up_sql())
    assert await _mask_idx_of(pool, tickets["backfilled"]) == bf["consumed_idx"]


async def test_backfill_leaves_an_already_populated_column_alone(bf):
    """`mask_idx IS NULL` is the predicate, so a populated column is never rewritten
    — the runner's write wins over the submitter's context. Seeded disagreeing
    because agreement would not distinguish "left alone" from "overwritten"."""
    pool, tickets = bf["pool"], bf["tickets"]
    await pool.execute(_backfill_up_sql())
    assert await _mask_idx_of(pool, tickets["already_set"]) == bf["already_idx"]


async def test_backfill_skips_rows_whose_mask_is_gone(bf):
    """A context naming a deleted mask stays NULL rather than aborting the
    migration on the FK. NULL is also what ON DELETE SET NULL would have left had
    the value been written before the mask was deleted."""
    pool, tickets = bf["pool"], bf["tickets"]
    await pool.execute(_backfill_up_sql())
    assert await _mask_idx_of(pool, tickets["mask_deleted"]) is None


async def test_backfill_ignores_contexts_without_a_usable_mask_idx(bf):
    """No mask_idx key, and a non-integer one, both leave the column NULL — the
    non-integer without raising: the join compares text, so nothing casts it."""
    pool, tickets = bf["pool"], bf["tickets"]
    await pool.execute(_backfill_up_sql())
    assert await _mask_idx_of(pool, tickets["no_mask_in_context"]) is None
    assert await _mask_idx_of(pool, tickets["non_numeric"]) is None


async def test_backfill_is_idempotent(bf):
    """Safe to re-run by hand after the restart to catch the deploy window."""
    pool, tickets = bf["pool"], bf["tickets"]
    await pool.execute(_backfill_up_sql())
    await pool.execute(_backfill_up_sql())
    assert await _mask_idx_of(pool, tickets["backfilled"]) == bf["consumed_idx"]
    assert await _mask_idx_of(pool, tickets["already_set"]) == bf["already_idx"]
