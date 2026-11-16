"""DB test for the work_ticket.processing_idx backfill migration.

The backfill (db/migrations/20261116000001_backfill_work_ticket_processing_idx.sql)
sets `qiita.work_ticket.processing_idx` on the long-read-assembly tickets that the
runner started before it recorded the column, to the processing whose params the
runner minted for each. The expected processing comes from the runner's own builder
(`_build_processing_params`) and mint, so the test fails if the SQL's reconstruction
of the params and the builder disagree.

The migration's up-SQL is read from the file, so the test tracks the real migration
rather than a hand-copied duplicate.
"""

import json
import secrets
from pathlib import Path

import pytest
import pytest_asyncio
from qiita_common.actions import LONG_READ_ASSEMBLY_ACTION_ID, context_schema_default
from qiita_common.models import ScopeTargetKind, WorkTicketState

from qiita_control_plane.repositories.assembly import ASSEMBLY_SAMPLE_COMPLETED
from qiita_control_plane.repositories.mask_definition import mint_mask_definition
from qiita_control_plane.repositories.processing import mint_processing
from qiita_control_plane.runner._mask import MASK_IDX_BINDING
from qiita_control_plane.runner._processing import ASSEMBLER_BINDING, _build_processing_params
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)
from qiita_control_plane.testing.db_teardown import delete_principal, teardown_entity_graph

pytestmark = pytest.mark.db

_CONTEXT_SCHEMA = {"properties": {"assembler": {"type": "string", "default": "myloasm"}}}


def _backfill_up_sql() -> str:
    path = (
        Path(__file__).resolve().parents[1]
        / "db"
        / "migrations"
        / "20261116000001_backfill_work_ticket_processing_idx.sql"
    )
    text = path.read_text()
    return text.split("-- migrate:up", 1)[1].split("-- migrate:down", 1)[0].strip()


@pytest_asyncio.fixture
async def bf(postgres_pool):
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="wtproc", suffix=suffix)
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
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
    version = f"wtproc-{suffix}"
    await postgres_pool.execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status)"
        " VALUES ($1, $2, $5::qiita.scope_target_kind, '{}'::text[], $3::jsonb,"
        "         $4::jsonb, '[]'::jsonb, 1, 1, '1 minute', 'active', 'failed')",
        LONG_READ_ASSEMBLY_ACTION_ID,
        version,
        '{"service": false, "human_roles": ["system_admin"]}',
        json.dumps(_CONTEXT_SCHEMA),
        ScopeTargetKind.PREP_SAMPLE.value,
    )

    async def _processing(action_context: dict) -> int:
        """The processing the runner mints for a ticket with this context."""
        params = _build_processing_params(
            LONG_READ_ASSEMBLY_ACTION_ID,
            version,
            {**action_context, MASK_IDX_BINDING: mask_idx},
            assembler_default=context_schema_default(_CONTEXT_SCHEMA, ASSEMBLER_BINDING),
        )
        async with postgres_pool.acquire() as conn:
            row = await mint_processing(
                conn, workflow=LONG_READ_ASSEMBLY_ACTION_ID, version=version, params=params
            )
        return row["processing_idx"]

    default_run = await _processing({})
    flye_run = await _processing({"assembler": "flye"})
    # Minted for another prep_sample: no gate row for this one.
    other_sample_run = await _processing({"assembler": "hifiasm"})
    for run in (default_run, flye_run):
        await postgres_pool.execute(
            "INSERT INTO qiita.assembly_sample (processing_idx, prep_sample_idx, state)"
            " VALUES ($1, $2, $3)",
            run,
            prep_sample_idx,
            ASSEMBLY_SAMPLE_COMPLETED,
        )

    async def _ticket(
        action_context: dict, *, mask: int | None = mask_idx, processing: int | None = None
    ) -> int:
        return await postgres_pool.fetchval(
            "INSERT INTO qiita.work_ticket"
            " (action_id, action_version, originator_principal_idx, scope_target_kind,"
            "  prep_sample_idx, action_context, mask_idx, processing_idx, state)"
            " VALUES ($1, $2, $3, $9::qiita.scope_target_kind, $4, $5::jsonb, $6, $7,"
            "         $8::qiita.work_ticket_state) RETURNING work_ticket_idx",
            LONG_READ_ASSEMBLY_ACTION_ID,
            version,
            principal_idx,
            prep_sample_idx,
            json.dumps(action_context),
            mask,
            processing,
            WorkTicketState.COMPLETED.value,
            ScopeTargetKind.PREP_SAMPLE.value,
        )

    tickets = {
        "default": await _ticket({"mask_idx": mask_idx}),
        "empty_assembler": await _ticket({"mask_idx": mask_idx, "assembler": ""}),
        "flye": await _ticket({"mask_idx": mask_idx, "assembler": "flye"}),
        # Its params' row was minted for another prep_sample only.
        "other_sample": await _ticket({"mask_idx": mask_idx, "assembler": "hifiasm"}),
        # Params no processing row holds: a ticket that ended before its mint.
        "never_minted": await _ticket({"mask_idx": mask_idx, "assembler": "verkko"}),
        # Not started: the runner had not recorded the consumed mask.
        "not_started": await _ticket({"mask_idx": mask_idx}, mask=None),
        # Set to another run than its params name, so "left alone" is distinguishable.
        "already_set": await _ticket({"mask_idx": mask_idx}, processing=flye_run),
    }

    yield {
        "pool": postgres_pool,
        "tickets": tickets,
        "default_run": default_run,
        "flye_run": flye_run,
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
    await teardown_entity_graph(
        postgres_pool,
        study_idxs=[],
        biosample_idxs=[biosample_idx],
        prep_sample_idxs=[prep_sample_idx],
    )
    # After the sweep, which removes the assembly_sample rows that reference these.
    await postgres_pool.execute(
        "DELETE FROM qiita.processing WHERE processing_idx = ANY($1::bigint[])",
        [default_run, flye_run, other_sample_run],
    )
    await postgres_pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)
    await delete_principal(postgres_pool, [principal_idx])


async def _processing_of(bf) -> dict[str, int | None]:
    return {
        name: await bf["pool"].fetchval(
            "SELECT processing_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1", ticket
        )
        for name, ticket in bf["tickets"].items()
    }


async def test_backfill_sets_the_processing_the_runner_minted(bf):
    """An omitted or empty assembler is the action's default, as the runner hashes it;
    a ticket whose params no row holds, or whose row was minted for another
    prep_sample, or that was not started, stays NULL; a set column is left alone.
    Re-running changes nothing."""
    before = await _processing_of(bf)
    assert before["default"] is None and before["already_set"] == bf["flye_run"]
    expected = {
        "default": bf["default_run"],
        "empty_assembler": bf["default_run"],
        "flye": bf["flye_run"],
        "other_sample": None,
        "never_minted": None,
        "not_started": None,
        "already_set": bf["flye_run"],
    }
    await bf["pool"].execute(_backfill_up_sql())
    assert await _processing_of(bf) == expected
    await bf["pool"].execute(_backfill_up_sql())
    assert await _processing_of(bf) == expected
