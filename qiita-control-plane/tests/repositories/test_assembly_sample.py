"""Repository-layer tests for the assembly_sample completion gate.

Exercises qiita.assembly_sample via the repositories.assembly helpers
(create_assembly_sample_pending / upsert_assembly_sample_completed /
upsert_assembly_sample_no_data / fetch_assembly_sample_state), plus the schema
guarantees the writers rest on: the CHECK's value set and the updated_at trigger.

Each test seeds its own principal + sequenced prep_sample + a qiita.processing
row, and teardown runs in FK-reverse order so the suite can share the
postgres_pool fixture.
"""

import secrets

import pytest
import pytest_asyncio

from qiita_control_plane.repositories.assembly import (
    create_assembly_sample_pending,
    fetch_assembly_sample_state,
    upsert_assembly_sample_completed,
    upsert_assembly_sample_no_data,
)
from qiita_control_plane.repositories.processing import mint_processing
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def gate(postgres_pool):
    """Seed a principal, a sequenced prep_sample, and a qiita.processing run."""
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="asm-gate", suffix=suffix)
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    async with postgres_pool.acquire() as conn:
        row = await mint_processing(
            conn,
            workflow="long-read-assembly",
            version="1.0.0",
            params={
                "workflow": "long-read-assembly",
                "version": "1.0.0",
                "mask_idx": 1,
                "assembler": "hifiasm_meta",
                "nonce": suffix,
            },
        )
    processing_idx = row["processing_idx"]

    yield {
        "pool": postgres_pool,
        "processing_idx": processing_idx,
        "prep_sample_idx": prep_sample_idx,
    }

    await postgres_pool.execute(
        "DELETE FROM qiita.assembly_sample WHERE processing_idx = $1", processing_idx
    )
    await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", prep_sample_idx)
    await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx)
    await postgres_pool.execute(
        "DELETE FROM qiita.processing WHERE processing_idx = $1", processing_idx
    )
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


async def _write_pending(gate):
    async with gate["pool"].acquire() as conn, conn.transaction():
        await create_assembly_sample_pending(
            conn,
            processing_idx=gate["processing_idx"],
            prep_sample_idx=gate["prep_sample_idx"],
        )


async def _state(gate) -> str | None:
    return await fetch_assembly_sample_state(
        gate["pool"],
        processing_idx=gate["processing_idx"],
        prep_sample_idx=gate["prep_sample_idx"],
    )


# ---------------------------------------------------------------------------
# create_assembly_sample_pending
# ---------------------------------------------------------------------------


async def test_absent_row_reads_none(gate):
    """No row means "not over" — never "assembled". The read accepts a pool."""
    assert await _state(gate) is None


async def test_create_pending_materializes_the_gate(gate):
    await _write_pending(gate)
    assert await _state(gate) == "pending"


async def test_create_pending_requires_a_transaction(gate):
    async with gate["pool"].acquire() as conn:
        with pytest.raises(RuntimeError):
            await create_assembly_sample_pending(
                conn,
                processing_idx=gate["processing_idx"],
                prep_sample_idx=gate["prep_sample_idx"],
            )


@pytest.mark.parametrize("terminal", ["completed", "no_data"])
async def test_create_pending_never_reopens_a_closed_row(gate, terminal):
    """The resume/redrive case: re-minting the same params re-resolves the same
    processing_idx and re-runs this, which must not walk a terminal row back to
    'pending'."""
    await _write_pending(gate)
    await gate["pool"].execute(
        "UPDATE qiita.assembly_sample SET state = $3"
        " WHERE processing_idx = $1 AND prep_sample_idx = $2",
        gate["processing_idx"],
        gate["prep_sample_idx"],
        terminal,
    )
    await _write_pending(gate)
    assert await _state(gate) == terminal


# ---------------------------------------------------------------------------
# the two terminal writers
# ---------------------------------------------------------------------------


async def test_completed_upsert_closes_the_gate_and_is_idempotent(gate):
    await _write_pending(gate)
    for _ in range(2):
        async with gate["pool"].acquire() as conn, conn.transaction():
            await upsert_assembly_sample_completed(
                conn,
                processing_idx=gate["processing_idx"],
                prep_sample_idx=gate["prep_sample_idx"],
            )
    assert await _state(gate) == "completed"


async def test_no_data_upsert_closes_the_gate(gate):
    """The StepNoData path: assembly_hash found no contig, so the terminal action
    never runs and this is what closes the row."""
    await _write_pending(gate)
    async with gate["pool"].acquire() as conn, conn.transaction():
        await upsert_assembly_sample_no_data(
            conn,
            processing_idx=gate["processing_idx"],
            prep_sample_idx=gate["prep_sample_idx"],
        )
    assert await _state(gate) == "no_data"


@pytest.mark.parametrize(
    "writer", [upsert_assembly_sample_completed, upsert_assembly_sample_no_data]
)
async def test_terminal_writers_stand_alone_without_a_pending_row(gate, writer):
    """Both write the row outright, so neither depends on the pre-loop
    materialization having run."""
    async with gate["pool"].acquire() as conn, conn.transaction():
        await writer(
            conn,
            processing_idx=gate["processing_idx"],
            prep_sample_idx=gate["prep_sample_idx"],
        )
    assert await _state(gate) in {"completed", "no_data"}


async def test_no_data_does_not_overwrite_completed(gate):
    """A prior run under this identity left contigs in DuckLake; a later run of the
    same identity finding none does not remove them, so 'no_data' must not deny
    rows that are there. The asymmetry with the completed writer below."""
    async with gate["pool"].acquire() as conn, conn.transaction():
        await upsert_assembly_sample_completed(
            conn,
            processing_idx=gate["processing_idx"],
            prep_sample_idx=gate["prep_sample_idx"],
        )
    async with gate["pool"].acquire() as conn, conn.transaction():
        await upsert_assembly_sample_no_data(
            conn,
            processing_idx=gate["processing_idx"],
            prep_sample_idx=gate["prep_sample_idx"],
        )
    assert await _state(gate) == "completed"


async def test_completed_overwrites_no_data(gate):
    """The other direction is a move forward and is allowed."""
    async with gate["pool"].acquire() as conn, conn.transaction():
        await upsert_assembly_sample_no_data(
            conn,
            processing_idx=gate["processing_idx"],
            prep_sample_idx=gate["prep_sample_idx"],
        )
    async with gate["pool"].acquire() as conn, conn.transaction():
        await upsert_assembly_sample_completed(
            conn,
            processing_idx=gate["processing_idx"],
            prep_sample_idx=gate["prep_sample_idx"],
        )
    assert await _state(gate) == "completed"


# ---------------------------------------------------------------------------
# schema: the CHECK's value set and the updated_at trigger
# ---------------------------------------------------------------------------


async def test_state_check_rejects_an_unknown_value(gate):
    import asyncpg

    await _write_pending(gate)
    with pytest.raises(asyncpg.CheckViolationError):
        await gate["pool"].execute(
            "UPDATE qiita.assembly_sample SET state = 'invalidated'"
            " WHERE processing_idx = $1 AND prep_sample_idx = $2",
            gate["processing_idx"],
            gate["prep_sample_idx"],
        )


async def test_updated_at_trigger_bumps_on_the_terminal_flip(gate):
    await _write_pending(gate)
    created, before = await gate["pool"].fetchrow(
        "SELECT created_at, updated_at FROM qiita.assembly_sample"
        " WHERE processing_idx = $1 AND prep_sample_idx = $2",
        gate["processing_idx"],
        gate["prep_sample_idx"],
    )
    assert created == before
    async with gate["pool"].acquire() as conn, conn.transaction():
        await upsert_assembly_sample_completed(
            conn,
            processing_idx=gate["processing_idx"],
            prep_sample_idx=gate["prep_sample_idx"],
        )
    after = await gate["pool"].fetchval(
        "SELECT updated_at FROM qiita.assembly_sample"
        " WHERE processing_idx = $1 AND prep_sample_idx = $2",
        gate["processing_idx"],
        gate["prep_sample_idx"],
    )
    assert after > before


async def test_processing_delete_is_restricted_while_a_gate_row_lives(gate):
    """RESTRICT, not CASCADE: a qiita.processing row is shared by every sample
    assembled under the same params, and the DuckLake rows stamped with it are
    beyond any FK's reach, so dropping the gate silently is what this refuses."""
    import asyncpg

    await _write_pending(gate)
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await gate["pool"].execute(
            "DELETE FROM qiita.processing WHERE processing_idx = $1", gate["processing_idx"]
        )
