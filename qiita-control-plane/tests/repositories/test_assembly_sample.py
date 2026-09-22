"""Repository-layer tests for the assembly_sample completion gate.

Exercises qiita.assembly_sample via the repositories.assembly helpers
(create_assembly_sample_pending / upsert_assembly_sample_completed /
upsert_assembly_sample_no_data / fetch_assembly_sample_state), plus what those
writers do to a row an operator withdrew, plus the schema guarantees they rest
on: the CHECK's value set, the invalidation biconditional, and the updated_at
trigger.

Each test seeds its own principal + sequenced prep_sample + a qiita.processing
row, and teardown runs in FK-reverse order so the suite can share the
postgres_pool fixture.
"""

import secrets

import asyncpg
import pytest
import pytest_asyncio

from qiita_control_plane.repositories.assembly import (
    AssemblySampleInvalidated,
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
        "principal_idx": principal_idx,
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


@pytest.mark.parametrize(
    ("closed", "expected"), [("completed", "completed"), ("no_data", "pending")]
)
async def test_create_pending_reopens_no_data_but_not_completed(gate, closed, expected):
    """The resume/redrive case, both arms: re-minting the same params re-resolves
    the same processing_idx and re-runs this over whatever the last run left. The
    asymmetry it must hold is stated on `create_assembly_sample_pending`."""
    await _write_pending(gate)
    await gate["pool"].execute(
        "UPDATE qiita.assembly_sample SET state = $3"
        " WHERE processing_idx = $1 AND prep_sample_idx = $2",
        gate["processing_idx"],
        gate["prep_sample_idx"],
        closed,
    )
    await _write_pending(gate)
    assert await _state(gate) == expected


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
    never runs and this is what closes the row. Reports None — nothing stood in
    the way, so the write landed."""
    await _write_pending(gate)
    async with gate["pool"].acquire() as conn, conn.transaction():
        standing = await upsert_assembly_sample_no_data(
            conn,
            processing_idx=gate["processing_idx"],
            prep_sample_idx=gate["prep_sample_idx"],
        )
    assert standing is None
    assert await _state(gate) == "no_data"


@pytest.mark.parametrize(
    ("writer", "expected"),
    [
        (upsert_assembly_sample_completed, "completed"),
        (upsert_assembly_sample_no_data, "no_data"),
    ],
)
async def test_terminal_writers_stand_alone_without_a_pending_row(gate, writer, expected):
    """Both write the row outright, so neither depends on the pre-loop
    materialization having run."""
    async with gate["pool"].acquire() as conn, conn.transaction():
        await writer(
            conn,
            processing_idx=gate["processing_idx"],
            prep_sample_idx=gate["prep_sample_idx"],
        )
    assert await _state(gate) == expected


async def test_no_data_does_not_overwrite_completed(gate):
    """'no_data' never walks a 'completed' row back — the guard on the DO UPDATE
    in `upsert_assembly_sample_no_data`, which carries the reasoning. Names the
    state that stood, so the caller can log which of the two guarded states it was
    instead of discarding the distinction."""
    async with gate["pool"].acquire() as conn, conn.transaction():
        await upsert_assembly_sample_completed(
            conn,
            processing_idx=gate["processing_idx"],
            prep_sample_idx=gate["prep_sample_idx"],
        )
    async with gate["pool"].acquire() as conn, conn.transaction():
        standing = await upsert_assembly_sample_no_data(
            conn,
            processing_idx=gate["processing_idx"],
            prep_sample_idx=gate["prep_sample_idx"],
        )
    assert standing == "completed"
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
# the withdrawn row: what a re-run may and may not do to it
# ---------------------------------------------------------------------------


async def _invalidate(gate) -> None:
    """Withdraw the pair straight in SQL. The route path is covered in
    tests/routes/test_assembly_lifecycle.py; here the point is what the WRITERS do
    to a row already in that state, so the row is put there directly."""
    await gate["pool"].execute(
        "UPDATE qiita.assembly_sample"
        "   SET state = 'invalidated', invalidated_at = now(),"
        "       invalidated_by_idx = $3, invalidation_reason = 'withdrawn'"
        " WHERE processing_idx = $1 AND prep_sample_idx = $2",
        gate["processing_idx"],
        gate["prep_sample_idx"],
        gate["principal_idx"],
    )


async def test_create_pending_leaves_an_invalidated_row_alone(gate):
    """A redrive does not quietly reopen a withdrawal. The re-run proceeds; its
    terminal write is what refuses."""
    await _write_pending(gate)
    await _invalidate(gate)
    await _write_pending(gate)
    assert await _state(gate) == "invalidated"


async def test_completed_refuses_to_overturn_an_invalidation(gate):
    """The one state 'completed' is NOT reachable from. Raises rather than
    returning quietly, because the caller is the terminal action of a run that
    thinks it succeeded."""
    await _write_pending(gate)
    await _invalidate(gate)
    with pytest.raises(AssemblySampleInvalidated) as ei:
        async with gate["pool"].acquire() as conn, conn.transaction():
            await upsert_assembly_sample_completed(
                conn,
                processing_idx=gate["processing_idx"],
                prep_sample_idx=gate["prep_sample_idx"],
            )
    assert str(gate["processing_idx"]) in str(ei.value)
    assert await _state(gate) == "invalidated"


async def test_no_data_does_not_overwrite_an_invalidation(gate):
    """Same guard, the other terminal writer — and unlike its completing twin this
    one does not raise: the run has already ended, and 'invalidated' is still the
    answer a consumer asking for contigs needs."""
    await _write_pending(gate)
    await _invalidate(gate)
    async with gate["pool"].acquire() as conn, conn.transaction():
        standing = await upsert_assembly_sample_no_data(
            conn,
            processing_idx=gate["processing_idx"],
            prep_sample_idx=gate["prep_sample_idx"],
        )
    assert standing == "invalidated"
    assert await _state(gate) == "invalidated"


async def test_invalidation_provenance_is_biconditional(gate):
    """The three columns are set exactly when the state is 'invalidated' — so a
    withdrawal can never be recorded without a reason, and restoring can never
    leave one behind."""
    await _write_pending(gate)
    with pytest.raises(asyncpg.CheckViolationError):
        await gate["pool"].execute(
            "UPDATE qiita.assembly_sample SET state = 'invalidated'"
            " WHERE processing_idx = $1 AND prep_sample_idx = $2",
            gate["processing_idx"],
            gate["prep_sample_idx"],
        )
    await _invalidate(gate)
    with pytest.raises(asyncpg.CheckViolationError):
        await gate["pool"].execute(
            "UPDATE qiita.assembly_sample SET state = 'completed'"
            " WHERE processing_idx = $1 AND prep_sample_idx = $2",
            gate["processing_idx"],
            gate["prep_sample_idx"],
        )


# ---------------------------------------------------------------------------
# schema: the CHECK's value set and the updated_at trigger
# ---------------------------------------------------------------------------


async def test_state_check_rejects_an_unknown_value(gate):
    await _write_pending(gate)
    with pytest.raises(asyncpg.CheckViolationError):
        await gate["pool"].execute(
            "UPDATE qiita.assembly_sample SET state = 'withdrawn'"
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
    """Deleting a qiita.processing row with a live gate row is refused, not
    cascaded — see the FK's column comment in the assembly_sample migration."""
    await _write_pending(gate)
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await gate["pool"].execute(
            "DELETE FROM qiita.processing WHERE processing_idx = $1", gate["processing_idx"]
        )
