"""Repository functions for the qiita.sequence_range table.

The mint path is a thin wrapper around the qiita.mint_sequence_range
plpgsql function — the atomic nextval/setval pair plus the INSERT live
in SQL so concurrent callers never observe an overlapping allocation
(the function holds a transaction-scoped advisory lock for the critical
section). Lets asyncpg.RaiseError (SQLSTATE 22023, raised by the SQL
function on count <= 0), asyncpg.UniqueViolationError (duplicate
prep_sample_idx), and asyncpg.ForeignKeyViolationError (unknown or
wrong-kind prep_sample_idx) propagate to the caller; the route layer
maps each to its HTTP status.

The count-cap enforced by Settings.max_sequence_mint_count is a
route-layer concern and intentionally not duplicated here — the
repository sees only a positive integer because Pydantic + the route
guard have already validated the upper bound when the request flows
through HTTP.
"""

import asyncpg

from . import INT4_MASK, require_transaction

# pg_advisory_xact_lock(class, key) class for a prep_sample's range mint vs the
# writers that act on its having none; allocated in repositories/__init__'s registry,
# distinct from sequencing_run's and fanout_dispatch's.
PREP_SAMPLE_RANGE_LOCK_CLASS = 0x05E9_0001


async def lock_prep_sample_range(conn: asyncpg.Connection, *, prep_sample_idx: int) -> None:
    """Take the transaction-scoped advisory lock on `prep_sample_idx`'s sequence_range,
    held to commit.

    The range mint (`POST /sequence-range`) and its `clear_empty_mask_completions`
    take it, and so does `complete_empty_mask_sample_in_tx`, before it reads that the
    prep_sample has no range. Under READ COMMITTED neither side sees the other's
    uncommitted rows: without the lock, a writer can find no range while a mint's
    is still uncommitted, and the mint's removal of empty completions
    (`clear_empty_mask_completions`) can miss the writer's uncommitted row. The
    prep_sample then holds reads and an empty completion at once. With the lock,
    whichever side takes it second waits for the first to commit, then sees its rows.

    The mint takes it before `mint_sequence_range`, whose allocation lock is global
    and held to commit: waiting for this lock after that one would stall every mint
    behind a block plan that holds it. A caller taking it for many prep_samples takes
    them in ascending prep_sample_idx order, so two such callers cannot deadlock."""
    require_transaction(conn)
    await conn.execute(
        "SELECT pg_advisory_xact_lock($1, $2)",
        PREP_SAMPLE_RANGE_LOCK_CLASS,
        prep_sample_idx & INT4_MASK,
    )


async def mint_sequence_range(
    conn: asyncpg.Connection,
    *,
    prep_sample_idx: int,
    count: int,
    principal_idx: int,
    work_ticket_idx: int | None,
) -> asyncpg.Record:
    """Allocate `count` contiguous sequence_idx values for prep_sample_idx.

    Returns the inserted qiita.sequence_range row as an asyncpg.Record.
    Raises asyncpg.InvalidParameterValueError (SQLSTATE 22023) on
    count <= 0; asyncpg.UniqueViolationError when prep_sample_idx
    already has a range; asyncpg.ForeignKeyViolationError when
    prep_sample_idx does not exist or its processing_kind is not
    'sequenced'.

    `work_ticket_idx` is recorded on the row as the range's minter — a reads job may
    only reuse a range its OWN ticket minted. It is Optional at THIS layer (the column
    is nullable) so non-ingest callers and fixtures can mint an unattributed range;
    the HTTP surface requires it (`SequenceRangeMintRequest.work_ticket_idx`, gt=0), so
    every range minted through the API carries provenance. An unattributed range reads
    as "not mine" on the reuse path, which fails closed.

    No `require_transaction(conn)` guard: the qiita.mint_sequence_range
    plpgsql function body — advisory-lock acquire, nextval, setval,
    INSERT — executes as a single SQL statement, so Postgres wraps it
    in one (implicit or explicit) transaction either way. The
    transaction-scoped advisory lock is held for the full critical
    section regardless of whether the caller opened an asyncpg
    transaction.
    """
    # Explicit column projection matching fetch_sequence_range_by_prep_sample_idx
    # so the route layer's name-based field access is symmetric across the two
    # paths and resistant to drift between the plpgsql function's return type
    # and the live qiita.sequence_range schema.
    return await conn.fetchrow(
        "SELECT idx, prep_sample_idx, processing_kind,"
        "       sequence_idx_start, sequence_idx_stop,"
        "       created_by_idx, created_at, minted_by_work_ticket_idx"
        "  FROM qiita.mint_sequence_range($1, $2, $3, $4)",
        prep_sample_idx,
        count,
        principal_idx,
        work_ticket_idx,
    )


async def fetch_sequence_range_by_prep_sample_idx(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    prep_sample_idx: int,
) -> asyncpg.Record | None:
    """Return the qiita.sequence_range row for prep_sample_idx, or None.

    Accepts either a pool or a connection so the helper composes inside
    an open transaction or stands alone.
    """
    return await pool_or_conn.fetchrow(
        # LEFT JOIN, not INNER: a range whose minting ticket was deleted (or was
        # never attributed) must still come back — the caller reads a missing/absent
        # state as "cannot prove this is safe to reuse" and fails closed.
        "SELECT sr.idx, sr.prep_sample_idx, sr.processing_kind,"
        "       sr.sequence_idx_start, sr.sequence_idx_stop,"
        "       sr.created_by_idx, sr.created_at, sr.minted_by_work_ticket_idx,"
        "       wt.state::text AS minted_by_work_ticket_state"
        "  FROM qiita.sequence_range sr"
        "  LEFT JOIN qiita.work_ticket wt"
        "    ON wt.work_ticket_idx = sr.minted_by_work_ticket_idx"
        " WHERE sr.prep_sample_idx = $1",
        prep_sample_idx,
    )
