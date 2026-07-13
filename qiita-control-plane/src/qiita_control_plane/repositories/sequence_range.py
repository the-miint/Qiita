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
