"""Repository functions for the qiita.mask_definition table.

A mask's identity is its read-filtering CONFIG (filter workflow + version +
host references + QC params). The mint path is a thin wrapper around the
qiita.mint_mask_definition plpgsql function, which upserts on params_hash so
the same config always resolves to the same mask_idx fleet-wide (idempotent).

The params_hash is computed control-plane-side via
qiita_common.hashing.canonical_params_hash (SHA-256 of the canonical config
JSON) — no pgcrypto dependency on the database. The function only enforces the
dedup and returns the row; asyncpg.ForeignKeyViolationError (unknown
principal_idx) and asyncpg.InvalidParameterValueError (SQLSTATE 22023, a
non-32-byte hash — unreachable via this helper) propagate to the caller.
"""

import json

import asyncpg
from qiita_common.hashing import canonical_params_hash


async def mint_mask_definition(
    conn: asyncpg.Connection,
    *,
    filter_workflow: str,
    filter_version: str,
    params: dict,
    principal_idx: int,
) -> asyncpg.Record:
    """Mint (or return the existing) mask_definition row for a config.

    Deduplicates on the canonical-JSON SHA-256 of `params` alone — the
    dedup key is the config blob, so the same config resolves to the same
    `mask_idx` fleet-wide. `filter_workflow` / `filter_version` are stored as
    descriptive columns; they are expected to also appear inside `params`
    so the hash covers them, but the hash is over `params` so two callers
    that pass the same `params` collapse to one row regardless.

    Returns the qiita.mask_definition row as an asyncpg.Record. Raises
    asyncpg.ForeignKeyViolationError when principal_idx does not exist.

    No `require_transaction(conn)` guard: the qiita.mint_mask_definition
    plpgsql body (the SELECT/INSERT upsert loop) executes as a single SQL
    statement, so Postgres wraps it in one transaction either way.
    """
    params_hash = canonical_params_hash(params)
    # asyncpg encodes a dict bound to a jsonb parameter via the JSON codec; pass
    # the serialized string explicitly so the behaviour is independent of
    # whether a JSON codec is registered on the connection.
    return await conn.fetchrow(
        "SELECT mask_idx, params_hash, filter_workflow, filter_version,"
        "       params, created_by_idx, created_at"
        "  FROM qiita.mint_mask_definition($1, $2, $3, $4::jsonb, $5)",
        params_hash,
        filter_workflow,
        filter_version,
        json.dumps(params),
        principal_idx,
    )


async def lookup_mask_idx_by_params(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    params: dict,
) -> int | None:
    """Return the mask_idx whose params_hash matches ``params``, or None.

    A pure LOOKUP — it computes the same canonical-JSON SHA-256 the mint path
    uses (`canonical_params_hash`) and SELECTs the existing row; it never
    mints. Used by the legacy backfill (`backfill_work_ticket_mask_idx`) to map
    an existing ticket's reconstructed config onto its already-minted mask
    without risking a fresh mint when the config drifted or the ticket failed
    before minting (returns None → the caller skips that ticket).
    """
    params_hash = canonical_params_hash(params)
    return await pool_or_conn.fetchval(
        "SELECT mask_idx FROM qiita.mask_definition WHERE params_hash = $1",
        params_hash,
    )


async def fetch_mask_definition_by_idx(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    mask_idx: int,
) -> asyncpg.Record | None:
    """Return the qiita.mask_definition row for mask_idx, or None.

    Accepts either a pool or a connection so the helper composes inside an
    open transaction or stands alone.
    """
    return await pool_or_conn.fetchrow(
        "SELECT mask_idx, params_hash, filter_workflow, filter_version,"
        "       params, created_by_idx, created_at"
        "  FROM qiita.mask_definition"
        " WHERE mask_idx = $1",
        mask_idx,
    )
