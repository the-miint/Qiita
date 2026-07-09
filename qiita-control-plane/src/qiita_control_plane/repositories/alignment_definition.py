"""Repository functions for the qiita.alignment_definition table.

An alignment's identity is its CONFIG: the sharded reference it aligns against,
the sharded aligner, the host-depletion mask its input reads carry, and the
reference's current shard-set. The mint path is a thin wrapper around the
qiita.mint_alignment_definition plpgsql function, which upserts on params_hash
so the same config always resolves to the same alignment_idx fleet-wide
(idempotent) — the exact discipline qiita.mask_definition / mint_mask_definition
use (this module mirrors repositories/mask_definition.py).

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


async def mint_alignment_definition(
    conn: asyncpg.Connection,
    *,
    params: dict,
    principal_idx: int,
) -> asyncpg.Record:
    """Mint (or return the existing) alignment_definition row for a config.

    Deduplicates on the canonical-JSON SHA-256 of `params` — the dedup key is
    the config blob, so the same config resolves to the same `alignment_idx`
    fleet-wide. `params` is the canonical alignment config:
    `{reference_idx, aligner, mask_idx, shard_ids: sorted[int]}`. Baking the
    reference's current shard-set into the hash is the growth foundation: a
    grown reference (different DISTINCT reference_membership.shard_id set) mints
    a NEW alignment_idx over only its new shards.

    Returns the qiita.alignment_definition row as an asyncpg.Record. Raises
    asyncpg.ForeignKeyViolationError when principal_idx does not exist.

    No `require_transaction(conn)` guard: the qiita.mint_alignment_definition
    plpgsql body (the SELECT/INSERT upsert loop) executes as a single SQL
    statement, so Postgres wraps it in one transaction either way.
    """
    params_hash = canonical_params_hash(params)
    # asyncpg encodes a dict bound to a jsonb parameter via the JSON codec; pass
    # the serialized string explicitly so the behaviour is independent of
    # whether a JSON codec is registered on the connection.
    return await conn.fetchrow(
        "SELECT alignment_idx, params_hash, params, created_by_idx, created_at"
        "  FROM qiita.mint_alignment_definition($1, $2::jsonb, $3)",
        params_hash,
        json.dumps(params),
        principal_idx,
    )


async def lookup_alignment_idx_by_params(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    params: dict,
) -> int | None:
    """Return the alignment_idx whose params_hash matches ``params``, or None.

    A pure LOOKUP — it computes the same canonical-JSON SHA-256 the mint path
    uses (`canonical_params_hash`) and SELECTs the existing row; it never mints.
    Accepts either a pool or a connection so it composes standalone or inside a
    transaction.
    """
    params_hash = canonical_params_hash(params)
    return await pool_or_conn.fetchval(
        "SELECT alignment_idx FROM qiita.alignment_definition WHERE params_hash = $1",
        params_hash,
    )


async def fetch_alignment_definition_by_idx(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    alignment_idx: int,
) -> asyncpg.Record | None:
    """Return the qiita.alignment_definition row for alignment_idx, or None.

    Accepts either a pool or a connection so the helper composes inside an open
    transaction or stands alone.
    """
    return await pool_or_conn.fetchrow(
        "SELECT alignment_idx, params_hash, params, created_by_idx, created_at"
        "  FROM qiita.alignment_definition"
        " WHERE alignment_idx = $1",
        alignment_idx,
    )
