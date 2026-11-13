"""The idempotent mint for `qiita.exported_processing` — the public handle a
published bundle's manifest cites in place of our `alignment_idx` or `mask_idx`.

Simpler than its two siblings: one row, no accession to prefer, no collision to
arbitrate. Two kinds: an alignment (`mint_exported_processing`) and a mask
(`mint_exported_mask_processing`). See the migration
`db/migrations/20260813000001_exported_processing.sql` for why the namespace is
entirely minted.
"""

import asyncpg

from .mask_definition import lock_active_mask

# The kind columns, each with its own partial unique index on live rows.
# Interpolated into SQL, so the set is closed.
_KIND_COLUMNS = frozenset({"alignment_idx", "mask_idx"})


class ProcessingVanishedError(RuntimeError):
    """The processing had no live handle after minting one for it.

    Reachable for an alignment only if its alignment_definition row is removed
    concurrently, which retires the handles it named. `qiita_common.actions`, at
    `BLOCK_MASK_ACTION_ID`, says how one can be removed. A mask's mint holds the mask
    row `FOR SHARE`, so a mask handle does not vanish this way.
    """

    def __init__(self, column: str, idx: int) -> None:
        self.column = column
        self.idx = idx
        super().__init__(f"{column}={idx} has no live exported processing identifier after minting")


async def _mint_live(
    conn: asyncpg.Connection, *, column: str, idx: int, created_by_idx: int
) -> asyncpg.Record:
    """INSERT a handle for `column = idx` unless a live one exists, then return the
    live row as `(column, export_processing_id)`. The INSERT conflicts against the
    kind's partial unique index on live rows and does nothing, so two concurrent
    callers both get the winner's row."""
    if column not in _KIND_COLUMNS:
        raise ValueError(f"not an exported_processing kind column: {column!r}")
    await conn.execute(
        f"INSERT INTO qiita.exported_processing ({column}, created_by_idx)"
        " VALUES ($1, $2)"
        f" ON CONFLICT ({column}) WHERE NOT retired DO NOTHING",
        idx,
        created_by_idx,
    )
    row = await conn.fetchrow(
        f"SELECT {column}, export_processing_id"
        "  FROM qiita.exported_processing"
        f" WHERE {column} = $1 AND NOT retired",
        idx,
    )
    if row is None:
        raise ProcessingVanishedError(column, idx)
    return row


async def mint_exported_processing(
    pool: asyncpg.Pool, *, alignment_idx: int, created_by_idx: int
) -> asyncpg.Record:
    """Ensure a live `export_processing_id` exists for this processing, and return it.

    **Idempotent, and that is the contract rather than an optimization**: the handle
    is published, so two bundles built from one processing must cite it identically
    or nobody can tell they share it. The INSERT conflicts against the partial unique
    index on live rows and does nothing.

    Two concurrent callers are safe without a lock (`_mint_live`).

    Raises `ProcessingVanishedError` rather than returning None. The INSERT
    guarantees a live row at the moment it runs, but READ COMMITTED means the SELECT
    takes a fresh snapshot, so an alignment_definition row removed in between retires the row and it
    drops out — and a manifest citing nothing is worse than a failed build.
    """
    async with pool.acquire() as conn, conn.transaction():
        return await _mint_live(
            conn, column="alignment_idx", idx=alignment_idx, created_by_idx=created_by_idx
        )


async def mint_exported_mask_processing(
    pool: asyncpg.Pool, *, mask_idx: int, created_by_idx: int
) -> asyncpg.Record:
    """Ensure a live `export_processing_id` exists for this mask, and return it as
    `(mask_idx, export_processing_id)`.

    Idempotent on the partial unique index over live mask rows, as
    `mint_exported_processing` is on the alignment one.

    The mask row is held `FOR SHARE` for the transaction (`lock_active_mask`), so a
    deprecation or a delete cannot commit between the check and the INSERT. Raises
    `MaskDefinitionNotFound` for an unknown mask and `MaskDefinitionDeprecated` for a
    deprecated one: a deprecated config is void, and a new export must not cite it.
    """
    async with pool.acquire() as conn, conn.transaction():
        await lock_active_mask(conn, mask_idx)
        return await _mint_live(
            conn, column="mask_idx", idx=mask_idx, created_by_idx=created_by_idx
        )
