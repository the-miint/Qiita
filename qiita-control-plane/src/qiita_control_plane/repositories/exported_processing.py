"""The idempotent mint for `qiita.exported_processing` — the public handle a
published bundle's manifest cites in place of our `alignment_idx`.

Simpler than its two siblings: one row, no accession to prefer, no collision to
arbitrate. See the migration `db/migrations/20260813000001_exported_processing.sql`
for why the namespace is entirely minted.
"""

import asyncpg

_SELECT_LIVE = (
    "SELECT alignment_idx, export_processing_id"
    "  FROM qiita.exported_processing"
    " WHERE alignment_idx = $1 AND NOT retired"
)


class ProcessingVanishedError(RuntimeError):
    """The processing had no live handle after minting one for it.

    The only way to get here is a concurrent purge of the alignment, which retires
    the handles it named.
    """

    def __init__(self, alignment_idx: int) -> None:
        self.alignment_idx = alignment_idx
        super().__init__(
            f"alignment {alignment_idx} has no live exported processing identifier after minting"
        )


async def mint_exported_processing(
    pool: asyncpg.Pool, *, alignment_idx: int, created_by_idx: int
) -> asyncpg.Record:
    """Ensure a live `export_processing_id` exists for this processing, and return it.

    **Idempotent, and that is the contract rather than an optimization**: the handle
    is published, so two bundles built from one processing must cite it identically
    or nobody can tell they share it. The INSERT conflicts against the partial unique
    index on live rows and does nothing.

    Two concurrent callers are safe without a lock: both INSERT, the index lets one
    row win, `DO NOTHING` swallows the loser, and the SELECT hands the winner to both.

    Raises `ProcessingVanishedError` rather than returning None. The INSERT
    guarantees a live row at the moment it runs, but READ COMMITTED means the SELECT
    takes a fresh snapshot, so an alignment purged in between retires the row and it
    drops out — and a manifest citing nothing is worse than a failed build.
    """
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(
            "INSERT INTO qiita.exported_processing (alignment_idx, created_by_idx)"
            " VALUES ($1, $2)"
            " ON CONFLICT (alignment_idx) WHERE NOT retired DO NOTHING",
            alignment_idx,
            created_by_idx,
        )
        row = await conn.fetchrow(_SELECT_LIVE, alignment_idx)

    if row is None:
        raise ProcessingVanishedError(alignment_idx)
    return row
