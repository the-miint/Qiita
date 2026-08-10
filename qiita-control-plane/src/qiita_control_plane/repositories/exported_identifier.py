"""Reads and the idempotent mint for `qiita.exported_identifier` — the public
handle a published artifact carries in place of our minted identifiers.
"""

import asyncpg

# Every caller-visible column, plus the accessions that ride along for
# information. Both accessions are LEFT-joined and nullable: an unaccessioned
# sample still gets an export_id, which is the point of having one.
#
# `sequenced_sample` is LEFT-joined because a prep_sample of a future
# non-sequenced processing kind has no subtype row at all.
_SELECT_LIVE = (
    "SELECT ei.prep_sample_idx, ei.export_id,"
    "       bs.biosample_accession, ss.ena_run_accession"
    "  FROM qiita.exported_identifier ei"
    "  JOIN qiita.prep_sample ps ON ps.idx = ei.prep_sample_idx"
    "  JOIN qiita.biosample bs ON bs.idx = ps.biosample_idx"
    "  LEFT JOIN qiita.sequenced_sample ss ON ss.prep_sample_idx = ps.idx"
    " WHERE ei.alignment_idx = $1"
    "   AND ei.prep_sample_idx = ANY($2::bigint[])"
    "   AND NOT ei.retired"
    " ORDER BY ei.prep_sample_idx"
)


async def mint_exported_identifiers(
    pool: asyncpg.Pool,
    *,
    alignment_idx: int,
    prep_sample_idxs: list[int],
    created_by_idx: int,
) -> list[asyncpg.Record]:
    """Ensure a live `export_id` exists for every `(alignment_idx, prep_sample)`
    pair, and return them all ascending by prep_sample_idx.

    **Idempotent, and that is the contract rather than an optimization**: an
    export_id is published, so asking twice for the same processed sample must
    give the same answer both times. The INSERT conflicts against the partial
    unique index on live rows and does nothing, so a re-request adds no row and
    changes no identifier.

    Concurrency-safe without a lock: two callers minting the same cohort both
    INSERT, the index lets exactly one row per pair win, `DO NOTHING` swallows
    the loser, and the SELECT afterwards returns the winner to both. This is why
    the mint is an upsert against a constraint rather than a read-then-write.

    Retired rows are excluded from the SELECT, so a purged-and-realigned pair
    mints a fresh identifier rather than resurrecting a retired one.
    """
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(
            "INSERT INTO qiita.exported_identifier"
            "       (alignment_idx, prep_sample_idx, created_by_idx)"
            " SELECT $1, prep_sample_idx, $3"
            "   FROM unnest($2::bigint[]) AS prep_sample_idx"
            " ON CONFLICT (alignment_idx, prep_sample_idx) WHERE NOT retired DO NOTHING",
            alignment_idx,
            prep_sample_idxs,
            created_by_idx,
        )
        return await conn.fetch(_SELECT_LIVE, alignment_idx, prep_sample_idxs)
