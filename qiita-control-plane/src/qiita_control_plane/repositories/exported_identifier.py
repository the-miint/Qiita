"""Reads and the idempotent mint for `qiita.exported_identifier` — the public
handle a published artifact carries in place of our minted identifiers.
"""

from collections.abc import Sequence

import asyncpg

# Every caller-visible column, plus the accessions that ride along for
# information. Both accessions are LEFT-joined and nullable: an unaccessioned
# sample still gets an export_id, which is the point of having one.
#
# `prep_sample` and `biosample` are INNER joins and cannot drop a row:
# `exported_identifier.prep_sample_idx` is RESTRICT and `prep_sample.biosample_idx`
# is NOT NULL + RESTRICT, so neither parent can vanish under a live identifier.
# `sequenced_sample` is LEFT because a prep_sample of a future non-sequenced
# processing kind has no subtype row at all; it cannot duplicate a row either,
# since its `prep_sample_idx` is UNIQUE.
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


class IncompleteMintError(RuntimeError):
    """The mint came back short of the cohort it was asked for.

    Carries the missing `prep_sample_idx` values so the route can say which.
    """

    def __init__(self, missing: list[int]) -> None:
        self.missing = missing
        super().__init__(
            f"{len(missing)} prep_sample(s) have no live exported identifier after"
            f" minting: {missing}"
        )


def _missing_from(rows: Sequence[asyncpg.Record], prep_sample_idxs: Sequence[int]) -> list[int]:
    """Cohort members with no row in `rows`, ascending.

    A separate function because it is the only part of the mint that can be
    exercised without racing two transactions against each other, and the thing it
    guards is the response's headline promise.
    """
    returned = {row["prep_sample_idx"] for row in rows}
    return sorted(idx for idx in set(prep_sample_idxs) if idx not in returned)


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

    Two concurrent callers minting the same fresh cohort are safe without a lock:
    both INSERT, the index lets exactly one row per pair win, `DO NOTHING` swallows
    the loser, and the SELECT afterwards hands the winner to both.

    **Raises `IncompleteMintError` rather than returning a short list.** The
    INSERT guarantees a live row per cohort member at the moment it runs, but the
    transaction is READ COMMITTED, so the SELECT takes a fresh snapshot: an
    alignment purged by an admin in between retires the rows it named
    (`retire_detached_exported_identifier`) and they drop out of this SELECT. The
    caller's promise is all-or-nothing, and a map quietly missing a sample is a
    published table with an unnamed column — far worse than a failed request. So
    the invariant is checked rather than assumed, which also catches a future
    `_SELECT_LIVE` that stops matching the INSERT.
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
        rows = await conn.fetch(_SELECT_LIVE, alignment_idx, prep_sample_idxs)

    missing = _missing_from(rows, prep_sample_idxs)
    if missing:
        raise IncompleteMintError(missing)
    return rows
