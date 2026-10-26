"""`qiita.syndna_read_count`: reads aligned to each SynDNA insert, per masked sample.

The writer is the read-mask `persist-syndna-read-count` action (and the admin
backfill, which feeds it the same file); the reader is the study-reader export,
`GET /mask-definition/{mask_idx}/syndna-read-count`. What a row counts is stated on
the table (the migration) and on `actions.library.syndna_read_counts`.
"""

from collections.abc import Mapping, Sequence

import asyncpg

from . import require_transaction
from ._sample_scope import sample_scope_sql
from .mask_definition import RESOLVED_SYNDNA_KEY, SYNDNA_REFERENCE_IDX_KEY

# The jsonb path from a mask's `params` to its SynDNA reference, unqualified so it
# composes into any query with one `params` column in scope. NULL when the mask
# was minted with SynDNA off (`resolved_syndna` is JSON null) — the "this mask counted
# nothing" case every reader refuses.
SYNDNA_REFERENCE_SQL = f"(params->'{RESOLVED_SYNDNA_KEY}'->>'{SYNDNA_REFERENCE_IDX_KEY}')::bigint"

_ROSTER_ALIAS = "msk"


async def fetch_mask_syndna_reference(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection, mask_idx: int
) -> asyncpg.Record | None:
    """The mask's row as `(mask_idx, reference_idx)`, or None when the mask does not
    exist. `reference_idx` is None when the mask ran without SynDNA."""
    return await pool_or_conn.fetchrow(
        f"SELECT mask_idx, {SYNDNA_REFERENCE_SQL} AS reference_idx"
        "  FROM qiita.mask_definition WHERE mask_idx = $1",
        mask_idx,
    )


async def fetch_syndna_inserts(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection, reference_idx: int
) -> list[asyncpg.Record]:
    """The reference's members as `(feature_idx, accession)`, ascending by
    feature_idx. `accession` is the FASTA header the load recorded, NULL where the
    load predates the column."""
    return list(
        await pool_or_conn.fetch(
            "SELECT feature_idx, accession FROM qiita.reference_membership"
            " WHERE reference_idx = $1 ORDER BY feature_idx",
            reference_idx,
        )
    )


async def replace_syndna_read_counts(
    conn: asyncpg.Connection,
    *,
    mask_idx: int,
    prep_sample_idx: int,
    counts: Mapping[int, int],
) -> int:
    """Replace the `(mask_idx, prep_sample_idx)` rows with `counts` (feature_idx →
    read count); return the number written. Delete-then-insert, so a retried
    workflow or a re-run backfill converges on the same rows. Must run inside a
    transaction: a failure between the two statements must not leave the pair
    uncounted."""
    require_transaction(conn)
    await conn.execute(
        "DELETE FROM qiita.syndna_read_count WHERE mask_idx = $1 AND prep_sample_idx = $2",
        mask_idx,
        prep_sample_idx,
    )
    feature_idxs = sorted(counts)
    await conn.execute(
        "INSERT INTO qiita.syndna_read_count"
        " (mask_idx, prep_sample_idx, feature_idx, read_count)"
        " SELECT $1, $2, f, c FROM unnest($3::bigint[], $4::bigint[]) AS t(f, c)",
        mask_idx,
        prep_sample_idx,
        feature_idxs,
        [counts[f] for f in feature_idxs],
    )
    return len(feature_idxs)


async def fetch_syndna_export_roster(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    mask_idx: int,
    *,
    study_idx: int | None,
    sequenced_pool_idx: int | None,
    prep_sample_idxs: Sequence[int] | None,
    limit: int,
) -> list[asyncpg.Record]:
    """Up to `limit` non-retired samples with a mask_sample row under `mask_idx`,
    matching every given filter, ascending by prep_sample_idx. Each row carries the
    gate state, the biosample accession and the sample's sequenced_pool_idx (NULL
    when it was never pooled).

    Every state is returned, not only 'completed': the export refuses a selection
    holding any other, and it can only say so if it sees them. No caller-visibility
    narrowing — the route authorizes the whole roster. Callers that need to detect
    truncation pass `limit = cap + 1`.
    """
    args: list = [mask_idx]
    scope, _narrowed = sample_scope_sql(
        alias=_ROSTER_ALIAS,
        args=args,
        sequenced_pool_idx=sequenced_pool_idx,
        prep_sample_idx=None,
        visible_to_principal_idx=None,
        study_idx=study_idx,
    )
    if prep_sample_idxs is not None:
        args.append(list(prep_sample_idxs))
        scope += f" AND {_ROSTER_ALIAS}.prep_sample_idx = ANY(${len(args)}::bigint[])"
    args.append(limit)
    return list(
        await pool_or_conn.fetch(
            f"SELECT {_ROSTER_ALIAS}.prep_sample_idx, {_ROSTER_ALIAS}.state,"
            "        bs.biosample_accession, ss.sequenced_pool_idx"
            f"  FROM qiita.mask_sample {_ROSTER_ALIAS}"
            f"  JOIN qiita.prep_sample ps ON ps.idx = {_ROSTER_ALIAS}.prep_sample_idx"
            "   JOIN qiita.biosample bs ON bs.idx = ps.biosample_idx"
            "   LEFT JOIN qiita.sequenced_sample ss"
            f"    ON ss.prep_sample_idx = {_ROSTER_ALIAS}.prep_sample_idx"
            f" WHERE {_ROSTER_ALIAS}.mask_idx = $1{scope}"
            f" ORDER BY {_ROSTER_ALIAS}.prep_sample_idx"
            f" LIMIT ${len(args)}",
            *args,
        )
    )


async def fetch_syndna_read_counts(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    mask_idx: int,
    prep_sample_idxs: Sequence[int],
) -> list[asyncpg.Record]:
    """Every stored `(prep_sample_idx, feature_idx, read_count)` row for these
    samples under `mask_idx`."""
    return list(
        await pool_or_conn.fetch(
            "SELECT prep_sample_idx, feature_idx, read_count FROM qiita.syndna_read_count"
            " WHERE mask_idx = $1 AND prep_sample_idx = ANY($2::bigint[])",
            mask_idx,
            list(prep_sample_idxs),
        )
    )
