"""Repository functions for qiita.assembly_membership.

The assembly analogue of the reference_membership INSERT that
`qiita_control_plane.actions.library.write_membership` performs inline: a bulk,
idempotent link of a prep_sample's assembly RUN (processing_idx) contigs — each a
qiita.feature minted via the SHARED mint-features path — to the bin they belong to
(kind = 'LCG' | 'MAG', bin_id). The DuckDB-side JOIN (bin_map x manifest x
feature_map -> (kind, bin_id, feature_idx)) and the batch streaming live in the
library primitive; this module owns only the raw bulk-insert SQL, mirroring
repositories.processing owning qiita.mint_processing's call.
"""

import asyncpg


async def insert_assembly_membership_rows(
    conn: asyncpg.Connection,
    *,
    prep_sample_idx: int,
    processing_idx: int,
    kinds: list[str],
    bin_ids: list[str],
    feature_idxs: list[int],
) -> int:
    """Bulk-insert one chunk of assembly_membership rows; return the count of
    newly-linked rows.

    The three lists are positionally aligned: row i links
    ``(prep_sample_idx, processing_idx, kinds[i], bin_ids[i], feature_idxs[i])``.
    ``ON CONFLICT DO NOTHING`` on the natural PK makes the write idempotent /
    replay-safe — a workflow retried from the start re-runs this primitive and
    re-inserting the same rows links nothing new. Wraps
    asyncpg.ForeignKeyViolationError into a ValueError so the caller surfaces a
    structured error instead of leaking the asyncpg exception (a feature_idx that
    isn't in qiita.feature means upstream mint-features produced inconsistent
    inputs).
    """
    if not feature_idxs:
        return 0
    try:
        rows = await conn.fetch(
            "INSERT INTO qiita.assembly_membership"
            " (prep_sample_idx, processing_idx, kind, bin_id, feature_idx)"
            " SELECT $1, $2, k, b, f"
            " FROM unnest($3::text[], $4::text[], $5::bigint[]) AS t(k, b, f)"
            " ON CONFLICT DO NOTHING"
            " RETURNING feature_idx",
            prep_sample_idx,
            processing_idx,
            kinds,
            bin_ids,
            feature_idxs,
        )
    except asyncpg.ForeignKeyViolationError as exc:
        raise ValueError(
            "One or more feature_idx / prep_sample_idx / processing_idx values do not exist"
        ) from exc
    return len(rows)
