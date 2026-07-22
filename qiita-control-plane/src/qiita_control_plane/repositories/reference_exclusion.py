"""Repository functions for the qiita.reference_exclusion blocklist.

A GLOBAL (not reference-scoped) blocklist of bad genome_idx / feature_idx that
must be excluded from downstream reference-data consumption. Each row targets
exactly one of genome_idx / feature_idx (a CHECK enforces it). Enforcement is a
query-time anti-join in the data plane over the RESOLVED feature set — a genome
block expands to all its features via feature_genome (feature_idx is UNIQUE in
feature_genome, so a feature maps to at most one genome) — so consumers never
touch a blocked feature, even one blocked through a genome before that feature
was loaded.

The base data is never deleted; blocking is reversible (remove the row). Unlike
read_mask, absence of a row safely means "not blocked" — there is no completion
gate, because a curated blocklist is complete by construction.

The target FKs are ON DELETE CASCADE: a block dies with its entity, which only
happens when delete_reference_cascade orphan-GCs a genome/feature that no longer
exists in any reference (so the block is moot). See the migration comment for
why this beats an unblock-before-delete interlock.

Every helper accepts a pool or a connection so it composes standalone or inside
an open transaction.
"""

import asyncpg

from ..auth.db import rows_affected

# The resolved excluded feature_idx set: direct feature blocks UNION every
# feature of a blocked genome. UNION (not UNION ALL) dedups a feature blocked
# both directly and via its genome. Small by design (a curated blocklist).
RESOLVE_EXCLUDED_FEATURES_SQL = (
    "SELECT feature_idx FROM qiita.reference_exclusion WHERE feature_idx IS NOT NULL"
    " UNION"
    " SELECT fg.feature_idx FROM qiita.reference_exclusion x"
    "   JOIN qiita.feature_genome fg USING (genome_idx)"
    "  WHERE x.genome_idx IS NOT NULL"
)

# Blocked features that appear in one reference, with why + external ids. The
# resolved excluded features of the reference (CTE) joined back to the exclusion
# rows that block them; DISTINCT ON collapses a feature blocked both ways to one
# row, preferring the direct feature block's reason.
LIST_FOR_REFERENCE_SQL = (
    "WITH member AS ("
    "  SELECT rm.feature_idx, rm.accession, fg.genome_idx, g.source, g.source_id"
    "    FROM qiita.reference_membership rm"
    "    LEFT JOIN qiita.feature_genome fg USING (feature_idx)"
    "    LEFT JOIN qiita.genome g ON g.genome_idx = fg.genome_idx"
    "   WHERE rm.reference_idx = $1"
    ")"
    " SELECT DISTINCT ON (m.feature_idx)"
    "   m.feature_idx, m.accession, m.genome_idx, m.source, m.source_id,"
    "   x.reason, x.excluded_at, x.excluded_by_idx,"
    "   (x.feature_idx IS NOT NULL) AS direct_block,"
    "   (x.genome_idx IS NOT NULL) AS via_genome"
    " FROM member m"
    " JOIN qiita.reference_exclusion x"
    "   ON x.feature_idx = m.feature_idx OR x.genome_idx = m.genome_idx"
    " ORDER BY m.feature_idx, (x.feature_idx IS NOT NULL) DESC"
)


async def add_exclusion(
    db: asyncpg.Pool | asyncpg.Connection,
    *,
    reason: str,
    excluded_by_idx: int,
    genome_idx: int | None = None,
    feature_idx: int | None = None,
) -> bool:
    """Block exactly one of `genome_idx` / `feature_idx`. Idempotent via
    ON CONFLICT DO NOTHING on the target's partial unique index — a re-block
    keeps the original reason. Returns True if a new row was inserted, False if
    the entity was already blocked.

    Raises ValueError if not exactly one target is given (fail-fast before the
    DB CHECK, which remains the backstop for any raw INSERT)."""
    if (genome_idx is None) == (feature_idx is None):
        raise ValueError("exactly one of genome_idx / feature_idx must be given")
    row = await db.fetchrow(
        "INSERT INTO qiita.reference_exclusion"
        " (genome_idx, feature_idx, reason, excluded_by_idx)"
        " VALUES ($1, $2, $3, $4)"
        " ON CONFLICT DO NOTHING"
        " RETURNING excluded_at",
        genome_idx,
        feature_idx,
        reason,
        excluded_by_idx,
    )
    return row is not None


async def remove_exclusion(
    db: asyncpg.Pool | asyncpg.Connection,
    *,
    genome_idx: int | None = None,
    feature_idx: int | None = None,
) -> int:
    """Unblock exactly one of `genome_idx` / `feature_idx`. Returns rows deleted
    (0 if the entity was not blocked). Raises ValueError if not exactly one
    target is given."""
    if (genome_idx is None) == (feature_idx is None):
        raise ValueError("exactly one of genome_idx / feature_idx must be given")
    if genome_idx is not None:
        result = await db.execute(
            "DELETE FROM qiita.reference_exclusion WHERE genome_idx = $1", genome_idx
        )
    else:
        result = await db.execute(
            "DELETE FROM qiita.reference_exclusion WHERE feature_idx = $1", feature_idx
        )
    return rows_affected(result)


async def resolve_excluded_features(db: asyncpg.Pool | asyncpg.Connection) -> list[int]:
    """The resolved excluded feature_idx set — direct feature blocks plus every
    feature of a blocked genome, deduplicated. This is what the data plane
    anti-joins; the control plane ships it wholesale to the lake."""
    rows = await db.fetch(RESOLVE_EXCLUDED_FEATURES_SQL)
    return [r["feature_idx"] for r in rows]


async def list_for_reference(
    db: asyncpg.Pool | asyncpg.Connection, reference_idx: int
) -> list[asyncpg.Record]:
    """What is filtered from `reference_idx` and why: one row per blocked feature
    that appears in the reference, carrying the exclusion reason, whether it was
    blocked directly or via its genome, the genome provenance (source,
    source_id), and the reference's own accession for the feature. Global blocks
    that touch no feature in this reference are absent."""
    return await db.fetch(LIST_FOR_REFERENCE_SQL, reference_idx)
