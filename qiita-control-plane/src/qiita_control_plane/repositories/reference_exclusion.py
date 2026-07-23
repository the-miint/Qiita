"""Repository functions for the qiita.reference_exclusion blocklist.

A GLOBAL (not reference-scoped) blocklist of bad genome_idx / feature_idx that
must be excluded from downstream reference-data consumption. Each row targets
exactly one of genome_idx / feature_idx (a CHECK enforces it). Enforcement is a
query-time anti-join in the data plane over the RESOLVED feature set — a genome
block expands to all its features via feature_genome — so consumers never touch
a blocked feature, even one blocked through a genome before that feature was
loaded. feature_genome is many-to-many (a plasmid can be shared across genomes),
so blocking one genome over-excludes a feature it shares with an unblocked genome
(correct — the block is on the feature bytes); and when a shared feature fans to
several genomes, LIST_FOR_REFERENCE_SQL reports its provenance deterministically —
preferring a genome that is itself blocked, then the lowest genome_idx (see the
constant's own comment for the full tiebreak).

The base data is never deleted. The blocklist is a DURABLE CURATORIAL RECORD:

* Targets are NOT foreign keys, so a block SURVIVES deletion of its entity and
  re-attaches when the same entity is re-ingested (feature_idx is content-hash
  stable; genome_idx is (source, source_id)-stable). See the migration comment.
* Unblocking is a SOFT delete (`unblocked_at` / `unblocked_by_idx`), never a row
  DELETE — the who/when/why of a block AND its unblock stay queryable. An ACTIVE
  block is `unblocked_at IS NULL`; resolution and the query endpoint see only
  active rows. Re-blocking an unblocked entity inserts a fresh active row.

Unlike read_mask, absence of an active row safely means "not blocked" — there is
no completion gate, because a curated blocklist is complete by construction.

Every helper accepts a pool or a connection so it composes standalone or inside
an open transaction.
"""

import asyncpg

from ..auth.db import rows_affected

# The resolved excluded feature_idx set: direct feature blocks UNION every
# feature of a blocked genome, ACTIVE blocks only (unblocked_at IS NULL). UNION
# (not UNION ALL) dedups a feature blocked both directly and via its genome.
# Small by design (a curated blocklist).
RESOLVE_EXCLUDED_FEATURES_SQL = (
    "SELECT feature_idx FROM qiita.reference_exclusion"
    "  WHERE feature_idx IS NOT NULL AND unblocked_at IS NULL"
    " UNION"
    " SELECT fg.feature_idx FROM qiita.reference_exclusion x"
    "   JOIN qiita.feature_genome fg USING (genome_idx)"
    "  WHERE x.genome_idx IS NOT NULL AND x.unblocked_at IS NULL"
)

# Blocked features that appear in one reference, with why + external ids. The
# reference's members joined to the ACTIVE exclusion rows that block them.
# feature_genome is many-to-many, so a shared feature fans to one candidate row
# per genome; DISTINCT ON collapses them to one row, preferring, in order:
# (1) the direct feature block (so a dual-blocked feature reports
#     direct_block=true, via_genome=false — see ReferenceExclusionListItem),
# (2) a genome that is itself actively blocked (via an EXISTS against the
#     blocklist — the direct-block candidate rows carry a feature-target x whose
#     own genome_idx is NULL, so the JOINed x cannot answer this),
# (3) the lowest genome_idx.
# So the reported (genome_idx, source, source_id) is deterministic and, whenever
# a genome-level block applies, names an actually-blocked genome, never an
# arbitrary or unblocked one.
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
    "   ON (x.feature_idx = m.feature_idx OR x.genome_idx = m.genome_idx)"
    "  AND x.unblocked_at IS NULL"
    " ORDER BY m.feature_idx, (x.feature_idx IS NOT NULL) DESC,"
    "   (EXISTS (SELECT 1 FROM qiita.reference_exclusion b"
    "             WHERE b.genome_idx = m.genome_idx AND b.unblocked_at IS NULL)) DESC,"
    "   m.genome_idx"
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
    ON CONFLICT DO NOTHING against the target's ACTIVE partial unique index — a
    re-block of an already-active entity keeps the original row and reason and
    returns False. Because the partial index is scoped to `unblocked_at IS NULL`,
    a target whose only prior rows are unblocked (soft-deleted) does NOT conflict,
    so re-blocking it inserts a fresh ACTIVE row and returns True. Returns True iff
    a new active block row was created.

    Does NOT verify the target exists (the columns are not foreign keys — a block
    deliberately outlives its entity); the route existence-checks before calling.
    Raises ValueError if not exactly one target is given (fail-fast before the DB
    CHECK, which remains the backstop for any raw INSERT)."""
    if (genome_idx is None) == (feature_idx is None):
        raise ValueError("exactly one of genome_idx / feature_idx must be given")
    row = await db.fetchrow(
        "INSERT INTO qiita.reference_exclusion"
        " (genome_idx, feature_idx, reason, excluded_by_idx)"
        " VALUES ($1, $2, $3, $4)"
        " ON CONFLICT DO NOTHING"
        " RETURNING reference_exclusion_idx",
        genome_idx,
        feature_idx,
        reason,
        excluded_by_idx,
    )
    return row is not None


async def remove_exclusion(
    db: asyncpg.Pool | asyncpg.Connection,
    *,
    unblocked_by_idx: int,
    genome_idx: int | None = None,
    feature_idx: int | None = None,
) -> int:
    """Unblock exactly one of `genome_idx` / `feature_idx` — a SOFT delete that
    stamps `unblocked_at`/`unblocked_by_idx` on the ACTIVE row, preserving it as a
    curatorial record. Returns rows affected (0 if the entity was not actively
    blocked — idempotent). Raises ValueError if not exactly one target is given."""
    if (genome_idx is None) == (feature_idx is None):
        raise ValueError("exactly one of genome_idx / feature_idx must be given")
    column = "genome_idx" if genome_idx is not None else "feature_idx"
    target = genome_idx if genome_idx is not None else feature_idx
    result = await db.execute(
        "UPDATE qiita.reference_exclusion"
        "   SET unblocked_at = now(), unblocked_by_idx = $1"
        f" WHERE {column} = $2 AND unblocked_at IS NULL",
        unblocked_by_idx,
        target,
    )
    return rows_affected(result)


async def resolve_excluded_features(db: asyncpg.Pool | asyncpg.Connection) -> list[int]:
    """The resolved excluded feature_idx set — direct feature blocks plus every
    feature of a blocked genome, ACTIVE blocks only, deduplicated. This is what
    the data plane anti-joins; the control plane ships it wholesale to the lake."""
    rows = await db.fetch(RESOLVE_EXCLUDED_FEATURES_SQL)
    return [r["feature_idx"] for r in rows]


async def list_for_reference(
    db: asyncpg.Pool | asyncpg.Connection, reference_idx: int
) -> list[asyncpg.Record]:
    """What is CURRENTLY filtered from `reference_idx` and why: one row per
    actively-blocked feature that appears in the reference, carrying the exclusion
    reason, whether it was blocked directly or via its genome, the genome
    provenance (source, source_id), and the reference's own accession for the
    feature. Unblocked (soft-deleted) history and global blocks that touch no
    feature in this reference are absent."""
    return await db.fetch(LIST_FOR_REFERENCE_SQL, reference_idx)
