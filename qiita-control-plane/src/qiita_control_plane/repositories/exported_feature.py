"""Reads and the idempotent mint for `qiita.exported_feature` — the public handle
a published artifact carries in place of our `genome_idx` / `feature_idx`.

The mint is **two passes**, and that is the design rather than a retry loop
bolted on. Pass one offers each entity its real accession; the UNIQUE index on
the published namespace rejects an accession another live row already holds, and
pass two gives exactly those entities a minted `QF<idx>` instead. The database
has to arbitrate because no client can: a caller sees the entities of one
artifact, never the accession some other caller published last week.
"""

from collections.abc import Sequence

import asyncpg

# Every caller-visible column. `accession` rides along even when it lost, so a
# caller who gets a QF handle for an entity that plainly has an accession can tell
# "there was none" from "another entity published it first".
#
# Genome entries first, then feature entries, each ascending — the order the
# response documents. `genome_idx IS NOT NULL` sorts before its complement in the
# ORDER BY because false < true.
_SELECT_LIVE = (
    "SELECT genome_idx, reference_idx, feature_idx, export_feature_id,"
    "       accession, accession_published"
    "  FROM qiita.exported_feature"
    " WHERE NOT retired"
    "   AND (genome_idx = ANY($1::bigint[])"
    "        OR (reference_idx = $2 AND feature_idx = ANY($3::bigint[])))"
    " ORDER BY genome_idx IS NULL, genome_idx, feature_idx"
)

# The genome kind. The JOIN is not decoration: it both resolves the accession and
# proves the genome exists, so an unknown genome_idx drops out here and is reported
# by the caller's completeness check rather than raising a foreign-key error whose
# message names our schema. source_id is NOT NULL on qiita.genome, so the genome
# kind always has an accession to offer.
_INSERT_GENOME = (
    "INSERT INTO qiita.exported_feature"
    "       (genome_idx, accession, accession_published, created_by_idx)"
    " SELECT g.genome_idx, gn.source_id, $3, $2"
    "   FROM unnest($1::bigint[]) AS g(genome_idx)"
    "   JOIN qiita.genome gn ON gn.genome_idx = g.genome_idx"
    " ON CONFLICT DO NOTHING"
)

# The feature kind. The JOIN against reference_membership resolves the accession
# AND enforces that the feature is in the reference the caller named — a feature
# outside it has no accession from it, so there would be nothing to publish.
# `rm.accession` is nullable, so `accession_published` can only be true when the
# caller asked for the accession attempt AND one exists.
_INSERT_FEATURE = (
    "INSERT INTO qiita.exported_feature"
    "       (reference_idx, feature_idx, accession, accession_published, created_by_idx)"
    " SELECT $1, f.feature_idx, rm.accession, $4 AND rm.accession IS NOT NULL, $3"
    "   FROM unnest($2::bigint[]) AS f(feature_idx)"
    "   JOIN qiita.reference_membership rm"
    "     ON rm.reference_idx = $1 AND rm.feature_idx = f.feature_idx"
    " ON CONFLICT DO NOTHING"
)


class IncompleteMintError(RuntimeError):
    """The mint came back short of the entity set it was asked for.

    Carries the missing identifiers so the route can say which. Unlike a collided
    accession — which costs a label and nothing else — a missing entity means the
    artifact would have an unnamed row, so it is fatal.
    """

    def __init__(self, *, genome_idx: list[int], feature_idx: list[int]) -> None:
        self.genome_idx = genome_idx
        self.feature_idx = feature_idx
        total = len(genome_idx) + len(feature_idx)
        super().__init__(
            f"{total} entit(ies) have no live exported feature identifier after minting:"
            f" genome_idx={genome_idx}, feature_idx={feature_idx}"
        )


def _missing(
    rows: Sequence[asyncpg.Record], genome_idx: Sequence[int], feature_idx: Sequence[int]
) -> tuple[list[int], list[int]]:
    """The requested entities with no row in `rows`, each ascending.

    A separate function because it is the only part of the mint that can be
    exercised without racing two transactions, and what it guards — every requested
    entity is named, or the request fails — is the response's headline promise.
    """
    seen_genomes = {row["genome_idx"] for row in rows if row["genome_idx"] is not None}
    seen_features = {row["feature_idx"] for row in rows if row["feature_idx"] is not None}
    return (
        sorted(set(genome_idx) - seen_genomes),
        sorted(set(feature_idx) - seen_features),
    )


async def mint_exported_features(
    pool: asyncpg.Pool,
    *,
    genome_idx: list[int],
    reference_idx: int | None,
    feature_idx: list[int],
    created_by_idx: int,
) -> list[asyncpg.Record]:
    """Ensure a live `export_feature_id` exists for every named entity, and return
    them all — genome entries first ascending, then feature entries ascending.

    **Idempotent, and that is the contract rather than an optimization**: the
    identifier is published, so asking twice for the same entity must give the same
    answer both times. Each INSERT conflicts against the per-kind partial unique
    index on live rows and does nothing, so a re-request adds no row and changes no
    identifier.

    Two concurrent callers minting the same fresh entity are safe without a lock:
    both INSERT, the index lets exactly one row win, `DO NOTHING` swallows the
    loser, and the SELECT hands the winner to both.

    **The second pass is where a collided accession lands.** `ON CONFLICT DO
    NOTHING` carries no target, so it also swallows a violation of the published
    namespace — an entity whose accession another live row already holds simply has
    no row after pass one. Those, and only those, are re-offered with
    `accession_published = false`, which generates `QF<idx>` and cannot collide
    because `idx` is unique. An entity that does not exist (or a feature outside the
    named reference) is missing after pass two as well, and raises.

    **Raises `IncompleteMintError` rather than returning a short list.** A map
    quietly missing an entity is a published table with an unnamed row, which is far
    worse than a failed request.
    """
    async with pool.acquire() as conn, conn.transaction():
        for attempt_accession in (True, False):
            if genome_idx:
                await conn.execute(_INSERT_GENOME, genome_idx, created_by_idx, attempt_accession)
            if feature_idx and reference_idx is not None:
                await conn.execute(
                    _INSERT_FEATURE,
                    reference_idx,
                    feature_idx,
                    created_by_idx,
                    attempt_accession,
                )
            rows = await conn.fetch(_SELECT_LIVE, genome_idx, reference_idx, feature_idx)
            if not any(_missing(rows, genome_idx, feature_idx)):
                break

    missing_genomes, missing_features = _missing(rows, genome_idx, feature_idx)
    if missing_genomes or missing_features:
        raise IncompleteMintError(genome_idx=missing_genomes, feature_idx=missing_features)
    return rows
