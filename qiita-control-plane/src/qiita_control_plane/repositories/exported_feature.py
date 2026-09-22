"""Reads and the idempotent mint for `qiita.exported_feature` — the public handle
a published artifact carries in place of our `genome_idx` / `feature_idx`.

The mint is **two passes**, and that is the design rather than a retry loop
bolted on. Pass one offers each entity its real accession; the UNIQUE index on
the published namespace rejects an accession another row already holds, and pass
two gives exactly those entities a minted `QF<idx>` instead. The database has to
arbitrate because no client can: a caller sees the entities of one artifact,
never the accession some other caller published last week.

Which entity keeps a contested accession is decided by **ascending idx** — see
the ORDER BY on each INSERT. That rule is arbitrary; what matters is that it is
fixed, because without it the winner is whatever row order the planner happened
to produce and it could change under a stats refresh alone.

The migration `db/migrations/20260813000000_exported_feature.sql` owns *why* the
namespace works this way — the snapshot, the two-pass fallback, the asymmetric
published-namespace index, what a new entity kind must update — and none of that
is restated here. What is decided here instead is which entities have an accession
to offer at all: `_SOURCE_ID_IS_EXTERNAL_ACCESSION` below is the authority for the
genome kind, over the migration's prose, which reads a genome as always carrying
one.
"""

from collections.abc import Sequence

import asyncpg
from qiita_common.models import GenomeSource

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

# `ON CONFLICT DO NOTHING` carries no inference target on either INSERT below, and
# that is forced rather than chosen: Postgres allows one target per clause, and two
# distinct constraints have to be caught here — the per-entity live index (the row
# already exists, so the mint is idempotent) and the published-namespace index (the
# accession is taken, so pass two mints a handle instead). Catching by raising and
# handling the violation would poison the enclosing transaction and force a savepoint
# per entity.
#
# The cost, and it is a real one: an untargeted DO NOTHING will silently swallow ANY
# future unique constraint on this table. Anyone adding one has to come back here and
# decide whether it should be caught or should fail loudly, because as written it will
# be caught.
_ON_CONFLICT = " ON CONFLICT DO NOTHING"

# Whether a genome's `source_id` is an accession outside Qiita, per source.
# `source = 'qiita'` marks a genome assembled from one of our own prep_samples (the
# `genome_qiita_origin_check` biconditional on qiita.genome); its source_id arrives
# verbatim from the loader's genome map, with no external authority behind it, and
# nothing outside this system resolves it. `export_feature_id` is published, which
# is what the opaque-identifier rule in CLAUDE.md governs. The true half is a
# DECLARED source, not a verified one: `actions.library._validate_genome_map` checks
# `genome_source` against the vocabulary and the qiita-origin biconditional, never
# the id.
#
# `_INSERT_GENOME` reads the true half as an allowlist rather than testing
# `<> 'qiita'`, so a source absent from this map is offered no accession and takes
# the minted handle instead of publishing whatever its source_id holds. Every
# GenomeSource member must appear here; `tests/test_exported_feature_mint.py` fails
# on one that does not.
_SOURCE_ID_IS_EXTERNAL_ACCESSION: dict[GenomeSource, bool] = {
    GenomeSource.GENBANK: True,
    GenomeSource.REFSEQ: True,
    GenomeSource.QIITA: False,
}

_EXTERNAL_GENOME_SOURCES: list[str] = [
    source.value for source, external in _SOURCE_ID_IS_EXTERNAL_ACCESSION.items() if external
]

# The genome kind. The JOIN is not decoration: it both resolves the accession and
# proves the genome exists, so an unknown genome_idx drops out here and is reported
# by the caller's completeness check rather than raising a foreign-key error whose
# message names our schema. Which sources are offered their source_id is
# `_SOURCE_ID_IS_EXTERNAL_ACCESSION` above; the rest are named 'QF<idx>' in this same
# pass and so never come back unnamed for pass two.
#
# ORDER BY: when one request names two entities whose accessions collide with each
# other, the lower idx keeps the accession. `_INSERT_FEATURE` orders for the same
# reason; the module docstring says why the rule has to exist at all.
_INSERT_GENOME = (
    "INSERT INTO qiita.exported_feature"
    "       (entity_kind, genome_idx, accession, accession_published, created_by_idx)"
    " SELECT 'genome', c.genome_idx, c.accession, $3 AND c.accession IS NOT NULL, $2"
    "   FROM (SELECT g.genome_idx,"
    "                CASE WHEN gn.source = ANY($4::text[]) THEN gn.source_id END AS accession"
    "           FROM unnest($1::bigint[]) AS g(genome_idx)"
    "           JOIN qiita.genome gn ON gn.genome_idx = g.genome_idx) AS c"
    "  ORDER BY c.genome_idx" + _ON_CONFLICT
)

# The feature kind. The JOIN against reference_membership resolves the accession
# AND enforces that the feature is in the reference the caller named — a feature
# outside it has no accession from it, so there would be nothing to publish.
# `rm.accession` is nullable, so `accession_published` can only be true when the
# caller asked for the accession attempt AND one exists.
_INSERT_FEATURE = (
    "INSERT INTO qiita.exported_feature"
    "       (entity_kind, reference_idx, feature_idx, accession, accession_published,"
    "        created_by_idx)"
    " SELECT 'feature', $1, f.feature_idx, rm.accession, $4 AND rm.accession IS NOT NULL, $3"
    "   FROM unnest($2::bigint[]) AS f(feature_idx)"
    "   JOIN qiita.reference_membership rm"
    "     ON rm.reference_idx = $1 AND rm.feature_idx = f.feature_idx"
    "  ORDER BY f.feature_idx" + _ON_CONFLICT
)


class IncompleteMintError(RuntimeError):
    """The mint came back short of the entity set it was asked for.

    Carries the missing identifiers so the route can say which.
    """

    def __init__(self, *, genome_idx: list[int], feature_idx: list[int]) -> None:
        self.genome_idx = genome_idx
        self.feature_idx = feature_idx
        total = len(genome_idx) + len(feature_idx)
        super().__init__(
            f"{total} entit(ies) have no live exported feature identifier after minting:"
            f" genome_idx={genome_idx}, feature_idx={feature_idx}"
        )


async def _offer(
    conn: asyncpg.Connection,
    *,
    genome_idx: Sequence[int],
    reference_idx: int | None,
    feature_idx: Sequence[int],
    created_by_idx: int,
    accession: bool,
) -> None:
    """Insert an identifier for each named entity, offering it its accession or not."""
    if genome_idx:
        await conn.execute(
            _INSERT_GENOME,
            list(genome_idx),
            created_by_idx,
            accession,
            _EXTERNAL_GENOME_SOURCES,
        )
    if feature_idx and reference_idx is not None:
        await conn.execute(
            _INSERT_FEATURE, reference_idx, list(feature_idx), created_by_idx, accession
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

    **The second pass is where a collided accession lands**, and it re-offers only
    the entities that came back unnamed — the whole set would be safe (an INSERT
    cannot overwrite a live row) but would pay for the rare collision on every mint.
    A minted `QF<idx>` cannot collide in turn, because `idx` is unique. An entity
    that does not exist, or a feature outside the named reference, is still missing
    after pass two and raises.

    **Raises `IncompleteMintError` rather than returning a short list.** A map
    quietly missing an entity is a published table with an unnamed row, which is far
    worse than a failed request.
    """
    async with pool.acquire() as conn, conn.transaction():
        await _offer(
            conn,
            genome_idx=genome_idx,
            reference_idx=reference_idx,
            feature_idx=feature_idx,
            created_by_idx=created_by_idx,
            accession=True,
        )
        rows = await conn.fetch(_SELECT_LIVE, genome_idx, reference_idx, feature_idx)
        unnamed_genomes, unnamed_features = _missing(rows, genome_idx, feature_idx)
        if unnamed_genomes or unnamed_features:
            await _offer(
                conn,
                genome_idx=unnamed_genomes,
                reference_idx=reference_idx,
                feature_idx=unnamed_features,
                created_by_idx=created_by_idx,
                accession=False,
            )
            rows = await conn.fetch(_SELECT_LIVE, genome_idx, reference_idx, feature_idx)
            unnamed_genomes, unnamed_features = _missing(rows, genome_idx, feature_idx)

    if unnamed_genomes or unnamed_features:
        raise IncompleteMintError(genome_idx=unnamed_genomes, feature_idx=unnamed_features)
    return rows
