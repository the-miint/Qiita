"""Feature minting routes.

`POST /feature/mint` is the pure-mint primitive: sequence-hash entries in,
feature_idx mapping out, no reference context. Genome associations
(feature_genome) are written here when the input carries them, since
genomes are reference-agnostic.

Linking already-minted feature_idx values to a reference lives in
`POST /reference/{reference_idx}/membership` (routes/reference.py).
Reference status transitions live in `PATCH /reference/{reference_idx}/status`
and are driven externally by the orchestrator.
"""

from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    FeatureHashEntry,
    FeatureMintRequest,
    FeatureMintResponse,
)

from ..auth.guards import require_scope, require_service
from ..auth.principal import Principal, ServiceAccount
from ..deps import get_db_pool

router = APIRouter(prefix="/feature", tags=["feature"])

# Chunk size for batch processing. Array params avoid the Postgres $65535
# scalar parameter limit, but large arrays increase memory pressure and
# transaction duration. 10K is a pragmatic default for the expected
# feature batch sizes.
_CHUNK_SIZE = 10_000


@router.post("/mint")
async def mint_features(
    body: FeatureMintRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    _service: ServiceAccount = Depends(require_service),
    _scope: Principal = Depends(require_scope(Scope.FEATURE_MINT)),
) -> FeatureMintResponse:
    """Mint feature_idx values for sequence hashes; reference-agnostic.

    Resubmitting the same batch is safe: features dedupe via
    `ON CONFLICT (sequence_hash) DO NOTHING` and feature_genome via
    `ON CONFLICT DO NOTHING`. The mapping returned covers every input
    hash; novel ones go into `minted`, pre-existing into `reused`.

    Caller is responsible for any reference-side bookkeeping
    (`POST /reference/{idx}/membership` to link, status transitions via
    `PATCH /reference/{idx}/status`).
    """
    full_mapping: dict[UUID, int] = {}
    total_minted = 0
    total_reused = 0

    for i in range(0, len(body.entries), _CHUNK_SIZE):
        chunk = body.entries[i : i + _CHUNK_SIZE]
        async with pool.acquire() as conn, conn.transaction():
            chunk_mapping, minted, reused = await _mint_chunk(conn, chunk)
            genome_entries = [e for e in chunk if e.genome_source is not None]
            if genome_entries:
                await _write_genome_associations(conn, genome_entries, chunk_mapping)
        full_mapping.update(chunk_mapping)
        total_minted += minted
        total_reused += reused

    return FeatureMintResponse(mapping=full_mapping, minted=total_minted, reused=total_reused)


async def _mint_chunk(
    conn: asyncpg.Connection,
    entries: list[FeatureHashEntry],
) -> tuple[dict[UUID, int], int, int]:
    """Upsert features, return (mapping, minted_count, reused_count)."""
    hashes = [e.sequence_hash for e in entries]

    # Find pre-existing
    existing = await conn.fetch(
        "SELECT feature_idx, sequence_hash FROM qiita.feature"
        " WHERE sequence_hash = ANY($1::uuid[])",
        hashes,
    )
    existing_map = {row["sequence_hash"]: row["feature_idx"] for row in existing}

    # Insert novel
    novel = [h for h in hashes if h not in existing_map]
    new_map: dict[UUID, int] = {}
    concurrent_reused = 0
    if novel:
        new_rows = await conn.fetch(
            "INSERT INTO qiita.feature (sequence_hash)"
            " SELECT unnest($1::uuid[])"
            " ON CONFLICT (sequence_hash) DO NOTHING"
            " RETURNING feature_idx, sequence_hash",
            novel,
        )
        new_map = {row["sequence_hash"]: row["feature_idx"] for row in new_rows}

        # Handle concurrent inserts: ON CONFLICT DO NOTHING means some may not RETURN.
        # These rows were created by another transaction — count them as reused.
        still_missing = [h for h in novel if h not in new_map]
        if still_missing:
            extra = await conn.fetch(
                "SELECT feature_idx, sequence_hash FROM qiita.feature"
                " WHERE sequence_hash = ANY($1::uuid[])",
                still_missing,
            )
            for row in extra:
                existing_map[row["sequence_hash"]] = row["feature_idx"]
            concurrent_reused = len(extra)

    mapping = {**existing_map, **new_map}

    # Every input hash must have a mapping — anything missing is a data integrity failure
    unmapped = set(hashes) - set(mapping.keys())
    if unmapped:
        raise RuntimeError(f"Failed to resolve feature_idx for {len(unmapped)} hashes")

    return mapping, len(new_map), len(existing_map) + concurrent_reused


async def _write_genome_associations(
    conn: asyncpg.Connection,
    entries: list[FeatureHashEntry],
    mapping: dict[UUID, int],
) -> None:
    """Batch upsert genomes and write feature_genome junction rows."""
    sources = [e.genome_source for e in entries]
    source_ids = [e.genome_source_id for e in entries]

    # Batch upsert genomes — DO UPDATE to guarantee RETURNING for every row
    genome_rows = await conn.fetch(
        "INSERT INTO qiita.genome (source, source_id)"
        " SELECT unnest($1::text[]), unnest($2::text[])"
        " ON CONFLICT (source, source_id) DO UPDATE SET source = EXCLUDED.source"
        " RETURNING genome_idx, source, source_id",
        sources,
        source_ids,
    )
    genome_map = {(row["source"], row["source_id"]): row["genome_idx"] for row in genome_rows}

    # Batch insert feature_genome junctions
    feat_idxs = [mapping[e.sequence_hash] for e in entries]
    genome_idxs = [genome_map[(e.genome_source, e.genome_source_id)] for e in entries]
    await conn.execute(
        "INSERT INTO qiita.feature_genome (feature_idx, genome_idx)"
        " SELECT unnest($1::bigint[]), unnest($2::bigint[])"
        " ON CONFLICT DO NOTHING",
        feat_idxs,
        genome_idxs,
    )
