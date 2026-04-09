"""Reference management routes."""

from typing import Annotated
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from pydantic import Field
from qiita_common.models import (
    VALID_STATUS_TRANSITIONS,
    FeatureHashEntry,
    FeatureMintRequest,
    FeatureMintResponse,
    PhylogenyTipRequest,
    PhylogenyTipResponse,
    ReferenceCreateRequest,
    ReferenceResponse,
    ReferenceStatusUpdate,
)

from ..deps import get_current_user, get_db_pool

router = APIRouter(prefix="/references", tags=["references"])

# Chunk size for batch processing. Array params avoid the Postgres $65535 scalar
# parameter limit, but large arrays increase memory pressure and transaction
# duration. 10K is a pragmatic default for the expected feature batch sizes.
_CHUNK_SIZE = 10_000


@router.post("", status_code=201)
async def create_reference(
    body: ReferenceCreateRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    user_id: UUID = Depends(get_current_user),
) -> ReferenceResponse:
    try:
        row = await pool.fetchrow(
            "INSERT INTO qiita.references (name, version, kind, created_by)"
            " VALUES ($1, $2, $3, $4)"
            " RETURNING reference_idx, name, version, kind, status, created_by, created_at",
            body.name,
            body.version,
            body.kind,
            user_id,
        )
    except asyncpg.UniqueViolationError:
        raise HTTPException(
            status_code=409,
            detail=f"Reference ({body.name!r}, {body.version!r}) already exists",
        )
    except asyncpg.PostgresError as exc:
        raise HTTPException(status_code=500, detail="Database error") from exc
    return ReferenceResponse(**dict(row))


@router.get("/{reference_idx}")
async def get_reference(
    reference_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> ReferenceResponse:
    row = await pool.fetchrow(
        "SELECT reference_idx, name, version, kind, status, created_by, created_at"
        " FROM qiita.references WHERE reference_idx = $1",
        reference_idx,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Reference not found")
    return ReferenceResponse(**dict(row))


@router.patch("/{reference_idx}/status")
async def update_reference_status(
    reference_idx: Annotated[int, Field(gt=0)],
    body: ReferenceStatusUpdate,
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> ReferenceResponse:
    target_status = body.status

    # Build the set of valid source statuses for this target
    valid_sources = [
        str(src) for src, targets in VALID_STATUS_TRANSITIONS.items() if target_status in targets
    ]
    if not valid_sources:
        raise HTTPException(
            status_code=409,
            detail=f"No valid transition to {target_status!r}",
        )

    # Atomic conditional UPDATE — avoids TOCTOU race
    row = await pool.fetchrow(
        "UPDATE qiita.references SET status = $1"
        " WHERE reference_idx = $2 AND status = ANY($3::text[])"
        " RETURNING reference_idx, name, version, kind, status, created_by, created_at",
        str(target_status),
        reference_idx,
        valid_sources,
    )
    if row is not None:
        return ReferenceResponse(**dict(row))

    # Distinguish 404 from 409
    current = await pool.fetchval(
        "SELECT status FROM qiita.references WHERE reference_idx = $1",
        reference_idx,
    )
    if current is None:
        raise HTTPException(status_code=404, detail="Reference not found")
    raise HTTPException(
        status_code=409,
        detail=f"Cannot transition from {current!r} to {target_status!r}",
    )


@router.post("/{reference_idx}/features/mint")
async def mint_features(
    reference_idx: Annotated[int, Field(gt=0)],
    body: FeatureMintRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    user_id: UUID = Depends(get_current_user),
) -> FeatureMintResponse:
    # Atomic status transition: hashing -> minting, or verify already minting.
    # Uses UPDATE ... WHERE to avoid TOCTOU race between concurrent callers.
    updated = await pool.fetchval(
        "UPDATE qiita.references SET status = 'minting'"
        " WHERE reference_idx = $1 AND status IN ('hashing', 'minting')"
        " RETURNING reference_idx",
        reference_idx,
    )
    if updated is None:
        # Distinguish "not found" from "wrong status"
        ref = await pool.fetchrow(
            "SELECT status FROM qiita.references WHERE reference_idx = $1",
            reference_idx,
        )
        if ref is None:
            raise HTTPException(status_code=404, detail="Reference not found")
        raise HTTPException(
            status_code=409,
            detail=f"Reference status is {ref['status']!r}, must be 'hashing' or 'minting'",
        )

    full_mapping: dict[UUID, int] = {}
    total_minted = 0
    total_reused = 0

    # Partial failure is recoverable: resubmitting the full batch is safe because
    # features and membership use ON CONFLICT DO NOTHING. Status remains 'minting'
    # and subsequent calls will succeed for the remaining entries.
    for i in range(0, len(body.entries), _CHUNK_SIZE):
        chunk = body.entries[i : i + _CHUNK_SIZE]
        async with pool.acquire() as conn:
            async with conn.transaction():
                chunk_mapping, minted, reused = await _mint_chunk(conn, chunk)
                await _write_membership(conn, reference_idx, list(chunk_mapping.values()))
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
        "SELECT feature_idx, sequence_hash FROM qiita.features"
        " WHERE sequence_hash = ANY($1::uuid[])",
        hashes,
    )
    existing_map = {row["sequence_hash"]: row["feature_idx"] for row in existing}

    # Insert novel
    novel = [h for h in hashes if h not in existing_map]
    new_map: dict[UUID, int] = {}
    if novel:
        new_rows = await conn.fetch(
            "INSERT INTO qiita.features (sequence_hash)"
            " SELECT unnest($1::uuid[])"
            " ON CONFLICT (sequence_hash) DO NOTHING"
            " RETURNING feature_idx, sequence_hash",
            novel,
        )
        new_map = {row["sequence_hash"]: row["feature_idx"] for row in new_rows}

        # Handle concurrent inserts: ON CONFLICT DO NOTHING means some may not RETURN
        still_missing = [h for h in novel if h not in new_map]
        if still_missing:
            extra = await conn.fetch(
                "SELECT feature_idx, sequence_hash FROM qiita.features"
                " WHERE sequence_hash = ANY($1::uuid[])",
                still_missing,
            )
            new_map.update({row["sequence_hash"]: row["feature_idx"] for row in extra})

    mapping = {**existing_map, **new_map}

    # Every input hash must have a mapping — anything missing is a data integrity failure
    unmapped = set(hashes) - set(mapping.keys())
    if unmapped:
        raise RuntimeError(f"Failed to resolve feature_idx for {len(unmapped)} hashes")

    return mapping, len(new_map), len(existing_map)


async def _write_membership(
    conn: asyncpg.Connection, reference_idx: int, feature_idxs: list[int]
) -> None:
    """Write reference_membership rows, ignoring duplicates."""
    if not feature_idxs:
        return
    await conn.execute(
        "INSERT INTO qiita.reference_membership (reference_idx, feature_idx)"
        " SELECT $1, unnest($2::bigint[])"
        " ON CONFLICT DO NOTHING",
        reference_idx,
        feature_idxs,
    )


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
        "INSERT INTO qiita.genomes (source, source_id)"
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


@router.post("/{reference_idx}/phylogeny-tips", status_code=201)
async def write_phylogeny_tips(
    reference_idx: Annotated[int, Field(gt=0)],
    body: PhylogenyTipRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> PhylogenyTipResponse:
    """Bulk insert phylogeny tip-to-feature mappings.

    Reference must be in 'loading' status. Uses ON CONFLICT DO NOTHING
    for idempotent resubmission.
    """
    # Verify reference is in loading status
    status = await pool.fetchval(
        "SELECT status FROM qiita.references WHERE reference_idx = $1",
        reference_idx,
    )
    if status is None:
        raise HTTPException(status_code=404, detail="Reference not found")
    if status != "loading":
        raise HTTPException(
            status_code=409,
            detail=f"Reference status is {status!r}, must be 'loading'",
        )

    # Validate all entries have matching reference_idx
    bad = [e for e in body.entries if e.reference_idx != reference_idx]
    if bad:
        raise HTTPException(
            status_code=422,
            detail=f"Entry reference_idx must match path ({reference_idx})",
        )

    ref_idxs = [e.reference_idx for e in body.entries]
    node_idxs = [e.node_index for e in body.entries]
    feat_idxs = [e.feature_idx for e in body.entries]

    result = await pool.execute(
        "INSERT INTO qiita.phylogeny_tip_feature (reference_idx, node_index, feature_idx)"
        " SELECT unnest($1::bigint[]), unnest($2::bigint[]), unnest($3::bigint[])"
        " ON CONFLICT DO NOTHING",
        ref_idxs,
        node_idxs,
        feat_idxs,
    )
    # asyncpg returns "INSERT 0 N" where N is the count
    inserted = int(result.split()[-1])
    return PhylogenyTipResponse(inserted=inserted)
