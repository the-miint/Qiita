"""Reference management routes.

Every route is wired to the principal resolver and guards. POST /reference
uses created_by_idx (BIGINT FK to qiita.principal) as the canonical owner
reference. GET /reference/{id} stays anonymous-OK (`get_current_principal`
directly, no guard); other routes pin to scope and kind constraints.
"""

import asyncio
import base64
import json
from typing import Annotated
from uuid import UUID

import asyncpg
import pyarrow.flight as _flight
from fastapi import APIRouter, Depends, HTTPException
from pydantic import Field
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    VALID_STATUS_TRANSITIONS,
    DoGetTicketRequest,
    DoGetTicketResponse,
    FeatureHashEntry,
    FeatureMintRequest,
    FeatureMintResponse,
    ReferenceCreateRequest,
    ReferenceResponse,
    ReferenceStatusUpdate,
    RegisterFilesRequest,
    RegisterFilesResponse,
)

from ..auth.guards import (
    require_complete_profile,
    require_scope,
    require_service,
)
from ..auth.principal import (
    HumanUser,
    Principal,
    ServiceAccount,
    get_current_principal,
)
from ..auth.tickets import sign_action, sign_ticket
from ..deps import get_data_plane_url, get_db_pool, get_hmac_secret

router = APIRouter(prefix="/reference", tags=["reference"])

# Chunk size for batch processing. Array params avoid the Postgres $65535 scalar
# parameter limit, but large arrays increase memory pressure and transaction
# duration. 10K is a pragmatic default for the expected feature batch sizes.
_CHUNK_SIZE = 10_000


_REFERENCE_RETURNING = "reference_idx, name, version, kind, status, created_by_idx, created_at"

_MSG_REFERENCE_NOT_FOUND = "Reference not found"


@router.post("", status_code=201)
async def create_reference(
    body: ReferenceCreateRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.REFERENCES_WRITE)),
) -> ReferenceResponse:
    """Create a reference. Humans only — service-kind principals can only
    mint features and register files into existing references."""
    try:
        row = await pool.fetchrow(
            "INSERT INTO qiita.reference"
            "  (name, version, kind, created_by_idx)"
            " VALUES ($1, $2, $3, $4)"
            f" RETURNING {_REFERENCE_RETURNING}",
            body.name,
            body.version,
            body.kind,
            user.principal_idx,
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
    _principal: Principal = Depends(get_current_principal),
) -> ReferenceResponse:
    """Anonymous-OK. Returns the full ReferenceResponse including
    created_by_idx; row-level visibility (e.g., hiding private references'
    owner) is not yet implemented."""
    row = await pool.fetchrow(
        f"SELECT {_REFERENCE_RETURNING} FROM qiita.reference WHERE reference_idx = $1",
        reference_idx,
    )
    if row is None:
        raise HTTPException(status_code=404, detail=_MSG_REFERENCE_NOT_FOUND)
    return ReferenceResponse(**dict(row))


@router.patch("/{reference_idx}/status")
async def update_reference_status(
    reference_idx: Annotated[int, Field(gt=0)],
    body: ReferenceStatusUpdate,
    pool: asyncpg.Pool = Depends(get_db_pool),
    _scope: Principal = Depends(require_scope(Scope.REFERENCES_WRITE)),
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
        "UPDATE qiita.reference SET status = $1"
        " WHERE reference_idx = $2 AND status = ANY($3::text[])"
        f" RETURNING {_REFERENCE_RETURNING}",
        str(target_status),
        reference_idx,
        valid_sources,
    )
    if row is not None:
        return ReferenceResponse(**dict(row))

    # Distinguish 404 from 409
    current = await pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1",
        reference_idx,
    )
    if current is None:
        raise HTTPException(status_code=404, detail=_MSG_REFERENCE_NOT_FOUND)
    raise HTTPException(
        status_code=409,
        detail=f"Cannot transition from {current!r} to {target_status!r}",
    )


@router.post("/{reference_idx}/feature/mint")
async def mint_features(
    reference_idx: Annotated[int, Field(gt=0)],
    body: FeatureMintRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    _service: ServiceAccount = Depends(require_service),
    _scope: Principal = Depends(require_scope(Scope.FEATURES_MINT)),
) -> FeatureMintResponse:
    # Atomic status transition: hashing -> minting, or verify already minting.
    # Uses UPDATE ... WHERE to avoid TOCTOU race between concurrent callers.
    updated = await pool.fetchval(
        "UPDATE qiita.reference SET status = 'minting'"
        " WHERE reference_idx = $1 AND status IN ('hashing', 'minting')"
        " RETURNING reference_idx",
        reference_idx,
    )
    if updated is None:
        # Distinguish "not found" from "wrong status"
        ref = await pool.fetchrow(
            "SELECT status FROM qiita.reference WHERE reference_idx = $1",
            reference_idx,
        )
        if ref is None:
            raise HTTPException(status_code=404, detail=_MSG_REFERENCE_NOT_FOUND)
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


# Tables that can appear in a DoGet ticket. Must match the data plane's
# ALLOWED_TABLES whitelist in flight_service.rs.
_DOGET_ALLOWED_TABLES = frozenset(
    {
        "reference_sequences",
        "reference_sequence_chunks",
        "reference_taxonomy",
        "reference_phylogeny",
        "reference_placements",
    }
)


@router.post("/{reference_idx}/ticket/doget", status_code=201)
async def create_doget_ticket(
    reference_idx: Annotated[int, Field(gt=0)],
    body: DoGetTicketRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    hmac_secret: bytes = Depends(get_hmac_secret),
    _scope: Principal = Depends(require_scope(Scope.TICKETS_DOGET)),
) -> DoGetTicketResponse:
    """Sign a DoGet ticket scoped to a reference.

    Reference must be active. The ticket contains only reference_idx — the
    data plane resolves feature membership at query time via the DuckLake
    reference_membership table (JOIN for reference_sequences, direct
    WHERE for taxonomy/phylogeny).

    Authorization is scope-only at this layer: any principal with
    `tickets:doget` can request a ticket. Row-level visibility (private
    references) is not yet implemented.
    """
    if body.table not in _DOGET_ALLOWED_TABLES:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown table {body.table!r}; allowed: {sorted(_DOGET_ALLOWED_TABLES)}",
        )

    # Reference must be active
    status = await pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1",
        reference_idx,
    )
    if status is None:
        raise HTTPException(status_code=404, detail=_MSG_REFERENCE_NOT_FOUND)
    if status != "active":
        raise HTTPException(
            status_code=409,
            detail=f"Reference status is {status!r}, must be 'active'",
        )

    ticket_bytes = sign_ticket(
        table=body.table,
        filter={"reference_idx": [reference_idx]},
        secret=hmac_secret,
    )
    return DoGetTicketResponse(ticket=base64.b64encode(ticket_bytes).decode())


@router.post("/{reference_idx}/register", status_code=201)
async def register_files(
    reference_idx: Annotated[int, Field(gt=0)],
    body: RegisterFilesRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    hmac_secret: bytes = Depends(get_hmac_secret),
    data_plane_url: str = Depends(get_data_plane_url),
    _service: ServiceAccount = Depends(require_service),
    _scope: Principal = Depends(require_scope(Scope.REFERENCES_REGISTER_FILES)),
) -> RegisterFilesResponse:
    """Register Parquet files in DuckLake via the data plane's DoAction.

    Workers only — the orchestrator is the canonical caller. Reference
    must be in 'loading' status.
    """
    status = await pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1",
        reference_idx,
    )
    if status is None:
        raise HTTPException(status_code=404, detail=_MSG_REFERENCE_NOT_FOUND)
    if status != "loading":
        raise HTTPException(
            status_code=409,
            detail=f"Reference status is {status!r}, must be 'loading'",
        )

    # Sign an action token for the data plane
    token = sign_action(
        action="register_files",
        payload={"staging_dir": body.staging_dir, "files": body.files},
        secret=hmac_secret,
    )

    # Call data plane DoAction — offloaded to thread because pyarrow
    # FlightClient is synchronous and would block the event loop.
    try:
        results = await asyncio.get_event_loop().run_in_executor(
            None, _do_action_register, data_plane_url, token
        )
    except _flight.FlightError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Data plane registration failed: {exc}",
        ) from exc

    if not results:
        return RegisterFilesResponse(registered=[])

    result_body = json.loads(results[0].body.to_pybytes())
    return RegisterFilesResponse(registered=result_body.get("registered", []))


def _do_action_register(data_plane_url: str, token: bytes) -> list:
    """Synchronous gRPC call to data plane — runs in thread executor."""
    with _flight.FlightClient(data_plane_url) as client:
        action = _flight.Action("register_files", token)
        return list(client.do_action(action))
