"""Named primitives composed by workflows.

A workflow YAML entry like

    - action: mint-features
      inputs: [hash.manifest]
      outputs: [feature_map.ndjson]

resolves through the LIBRARY dict at the bottom of this module. The
runner looks up the name and invokes the callable with the inputs the
entry declares. The same primitive backs the corresponding REST route
(POST /feature/mint, POST /reference/{idx}/membership,
POST /reference/{idx}/register), so HTTP callers and workflow
invocations share one implementation — divergence here would silently
mis-mint features or skip registration steps.

Library callables take the asyncpg pool plus the inputs they need;
status-state guards are the caller's responsibility because routes
return HTTPException on bad state and workflow runners want to handle
it differently. Internal per-chunk helpers (`_mint_chunk` and friends)
are exported for the deprecated POST /reference/{idx}/feature/mint
route, which writes mint+membership+genome inside one transaction and
doesn't fit any single public primitive.
"""

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

import asyncpg
import pyarrow.flight as _flight
from qiita_common.api_paths import LibraryPrimitive
from qiita_common.models import FeatureHashEntry

from ..auth.tickets import sign_action

# Chunk size for batch processing. Array params avoid the Postgres $65535
# scalar parameter limit, but large arrays increase memory pressure and
# transaction duration. 10K is a pragmatic default for the expected
# feature batch sizes.
_CHUNK_SIZE = 10_000


# =============================================================================
# Internal per-chunk helpers
# =============================================================================


async def _mint_chunk(
    conn: asyncpg.Connection,
    entries: list[FeatureHashEntry],
) -> tuple[dict[UUID, int], int, int]:
    """Upsert features for one chunk; returns (mapping, minted, reused)."""
    hashes = [e.sequence_hash for e in entries]

    existing = await conn.fetch(
        "SELECT feature_idx, sequence_hash FROM qiita.feature"
        " WHERE sequence_hash = ANY($1::uuid[])",
        hashes,
    )
    existing_map = {row["sequence_hash"]: row["feature_idx"] for row in existing}

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

        # Concurrent inserts: ON CONFLICT DO NOTHING means some rows may
        # not RETURN. Re-look-up missing ones and count them as reused.
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

    # Batch upsert genomes — DO UPDATE to guarantee RETURNING for every row.
    genome_rows = await conn.fetch(
        "INSERT INTO qiita.genome (source, source_id)"
        " SELECT unnest($1::text[]), unnest($2::text[])"
        " ON CONFLICT (source, source_id) DO UPDATE SET source = EXCLUDED.source"
        " RETURNING genome_idx, source, source_id",
        sources,
        source_ids,
    )
    genome_map = {(row["source"], row["source_id"]): row["genome_idx"] for row in genome_rows}

    feat_idxs = [mapping[e.sequence_hash] for e in entries]
    genome_idxs = [genome_map[(e.genome_source, e.genome_source_id)] for e in entries]
    await conn.execute(
        "INSERT INTO qiita.feature_genome (feature_idx, genome_idx)"
        " SELECT unnest($1::bigint[]), unnest($2::bigint[])"
        " ON CONFLICT DO NOTHING",
        feat_idxs,
        genome_idxs,
    )


async def _write_membership_rows(
    conn: asyncpg.Connection,
    reference_idx: int,
    feature_idxs: list[int],
) -> int:
    """INSERT ... RETURNING for one chunk; returns count of newly-linked rows.

    Wraps asyncpg.ForeignKeyViolationError into a ValueError so the public
    `write_membership` and the route handler both surface a structured
    error instead of letting the asyncpg exception leak to callers.
    """
    if not feature_idxs:
        return 0
    try:
        rows = await conn.fetch(
            "INSERT INTO qiita.reference_membership (reference_idx, feature_idx)"
            " SELECT $1, unnest($2::bigint[])"
            " ON CONFLICT DO NOTHING"
            " RETURNING feature_idx",
            reference_idx,
            feature_idxs,
        )
    except asyncpg.ForeignKeyViolationError as exc:
        raise ValueError("One or more feature_idx values do not exist in qiita.feature") from exc
    return len(rows)


def _do_action_register(data_plane_url: str, token: bytes) -> list:
    """Synchronous gRPC call to data plane — runs in thread executor."""
    with _flight.FlightClient(data_plane_url) as client:
        action = _flight.Action("register_files", token)
        return list(client.do_action(action))


# =============================================================================
# Public primitives — exposed through LIBRARY by name
# =============================================================================


async def mint_features(
    pool: asyncpg.Pool,
    entries: list[FeatureHashEntry],
) -> tuple[dict[UUID, int], int, int]:
    """Mint feature_idx values for a sequence-hash batch; reference-agnostic.

    Returns (mapping, minted, reused) where `mapping` covers every input
    hash, `minted` counts novel rows inserted in qiita.feature, and
    `reused` counts pre-existing rows. Genome associations are written
    when entries carry genome_source / genome_source_id.

    Chunks the batch into transactions of `_CHUNK_SIZE` so a partial
    failure on a large batch doesn't lose progress on prior chunks; the
    feature INSERT uses ON CONFLICT DO NOTHING and feature_genome too,
    so resubmitting the full batch after a chunk failure converges.
    """
    full_mapping: dict[UUID, int] = {}
    total_minted = 0
    total_reused = 0
    for i in range(0, len(entries), _CHUNK_SIZE):
        chunk = entries[i : i + _CHUNK_SIZE]
        async with pool.acquire() as conn, conn.transaction():
            chunk_mapping, minted, reused = await _mint_chunk(conn, chunk)
            genome_entries = [e for e in chunk if e.genome_source is not None]
            if genome_entries:
                await _write_genome_associations(conn, genome_entries, chunk_mapping)
        full_mapping.update(chunk_mapping)
        total_minted += minted
        total_reused += reused
    return full_mapping, total_minted, total_reused


async def write_membership(
    pool: asyncpg.Pool,
    reference_idx: int,
    feature_idxs: list[int],
) -> tuple[int, int]:
    """Link already-minted feature_idx values to a reference.

    Returns (linked, already_linked). Idempotent — repeat calls converge.
    Status-state guards live in the calling route handler / runner; this
    primitive does not check the reference's status.

    Raises ValueError if any feature_idx does not exist in qiita.feature
    (FK violation surfaced as a structured error).
    """
    async with pool.acquire() as conn:
        linked = await _write_membership_rows(conn, reference_idx, feature_idxs)
    return linked, len(feature_idxs) - linked


async def register_files(
    *,
    staging_dir: str,
    files: dict[str, str],
    hmac_secret: bytes,
    data_plane_url: str,
) -> list[str]:
    """Register Parquet files in DuckLake via the data plane's DoAction.

    Signs the HMAC action token, calls Flight in a thread (FlightClient
    is synchronous), and returns the list of registered permanent paths.
    Status-state guards live in the caller; reference-add typically
    requires status='loading' before invoking.

    Raises pyarrow.flight.FlightError on transport / data-plane failure.
    """
    token = sign_action(
        action="register_files",
        payload={"staging_dir": staging_dir, "files": files},
        secret=hmac_secret,
    )
    results = await asyncio.get_event_loop().run_in_executor(
        None, _do_action_register, data_plane_url, token
    )
    if not results:
        return []
    result_body = json.loads(results[0].body.to_pybytes())
    return result_body.get("registered", [])


# =============================================================================
# Name → callable lookup for the workflow runner
# =============================================================================
#
# Heterogeneous signatures by design — each primitive takes what it
# needs. The runner unpacks workflow-step inputs into the right kwargs
# at dispatch time. Adding a primitive here is a contract change visible
# to every workflow YAML; do it deliberately.

LIBRARY: dict[str, Callable[..., Awaitable[Any]]] = {
    LibraryPrimitive.MINT_FEATURES: mint_features,
    LibraryPrimitive.WRITE_MEMBERSHIP: write_membership,
    LibraryPrimitive.REGISTER_FILES: register_files,
}
