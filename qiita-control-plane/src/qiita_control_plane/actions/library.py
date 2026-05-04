"""Named primitives composed by workflows.

A workflow YAML entry like

    - action: mint-features
      inputs: [manifest]
      outputs: [feature_map]

resolves through the LIBRARY dict at the bottom of this module. The
runner looks up the name and invokes the callable with paths into the
shared workspace; the runner and the dispatch handler agree on a
Parquet-everywhere on-disk format so DuckDB reads stream chunked and
Python never holds a full mapping in memory.

Library callables take the asyncpg pool plus the input paths they need;
status-state guards are the caller's responsibility because routes
return HTTPException on bad state and workflow runners want to handle
it differently.
"""

import asyncio
import json
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg
import duckdb
import pyarrow.flight as _flight
from qiita_common.api_paths import LibraryPrimitive
from qiita_common.models import FeatureHashEntry
from qiita_common.parquet import validate_parquet_path

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
    feat_idxs: list[int],
    sources: list[str],
    source_ids: list[str],
) -> None:
    """Batch upsert genomes and write feature_genome junction rows.

    All three lists are positionally aligned: row i links
    feat_idxs[i] to (sources[i], source_ids[i]). DO UPDATE on the genome
    upsert guarantees RETURNING fires for every row even when the genome
    already exists.
    """
    if not feat_idxs:
        return

    genome_rows = await conn.fetch(
        "INSERT INTO qiita.genome (source, source_id)"
        " SELECT unnest($1::text[]), unnest($2::text[])"
        " ON CONFLICT (source, source_id) DO UPDATE SET source = EXCLUDED.source"
        " RETURNING genome_idx, source, source_id",
        sources,
        source_ids,
    )
    genome_map = {(row["source"], row["source_id"]): row["genome_idx"] for row in genome_rows}

    genome_idxs = [genome_map[(s, sid)] for s, sid in zip(sources, source_ids, strict=True)]
    await conn.execute(
        "INSERT INTO qiita.feature_genome (feature_idx, genome_idx)"
        " SELECT unnest($1::bigint[]), unnest($2::bigint[])"
        " ON CONFLICT DO NOTHING",
        feat_idxs,
        genome_idxs,
    )


async def _associate_genomes(
    pool: asyncpg.Pool,
    manifest_path: Path,
    genome_map_path: Path,
    feature_mapping: dict[UUID, int],
) -> None:
    """Write qiita.feature_genome rows for the entries in `genome_map_path`.

    DuckDB JOINs manifest (read_id → sequence_hash) against genome_map
    (read_id → genome_source, genome_source_id) on read_id. Rows whose
    read_id isn't in the manifest are dropped by the INNER JOIN — the
    genome map may legitimately cover only a subset of FASTA reads.
    """
    with duckdb.connect(":memory:") as duck:
        rows = duck.execute(
            "SELECT m.sequence_hash, g.genome_source, g.genome_source_id"
            " FROM read_parquet(?) AS m"
            " JOIN read_parquet(?) AS g USING (read_id)"
            " ORDER BY m.sequence_hash",
            [str(manifest_path), str(genome_map_path)],
        ).fetchall()

    for i in range(0, len(rows), _CHUNK_SIZE):
        chunk = rows[i : i + _CHUNK_SIZE]
        feat_idxs = [feature_mapping[h] for h, _, _ in chunk]
        sources = [s for _, s, _ in chunk]
        source_ids = [sid for _, _, sid in chunk]
        async with pool.acquire() as conn, conn.transaction():
            await _write_genome_associations(conn, feat_idxs, sources, source_ids)


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
    manifest_path: Path,
    output_dir: Path,
    genome_map_path: Path | None = None,
) -> tuple[Path, int, int]:
    """Mint feature_idx values for sequence hashes in a manifest Parquet file.

    `manifest_path` points to a Parquet file with a `sequence_hash` column
    (UUIDs). The function reads it via DuckDB, upserts qiita.feature in
    chunks of `_CHUNK_SIZE`, and writes a `feature_map.parquet` into
    `output_dir` with columns (sequence_hash UUID, feature_idx BIGINT).

    Returns (feature_map_path, minted, reused). `minted` counts novel
    rows inserted; `reused` counts pre-existing rows. Idempotent:
    qiita.feature uses ON CONFLICT DO NOTHING, so resubmitting after a
    partial-batch failure converges.

    Reference-agnostic — the mint operation does not touch any reference
    table.

    If `genome_map_path` is supplied, qiita.feature_genome rows are also
    written for each entry in that Parquet. Schema:
    `(read_id TEXT, genome_source TEXT, genome_source_id TEXT)`. The
    read_id key is JOINed against the manifest's read_id; rows whose
    read_id isn't in the manifest are dropped (a genome map may cover
    only a subset of the FASTA's reads, e.g. amplicon mixed with full
    genomes).
    """
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    if genome_map_path is not None and not genome_map_path.exists():
        raise FileNotFoundError(f"Genome map not found: {genome_map_path}")
    output_dir.mkdir(parents=True, exist_ok=True)
    feature_map_path = output_dir / "feature_map.parquet"

    # Read all sequence_hash values from the manifest into Python. For
    # the reference-add flow this is bounded (~300K UUIDs ≈ 25 MB), and
    # the per-chunk Postgres transactions below are the actual memory
    # bottleneck. Larger workflows can switch to streaming via
    # fetch_arrow_reader once we have one that needs it.
    with duckdb.connect(":memory:") as duck:
        rows = duck.execute(
            "SELECT sequence_hash FROM read_parquet(?) ORDER BY sequence_hash",
            [str(manifest_path)],
        ).fetchall()
    hashes: list[UUID] = [row[0] for row in rows]

    full_mapping: dict[UUID, int] = {}
    total_minted = 0
    total_reused = 0
    for i in range(0, len(hashes), _CHUNK_SIZE):
        chunk_hashes = hashes[i : i + _CHUNK_SIZE]
        chunk_entries = [FeatureHashEntry(sequence_hash=h) for h in chunk_hashes]
        async with pool.acquire() as conn, conn.transaction():
            chunk_mapping, minted, reused = await _mint_chunk(conn, chunk_entries)
        full_mapping.update(chunk_mapping)
        total_minted += minted
        total_reused += reused

    # Write feature_map.parquet via DuckDB. The temp table is the
    # cleanest way to ferry a Python dict into Parquet without going
    # through pyarrow directly.
    pairs = [(str(h), idx) for h, idx in full_mapping.items()]
    with duckdb.connect(":memory:") as duck:
        duck.execute("CREATE TEMP TABLE feature_map (sequence_hash UUID, feature_idx BIGINT)")
        # Empty manifest is a valid degenerate state (FASTA had no sequences);
        # DuckDB's executemany rejects an empty parameter list, so guard
        # before the call. The COPY below still produces a valid empty Parquet.
        if pairs:
            duck.executemany("INSERT INTO feature_map VALUES (?, ?)", pairs)
        out = validate_parquet_path(feature_map_path)
        duck.execute(
            "COPY (SELECT sequence_hash, feature_idx FROM feature_map "
            "      ORDER BY feature_idx) "
            f"TO '{out}' (FORMAT PARQUET, PARQUET_VERSION 'v2', COMPRESSION 'zstd')"
        )

    if genome_map_path is not None:
        await _associate_genomes(pool, manifest_path, genome_map_path, full_mapping)

    return feature_map_path, total_minted, total_reused


async def write_membership(
    pool: asyncpg.Pool,
    reference_idx: int,
    feature_map_path: Path,
) -> tuple[int, int]:
    """Link already-minted feature_idx values from a feature_map Parquet
    file to a reference.

    `feature_map_path` is a Parquet file with a `feature_idx` column
    (typically the output of `mint_features`). Reads it chunked via
    DuckDB and bulk-inserts qiita.reference_membership.

    Returns (linked, already_linked). Idempotent. Raises ValueError if
    any feature_idx is missing from qiita.feature (FK violation surfaced
    as a structured error).
    """
    if not feature_map_path.exists():
        raise FileNotFoundError(f"Feature map not found: {feature_map_path}")

    total_linked = 0
    total_seen = 0
    async with pool.acquire() as conn:
        # Stream feature_idx in batches via DuckDB's Arrow reader so we
        # never materialise the full list in Python.
        with duckdb.connect(":memory:") as duck:
            reader = duck.execute(
                "SELECT feature_idx FROM read_parquet(?)",
                [str(feature_map_path)],
            ).to_arrow_reader(_CHUNK_SIZE)
            for batch in reader:
                feature_idxs = batch.column("feature_idx").to_pylist()
                if not feature_idxs:
                    continue
                async with conn.transaction():
                    chunk_linked = await _write_membership_rows(conn, reference_idx, feature_idxs)
                total_linked += chunk_linked
                total_seen += len(feature_idxs)
    return total_linked, total_seen - total_linked


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
