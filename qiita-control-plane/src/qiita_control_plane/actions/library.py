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
from qiita_common.parquet import PARQUET_OPTS, validate_parquet_path

from ..auth.tickets import sign_action
from ..repositories.block import (
    fetch_block_members,
    finalize_mask_sample,
    has_incomplete_covering_block,
    lock_mask_sample,
    set_block_state,
)

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
    feature_map_path: Path,
) -> None:
    """Write qiita.feature_genome rows for the entries in `genome_map_path`.

    DuckDB JOINs the manifest (read_id → sequence_hash) against genome_map
    (read_id → genome_source, genome_source_id) on read_id, and against the
    already-written feature_map (sequence_hash → feature_idx) on
    sequence_hash — so feature_idx is resolved set-side in DuckDB rather than
    from an in-memory Python mapping. Rows whose read_id isn't in the manifest
    are dropped by the INNER JOIN — the genome map may legitimately cover only
    a subset of FASTA reads. Streamed in `_CHUNK_SIZE` batches so a
    genome-scale map never materialises in Python.
    """
    with duckdb.connect(":memory:") as duck:
        reader = duck.execute(
            "SELECT fm.feature_idx, g.genome_source, g.genome_source_id"
            " FROM read_parquet(?) AS m"
            " JOIN read_parquet(?) AS g USING (read_id)"
            " JOIN read_parquet(?) AS fm USING (sequence_hash)",
            [str(manifest_path), str(genome_map_path), str(feature_map_path)],
        ).to_arrow_reader(_CHUNK_SIZE)
        for batch in reader:
            feat_idxs = batch.column("feature_idx").to_pylist()
            if not feat_idxs:
                continue
            sources = batch.column("genome_source").to_pylist()
            source_ids = batch.column("genome_source_id").to_pylist()
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


def _do_action_delete_reference(data_plane_url: str, token: bytes) -> list:
    """Synchronous gRPC call to data plane — runs in thread executor."""
    with _flight.FlightClient(data_plane_url) as client:
        action = _flight.Action("delete_reference", token)
        return list(client.do_action(action))


def _do_action_delete_mask(data_plane_url: str, token: bytes) -> list:
    """Synchronous gRPC call to data plane — runs in thread executor."""
    with _flight.FlightClient(data_plane_url) as client:
        action = _flight.Action("delete_mask", token)
        return list(client.do_action(action))


def _do_action_delete_pool_reads(data_plane_url: str, token: bytes) -> list:
    """Synchronous gRPC call to data plane — runs in thread executor."""
    with _flight.FlightClient(data_plane_url) as client:
        action = _flight.Action("delete_pool_reads", token)
        return list(client.do_action(action))


def _do_action_mask_metrics(data_plane_url: str, token: bytes) -> list:
    """Synchronous gRPC call to data plane — runs in thread executor."""
    with _flight.FlightClient(data_plane_url) as client:
        action = _flight.Action("mask_metrics", token)
        return list(client.do_action(action))


def _do_action_delete_read_mask_block(data_plane_url: str, token: bytes) -> list:
    """Synchronous gRPC call to data plane — runs in thread executor."""
    with _flight.FlightClient(data_plane_url) as client:
        action = _flight.Action("delete_read_mask_block", token)
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
    (UUIDs). The function streams it via DuckDB in `_CHUNK_SIZE` batches,
    upserts qiita.feature per batch, and writes a `feature_map.parquet` into
    `output_dir` with columns (sequence_hash UUID, feature_idx BIGINT).

    Streaming is deliberate: this primitive runs in-process on the control
    plane's single event loop, so it must never materialise the whole hash
    set (a genome-scale reference is tens of millions of rows). Each batch is
    bounded, the per-batch upsert `await`s (yielding the loop), and the one
    large blocking step — the final Parquet write — is offloaded to a thread.
    Mirrors the streaming contract `write_membership` already follows.

    Returns (feature_map_path, minted, reused). `minted` counts novel
    rows inserted; `reused` counts pre-existing rows. Idempotent:
    qiita.feature uses ON CONFLICT DO NOTHING, so resubmitting after a
    partial-batch failure converges, and the feature_map is de-duplicated to
    one row per sequence_hash (identical sequences validly repeat in a
    manifest under distinct read_ids).

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

    total_minted = 0
    total_reused = 0
    # Two connections: `read_conn` streams the manifest while `write_conn`
    # holds the feature_map temp table and the final COPY. Separate so the
    # open manifest reader and the per-batch INSERTs don't contend on one
    # connection's single in-flight query. `temp_directory` lets write_conn
    # spill the temp table to the (ephemeral) workspace under memory pressure
    # rather than growing unbounded in the CP's RAM.
    read_conn = duckdb.connect(":memory:")
    write_conn = duckdb.connect(":memory:")
    try:
        # ROW_GROUP_SIZE_BYTES in PARQUET_OPTS requires
        # preserve_insertion_order=false (DuckDB errors at bind time
        # otherwise). The COPY's explicit ORDER BY still clusters each row
        # group tightly by feature_idx — which is what row-group pushdown reads.
        write_conn.execute("SET preserve_insertion_order=false")
        write_conn.execute(f"SET temp_directory='{validate_parquet_path(output_dir)}'")
        write_conn.execute("CREATE TEMP TABLE feature_map (sequence_hash UUID, feature_idx BIGINT)")

        # No ORDER BY on the read: minting is order-independent (per-hash
        # upsert), and an ORDER BY would force a full blocking sort before the
        # first batch, defeating the streaming. The output is separately
        # ordered by feature_idx at COPY time.
        reader = read_conn.execute(
            "SELECT sequence_hash FROM read_parquet(?)",
            [str(manifest_path)],
        ).to_arrow_reader(_CHUNK_SIZE)
        for batch in reader:
            raw_hashes = batch.column("sequence_hash").to_pylist()
            if not raw_hashes:
                continue
            entries = [FeatureHashEntry(sequence_hash=h) for h in raw_hashes]
            async with pool.acquire() as conn, conn.transaction():
                chunk_mapping, minted, reused = await _mint_chunk(conn, entries)
            total_minted += minted
            total_reused += reused
            # `chunk_mapping` is keyed by unique sequence_hash, so within-batch
            # duplicates collapse here; cross-batch duplicates are collapsed by
            # SELECT DISTINCT at COPY time below.
            if chunk_mapping:
                write_conn.executemany(
                    "INSERT INTO feature_map VALUES (?, ?)",
                    [(str(h), idx) for h, idx in chunk_mapping.items()],
                )

        out = validate_parquet_path(feature_map_path)
        # The one large blocking step — DISTINCT + sort + Parquet encode — runs
        # off the event loop so it never starves the API the CP also serves. An
        # empty manifest yields zero rows and a valid empty Parquet here.
        await asyncio.to_thread(
            write_conn.execute,
            "COPY (SELECT DISTINCT sequence_hash, feature_idx FROM feature_map "
            f"      ORDER BY feature_idx) TO '{out}' ({PARQUET_OPTS})",
        )
    finally:
        read_conn.close()
        write_conn.close()

    if genome_map_path is not None:
        await _associate_genomes(pool, manifest_path, genome_map_path, feature_map_path)

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


async def register_index(
    pool: asyncpg.Pool,
    reference_idx: int,
    index_type: str,
    fs_path: str,
    params: dict[str, Any],
) -> int:
    """Record a built search index (e.g. a rype `.ryxdi`) for a reference in
    qiita.reference_index. `fs_path` is the on-disk location; `params` is the
    build configuration (k, w, bucket_name, ...) stored as JSONB — the
    authoritative manifest lives inside the index artifact itself.

    Returns the reference_index_idx. Idempotent on
    (reference_idx, index_type, fs_path): a workflow retried from the start
    re-runs this primitive, and re-inserting would otherwise duplicate the
    row (the table has no UNIQUE on that triple, by design, so growth can
    append generations). The conditional INSERT + fallback SELECT returns the
    existing row's id instead. This guards the sequential re-run path; truly
    concurrent registrations of the same reference are not expected (one
    workflow runs per reference at a time).
    """
    row = await pool.fetchrow(
        "INSERT INTO qiita.reference_index (reference_idx, index_type, fs_path, params)"
        " SELECT $1, $2, $3, $4::jsonb"
        " WHERE NOT EXISTS ("
        "   SELECT 1 FROM qiita.reference_index"
        "   WHERE reference_idx = $1 AND index_type = $2 AND fs_path = $3)"
        " RETURNING reference_index_idx",
        reference_idx,
        index_type,
        fs_path,
        json.dumps(params),
    )
    if row is not None:
        return row["reference_index_idx"]
    return await pool.fetchval(
        "SELECT reference_index_idx FROM qiita.reference_index"
        " WHERE reference_idx = $1 AND index_type = $2 AND fs_path = $3",
        reference_idx,
        index_type,
        fs_path,
    )


def _read_mask_counts(read_mask_path: Path) -> tuple[int, int, int]:
    """Derive the three per-stage both-mates (`*_r1r2`) read counts from a
    read_mask Parquet, returning (raw, biological, quality_filtered).

    The mask has one row per read (a single-end read or a paired-end pair), so a
    bare COUNT(*) would silently HALVE paired-end totals — the persisted columns
    are `_r1r2` (both mates; a PE pair counts as 2). The mask records its layout
    per row: `right_trim2` is non-NULL (0 or more) for paired-end and NULL for
    single-end, so `COUNT(right_trim2)` is the R2 count and
    `COUNT(*) + COUNT(right_trim2)` is the both-mates total — correct for SE
    (no R2), PE, and a mix, with no SE/PE branching.

    Buckets by `reason` (ReadMaskReason): raw = every row, biological = rows that
    didn't fail QC (`reason NOT LIKE 'qc_%'` — i.e. `pass` or a `host_*` hit),
    quality_filtered = the `pass` rows the read_masked view surfaces. raw >=
    biological >= quality_filtered holds by construction (host_* only overrides
    pass), satisfying the sequenced_sample monotonic CHECK."""
    path_sql = validate_parquet_path(read_mask_path)
    with duckdb.connect(":memory:") as duck:
        raw, biological, quality_filtered = duck.execute(
            "SELECT "
            "  count(*) + count(right_trim2), "
            "  count(*) FILTER (WHERE reason NOT LIKE 'qc_%') "
            "    + count(right_trim2) FILTER (WHERE reason NOT LIKE 'qc_%'), "
            "  count(*) FILTER (WHERE reason = 'pass') "
            "    + count(right_trim2) FILTER (WHERE reason = 'pass') "
            f"FROM read_parquet('{path_sql}')"
        ).fetchone()
    return raw, biological, quality_filtered


async def _update_sequenced_sample_read_counts(
    conn: asyncpg.Connection | asyncpg.Pool,
    prep_sample_idx: int,
    *,
    raw: int,
    biological: int,
    quality_filtered: int,
) -> int | None:
    """Write the three per-stage both-mates (`*_r1r2`) read counts onto the 1:1
    sequenced_sample row for `prep_sample_idx`; return its idx, or None if no such
    row exists (the caller raises its own ordering-specific error).

    Shared by `persist_read_metrics` (per-sample path, counts from a local
    parquet) and `_finalize_sample_metrics` (block-compute path, counts from the
    DuckLake `mask_metrics` aggregate). Idempotent — a retried workflow overwrites
    with the same counts. Accepts a pool or a connection so it composes standalone
    or inside a transaction. The DB CHECK (quality_filtered <= biological <= raw)
    enforces stage monotonicity at write time."""
    return await conn.fetchval(
        "UPDATE qiita.sequenced_sample"
        " SET raw_read_count_r1r2 = $2,"
        "     biological_read_count_r1r2 = $3,"
        "     quality_filtered_read_count_r1r2 = $4"
        " WHERE prep_sample_idx = $1"
        " RETURNING idx",
        prep_sample_idx,
        raw,
        biological,
        quality_filtered,
    )


async def persist_read_metrics(
    pool: asyncpg.Pool,
    prep_sample_idx: int,
    read_mask_path: Path,
) -> int:
    """Persist the three per-stage read counts onto the 1:1
    sequenced_sample row for `prep_sample_idx`, deriving them from the
    `read_mask` Parquet, and return its idx.

    The counts are the both-mates (`*_r1r2`) totals computed from the mask's
    per-read `reason` (see `_read_mask_counts`): raw = all reads, biological =
    reads that passed QC (pass + host hits), quality_filtered = pass reads. The
    DB CHECK enforces quality_filtered <= biological <= raw, which holds by
    construction (host_* only overrides pass), so a garbled mask fails loudly at
    write time rather than persisting silently.

    Fail-fast (loud) when no sequenced_sample row exists for the prep_sample: a
    sequenced prep_sample reaches read-metric persistence only after pooling
    created its 1:1 sequenced_sample, so a miss is a real ordering bug, not a
    benign skip. The UPDATE is idempotent — a workflow retried from the start
    re-runs this primitive and overwrites with the same counts (the
    set_updated_at trigger bumps updated_at / the ETag, which is correct: the
    row did change)."""
    if not read_mask_path.exists():
        raise FileNotFoundError(f"read_mask parquet not found: {read_mask_path}")
    raw_read_count_r1r2, biological_read_count_r1r2, quality_filtered_read_count_r1r2 = (
        _read_mask_counts(read_mask_path)
    )
    ss_idx = await _update_sequenced_sample_read_counts(
        pool,
        prep_sample_idx,
        raw=raw_read_count_r1r2,
        biological=biological_read_count_r1r2,
        quality_filtered=quality_filtered_read_count_r1r2,
    )
    if ss_idx is None:
        raise RuntimeError(
            f"no sequenced_sample row for prep_sample_idx={prep_sample_idx}; "
            "read-metric persistence requires the sample to be pooled "
            "(its 1:1 sequenced_sample created) before fastq-to-parquet runs"
        )
    return ss_idx


async def persist_qc_report(
    pool: asyncpg.Pool,
    prep_sample_idx: int,
    raw_qc_report: dict[str, Any],
    filtered_qc_report: dict[str, Any],
) -> int:
    """Persist the two fastqc-equivalent QC reports onto the 1:1 sequenced_sample
    row for `prep_sample_idx` and return its idx.

    The reports are the qc_report.json documents the runner read from the
    qc_report_raw / qc_report_filtered step sidecars; they are stored verbatim as
    JSONB (raw -> raw_qc_report, filtered -> filtered_qc_report). The pool-level
    merged report aggregates them on read.

    Fail-fast (loud) when no sequenced_sample row exists for the prep_sample —
    same ordering invariant as persist_read_metrics: a sequenced prep_sample
    reaches QC-report persistence only after pooling created its 1:1
    sequenced_sample, so a miss is a real ordering bug, not a benign skip. The
    UPDATE is idempotent — a workflow retried from the start overwrites with the
    same reports."""
    ss_idx = await pool.fetchval(
        "UPDATE qiita.sequenced_sample"
        " SET raw_qc_report = $2::jsonb,"
        "     filtered_qc_report = $3::jsonb"
        " WHERE prep_sample_idx = $1"
        " RETURNING idx",
        prep_sample_idx,
        json.dumps(raw_qc_report),
        json.dumps(filtered_qc_report),
    )
    if ss_idx is None:
        raise RuntimeError(
            f"no sequenced_sample row for prep_sample_idx={prep_sample_idx}; "
            "QC-report persistence requires the sample to be pooled "
            "(its 1:1 sequenced_sample created) before fastq-to-parquet runs"
        )
    return ss_idx


async def register_files(
    *,
    staging_dir: str,
    files: dict[str, str],
    work_ticket_idx: int,
    hmac_secret: bytes,
    data_plane_url: str,
) -> list[str]:
    """Register Parquet files in DuckLake via the data plane's DoAction.

    Signs the HMAC action token, calls Flight in a thread (FlightClient
    is synchronous), and returns the list of registered permanent paths.
    Status-state guards live in the caller; reference-add typically
    requires status='loading' before invoking.

    `work_ticket_idx` rides in the signed payload so the data plane can mint a
    unique, ticket-traceable lake filename per file — the producer reuses fixed
    basenames across loads, so the bare name would collide with an
    already-registered file in the same per-table dir.

    Raises pyarrow.flight.FlightError on transport / data-plane failure.
    """
    token = sign_action(
        action="register_files",
        payload={
            "staging_dir": staging_dir,
            "files": files,
            "work_ticket_idx": work_ticket_idx,
        },
        secret=hmac_secret,
    )
    results = await asyncio.get_event_loop().run_in_executor(
        None, _do_action_register, data_plane_url, token
    )
    if not results:
        return []
    result_body = json.loads(results[0].body.to_pybytes())
    return result_body.get("registered", [])


async def delete_reference_data(
    *,
    reference_idx: int,
    hmac_secret: bytes,
    data_plane_url: str,
) -> dict:
    """Delete a reference's DuckLake rows via the data plane's DoAction.

    Signs a `delete_reference` action token carrying only `reference_idx` and
    returns the data plane's per-table delete counts. The data plane computes
    which features are orphaned (owned by no other reference) from its own
    DuckLake `reference_membership` — shared features keep their sequences.

    Idempotent: a reference whose data never loaded deletes zero rows. Raises
    pyarrow.flight.FlightError on transport / data-plane failure."""
    token = sign_action(
        action="delete_reference",
        payload={"reference_idx": reference_idx},
        secret=hmac_secret,
    )
    results = await asyncio.get_event_loop().run_in_executor(
        None, _do_action_delete_reference, data_plane_url, token
    )
    if not results:
        return {}
    return json.loads(results[0].body.to_pybytes())


async def delete_pool_reads_data(
    *,
    prep_sample_idxs: list[int],
    hmac_secret: bytes,
    data_plane_url: str,
) -> dict:
    """Delete a sequenced_pool's `read` / `read_mask` DuckLake rows via the data
    plane's DoAction.

    Signs a `delete_pool_reads` action token carrying the pool's prep_sample set
    (its reads/masks are keyed by `prep_sample_idx`; the data plane has no
    run/pool column to expand from) and returns the data plane's per-table delete
    counts. The set is exclusive to the pool — its prep_samples belong to no
    other pool — so the delete cannot touch another pool's reads.

    Idempotent: an empty set short-circuits without a Flight call (returns `{}`);
    a pool whose reads were never written deletes zero rows. Raises
    pyarrow.flight.FlightError on transport / data-plane failure."""
    if not prep_sample_idxs:
        return {}
    token = sign_action(
        action="delete_pool_reads",
        payload={"prep_sample_idxs": prep_sample_idxs},
        secret=hmac_secret,
    )
    results = await asyncio.get_event_loop().run_in_executor(
        None, _do_action_delete_pool_reads, data_plane_url, token
    )
    if not results:
        return {}
    return json.loads(results[0].body.to_pybytes())


async def delete_mask_data(
    *,
    mask_idx: int,
    hmac_secret: bytes,
    data_plane_url: str,
) -> int:
    """Delete a mask's DuckLake read_mask rows via the data plane's DoAction.

    Signs a `delete_mask` action token carrying only `mask_idx` and returns the
    data plane's rows-deleted count. The delete is a logical `DELETE FROM
    read_mask WHERE mask_idx = ?` inside one DuckLake transaction — no parquet is
    reclaimed from disk (mirrors `delete_reference`).

    Idempotent: a mask whose rows never registered (or were already deleted)
    deletes zero rows and still succeeds. Raises pyarrow.flight.FlightError on
    transport / data-plane failure."""
    token = sign_action(
        action="delete_mask",
        payload={"mask_idx": mask_idx},
        secret=hmac_secret,
    )
    results = await asyncio.get_event_loop().run_in_executor(
        None, _do_action_delete_mask, data_plane_url, token
    )
    if not results:
        return 0
    return json.loads(results[0].body.to_pybytes()).get("rows_deleted", 0)


async def mask_metrics_data(
    *,
    mask_idx: int,
    prep_sample_idx: int,
    hmac_secret: bytes,
    data_plane_url: str,
) -> dict[str, int]:
    """Aggregate a sample's per-stage read counts for one mask from the DuckLake
    `read_mask` table via the data plane's `mask_metrics` DoAction.

    Returns `{raw, biological, quality_filtered, row_count}` — the both-mates
    (`*_r1r2`) totals `sequenced_sample` stores plus `row_count` (one per
    read/pair) the reconcile count-assertion checks against `sequence_range`.
    Unlike the per-sample path's local-parquet `_read_mask_counts`, this reads the
    PERSISTED table because a block-masked sample's rows are written by several
    blocks. Raises pyarrow.flight.FlightError on transport / data-plane failure,
    RuntimeError on an empty result."""
    token = sign_action(
        action="mask_metrics",
        payload={"mask_idx": mask_idx, "prep_sample_idx": prep_sample_idx},
        secret=hmac_secret,
    )
    results = await asyncio.get_event_loop().run_in_executor(
        None, _do_action_mask_metrics, data_plane_url, token
    )
    if not results:
        raise RuntimeError("mask_metrics DoAction returned no result")
    return json.loads(results[0].body.to_pybytes())


async def delete_read_mask_block_data(
    *,
    mask_idx: int,
    members: list[dict[str, int]],
    hmac_secret: bytes,
    data_plane_url: str,
) -> int:
    """Delete one block's exact `read_mask` footprint via the `delete_read_mask_block`
    DoAction, returning the rows-deleted count.

    `members` is the block's cover-map as `{prep_sample_idx, sequence_idx_start,
    sequence_idx_stop}` dicts (from `block_member`). The data plane deletes the
    rows for `mask_idx` whose `(prep_sample_idx, sequence_idx)` fall in those
    sub-ranges — exact by construction (per-member OR), so a split sample's
    sibling-block rows survive. This is the idempotent-block-replace step run
    immediately before register-files.

    Idempotent: a fresh block (no rows yet) deletes 0 and still succeeds; an empty
    `members` list short-circuits without a Flight call (an empty block is a
    control-plane bug the runner never dispatches, guarded here too). Raises
    pyarrow.flight.FlightError on transport / data-plane failure."""
    if not members:
        return 0
    token = sign_action(
        action="delete_read_mask_block",
        payload={"mask_idx": mask_idx, "members": members},
        secret=hmac_secret,
    )
    results = await asyncio.get_event_loop().run_in_executor(
        None, _do_action_delete_read_mask_block, data_plane_url, token
    )
    if not results:
        return 0
    return json.loads(results[0].body.to_pybytes()).get("rows_deleted", 0)


async def _finalize_sample_metrics(
    conn: asyncpg.Connection,
    *,
    prep_sample_idx: int,
    mask_idx: int,
    hmac_secret: bytes,
    data_plane_url: str,
) -> None:
    """Roll a finalized sample's per-stage read counts onto its sequenced_sample,
    with the fail-loud count assertion. The block-compute analog of
    `persist_read_metrics`, but sourced from the DuckLake `mask_metrics` aggregate
    (across all the sample's blocks) rather than a single local parquet.

    Count assertion: the total `read_mask` rows for `(prep_sample, mask)`
    must equal the sample's `sequence_range` count (`stop - start + 1`, one per
    read/pair). This runs only once the finalize gate has confirmed every covering
    block completed, so a mismatch is a real cover-map / masking defect (the
    blocks did not fully tile the sample) — raise, don't persist a wrong count.
    The UPDATE mirrors persist_read_metrics; the DB CHECK
    (quality_filtered <= biological <= raw) holds by construction."""
    counts = await mask_metrics_data(
        mask_idx=mask_idx,
        prep_sample_idx=prep_sample_idx,
        hmac_secret=hmac_secret,
        data_plane_url=data_plane_url,
    )
    expected = await conn.fetchval(
        "SELECT sequence_idx_stop - sequence_idx_start + 1"
        "  FROM qiita.sequence_range WHERE prep_sample_idx = $1",
        prep_sample_idx,
    )
    if expected is None:
        raise RuntimeError(
            f"no sequence_range for prep_sample_idx={prep_sample_idx}; a sequenced "
            "sample reconciled from a block must have a minted read range"
        )
    if counts["row_count"] != expected:
        raise RuntimeError(
            f"read_mask row count {counts['row_count']} != sequence_range count "
            f"{expected} for (prep_sample={prep_sample_idx}, mask={mask_idx}); the "
            "block cover-map does not fully tile the sample's reads (a planning or "
            "masking defect) — refusing to finalize a partially-masked sample"
        )
    ss_idx = await _update_sequenced_sample_read_counts(
        conn,
        prep_sample_idx,
        raw=counts["raw"],
        biological=counts["biological"],
        quality_filtered=counts["quality_filtered"],
    )
    if ss_idx is None:
        raise RuntimeError(
            f"no sequenced_sample row for prep_sample_idx={prep_sample_idx}; "
            "block reconcile requires the sample to be pooled (its 1:1 "
            "sequenced_sample created) before masking"
        )


async def delete_read_mask_block(
    pool: asyncpg.Pool,
    *,
    block_idx: int,
    mask_idx: int,
    hmac_secret: bytes,
    data_plane_url: str,
) -> dict[str, Any]:
    """Idempotent block replace: delete this block's exact `read_mask` footprint
    before register-files re-writes it, so a block re-run never double-counts.

    Reads the block's cover-map (`block_member`) and asks the data plane to delete
    the `read_mask` rows for `mask_idx` whose `(prep_sample_idx, sequence_idx)`
    fall in the members' sub-ranges. The delete is exact by construction (per-member
    OR residual), so a sample split across several blocks keeps its sibling blocks'
    rows — only THIS block's footprint goes.

    On a fresh block this deletes 0 rows (nothing registered yet); on a re-run
    (retry, or an operator-resubmitted block covering the same footprint) it clears
    the prior rows so the subsequent register-files leaves exactly one copy and the
    reconcile count-assertion holds. Read-only on Postgres — the delete lands in
    DuckLake. Returns the rows-deleted count for the workflow log."""
    members = [
        {
            "prep_sample_idx": prep_sample_idx,
            "sequence_idx_start": min_seq,
            "sequence_idx_stop": max_seq,
        }
        for prep_sample_idx, min_seq, max_seq in await fetch_block_members(pool, block_idx)
    ]
    if not members:
        raise RuntimeError(
            f"block {block_idx} has no block_member rows; a block must carry its "
            "cover-map (materialized at plan time) before any step runs"
        )
    rows_deleted = await delete_read_mask_block_data(
        mask_idx=mask_idx,
        members=members,
        hmac_secret=hmac_secret,
        data_plane_url=data_plane_url,
    )
    return {"block_idx": block_idx, "rows_deleted": rows_deleted}


async def reconcile_block(
    pool: asyncpg.Pool,
    *,
    block_idx: int,
    mask_idx: int,
    hmac_secret: bytes,
    data_plane_url: str,
) -> dict[str, Any]:
    """Terminal step of the bulk-block read-mask workflow: mark this block done,
    then finalize each sample it covers whose LAST covering block just completed.

    In one transaction (the whole reconcile is atomic):

    1. Flip this block to 'completed' — its `read_mask` rows are registered
       (register-files ran before this step).
    2. For each covered sample, in a stable prep_sample_idx order (deadlock-free):
       take the `mask_sample` gate row's `FOR UPDATE` lock (serializes concurrent
       block finalizers of the same sample), skip if already 'completed'
       (idempotent / lost the race), skip if any covering block is not yet
       'completed' (a sibling still owes reads), else roll the per-stage counts
       onto `sequenced_sample` (with the count assertion) and flip the gate to
       'completed'.

    The lock is held across the metrics DoAction so the check-and-flip is atomic;
    a block is a long SLURM job, so this per-sample lock hold is rare and cheap.
    Idempotent: a re-run flips nothing new (its block is already completed and its
    samples already finalized). Returns a summary of the samples this block
    finalized."""
    finalized: list[int] = []
    async with pool.acquire() as conn, conn.transaction():
        # This block's work is done; count it as completed BEFORE the per-sample
        # gate check below (same txn — the UPDATE is visible to our own SELECTs).
        # Guarded so the bool is meaningful, but we proceed regardless: a re-run
        # where it is already completed still (idempotently) finalizes its samples.
        await set_block_state(
            conn,
            block_idx=block_idx,
            new_state="completed",
            expected_states=["pending", "processing", "failed"],
        )
        for prep_sample_idx, _min_seq, _max_seq in await fetch_block_members(conn, block_idx):
            state = await lock_mask_sample(conn, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx)
            if state is None:
                raise RuntimeError(
                    f"mask_sample gate row missing for (mask={mask_idx}, "
                    f"prep_sample={prep_sample_idx}); it must be materialized PENDING "
                    "at plan time before any block runs"
                )
            if state == "completed":
                # Already finalized (idempotent re-run, or a concurrent block
                # finalizer won this sample's race) — nothing to do.
                continue
            if await has_incomplete_covering_block(
                conn, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx
            ):
                # A sibling block still owes this sample reads — do not finalize a
                # partially-masked sample (the export gate depends on this).
                continue
            await _finalize_sample_metrics(
                conn,
                prep_sample_idx=prep_sample_idx,
                mask_idx=mask_idx,
                hmac_secret=hmac_secret,
                data_plane_url=data_plane_url,
            )
            await finalize_mask_sample(conn, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx)
            finalized.append(prep_sample_idx)
    return {"block_idx": block_idx, "finalized_samples": finalized}


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
    LibraryPrimitive.REGISTER_INDEX: register_index,
    LibraryPrimitive.PERSIST_READ_METRICS: persist_read_metrics,
    LibraryPrimitive.PERSIST_QC_REPORT: persist_qc_report,
    LibraryPrimitive.DELETE_READ_MASK_BLOCK: delete_read_mask_block,
    LibraryPrimitive.RECONCILE_BLOCK: reconcile_block,
}
