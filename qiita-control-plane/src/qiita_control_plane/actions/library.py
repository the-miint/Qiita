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
import itertools
import json
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg
import duckdb
import pyarrow as pa
import pyarrow.flight as _flight
import pyarrow.parquet as pq
from qiita_common.api_paths import LibraryPrimitive
from qiita_common.models import (
    INDEX_TYPE_RYPE_ROUTER,
    FeatureHashEntry,
    GenomeSource,
    ReferenceStatus,
)
from qiita_common.parquet import PARQUET_OPTS, validate_parquet_path

from ..auth.tickets import sign_action, sign_ticket
from ..repositories.assembly import insert_assembly_membership_rows
from ..repositories.block import (
    fetch_block_members,
    finalize_alignment_sample,
    finalize_mask_sample,
    has_incomplete_covering_alignment_block,
    has_incomplete_covering_block,
    lock_alignment_sample,
    lock_mask_sample,
    set_block_state,
)
from ..repositories.reference_membership import count_reference_shards
from ..shard_planner import _SHARD_COUNT, LineageItem, tile_by_lineage
from .reference import IllegalStatusTransition, transition_reference_status

# Chunk size for batch processing. Array params avoid the Postgres $65535
# scalar parameter limit, but large arrays increase memory pressure and
# transaction duration. 10K is a pragmatic default for the expected
# feature batch sizes.
_CHUNK_SIZE = 10_000

# Deterministic basename `mint_features` writes its feature-map Parquet under.
# Single-sourced because the runner's restart path (`_reconstruct_action_outputs`)
# rebuilds this path WITHOUT re-running the primitive, so the two must not drift.
MINT_FEATURES_OUTPUT_BASENAME = "feature_map.parquet"


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
    prep_sample_idxs: list[int | None],
) -> None:
    """Batch upsert genomes and write feature_genome junction rows.

    All four lists are positionally aligned: row i links feat_idxs[i] to
    (sources[i], source_ids[i]) with originating prep_sample_idxs[i] (NULL for
    external genomes; the qiita-origin sample for source='qiita'). DO UPDATE on
    the genome upsert guarantees RETURNING fires for every row even when the
    genome already exists, and keeps prep_sample_idx current on re-ingest.
    """
    if not feat_idxs:
        return

    # Dedupe to one row per (source, source_id) before the upsert. A genome (a
    # binned MAG or a circular isolate) maps to many features — its contigs — all
    # sharing that genome's source_id, and a single sample's assembly can yield MANY
    # such genomes (each bin its own (source, source_id)). So a given (source,
    # source_id) recurs across the batch once per contig of that genome, and
    # Postgres refuses to let one INSERT ... ON CONFLICT DO UPDATE touch the same
    # conflict target twice ("cannot affect row a second time"). The dict keeps the
    # last prep_sample_idx per key (consistent for valid input — a genome has one
    # origin sample, already vetted by _validate_genome_map).
    genome_prep = {
        (s, sid): prep for s, sid, prep in zip(sources, source_ids, prep_sample_idxs, strict=True)
    }
    uniq_sources = [s for s, _ in genome_prep]
    uniq_source_ids = [sid for _, sid in genome_prep]
    uniq_preps = list(genome_prep.values())

    genome_rows = await conn.fetch(
        "INSERT INTO qiita.genome (source, source_id, prep_sample_idx)"
        " SELECT unnest($1::text[]), unnest($2::text[]), unnest($3::bigint[])"
        " ON CONFLICT (source, source_id)"
        " DO UPDATE SET source = EXCLUDED.source,"
        "               prep_sample_idx = EXCLUDED.prep_sample_idx"
        " RETURNING genome_idx, source, source_id",
        uniq_sources,
        uniq_source_ids,
        uniq_preps,
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


def _validate_genome_map(duck: duckdb.DuckDBPyConnection, genome_map_path: Path) -> bool:
    """Fail-fast validation of the whole genome map before any DB write.

    Returns whether the map carries a `prep_sample_idx` column — external-only
    maps may omit it (treated as all-NULL). Raises ValueError if any
    `genome_source` is outside the GenomeSource vocabulary, or if the
    qiita-origin rule is violated (prep_sample_idx set iff genome_source='qiita').
    One DISTINCT scan, so a genome-scale map is never materialised.
    """
    columns = {
        c[0]
        for c in duck.execute(
            "SELECT * FROM read_parquet(?) LIMIT 0", [str(genome_map_path)]
        ).description
    }
    missing = {"genome_source", "genome_source_id"} - columns
    if missing:
        raise ValueError(f"genome_map is missing required column(s): {sorted(missing)}")
    has_prep = "prep_sample_idx" in columns
    prep_expr = "prep_sample_idx" if has_prep else "CAST(NULL AS BIGINT)"
    combos = duck.execute(
        f"SELECT DISTINCT genome_source, (({prep_expr}) IS NOT NULL) AS has_prep"
        " FROM read_parquet(?)",
        [str(genome_map_path)],
    ).fetchall()

    allowed = {s.value for s in GenomeSource}
    bad_vocab = {src for src, _ in combos if src not in allowed}
    if bad_vocab:
        raise ValueError(
            "genome_map has genome_source value(s) outside the allowed "
            f"vocabulary {sorted(allowed)}: {sorted(str(s) for s in bad_vocab)}"
        )
    # Reached only when every source is valid (so no NULL sources here).
    bad_origin = sorted({src for src, has in combos if (src == GenomeSource.QIITA.value) != has})
    if bad_origin:
        raise ValueError(
            "genome_map violates the qiita-origin rule (prep_sample_idx is set "
            f"iff genome_source='qiita'); offending source(s): {bad_origin}"
        )
    return has_prep


async def _associate_genomes(
    pool: asyncpg.Pool,
    manifest_path: Path,
    genome_map_path: Path,
    feature_map_path: Path,
) -> None:
    """Write qiita.feature_genome (and qiita.genome) rows for `genome_map_path`.

    DuckDB JOINs the manifest (read_id → sequence_hash) against genome_map
    (read_id → genome_source, genome_source_id[, prep_sample_idx]) on read_id,
    and against the already-written feature_map (sequence_hash → feature_idx) on
    sequence_hash — so feature_idx is resolved set-side in DuckDB rather than
    from an in-memory Python mapping. Rows whose read_id isn't in the manifest
    are dropped by the INNER JOIN — the genome map may legitimately cover only
    a subset of FASTA reads. Streamed in `_CHUNK_SIZE` batches so a
    genome-scale map never materialises in Python.

    The whole map is validated up front (`_validate_genome_map`) — vocabulary
    and the qiita-origin rule — so a bad map fails before any DB write.
    """
    with duckdb.connect(":memory:") as duck:
        has_prep = _validate_genome_map(duck, genome_map_path)
        prep_select = "g.prep_sample_idx" if has_prep else "CAST(NULL AS BIGINT) AS prep_sample_idx"
        reader = duck.execute(
            f"SELECT fm.feature_idx, g.genome_source, g.genome_source_id, {prep_select}"
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
            prep_sample_idxs = batch.column("prep_sample_idx").to_pylist()
            async with pool.acquire() as conn, conn.transaction():
                await _write_genome_associations(
                    conn, feat_idxs, sources, source_ids, prep_sample_idxs
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


def _do_action(action_type: str, data_plane_url: str, token: bytes) -> list:
    """Synchronous gRPC DoAction against the data plane — runs in a thread
    executor. Every CP-side DoAction primitive differs only by action name, so
    they share this one client-open/call/collect body: the single place the
    Flight client is constructed, hence the single place to add a timeout, TLS,
    or error mapping later. `action_type` is positional so it forwards cleanly
    through `run_in_executor(None, _do_action, name, url, token)`.
    """
    with _flight.FlightClient(data_plane_url) as client:
        return list(client.do_action(_flight.Action(action_type, token)))


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
    feature_map_path = output_dir / MINT_FEATURES_OUTPUT_BASENAME

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


async def write_shard_assignment(
    pool: asyncpg.Pool,
    reference_idx: int,
    shards: Sequence[Sequence[int]],
) -> int:
    """Record a shard planner's output onto qiita.reference_membership.shard_id.

    `shards[i]` is the list of feature_idx assigned to shard `i`; each listed
    feature's membership row for this reference is stamped with that shard index.
    A feature present in no shard list keeps `shard_id NULL` (e.g. a deferred
    16S / no-genome feature the current sharding pass does not cover).

    Clear-first: as the first statement in the transaction it NULLs every
    membership row's shard_id for this reference, then sets the new layout. So a
    re-plan that DROPS a feature (present before, absent now) leaves it NULL
    instead of carrying a stale shard_id from the prior plan — the persisted
    assignment always reflects exactly the passed `shards`.

    Idempotent and replay-safe: clear-then-set over one transaction, so
    re-running the same assignment sets the same values without error. Scoped to
    `reference_idx`, so a feature shared across references (same feature_idx) is
    stamped only for this reference's membership row. Batched in `_CHUNK_SIZE`
    slices so a GG2-scale reference doesn't send one giant array. Returns the
    total number of membership rows updated to a non-NULL shard (feature_idx
    values not in this reference's membership match nothing and are not counted;
    the clear-first NULLing is not counted).
    """
    total_updated = 0
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(
            "UPDATE qiita.reference_membership SET shard_id = NULL WHERE reference_idx = $1",
            reference_idx,
        )
        # Flatten to a (feature_idx, shard_id) pair stream and apply one
        # set-based UPDATE...FROM unnest per _CHUNK_SIZE slice — a handful of
        # round-trips instead of one per shard. `itertools.islice` keeps only a
        # slice in memory at a time (the generator never materializes all pairs).
        # Each feature is in exactly one shard (feature_genome.feature_idx is
        # UNIQUE), so no pair targets a row twice. RETURNING preserves the count.
        pair_stream = (
            (feature_idx, shard_id)
            for shard_id, feature_idxs in enumerate(shards)
            for feature_idx in feature_idxs
        )
        while batch := list(itertools.islice(pair_stream, _CHUNK_SIZE)):
            rows = await conn.fetch(
                "UPDATE qiita.reference_membership rm SET shard_id = t.shard_id"
                " FROM unnest($1::bigint[], $2::int[]) AS t(feature_idx, shard_id)"
                " WHERE rm.reference_idx = $3 AND rm.feature_idx = t.feature_idx"
                " RETURNING rm.feature_idx",
                [feature_idx for feature_idx, _ in batch],
                [shard_id for _, shard_id in batch],
                reference_idx,
            )
            total_updated += len(rows)
    return total_updated


# Taxonomy rank columns, coarsest→finest, in the lineage sort-key order. `class`
# and `order` are quoted — `order` is a SQL keyword and `class` is reserved in
# some dialects; DuckDB stores the reference_taxonomy columns under these exact
# (lowercase, prefix-stripped) names (see the data plane's qiita_lake schema).
_TAXONOMY_RANK_COLUMNS = (
    "domain",
    "phylum",
    '"class"',
    '"order"',
    "family",
    "genus",
    "species",
    "strain",
)

_REFERENCE_TAXONOMY_TABLE = "reference_taxonomy"


def _do_get_reference_taxonomy(data_plane_url: str, ticket_bytes: bytes, out_path: Path) -> Path:
    """Synchronous Flight DoGet of a reference's taxonomy rows, streamed to a
    Parquet at `out_path` (one row per feature: feature_idx + the eight rank
    columns). Runs in a thread executor (pyarrow.flight is sync); isolated as a
    module function so plan_shards's DB test can stub the whole seam.

    Streams via `.to_reader()` (not `read_all()`) so a GG2-scale taxonomy never
    fully materializes in memory. The writer is created from the stream schema
    up front, so an empty stream still writes a valid, correctly-typed Parquet
    (a reference with no taxonomy loaded → every genome sorts as unclassified)."""
    with _flight.FlightClient(data_plane_url) as client:
        reader = client.do_get(_flight.Ticket(ticket_bytes)).to_reader()
        writer = pq.ParquetWriter(str(out_path), reader.schema, compression="snappy")
        try:
            for batch in reader:
                writer.write_batch(batch)
        finally:
            writer.close()
    return out_path


async def _export_member_genome(pool: asyncpg.Pool, reference_idx: int, out_path: Path) -> None:
    """Stream this reference's (feature_idx, genome_idx) pairs from Postgres to a
    Parquet at `out_path`, in `_CHUNK_SIZE` batches (a GG2-scale reference has
    millions of members — never one giant array). The INNER JOIN to
    feature_genome drops features with no genome, which is deliberate: no-genome
    features (16S / deferred) never enter a shard and keep shard_id NULL.

    An empty result still writes a valid two-column Parquet (schema created up
    front) so DuckDB's read_parquet doesn't fail on a zero-genome reference."""
    schema = pa.schema([("feature_idx", pa.int64()), ("genome_idx", pa.int64())])
    writer = pq.ParquetWriter(str(out_path), schema, compression="snappy")
    try:
        async with pool.acquire() as conn, conn.transaction():
            cursor = await conn.cursor(
                "SELECT rm.feature_idx, fg.genome_idx"
                " FROM qiita.reference_membership rm"
                " JOIN qiita.feature_genome fg USING (feature_idx)"
                " WHERE rm.reference_idx = $1",
                reference_idx,
            )
            while batch := await cursor.fetch(_CHUNK_SIZE):
                writer.write_table(
                    pa.table(
                        {
                            "feature_idx": pa.array([r["feature_idx"] for r in batch], pa.int64()),
                            "genome_idx": pa.array([r["genome_idx"] for r in batch], pa.int64()),
                        }
                    )
                )
    finally:
        writer.close()


def _genome_lineages(con: duckdb.DuckDBPyConnection) -> list[LineageItem]:
    """Reduce the DuckDB `member_genome` (feature_idx, genome_idx) + `taxonomy`
    relations to one LineageItem per genome. The lineage is the semicolon-joined
    rank string of the genome's LOWEST-feature_idx member (via `arg_min`), so the
    representative is deterministic regardless of scan order and independent of
    which sibling features carry divergent taxonomy. `concat_ws` skips NULL
    ranks, so an unclassified genome (all ranks NULL, or no taxonomy row via the
    LEFT JOIN) reduces to lineage '' — which sorts first in the tiler."""
    ranks = ", ".join(f"t.{col}" for col in _TAXONOMY_RANK_COLUMNS)
    rows = con.execute(
        f"SELECT mg.genome_idx,"
        f"       arg_min(concat_ws(';', {ranks}), mg.feature_idx) AS lineage"
        f"  FROM member_genome mg"
        f"  LEFT JOIN taxonomy t ON t.feature_idx = mg.feature_idx"
        f" GROUP BY mg.genome_idx"
    ).fetchall()
    return [LineageItem(item_id=genome_idx, lineage=lineage or "") for genome_idx, lineage in rows]


def _compute_shards(
    con: duckdb.DuckDBPyConnection, *, num_shards: int = _SHARD_COUNT
) -> list[list[int]]:
    """Given a DuckDB connection with `member_genome` + `taxonomy` relations,
    return `shards[k]` = the feature_idxs assigned to shard `k`: reduce to one
    lineage per genome, tile lineage-sorted (`tile_by_lineage`), then expand each
    genome back to ITS features via a DuckDB join (keeping the fan-out in DuckDB,
    not Python). Every genome-bearing member feature lands in exactly one shard;
    a no-genome feature is absent from `member_genome` and so from every shard.
    A zero-genome reference yields `[]`."""
    shards = tile_by_lineage(_genome_lineages(con), num_shards)
    if not shards:
        return []
    con.execute("CREATE OR REPLACE TEMP TABLE shard_map (genome_idx BIGINT, shard_id INTEGER)")
    # One vectorized insert via a registered Arrow table, not a row-by-row
    # executemany (genome count is GG2-scale). The pre-created typed shard_map +
    # INSERT...SELECT pins the column types (BIGINT/INTEGER) regardless of Arrow.
    shard_pairs = pa.table(
        {
            "genome_idx": pa.array(
                [genome_idx for genomes in shards for genome_idx in genomes], pa.int64()
            ),
            "shard_id": pa.array(
                [k for k, genomes in enumerate(shards) for _ in genomes], pa.int32()
            ),
        }
    )
    con.register("shard_pairs", shard_pairs)
    con.execute("INSERT INTO shard_map SELECT genome_idx, shard_id FROM shard_pairs")
    con.unregister("shard_pairs")
    rows = con.execute(
        "SELECT sm.shard_id, list(mg.feature_idx ORDER BY mg.feature_idx) AS features"
        "  FROM member_genome mg"
        "  JOIN shard_map sm USING (genome_idx)"
        " GROUP BY sm.shard_id"
        " ORDER BY sm.shard_id"
    ).fetchall()
    return [features for _shard_id, features in rows]


async def plan_shards(
    pool: asyncpg.Pool,
    reference_idx: int,
    *,
    signing_key: bytes,
    data_plane_url: str,
    workspace: Path,
    num_shards: int = _SHARD_COUNT,
) -> int:
    """Assign this reference's genome-bearing features to `num_shards`
    lineage-sorted shards, persisting the result onto
    reference_membership.shard_id. Returns N, the number of shards actually
    produced (`min(num_shards, genome_count)`; 0 for a reference with no
    genomes — nothing to shard).

    The cross-store assembly stays off the CP event loop's Python heap: the
    (feature_idx, genome_idx) map streams from Postgres to a Parquet in chunks,
    the taxonomy streams from the data plane (DoGet) to a Parquet, and the
    lineage reduce + genome→feature expansion run in a local in-memory DuckDB
    over those two Parquets. Only the final shard lists (feature_idx arrays)
    materialize in Python, handed to `write_shard_assignment`.

    Idempotent / re-plan-safe: write_shard_assignment clears-first, so a re-plan
    that drops a genome leaves its features NULL. DoGet is read-only, so a resume
    re-materializes the same inputs."""
    workspace.mkdir(parents=True, exist_ok=True)
    member_parquet = workspace / "member_genome.parquet"
    taxonomy_parquet = workspace / "taxonomy.parquet"

    await _export_member_genome(pool, reference_idx, member_parquet)

    ticket = sign_ticket(
        table=_REFERENCE_TAXONOMY_TABLE,
        filter={"reference_idx": [reference_idx]},
        secret=signing_key,
    )
    await asyncio.get_event_loop().run_in_executor(
        None, _do_get_reference_taxonomy, data_plane_url, ticket, taxonomy_parquet
    )

    with duckdb.connect(":memory:") as con:
        con.execute(
            "CREATE TABLE member_genome AS"
            f" SELECT feature_idx, genome_idx FROM read_parquet('{member_parquet}')"
        )
        con.execute(f"CREATE TABLE taxonomy AS SELECT * FROM read_parquet('{taxonomy_parquet}')")
        feature_shards = _compute_shards(con, num_shards=num_shards)

    await write_shard_assignment(pool, reference_idx, feature_shards)
    return len(feature_shards)


# DuckDB JOIN that resolves each assembly contig to its bin + feature_idx.
# bin_map (read_id -> kind, bin_id) x manifest (read_id -> sequence_hash) x
# feature_map (sequence_hash -> feature_idx). The read_id is assembly_hash's
# synthetic globally-unique id (kind:bin_id:contig_id), so the join is 1:1 per
# contig. INNER JOINs by construction: every bin_map read_id is a manifest read_id
# (both from the same assembly_hash scan) and every manifest hash was minted by
# mint-features, so no contig is dropped. Exposed as a module constant so the join
# is unit-testable against Parquet fixtures without a Postgres pool.
ASSEMBLY_MEMBERSHIP_JOIN_SQL = (
    "SELECT bm.kind, bm.bin_id, fm.feature_idx"
    " FROM read_parquet(?) AS bm"
    " JOIN read_parquet(?) AS m ON bm.read_id = m.read_id"
    " JOIN read_parquet(?) AS fm ON m.sequence_hash = fm.sequence_hash"
)


async def write_assembly_membership(
    pool: asyncpg.Pool,
    prep_sample_idx: int,
    processing_idx: int,
    bin_map_path: Path,
    manifest_path: Path,
    feature_map_path: Path,
) -> tuple[int, int]:
    """Link a prep_sample's assembly-run contigs to qiita.assembly_membership.

    The assembly analogue of `write_membership`. DuckDB JOINs `bin_map`
    (read_id -> kind, bin_id) against `manifest` (read_id -> sequence_hash) and
    the already-minted `feature_map` (sequence_hash -> feature_idx), resolving
    each contig set-side to `(kind, bin_id, feature_idx)`; the stream is read in
    `_CHUNK_SIZE` batches and bulk-inserted into qiita.assembly_membership with
    `(prep_sample_idx, processing_idx)` stamped from this run. Never materialises
    the whole mapping in Python — same streaming contract mint_features /
    write_membership follow.

    Returns `(linked, already_linked)`. Idempotent (ON CONFLICT DO NOTHING on the
    natural PK): a workflow retried from the start re-links nothing new. Raises
    ValueError (FK violation surfaced structured) if any feature_idx is missing
    from qiita.feature.
    """
    for label, path in [
        ("bin_map", bin_map_path),
        ("manifest", manifest_path),
        ("feature_map", feature_map_path),
    ]:
        if not path.exists():
            raise FileNotFoundError(f"{label} not found: {path}")

    total_linked = 0
    total_seen = 0
    async with pool.acquire() as conn:
        with duckdb.connect(":memory:") as duck:
            reader = duck.execute(
                ASSEMBLY_MEMBERSHIP_JOIN_SQL,
                [str(bin_map_path), str(manifest_path), str(feature_map_path)],
            ).to_arrow_reader(_CHUNK_SIZE)
            for batch in reader:
                kinds = batch.column("kind").to_pylist()
                if not kinds:
                    continue
                bin_ids = batch.column("bin_id").to_pylist()
                feature_idxs = batch.column("feature_idx").to_pylist()
                async with conn.transaction():
                    linked = await insert_assembly_membership_rows(
                        conn,
                        prep_sample_idx=prep_sample_idx,
                        processing_idx=processing_idx,
                        kinds=kinds,
                        bin_ids=bin_ids,
                        feature_idxs=feature_idxs,
                    )
                total_linked += linked
                total_seen += len(feature_idxs)
    return total_linked, total_seen - total_linked


async def register_index(
    pool: asyncpg.Pool,
    reference_idx: int,
    index_type: str,
    fs_path: str,
    params: dict[str, Any],
    shard_id: int | None = None,
) -> int:
    """Record a built search index (e.g. a rype `.ryxdi`) for a reference in
    qiita.reference_index. `fs_path` is the on-disk location; `params` is the
    build configuration (k, w, bucket_name, ...) stored as JSONB — the
    authoritative manifest lives inside the index artifact itself.

    `shard_id` is recorded verbatim: None for an unsharded whole-reference index
    (host `rype`/`minimap2`), or the shard's index (0..N-1) for a sharded
    analysis index that writes one row per shard.

    Returns the reference_index_idx. Idempotent on
    (reference_idx, index_type, fs_path): a workflow retried from the start
    re-runs this primitive, and re-inserting would otherwise duplicate the
    row (the table has no UNIQUE on that triple, by design, so growth can
    append generations). The conditional INSERT + fallback SELECT returns the
    existing row's id instead. `shard_id` is deliberately NOT part of that key:
    each shard's `fs_path` is already shard-distinct (the per-aligner shard root
    encodes the shard, e.g. `.../minimap2-shards/{shard_id}.mmi`,
    `.../bowtie2-shards/{shard_id}/index`),
    so distinct shards never collide and re-registering the same shard dedups on
    path exactly like the unsharded case — a future sharded-index builder must
    preserve that shard->path bijection. This guards the sequential re-run path;
    truly concurrent registrations of the same reference are not expected (one
    workflow runs per reference at a time).
    """
    row = await pool.fetchrow(
        "INSERT INTO qiita.reference_index (reference_idx, index_type, fs_path, params, shard_id)"
        " SELECT $1, $2, $3, $4::jsonb, $5"
        " WHERE NOT EXISTS ("
        "   SELECT 1 FROM qiita.reference_index"
        "   WHERE reference_idx = $1 AND index_type = $2 AND fs_path = $3)"
        " RETURNING reference_index_idx",
        reference_idx,
        index_type,
        fs_path,
        json.dumps(params),
        shard_id,
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


async def finalize_shard(
    pool: asyncpg.Pool,
    reference_idx: int,
    expected_index_types: Sequence[str],
) -> dict[str, Any]:
    """Terminal step of each shard's build ticket: count-based, fail-closed
    completion of a sharded reference.

    Derives N = the number of shards from `reference_membership` (COUNT(DISTINCT
    shard_id) over the non-NULL rows — no `shard_count` column to keep in sync),
    then, for each expected `index_type` (the build_* subset this reference was
    sharded for), counts registered shard rows in `reference_index`. Iff every
    expected type has a registered row for all N shards AND the ONE
    whole-reference `rype_router` row is registered (shard_id NULL), it does the
    guarded `indexing -> active` transition so `active` guarantees a
    fully-index-complete AND routable sharded reference (the consumer needs no
    coverage check). A single still-missing shard, or a missing router, leaves the
    reference honestly in `indexing` (fail-closed) — this primitive NEVER
    transitions to `failed` (the FSM's only exit from `failed` is `-> pending`, a
    full-ingest restart — wrong blast radius; an operator redrives the failed
    shard / router ticket instead).

    The router is checked SEPARATELY from `expected_index_types` (which is the
    per-shard set): it is whole-reference, built once by the parent reference-add
    ticket, not per shard. Both the child shard `finalize-shard` calls and the
    parent's own `finalize-shard` (after it registers the router) converge on this
    check — whichever completes the full set (all N shards for every expected type
    + the router) last flips `active`.

    Race-safe: register_index rows are dedup'd on (reference_idx, index_type,
    fs_path) and committed before each ticket's own finalize_shard, so the last
    finalize in wall-clock time observes every sibling's rows; the guarded
    UPDATE (transition_reference_status) lets exactly one racer flip `active`,
    and a finalize that finds the reference already `active` treats the
    IllegalStatusTransition as idempotent success.

    Returns a JSON-able summary: `activated` (whether the reference is now
    active), the derived `expected_shards` N, and per-type `registered_shards`.
    """
    async with pool.acquire() as conn, conn.transaction():
        n = await count_reference_shards(conn, reference_idx)
        registered: dict[str, int] = {}
        for index_type in expected_index_types:
            registered[index_type] = await conn.fetchval(
                "SELECT count(DISTINCT shard_id) FROM qiita.reference_index"
                " WHERE reference_idx = $1 AND index_type = $2 AND shard_id IS NOT NULL",
                reference_idx,
                index_type,
            )
        # The whole-reference rype_router (shard_id NULL) — built once by the
        # parent, not per shard. Required for `active`: without it the sharded
        # aligners can't route a read to its shard(s), so the reference is not
        # actually alignable.
        router_present = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM qiita.reference_index"
            " WHERE reference_idx = $1 AND index_type = $2 AND shard_id IS NULL)",
            reference_idx,
            INDEX_TYPE_RYPE_ROUTER,
        )
        # Fail-closed: require at least one expected type AND at least one shard
        # AND the router, so a degenerate call (no expected types → registered={}
        # → all([]) is vacuously True) can NEVER flip `active` with zero indexes.
        # `active` must always mean "every expected index is built for all N shards
        # and the whole-reference router is registered".
        complete = (
            n > 0
            and bool(registered)
            and all(count >= n for count in registered.values())
            and router_present
        )
        activated = False
        if complete:
            try:
                await transition_reference_status(conn, reference_idx, ReferenceStatus.ACTIVE)
                activated = True
            except IllegalStatusTransition:
                # A sibling finalize already flipped `active` (or the reference
                # is otherwise past `indexing`) — idempotent success, not a fault.
                current = await conn.fetchval(
                    "SELECT status FROM qiita.reference WHERE reference_idx = $1",
                    reference_idx,
                )
                activated = current == ReferenceStatus.ACTIVE.value
                if not activated:
                    raise
    return {
        "reference_idx": reference_idx,
        "expected_shards": n,
        "registered_shards": registered,
        "router_present": bool(router_present),
        "activated": activated,
    }


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
    signing_key: bytes,
    data_plane_url: str,
) -> list[str]:
    """Register Parquet files in DuckLake via the data plane's DoAction.

    Signs the Ed25519 action token, calls Flight in a thread (FlightClient
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
        secret=signing_key,
    )
    results = await asyncio.get_event_loop().run_in_executor(
        None, _do_action, "register_files", data_plane_url, token
    )
    if not results:
        return []
    result_body = json.loads(results[0].body.to_pybytes())
    return result_body.get("registered", [])


async def delete_reference_data(
    *,
    reference_idx: int,
    signing_key: bytes,
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
        secret=signing_key,
    )
    results = await asyncio.get_event_loop().run_in_executor(
        None, _do_action, "delete_reference", data_plane_url, token
    )
    if not results:
        return {}
    return json.loads(results[0].body.to_pybytes())


async def delete_pool_reads_data(
    *,
    prep_sample_idxs: list[int],
    signing_key: bytes,
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
        secret=signing_key,
    )
    results = await asyncio.get_event_loop().run_in_executor(
        None, _do_action, "delete_pool_reads", data_plane_url, token
    )
    if not results:
        return {}
    return json.loads(results[0].body.to_pybytes())


async def delete_mask_data(
    *,
    mask_idx: int,
    signing_key: bytes,
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
        secret=signing_key,
    )
    results = await asyncio.get_event_loop().run_in_executor(
        None, _do_action, "delete_mask", data_plane_url, token
    )
    if not results:
        return 0
    return json.loads(results[0].body.to_pybytes()).get("rows_deleted", 0)


async def mask_metrics_data(
    *,
    mask_idx: int,
    prep_sample_idx: int,
    signing_key: bytes,
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
        secret=signing_key,
    )
    results = await asyncio.get_event_loop().run_in_executor(
        None, _do_action, "mask_metrics", data_plane_url, token
    )
    if not results:
        raise RuntimeError("mask_metrics DoAction returned no result")
    return json.loads(results[0].body.to_pybytes())


async def delete_read_mask_block_data(
    *,
    mask_idx: int,
    members: list[dict[str, int]],
    signing_key: bytes,
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
        secret=signing_key,
    )
    results = await asyncio.get_event_loop().run_in_executor(
        None, _do_action, "delete_read_mask_block", data_plane_url, token
    )
    if not results:
        return 0
    return json.loads(results[0].body.to_pybytes()).get("rows_deleted", 0)


async def delete_alignment_block_data(
    *,
    alignment_idx: int,
    members: list[dict[str, int]],
    signing_key: bytes,
    data_plane_url: str,
) -> int:
    """Delete one block's exact `alignment` footprint via the
    `delete_alignment_block` DoAction, returning the rows-deleted count. The
    alignment twin of `delete_read_mask_block_data`.

    `members` is the block's cover-map as `{prep_sample_idx, sequence_idx_start,
    sequence_idx_stop}` dicts (from `block_member`). The data plane deletes the
    rows for `alignment_idx` whose `(prep_sample_idx, sequence_idx)` fall in those
    sub-ranges — exact by construction (per-member OR) and feature_idx-agnostic
    (all of a read's alignment rows go, since a read produces multiple rows via
    cross-shard + PE multiplicity), so a split sample's sibling-block rows survive.
    This is the idempotent-block-replace step run immediately before register-files.

    Idempotent: a fresh block (no rows yet) deletes 0 and still succeeds; an empty
    `members` list short-circuits without a Flight call (an empty block is a
    control-plane bug the runner never dispatches, guarded here too). Raises
    pyarrow.flight.FlightError on transport / data-plane failure."""
    if not members:
        return 0
    token = sign_action(
        action="delete_alignment_block",
        payload={"alignment_idx": alignment_idx, "members": members},
        secret=signing_key,
    )
    results = await asyncio.get_event_loop().run_in_executor(
        None, _do_action, "delete_alignment_block", data_plane_url, token
    )
    if not results:
        return 0
    return json.loads(results[0].body.to_pybytes()).get("rows_deleted", 0)


async def delete_alignment_data(
    *,
    alignment_idx: int,
    signing_key: bytes,
    data_plane_url: str,
) -> int:
    """Delete an alignment's DuckLake `alignment` rows via the data plane's
    `delete_alignment` DoAction, returning the rows-deleted count. The alignment
    twin of `delete_mask_data` — the whole-alignment purge the
    disallow-without-delete resubmission rule needs.

    Signs a `delete_alignment` action token carrying only `alignment_idx`. The
    delete is a logical `DELETE FROM alignment WHERE alignment_idx = ?` inside one
    DuckLake transaction — no parquet is reclaimed from disk (mirrors
    `delete_mask`). Idempotent: an alignment whose rows never registered (or were
    already deleted) deletes zero rows and still succeeds. Raises
    pyarrow.flight.FlightError on transport / data-plane failure."""
    token = sign_action(
        action="delete_alignment",
        payload={"alignment_idx": alignment_idx},
        secret=signing_key,
    )
    results = await asyncio.get_event_loop().run_in_executor(
        None, _do_action, "delete_alignment", data_plane_url, token
    )
    if not results:
        return 0
    return json.loads(results[0].body.to_pybytes()).get("rows_deleted", 0)


async def _finalize_sample_metrics(
    conn: asyncpg.Connection,
    *,
    prep_sample_idx: int,
    mask_idx: int,
    signing_key: bytes,
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
        signing_key=signing_key,
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
    signing_key: bytes,
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
        signing_key=signing_key,
        data_plane_url=data_plane_url,
    )
    return {"block_idx": block_idx, "rows_deleted": rows_deleted}


async def reconcile_block(
    pool: asyncpg.Pool,
    *,
    block_idx: int,
    mask_idx: int,
    signing_key: bytes,
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
                signing_key=signing_key,
                data_plane_url=data_plane_url,
            )
            await finalize_mask_sample(conn, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx)
            finalized.append(prep_sample_idx)
    return {"block_idx": block_idx, "finalized_samples": finalized}


async def delete_alignment_block(
    pool: asyncpg.Pool,
    *,
    block_idx: int,
    alignment_idx: int,
    signing_key: bytes,
    data_plane_url: str,
) -> dict[str, Any]:
    """Idempotent block replace: delete this block's exact `alignment` footprint
    before register-files re-writes it, so a block re-run never double-counts. The
    alignment twin of `delete_read_mask_block`.

    Reads the block's cover-map (`block_member`) and asks the data plane to delete
    the `alignment` rows for `alignment_idx` whose `(prep_sample_idx, sequence_idx)`
    fall in the members' sub-ranges. The delete is exact by construction (per-member
    OR residual) and feature_idx-agnostic — it clears ALL of a read's alignment rows
    (a read produces multiple rows via cross-shard + PE multiplicity) — so a sample
    split across several blocks keeps its sibling blocks' rows; only THIS block's
    footprint goes.

    On a fresh block this deletes 0 rows (nothing registered yet); on a re-run
    (retry, or an operator-resubmitted block covering the same footprint) it clears
    the prior rows so the subsequent register-files leaves exactly one copy. There
    is no reconcile count-assertion for alignment (rows are not 1:1 with reads), but
    the delete-then-register discipline keeps a retried block from accumulating
    duplicate alignment rows. Read-only on Postgres — the delete lands in DuckLake.
    Returns the rows-deleted count for the workflow log."""
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
    rows_deleted = await delete_alignment_block_data(
        alignment_idx=alignment_idx,
        members=members,
        signing_key=signing_key,
        data_plane_url=data_plane_url,
    )
    return {"block_idx": block_idx, "rows_deleted": rows_deleted}


async def reconcile_alignment_block(
    pool: asyncpg.Pool,
    *,
    block_idx: int,
    alignment_idx: int,
) -> dict[str, Any]:
    """Terminal step of the `align` workflow: mark this block done, then finalize
    each sample it covers whose LAST covering block just completed. The alignment
    twin of `reconcile_block`, keyed on `alignment_idx` not `mask_idx`.

    In one transaction (the whole reconcile is atomic):

    1. Flip this block to 'completed' — its `alignment` rows are registered
       (register-files ran before this step).
    2. For each covered sample, in a stable prep_sample_idx order (deadlock-free):
       take the `alignment_sample` gate row's `FOR UPDATE` lock (serializes
       concurrent block finalizers of the same sample), skip if already 'completed'
       (idempotent / lost the race), skip if any covering block is not yet
       'completed' (a sibling still owes alignments), else flip the gate to
       'completed'.

    **No count-assertion, no metrics rollup** (unlike `reconcile_block`): alignment
    rows are NOT 1:1 with reads — a read routed to K shards emits K rows, and a
    paired-end read emits one row per mate — so "row count == read count" does not
    hold and completion is purely "every covering block done". This is also why the
    primitive needs no data-plane hop (no `signing_key`/`data_plane_url`): it only
    touches Postgres gate rows.

    Idempotent: a re-run flips nothing new (its block is already completed and its
    samples already finalized). Returns a summary of the samples this block
    finalized."""
    finalized: list[int] = []
    async with pool.acquire() as conn, conn.transaction():
        # This block's work is done; count it as completed BEFORE the per-sample
        # gate check below (same txn — the UPDATE is visible to our own SELECTs).
        # We proceed regardless of the guarded bool: a re-run where it is already
        # completed still (idempotently) finalizes its samples.
        await set_block_state(
            conn,
            block_idx=block_idx,
            new_state="completed",
            expected_states=["pending", "processing", "failed"],
        )
        for prep_sample_idx, _min_seq, _max_seq in await fetch_block_members(conn, block_idx):
            state = await lock_alignment_sample(
                conn, alignment_idx=alignment_idx, prep_sample_idx=prep_sample_idx
            )
            if state is None:
                raise RuntimeError(
                    f"alignment_sample gate row missing for (alignment={alignment_idx}, "
                    f"prep_sample={prep_sample_idx}); it must be materialized PENDING "
                    "at plan time before any block runs"
                )
            if state == "completed":
                # Already finalized (idempotent re-run, or a concurrent block
                # finalizer won this sample's race) — nothing to do.
                continue
            if await has_incomplete_covering_alignment_block(
                conn, alignment_idx=alignment_idx, prep_sample_idx=prep_sample_idx
            ):
                # A sibling block still owes this sample alignments — do not
                # finalize a partially-aligned sample.
                continue
            await finalize_alignment_sample(
                conn, alignment_idx=alignment_idx, prep_sample_idx=prep_sample_idx
            )
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
    LibraryPrimitive.WRITE_ASSEMBLY_MEMBERSHIP: write_assembly_membership,
    LibraryPrimitive.REGISTER_FILES: register_files,
    LibraryPrimitive.REGISTER_INDEX: register_index,
    LibraryPrimitive.PLAN_SHARDS: plan_shards,
    LibraryPrimitive.FINALIZE_SHARD: finalize_shard,
    LibraryPrimitive.PERSIST_READ_METRICS: persist_read_metrics,
    LibraryPrimitive.PERSIST_QC_REPORT: persist_qc_report,
    LibraryPrimitive.DELETE_READ_MASK_BLOCK: delete_read_mask_block,
    LibraryPrimitive.RECONCILE_BLOCK: reconcile_block,
    LibraryPrimitive.DELETE_ALIGNMENT_BLOCK: delete_alignment_block,
    LibraryPrimitive.RECONCILE_ALIGNMENT_BLOCK: reconcile_alignment_block,
}
