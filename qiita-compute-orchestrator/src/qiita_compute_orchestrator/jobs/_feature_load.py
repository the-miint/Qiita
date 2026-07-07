"""Shared feature-sequence writers + assembly `kind` literals.

Neutral helper for the two job modules that re-key hash-keyed sequence
outputs to `feature_idx` and emit the DuckLake-shape staging Parquets:
`reference_load` (reference-add tail) and `assembly_load` (long-read
assembly tail). The shared `qiita.feature` space means an assembled
contig and a reference sequence with the same bytes carry the SAME
`feature_idx`, so the sequence + chunk writers are byte-for-byte
identical across the two tails — they live here, in neither job module.

This is a **private shared helper**, not a dispatchable native job: it
exports neither `Inputs` nor `execute`, and its leading-underscore name
exempts it from the boot-time job scan (`scan_native_jobs`). Nothing
routes work here directly; the two job modules import from it.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
from qiita_common.parquet import validate_parquet_path

from ..miint import PARQUET_OPTS, PARQUET_OPTS_CHUNKED

# The closed `kind` value set stored in the DuckLake/Postgres assembly tables,
# single-sourced here so `assembly_hash` (producer of `bin_map.kind`) and
# `assembly_load` (writer of `assembly_membership.kind` / `bin_quality.kind`)
# stay in lockstep — `bin_quality` joins `assembly_membership` on `kind`, so a
# drift between the two would silently break that join. Plain module constants,
# not a cross-language enum: `kind` is a TEXT column with no Postgres ENUM twin
# (deliberately extensible — a future 'plasmid'/'small_circular' kind is
# intended), so the shared Python constant is the fail-fast guard, not a DB
# CHECK.
KIND_LCG = "LCG"  # a circular genome (large circular genome)
KIND_MAG = "MAG"  # a refined metagenome-assembled bin

# Per-batch chunk budget for `write_feature_sequence_chunks`. Same
# rationale as hash_sequences: each batch's in-memory sort is bounded
# by `_CHUNK_BUDGET_PER_BATCH × chunk_size` (~3.2 GB at 64 KB chunks).
# Bin-packing by chunk-count (not feature-count) is load-bearing on
# GG2 backbone where feature sizes span 3+ orders of magnitude;
# feature-count batching would concentrate the genome tail into the
# first batch and OOM even the 31 GB cap.
_CHUNK_BUDGET_PER_BATCH = 50_000


def build_feature_id_map(
    conn: duckdb.DuckDBPyConnection,
    manifest_path: Path,
) -> None:
    """Join manifest + feature_map (TEMP TABLE pre-loaded by execute) on
    sequence_hash. Raises ValueError if any manifest row lacks a matching
    feature_map row — mint-features is supposed to mint a feature_idx for
    every distinct hash, so a gap means upstream produced inconsistent
    inputs (permanent error)."""
    manifest_count = conn.execute(
        "SELECT count(*) FROM read_parquet(?)",
        [str(manifest_path)],
    ).fetchone()[0]

    conn.execute(
        "CREATE TEMP TABLE id_map AS "
        "SELECT m.read_id, fm.feature_idx,"
        "  m.sequence_hash,"
        "  m.sequence_length_bp "
        "FROM read_parquet(?) m "
        "JOIN feature_map fm "
        "  ON m.sequence_hash = fm.sequence_hash",
        [str(manifest_path)],
    )

    id_map_count = conn.execute("SELECT count(*) FROM id_map").fetchone()[0]
    if id_map_count != manifest_count:
        n_unmapped = manifest_count - id_map_count
        unmapped = conn.execute(
            "SELECT m.sequence_hash FROM read_parquet(?) m "
            "ANTI JOIN id_map x ON m.sequence_hash = x.sequence_hash "
            "LIMIT 10",
            [str(manifest_path)],
        ).fetchall()
        hashes = [str(r[0]) for r in unmapped]
        raise ValueError(f"{n_unmapped} unmapped sequence hash(es) in feature_map: {hashes}")


def write_feature_sequences(
    conn: duckdb.DuckDBPyConnection,
    out: str,
) -> None:
    """Emit DuckLake's `reference_sequences` shape — one row per unique
    feature_idx with `(feature_idx, sequence_hash, sequence_length_bp)`.
    Pulls everything from id_map (which already carries the per-read
    triple from the manifest × feature_map JOIN); reads sharing a
    canonical hash all carry the same length, so DISTINCT ON
    feature_idx collapses them deterministically."""
    conn.execute(
        "COPY ("
        "  SELECT DISTINCT ON (feature_idx)"
        "    feature_idx, sequence_hash, sequence_length_bp"
        "  FROM id_map"
        "  ORDER BY feature_idx"
        f") TO '{out}' ({PARQUET_OPTS})"
    )


def write_feature_sequence_chunks(
    conn: duckdb.DuckDBPyConnection,
    reference_sequence_chunks_path: Path,
    out_dir: Path,
) -> None:
    """Re-key hash_sequences' chunks (hash-keyed) to DuckLake's
    `reference_sequence_chunks` schema (feature_idx-keyed), as a
    DIRECTORY of `part_*.parquet` files.

    `reference_sequence_chunks_path` (input) is a DIRECTORY of
    `part_*.parquet` files written by hash_sequences. Read via glob.

    `out_dir` (output) likewise becomes a directory of part files.
    The runner's register-files convention picks up this directory as
    a multi-file DuckLake table (table name = `reference_sequence_chunks`).

    **Why batched, not a single sort+write.** The original single-file
    pipeline (parallel readers feeding one writer with a global
    `ORDER BY feature_idx, chunk_index`) OOMs at GG2 scale: ~30+ GB of
    chunk_data piles up in the reader→writer back-pressure queue
    because zstd-decode is 5-10× faster than zstd-encode. `threads=1`
    workarounds bring memory below the queue limit but the sort itself
    needs ~22 GiB peak on GG2 backbone, exceeding what a 30 GiB host
    can offer with Postgres + Python + OS overhead. See
    miint-localdocs/sequence-chunking-assessment.md for the benchmark.

    **Batched shape.** Bin-pack features by chunk count into batches
    of ≤ `_CHUNK_BUDGET_PER_BATCH` chunks (~3.2 GB raw per batch),
    write each batch as its own `part_NNNNN.parquet` with an internal
    `ORDER BY (feature_idx, chunk_index)`. Batches walk feature_idx
    in ascending order, so the parts collectively form one globally-
    sorted dataset readable via `read_parquet(dir/part_*.parquet)`.
    Per-batch peak memory is bounded by the in-memory sort over one
    batch (~3.2 GB), well under the caller's DuckDB memory cap.

    **Memory safety.** The per-batch COPY joins a `feature_map` subset
    pre-filtered to the batch's hashes (the `fmb` CTE), not the full
    `feature_map`, so the join is bounded by one batch by construction —
    the optimizer cannot reorder a full-table join ahead of the batch
    filter and materialize the whole glob's chunk_data (the OOM that the
    hash_sequences output side hit; see that job for the same fix).

    **Cost tradeoff.** Each batch re-scans the full input glob, filtered
    by `WHERE rc.sequence_hash = ANY(?)` (applied during the Parquet scan
    via late materialisation). Scan dominates per-batch wall time, so the
    number of batches matters more than per-batch size — the bin-pack
    keeps it to one batch per ~`_CHUNK_BUDGET_PER_BATCH` chunks."""
    parts_glob = str(reference_sequence_chunks_path / "part_*.parquet")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Metadata scan: (feature_idx, sequence_hash, n_chunks) ordered by
    # feature_idx. Only sequence_hash is read from the input Parquet —
    # columnar storage makes the count(*) cheap (~1-2 sec) even though
    # the input is ~30 GB total. JOIN with the small feature_map TEMP
    # TABLE attaches feature_idx; defensive against any hash without a
    # mint (every input hash should have one via build_feature_id_map's gap
    # check, but this keeps the count semantically correct).
    rows = conn.execute(
        "SELECT fm.feature_idx, rc.sequence_hash, count(*) AS n_chunks "
        "FROM read_parquet(?) rc "
        "JOIN feature_map fm ON rc.sequence_hash = fm.sequence_hash "
        "GROUP BY rc.sequence_hash, fm.feature_idx "
        "ORDER BY fm.feature_idx",
        [parts_glob],
    ).fetchall()

    # Each batch is a list of sequence_hash strings to filter on.
    # Bin-pack in feature_idx order so output parts collectively form
    # a feature_idx-sorted dataset (each part is internally sorted by
    # feature_idx; batches walk feature_idx ascending).
    batches: list[list[str]] = []
    current_batch: list[str] = []
    current_chunks = 0
    for _feature_idx, sequence_hash, n_chunks in rows:
        if current_batch and current_chunks + n_chunks > _CHUNK_BUDGET_PER_BATCH:
            batches.append(current_batch)
            current_batch = []
            current_chunks = 0
        current_batch.append(str(sequence_hash))
        current_chunks += n_chunks
    if current_batch:
        batches.append(current_batch)

    if batches:
        for i, batch_hashes in enumerate(batches):
            part_path = out_dir / f"part_{i:05d}.parquet"
            part_out = validate_parquet_path(part_path)
            # Two phases per part so the full-glob scan and the feature_idx
            # sort never co-peak in memory. A single COPY doing
            # scan + join + ORDER BY(chunk_data) + write at once OOMed at
            # genome scale: the sort is a pipeline breaker, so it buffers the
            # batch's wide ~64 KB rows while the 30 GB scan and the 8-thread
            # write buffers are all still live. Splitting the scan from the
            # sort means at most one of them is resident at a time.
            #
            # Phase 1 — STREAM this batch's chunks into a temp table, re-keyed
            # hash → feature_idx, with NO sort. The `fmb` CTE bounds the build
            # side to one batch of hashes (so it's the hash-join build and
            # chunk_data streams through the probe, never into the build); the
            # `WHERE ... = ANY(...)` on the input column keeps the filter on the
            # Parquet scan so late materialisation skips chunk_data for
            # non-matching rows. The insert is bounded by the batch and spills
            # to temp_directory under pressure.
            conn.execute(
                "CREATE OR REPLACE TEMP TABLE part_chunks AS "
                "  WITH fmb AS ("
                "    SELECT feature_idx, sequence_hash"
                "    FROM feature_map"
                "    WHERE sequence_hash = ANY(CAST(? AS UUID[]))"
                "  )"
                "  SELECT fmb.feature_idx, rc.chunk_index, rc.chunk_data"
                "  FROM read_parquet(?) rc"
                "  JOIN fmb ON rc.sequence_hash = fmb.sequence_hash"
                "  WHERE rc.sequence_hash = ANY(CAST(? AS UUID[]))",
                [batch_hashes, parts_glob, batch_hashes],
            )
            # Phase 2 — sort THIS part in isolation and write it. The sort sees
            # only the materialised batch (≤ _CHUNK_BUDGET_PER_BATCH chunks),
            # never the 30 GB glob, so it fits the cap (spilling if needed). The
            # per-part `ORDER BY feature_idx` clusters row groups so a
            # `WHERE feature_idx IN (...)` DoGet prunes row groups WITHIN a part;
            # feature_idx-ascending batches keep the parts a globally
            # disjoint-range dataset for catalog-level FILE pruning. Keeping both
            # levels is the point of sorting here — input order is
            # parallel-scrambled upstream (preserve_insertion_order=false), so
            # without this sort a point query would scan a whole part. The
            # secondary `chunk_index` orders a feature's chunks for cheap
            # reassembly.
            conn.execute(
                "COPY ("
                "  SELECT feature_idx, chunk_index, chunk_data"
                "  FROM part_chunks"
                "  ORDER BY feature_idx, chunk_index"
                f") TO '{part_out}' ({PARQUET_OPTS_CHUNKED})"
            )
        conn.execute("DROP TABLE IF EXISTS part_chunks")
    else:
        # No minted features → emit one empty part so the directory is
        # non-empty and the runner's `dir.glob('*.parquet')` discovers
        # the multi-file table. register-files would otherwise error
        # on a zero-file directory.
        empty_part = out_dir / "part_00000.parquet"
        empty_out = validate_parquet_path(empty_part)
        conn.execute(
            "COPY ("
            "  SELECT"
            "    CAST(NULL AS BIGINT) AS feature_idx,"
            "    CAST(NULL AS INTEGER) AS chunk_index,"
            "    CAST(NULL AS VARCHAR) AS chunk_data"
            "  WHERE FALSE"
            f") TO '{empty_out}' ({PARQUET_OPTS_CHUNKED})"
        )
