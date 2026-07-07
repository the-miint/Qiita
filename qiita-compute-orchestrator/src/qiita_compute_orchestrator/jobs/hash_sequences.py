"""Native job: hash + emit a chunked-by-feature reference sequence Parquet.

The CLI's DoPut writes an `upload.parquet` with shape
`(read_id VARCHAR, chunk_index INTEGER, chunk_data VARCHAR)` — sequences
are chunked at the client to keep per-row Parquet width bounded for
genome-scale inputs (single GG2 records run up to ~21 MB).

This step reads that chunked upload and produces:

  - `manifest.parquet` — `(read_id, sequence_hash, sequence_length_bp)`
    One row per upload read.
  - `reference_sequence_chunks.parquet` — `(sequence_hash, chunk_index,
    chunk_data)`. Same 64 KB chunks as the upload, relabeled from
    read_id to canonical sequence_hash. When multiple reads collapse to
    the same canonical hash (a read + its reverse complement), only one
    read's chunks survive — the lex-smallest read_id, deterministically.

**Canonical hashing.** A sequence and its reverse complement describe
the same molecular entity. We compute md5 on BOTH strands and store the
lex-smaller as the canonical `sequence_hash`:

    sequence_hash = LEAST(md5(upper(seq)),
                          md5(sequence_dna_reverse_complement(upper(seq))))::uuid

The stored chunk bytes are NEVER transformed — `chunk_data` is exactly
what the client uploaded. Two strand orientations of the same molecule
get one canonical hash but only one set of chunks survives (the one
whose read_id won the DISTINCT ON). Stored as DuckDB UUID (16 bytes) to
match the wire-side `sequence_hash` and `feature_idx` types — no
VARCHAR md5 hexstring is written anywhere (per the project's
hash-storage rule).

The reverse complement comes from miint's scalar
`sequence_dna_reverse_complement`, which honors full IUPAC ambiguity
codes (A↔T, C↔G, R↔Y, S↔S, W↔W, K↔M, B↔V, D↔H, N↔N) and preserves
non-base characters (e.g. gaps). We `upper()` the input first so case
variation in the upload (`atcg` vs `ATCG`) doesn't desync the hash.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from pydantic import BaseModel
from qiita_common.chunking import reassemble_chunks_expr
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    PARQUET_OPTS,
    PARQUET_OPTS_CHUNKED,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)

YAML_STEP_NAME = "hash_sequences"

# DuckDB resource caps for this step. With the read_id-batched
# pipeline below, peak memory per batch ≈ batch_size × avg record
# size (~50K × ~30 KB ≈ 1.5 GB, ~10 GB worst case if a batch lands
# many of GG2's ~21 MB genome tail). `_DUCKDB_MEMORY_GB` is now only the
# OFF-SLURM fallback (local backend / tests); under SLURM the limit tracks
# the real cgroup via `resolve_duckdb_memory_gb()` (SLURM_MEM_PER_NODE), so a
# `--mem-gb` override reaches DuckDB. The 24 GB literal is now only the
# off-SLURM fallback and is intentionally decoupled from the YAML allocation
# (currently mem_gb=32) — under SLURM DuckDB is capped at the actual cgroup, so
# the literal need not equal it. DuckDB owns the whole box here (no in-process
# co-consumer), so it gets the allocation minus headroom.
_DUCKDB_MEMORY_GB = 24
_DUCKDB_THREADS = 8

# Per-batch chunk budget for the aggregation pass below. Sized so the
# string_agg HASH_AGG state for one batch (which buffers every
# in-flight group's chunks until the group finalises) stays well
# below `_DUCKDB_MEMORY_GB`. 50K chunks × 64 KB/chunk = 3.2 GB max
# uncompressed per batch.
#
# Batching by chunk count (not read count) is load-bearing because
# read sizes vary by 3+ orders of magnitude on GG2 backbone: ~95% of
# reads are 1-chunk 16S amplicons (~1.5 KB) and a tail of genomes
# reach 327 chunks (~21 MB). Read_id-count batching (e.g., 50K reads
# per batch) puts ~661K chunks / ~40 GB into the first alphabetical
# batch (G0/G9 prefixes are genomes; they cluster at the front of a
# sort), OOMing even a 24 GB DuckDB cap. Chunk-count batching
# distributes the genome tail across many batches by bin-packing.
_CHUNK_BUDGET_PER_BATCH = 50_000


class Inputs(BaseModel):
    """Typed input contract for hash_sequences.

    `fasta_path` is the workflow-declared input — the runner resolves
    `fasta_upload_idx` → staging path (compute_upload_staging_path on
    the resolved upload row) and injects under this name. The field is
    role-named (matching the fastq_to_parquet `fastq_path` convention)
    rather than the upload-domain-generic `upload_path` because the
    YAML's `inputs:` list IS the kwarg-name for `Inputs.model_validate`;
    the runner's `{prefix}_upload_idx → {prefix}_path` convention
    requires the role name to live on the model.

    `reference_idx` and `work_ticket_idx` are framework-injected scope
    scalars merged by `flatten_native_inputs`. Both are accepted (typed)
    even though this step doesn't consume them — declaring them on the
    Inputs model keeps the contract explicit and matches the
    fastq_to_parquet convention; without the declaration Pydantic
    silently drops them on `model_validate`, which would hide a mis-wired
    scope dispatch.
    """

    fasta_path: Path
    reference_idx: int
    work_ticket_idx: int


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """Read the chunked upload Parquet; emit manifest + chunks.

    Upload shape: `(read_id, chunk_index, chunk_data)`. Reconstruct each
    read via `string_agg(... ORDER BY chunk_index)`, compute canonical
    hash on both strands, then relabel the upload chunks by sequence_hash
    via JOIN. See module docstring for the canonical-hash semantics."""
    if not inputs.fasta_path.exists():
        raise FileNotFoundError(f"upload parquet not found: {inputs.fasta_path}")

    workspace.mkdir(parents=True, exist_ok=True)
    manifest_path = workspace / "manifest.parquet"
    # reference_sequence_chunks is a DIRECTORY of part_*.parquet files
    # rather than a single file — the consumer contract is
    # `read_parquet(dir/part_*.parquet)`. The relabel below writes one
    # part in a single streaming scan; the directory shape is retained so
    # the output can be split into multiple parts later without touching
    # any consumer.
    reference_sequence_chunks_dir = workspace / "reference_sequence_chunks"
    reference_sequence_chunks_dir.mkdir(parents=True, exist_ok=True)
    manifest_out = validate_parquet_path(manifest_path)

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS),
                threads=_DUCKDB_THREADS,
            )

            # Chunk-budget-batched reconstruction. A single HASH_AGG over
            # the full chunked Parquet buffers every in-flight group's
            # chunks concurrently — for GG2 backbone (~331K reads, max
            # ~21 MB per record, ~40 GB uncompressed sequence data) that
            # exceeds any reasonable DuckDB cap. We batch by chunk-count
            # budget: Python first collects (read_id, n_chunks) for every
            # read (cheap count(*) aggregate, ~10 MB transfer), bin-packs
            # reads into batches each totalling ≤ _CHUNK_BUDGET_PER_BATCH
            # chunks, and each batch's string_agg + native md5 runs
            # inside DuckDB. DuckDB still does the heavy lifting (native
            # md5, parallel string_agg); Python only carries the small
            # read-list metadata. Per-batch HASH_AGG state is bounded by
            # _CHUNK_BUDGET_PER_BATCH × chunk_size (~3.2 GB).
            #
            # The full Parquet is re-scanned per batch (DuckDB can't
            # prune row-groups since the upload Parquet is single-RG by
            # construction in the data plane writer); the scan dominates
            # per-batch wall time, so the number of batches matters more
            # than the per-batch size. GG2 backbone bin-packs to ~20
            # batches.
            chunks_per_read = conn.execute(
                "SELECT read_id, count(*) AS n_chunks "
                "FROM read_parquet(?) "
                "GROUP BY read_id "
                "ORDER BY read_id",
                [str(inputs.fasta_path)],
            ).fetchall()

            batches: list[list[str]] = []
            current_batch: list[str] = []
            current_chunks = 0
            for read_id, n_chunks in chunks_per_read:
                if current_batch and current_chunks + n_chunks > _CHUNK_BUDGET_PER_BATCH:
                    batches.append(current_batch)
                    current_batch = []
                    current_chunks = 0
                current_batch.append(read_id)
                current_chunks += n_chunks
            if current_batch:
                batches.append(current_batch)

            conn.execute(
                "CREATE TEMP TABLE hashed ("
                "  read_id VARCHAR, "
                "  sequence_hash UUID, "
                "  sequence_length_bp BIGINT"
                ")"
            )

            for batch in batches:
                # `c.read_id = ANY(?)` lets DuckDB take the batch list
                # as a single LIST<VARCHAR> parameter and apply it as a
                # filter during the Parquet scan — no temp table, no
                # per-row INSERT round-trip.
                #
                # Canonical hash = LEAST(forward_md5, reverse_md5). Bytes
                # stay as uploaded; canonical identity lives in the hash.
                conn.execute(
                    "INSERT INTO hashed "
                    "WITH per_read AS ("
                    "  SELECT "
                    "    c.read_id, "
                    f"    {reassemble_chunks_expr('c.')} AS sequence "
                    "  FROM read_parquet(?) c "
                    "  WHERE c.read_id = ANY(?) "
                    "  GROUP BY c.read_id"
                    ") "
                    "SELECT "
                    "  read_id, "
                    "  LEAST("
                    "    md5(upper(sequence))::uuid,"
                    "    md5(sequence_dna_reverse_complement(upper(sequence)))::uuid"
                    "  ) AS sequence_hash, "
                    "  CAST(length(sequence) AS BIGINT) AS sequence_length_bp "
                    "FROM per_read",
                    [str(inputs.fasta_path), batch],
                )

            # manifest.parquet — one row per upload read.
            conn.execute(
                "COPY ("
                "  SELECT read_id, sequence_hash, sequence_length_bp"
                "  FROM hashed"
                f") TO '{manifest_out}' ({PARQUET_OPTS})"
            )

            # reference_sequence_chunks/part_00000.parquet — relabel the
            # upload chunks from read_id to canonical sequence_hash in a
            # SINGLE streaming scan. When two reads share a canonical hash
            # (a sequence + its reverse complement) we keep ONE — the
            # lex-smaller read_id, deterministically (DISTINCT ON).
            #
            # No write-time ORDER BY. The sequence_hash sort is not
            # load-bearing for any consumer: reference_load re-keys
            # sequence_hash → feature_idx with its own full scan, the data
            # plane's DoGet filters by feature_idx (never sequence_hash),
            # and sequence reassembly sorts chunk_index in-memory per
            # feature. A whole-file ORDER BY (or PARTITION_BY) over 30+ GB
            # of 64 KB chunk_data OOMs DuckDB's caps — its sort can't spill
            # rows that fat, and the partitioned writer either OOMs or
            # shatters the output into tens of thousands of tiny files.
            #
            # The streaming relabel is bounded BY CONSTRUCTION: `canonical`
            # has one narrow row (read_id + uuid) per distinct hash, and
            # because chunks ≥ reads ≥ canonical it is ALWAYS the
            # lower-cardinality join input, so the optimizer builds the
            # hash table on it and chunk_data rides the probe side straight
            # to the writer — never buffered into a build side or a sort.
            # Peak memory is ~1 GB/thread, constant in file size. This
            # replaces the old per-batch loop, which re-scanned the whole
            # upload once per batch AND left the full-`hashed` JOIN free to
            # reorder ahead of the batch filter — at genome scale that
            # materialized the entire file's chunk_data and OOM'd.
            #
            # The `canonical` CTE's ORDER BY is on the narrow hashed table
            # only (no chunk_data) — it is what makes DISTINCT ON pick the
            # lex-smallest read_id deterministically, and it spills cheaply.
            # Empty input writes a valid 0-row part (schema from the
            # projection), keeping the directory non-empty for consumers.
            part_out = validate_parquet_path(reference_sequence_chunks_dir / "part_00000.parquet")
            conn.execute(
                "COPY ("
                "  WITH canonical AS ("
                "    SELECT DISTINCT ON (sequence_hash) read_id, sequence_hash"
                "    FROM hashed"
                "    ORDER BY sequence_hash, read_id"
                "  )"
                "  SELECT cr.sequence_hash, c.chunk_index, c.chunk_data"
                "  FROM read_parquet(?) c"
                "  JOIN canonical cr ON c.read_id = cr.read_id"
                f") TO '{part_out}' ({PARQUET_OPTS_CHUNKED})",
                [str(inputs.fasta_path)],
            )

            conn.execute("DROP TABLE hashed")
        success = True
    finally:
        # On any failure path (interrupted COPY, DuckDB OOM, ...) remove
        # partial Parquets so the launcher's manifest walker doesn't
        # promote a half-written result as the step's output. Best-effort:
        # a hard SIGKILL leaves them behind, but the runner allocates a
        # fresh attempt-N+1 workspace on retry so it doesn't cascade.
        if not success:
            manifest_path.unlink(missing_ok=True)
            shutil.rmtree(reference_sequence_chunks_dir, ignore_errors=True)

    return {
        "manifest": manifest_path,
        "reference_sequence_chunks": reference_sequence_chunks_dir,
    }
