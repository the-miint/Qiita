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
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    PARQUET_OPTS,
    PARQUET_OPTS_CHUNKED,
    ensure_miint_installed,
    open_conn,
)

YAML_STEP_NAME = "hash_sequences"

# DuckDB resource caps mirror fastq_to_parquet (#38 will plumb these
# from JobParams.baseline_resources). hash_sequences is the cheapest
# of the native jobs — string canonicalize + md5 + dedup — but the
# chunked write of a multi-GB reference can spill, so memory_limit and
# a workspace-local temp_directory are still load-bearing.
_DUCKDB_MAX_MEMORY_GB = 7
_DUCKDB_MAX_THREADS = 2


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
    reference_sequence_chunks_path = workspace / "reference_sequence_chunks.parquet"
    manifest_out = validate_parquet_path(manifest_path)
    reference_sequence_chunks_out = validate_parquet_path(reference_sequence_chunks_path)
    duckdb_tmp = workspace / ".duckdb_tmp"
    duckdb_tmp.mkdir(parents=True, exist_ok=True)

    await ensure_miint_installed()

    success = False
    try:
        with open_conn() as conn:
            conn.execute("LOAD miint;")
            conn.execute(f"SET memory_limit='{_DUCKDB_MAX_MEMORY_GB}GB'")
            conn.execute(f"SET threads={_DUCKDB_MAX_THREADS}")
            # preserve_insertion_order=false is REQUIRED for the
            # chunked-sequence write (see feedback_sequence_chunking) —
            # without it DuckDB buffers row groups in memory rather than
            # flushing eagerly, OOMing on genome-heavy uploads.
            conn.execute("SET preserve_insertion_order=false")
            conn.execute(f"SET temp_directory='{duckdb_tmp}'")

            # Reconstruct each read's full sequence by ordered chunk
            # concatenation. DuckDB's vectorized aggregation streams this
            # group-by-group; peak memory per group ≈ one sequence's
            # length (capped at the upload's longest record).
            conn.execute(
                "CREATE TEMP TABLE per_read AS "
                "SELECT "
                "  read_id, "
                "  string_agg(chunk_data, '' ORDER BY chunk_index) AS sequence "
                "FROM read_parquet(?) "
                "GROUP BY read_id",
                [str(inputs.fasta_path)],
            )

            # Canonical hash = LEAST(forward_md5, reverse_md5). The bytes
            # stay as the client uploaded them; the canonical identity
            # lives in the hash alone.
            conn.execute(
                "CREATE TEMP TABLE hashed AS "
                "SELECT "
                "  read_id, "
                "  LEAST("
                "    md5(upper(sequence))::uuid,"
                "    md5(sequence_dna_reverse_complement(upper(sequence)))::uuid"
                "  ) AS sequence_hash, "
                "  CAST(length(sequence) AS BIGINT) AS sequence_length_bp "
                "FROM per_read"
            )
            conn.execute("DROP TABLE per_read")

            # manifest.parquet — one row per upload read.
            conn.execute(
                "COPY ("
                "  SELECT read_id, sequence_hash, sequence_length_bp"
                "  FROM hashed"
                f") TO '{manifest_out}' ({PARQUET_OPTS})"
            )

            # reference_sequence_chunks.parquet — relabel upload chunks
            # by sequence_hash. When two reads share a canonical hash
            # (sequence + its reverse complement) we keep ONE — the
            # lex-smaller read_id, deterministically. ORDER BY at write
            # time gives the chunks of each hash on-disk locality so a
            # downstream `WHERE sequence_hash = X` doesn't scan the
            # whole file.
            conn.execute(
                "COPY ("
                "  WITH canonical_read AS ("
                "    SELECT DISTINCT ON (sequence_hash) read_id, sequence_hash"
                "    FROM hashed"
                "    ORDER BY sequence_hash, read_id"
                "  )"
                "  SELECT cr.sequence_hash, c.chunk_index, c.chunk_data"
                "  FROM read_parquet(?) c"
                "  JOIN canonical_read cr ON c.read_id = cr.read_id"
                "  ORDER BY cr.sequence_hash, c.chunk_index"
                f") TO '{reference_sequence_chunks_out}' ({PARQUET_OPTS_CHUNKED})",
                [str(inputs.fasta_path)],
            )

            conn.execute("DROP TABLE hashed")
        success = True
    finally:
        # Clean up the DuckDB spill dir BEFORE returning so the SLURM
        # launcher's manifest walker (running after execute()) sees only
        # the output Parquets.
        shutil.rmtree(duckdb_tmp, ignore_errors=True)
        # On any failure path (interrupted COPY, DuckDB OOM, ...) remove
        # partial Parquets so the launcher's manifest walker doesn't
        # promote a half-written result as the step's output. Best-effort:
        # a hard SIGKILL leaves them behind, but the runner allocates a
        # fresh attempt-N+1 workspace on retry so it doesn't cascade.
        if not success:
            for partial in (manifest_path, reference_sequence_chunks_path):
                partial.unlink(missing_ok=True)

    return {
        "manifest": manifest_path,
        "reference_sequence_chunks": reference_sequence_chunks_path,
    }
