"""Native job: canonicalize, hash, dedup, and chunk an uploaded
sequence Parquet.

The CLI's DoPut writes an `upload.parquet` with shape
`(read_id VARCHAR, sequence VARCHAR)` to the data plane's staging area.
This step reads that file and produces the three Parquets the workflow
needs to mint features and load reference data:

  - `manifest.parquet`           — `(read_id, sequence_hash, length)`
    One row per upload read. Carries the read's source identifier and
    the canonical-form sequence_hash it maps to. Downstream consumers
    (load step taxonomy/phylogeny/placements) JOIN on `read_id`.
  - `reference_sequence.parquet` — `(sequence_hash, sequence_length_bp)`
    One row per UNIQUE canonical hash. Drives `mint-features`'s per-hash
    feature_idx allocation; `write-membership` uses the same set to
    populate `qiita.reference_membership`.
  - `reference_sequence_chunks.parquet` — `(sequence_hash, chunk_index,
    chunk_data)` One row per 64 KB chunk per unique canonical sequence;
    the data plane registers this into DuckLake for query-time retrieval.

**Canonical form.** A sequence and its reverse complement describe the
same molecular entity; both must collapse to one feature. The canonical
form is `LEAST(upper(seq), sequence_dna_reverse_complement(upper(seq)))`
— the lexicographically smaller of the two strands. The hash is `md5()`
over that, stored as DuckDB UUID (16 bytes) to match the wire-side
`sequence_hash` and `feature_idx` types — no VARCHAR md5 hexstring is
written anywhere (per the project's hash-storage rule).

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
    CHUNK_SIZE,
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

    `upload_path` is the workflow-declared input — the runner resolves
    `fasta_upload_idx` → `staging_path_for(upload_staging_root, idx)`
    before invoking this step (see Cycle 4 of the upload-doput plan).

    `reference_idx` and `work_ticket_idx` are framework-injected scope
    scalars merged by `flatten_native_inputs`. Both are accepted (typed)
    even though this step doesn't consume them — declaring them on the
    Inputs model keeps the contract explicit and matches the
    fastq_to_parquet convention; without the declaration Pydantic
    silently drops them on `model_validate`, which would hide a mis-wired
    scope dispatch.
    """

    upload_path: Path
    reference_idx: int
    work_ticket_idx: int


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """Read the staged upload Parquet; emit manifest + reference_sequence
    + reference_sequence_chunks. See module docstring for the pipeline."""
    if not inputs.upload_path.exists():
        raise FileNotFoundError(f"upload parquet not found: {inputs.upload_path}")

    workspace.mkdir(parents=True, exist_ok=True)
    manifest_path = workspace / "manifest.parquet"
    reference_sequence_path = workspace / "reference_sequence.parquet"
    reference_sequence_chunks_path = workspace / "reference_sequence_chunks.parquet"
    manifest_out = validate_parquet_path(manifest_path)
    reference_sequence_out = validate_parquet_path(reference_sequence_path)
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

            # Canonical form is materialized once per read in a nested
            # subquery, then md5'd in the outer SELECT — keeps
            # `sequence_dna_reverse_complement(upper(sequence))` from being
            # evaluated twice (once for `canonical_sequence`, once
            # inside `md5(LEAST(…))`). On a 5M-row reference upload
            # the inner-then-outer shape is ~2x faster than restating
            # the call in both columns; DuckDB does not CSE non-trivial
            # function chains across top-level SELECT items.
            conn.execute(
                "CREATE TEMP TABLE upload_canonical AS "
                "SELECT "
                "  read_id,"
                "  canonical_sequence,"
                "  md5(canonical_sequence)::uuid AS sequence_hash,"
                "  CAST(length(sequence) AS BIGINT) AS sequence_length_bp "
                "FROM ("
                "  SELECT "
                "    read_id,"
                "    sequence,"
                "    LEAST("
                "      upper(sequence),"
                "      sequence_dna_reverse_complement(upper(sequence))"
                "    ) AS canonical_sequence"
                "  FROM read_parquet(?)"
                ")",
                [str(inputs.upload_path)],
            )

            # manifest.parquet — one row per upload read.
            # `sequence_length_bp` (not `length`) matches the column
            # name used in reference_sequence.parquet so a downstream
            # JOIN on a common column doesn't trip over naming drift;
            # mint-features / write-membership (Cycle 4) read this name
            # directly from both files.
            conn.execute(
                "COPY ("
                "  SELECT read_id, sequence_hash, sequence_length_bp"
                "  FROM upload_canonical"
                "  ORDER BY sequence_hash"
                f") TO '{manifest_out}' ({PARQUET_OPTS})"
            )

            # reference_sequence.parquet — one row per UNIQUE canonical
            # hash. Length is taken from `length(ANY_VALUE(canonical_sequence))`
            # so it always tracks the canonical form rather than the
            # source read length; for the current canonicalization
            # (strand-fold only) these are equal, but any future
            # normalization step (gap stripping, case folding beyond
            # upper) would silently desync the two if we picked from
            # the source-length column instead.
            conn.execute(
                "COPY ("
                "  SELECT "
                "    sequence_hash,"
                "    CAST(length(ANY_VALUE(canonical_sequence)) AS BIGINT) AS sequence_length_bp"
                "  FROM upload_canonical"
                "  GROUP BY sequence_hash"
                "  ORDER BY sequence_hash"
                f") TO '{reference_sequence_out}' ({PARQUET_OPTS})"
            )

            # reference_sequence_chunks.parquet — 64 KB chunks over each
            # unique canonical sequence. The chunking macro mirrors the
            # legacy `_write_sequence_chunks` shape in backends/local.py
            # (removed in Cycle 4) — same list_transform + UNNEST pattern,
            # same CHUNK_SIZE, same ROW_GROUP_SIZE. Keyed by
            # sequence_hash here (vs feature_idx in the legacy path)
            # because mint-features hasn't run yet.
            conn.execute(
                "CREATE OR REPLACE MACRO chunk_seq(str) AS "
                "list_transform("
                f"  range(1, CAST(length(str) + 1 AS BIGINT), {CHUNK_SIZE}),"
                "  lambda idx : {"
                f"    'chunk_index': CAST((idx - 1) / {CHUNK_SIZE} AS INTEGER),"
                f"    'chunk_data': substring(str, CAST(idx AS BIGINT), {CHUNK_SIZE})"
                "  }"
                ")"
            )
            conn.execute(
                "COPY ("
                "  WITH unique_seqs AS ("
                "    SELECT sequence_hash, ANY_VALUE(canonical_sequence) AS canonical_sequence"
                "    FROM upload_canonical"
                "    GROUP BY sequence_hash"
                "  ),"
                "  unnested AS ("
                "    SELECT sequence_hash, UNNEST(chunk_seq(canonical_sequence)) AS chunk"
                "    FROM unique_seqs"
                "  )"
                "  SELECT sequence_hash, chunk.chunk_index, chunk.chunk_data"
                "  FROM unnested"
                "  ORDER BY sequence_hash, chunk.chunk_index"
                f") TO '{reference_sequence_chunks_out}' ({PARQUET_OPTS_CHUNKED})"
            )

            conn.execute("DROP TABLE upload_canonical")
        success = True
    finally:
        # Clean up the DuckDB spill dir BEFORE returning so the SLURM
        # launcher's manifest walker (running after execute()) sees only
        # the three output Parquets.
        shutil.rmtree(duckdb_tmp, ignore_errors=True)
        # On any failure path (interrupted COPY, DuckDB OOM, ...) remove
        # partial Parquets so the launcher's manifest walker doesn't
        # promote a half-written result as the step's output. Best-effort:
        # a hard SIGKILL leaves them behind, but the runner allocates a
        # fresh attempt-N+1 workspace on retry so it doesn't cascade.
        if not success:
            for partial in (
                manifest_path,
                reference_sequence_path,
                reference_sequence_chunks_path,
            ):
                partial.unlink(missing_ok=True)

    return {
        "manifest": manifest_path,
        "reference_sequence": reference_sequence_path,
        "reference_sequence_chunks": reference_sequence_chunks_path,
    }
