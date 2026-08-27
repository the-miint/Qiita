"""Native job: stage many local FASTA files into one chunked Parquet.

The local-ingest front-end for reference-add. Where the remote path streams a
single FASTA over Arrow Flight DoPut and lands an `upload.parquet`, this job
reads a *manifest* of absolute FASTA paths already resident on the compute host
and produces the **same** chunked shape:

  `fasta_chunks/part_*.parquet` — `(read_id VARCHAR, chunk_index INTEGER,
  chunk_data VARCHAR)`

That is exactly what `hash_sequences` consumes, so the rest of the reference-add
pipeline (`hash_sequences` → `mint-features` → `write-membership` → `load` →
`register-files` → …) runs unchanged. This job is the *only* new step on the
local path; everything downstream is reused verbatim. The host-reference index
builds (`build_rype_index`, `build_minimap2_index`) consume the feature-keyed
chunks the `load` step re-emits — there is no minimap2-specific side channel
here.

**Parsing + chunking are done in DuckDB, not Python.** FASTA records are read
with miint's `read_fastx` table function (native parser; `.gz` transparent;
`read_id` is the header's first token, matching the remote path), and the 64 KB
chunking is miint's native `sequence_split` (`UNNEST`ed) — never a hand-rolled
Python parser. `read_fastx` accepts a `VARCHAR[]` of paths, so a batch of the
manifest is one streaming scan — no per-file Python loop and no sequence bytes
through Python. The manifest is fed in `_MANIFEST_BATCH_FILES`-sized batches
because a single call holds every path's file handle open until it ends
(duckdb-miint#260). The scan runs twice: pass 1 keeps only `(read_id, length,
filepath)` for the sanity checks (so the full sequences are never materialised —
the old per-file temp table was the dominant spill source), accumulating into
one temp table so the checks still see the whole manifest; pass 2 streams
read → split → Parquet without landing sequences in a table, one Parquet part
per batch. Re-reading each FASTA (2× decompress) is deliberate — far cheaper
than spilling hundreds of genomes. Empty files are pre-filtered out
(`read_fastx` raises on a 0-record input, and one empty path aborts the batch
it lands in).

**Bounded memory is config, not algorithm.** `read_fastx(..., max_batch_bytes)`
caps each read batch by bytes so a multi-MB genome record (GG2 reaches ~21 MB)
can't form a giant vector; `apply_duckdb_settings` sets `memory_limit` +
`temp_directory` (operators spill, not OOM) + `preserve_insertion_order=false`;
and `PARQUET_OPTS_CHUNKED`'s `ROW_GROUP_SIZE` bounds the write buffer. At
~300 GB the combined-Parquet ingest plus the unchanged quadratic
`hash_sequences`/`load` is still a deferred performance effort, but the external
contract (this step's `fasta_path` output wired into `hash_sequences`) is stable
across both phases.

**read_id is the global join key.** read_id is the genome_map join key and is
globally unique by contract; a duplicate (across files or within one file) is a
data error, never silently namespaced — fail fast with `ValueError` (which
`run_native_job` maps to BAD_INPUT). A header with no sequence body is likewise
bad data and fails fast.

Under SLURM the manifest and every listed FASTA must be on the shared filesystem
visible from the compute node (bind mounts expose host paths, they do not copy),
which is why every path must be absolute — the workflow YAML's `pattern:"^/"`
enforces the same at the wire.
"""

from __future__ import annotations

import logging
import shutil
from itertools import batched
from pathlib import Path

from pydantic import BaseModel
from qiita_common.chunking import (
    SOFT_MASK_WARNING,
    sequence_split_expr,
    soft_masked_expr,
)
from qiita_common.duckdb_miint import is_empty_sequence_file
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    PARQUET_OPTS_CHUNKED,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)

YAML_STEP_NAME = "stage_local_fasta"

# DuckDB resource caps for this step. `_DUCKDB_MEMORY_GB` is the OFF-SLURM
# fallback (local backend / tests), sized to the YAML baseline (mem_gb=32 minus
# ~2 GB headroom). Under SLURM the limit instead tracks the real cgroup via
# `resolve_duckdb_memory_gb()` (SLURM_MEM_PER_NODE), so a `--mem-gb` override
# actually reaches DuckDB — a genome-scale FASTA OOM'd here at the old
# fixed 7 GB no matter how large the allocation was. DuckDB owns the whole box in
# this step (no in-process co-consumer), so it gets the allocation minus headroom.
_DUCKDB_MEMORY_GB = 30
_DUCKDB_THREADS = 8

# Byte budget for each `read_fastx` batch. Caps the read-side vector so a run of
# multi-MB genome records can't materialise a 2048-row × 21 MB chunk before the
# chunking operator runs. One of three memory levers — the others are the
# `memory_limit`/`temp_directory` spill (apply_duckdb_settings) and the write
# buffer (`ROW_GROUP_SIZE` in PARQUET_OPTS_CHUNKED).
_READ_FASTX_MAX_BATCH_BYTES = "128MB"

# How many manifest paths go into ONE `read_fastx` call. `read_fastx` opens a
# file per path and holds that handle until the whole call ends — a reader is
# never released when its file is exhausted — so open descriptors and zlib
# state grow with the number of paths in a single call, not with the bytes read
# (duckdb-miint#260). Measured on gzipped WoL3 genomes: ~1 descriptor and
# ~464 KB of RSS retained per path, both linear. A 196,062-path manifest
# therefore needs ~196k descriptors and ~89 GB before the call can finish, and
# a 64 GB run died at path 131,070 with `Empty file` — which is what
# `read_fastx` reports when an open fails. Batching bounds both by the batch
# instead: 500 paths holds ~500 descriptors and ~230 MB. 500 and not 1000
# because the descriptor ceiling depends on how the job was launched — an
# interactive `srun` on a compute node reports a 1024 soft limit, while the
# batch job that died at path 131,070 evidently had ~131,072 — so the batch has
# to stay inside the smaller of the two with room for DuckDB's own descriptors.
# Pass `paths` whole again once duckdb-miint#260 ships.
_MANIFEST_BATCH_FILES = 500

_LOG = logging.getLogger(__name__)

# Cap on how many offending read_ids we name in a fail-fast error so a
# pathologically bad manifest doesn't build a multi-megabyte message.
_MAX_REPORT = 20


class Inputs(BaseModel):
    """Typed input contract for stage_local_fasta.

    `fasta_manifest_path` is the workflow-declared input (action_context →
    `inputs:[fasta_manifest_path]`): an absolute path to a text file listing
    one absolute FASTA path per line (blank lines and `#` comments ignored).

    `reference_idx` and `work_ticket_idx` are framework-injected scope scalars
    merged by `flatten_native_inputs` (`SCOPE_SCALARS_BY_KIND[REFERENCE]` plus
    the always-on work_ticket_idx). This step doesn't consume them, but
    declaring them — as hash_sequences does — keeps the contract explicit;
    without the declaration Pydantic would silently drop them and hide a
    mis-wired scope dispatch.
    """

    fasta_manifest_path: Path
    reference_idx: int
    work_ticket_idx: int


def _read_manifest(manifest_path: Path) -> list[Path]:
    """Parse the manifest into validated absolute FASTA paths.

    One absolute path per line; blank lines and `#` comments are skipped. Every
    path must be absolute and an existing file (mirroring bcl_convert_prep's
    guards). Raises ValueError on the first bad entry or if no real path lines
    remain.
    """
    fasta_paths: list[Path] = []
    for raw in manifest_path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        entry = Path(line)
        if not entry.is_absolute():
            raise ValueError(f"manifest entry must be absolute, got {line!r}")
        if not entry.exists() or not entry.is_file():
            raise ValueError(f"FASTA file in manifest not found or not a file: {entry}")
        fasta_paths.append(entry)

    if not fasta_paths:
        raise ValueError(f"manifest lists zero FASTA files: {manifest_path}")
    return fasta_paths


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """Read every manifest FASTA with batched `read_fastx(VARCHAR[])` scans; emit
    the chunked Parquet parts. Validates the manifest and each listed file, stages
    only `(read_id, length, filepath)` into a small temp table, fails fast on an
    empty-body record or a duplicate read_id, then re-scans and COPYs the
    `sequence_split`/`UNNEST` chunking to one Parquet part per batch without
    materialising the sequences. Returns `{"fasta_path": <directory>}`.
    """
    if not inputs.fasta_manifest_path.is_absolute():
        raise ValueError(
            f"fasta_manifest_path must be absolute, got {inputs.fasta_manifest_path!r}"
        )
    if not inputs.fasta_manifest_path.exists() or not inputs.fasta_manifest_path.is_file():
        raise ValueError(f"FASTA manifest not found or not a file: {inputs.fasta_manifest_path}")

    fasta_paths = _read_manifest(inputs.fasta_manifest_path)

    workspace.mkdir(parents=True, exist_ok=True)
    # A DIRECTORY of `part_*.parquet`, not one file: Parquet has no append, so a
    # batched pass 2 writes a part per batch. `read_parquet` takes the directory
    # itself, and hash_sequences passes `fasta_path` straight into it, so the
    # consumer contract is unchanged. Same shape hash_sequences emits for
    # `reference_sequence_chunks`.
    fasta_dir = workspace / "fasta_chunks"
    fasta_dir.mkdir(parents=True, exist_ok=True)

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS),
                threads=_DUCKDB_THREADS,
            )

            # read_fastx takes a VARCHAR[] of paths — one streaming scan per
            # batch, no per-file Python loop and no full-sequence temp table.
            # Pre-filter empties: read_fastx raises "Empty file" on a 0-record
            # input, and one empty file aborts the batch it lands in.
            paths = [str(p) for p in fasta_paths if not is_empty_sequence_file(p)]
            if not paths:
                raise ValueError(
                    f"manifest lists only empty FASTA files: {inputs.fasta_manifest_path}"
                )

            # Pass 1 — lengths + filepath only; no sequence bytes retained, so
            # the sanity checks (empty-body, dup read_id) cost a tiny table
            # instead of materialising every genome (the old spill source).
            # `max_batch_bytes` caps the read-side vector for genome-scale
            # records; `include_filepath` lets the dup report name the file.
            # One statement per batch, all appending to the SAME temp table, so
            # the checks below still run once over the whole manifest. The
            # accumulated table holds exactly the rows the single-call version
            # built — four scalars per read, no sequences.
            sanity_select = (
                "SELECT read_id, length(sequence1) AS length, filepath, "
                f"{soft_masked_expr('sequence1')} AS soft_masked "
                f"FROM read_fastx(?, max_batch_bytes:='{_READ_FASTX_MAX_BATCH_BYTES}', "
                "include_filepath:=true)"
            )
            for index, batch in enumerate(batched(paths, _MANIFEST_BATCH_FILES)):
                statement = (
                    f"CREATE TEMP TABLE read_sanity AS {sanity_select}"
                    if index == 0
                    else f"INSERT INTO read_sanity {sanity_select}"
                )
                conn.execute(statement, [list(batch)])

            # Empty-body records: a named read with no sequence is bad data.
            # read_fastx surfaces them as length-0 rows, so detect in SQL.
            empties = conn.execute(
                "SELECT read_id FROM read_sanity WHERE length = 0 ORDER BY read_id LIMIT ?",
                [_MAX_REPORT],
            ).fetchall()
            if empties:
                names = ", ".join(row[0] for row in empties)
                raise ValueError(f"FASTA record(s) with a header but no sequence body: {names}")

            # Duplicate read_id: the global genome_map join key must be unique.
            # One row per read here, so a plain GROUP BY HAVING count(*) > 1.
            dupes = conn.execute(
                "SELECT read_id, string_agg(DISTINCT filepath, ', ') AS files "
                "FROM read_sanity "
                "GROUP BY read_id HAVING count(*) > 1 "
                "ORDER BY read_id LIMIT ? ",
                [_MAX_REPORT],
            ).fetchall()
            if dupes:
                names = ", ".join(row[0] for row in dupes)
                files = ", ".join(row[1] for row in dupes)
                raise ValueError(
                    "duplicate read_id in FASTA manifest — read_id is the global "
                    "genome_map join key and must be unique.\n"
                    f"read_ids -> {names}\n"
                    f"files -> {files}"
                )

            # The flag rides pass 1's existing scan, so this costs no extra read —
            # unlike `reference load`, which pays a second bounded one. What the
            # masking costs is on `SOFT_MASK_WARNING`.
            masked = conn.execute(
                "SELECT DISTINCT filepath FROM read_sanity WHERE soft_masked "
                "ORDER BY filepath LIMIT ?",
                [_MAX_REPORT],
            ).fetchall()
            if masked:
                _LOG.warning(SOFT_MASK_WARNING, ", ".join(row[0] for row in masked))

            # Pass 2 — chunk + write in one streaming COPY per batch using
            # miint's native `sequence_split` (shared chunker; see
            # qiita_common.chunking). Each sequence splits into 64 KB pieces in a
            # single linear pass; UNNEST gives one row per chunk. Sequences flow
            # read -> split -> parquet without ever landing in a table. This
            # re-reads each FASTA (2x decompress total) — deliberate: far cheaper
            # than materialising hundreds of genomes only to spill them. The
            # batches are the same slices pass 1 used, so a part's contents are
            # the batch's genomes; nothing downstream depends on which part a
            # read lands in (`preserve_insertion_order=false` already leaves row
            # order unspecified).
            for index, batch in enumerate(batched(paths, _MANIFEST_BATCH_FILES)):
                part_out = validate_parquet_path(fasta_dir / f"part_{index:05d}.parquet")
                conn.execute(
                    "COPY ("
                    "  SELECT read_id, c.chunk_index, c.chunk_data FROM ("
                    f"    SELECT read_id, UNNEST({sequence_split_expr('sequence1')}) AS c "
                    f"    FROM read_fastx(?, max_batch_bytes:='{_READ_FASTX_MAX_BATCH_BYTES}')"
                    "  )"
                    f") TO '{part_out}' ({PARQUET_OPTS_CHUNKED})",
                    [list(batch)],
                )
        success = True
    finally:
        # On any failure (bad manifest entry, empty-body, dup read_id,
        # interrupted COPY) remove the parts directory so the launcher's manifest
        # walker (which runs after execute()) can't promote a partial set of
        # parts as this step's output. A batched pass 2 makes that a real
        # possibility: batch 0 can be on disk when batch 7 raises.
        if not success:
            shutil.rmtree(fasta_dir, ignore_errors=True)

    return {"fasta_path": fasta_dir}
