"""Native job: hash the long-read-assembly container's assembled contigs into the
same manifest + hash-keyed-chunks shape `hash_sequences` produces, plus a bin_map.

Head of the assembly-storage tail. The heavy tools ran in containers; this native
step reads their circular-genome (LCG) and refined-MAG FASTA outputs and produces
the inputs the SHARED reference-load machinery consumes downstream
(mint-features -> write-assembly-membership -> assembly_load):

  - `manifest.parquet` — `(read_id, sequence_hash, sequence_length_bp)`, one row
    per contig. `read_id` is a SYNTHETIC globally-unique id
    `kind || ':' || bin_id || ':' || contig_id` (a raw contig id can repeat across
    bins/files, so it is never keyed on alone). This is the exact shape
    `mint-features` consumes and `build_feature_id_map` re-keys.
  - `assembly_chunks/` — a DIRECTORY of `part_*.parquet`
    `(sequence_hash, chunk_index, chunk_data)`, the hash-keyed 64 KB chunks
    `write_feature_sequence_chunks` re-keys to feature_idx. Identical contigs (same
    canonical bytes) collapse to one set of chunks (DISTINCT ON sequence_hash),
    exactly like `hash_sequences`.
  - `bin_map.parquet` — `(read_id, kind, bin_id)`, the per-contig bin membership
    `write-assembly-membership` / `assembly_load` join against.

**Shared canonical identity.** `sequence_hash` is
`qiita_common.chunking.canonical_sequence_hash_expr` — the SAME expression
`hash_sequences` uses — so an assembled contig whose bytes match a reference
sequence mints the IDENTICAL feature_idx (both dedup against qiita.feature).

**Parsing + chunking are done in DuckDB, not Python.** FASTA records are read with
miint's `read_fastx` (native parser; `.gz` transparent; `read_id` is the header's
first token) and split into 64 KB chunks with miint's native `sequence_split`
(`UNNEST`ed) — never a hand-rolled parser. `read_fastx` returns `filepath` exactly
as passed, so a small in-memory `file_meta(filepath, kind, bin_id)` table (built
from the globbed paths) JOINs the scan back to each contig's kind + bin without
fragile filename regex.

0 contigs (no LCG, no MAG under either dir) is a terminal no-data outcome
(`StepNoData`), mirroring qp-pacbio's skip-on-empty branch — not a
failure.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from pydantic import BaseModel
from qiita_common.backend_failure import StepNoData
from qiita_common.chunking import canonical_sequence_hash_expr, sequence_split_expr
from qiita_common.duckdb_miint import is_empty_sequence_file
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    PARQUET_OPTS,
    PARQUET_OPTS_CHUNKED,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)
from ._feature_load import KIND_LCG, KIND_MAG

YAML_STEP_NAME = "assembly_hash"

# FASTA extensions accepted for contig files (both plain and gzip-compressed).
_FASTA_GLOBS = ("*.fna", "*.fna.gz", "*.fa", "*.fa.gz", "*.fasta", "*.fasta.gz")

# DuckDB resource caps. `_DUCKDB_MEMORY_GB` is the OFF-SLURM fallback (local
# backend / tests); under SLURM the limit tracks the real cgroup via
# `resolve_duckdb_memory_gb()`. DuckDB owns the whole box here (no in-process
# co-consumer), so it gets the allocation minus headroom.
_DUCKDB_MEMORY_GB = 6
_DUCKDB_THREADS = 4

# Byte budget for each `read_fastx` batch — caps the read-side vector so a run of
# multi-MB contig records can't materialise a giant chunk before the chunker runs.
_READ_FASTX_MAX_BATCH_BYTES = "128MB"


class Inputs(BaseModel):
    """Typed input contract for assembly_hash.

    `genomes_dir` (holds `LCG/`) and `refined_bins_dir` (MAG bins) are the upstream
    container steps' outputs. `prep_sample_idx` / `work_ticket_idx` are
    framework-injected scope scalars (declared for an explicit contract even though
    this step doesn't read them — sequences are run-agnostic, so no processing_idx
    is needed here).
    """

    genomes_dir: Path
    refined_bins_dir: Path
    prep_sample_idx: int
    work_ticket_idx: int


def _local_id(path: Path) -> str:
    """The bin_id for a FASTA file: its stem with any FASTA suffix (and a trailing
    `.gz`) stripped — `bin.3.fa.gz` -> `bin.3`."""
    name = path.name
    if name.endswith(".gz"):
        name = name[: -len(".gz")]
    return Path(name).stem  # strips the final .fna/.fa/.fasta


def _fasta_files(base: Path) -> list[Path]:
    """Every FASTA file directly under `base` (sorted, deduped). Returns [] if the
    directory is absent (a legitimately empty upstream step)."""
    if not base.is_dir():
        return []
    found: list[Path] = []
    for pattern in _FASTA_GLOBS:
        found.extend(base.glob(pattern))
    return sorted(set(found))


def _file_meta(genomes_dir: Path, refined_bins_dir: Path) -> list[tuple[str, str, str]]:
    """Build the `(filepath, kind, bin_id)` rows for every non-empty contig FASTA:
    LCG under `<genomes_dir>/LCG`, MAG under `<refined_bins_dir>`. Empty files are
    dropped (`read_fastx` raises on a 0-record input, and one empty path aborts the
    whole `VARCHAR[]` scan)."""
    meta: list[tuple[str, str, str]] = []
    for base, kind in ((genomes_dir / "LCG", KIND_LCG), (refined_bins_dir, KIND_MAG)):
        for path in _fasta_files(base):
            if is_empty_sequence_file(path):
                continue
            meta.append((str(path), kind, _local_id(path)))
    return meta


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    meta = _file_meta(inputs.genomes_dir, inputs.refined_bins_dir)

    # A sample that assembled nothing binnable AND has no circular genome is a
    # terminal no-data outcome — no contigs to hash, not a failure.
    if not meta:
        raise StepNoData(
            step_name=YAML_STEP_NAME,
            reason=(
                f"no contigs to hash for prep_sample_idx={inputs.prep_sample_idx} "
                f"(no LCG under {inputs.genomes_dir}/LCG, no MAG under "
                f"{inputs.refined_bins_dir})"
            ),
        )
    paths = [row[0] for row in meta]

    workspace.mkdir(parents=True, exist_ok=True)
    manifest_path = workspace / "manifest.parquet"
    bin_map_path = workspace / "bin_map.parquet"
    # assembly_chunks is a DIRECTORY of part_*.parquet (the shape
    # write_feature_sequence_chunks re-keys); one part here, kept a directory so
    # register-files / the re-key treat it as a multi-file DuckLake table.
    chunks_dir = workspace / "assembly_chunks"
    chunks_dir.mkdir(parents=True, exist_ok=True)
    manifest_out = validate_parquet_path(manifest_path)
    bin_map_out = validate_parquet_path(bin_map_path)
    chunks_part_out = validate_parquet_path(chunks_dir / "part_00000.parquet")

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS),
                threads=_DUCKDB_THREADS,
            )

            # file_meta bridges each read_fastx row back to its kind + bin_id.
            # read_fastx returns `filepath` verbatim (see module docstring), so the
            # JOIN key is exact — no filename regex.
            conn.execute(
                "CREATE TEMP TABLE file_meta (filepath VARCHAR, kind VARCHAR, bin_id VARCHAR)"
            )
            conn.executemany("INSERT INTO file_meta VALUES (?, ?, ?)", meta)

            # Pass 1 — per-contig metadata (kind, bin_id, synthetic read_id, hash,
            # length). No sequence bytes retained, so manifest + bin_map cost a tiny
            # table. The synthetic read_id disambiguates a contig id reused across
            # bins/files. `sequence_hash` is the SHARED canonical hash so identical
            # bytes mint the same feature_idx as a reference sequence.
            conn.execute(
                "CREATE TEMP TABLE contig AS "
                "SELECT "
                "  fm.kind AS kind, "
                "  fm.bin_id AS bin_id, "
                "  fm.kind || ':' || fm.bin_id || ':' || rf.read_id AS read_id, "
                f"  {canonical_sequence_hash_expr('rf.sequence1')} AS sequence_hash, "
                "  CAST(length(rf.sequence1) AS BIGINT) AS sequence_length_bp "
                f"FROM read_fastx(?, max_batch_bytes:='{_READ_FASTX_MAX_BATCH_BYTES}', "
                "  include_filepath:=true) rf "
                "JOIN file_meta fm ON rf.filepath = fm.filepath",
                [paths],
            )

            conn.execute(
                "COPY (SELECT read_id, sequence_hash, sequence_length_bp FROM contig) "
                f"TO '{manifest_out}' ({PARQUET_OPTS})"
            )
            conn.execute(
                "COPY (SELECT read_id, kind, bin_id FROM contig) "
                f"TO '{bin_map_out}' ({PARQUET_OPTS})"
            )

            # Pass 2 — hash-keyed chunks. Re-scan every FASTA, dedup by canonical
            # sequence_hash (identical contigs collapse to one — the lex-smaller
            # contig id wins deterministically), and stream 64 KB `sequence_split`
            # chunks straight to Parquet. Bytes are chunked exactly as read; the
            # canonical identity lives in the hash. Mirrors hash_sequences' relabel.
            conn.execute(
                "COPY ("
                "  WITH per_contig AS ("
                "    SELECT rf.read_id AS contig_id, "
                f"      {canonical_sequence_hash_expr('rf.sequence1')} AS sequence_hash, "
                "      rf.sequence1 AS sequence "
                f"    FROM read_fastx(?, max_batch_bytes:='{_READ_FASTX_MAX_BATCH_BYTES}') rf"
                "  ), "
                "  dedup AS ("
                "    SELECT DISTINCT ON (sequence_hash) sequence_hash, sequence "
                "    FROM per_contig ORDER BY sequence_hash, contig_id"
                "  ) "
                "  SELECT sequence_hash, c.chunk_index, c.chunk_data FROM ("
                f"    SELECT sequence_hash, UNNEST({sequence_split_expr('sequence')}) AS c "
                "    FROM dedup"
                "  )"
                f") TO '{chunks_part_out}' ({PARQUET_OPTS_CHUNKED})",
                [paths],
            )
        success = True
    finally:
        # On any failure remove partial outputs so the launcher's manifest walker
        # can't promote a half-written result as this step's output.
        if not success:
            manifest_path.unlink(missing_ok=True)
            bin_map_path.unlink(missing_ok=True)
            shutil.rmtree(chunks_dir, ignore_errors=True)

    return {"manifest": manifest_path, "assembly_chunks": chunks_dir, "bin_map": bin_map_path}
