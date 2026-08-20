"""Native job: hash the long-read-assembly container's assembled contigs into the
same manifest + hash-keyed-chunks shape `hash_sequences` produces, plus a bin_map.

Head of the assembly-storage tail. The heavy tools ran in containers; this native
step reads their circular-genome (LCG), refined-MAG, and unbinned-residue FASTA
outputs and produces the inputs the SHARED reference-load machinery consumes
downstream (mint-features -> write-assembly-membership -> assembly_load):

  - `manifest.parquet` — `(read_id, sequence_hash, sequence_length_bp)`, one row
    per contig. `read_id` is a SYNTHETIC id
    `kind || ':' || bin_id || ':' || sequence_index` — a contig's own id repeats
    across bins and files, so it is not an identity. This is the exact shape
    `mint-features` consumes and `build_feature_id_map` re-keys.
  - `assembly_chunks/` — a DIRECTORY of `part_*.parquet`
    `(sequence_hash, chunk_index, chunk_data)`, the hash-keyed 64 KB chunks
    `write_feature_sequence_chunks` re-keys to feature_idx. Identical contigs (same
    canonical bytes) collapse to one set of chunks (DISTINCT ON sequence_hash),
    exactly like `hash_sequences`.
  - `bin_map.parquet` — `(read_id, kind, bin_id, contig_id)`, the per-contig bin
    membership `write-assembly-membership` / `assembly_load` join against.
    `contig_id` is the assembler's own id for the record, carried beside the key
    rather than inside it; nothing joins on it.

**The synthetic read_id is unique across the scan by construction.** Its last
component is `read_fastx`'s `sequence_index`, a 1-based ordinal over the records of
ONE file that restarts at 1 in the next
(https://the-miint.github.io/duckdb-miint/reading/), and `(kind, bin_id)` names
exactly one file: LCG and UNBINNED are a single file each, and `_file_meta` refuses
two refined-bin FASTAs that stem to one bin_id. So no two records carry the same
triple. The composition is injective on top of that — `kind` is one of the
`assembly_constants` literals and `sequence_index` is digits, neither holding a
`:`, so the first and the last `:` of the string are the two separators however
many `:` a bin_id carries.

Pass 2 joins `winner` on that id over a fresh scan, so it also rests on
`sequence_index` being the record's position in the file rather than an artifact of
one scan — pinned in `tests/jobs/test_read_fastx_miint_contract.py`.

**Shared canonical identity.** `sequence_hash` is
`qiita_common.chunking.canonical_sequence_hash_expr` — the SAME expression
`hash_sequences` uses — so an assembled contig whose bytes match a reference
sequence mints the IDENTICAL feature_idx (both dedup against qiita.feature).

**Parsing + chunking are done in DuckDB, not Python.** FASTA records are read with
miint's `read_fastx` (native parser; `.gz` transparent; `read_id` is the header's
first token) and split into 64 KB chunks with miint's native `sequence_split`
(`UNNEST`ed) — never a hand-rolled parser. `read_fastx` returns `filepath` exactly
as passed, so a small in-memory `file_meta(filepath, kind, bin_id)` table (built
from the input paths) JOINs the scan back to each contig's kind + bin without
fragile filename regex. A MAG's bin_id is its file (one FASTA per bin groups many
contigs); the LCG contigs arrive as a single `circular.fa` multi-FASTA and the
unbinned residue as a single `noLCG.fa` one, and each of those records is its own
subject, so their bin_id is the contig id itself — carried as a NULL
`file_meta.bin_id` and COALESCE'd from the read_fastx record in the scan. (That
also means the container needs no per-contig FASTA split: `read_fastx` reads the
whole multi-FASTA and the id column IS the bin_id.)

**The unbinned subject set is the RESIDUE of `noLCG.fa`, not all of it.** The
refined MAGs are drawn from the noLCG contigs, so hashing that file whole would
give every binned contig a second bin_map row (one MAG, one UNBINNED) and so a
second `assembly_membership` row for the same feature_idx. `file_meta` maps a
FILE to a (kind, bin_id), so the subset is taken per RECORD instead: the `contig`
scan below reads both files whole, and the DELETE that follows it removes every
UNBINNED row whose `sequence_hash` also appears under KIND_MAG.

The match key is `canonical_sequence_hash_expr`'s hash, not the contig id. That a
bin FASTA carries the assembler's contig ids through is measured for hifiasm_meta
and unmeasured for myloasm, so the match keys on the bytes that are actually
stored. The hash's strand folding carries into the exclusion: a bin holding a
contig on the opposite strand still excludes its noLCG record. An id still labels
the row, as the UNBINNED bin_id below — never something matched on.

Two consequences of keying on content. Hash-equal noLCG records share a fate: a
bin claiming either drops both. And the exclusion set is the KIND_MAG rows alone —
`assemble.sh` partitions the assembler's contigs between circular.fa and noLCG.fa,
so a hash shared with an LCG record is two contigs carrying one sequence rather
than one contig read twice, and both keep their bin_map row.

0 contigs of any kind is a terminal no-data outcome (`StepNoData`, raised in
`execute`), not a failure.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from pydantic import BaseModel
from qiita_common.assembly_constants import KIND_LCG, KIND_MAG, KIND_UNBINNED
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
from ._assembly import LCG_FILE, NOLCG_FILE

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

    `genomes_dir` (holds `circular.fa` + `noLCG.fa`) and `refined_bins_dir` (MAG bins)
    are the upstream container steps' outputs. `prep_sample_idx` / `work_ticket_idx` are
    framework-injected scope scalars, declared for an explicit contract: nothing this
    step writes is keyed on either, and sequences are run-agnostic, so it needs no
    processing_idx either.
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


def _file_meta(genomes_dir: Path, refined_bins_dir: Path) -> list[tuple[str, str, str | None]]:
    """Build the `(filepath, kind, bin_id)` rows for every non-empty contig FASTA.

    - LCG: the single `<genomes_dir>/circular.fa` multi-FASTA of circular contigs.
      `bin_id` is NULL — an LCG contig is its own genome, so its bin_id is the
      contig id itself (the read_fastx record id), COALESCE'd in the scan rather
      than carried here. Two records of that one file whose headers share a first
      token therefore land under one `(kind, bin_id)`, each keeping its own
      read_id, feature and chunks.
    - UNBINNED: the single `<genomes_dir>/noLCG.fa` multi-FASTA, same NULL-bin_id
      shape as LCG. This row covers the WHOLE file; the residue subset is taken
      per record in `execute` (see the module docstring).
    - MAG: each refined-bin FASTA under `<refined_bins_dir>`; `bin_id` = the
      filename stem (a bin groups many contigs under one file). Two files stemming
      to one bin_id raise, which is what makes `(kind, bin_id)` name a single file
      for this kind — the property the synthetic read_id's uniqueness rests on
      (module docstring). `_FASTA_GLOBS` accepts several suffixes, so `bin.1.fa`
      and `bin.1.fna` would otherwise be two bins the rest of the tail keys as one.

    Empty files are dropped (`read_fastx` raises on a 0-record input, and one empty
    path aborts the whole `VARCHAR[]` scan)."""
    meta: list[tuple[str, str, str | None]] = []
    for name, kind in ((LCG_FILE, KIND_LCG), (NOLCG_FILE, KIND_UNBINNED)):
        path = genomes_dir / name
        if path.is_file() and not is_empty_sequence_file(path):
            meta.append((str(path), kind, None))
    bin_files: dict[str, Path] = {}
    for path in _fasta_files(refined_bins_dir):
        if is_empty_sequence_file(path):
            continue
        bin_id = _local_id(path)
        if bin_id in bin_files:
            raise ValueError(
                f"two refined-bin FASTAs stem to bin_id {bin_id!r} under {refined_bins_dir}: "
                f"{bin_files[bin_id].name} and {path.name}"
            )
        bin_files[bin_id] = path
        meta.append((str(path), KIND_MAG, bin_id))
    return meta


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    meta = _file_meta(inputs.genomes_dir, inputs.refined_bins_dir)

    # A sample whose assembler produced no contig at all is a terminal no-data
    # outcome — nothing to hash, not a failure.
    if not meta:
        raise StepNoData(
            step_name=YAML_STEP_NAME,
            reason=(
                f"no contigs to hash for prep_sample_idx={inputs.prep_sample_idx} "
                f"(no LCG at {inputs.genomes_dir}/{LCG_FILE}, no contigs at "
                f"{inputs.genomes_dir}/{NOLCG_FILE}, no MAG under "
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

            # Pass 1 — per-contig metadata (kind, bin_id, contig_id, synthetic
            # read_id, hash, length). No sequence bytes retained, so manifest +
            # bin_map cost a tiny table. `sequence_hash` is the SHARED canonical
            # hash so identical bytes mint the same feature_idx as a reference
            # sequence.
            conn.execute(
                "CREATE TEMP TABLE contig AS "
                "SELECT "
                "  fm.kind AS kind, "
                "  COALESCE(fm.bin_id, rf.read_id) AS bin_id, "
                "  rf.read_id AS contig_id, "
                "  fm.kind || ':' || COALESCE(fm.bin_id, rf.read_id) "
                "|| ':' || CAST(rf.sequence_index AS VARCHAR) AS read_id, "
                f"  {canonical_sequence_hash_expr('rf.sequence1')} AS sequence_hash, "
                "  CAST(length(rf.sequence1) AS BIGINT) AS sequence_length_bp "
                f"FROM read_fastx(?, max_batch_bytes:='{_READ_FASTX_MAX_BATCH_BYTES}', "
                "  include_filepath:=true) rf "
                "JOIN file_meta fm ON rf.filepath = fm.filepath",
                [paths],
            )

            # Reduce the UNBINNED rows to the RESIDUE: drop the noLCG contigs a
            # refined bin already claims, matched on the canonical sequence_hash
            # (module docstring: why the residue and not the whole file, and why
            # that key and not the contig id). The MAG hash set is materialised
            # first so the DELETE never reads the table it writes; it is narrow
            # (one UUID per distinct binned sequence, no bytes).
            #
            # This runs before the manifest/bin_map COPYs and before `winner`, so
            # pass 2 inherits the exclusion through its `winner` join — a dropped
            # contig has no winner row under its synthetic read_id.
            conn.execute(
                "CREATE TEMP TABLE binned_hash AS "
                "SELECT DISTINCT sequence_hash FROM contig WHERE kind = ?",
                [KIND_MAG],
            )
            conn.execute(
                "DELETE FROM contig WHERE kind = ? "
                "AND sequence_hash IN (SELECT sequence_hash FROM binned_hash)",
                [KIND_UNBINNED],
            )

            conn.execute(
                "COPY (SELECT read_id, sequence_hash, sequence_length_bp FROM contig) "
                f"TO '{manifest_out}' ({PARQUET_OPTS})"
            )
            conn.execute(
                "COPY (SELECT read_id, kind, bin_id, contig_id FROM contig) "
                f"TO '{bin_map_out}' ({PARQUET_OPTS})"
            )

            # winner — the ONE surviving contig per canonical sequence_hash, chosen
            # as a NARROW `DISTINCT ON (sequence_hash)` over the in-memory `contig`
            # table (synthetic read_id + hash only, NO sequence bytes), tie-broken by
            # the synthetic read_id so re-runs pick the same representative
            # deterministically. The canonical hash folds a sequence and its
            # reverse-complement into one hash, so distinct raw bytes can share a
            # hash; keeping the lex-smaller synthetic read_id makes the stored bytes
            # reproducible (this tail is replay-safe). This narrow sort is the crux
            # of bounding memory: the multi-MB sequence payload NEVER rides it.
            conn.execute(
                "CREATE TEMP TABLE winner AS "
                "SELECT DISTINCT ON (sequence_hash) read_id, sequence_hash "
                "FROM contig ORDER BY sequence_hash, read_id"
            )

            # Pass 2 — hash-keyed chunks, STREAMING. Re-scan the FASTA, re-derive
            # each contig's synthetic read_id (same file_meta join as pass 1), keep
            # ONLY the winners (a tiny build side), and UNNEST 64 KB `sequence_split`
            # chunks straight to Parquet keyed by the winner's canonical hash. The
            # join filters to winners BEFORE the UNNEST, so `sequence_split` runs only
            # on surviving rows and each winner's chunks stream to the writer — the
            # full sequence set is never sorted or materialized at once. Peak memory
            # is ~constant in total contig size (bounded by the read_fastx batch +
            # in-flight chunk lists), not O(total bytes) — the same shape that keeps
            # hash_sequences bounded on multi-GB genome inputs. Bytes are chunked
            # exactly as read; the canonical identity lives in the hash.
            conn.execute(
                "COPY ("
                "  SELECT sequence_hash, c.chunk_index, c.chunk_data FROM ("
                "    SELECT w.sequence_hash AS sequence_hash, "
                f"      UNNEST({sequence_split_expr('rf.sequence1')}) AS c "
                f"    FROM read_fastx(?, max_batch_bytes:='{_READ_FASTX_MAX_BATCH_BYTES}', "
                "      include_filepath:=true) rf "
                "    JOIN file_meta fm ON rf.filepath = fm.filepath "
                "    JOIN winner w ON w.read_id = "
                "      (fm.kind || ':' || COALESCE(fm.bin_id, rf.read_id) || ':' "
                "       || CAST(rf.sequence_index AS VARCHAR))"
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
