"""Native job: stage many local FASTA files into one chunked Parquet.

The local-ingest front-end for reference-add. Where the remote path streams a
single FASTA over Arrow Flight DoPut and lands an `upload.parquet`, this job
reads a *manifest* of absolute FASTA paths already resident on the compute host
and produces the **same** chunked shape:

  `fasta.parquet` — `(read_id VARCHAR, chunk_index INTEGER, chunk_data VARCHAR)`

That is exactly what `hash_sequences` consumes, so the rest of the reference-add
pipeline (`hash_sequences` → `mint-features` → `write-membership` → `load` →
`register-files` → …) runs unchanged. This job is the *only* new step on the
local path; everything downstream is reused verbatim.

**Parsing + chunking are done in DuckDB, not Python.** FASTA records are read
with miint's `read_fastx` table function (native parser; `.gz` transparent;
`read_id` is the header's first token, matching the remote path), and the 64 KB
chunking is the blessed `list_transform` + `UNNEST` macro — never a hand-rolled
Python parser. `read_fastx` only accepts a literal path arg (no lateral join
over a path column), so the manifest is iterated in Python with one
`read_fastx(?)` per file; that is control flow only — no sequence bytes pass
through Python.

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

import shutil
from pathlib import Path

from pydantic import BaseModel
from qiita_common.chunking import CHUNK_LIST_MACRO_SQL
from qiita_common.duckdb_miint import is_empty_sequence_file
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    PARQUET_OPTS_CHUNKED,
    apply_duckdb_settings,
    ensure_miint_installed,
    open_conn,
)

YAML_STEP_NAME = "stage_local_fasta"

# DuckDB resource caps for this step. These literals duplicate the workflow
# YAML's baseline_resources for this step (mem_gb=8, cpu=4); the DuckDB cap is
# mem_gb minus ~1 GB Python headroom. A mismatch is visible at review time. A
# future refactor should thread the YAML values through `Inputs` instead of
# duplicating (see hash_sequences' note).
_DUCKDB_MEMORY_GB = 7
_DUCKDB_THREADS = 4

# Byte budget for each `read_fastx` batch. Caps the read-side vector so a run of
# multi-MB genome records can't materialise a 2048-row × 21 MB chunk before the
# chunking operator runs. One of three memory levers — the others are the
# `memory_limit`/`temp_directory` spill (apply_duckdb_settings) and the write
# buffer (`ROW_GROUP_SIZE` in PARQUET_OPTS_CHUNKED).
_READ_FASTX_MAX_BATCH_BYTES = "64MB"

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

    One path per line; blank lines and `#` comments are skipped. Every entry
    must be absolute and an existing file (fail fast, mirroring
    bcl_convert_prep's guards). Raises ValueError on the first bad entry or if
    no real path lines remain.
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
    """Read every manifest FASTA with `read_fastx`; emit one combined chunked
    Parquet. Validates the manifest and each listed file, stages all reads into
    a DuckDB table (one `read_fastx` per file), fails fast on an empty-body
    record or a duplicate read_id, then COPYs the `list_transform`/`UNNEST`
    chunking to `fasta.parquet`. Returns `{"fasta_path": <parquet>}`.
    """
    if not inputs.fasta_manifest_path.is_absolute():
        raise ValueError(
            f"fasta_manifest_path must be absolute, got {inputs.fasta_manifest_path!r}"
        )
    if not inputs.fasta_manifest_path.exists() or not inputs.fasta_manifest_path.is_file():
        raise ValueError(f"FASTA manifest not found or not a file: {inputs.fasta_manifest_path}")

    fasta_paths = _read_manifest(inputs.fasta_manifest_path)

    workspace.mkdir(parents=True, exist_ok=True)
    fasta_parquet = workspace / "fasta.parquet"
    fasta_out = validate_parquet_path(fasta_parquet)
    duckdb_tmp = workspace / ".duckdb_tmp"
    duckdb_tmp.mkdir(parents=True, exist_ok=True)

    await ensure_miint_installed()

    success = False
    try:
        with open_conn() as conn:
            conn.execute("LOAD miint;")
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=_DUCKDB_MEMORY_GB,
                threads=_DUCKDB_THREADS,
            )

            # Stage one row per read across all files. read_fastx parses FASTA
            # natively (gz-transparent) and only takes a literal path, so we
            # loop in Python — control flow only, no sequence bytes in Python.
            # `max_batch_bytes` caps the read-side vector for genome-scale
            # records. A 0-record file would make read_fastx throw "Empty
            # file"; the plan skips such files, so pre-check and continue.
            conn.execute("CREATE TEMP TABLE reads (read_id VARCHAR, sequence VARCHAR)")
            for fasta_path in fasta_paths:
                if is_empty_sequence_file(fasta_path):
                    continue
                conn.execute(
                    "INSERT INTO reads "
                    "SELECT read_id, sequence1 "
                    f"FROM read_fastx(?, max_batch_bytes:='{_READ_FASTX_MAX_BATCH_BYTES}')",
                    [str(fasta_path)],
                )

            # Empty-body records: a named read with no sequence is bad data.
            # read_fastx surfaces them as length-0 rows, so detect in SQL.
            empties = conn.execute(
                "SELECT read_id FROM reads WHERE length(sequence) = 0 ORDER BY read_id LIMIT ?",
                [_MAX_REPORT],
            ).fetchall()
            if empties:
                names = ", ".join(row[0] for row in empties)
                raise ValueError(f"FASTA record(s) with a header but no sequence body: {names}")

            # Duplicate read_id: the global genome_map join key must be unique.
            # One row per read here, so a plain GROUP BY HAVING count(*) > 1.
            dupes = conn.execute(
                "SELECT read_id FROM reads GROUP BY read_id HAVING count(*) > 1 "
                "ORDER BY read_id LIMIT ?",
                [_MAX_REPORT],
            ).fetchall()
            if dupes:
                names = ", ".join(row[0] for row in dupes)
                raise ValueError(
                    "duplicate read_id in FASTA manifest — read_id is the global "
                    f"genome_map join key and must be unique: {names}"
                )

            # Chunk + write in one COPY using the shared `chunk_list` macro
            # (single chunking definition for both this job and the CLI; see
            # qiita_common.chunking). Each sequence explodes into 64 KB pieces
            # via list_transform/UNNEST; the substring()s are views, no
            # re-materialisation.
            conn.execute(CHUNK_LIST_MACRO_SQL)
            conn.execute(
                "COPY ("
                "  SELECT read_id, c.chunk_index, c.chunk_data FROM ("
                "    SELECT read_id, UNNEST(chunk_list(sequence)) AS c FROM reads"
                "  )"
                f") TO '{fasta_out}' ({PARQUET_OPTS_CHUNKED})"
            )

            conn.execute("DROP TABLE reads")
        success = True
    finally:
        # Drop the DuckDB spill dir before returning so the SLURM launcher's
        # manifest walker (which runs after execute()) sees only fasta.parquet.
        shutil.rmtree(duckdb_tmp, ignore_errors=True)
        # On any failure (bad manifest entry, empty-body, dup read_id,
        # interrupted COPY) remove a partial Parquet so the launcher's walker
        # can't promote a half-written file as this step's output.
        if not success:
            fasta_parquet.unlink(missing_ok=True)

    return {"fasta_path": fasta_parquet}
