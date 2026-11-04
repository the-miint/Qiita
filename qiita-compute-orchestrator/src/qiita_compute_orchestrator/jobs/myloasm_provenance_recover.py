"""Recover the `myloasm_*` provenance tables for one myloasm run that finished on
long-read-assembly 1.0.0 or 1.0.1, which did not write them. A one-off; the
workflow's header names the issue that removes it.

The source is the run's own `assemble` step output, still in the workspace of the
ticket that ran it: myloasm's `-o` tree under `assembler/`, the splitter's `genomes/`,
and the `manifest.json` that the step wrote when it finished. The ticket names it by
`(source_work_ticket_idx, assemble_attempt)`, which `qiita_common.workspace` turns into
a path under `PATH_SCRATCH`, never by a path that a submitter could point elsewhere.

What 1.0.2 does inside the container and in `assembly_load` happens here instead,
through the same shared transform (`_myloasm_provenance`). Two inputs differ:

- **Read names.** Those versions named the assembler's reads by `read_id`, so each
  name is looked up in the prep_sample's `read_masked` pass-set under the run's mask.
- **Contig features.** 1.0.2 joins `bin_map` to the run's minted features. Here the
  lake is the authority: each genome contig's canonical hash is matched to the run's
  `assembled_sequence` rows.

Three checks tie the tree to the registered run before anything is written. The
manifest must list the primary contig FASTA and the two genome FASTAs, at the sizes
that they have now (`_check_source`). Every genome contig must hash to one of the
run's `assembled_sequence` rows, and every one of those rows must be some genome
contig's hash (`_contig_features`). Every read name that the dump uses must resolve
(the transform's own check).
"""

from __future__ import annotations

import json
import shutil
import stat
from collections.abc import Callable
from pathlib import Path

import duckdb
from pydantic import BaseModel, Field
from qiita_common.actions import STEP_MANIFEST_FILENAME
from qiita_common.chunking import canonical_sequence_hash_expr
from qiita_common.duckdb_miint import is_empty_sequence_file
from qiita_common.myloasm_constants import MYLOASM_TABLES
from qiita_common.parquet import validate_parquet_path
from qiita_common.sql import sql_string_literal
from qiita_common.workspace import (
    WORK_TICKET_SUBDIR,
    step_attempt_dir,
    step_output_dir,
    ticket_workspace,
)

from ..config import get_settings
from ..data_plane_client import open_assembled_sequence_stream, open_read_masked_stream
from ..miint import (
    PARQUET_OPTS_INTERMEDIATE,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)
from ._assembly import LCG_FILE, NOLCG_FILE, READ_FASTX_MAX_BATCH_BYTES
from ._myloasm_provenance import (
    CONTIG_FEATURE_TABLE,
    RAW_READ_NAMES,
    READ_NAME_TABLE,
    register_raw_tables,
    require_none,
    staging_paths,
    write_myloasm_staging,
)

# The step whose output is the source, and where assemble.sh put each part of it.
_SOURCE_STEP = "assemble"
_ASSEMBLER_DIR = "assembler"
_GENOMES_DIR = "genomes"
_PRIMARY_FASTA = f"{_ASSEMBLER_DIR}/assembly_primary.fa"
# The dump's tables that are never empty for a run with primary contigs: the
# container's own set (`_NEVER_EMPTY` in workflows/long-read-assembly/myloasm_provenance.py).
_NEVER_EMPTY = ("myloasm_contig", "myloasm_run")
# The two columns that the read lookup binds, and so the only two that it asks the
# data plane for: a long-read prep_sample's `read_masked` rows are mostly their
# sequences and qualities.
_READ_LOOKUP_COLUMNS = ["read_id", "sequence_idx"]

_DUCKDB_MEMORY_GB = 8
_DUCKDB_THREADS = 4

_GENOME_CONTIG = "genome_contig"
_RUN_FEATURE = "run_feature"


class Inputs(BaseModel):
    """Typed input contract for myloasm_provenance_recover.

    `prep_sample_idx` / `work_ticket_idx` are framework-injected. The rest ride the
    workflow's `params:`. `assembly_processing_idx` and `assembly_mask_idx` name the
    finished run and the mask that its reads came from; they are prefixed because the
    bare `processing_idx` / `mask_idx` are bindings that the runner acts on (it would
    mint a new run or mask). `source_work_ticket_idx` and `assemble_attempt` locate
    that run's completed `assemble` attempt; the runner numbers attempts from 0.
    """

    prep_sample_idx: int
    work_ticket_idx: int
    assembly_processing_idx: int = Field(gt=0)
    assembly_mask_idx: int = Field(gt=0)
    source_work_ticket_idx: int = Field(gt=0)
    assemble_attempt: int = Field(ge=0)


def _source_output(inputs: Inputs) -> Path:
    ticket = ticket_workspace(
        Path(get_settings().path_scratch) / WORK_TICKET_SUBDIR, inputs.source_work_ticket_idx
    )
    return step_output_dir(step_attempt_dir(ticket, _SOURCE_STEP, inputs.assemble_attempt))


def _is(path: Path, mode_test: Callable[[int], bool]) -> bool:
    """Whether `path` exists and passes `mode_test` (`stat.S_ISREG`, `stat.S_ISDIR`).
    Only absence reads as False: a path that cannot be looked at (no permission to
    traverse its parent) raises, where `Path.is_file()` would report it missing."""
    try:
        return mode_test(path.stat().st_mode)
    except FileNotFoundError, NotADirectoryError:
        return False


def _check_source(output: Path) -> None:
    """Refuse a source unless its manifest lists each file read directly here, at
    the size that the file has now."""
    manifest = output / STEP_MANIFEST_FILENAME
    if not _is(manifest, stat.S_ISREG):
        raise FileNotFoundError(
            f"no {STEP_MANIFEST_FILENAME} at {manifest}: no finished {_SOURCE_STEP} attempt there"
        )
    if not _is(output / _ASSEMBLER_DIR, stat.S_ISDIR):
        raise FileNotFoundError(f"no assembler tree at {output / _ASSEMBLER_DIR}")
    listed = {f["path"]: f["size_bytes"] for f in json.loads(manifest.read_text())["files"]}
    for rel in (_PRIMARY_FASTA, f"{_GENOMES_DIR}/{LCG_FILE}", f"{_GENOMES_DIR}/{NOLCG_FILE}"):
        if rel not in listed:
            raise ValueError(f"{manifest} does not list {rel}")
        path = output / rel
        if not _is(path, stat.S_ISREG):
            raise FileNotFoundError(f"{path} is listed in {manifest} but missing")
        size = path.stat().st_size
        if size != listed[rel]:
            raise ValueError(
                f"{path} is {size} bytes, but {manifest} recorded {listed[rel]}: "
                "it changed after the step finished"
            )


def _dump_raw_tables(conn: duckdb.DuckDBPyConnection, asm_dir: Path, raw_dir: Path) -> None:
    """Write the raw dump that 1.0.2's assemble step writes in its container: each
    `MYLOASM_TABLES` reader over `asm_dir` into `raw_dir/<table>.parquet`, verbatim,
    as `workflows/long-read-assembly/myloasm_provenance.py` does, refusing an empty
    `_NEVER_EMPTY` table as it does."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    src = sql_string_literal(asm_dir)
    for table, spec in MYLOASM_TABLES.items():
        dest = validate_parquet_path(raw_dir / f"{table}.parquet")
        (rows,) = conn.execute(
            f"COPY (FROM {spec.reader}({src})) TO '{dest}' ({PARQUET_OPTS_INTERMEDIATE})"
        ).fetchone()
        if table in _NEVER_EMPTY and rows == 0:
            raise ValueError(f"{spec.reader} returned no rows for {asm_dir}")


def _read_names_from_lookup(conn: duckdb.DuckDBPyConnection, lookup: str) -> None:
    """`READ_NAME_TABLE` from `lookup`, the prep_sample's `(read_id, sequence_idx)`
    pairs, keeping only the names that the dump uses: the lookup is the whole
    pass-set. `lookup` is read once, so a single-consumption stream will do. A name
    that the lookup lacks is the transform's refusal ("no sequence_idx")."""
    conn.execute(
        f"CREATE TEMP TABLE {READ_NAME_TABLE} AS "
        f"SELECT {', '.join(_READ_LOOKUP_COLUMNS)} FROM {lookup} "
        f"WHERE read_id IN (SELECT read_id FROM {RAW_READ_NAMES})"
    )


def _genome_contigs(conn: duckdb.DuckDBPyConnection, genomes: Path) -> None:
    """Each genome contig's id and canonical hash, from the files that
    `assembly_hash` hashed for this run, read as it reads them."""
    fastas = [
        str(p) for p in (genomes / LCG_FILE, genomes / NOLCG_FILE) if not is_empty_sequence_file(p)
    ]
    if not fastas:
        raise ValueError(f"no genome contigs under {genomes}")
    conn.execute(
        f"CREATE TEMP TABLE {_GENOME_CONTIG} AS SELECT read_id AS contig_id, "
        f"{canonical_sequence_hash_expr('sequence1')} AS sequence_hash "
        f"FROM read_fastx(?, max_batch_bytes:='{READ_FASTX_MAX_BATCH_BYTES}')",
        [fastas],
    )


def _contig_features(conn: duckdb.DuckDBPyConnection, lake: str) -> None:
    """`CONTIG_FEATURE_TABLE` from the genome contigs and the run's lake rows,
    refusing any contig or feature the other side lacks."""
    # The CAST is the intended use, not a workaround: the data plane streams a UUID
    # column as its canonical text, pinned by its test
    # `a_uuid_column_streams_as_its_canonical_text`.
    conn.execute(
        f"CREATE TEMP TABLE {_RUN_FEATURE} AS "
        f"SELECT feature_idx, CAST(sequence_hash AS UUID) AS sequence_hash FROM {lake}"
    )
    require_none(
        conn,
        f"SELECT contig_id FROM {_GENOME_CONTIG} ANTI JOIN {_RUN_FEATURE} USING (sequence_hash) "
        "ORDER BY contig_id",
        "genome contig(s) whose sequence is not in the run's assembled_sequence; the "
        "source tree is not the registered run's",
    )
    require_none(
        conn,
        f"SELECT feature_idx FROM {_RUN_FEATURE} ANTI JOIN {_GENOME_CONTIG} USING (sequence_hash) "
        "ORDER BY feature_idx",
        "assembled_sequence feature(s) of the run that no genome contig hashes to",
    )
    conn.execute(
        f"CREATE TEMP TABLE {CONTIG_FEATURE_TABLE} AS SELECT g.contig_id, r.feature_idx "
        f"FROM {_GENOME_CONTIG} g JOIN {_RUN_FEATURE} r USING (sequence_hash)"
    )


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    output = _source_output(inputs)
    _check_source(output)

    workspace.mkdir(parents=True, exist_ok=True)
    staging = workspace / "myloasm_staging"
    staging.mkdir(parents=True, exist_ok=True)
    # The raw dump is scratch, not output: register-files loads everything in
    # `staging`, and nothing else here is a declared output.
    raw = workspace / ".myloasm_raw"

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS),
                threads=_DUCKDB_THREADS,
            )
            _dump_raw_tables(conn, output / _ASSEMBLER_DIR, raw)
            register_raw_tables(conn, raw)

            async with open_read_masked_stream(
                conn,
                prep_sample_idx=inputs.prep_sample_idx,
                mask_idx=inputs.assembly_mask_idx,
                columns=_READ_LOOKUP_COLUMNS,
            ) as reads:
                _read_names_from_lookup(conn, reads)

            _genome_contigs(conn, output / _GENOMES_DIR)
            async with open_assembled_sequence_stream(
                conn,
                prep_sample_idx=inputs.prep_sample_idx,
                processing_idx=inputs.assembly_processing_idx,
            ) as lake:
                _contig_features(conn, lake)

            write_myloasm_staging(
                conn,
                staging_dir=staging,
                prep_sample_idx=inputs.prep_sample_idx,
                processing_idx=inputs.assembly_processing_idx,
            )
        success = True
    finally:
        shutil.rmtree(raw, ignore_errors=True)
        if not success:
            for partial in staging_paths(staging).values():
                partial.unlink(missing_ok=True)

    return {"staging_dir": staging}
