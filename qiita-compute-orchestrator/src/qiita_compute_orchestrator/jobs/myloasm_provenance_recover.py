"""Recover the `myloasm_*` provenance tables for one myloasm run that finished on
long-read-assembly 1.0.0 or 1.0.1, which did not write them. A one-off: this job, its
`myloasm-provenance-recover` workflow and the backfill that submits it are deleted
once every such run has its tables or an explained residue line (the workflow's
header names the issue that tracks it).

The source is the run's own `assemble` step output, still in its ticket's workspace:
myloasm's `-o` tree under `assembler/`, the splitter's `genomes/`, and the
`manifest.json` the step wrote when it finished. The ticket names it by
`(source_work_ticket_idx, assemble_attempt)`, which `qiita_common.workspace` turns into
a path under `PATH_SCRATCH`, never by a path a submitter could point elsewhere.

What 1.0.2 does inside the container and in `assembly_load` happens here instead,
through the same shared transform (`_myloasm_provenance`). Two inputs differ:

- **Read names.** Those versions named the assembler's reads by `read_id`, so each name is
  looked up in the sample's `read_masked` pass-set under the run's mask, fetching
  only `(read_id, sequence_idx)`.
- **Contig features.** 1.0.2 joins `bin_map` to the run's minted features. Here the
  lake is the authority: each genome contig's canonical hash is matched to the run's
  `assembled_sequence` rows.

Three checks tie the tree to the registered run before anything is written. The
manifest must list the files read here, at the sizes they have now. The genome
contigs and the run's `assembled_sequence` rows must match one to one by hash, in
both directions. Every read name the dump uses must resolve (the transform's own
check).
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import duckdb
from pydantic import BaseModel, Field
from qiita_common.actions import STEP_MANIFEST_FILENAME
from qiita_common.chunking import canonical_sequence_hash_expr
from qiita_common.duckdb_miint import is_empty_sequence_file
from qiita_common.workspace import (
    WORK_TICKET_SUBDIR,
    step_attempt_dir,
    step_output_dir,
    ticket_workspace,
)

from ..config import get_settings
from ..data_plane_client import open_assembled_sequence_stream, open_read_masked_stream
from ..miint import (
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)
from ._assembly import LCG_FILE, NOLCG_FILE
from ._myloasm_provenance import (
    CONTIG_FEATURE_TABLE,
    dump_raw_tables,
    read_names_from_lookup,
    register_raw_tables,
    require_none,
    staging_paths,
    write_myloasm_staging,
)

# The step whose output is the source, and where its assemble.sh put each part.
_SOURCE_STEP = "assemble"
_ASSEMBLER_DIR = "assembler"
_GENOMES_DIR = "genomes"
_PRIMARY_FASTA = f"{_ASSEMBLER_DIR}/assembly_primary.fa"
# The two columns the read lookup binds; the rest of a long-read sample's
# `read_masked` rows are its sequences and qualities.
_READ_LOOKUP_COLUMNS = ["read_id", "sequence_idx"]

_DUCKDB_MEMORY_GB = 8
_DUCKDB_THREADS = 4

_GENOME_CONTIG = "genome_contig"
_RUN_FEATURE = "run_feature"


class Inputs(BaseModel):
    """Typed input contract for myloasm_provenance_recover.

    `prep_sample_idx` / `work_ticket_idx` are framework-injected. The rest ride the
    workflow's `params:`. `assembly_processing_idx` and `assembly_mask_idx` name the
    finished run and the mask its reads came from; they are prefixed because the bare
    `processing_idx` / `mask_idx` are bindings the runner acts on (it would mint a new
    run or mask). `source_work_ticket_idx` and `assemble_attempt` locate that run's
    completed `assemble` attempt.
    """

    prep_sample_idx: int
    work_ticket_idx: int
    assembly_processing_idx: int = Field(gt=0)
    assembly_mask_idx: int = Field(gt=0)
    source_work_ticket_idx: int = Field(gt=0)
    assemble_attempt: int = Field(gt=0)


def _source_output(inputs: Inputs) -> Path:
    ticket = ticket_workspace(
        Path(get_settings().path_scratch) / WORK_TICKET_SUBDIR, inputs.source_work_ticket_idx
    )
    return step_output_dir(step_attempt_dir(ticket, _SOURCE_STEP, inputs.assemble_attempt))


def _check_source(output: Path) -> None:
    """Refuse a source that is not the finished step's output as it was written."""
    manifest = output / STEP_MANIFEST_FILENAME
    if not manifest.is_file():
        raise FileNotFoundError(
            f"no {STEP_MANIFEST_FILENAME} at {manifest}: no finished {_SOURCE_STEP} attempt there"
        )
    if not (output / _ASSEMBLER_DIR).is_dir():
        raise FileNotFoundError(f"no assembler tree at {output / _ASSEMBLER_DIR}")
    listed = {f["path"]: f["size_bytes"] for f in json.loads(manifest.read_text())["files"]}
    for rel in (_PRIMARY_FASTA, f"{_GENOMES_DIR}/{LCG_FILE}", f"{_GENOMES_DIR}/{NOLCG_FILE}"):
        if rel not in listed:
            raise ValueError(f"{manifest} does not list {rel}")
        path = output / rel
        if not path.is_file():
            raise FileNotFoundError(f"{path} is listed in {manifest} but missing")
        size = path.stat().st_size
        if size != listed[rel]:
            raise ValueError(
                f"{path} is {size} bytes, but {manifest} recorded {listed[rel]}: "
                "it changed after the step finished"
            )


def _genome_contigs(conn: duckdb.DuckDBPyConnection, genomes: Path) -> None:
    """Each genome contig's id and canonical hash: the files `assembly_hash` hashed
    for this run, so the same bytes give the same hash."""
    fastas = [
        str(p) for p in (genomes / LCG_FILE, genomes / NOLCG_FILE) if not is_empty_sequence_file(p)
    ]
    if not fastas:
        raise ValueError(f"no genome contigs under {genomes}")
    conn.execute(
        f"CREATE TEMP TABLE {_GENOME_CONTIG} AS SELECT read_id AS contig_id, "
        f"{canonical_sequence_hash_expr('sequence1')} AS sequence_hash FROM read_fastx(?)",
        [fastas],
    )


def _contig_features(conn: duckdb.DuckDBPyConnection, lake: str) -> None:
    """`CONTIG_FEATURE_TABLE` from the genome contigs and the run's lake rows,
    refusing any contig or feature the other side lacks."""
    # The CAST is correct, not a workaround: the data plane streams a UUID as its
    # canonical text by design (`a_uuid_column_streams_as_its_canonical_text`).
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
            dump_raw_tables(conn, output / _ASSEMBLER_DIR, raw)
            register_raw_tables(conn, raw)

            async with open_read_masked_stream(
                conn,
                prep_sample_idx=inputs.prep_sample_idx,
                mask_idx=inputs.assembly_mask_idx,
                columns=_READ_LOOKUP_COLUMNS,
            ) as reads:
                read_names_from_lookup(conn, reads)

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
