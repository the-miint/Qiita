"""Native job: parse the pacbio-processing container outputs into the two final
Parquets and stage them for register-files.

Tail of the pacbio-processing workflow. The heavy tools ran in containers; this
native step reads their outputs and writes DuckLake-ready Parquet keyed by
`prep_sample_idx`:

  assembled_genome.parquet  → the `assembled_genome` table. One row per contig of
                              every circular genome (LCG) and refined MAG bin.
  genome_quality.parquet    → the `genome_quality` table. One row per MAG carrying
                              its CheckM metrics (+ DAS_Tool provenance).

Output basenames ARE the DuckLake table names — a downstream register-files step
maps <stem>.parquet to table = stem.

Container→native contract (normalized intermediates the container entrypoints
write, so this parser never depends on a tool's raw text format):

  <genomes_dir>/LCG/<id>.fna[.gz]        one file per circular genome (LCG)
  <refined_bins_dir>/<id>.fa[.gz]        one file per refined MAG bin
  <checkm_dir>/checkm_quality.tsv        header + one row per MAG:
      genome_local_id, marker_lineage, completeness, contamination,
      strain_heterogeneity, genome_size, n_contigs
  <refined_bins_dir>/das_tool_scores.tsv (optional) header + one row per MAG:
      genome_local_id, das_tool_score, source_binner

`genome_local_id` is the FASTA file stem (unique within a sample). CheckM's
Bin Id equals that stem (CheckM ran on the MAG dir with `-x fa`), so the quality
rows join the genome rows by `genome_local_id`.

Empty/partial semantics (mirrors qp-pacbio's skip-on-empty branches):
  * A sample that yields NO genomes at all (no LCG, no MAG) → StepNoData.
  * LCG-only (assembly produced circular genomes but binning found no MAGs) is a
    SUCCESS: assembled_genome has the LCG rows, genome_quality is written empty.
"""

from __future__ import annotations

import csv
import gzip
from collections.abc import Iterator
from pathlib import Path
from typing import Literal

from pydantic import BaseModel
from qiita_common.backend_failure import StepNoData
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    PARQUET_OPTS,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_conn,
    resolve_duckdb_memory_gb,
)

YAML_STEP_NAME = "pacbio_ingest"

# FASTA extensions accepted for genome files (both plain and gzip-compressed).
_FASTA_GLOBS = ("*.fna", "*.fna.gz", "*.fa", "*.fa.gz", "*.fasta", "*.fasta.gz")

# Normalized-intermediate basenames the container entrypoints write.
_CHECKM_TSV = "checkm_quality.tsv"
_DAS_SCORES_TSV = "das_tool_scores.tsv"

_DUCKDB_FALLBACK_GB = 4
_DUCKDB_THREADS = 2


class Inputs(BaseModel):
    """Typed input contract for pacbio_ingest.

    `genomes_dir` (holds LCG/), `refined_bins_dir` (MAG bins + optional scores),
    and `checkm_dir` (checkm_quality.tsv) are the upstream container steps'
    outputs. `assembler` is stamped as provenance. `prep_sample_idx` /
    `work_ticket_idx` are framework-injected scope scalars.
    """

    genomes_dir: Path
    refined_bins_dir: Path
    checkm_dir: Path
    assembler: Literal["hifiasm_meta", "myloasm"] = "hifiasm_meta"
    prep_sample_idx: int
    work_ticket_idx: int


def _iter_fasta(path: Path) -> Iterator[tuple[str, str]]:
    """Yield (contig_id, sequence) for each record in a FASTA file (gzip aware).
    contig_id is the first whitespace-delimited token of the header line."""
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as fh:
        contig_id: str | None = None
        seq_parts: list[str] = []
        for line in fh:
            line = line.rstrip("\n")
            if line.startswith(">"):
                if contig_id is not None:
                    yield contig_id, "".join(seq_parts)
                contig_id = line[1:].split()[0] if len(line) > 1 else ""
                seq_parts = []
            elif line:
                seq_parts.append(line)
        if contig_id is not None:
            yield contig_id, "".join(seq_parts)


def _genome_files(base: Path) -> list[Path]:
    """Every FASTA file directly under `base` (sorted for deterministic order).
    Returns [] if the directory is absent (a legitimately empty upstream step)."""
    if not base.is_dir():
        return []
    found: list[Path] = []
    for pattern in _FASTA_GLOBS:
        found.extend(base.glob(pattern))
    return sorted(set(found))


def _local_id(path: Path) -> str:
    """The genome_local_id for a FASTA file: its stem with any FASTA suffix
    (and a trailing .gz) stripped — `bin.3.fa.gz` -> `bin.3`."""
    name = path.name
    for suffix in (".gz",):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    stem = Path(name).stem  # strips the final .fna/.fa/.fasta
    return stem


def _collect_genome_rows(
    base: Path, kind: str, prep_sample_idx: int, assembler: str
) -> list[tuple]:
    """assembled_genome rows for every contig of every FASTA file under `base`."""
    rows: list[tuple] = []
    for path in _genome_files(base):
        local_id = _local_id(path)
        for contig_id, sequence in _iter_fasta(path):
            rows.append(
                (
                    prep_sample_idx,
                    kind,
                    local_id,
                    contig_id,
                    sequence,
                    len(sequence),
                    assembler,
                )
            )
    return rows


def _read_tsv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    """Return (header, rows-as-dicts) for a tab-separated file; ([], []) if
    absent. Column access is by NAME so a producer can add/reorder columns."""
    if not path.is_file():
        return [], []
    with path.open("rt", newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        return list(reader.fieldnames or []), list(reader)


def _f(value: str | None) -> float | None:
    return float(value) if value not in (None, "") else None


def _i(value: str | None) -> int | None:
    return int(float(value)) if value not in (None, "") else None


def _collect_quality_rows(
    checkm_dir: Path,
    refined_bins_dir: Path,
    prep_sample_idx: int,
    assembler: str,
) -> list[tuple]:
    """genome_quality rows (one per MAG) from checkm_quality.tsv, joined with the
    optional DAS_Tool scores by genome_local_id. Empty if CheckM produced no
    table (e.g. the sample had no MAGs)."""
    _, checkm_rows = _read_tsv(checkm_dir / _CHECKM_TSV)
    _, score_rows = _read_tsv(refined_bins_dir / _DAS_SCORES_TSV)
    scores = {
        r["genome_local_id"]: (_f(r.get("das_tool_score")), r.get("source_binner"))
        for r in score_rows
    }

    rows: list[tuple] = []
    for r in checkm_rows:
        local_id = r["genome_local_id"]
        das_score, source_binner = scores.get(local_id, (None, None))
        rows.append(
            (
                prep_sample_idx,
                "MAG",
                local_id,
                r.get("marker_lineage") or None,
                _f(r.get("completeness")),
                _f(r.get("contamination")),
                _f(r.get("strain_heterogeneity")),
                _i(r.get("genome_size")),
                _i(r.get("n_contigs")),
                das_score,
                source_binner,
                assembler,
            )
        )
    return rows


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    prep = inputs.prep_sample_idx
    assembler = inputs.assembler

    genome_rows = _collect_genome_rows(
        inputs.genomes_dir / "LCG", "LCG", prep, assembler
    ) + _collect_genome_rows(inputs.refined_bins_dir, "MAG", prep, assembler)

    # A sample that assembled nothing binnable AND has no circular genome is a
    # terminal no-data outcome — no rows to store, not a failure.
    if not genome_rows:
        raise StepNoData(
            step_name=YAML_STEP_NAME,
            reason=(
                f"no genomes recovered for prep_sample_idx={prep} "
                f"(no LCG under {inputs.genomes_dir}/LCG, no MAG under "
                f"{inputs.refined_bins_dir})"
            ),
        )

    quality_rows = _collect_quality_rows(
        inputs.checkm_dir, inputs.refined_bins_dir, prep, assembler
    )

    staging = workspace / "genome_staging"
    staging.mkdir(parents=True, exist_ok=True)
    assembled_out = validate_parquet_path(staging / "assembled_genome.parquet")
    quality_out = validate_parquet_path(staging / "genome_quality.parquet")

    memory_gb = resolve_duckdb_memory_gb(_DUCKDB_FALLBACK_GB, threads=_DUCKDB_THREADS)
    with duckdb_tmp_dir(workspace) as duckdb_tmp, open_conn() as conn:
        apply_duckdb_settings(conn, duckdb_tmp, memory_gb=memory_gb, threads=_DUCKDB_THREADS)

        # Schemas match the DuckLake tables exactly (column names + types) so
        # register-files' ducklake_add_data_files accepts them.
        conn.execute(
            "CREATE TABLE assembled_genome ("
            "  prep_sample_idx BIGINT, kind VARCHAR, genome_local_id VARCHAR,"
            "  contig_id VARCHAR, sequence VARCHAR, length_bp BIGINT,"
            "  assembler VARCHAR)"
        )
        conn.executemany("INSERT INTO assembled_genome VALUES (?, ?, ?, ?, ?, ?, ?)", genome_rows)
        conn.execute(f"COPY assembled_genome TO '{assembled_out}' ({PARQUET_OPTS})")

        conn.execute(
            "CREATE TABLE genome_quality ("
            "  prep_sample_idx BIGINT, kind VARCHAR, genome_local_id VARCHAR,"
            "  marker_lineage VARCHAR, completeness DOUBLE, contamination DOUBLE,"
            "  strain_heterogeneity DOUBLE, genome_size BIGINT, n_contigs BIGINT,"
            "  das_tool_score DOUBLE, source_binner VARCHAR, assembler VARCHAR)"
        )
        if quality_rows:
            conn.executemany(
                "INSERT INTO genome_quality VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                quality_rows,
            )
        # Written even when empty (LCG-only sample) so register-files always finds
        # both tables' Parquet with the correct schema.
        conn.execute(f"COPY genome_quality TO '{quality_out}' ({PARQUET_OPTS})")

    return {"genome_staging_dir": staging}
