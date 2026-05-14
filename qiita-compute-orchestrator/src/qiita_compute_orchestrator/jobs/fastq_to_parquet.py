"""Native job: convert a FASTQ (or FASTA) file to a Parquet of reads.

Reads via DuckDB + miint's `read_fastx` table function, computes a UUID
sequence_hash via `md5(sequence)`, and writes a single Parquet file
(`reads.parquet`) to the workspace. Mirrors the convention used by the
reference-side hash job in `backends/local.py::_run_hash` so a sample
FASTQ's reads share their hash representation with reference sequences
— a future analysis can join on `sequence_hash` natively.

Schema (sorted by sequence_hash so downstream dedup joins are
zero-shuffle):

    read_id           VARCHAR  NOT NULL    -- FASTQ/A record id
    sequence          VARCHAR  NOT NULL    -- aliased from miint's sequence1
    quality           VARCHAR              -- aliased from quality1; NULL for FASTA
    sequence_length   BIGINT   NOT NULL    -- length(sequence)
    sequence_hash     UUID     NOT NULL    -- CAST(md5(sequence) AS UUID)

The output Parquet is a workspace artifact only; this commit does not
register it into DuckLake (no `sample_reads` table exists yet).
DuckLake registration is a separate follow-up.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel
from qiita_common.parquet import validate_parquet_path

from ..miint import _ensure_miint_installed, _open_conn

_PARQUET_OPTS = "FORMAT PARQUET, PARQUET_VERSION 'v2', COMPRESSION 'zstd'"


class Inputs(BaseModel):
    """Typed input contract for fastq_to_parquet.

    `fastq_path` is the workflow-declared input (the action_context's
    fastq_path flows through here). `sequenced_sample_idx` and
    `work_ticket_idx` are framework-injected scope scalars merged by
    `flatten_native_inputs`; the job records them implicitly via its
    output's location under the per-step workspace, but accepts them
    on the Inputs model so a future schema extension can stamp them
    into the Parquet without a contract change.
    """

    fastq_path: Path
    sequenced_sample_idx: int
    work_ticket_idx: int


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """Read inputs.fastq_path with miint's read_fastx and write a
    sorted Parquet to <workspace>/reads.parquet. Returns the mapping
    `{"reads": <path>}` so the YAML's `outputs: [reads]` resolves.
    """
    if not inputs.fastq_path.exists():
        raise FileNotFoundError(f"FASTQ file not found: {inputs.fastq_path}")

    workspace.mkdir(parents=True, exist_ok=True)
    out_path = workspace / "reads.parquet"
    out = validate_parquet_path(out_path)

    await _ensure_miint_installed()
    with _open_conn() as conn:
        conn.execute("LOAD miint;")
        # ORDER BY sequence_hash matches the reference-side convention
        # in backends/local.py::_run_hash so downstream consumers that
        # JOIN sample reads against reference sequences benefit from
        # sorted Parquet on both sides.
        conn.execute(
            "COPY ("
            "  SELECT read_id,"
            "    sequence1 AS sequence,"
            "    quality1 AS quality,"
            "    CAST(length(sequence1) AS BIGINT) AS sequence_length,"
            "    CAST(md5(sequence1) AS UUID) AS sequence_hash"
            "  FROM read_fastx(?)"
            "  ORDER BY sequence_hash"
            f") TO '{out}' ({_PARQUET_OPTS})",
            [str(inputs.fastq_path)],
        )

    return {"reads": out_path}
