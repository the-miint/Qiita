"""Native job: convert a FASTQ (or FASTA) file to a Parquet of reads.

Reads via DuckDB + miint's `read_fastx` table function and writes a
single Parquet file (`reads.parquet`) to the workspace. Every read in
the input becomes one row — no deduplication, even when sequences
match.

Schema (sorted by read_id, which is the FASTQ/A header line — the
sample's own labeling of its reads):

    read_id           VARCHAR     NOT NULL  -- FASTQ/A record id, e.g. "read_001"
    sequence          VARCHAR     NOT NULL  -- aliased from miint's sequence1
    quality           UTINYINT[]            -- from qual1; phred-decoded; NULL for FASTA
    sequence_length   BIGINT      NOT NULL  -- length(sequence)

The `quality` column is stored as miint's phred-decoded score array
(values 0–93 for Sanger), not the FASTQ ASCII string. miint already
applies the offset on read; downstream code consumes integer phred
scores directly with no re-decoding step.

The output Parquet is a workspace artifact only; this job does not
register it into DuckLake (no `sample_reads` table exists yet) and
the schema deliberately omits any CP-minted identifier columns. When
DuckLake registration lands, the table will gain the
qiita_common.models.ScopeTarget hierarchy fields and the sort order
will change to the convention documented in CLAUDE.md
("Result file requirements"); both changes happen together with the
registration design.

Sibling: `LocalBackend._run_hash` in `backends/local.py` is structurally
similar — same DuckDB+miint plumbing, same `PARQUET_OPTS`, same use of
`read_fastx` — but is a *reference-side dedup* job, not a per-sample
ingest. Concrete divergences:

  - `_run_hash` rejects duplicate `read_id` values with
    `ValueError → BAD_INPUT` because a reference FASTA must have
    unique sequence identifiers. This job keeps every read, dups and
    all, because a sample FASTQ legitimately repeats sequences across
    reads.
  - `_run_hash` writes `(read_id, sequence_hash, length)` — a
    manifest sorted by `sequence_hash` for downstream feature minting.
    This job writes raw reads, sorted by `read_id` for natural
    ingestion-order iteration.

They share mechanics, not semantics. A future "convergence" follow-up
would only make sense if a third caller appears that wants the
overlap.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
from pydantic import BaseModel
from qiita_common.parquet import validate_parquet_path

from ..miint import PARQUET_OPTS, ensure_miint_installed, open_conn


class Inputs(BaseModel):
    """Typed input contract for fastq_to_parquet.

    `fastq_path` is the workflow-declared input (the action_context's
    fastq_path flows through here). `prep_sample_idx` and
    `work_ticket_idx` are framework-injected scope scalars merged by
    `flatten_native_inputs`; the job records them implicitly via its
    output's location under the per-step workspace, but accepts them
    on the Inputs model so a future schema extension can stamp them
    into the Parquet without a contract change.
    """

    fastq_path: Path
    prep_sample_idx: int
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

    await ensure_miint_installed()
    with open_conn() as conn:
        conn.execute("LOAD miint;")
        # Materialize read_fastx output into a temp table first so the
        # empty-file branch can substitute a header-only fallback table
        # with the same schema. DuckDB raises on a zero-byte input from
        # read_fastx (miint quirk); the reference-side _run_hash uses
        # the same pattern.
        try:
            conn.execute(
                "CREATE TEMP TABLE reads AS "
                "SELECT read_id,"
                "  sequence1 AS sequence,"
                "  qual1 AS quality,"
                "  CAST(length(sequence1) AS BIGINT) AS sequence_length "
                "FROM read_fastx(?)",
                [str(inputs.fastq_path)],
            )
        except duckdb.Error as exc:
            if "Empty file" in str(exc):
                conn.execute(
                    "CREATE TEMP TABLE reads ("
                    "  read_id VARCHAR, sequence VARCHAR,"
                    "  quality UTINYINT[], sequence_length BIGINT"
                    ")"
                )
            else:
                raise

        # ORDER BY read_id matches the FASTQ's natural ingestion order
        # — consumers iterating in submission order get it for free.
        # When DuckLake registration lands and the schema gains the
        # CP-minted identifier columns, the sort changes to the
        # CLAUDE.md "Result file requirements" convention.
        conn.execute(f"COPY (SELECT * FROM reads ORDER BY read_id) TO '{out}' ({PARQUET_OPTS})")
        conn.execute("DROP TABLE reads")

    return {"reads": out_path}
