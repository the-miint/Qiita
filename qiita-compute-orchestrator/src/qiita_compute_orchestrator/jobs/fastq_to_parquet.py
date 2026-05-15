"""Native job: convert a FASTQ (or FASTA) file to a Parquet of reads,
keyed by a CP-minted `sequence_idx` BIGINT.

Reads via DuckDB + miint's `read_fastx` table function, mints a
contiguous bigint range from the control plane (`POST /sequence-range`,
PR #36's allocator), and writes a Parquet with one row per input read.
No deduplication — every read becomes one row, including duplicate
sequences.

Schema (sorted by sequence_idx, the lake-friendly join key):

    sequence_idx      BIGINT      NOT NULL  -- CP-minted, contiguous within this sample
    read_id           VARCHAR     NOT NULL  -- FASTQ/A record id (label, no longer the join key)
    sequence          VARCHAR     NOT NULL  -- aliased from miint's sequence1
    quality           UTINYINT[]            -- from qual1; phred-decoded; NULL for FASTA
    sequence_length   BIGINT      NOT NULL  -- length(sequence)

Pipeline (B-staged-Parquet):

  Phase 1: FASTQ -> intermediate Parquet (no sequence_idx yet).
           One streaming pass through miint's read_fastx; the
           intermediate Parquet is zstd-compressed so disk peak stays
           small. Empty-file branch substitutes a header-only schema
           so consumers see the same schema regardless of input size.

  Phase 2: Count via Parquet footer (sub-second; no data scan).

  Phase 3: POST /api/v1/sequence-range with the exact count. The CP
           function holds an advisory lock for the nextval/setval/INSERT
           critical section and returns the minted (start, stop) range.
           Skipped when count = 0 (the CP rejects count <= 0; empty
           samples just write an empty final Parquet with the full
           schema and no minted range).

  Phase 4: Read intermediate + assign sequence_idx via
           `start + row_number() OVER (ORDER BY read_id) - 1`, write
           the final Parquet sorted by sequence_idx (which is
           monotonic in read_id by construction).

  Phase 5 (try/finally): cleanup intermediate + DuckDB temp_directory
           before returning. The SLURM launcher's manifest walker runs
           AFTER execute() returns, so the transient files are
           invisible to it — the manifest sees only reads.parquet.

DuckDB settings applied on every connection:

  - `max_memory='{N}GB'`     : cap RAM so SLURM cgroups don't OOM-kill.
  - `max_threads={N}`        : match the cgroup cpu allocation; defaults
                               try to use all host cores.
  - `preserve_insertion_order=false` : let DuckDB parallelize freely.
                               Determinism is guaranteed by the explicit
                               ORDER BY read_id in phase 4 (both as the
                               window-function ordering and the COPY's
                               output ordering).
  - `temp_directory='{workspace}/.duckdb_tmp'` : spill on the same fast
                               scratch as the workspace, not the system
                               /tmp (which is often small tmpfs).

The `max_memory` and `max_threads` values are conservative hardcodes
in this commit; a follow-up should plumb them from JobParams /
baseline_resources so each step's allocation drives DuckDB's own
limits.

Recovery semantics. If execute() crashes between phase 3 (mint) and
phase 4's COPY, the prep_sample's sequence_range row already exists.
A retry's POST returns 409 (SequenceRangeAlreadyExists); the helper
raises and the runner classifies as UNKNOWN_PERMANENT. Operator
recovery: DELETE the prep_sample (CASCADE removes the range) and
resubmit the work_ticket. See sequence_range.py module docstring for
the longer-term GET-on-mint-scope follow-up that would make retries
transparent.

Sibling: `LocalBackend._run_hash` in `backends/local.py` is
structurally similar — same DuckDB+miint plumbing, same `PARQUET_OPTS`,
same use of `read_fastx` — but is a *reference-side dedup* job, not a
per-sample ingest. _run_hash rejects duplicate read_ids and writes a
manifest sorted by content hash; this job keeps every read and writes
raw reads sorted by the CP-minted sequence_idx. They share mechanics,
not semantics.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
from pydantic import BaseModel
from qiita_common.parquet import validate_parquet_path

from ..miint import PARQUET_OPTS, ensure_miint_installed, open_conn
from ..sequence_range import make_cp_client, mint_sequence_range

# Conservative DuckDB resource caps. TODO: plumb from
# JobParams.baseline_resources so each step's SLURM allocation drives
# DuckDB's own limits. For now these match the YAML's declared
# baseline_resources (cpu=2, mem_gb=4) with headroom for Python+miint.
_DUCKDB_MAX_MEMORY_GB = 2
_DUCKDB_MAX_THREADS = 2


def _apply_duckdb_settings(conn: duckdb.DuckDBPyConnection, duckdb_tmp: Path) -> None:
    """Apply the four standard DuckDB knobs the other dev recommended.
    Called at the top of every connection in the pipeline."""
    conn.execute(f"SET max_memory='{_DUCKDB_MAX_MEMORY_GB}GB'")
    conn.execute(f"SET max_threads={_DUCKDB_MAX_THREADS}")
    conn.execute("SET preserve_insertion_order=false")
    conn.execute(f"SET temp_directory='{duckdb_tmp}'")


class Inputs(BaseModel):
    """Typed input contract for fastq_to_parquet.

    `fastq_path` is the workflow-declared input (the action_context's
    fastq_path flows through here). `prep_sample_idx` and
    `work_ticket_idx` are framework-injected scope scalars merged by
    `flatten_native_inputs`; `prep_sample_idx` is also the key the
    CP's sequence-range allocator uses, so it's load-bearing here
    (not just provenance as the comment used to imply).
    """

    fastq_path: Path
    prep_sample_idx: int
    work_ticket_idx: int


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """B-staged-Parquet pipeline. See module docstring for the
    full pipeline description."""
    if not inputs.fastq_path.exists():
        raise FileNotFoundError(f"FASTQ file not found: {inputs.fastq_path}")

    workspace.mkdir(parents=True, exist_ok=True)
    intermediate = workspace / "_intermediate_reads.parquet"
    out_path = workspace / "reads.parquet"
    out = validate_parquet_path(out_path)
    duckdb_tmp = workspace / ".duckdb_tmp"
    duckdb_tmp.mkdir(parents=True, exist_ok=True)

    await ensure_miint_installed()

    try:
        # Phase 1: FASTQ -> intermediate Parquet.
        with open_conn() as conn:
            _apply_duckdb_settings(conn, duckdb_tmp)
            conn.execute("LOAD miint;")
            try:
                conn.execute(
                    "COPY ( SELECT read_id,"
                    "         sequence1 AS sequence,"
                    "         qual1 AS quality,"
                    "         CAST(length(sequence1) AS BIGINT) AS sequence_length "
                    "FROM read_fastx(?)) "
                    f"TO '{intermediate}' ({PARQUET_OPTS})",
                    [str(inputs.fastq_path)],
                )
            except duckdb.Error as exc:
                if "Empty file" not in str(exc):
                    raise
                # miint refuses zero-byte input. Synthesize an empty
                # intermediate Parquet with the right schema so phases
                # 2+4 stay schema-uniform.
                conn.execute(
                    "CREATE TEMP TABLE _empty ("
                    "  read_id VARCHAR, sequence VARCHAR,"
                    "  quality UTINYINT[], sequence_length BIGINT"
                    ")"
                )
                conn.execute(f"COPY (SELECT * FROM _empty) TO '{intermediate}' ({PARQUET_OPTS})")

        # Phase 2: count via Parquet footer (no scan).
        with open_conn() as conn:
            _apply_duckdb_settings(conn, duckdb_tmp)
            count = conn.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(intermediate)]
            ).fetchone()[0]

        # Phase 3: mint a sequence_idx range from the CP (skipped for
        # empty samples — the CP rejects count <= 0, and an empty file
        # has no reads to key).
        if count > 0:
            async with make_cp_client() as http:
                rng = await mint_sequence_range(
                    http=http, prep_sample_idx=inputs.prep_sample_idx, count=count
                )
            sequence_idx_start = rng.sequence_idx_start
        else:
            # Sentinel — phase 4 won't reference it for an empty input
            # because the SELECT produces zero rows.
            sequence_idx_start = 0

        # Phase 4: rewrite intermediate -> final with sequence_idx
        # assigned and physically sorted on disk.
        with open_conn() as conn:
            _apply_duckdb_settings(conn, duckdb_tmp)
            # ORDER BY read_id inside the window AND on the outer SELECT:
            # the window-function ordering controls sequence_idx assignment
            # (deterministic across runs of the same input); the outer
            # ORDER BY controls the physical row order in the final
            # Parquet (preserve_insertion_order=false means the COPY
            # respects only explicit ORDER BY clauses).
            conn.execute(
                "COPY ( SELECT "
                "  ? + row_number() OVER (ORDER BY read_id) - 1 AS sequence_idx,"
                "  read_id, sequence, quality, sequence_length "
                "FROM read_parquet(?) "
                "ORDER BY read_id ) "
                f"TO '{out}' ({PARQUET_OPTS})",
                [sequence_idx_start, str(intermediate)],
            )
    finally:
        # Clean up transient artifacts BEFORE returning so the SLURM
        # launcher's manifest walker (which runs after execute()) sees
        # only reads.parquet. Best-effort: a hard-killed process leaves
        # these behind in the failed-attempt workspace, but the runner
        # creates a fresh attempt-N+1 dir on retry so it doesn't cascade.
        intermediate.unlink(missing_ok=True)
        shutil.rmtree(duckdb_tmp, ignore_errors=True)

    return {"reads": out_path}
