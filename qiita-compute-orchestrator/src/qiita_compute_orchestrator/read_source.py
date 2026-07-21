"""One seam for "bind this step's reads", whatever the source.

A job that consumes a block/sample of reads gets them one of two ways, and the
difference is a property of the WORKFLOW, not of the job:

* **Streamed** (block workflows: read-mask-block, align) — the job mints a
  block-read DoGet ticket at runtime and streams the rows from the data plane
  (`data_plane_client.open_read_block_stream`). No shared-filesystem handoff, and
  the bulk read work happens on the compute node rather than as a synchronous
  Parquet COPY on the control plane's submit path.
* **A local Parquet** (the per-sample read-mask path) — the runner binds the
  durable staging copy `ingest_reads` already wrote, or, when that ephemeral copy
  is gone, one the data plane re-materializes via the `export_read` DoAction.

The per-sample path is deliberately NOT streamed. Its fast path costs the data
plane NOTHING (the file is already on disk from ingest), and its workflow feeds
`reads` to FIVE separate SLURM jobs (syndna, lima_export, lima_mask, qc,
host_filter) — so streaming it would turn zero data-plane calls per sample into
five. Block workflows have the opposite profile: their reads are always
data-plane-sourced, and there are one or two consumers.

`bind_step_reads` hides that split behind one relation name, so a job body reads
the same either way and the per-sample path can migrate later (or not) without
touching a job.

**Both branches end at the same lazy VIEW over a Parquet, and that is the point.**
`qc` streams: its peak memory is flat in row count, and that property is
load-bearing — materializing a whole block into the heap would reintroduce
exactly the memory-scales-with-input shape that OOM-killed 24/26 samples on the
first real PacBio run. A block is tiled to ~10M reads regardless of platform
(`block_planner._BLOCK_TARGET_READS`) against a job DuckDB capped at 8 GB, so
"it will fit" is not a safe assumption and "DuckDB will spill a base table" is
not a behaviour this repo has probed.

So the stream branch does NOT hold the block in DuckDB. It drains the Flight
reader exactly once with `COPY (SELECT * FROM <stream>) TO <workspace>/…parquet`
— a streaming write whose memory is flat in row count — and then binds the same
lazy `read_parquet` view the staged-Parquet branch binds. That also solves the
two constraints a raw stream relation cannot satisfy on its own: the reader is
single-consumption (`align_sharded` scans its reads twice), and miint resolves
relation names on a SEPARATE connection where a registered stream relation is
invisible (see docs/duckdb-miint.md) — a `read_parquet` view resolves there
fine, which is exactly what these jobs did before this change.

The spill file lives in the JOB'S OWN workspace, on node-local scratch, and is
deleted when the binding closes. It is not a step-to-step filepath handoff and
does not reintroduce the shared-filesystem coupling this seam exists to remove:
nothing outside this one job ever learns the path.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from qiita_common.parquet import validate_parquet_path

from .data_plane_client import open_read_block_stream
from .miint import PARQUET_OPTS

if TYPE_CHECKING:
    import duckdb

# The relation a job binds its reads as. A single name so every consumer's SQL
# reads identically regardless of which branch bound it.
READS_RELATION = "step_reads"

# The relation the raw Flight stream is registered as before it is drained.
# Distinct from READS_RELATION so the two never collide inside one connection.
_STREAM_RELATION = "_step_reads_stream"

# Basename of the node-local Parquet the streamed block is drained into, inside
# the job's own workspace. Deleted when the binding closes.
_STREAM_SPILL_FILENAME = "streamed_reads.parquet"


@asynccontextmanager
async def bind_step_reads(
    conn: duckdb.DuckDBPyConnection,
    *,
    reads: Path | None,
    work_ticket_idx: int,
    workspace: Path,
    relation: str = READS_RELATION,
) -> AsyncIterator[str]:
    """Bind this step's reads into `conn` as `relation`, yielding the name.

    `reads` is the runner-bound Parquet when the workflow stages one, and `None`
    when it does not — which is the signal to stream the work ticket's block from
    the data plane. Exactly one source is used; there is no fallback between them,
    because a silent fallback would mask a misconfigured workflow by reading the
    wrong reads (raw where masked was meant, or a stale staging copy where the
    lake is authoritative).

    The bound relation carries the data plane's shared export projection —
    `prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2` —
    on both branches, so a consumer's SQL is source-agnostic.

    A missing Parquet raises `FileNotFoundError` (fail fast, before any DuckDB
    work). A stream failure surfaces as the underlying httpx/Flight error, which
    the native-job dispatcher classifies.

    `conn` must already have been through `miint.apply_duckdb_settings` (every
    caller does, before binding): the stream branch's `COPY` uses the shared
    `PARQUET_OPTS`, whose `ROW_GROUP_SIZE_BYTES` DuckDB rejects unless
    `preserve_insertion_order=false` — which that helper sets. Row order carries
    no meaning here anyway; the retired data-plane export disabled it for the same
    reason, and every consumer (qc per-row, host_filter/align by join) is
    order-independent.
    """
    if reads is not None:
        if not reads.exists():
            raise FileNotFoundError(f"reads parquet not found: {reads}")
        async with _bind_parquet_view(conn, reads, relation) as rel:
            yield rel
        return

    # Drain the Flight reader straight to a node-local Parquet. `COPY` streams
    # row groups to disk, so peak memory is flat in row count rather than in
    # block size — see the module note on why holding the block in DuckDB is not
    # an option. Written INSIDE the stream's context so the Flight client stays
    # open for the scan that drains it and closes before the (long) compute.
    workspace.mkdir(parents=True, exist_ok=True)
    spilled = workspace / _STREAM_SPILL_FILENAME
    try:
        async with open_read_block_stream(
            conn, work_ticket_idx=work_ticket_idx, relation=_STREAM_RELATION
        ) as stream_rel:
            conn.execute(
                f"COPY (SELECT * FROM {stream_rel}) "
                f"TO '{validate_parquet_path(spilled)}' ({PARQUET_OPTS})"
            )
        async with _bind_parquet_view(conn, spilled, relation) as rel:
            yield rel
    finally:
        # The job's own scratch, not an output — never leave it behind, including
        # on failure (the SLURM launcher's manifest walker scans this workspace).
        spilled.unlink(missing_ok=True)


@asynccontextmanager
async def _bind_parquet_view(
    conn: duckdb.DuckDBPyConnection, path: Path, relation: str
) -> AsyncIterator[str]:
    """Bind `path` as a lazy `read_parquet` VIEW named `relation`.

    The single binding both branches end at (see the module note): lazy, so peak
    memory stays flat in row count, re-scannable, and resolvable by miint on its
    own connection.
    """
    conn.execute(
        f"CREATE VIEW {relation} AS SELECT * FROM read_parquet('{validate_parquet_path(path)}')"
    )
    try:
        yield relation
    finally:
        conn.execute(f"DROP VIEW IF EXISTS {relation}")
