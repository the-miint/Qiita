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

**Why the two branches bind differently.** A Parquet is bound as a lazy VIEW over
`read_parquet` — unchanged from before, and load-bearing: `qc` streams, so its
peak memory is flat in row count, and materializing a whole sample would
reintroduce exactly the memory-scales-with-input shape that OOM-killed the PacBio
ingest. An Arrow Flight stream cannot be a lazy view: the reader is consumed
ONCE, so a second scan sees nothing, and miint resolves relation names on a
SEPARATE connection where a registered stream relation is invisible (see
docs/duckdb-miint.md). So the stream branch materializes to a real non-temp
TABLE. That is bounded, not unbounded: callers set a DuckDB `memory_limit` and a
`temp_directory` under the job workspace (`apply_duckdb_settings` +
`duckdb_tmp_dir`), so a block larger than the limit spills to node-local disk
rather than growing the heap.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from qiita_common.parquet import validate_parquet_path

from .data_plane_client import open_read_block_stream

if TYPE_CHECKING:
    import duckdb

# The relation a job binds its reads as. A single name so every consumer's SQL
# reads identically regardless of which branch bound it.
READS_RELATION = "step_reads"

# The relation the raw Flight stream is registered as before materialization.
# Distinct from READS_RELATION so the two never collide inside one connection.
_STREAM_RELATION = "_step_reads_stream"


@asynccontextmanager
async def bind_step_reads(
    conn: duckdb.DuckDBPyConnection,
    *,
    reads: Path | None,
    work_ticket_idx: int,
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
    """
    if reads is not None:
        if not reads.exists():
            raise FileNotFoundError(f"reads parquet not found: {reads}")
        # Lazy VIEW — see the module note on why this branch must not materialize.
        conn.execute(
            f"CREATE VIEW {relation} AS "
            f"SELECT * FROM read_parquet('{validate_parquet_path(reads)}')"
        )
        try:
            yield relation
        finally:
            conn.execute(f"DROP VIEW IF EXISTS {relation}")
        return

    async with open_read_block_stream(
        conn, work_ticket_idx=work_ticket_idx, relation=_STREAM_RELATION
    ) as stream_rel:
        # Materialize INSIDE the stream's context: the Flight client stays open
        # for the duration of the scan that drains it, and draining it here lets
        # the client close before the (long) compute runs. Non-temp so miint can
        # resolve it on its own connection.
        conn.execute(f"CREATE TABLE {relation} AS SELECT * FROM {stream_rel}")
    try:
        yield relation
    finally:
        conn.execute(f"DROP TABLE IF EXISTS {relation}")
