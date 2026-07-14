"""Shared miint extension install + DuckDB connection helpers.

Lives at the sibling level (NOT inside `jobs/`) because `jobs/` is the
native-job package — every non-dunder file there must export an
`Inputs` model and an `execute` coroutine (enforced by
`scan_native_jobs`). Shared helpers go alongside, not inside.

miint is **not** installed at runtime here. The deploy stages it once into the
shared `MIINT_EXTENSION_DIRECTORY` (`stage_miint_extension`, driven by
`scripts/stage-miint-extension.sh`); every runtime path on the cluster — native
jobs and the compute-readiness probe — only `LOAD`s it via `open_miint_conn()`.
That removes the per-job download, the compute-node mirror dependency, and the
writable-`$HOME` requirement the old lazy `FORCE INSTALL` carried.

miint installs from the team mirror by default — the same build across all
Qiita components, no community-vs-mirror patchwork (`MIINT_EXTENSION_REPO`
overrides for a local/dev build). Because the source is always a mirror (its
signing chain is the team's own, not DuckDB's), `allow_unsigned_extensions=true`
is always set. The install/load SQL and connect config are single-sourced in
`qiita_common.duckdb_miint`.
"""

from __future__ import annotations

import math
import os
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb
from qiita_common.chunking import CHUNK_ROW_GROUP_SIZE
from qiita_common.duckdb_miint import (
    miint_connect_config,
    miint_install_sql,
    miint_load_sql,
)
from qiita_common.parquet import (
    PARQUET_OPTS,
    PARQUET_OPTS_INTERMEDIATE,  # noqa: F401  re-exported for jobs that import it from here
)

from .miint_staging import write_staging_marker

# PARQUET_OPTS / PARQUET_OPTS_INTERMEDIATE are single-sourced in
# `qiita_common.parquet` (the one module this service and the control plane both
# depend on) so a Parquet-version, compression, or row-group bump touches ONE
# place. They are imported above and re-exported here for the jobs that pull
# them via `from ..miint import PARQUET_OPTS`; see that module for the
# ROW_GROUP_SIZE_BYTES / preserve_insertion_order semantics.
#
# Cross-component contract: result files written with these options are
# registered into DuckLake by the Rust data plane (qiita-data-plane, DoAction
# "register"). Any bump (PARQUET_VERSION, COMPRESSION, etc.) must be verified
# against the data plane's pinned DuckDB version before merging — orchestrator
# unit tests don't exercise the read side, so a breaking bump would surface only
# in `make test-integration` with a confusing data-plane-side trace.
#
# ROW_GROUP_SIZE_BYTES (carried by both) requires preserve_insertion_order=false,
# which `apply_duckdb_settings` below sets on every pipeline connection.

# Chunked-sequence write constants. Sequence data (genome-scale up to
# ~21 MB per record on GG2) is broken into 64 KB chunks so the DuckLake
# row layout stays narrow on long entries. DuckDB flushes a row group on
# whichever cap it hits first: the ROW_GROUP_SIZE row count (16384 rows ×
# ~64 KB chunk_data ≈ 1 GB, empirically tuned against GG2 backbone —
# 4.2 GB peak RSS; 32768 OOMs on 30 GB hosts) OR the ROW_GROUP_SIZE_BYTES
# '64MB' size cap inherited from PARQUET_OPTS. On dense chunk data the
# 64 MB cap binds first (~1024 chunks), so row groups land at ~64 MB
# rather than ~1 GB — strictly lower write memory and finer pruning, with
# the row-count cap still backstopping sparse/narrow records.
#
# CHUNK_ROW_GROUP_SIZE (and the matching CHUNK_SIZE) are single-sourced in
# `qiita_common.chunking`, shared with the CLI's DoPut path. The actual
# chunking is miint's native `sequence_split` over read_fastx, in
# `stage_local_fasta` (which builds the expression via
# `qiita_common.chunking.sequence_split_expr`).
PARQUET_OPTS_CHUNKED: str = f"{PARQUET_OPTS}, ROW_GROUP_SIZE {CHUNK_ROW_GROUP_SIZE}"


# DuckDB resource caps for native jobs.
#
# Native jobs running under SLURM share their cgroup with the wrapping Python
# process + miint runtime + OS overhead, plus (in some steps) an in-process
# co-consumer like rype/minimap2. DuckDB's `memory_limit` only bounds DuckDB
# itself, so a job must set it BELOW the cgroup. The size each job wants is
# resolved by `resolve_duckdb_memory_gb()` from the real cgroup
# (`slurm_alloc_gb()` / `SLURM_MEM_PER_NODE`) so a per-run `--mem-gb` override
# reaches DuckDB; a per-job literal is only the off-SLURM fallback.
# Threads are still passed as a literal (the cgroup cpu allocation) and also size
# the headroom — see `duckdb_headroom_gb`.


@contextmanager
def duckdb_tmp_dir(workspace: Path) -> Iterator[Path]:
    """Create the `<workspace>/.duckdb_tmp` spill directory and remove it on exit.

    Every native DuckDB job spills to this dir — it is what `apply_duckdb_settings`
    hands DuckDB as its `temp_directory`. Routing all of them through this context
    manager makes teardown **structural**: the spill dir (which can grow large and
    otherwise accumulates in the shared work-ticket workspace — SLURM has hit "no
    space in /tmp" from leftover spill) is removed whether the body returns or
    raises. `ignore_errors=True` so cleanup never masks the job's own failure.

    Yields the spill dir to hand to `apply_duckdb_settings`."""
    duckdb_tmp = workspace / ".duckdb_tmp"
    duckdb_tmp.mkdir(parents=True, exist_ok=True)
    try:
        yield duckdb_tmp
    finally:
        shutil.rmtree(duckdb_tmp, ignore_errors=True)


def apply_duckdb_settings(
    conn: duckdb.DuckDBPyConnection,
    duckdb_tmp: Path,
    *,
    memory_gb: int,
    threads: int,
) -> None:
    """Apply the four DuckDB settings every chunked-Parquet pipeline
    connection needs:

    - `memory_limit='{N}GB'` — cap RAM so SLURM cgroups don't OOM-kill.
    - `threads={N}` — bound parallelism. The default would try to use
      all host cores, which can blow `memory_limit` because parallel
      operators (HASH_AGG, sort) keep per-thread state.
    - `preserve_insertion_order=false` — let DuckDB parallelize freely.
      All chunked-sequence pipelines reconstruct order via an explicit
      `chunk_index` (or per-file `sequence_index`), so DuckDB doesn't
      need to preserve scan order. Required for the chunked-sequence
      write path (see `feedback_sequence_chunking`) — without it the
      vectorized engine buffers row groups in memory rather than
      flushing eagerly, OOMing on genome-heavy uploads.
    - `temp_directory='{workspace}/.duckdb_tmp'` — spill on the same
      fast scratch as the workspace, not the system /tmp (which is
      often small tmpfs).

    `memory_gb` and `threads` are the actual DuckDB caps the caller
    wants set — NOT the workflow YAML's allocation. The caller is
    responsible for choosing values that fit (a) the SLURM cgroup
    allocation (memory_gb < yaml_mem_gb), and (b) the query shape's
    parallel-state cost (threads may need to be below yaml_cpu for
    HASH_AGG-heavy workloads — see hash_sequences). A future refactor
    should derive these from JobParams.baseline_resources plus per-job
    overrides, eliminating the duplicated literals."""
    conn.execute(f"SET memory_limit='{memory_gb}GB'")
    conn.execute(f"SET threads={threads}")
    conn.execute("SET preserve_insertion_order=false")
    conn.execute(f"SET temp_directory='{duckdb_tmp}'")


# Headroom reserved below the SLURM cgroup ceiling so DuckDB's `memory_limit`
# (and any in-process co-consumer's budget) sits under the cgroup — otherwise the
# kernel OOM-kills the whole step instead of DuckDB raising a catchable
# OutOfMemory. Two components:
#
#  - a flat base for the Python interpreter + miint/OS overhead, and
#  - a per-thread term, because `memory_limit` is a SOFT target DuckDB overshoots
#    in actual RSS, and the overshoot grows with parallelism (per-thread operator
#    state — sort/HASH_AGG runs that DuckDB doesn't perfectly bound). A flat
#    headroom that is fine for the 4-thread steps is tight for the 8-thread `load`
#    step; fastq_to_parquet's R11 note measured ~2.4 GB *resident* per thread on
#    long-read work, most of it inside the limit. We reserve ~0.5 GB/thread on
#    TOP of the limit as the above-limit margin — an envelope sized for the
#    8-thread `load` step, to refine against a real genome-scale MaxRSS.
DUCKDB_BASE_HEADROOM_GB = 2
DUCKDB_PER_THREAD_HEADROOM_GB = 0.5


def slurm_alloc_gb() -> int | None:
    """The step's true memory ceiling (GB) from the SLURM cgroup, or None off SLURM.

    The SLURM launcher submits each step with `memory_per_node` (i.e. `--mem`), so
    SLURM exports `SLURM_MEM_PER_NODE` (in MB) into the job environment — the
    authoritative per-step allocation, and the ONLY channel by which the per-run
    `--mem-gb` override reaches a job's in-process memory caps. The local
    backend and unit tests run with the var absent → returns None, and callers fall
    back to their YAML-baseline-derived literal. A malformed value is treated as
    absent (fail soft to the literal rather than crash a job over an env quirk)."""
    raw = os.environ.get("SLURM_MEM_PER_NODE")
    if not raw:
        return None
    try:
        return int(raw) // 1024
    except ValueError:
        return None


def duckdb_headroom_gb(threads: int) -> int:
    """Headroom (GB) to keep below the cgroup ceiling for a `threads`-wide DuckDB
    run: the flat base plus a per-thread above-limit margin (see the constants).
    Rounds the per-thread term up so the margin is never under-reserved."""
    return DUCKDB_BASE_HEADROOM_GB + math.ceil(threads * DUCKDB_PER_THREAD_HEADROOM_GB)


def resolve_duckdb_memory_gb(
    fallback_gb: int, *, threads: int, reserve_gb: int = 0, cap_gb: int | None = None
) -> int:
    """DuckDB `memory_limit` (GB) sized to the real SLURM allocation, not a literal.

    Under SLURM the limit tracks the cgroup: ``alloc - headroom(threads) -
    reserve_gb``, which is how a `--mem-gb` override finally reaches DuckDB. Off
    SLURM (`slurm_alloc_gb()` is None) it returns `fallback_gb` — the job's
    existing YAML-baseline-derived literal — so the local backend and tests are
    unchanged.

    `threads` sizes the headroom (DuckDB's above-limit RSS overshoot scales with
    parallelism — see `duckdb_headroom_gb`); pass the same value handed to
    `apply_duckdb_settings`. `reserve_gb` carves the cgroup out for an in-process
    co-consumer that shares the box with DuckDB (rype / minimap2 do their heavy
    work in-process). `cap_gb` bounds DuckDB's share even when the allocation is
    large — a co-consumer job wants DuckDB modest (it only feeds/reassembles
    chunks), not allocation-sized. Never returns < 1."""
    alloc = slurm_alloc_gb()
    resolved = fallback_gb if alloc is None else alloc - duckdb_headroom_gb(threads) - reserve_gb
    if cap_gb is not None:
        resolved = min(resolved, cap_gb)
    return max(1, resolved)


def open_conn() -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB connection with the miint-load config: unsigned
    extensions are always allowed (miint installs from a mirror, not a
    DuckDB-signed channel), and the extension_directory is used when
    MIINT_EXTENSION_DIRECTORY is set. Config resolution is shared with the CLI
    via `qiita_common.duckdb_miint.miint_connect_config`. Does **not** LOAD
    miint — use `open_miint_conn()` when you need the extension."""
    return duckdb.connect(":memory:", config=miint_connect_config())


def open_miint_conn() -> duckdb.DuckDBPyConnection:
    """Open a fresh DuckDB connection with the miint extension LOADed.

    LOAD-only: miint is pre-staged into MIINT_EXTENSION_DIRECTORY at deploy, so
    this never installs, downloads, or touches the mirror. If the extension is
    missing from the staged directory (a forgotten/failed deploy stage), the
    LOAD raises a DuckDB error — fail loud, as intended. The caller owns the
    connection and must close it."""
    conn = open_conn()
    conn.execute(miint_load_sql())
    return conn


def stage_miint_extension() -> str:
    """Deploy-time staging: FORCE INSTALL miint into the configured
    extension_directory, then LOAD it to prove the staged build is usable.

    Runs **once per deploy** (via `scripts/stage-miint-extension.sh`), not per
    job — so FORCE (refresh to the mirror's current build) is the right call
    here, unlike the retired per-job install. Returns the resolved
    extension_directory for the caller to report (or the DuckDB default marker
    when MIINT_EXTENSION_DIRECTORY is unset, e.g. in a dev/test stage)."""
    with open_conn() as conn:
        conn.execute(miint_install_sql(force=True))
        conn.execute(miint_load_sql())
        # Install the GPL-boundary tool host ONCE, here at deploy time — not per
        # job. Several GPL-licensed tools (bowtie2 alignment, vsearch, …) run
        # out-of-process behind this boundary; a native job that uses one only
        # LOADs miint and relies on the boundary being pre-installed. Idempotent
        # (a no-op once the binary is cached), and this stage runs as the account
        # that owns the extension directory (qiita-orch in prod), the same account
        # the SLURM jobs run as, so the cached binary is reachable at job runtime.
        conn.execute("SELECT install_gpl_boundary()")
    # Record the staged build's fingerprint so the next deploy can skip a
    # redundant FORCE INSTALL when the mirror hasn't moved (see miint_staging).
    write_staging_marker()
    return os.environ.get("MIINT_EXTENSION_DIRECTORY", "<duckdb default ~/.duckdb>")
