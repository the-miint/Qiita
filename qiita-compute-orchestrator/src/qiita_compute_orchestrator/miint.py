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

import os
from pathlib import Path

import duckdb
from qiita_common.chunking import CHUNK_ROW_GROUP_SIZE
from qiita_common.duckdb_miint import (
    miint_connect_config,
    miint_install_sql,
    miint_load_sql,
)

# Canonical DuckDB COPY options for the *final* Parquet artifacts the
# orchestrator writes — the ones the Rust data plane registers into
# DuckLake. Lives here (next to the only DuckDB connection helpers) so
# a Parquet-version or compression bump touches one place.
# backends/local.py extends this with ROW_GROUP_SIZE for the chunked
# sequence-data write (see _PARQUET_OPTS_CHUNKED there); native jobs
# use this as-is for their final output.
#
# Cross-component contract: result files written with these options are
# registered into DuckLake by the Rust data plane (qiita-data-plane,
# DoAction "register"). Any bump (PARQUET_VERSION, COMPRESSION, etc.)
# must be verified against the data plane's pinned DuckDB version
# before merging — orchestrator unit tests don't exercise the read
# side, so a breaking bump would surface only in `make test-integration`
# with a confusing data-plane-side trace.
PARQUET_OPTS: str = "FORMAT PARQUET, PARQUET_VERSION 'v2', COMPRESSION 'zstd'"

# Same shape, but COMPRESSION 'snappy' instead of zstd. Use for
# transient/intermediate Parquet files that are read once by a later
# pipeline phase in the same job and then deleted — snappy decompresses
# noticeably faster than zstd at the cost of larger on-disk files,
# which is the right tradeoff when the file's lifetime is "until the
# next phase reads it." NOT for files the data plane registers into
# DuckLake (those want the smaller zstd footprint for long-term
# storage); see PARQUET_OPTS for that path.
PARQUET_OPTS_INTERMEDIATE: str = "FORMAT PARQUET, PARQUET_VERSION 'v2', COMPRESSION 'snappy'"

# Chunked-sequence write constants. Sequence data (genome-scale up to
# ~21 MB per record on GG2) is broken into 64 KB chunks so the DuckLake
# row layout stays narrow on long entries. ROW_GROUP_SIZE keeps DuckDB
# from buffering an unbounded number of chunks in memory before flush:
# 16384 rows × ~64 KB chunk_data ≈ 1 GB per row group, empirically tuned
# against GG2 backbone (4.2 GB peak RSS; 32768 OOMs on 30 GB hosts).
#
# CHUNK_ROW_GROUP_SIZE (and the matching CHUNK_SIZE) are single-sourced in
# `qiita_common.chunking`, shared with the CLI's DoPut path. The actual
# chunking is miint's native `sequence_split` over read_fastx, in
# `stage_local_fasta` (which builds the expression via
# `qiita_common.chunking.sequence_split_expr`).
PARQUET_OPTS_CHUNKED: str = f"{PARQUET_OPTS}, ROW_GROUP_SIZE {CHUNK_ROW_GROUP_SIZE}"


# DuckDB resource caps for native jobs.
#
# Native jobs running under SLURM share their cgroup with the wrapping
# Python process + miint runtime + OS overhead. DuckDB's `memory_limit`
# only bounds DuckDB itself, so we leave ~1 GB of headroom and set the
# cap to `yaml_mem_gb - 1`. Threads match the cgroup cpu allocation 1:1;
# under-allocating threads only costs throughput, but exceeding
# memory_limit OOM-kills the job.
#
# Each call site passes the YAML numbers as literals — a mismatch with
# the workflow YAML is visible at review time, not a runtime surprise.
# A future refactor should thread `JobParams.baseline_resources` into
# the job at dispatch time so the literals can go away.
#
# Defined here so per-job overrides (or that future plumb) have one
# place to land, rather than three independent copies in the jobs/*
# modules.


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
    return os.environ.get("MIINT_EXTENSION_DIRECTORY", "<duckdb default ~/.duckdb>")
