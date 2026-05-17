"""Shared miint extension install + DuckDB connection helpers.

Lives at the sibling level (NOT inside `jobs/`) because `jobs/` is the
native-job package — every non-dunder file there must export an
`Inputs` model and an `execute` coroutine (enforced by
`scan_native_jobs`). Shared helpers go alongside, not inside.

Two callers:
- `qiita_compute_orchestrator.backends.local.LocalBackend` (container
  step path: hash, load).
- `qiita_compute_orchestrator.jobs.fastq_to_parquet` (native step
  path; reads + transforms FASTQ via DuckDB+miint).
Both use `ensure_miint_installed()` to lazily install miint once per
process (concurrency-safe via asyncio.Lock), then `open_conn()` to
materialize a fresh DuckDB connection with the right config — the
caller `LOAD miint;`s the extension on its own connection.

`MIINT_EXTENSION_REPO` env override exists for the team mirror at
https://ftp.microbio.me/pub/miint; without it, install pulls from
DuckDB community-extensions. The override implies
`allow_unsigned_extensions=true` (the mirror's signing chain is the
team's own, not DuckDB's).
"""

from __future__ import annotations

import asyncio
import os

import duckdb

_miint_install_lock = asyncio.Lock()
_miint_installed = False

_MIINT_EXT_REPO = os.environ.get("MIINT_EXTENSION_REPO")

# Canonical DuckDB COPY options for every Parquet file the orchestrator
# writes. Lives here (next to the only DuckDB connection helpers) so a
# Parquet-version or compression bump touches one place. backends/local.py
# extends this with ROW_GROUP_SIZE for the chunked sequence-data write
# (see _PARQUET_OPTS_CHUNKED there); native jobs use this as-is.
#
# Cross-component contract: result files written with these options are
# registered into DuckLake by the Rust data plane (qiita-data-plane,
# DoAction "register"). Any bump (PARQUET_VERSION, COMPRESSION, etc.)
# must be verified against the data plane's pinned DuckDB version
# before merging — orchestrator unit tests don't exercise the read
# side, so a breaking bump would surface only in `make test-integration`
# with a confusing data-plane-side trace.
PARQUET_OPTS: str = "FORMAT PARQUET, PARQUET_VERSION 'v2', COMPRESSION 'zstd'"


def open_conn() -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection. Unsigned-extensions config is enabled
    only when MIINT_EXTENSION_REPO points at a non-default repo (the
    team mirror), since that path serves community-signed binaries."""
    if _MIINT_EXT_REPO is not None:
        return duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    return duckdb.connect(":memory:")


async def ensure_miint_installed() -> None:
    """Install miint once per process, concurrency-safe."""
    global _miint_installed
    if _miint_installed:
        return
    async with _miint_install_lock:
        if _miint_installed:
            return
        with open_conn() as conn:
            if _MIINT_EXT_REPO is not None:
                conn.execute(f"FORCE INSTALL miint FROM '{_MIINT_EXT_REPO}';")
            else:
                conn.execute("INSTALL miint FROM community;")
        _miint_installed = True


# Substring used to recognize miint's "zero records read" exception.
# miint's C++ side throws `std::runtime_error("Empty file: " + path)` from
# SequenceReader.cpp (see duckdb-miint/src/SequenceReader.cpp:63,78) when
# the sequence stream yields no records; DuckDB surfaces this as a plain
# duckdb.Error whose str() carries the C++ message. We can't switch to a
# file-size check because gzipped-empty FASTQs are ~20 bytes (non-zero)
# and zero-record cases include malformed inputs miint can't parse, so
# the substring match is the only signal available today.
#
# Brittle by construction: any reword on the miint side silently breaks
# the empty-fallback path on every caller. Tracked by #39 (typed signal
# from miint). Until that lands, all string-match callers route through
# `is_miint_empty_file_error` so a future reword updates one place.
_MIINT_EMPTY_FILE_SUBSTRING = "Empty file"


def is_miint_empty_file_error(exc: BaseException) -> bool:
    """True iff `exc` is a duckdb.Error from miint reporting zero
    records in the input file. Callers use this to branch into a
    schema-uniform empty-Parquet fallback instead of failing the job
    on legitimately empty inputs (the typical case: an empty
    `.fastq.gz` from a sequencing run that produced no reads)."""
    return isinstance(exc, duckdb.Error) and _MIINT_EMPTY_FILE_SUBSTRING in str(exc)
