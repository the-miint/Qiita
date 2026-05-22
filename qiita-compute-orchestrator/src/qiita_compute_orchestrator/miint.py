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
import gzip
import os
from pathlib import Path

import duckdb

_miint_install_lock = asyncio.Lock()
_miint_installed = False

_MIINT_EXT_REPO = os.environ.get("MIINT_EXTENSION_REPO")

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


def is_empty_sequence_file(path: Path) -> bool:
    """True iff `path` decompresses to zero bytes — i.e., the file
    holds no FASTQ/FASTA content. Callers pre-check with this before
    handing the path to miint's `read_fastx`, which throws
    `std::runtime_error("Empty file: " + path)` on zero-record inputs
    (see duckdb-miint/src/SequenceReader.cpp:63,78). Pre-checking lets
    us route empty inputs through an explicit code path instead of
    catching the exception and matching its wording — see #39 for the
    upstream-fix proposal that would let miint return a 0-row relation
    here, matching `read_csv`'s behavior.

    Why a decompressed-stream peek and not `os.path.getsize == 0`:
    the realistic empty case is a `.fastq.gz` from a sequencing run
    that produced no reads — that file is still ~20 bytes of gzip
    framing on disk, but `gzip.open(...).read(1)` returns `b""`.

    Files with bytes but no parseable records (malformed FASTQ — stray
    whitespace, comment lines) report False here and surface as a
    duckdb.Error from `read_fastx` downstream. That's a real data
    error and should fail loudly, not be silently treated as empty."""
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rb") as f:
        return f.read(1) == b""
