"""Shared Parquet helpers used across qiita services."""

from pathlib import Path

# DuckDB COPY option sets for the Parquet qiita writes, one per kind of file. A writer
# picks the set for where its file ends up; there is no unqualified set to reach for.
#
#   PARQUET_OPTS_AT_REST       a file the data plane registers into DuckLake. Registration
#                              moves the writer's bytes into the lake without rewriting
#                              them, so the writer is the only place their compression is
#                              decided.
#   PARQUET_OPTS_TRANSFER      a file that leaves the job but not into the lake: an input a
#                              later workflow step reads, a file the CLI leaves on the
#                              user's disk.
#   PARQUET_OPTS_INTERMEDIATE  a file a later phase of the same job reads once and deletes.
#
# Single-sourced here because qiita-common is the one module both the compute orchestrator
# and the control plane depend on. The orchestrator (`qiita_compute_orchestrator.miint`)
# re-exports these and derives the chunked variants.
#
# ROW_GROUP_SIZE_BYTES '64MB' (all three sets) caps each row group by encoded size (on top
# of DuckDB's default row-count threshold) so a wide-row result flushes row groups at
# ~64 MB instead of buffering one giant group — sharper row-group predicate pushdown
# (tighter per-group min/max) and lower peak write memory. It requires
# `SET preserve_insertion_order=false` on the writing connection (DuckDB errors at bind
# time otherwise). A COPY whose query ends in ORDER BY does not get the cap; a sorted
# writer of wide rows adds the row count `sorted_row_group_size` gives it.


def _copy_options(codec: str, level: int | None = None) -> str:
    level_option = "" if level is None else f", COMPRESSION_LEVEL {level}"
    return (
        "FORMAT PARQUET, PARQUET_VERSION 'v2',"
        f" COMPRESSION '{codec}'{level_option}, ROW_GROUP_SIZE_BYTES '64MB'"
    )


# The codec, as its own constant rather than a literal inside the sets: a pyarrow
# `ParquetWriter` takes it directly, so the writers that bypass DuckDB name this instead of
# spelling it again. `ROW_GROUP_SIZE_BYTES` below exists for the same reason.
PARQUET_COMPRESSION: str = "zstd"

# The zstd level for data at rest, measured on DuckDB 1.5.5's Parquet writer. On one
# production long-read prep_sample it took sequence1 from 1.314 to 1.123 bits/base on disk
# (-14.6%); level 19 compressed the same at about half the write speed, and levels 4-12
# found fewer repeats in DNA than level 3. The cost is write CPU: ~6 MB/s per thread on DNA
# against ~350 MB/s at level 3. Decompression speed does not depend on the level.
PARQUET_COMPRESSION_LEVEL_AT_REST: int = 16
PARQUET_OPTS_AT_REST: str = _copy_options(PARQUET_COMPRESSION, PARQUET_COMPRESSION_LEVEL_AT_REST)

# zstd at DuckDB's default level: these files are read again but never kept, so the
# at-rest level's write cost buys nothing that lasts.
PARQUET_OPTS_TRANSFER: str = _copy_options(PARQUET_COMPRESSION)

# snappy decompresses faster than zstd at the cost of a larger file, which suits a file
# whose lifetime is "until the next phase reads it".
PARQUET_COMPRESSION_INTERMEDIATE: str = "snappy"
PARQUET_OPTS_INTERMEDIATE: str = _copy_options(PARQUET_COMPRESSION_INTERMEDIATE)

# The `ROW_GROUP_SIZE_BYTES '64MB'` cap above, as an int, for writers that size row
# groups themselves. DuckDB's MB is decimal (its MiB is binary), so this is 64,000,000;
# a test pins the two equal. Sizing by encoded bytes (not a fixed row count) is what
# keeps row groups sane across qiita's varying row widths — a fixed row count large
# enough for narrow reads is far too big for wide rows.
ROW_GROUP_SIZE_BYTES: int = 64_000_000

# DuckDB's Parquet writer, by rows: a row group of this many unless a COPY says otherwise,
# built from vectors of this many.
_DUCKDB_ROW_GROUP_SIZE: int = 122_880
_DUCKDB_VECTOR_SIZE: int = 2_048


def sorted_row_group_size(bytes_per_row: int) -> int:
    """`ROW_GROUP_SIZE` for a COPY whose query ends in ORDER BY, given its rows' mean width.

    A sorted COPY writes one file in sort order, each row group a contiguous run of the
    key, but DuckDB 1.5.5 cuts those row groups by row count alone. ROW_GROUP_SIZE_BYTES
    does not apply, and a group closes at the first whole vector at or past ROW_GROUP_SIZE
    (122,880 rows unless set), so 122,880 HiFi reads become one ~3.7 GB row group. The
    writer compresses each row group on one thread, so a level-16 write of such a group
    runs on one core however many threads the connection has.

    Returns the most whole vectors that fit ROW_GROUP_SIZE_BYTES, never more rows than
    DuckDB's default (so narrow rows keep the groups an unsorted write would give them) and
    never less than one vector, the least DuckDB writes. Rows wider than
    ROW_GROUP_SIZE_BYTES / 2,048 therefore get one-vector groups past the cap."""
    if bytes_per_row < 1:
        raise ValueError(f"bytes_per_row must be positive, got {bytes_per_row}")
    vectors = ROW_GROUP_SIZE_BYTES // bytes_per_row // _DUCKDB_VECTOR_SIZE
    return min(_DUCKDB_ROW_GROUP_SIZE, max(1, vectors) * _DUCKDB_VECTOR_SIZE)


def validate_parquet_path(path: Path) -> str:
    """Reject paths with characters that can't be safely string-interpolated
    into a DuckDB COPY statement. The COPY target is a SQL string literal
    so we can't bind it as a parameter; sanitise instead."""
    text = str(path)
    if "'" in text or "\\" in text or any(ord(c) < 0x20 for c in text):
        raise ValueError(f"Output path contains unsafe characters: {text}")
    return text


# Media type for a Parquet response body. Apache registered
# `application/vnd.apache.parquet` in 2024; it is what the genome-map routes
# serve and what a client asserts it received. Lives here, beside the write
# options, so the server and the CLI name one string.
PARQUET_MEDIA_TYPE: str = "application/vnd.apache.parquet"

# The OpenAPI `responses=` a Parquet route declares, so the schema says what the
# body actually is instead of defaulting to JSON.
PARQUET_RESPONSES: dict = {200: {"content": {PARQUET_MEDIA_TYPE: {}}}}
