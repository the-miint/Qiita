"""Shared Parquet helpers used across qiita services."""

from pathlib import Path

# Canonical DuckDB COPY options for the Parquet artifacts qiita writes.
# Single-sourced HERE because qiita-common is the one module both the
# compute orchestrator and the control plane depend on, so a Parquet-version,
# compression, or row-group bump touches exactly one place. The orchestrator
# (`qiita_compute_orchestrator.miint`) re-exports these and derives the chunked
# variant; the control plane imports PARQUET_OPTS directly for its mint write.
#
# ROW_GROUP_SIZE_BYTES '64MB' caps each row group by encoded size (on top of
# DuckDB's default row-count threshold) so a wide-row result flushes row groups
# at ~64 MB instead of buffering one giant group — sharper row-group predicate
# pushdown (tighter per-group min/max) and lower peak write memory. It REQUIRES
# `SET preserve_insertion_order=false` on the writing connection (DuckDB errors
# at bind time otherwise). It does NOT produce a globally byte-sorted file —
# under parallel writes row groups land in thread-finish order — but each row
# group stays clustered on the COPY's ORDER BY key, which is what catalog
# pruning and row-group pushdown read (min/max stats, not physical row order).
PARQUET_OPTS: str = (
    "FORMAT PARQUET, PARQUET_VERSION 'v2', COMPRESSION 'zstd', ROW_GROUP_SIZE_BYTES '64MB'"
)

# Same shape with COMPRESSION 'snappy' — for transient/intermediate files read
# once by a later pipeline phase then deleted (snappy decompresses faster than
# zstd at the cost of a larger on-disk file, the right tradeoff for a file whose
# lifetime is "until the next phase reads it"). NOT for files the data plane
# registers into DuckLake (those want zstd's smaller long-term footprint; see
# PARQUET_OPTS). Carries the same ROW_GROUP_SIZE_BYTES cap and the same
# preserve_insertion_order=false requirement.
PARQUET_OPTS_INTERMEDIATE: str = (
    "FORMAT PARQUET, PARQUET_VERSION 'v2', COMPRESSION 'snappy', ROW_GROUP_SIZE_BYTES '64MB'"
)

# The `ROW_GROUP_SIZE_BYTES '64MB'` cap above, as an int, for write paths that
# size row groups in Python rather than via a DuckDB COPY (e.g. a pyarrow
# `ParquetWriter` fed a Flight stream — the admin masked-read export). Sizing by
# encoded bytes (not a fixed row count) is what keeps row groups sane across
# qiita's varying row widths — a fixed row count large enough for narrow reads is
# far too big for wide rows. Keep in sync with the literal in PARQUET_OPTS.
ROW_GROUP_SIZE_BYTES: int = 64 * 1024 * 1024


def validate_parquet_path(path: Path) -> str:
    """Reject paths with characters that can't be safely string-interpolated
    into a DuckDB COPY statement. The COPY target is a SQL string literal
    so we can't bind it as a parameter; sanitise instead."""
    text = str(path)
    if "'" in text or "\\" in text or any(ord(c) < 0x20 for c in text):
        raise ValueError(f"Output path contains unsafe characters: {text}")
    return text
