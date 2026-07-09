"""Shared seam around miint's `rype_classify`.

`host_filter` and `syndna` both ask rype the same question — "which of these reads
matched the index?" — and both answer it as a boolean. Keeping one implementation
means the DISTINCT, the id-type coercion, and the parameter-binding rationale are
stated once rather than drifting between two near-copies.

Both modules re-export this as their own `_run_rype_classify` so their unit tests
can monkeypatch the seam per-module without stubbing the other's.
"""

from __future__ import annotations

from pathlib import Path

import duckdb


def run_rype_classify(
    conn: duckdb.DuckDBPyConnection,
    index_path: Path,
    sequence_table: str,
    dest_table: str,
    *,
    threshold: float,
) -> None:
    """Append the DISTINCT `sequence_idx` set matching `index_path` into `dest_table`.

    Positional args (index path, sequence-table NAME) + `threshold` are bound as
    `?` (INSERT...SELECT is DML, so prepared params are accepted here). DISTINCT
    because the table-function interface does not guarantee one best-hit row per
    read.

    `dest_table.sequence_idx` must be declared BIGINT: rype's `read_id` output type
    is BUILD-DEPENDENT (a BIGINT input has come back VARCHAR on a mirror build), so
    the column coerces it on insert. Never trust the returned type.
    """
    conn.execute(
        f"INSERT INTO {dest_table} "
        "SELECT DISTINCT read_id AS sequence_idx "
        "FROM rype_classify(?, ?, id_column := 'read_id', threshold := ?)",
        [str(index_path), sequence_table, threshold],
    )
