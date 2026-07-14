"""Reassemble feature-keyed sequence chunks into an aligner `(read_id, sequence1)`
subject table.

Shared by the aligner-subject builders (`jobs/build_minimap2_index`,
`jobs/build_bowtie2_index`): both minimap2 and bowtie2 index the SAME per-feature
subject — the whole reference contig reassembled from its 64 KB chunks — and both
miint save functions read it by TABLE NAME on a separate bind/execute connection.
Single-sourcing the reassembly here keeps the two builders' subjects byte-identical
and removes the duplicated `string_agg ... GROUP BY` SQL each carried.

Lives OUTSIDE `jobs/` on purpose: the boot scan (`scan_native_jobs`) validates
every non-dunder module under `jobs/` as a native job (exactly `Inputs` +
`execute`), so shared helpers are siblings. It is NOT in `qiita_common.chunking`
either — that module stays pure SQL-string text with no `duckdb` import; this one
executes against a live connection.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from qiita_common.chunking import reassemble_chunks_expr

if TYPE_CHECKING:
    import duckdb


def stage_subject(
    conn: duckdb.DuckDBPyConnection,
    source: str,
    *,
    subject_table: str,
) -> int:
    """Materialise a `(read_id, sequence1)` subject TABLE from feature-keyed
    chunk rows and return its row count.

    `source` is any FROM-able relation operand carrying `(feature_idx,
    chunk_index, chunk_data)` columns: a registered stream relation NAME (the
    `stream_reference_chunks` relation) OR a `read_parquet('…')` expression (the
    host-mode staging path). The chunks are reassembled per feature via
    `string_agg(chunk_data ORDER BY chunk_index)` (`reassemble_chunks_expr`, the
    single-sourced inverse of the chunking split), so the subject is independent
    of scan order; `read_id` is the `feature_idx` (it surfaces as the alignment
    `reference` column). No `feature_idx` subsetting happens here — the stream is
    already roster-scoped by its DoGet ticket, and the host source is the whole
    reference.

    A non-temp TABLE (not a VIEW or TEMP): the `GROUP BY` reassembly is a blocking
    aggregation (materialise once rather than recompute on every scan the aligner
    issues), and miint's `save_*_index` opens a SEPARATE connection on the same
    DuckDB instance during bind/execute, which resolves regular `view`/`table`
    names but not TEMP tables / CTEs (see docs/duckdb-miint.md).

    `source` is inlined into the DDL (DuckDB rejects prepared parameters inside
    CREATE TABLE AS); the caller is responsible for escaping any path it embeds
    (host mode uses `validate_parquet_path`), and a stream relation name is a
    controlled identifier.
    """
    conn.execute(
        f"CREATE OR REPLACE TABLE {subject_table} AS "
        "SELECT feature_idx AS read_id, "
        f"{reassemble_chunks_expr()} AS sequence1 "
        f"FROM {source} GROUP BY feature_idx"
    )
    return conn.execute(f"SELECT count(*) FROM {subject_table}").fetchone()[0]
