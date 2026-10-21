"""Repository reads for qiita.prep_sample_qc_check and the qiita.qc_verdict view."""

import asyncpg


async def fetch_qc_checks_by_prep_sample_idx(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection, prep_sample_idx: int
) -> list[asyncpg.Record]:
    """Return every check row for the prep_sample, invalidated ones included."""
    rows = await pool_or_conn.fetch(
        'SELECT "check", severity, reason, mask_idx, alignment_idx,'
        "       measured_value, threshold, work_ticket_idx, created_at,"
        "       state, invalidated_at, invalidation_reason"
        "  FROM qiita.prep_sample_qc_check"
        ' WHERE prep_sample_idx = $1 ORDER BY "check", idx',
        prep_sample_idx,
    )
    return list(rows)


async def fetch_qc_verdict_by_prep_sample_idx(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection, prep_sample_idx: int
) -> asyncpg.Record | None:
    """Return the prep_sample's `qc_verdict` row, or None when no row counts."""
    return await pool_or_conn.fetchrow(
        'SELECT severity, "check" FROM qiita.qc_verdict WHERE prep_sample_idx = $1',
        prep_sample_idx,
    )
