"""Repository reads and writes for qiita.prep_sample_qc_check and the qiita.qc_verdict view."""

import asyncpg
from qiita_common.models import QcCheckResult

from . import require_transaction


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


async def upsert_qc_checks(
    conn: asyncpg.Connection,
    *,
    work_ticket_idx: int,
    mask_idx: int | None,
    alignment_idx: int | None,
    checks: list[QcCheckResult],
) -> int:
    """Upsert one row per check under the producer; a re-run resets its row to `current`."""
    require_transaction(conn)
    await conn.execute(
        "INSERT INTO qiita.prep_sample_qc_check"
        ' (prep_sample_idx, "check", severity, reason, measured_value, threshold,'
        "  mask_idx, alignment_idx, work_ticket_idx)"
        " SELECT p, c, s, r, m, t, $7, $8, $9"
        "   FROM unnest($1::bigint[], $2::text[], $3::text[], $4::text[],"
        "               $5::float8[], $6::float8[]) AS u(p, c, s, r, m, t)"
        ' ON CONFLICT (prep_sample_idx, "check", mask_idx, alignment_idx) DO UPDATE SET'
        "  severity = EXCLUDED.severity, reason = EXCLUDED.reason,"
        "  measured_value = EXCLUDED.measured_value, threshold = EXCLUDED.threshold,"
        "  work_ticket_idx = EXCLUDED.work_ticket_idx, created_at = now(),"
        "  state = DEFAULT, invalidated_at = NULL, invalidated_by_idx = NULL,"
        "  invalidation_reason = NULL",
        [c.prep_sample_idx for c in checks],
        [str(c.check) for c in checks],
        [str(c.severity) for c in checks],
        [c.reason for c in checks],
        [c.measured_value for c in checks],
        [c.threshold for c in checks],
        mask_idx,
        alignment_idx,
        work_ticket_idx,
    )
    return len(checks)
