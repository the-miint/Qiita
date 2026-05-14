"""Repository functions for the qiita.sequencing_run and qiita.sequenced_pool tables.

Direct functions cover the run-level row and the per-pool row that attaches
to it. Subtype-level rows for individual sequenced samples live in the
sibling prep_sample module, alongside the composer that ties the whole
sequencing-ingestion chain together.

Write functions take an asyncpg.Connection as their first positional
argument, never acquire their own connection, and never open their own
top-level transaction; the caller controls transaction scope so multiple
calls compose atomically on one connection. Read functions accept either
a pool or a connection so they compose inside an open transaction or
stand alone.
"""

import json
from datetime import datetime
from typing import Any

import asyncpg
from qiita_common.models import Platform


async def insert_sequencing_run(
    conn: asyncpg.Connection,
    *,
    instrument_run_id: str,
    platform: Platform,
    created_by_idx: int,
    instrument_model: str | None = None,
    instrument_serial: str | None = None,
    run_performed_at: datetime | None = None,
    extra_metadata: dict[str, Any] | None = None,
) -> int:
    """Insert a row into qiita.sequencing_run and return the generated idx.

    Exposes every column the caller may legitimately set on a fresh row:
    the instrument-assigned id, the platform enum, the optional model /
    serial / performed-at metadata, and the free-form extra_metadata JSONB.
    Retirement and audit-timestamp columns are populated by triggers,
    defaults, or schema CHECKs and are not parameters of this function.

    Raises asyncpg.UniqueViolationError if instrument_run_id collides with
    an existing row, asyncpg.PostgresError on other constraint failures.
    """
    # extra_metadata is serialised to JSONB; asyncpg has no default jsonb
    # codec, so the dict is JSON-encoded here and cast on the SQL side
    # via $6::jsonb (see _encode_jsonb).
    return await conn.fetchval(
        "INSERT INTO qiita.sequencing_run ("
        "    instrument_run_id, platform, instrument_model, instrument_serial,"
        "    run_performed_at, extra_metadata, created_by_idx"
        ") VALUES ($1, $2::qiita.platform, $3, $4, $5, $6::jsonb, $7)"
        " RETURNING idx",
        instrument_run_id,
        platform,
        instrument_model,
        instrument_serial,
        run_performed_at,
        _encode_jsonb(extra_metadata),
        created_by_idx,
    )


async def fetch_sequencing_run(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    sequencing_run_idx: int,
) -> asyncpg.Record | None:
    """Return the qiita.sequencing_run row for the given idx, or None on miss.

    Selects the caller-visible column set on the row so the route's
    row-to-response shaping has a single source of truth. Accepts either
    a pool or a connection so the helper composes inside an open
    transaction or stands alone.
    """
    # Single-row fetch by idx; column list mirrors the future
    # SequencingRunResponse shape (idx -> sequencing_run_idx at the route).
    return await pool_or_conn.fetchrow(
        "SELECT idx, instrument_run_id, platform, instrument_model,"
        " instrument_serial, run_performed_at, extra_metadata,"
        " created_by_idx, created_at,"
        " retired, retired_by_idx, retired_at, retire_reason"
        " FROM qiita.sequencing_run WHERE idx = $1",
        sequencing_run_idx,
    )


async def fetch_sequencing_run_exists(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    sequencing_run_idx: int,
) -> bool:
    """Return True iff a qiita.sequencing_run row exists at this idx.

    Exposed separately from fetch_sequencing_run so route handlers can run
    a cheap pre-flight 404 check without pulling the full row across the
    wire. Retired runs still return True — the read surface decides what
    to do with the retirement flag once the row is in hand.
    """
    return await pool_or_conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM qiita.sequencing_run WHERE idx = $1)",
        sequencing_run_idx,
    )


async def insert_sequenced_pool(
    conn: asyncpg.Connection,
    *,
    sequencing_run_idx: int,
    run_preflight_blob: bytes,
    run_preflight_filename: str,
    created_by_idx: int,
    extra_metadata: dict[str, Any] | None = None,
) -> int:
    """Insert a row into qiita.sequenced_pool and return the generated idx.

    Exposes every column the caller may legitimately set on a fresh row:
    the FK back to the sequencing_run, the run-preflight blob and its
    originating filename (both NOT NULL in the schema), and the free-form
    extra_metadata JSONB. Retirement and audit-timestamp columns are
    populated by triggers, defaults, or schema CHECKs and are not
    parameters of this function.

    Raises asyncpg.ForeignKeyViolationError on a bad sequencing_run_idx,
    asyncpg.PostgresError on other constraint failures.
    """
    # BYTEA accepts the raw bytes object; JSONB requires a JSON string
    # cast (asyncpg has no default jsonb codec — see _encode_jsonb).
    return await conn.fetchval(
        "INSERT INTO qiita.sequenced_pool ("
        "    sequencing_run_idx, run_preflight_blob, run_preflight_filename,"
        "    extra_metadata, created_by_idx"
        ") VALUES ($1, $2, $3, $4::jsonb, $5)"
        " RETURNING idx",
        sequencing_run_idx,
        run_preflight_blob,
        run_preflight_filename,
        _encode_jsonb(extra_metadata),
        created_by_idx,
    )


async def fetch_sequenced_pool(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    sequenced_pool_idx: int,
) -> asyncpg.Record | None:
    """Return the qiita.sequenced_pool row for the given idx, or None on miss.

    Selects the caller-visible column set so a future read surface has a
    single source of truth for row -> response shaping. Accepts either a
    pool or a connection so the helper composes inside an open transaction
    or stands alone.
    """
    # Column list mirrors the future SequencedPoolResponse shape; the BYTEA
    # blob is returned as-is so callers can byte-compare round-trips.
    return await pool_or_conn.fetchrow(
        "SELECT idx, sequencing_run_idx, run_preflight_blob, run_preflight_filename,"
        " extra_metadata, created_by_idx, created_at"
        " FROM qiita.sequenced_pool WHERE idx = $1",
        sequenced_pool_idx,
    )


async def fetch_sequenced_sample_idxs_for_run(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    sequencing_run_idx: int,
    limit: int,
) -> list[int]:
    """Return up to `limit` sequenced_sample idxs reachable from the run.

    Walks the run -> sequenced_pool -> sequenced_sample -> prep_sample
    chain and excludes sequenced_samples whose supertype prep_sample row
    is retired. Sort: (sequenced_sample.created_at DESC, idx DESC) so
    newer rows surface first. Callers that need to detect truncation pass
    `limit = cap + 1`; if the returned list has length > cap, the
    underlying set exceeded the cap.
    """
    # Single round trip; the partial index prep_sample_active_idx covers
    # the retired = false predicate and the join filters down to one run.
    rows = await pool_or_conn.fetch(
        "SELECT ss.idx"
        " FROM qiita.sequenced_sample ss"
        " JOIN qiita.sequenced_pool sp ON sp.idx = ss.sequenced_pool_idx"
        " JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx"
        " WHERE sp.sequencing_run_idx = $1"
        "   AND ps.retired = false"
        " ORDER BY ss.created_at DESC, ss.idx DESC"
        " LIMIT $2",
        sequencing_run_idx,
        limit,
    )
    return [r["idx"] for r in rows]


def _encode_jsonb(value: dict[str, Any] | None) -> str | None:
    """Serialise a Python dict to a JSON string for the JSONB cast.

    asyncpg does not register a default jsonb codec, so values destined for
    a JSONB column must be passed as a string and cast on the SQL side via
    `::jsonb`. None passes through unchanged so a NULL column write is
    representable.
    """
    if value is None:
        return None
    # sort_keys=True keeps the on-wire form deterministic; JSONB does not
    # preserve key order anyway, but a stable encode helps test diffs.
    return json.dumps(value, sort_keys=True)
