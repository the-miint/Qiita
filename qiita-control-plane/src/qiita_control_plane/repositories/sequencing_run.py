"""Repository functions for the qiita.sequencing_run and qiita.sequenced_pool tables.

Direct functions cover the run-level row and the per-pool row that attaches
to it. Subtype-level rows for individual sequenced samples, the run-scoped
sequenced_sample idx read, and the composer that ties the whole
sequencing-ingestion chain together live in the sibling sequenced_sample
module.

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


class PayloadMismatch(Exception):
    """Raised by a find-or-create repo function when an existing row's natural
    key matches but a supplied non-None field disagrees with the stored value.

    The route layer maps this to 409 with a structured detail carrying the
    field name, the existing value, and the supplied value so the operator
    (or the CLI) can fix the request and retry.
    """

    def __init__(self, field: str, existing_value: Any, supplied_value: Any) -> None:
        super().__init__(
            f"existing {field} differs from supplied value: "
            f"existing={existing_value!r}, supplied={supplied_value!r}"
        )
        self.field = field
        self.existing_value = existing_value
        self.supplied_value = supplied_value


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
) -> tuple[int, bool]:
    """Find-or-create a row in qiita.sequencing_run keyed on instrument_run_id.

    Returns ``(idx, created)``. ``created=True`` means a new row was minted;
    ``created=False`` means the existing row was reused after every supplied
    non-None field matched its stored value. The route uses ``created`` to
    pick HTTP 201 vs 200.

    Exposes every column the caller may legitimately set on a fresh row: the
    instrument-assigned id, the platform enum, the optional model / serial /
    performed-at metadata, and the free-form extra_metadata JSONB. Retirement
    and audit-timestamp columns are populated by triggers, defaults, or
    schema CHECKs and are not parameters of this function.

    Raises:
      PayloadMismatch: a row with this instrument_run_id exists but a
        supplied non-None field disagrees with the stored value. Carries
        the field name and both values for the route's 409 detail.
      asyncpg.PostgresError: other constraint failures (e.g., a malformed
        platform value reaching the ENUM cast).
    """
    # INSERT ... ON CONFLICT DO NOTHING returns NULL on collision; we then
    # SELECT the existing row and compare the supplied non-None fields. A
    # null-supplied field is treated as "caller didn't override" and is not
    # compared, so retrying a previous CLI invocation with the same minimal
    # args (no instrument_model the second time) still converges on 200.
    inserted_idx = await conn.fetchval(
        "INSERT INTO qiita.sequencing_run ("
        "    instrument_run_id, platform, instrument_model, instrument_serial,"
        "    run_performed_at, extra_metadata, created_by_idx"
        ") VALUES ($1, $2::qiita.platform, $3, $4, $5, $6::jsonb, $7)"
        " ON CONFLICT (instrument_run_id) DO NOTHING"
        " RETURNING idx",
        instrument_run_id,
        platform,
        instrument_model,
        instrument_serial,
        run_performed_at,
        _encode_jsonb(extra_metadata),
        created_by_idx,
    )
    if inserted_idx is not None:
        return (inserted_idx, True)

    # Existing row matched the unique key; fetch it for payload comparison.
    existing = await conn.fetchrow(
        "SELECT idx, instrument_run_id, platform, instrument_model,"
        " instrument_serial, run_performed_at, extra_metadata"
        " FROM qiita.sequencing_run WHERE instrument_run_id = $1",
        instrument_run_id,
    )
    if existing is None:
        # The ON CONFLICT path was taken but no row is visible — a race
        # window where another transaction's row was rolled back between
        # the INSERT attempt and our SELECT, or row visibility under a
        # surprising isolation level. Re-raise as an opaque error rather
        # than silently looping; the caller can retry.
        raise asyncpg.PostgresError(
            f"find-or-create on sequencing_run({instrument_run_id!r}) collided"
            " on insert but the existing row is not visible"
        )

    _assert_field_matches(
        "platform",
        existing["platform"],
        platform,
    )
    _assert_field_matches(
        "instrument_model",
        existing["instrument_model"],
        instrument_model,
    )
    _assert_field_matches(
        "instrument_serial",
        existing["instrument_serial"],
        instrument_serial,
    )
    _assert_field_matches(
        "run_performed_at",
        existing["run_performed_at"],
        run_performed_at,
    )
    _assert_jsonb_matches(
        "extra_metadata",
        existing["extra_metadata"],
        extra_metadata,
    )
    return (existing["idx"], False)


def _assert_field_matches(field: str, existing_value: Any, supplied_value: Any) -> None:
    """Reject the find-or-create read-through when a non-None supplied value
    disagrees with what's stored. Supplied None is treated as 'caller didn't
    override' and is not compared."""
    if supplied_value is None:
        return
    if existing_value != supplied_value:
        raise PayloadMismatch(field, existing_value, supplied_value)


def _assert_jsonb_matches(
    field: str, existing_raw: str | None, supplied_value: dict[str, Any] | None
) -> None:
    """JSONB equality. Postgres returns JSONB as a string via asyncpg's
    default codec, so we json.loads the stored value back into a dict and
    compare with the supplied dict — Python dict equality is itself
    order-insensitive, so key-order differences in the stored JSON don't
    produce a spurious mismatch."""
    if supplied_value is None:
        return
    existing_value = json.loads(existing_raw) if existing_raw is not None else None
    if existing_value != supplied_value:
        raise PayloadMismatch(field, existing_value, supplied_value)


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
    # Single-row fetch of the caller-visible column set by idx.
    return await pool_or_conn.fetchrow(
        "SELECT idx, instrument_run_id, platform, instrument_model,"
        " instrument_serial, run_performed_at, extra_metadata,"
        " created_by_idx, created_at,"
        " retired, retired_by_idx, retired_at, retire_reason"
        " FROM qiita.sequencing_run WHERE idx = $1",
        sequencing_run_idx,
    )


# same-pattern-ok: per-entity guarded natural-key fetcher, matches study/biosample convention
async def fetch_sequencing_run_idxs_by_instrument_run_id(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    values: list[str],
) -> dict[str, int]:
    """Return `{instrument_run_id: sequencing_run_idx}` for every value in
    `values` that resolves to a qiita.sequencing_run row. Values absent from
    the table are omitted from the returned map.

    instrument_run_id is UNIQUE across all rows, so each key maps to at most
    one idx. Retired runs resolve too: the row's `retired` flag is disclosed
    on the by-idx read, so resolution and disclosure see the same rows.
    """
    if not values:
        return {}
    rows = await pool_or_conn.fetch(
        "SELECT idx, instrument_run_id FROM qiita.sequencing_run"
        " WHERE instrument_run_id = ANY($1::text[])",
        values,
    )
    return {r["instrument_run_id"]: r["idx"] for r in rows}


async def fetch_sequencing_run_created_by(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    sequencing_run_idx: int,
) -> int | None:
    """Return only `created_by_idx` for the run; None on miss. Narrow
    SELECT for the caller-ownership guard, which would otherwise pull
    every column (notably the JSONB extra_metadata) just to read one int."""
    return await pool_or_conn.fetchval(
        "SELECT created_by_idx FROM qiita.sequencing_run WHERE idx = $1",
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
    run_preflight_blob: bytes | None = None,
    run_preflight_filename: str | None = None,
    created_by_idx: int,
    extra_metadata: dict[str, Any] | None = None,
) -> tuple[int, bool]:
    """Find-or-create a row in qiita.sequenced_pool keyed on
    ``(sequencing_run_idx, run_preflight_filename)`` via the partial
    unique index ``sequenced_pool_one_per_run_and_filename``.

    Returns ``(idx, created)``. ``created=True`` means a new row was minted;
    ``created=False`` means the existing row was reused after byte-equal
    blob + JSON-canonical extra_metadata comparison succeeded.

    The no-preflight case (both blob and filename NULL) is outside the
    partial index's predicate, so the insert always succeeds and returns
    ``(new_idx, True)``. The schema CHECK
    ``sequenced_pool_run_preflight_pair_consistent`` enforces that blob
    and filename are co-populated.

    Raises:
      PayloadMismatch: a row with the same ``(run_idx, filename)`` exists
        but the supplied blob bytes or extra_metadata disagree.
      asyncpg.ForeignKeyViolationError: bad sequencing_run_idx.
      asyncpg.CheckViolationError: half-populated preflight pair.
      asyncpg.PostgresError: other constraint failures.
    """
    inserted_idx = await conn.fetchval(
        "INSERT INTO qiita.sequenced_pool ("
        "    sequencing_run_idx, run_preflight_blob, run_preflight_filename,"
        "    extra_metadata, created_by_idx"
        ") VALUES ($1, $2, $3, $4::jsonb, $5)"
        " ON CONFLICT (sequencing_run_idx, run_preflight_filename)"
        "   WHERE run_preflight_filename IS NOT NULL"
        " DO NOTHING"
        " RETURNING idx",
        sequencing_run_idx,
        run_preflight_blob,
        run_preflight_filename,
        _encode_jsonb(extra_metadata),
        created_by_idx,
    )
    if inserted_idx is not None:
        return (inserted_idx, True)

    # Collision on (run_idx, filename). Fetch the existing row's blob +
    # extra_metadata for comparison.
    existing = await conn.fetchrow(
        "SELECT idx, run_preflight_blob, extra_metadata"
        " FROM qiita.sequenced_pool"
        " WHERE sequencing_run_idx = $1 AND run_preflight_filename = $2",
        sequencing_run_idx,
        run_preflight_filename,
    )
    if existing is None:
        raise asyncpg.PostgresError(
            "find-or-create on sequenced_pool"
            f"({sequencing_run_idx!r}, {run_preflight_filename!r})"
            " collided on insert but the existing row is not visible"
        )

    _assert_blob_matches("run_preflight_blob", existing["run_preflight_blob"], run_preflight_blob)
    _assert_jsonb_matches(
        "extra_metadata",
        existing["extra_metadata"],
        extra_metadata,
    )
    return (existing["idx"], False)


def _assert_blob_matches(
    field: str, existing_blob: bytes | None, supplied_blob: bytes | None
) -> None:
    """Compare two BYTEA values for byte-equality. None-supplied is
    'caller didn't override' and is not compared, consistent with
    _assert_field_matches.

    Note: this performs a Python-side byte comparison after both blobs
    have crossed the wire. Acceptable because the caller's blob is in
    memory anyway (multipart upload payload), and the only path that
    reaches this branch is the rare retry-after-collision. For very
    large blobs (10s of MB) consider replacing with a server-side
    `digest(run_preflight_blob, 'sha256')` comparison; today's preflight
    SQLite files are sub-MB so the byte-compare is cheap."""
    if supplied_blob is None:
        return
    if existing_blob != supplied_blob:
        # Report sizes, not bytes — blobs in error messages would explode
        # log volume and could leak data.
        existing_size = len(existing_blob) if existing_blob is not None else None
        raise PayloadMismatch(field, f"<{existing_size} bytes>", f"<{len(supplied_blob)} bytes>")


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


async def fetch_sequenced_pool_created_by(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    sequenced_pool_idx: int,
) -> int | None:
    """Return only `created_by_idx` for the pool; None on miss. Narrow
    SELECT for the caller-ownership guard — the full `fetch_sequenced_pool`
    pulls the BYTEA `run_preflight_blob` column which can be 1+ MB per
    row, an unacceptable cost for an auth check."""
    return await pool_or_conn.fetchval(
        "SELECT created_by_idx FROM qiita.sequenced_pool WHERE idx = $1",
        sequenced_pool_idx,
    )


async def fetch_sequenced_pool_preflight(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    sequencing_run_idx: int,
    sequenced_pool_idx: int,
) -> asyncpg.Record | None:
    """Read the (blob, filename) pair for a sequenced_pool, scoped to a
    specific sequencing_run.

    The JOIN with qiita.sequencing_run enforces run-pool membership at
    SELECT time: if the pool exists but belongs to a different run, the
    fetch returns None and the route maps to 404. The same None result
    is returned when the pool's preflight is unpopulated (blob and
    filename both NULL) — the route maps that to a separate 404 so the
    operator can distinguish missing-membership from missing-preflight.

    Selects only the columns the SequencedPoolPreflightResponse needs;
    the BYTEA blob is returned as raw bytes and the route base64-encodes
    via the response model's ``Base64Bytes`` field.
    """
    return await pool_or_conn.fetchrow(
        "SELECT sp.run_preflight_blob, sp.run_preflight_filename"
        " FROM qiita.sequenced_pool sp"
        " JOIN qiita.sequencing_run sr ON sr.idx = sp.sequencing_run_idx"
        " WHERE sp.idx = $1 AND sr.idx = $2",
        sequenced_pool_idx,
        sequencing_run_idx,
    )


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
