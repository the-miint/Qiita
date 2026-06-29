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
from qiita_common.actions import BCL_CONVERT_ACTION_ID, READ_MASK_ACTION_ID
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
    """Find-or-create a row in qiita.sequenced_pool keyed on the preflight
    *content* — ``(sequencing_run_idx, run_preflight_sha256)`` via the partial
    unique index ``sequenced_pool_one_per_run_and_hash`` — so re-uploading the
    same preflight bytes under any filename resolves to the same pool.
    ``run_preflight_sha256`` is a STORED generated column (``sha256`` of the
    blob, computed in-DB); the caller never supplies it.

    Returns ``(idx, created)``. ``created=True`` means a new row was minted;
    ``created=False`` means the existing same-content row was reused after a
    JSON-canonical extra_metadata comparison succeeded.

    The no-preflight case (both blob and filename NULL) is outside the
    partial index's predicate, so the insert always succeeds and returns
    ``(new_idx, True)``. The schema CHECK
    ``sequenced_pool_run_preflight_pair_consistent`` enforces that blob
    and filename are co-populated.

    The ``sequenced_pool_one_per_run_and_filename`` index is retained as an
    independent, permanent uniqueness rule: an INSERT of a *different* preflight
    that reuses an existing filename within the run trips that index (not the
    content index this ON CONFLICT targets) and is surfaced as a PayloadMismatch
    409 by design — two distinct pools in a run must differ in both content and
    filename, so the operator renames. (Same content + same filename is the
    idempotent-retry case: the content ON CONFLICT reuses the row, so the
    filename collision never surfaces. This relies on Postgres evaluating the
    ON CONFLICT arbiter — the content index — before inserting into any
    non-arbiter unique index, so DO NOTHING fires before the filename index can
    raise. Keep the content index as the ON CONFLICT target if you touch this.)

    Raises:
      PayloadMismatch: a same-content row exists but extra_metadata disagrees,
        or the filename collides with a different-content pool.
      asyncpg.ForeignKeyViolationError: bad sequencing_run_idx.
      asyncpg.CheckViolationError: half-populated preflight pair.
      asyncpg.PostgresError: other constraint failures.
    """
    try:
        inserted_idx = await conn.fetchval(
            "INSERT INTO qiita.sequenced_pool ("
            "    sequencing_run_idx, run_preflight_blob, run_preflight_filename,"
            "    extra_metadata, created_by_idx"
            ") VALUES ($1, $2, $3, $4::jsonb, $5)"
            " ON CONFLICT (sequencing_run_idx, run_preflight_sha256)"
            "   WHERE run_preflight_sha256 IS NOT NULL"
            " DO NOTHING"
            " RETURNING idx",
            sequencing_run_idx,
            run_preflight_blob,
            run_preflight_filename,
            _encode_jsonb(extra_metadata),
            created_by_idx,
        )
    except asyncpg.UniqueViolationError as exc:
        # The content index this ON CONFLICT targets did not fire, but the
        # (permanent) filename index did — same run + same filename + different
        # content. By design that is a 409, not a new pool: distinct pools must
        # differ in both content and filename. Map it to a clear PayloadMismatch
        # 409 instead of a raw 500.
        if exc.constraint_name == "sequenced_pool_one_per_run_and_filename":
            raise PayloadMismatch(
                "run_preflight_filename",
                f"<a different-content pool already uses filename "
                f"{run_preflight_filename!r} in this run; rename this preflight "
                f"to mint a separate pool>",
                run_preflight_filename,
            ) from exc
        raise
    if inserted_idx is not None:
        return (inserted_idx, True)

    # Content collision: a pool with byte-identical preflight already exists in
    # this run. Fetch it to reconcile extra_metadata (a same-content re-POST may
    # still disagree on the JSON sidecar).
    existing = await conn.fetchrow(
        "SELECT idx, extra_metadata"
        " FROM qiita.sequenced_pool"
        " WHERE sequencing_run_idx = $1 AND run_preflight_sha256 = sha256($2)",
        sequencing_run_idx,
        run_preflight_blob,
    )
    if existing is None:
        raise asyncpg.PostgresError(
            "find-or-create on sequenced_pool"
            f"({sequencing_run_idx!r}, <preflight content hash>)"
            " collided on insert but the existing row is not visible"
        )

    _assert_jsonb_matches(
        "extra_metadata",
        existing["extra_metadata"],
        extra_metadata,
    )
    return (existing["idx"], False)


async def fetch_sequenced_pool(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    sequenced_pool_idx: int,
) -> asyncpg.Record | None:
    """Return the qiita.sequenced_pool row for the given idx, or None on miss.

    The guard / byte-compare fetch: it pulls the BYTEA `run_preflight_blob` so
    callers can round-trip it, and backs `require_sequenced_pool_in_run`. The
    GET read surface (SequencedPoolResponse) shapes from
    `fetch_sequenced_pool_read_metrics` instead — that one omits the blob and
    adds the read-metric rollup. Accepts either a pool or a connection so the
    helper composes inside an open transaction or stands alone.
    """
    # The BYTEA blob is returned as-is so callers can byte-compare round-trips.
    return await pool_or_conn.fetchrow(
        "SELECT idx, sequencing_run_idx, run_preflight_blob, run_preflight_filename,"
        " extra_metadata, created_by_idx, created_at"
        " FROM qiita.sequenced_pool WHERE idx = $1",
        sequenced_pool_idx,
    )


async def fetch_sequenced_pool_read_metrics(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    sequenced_pool_idx: int,
) -> asyncpg.Record | None:
    """Return the pool's caller-visible metadata (minus the BYTEA preflight
    blob) plus the compute-on-read read-metric rollup, or None on a missing pool.

    The three `SUM(...)::bigint` columns aggregate the per-stage read counts
    over the pool's sequenced_samples; `count(ss.idx)` is the pool's sample
    total and `count(ss.raw_read_count_r1r2)` is how many of those carry metrics.
    Every aggregate uses `FILTER (WHERE ps.retired IS NOT TRUE)` so a retired
    prep_sample contributes to neither the sums nor the counts (matching the
    roster route's retired exclusion). `prep_sample.retired` is `NOT NULL`, so on
    a real sample `IS NOT TRUE` is equivalent to the roster's `= false`; the
    NULL-tolerant form is just defensive against the LEFT-JOIN'd all-NULL `ps` of
    a zero-sample pool (whose row the LEFT JOIN keeps regardless — sums NULL,
    counts 0). `SUM` of BIGINT is NUMERIC in Postgres, so
    the explicit `::bigint` cast hands asyncpg a clean int (a single pool's read
    total is far below the BIGINT ceiling). The passing fraction is recomputed
    from these sums in PoolReadMetrics — never a mean of per-sample fractions."""
    return await pool_or_conn.fetchrow(
        "SELECT sp.idx, sp.sequencing_run_idx, sp.run_preflight_filename,"
        " sp.extra_metadata, sp.created_by_idx, sp.created_at,"
        " SUM(ss.raw_read_count_r1r2) FILTER (WHERE ps.retired IS NOT TRUE)::bigint"
        "   AS raw_read_count_r1r2,"
        " SUM(ss.biological_read_count_r1r2) FILTER (WHERE ps.retired IS NOT TRUE)::bigint"
        "   AS biological_read_count_r1r2,"
        " SUM(ss.quality_filtered_read_count_r1r2) FILTER (WHERE ps.retired IS NOT TRUE)::bigint"
        "   AS quality_filtered_read_count_r1r2,"
        " count(ss.idx) FILTER (WHERE ps.retired IS NOT TRUE) AS sample_count,"
        " count(ss.raw_read_count_r1r2) FILTER (WHERE ps.retired IS NOT TRUE)"
        "   AS samples_with_metrics"
        " FROM qiita.sequenced_pool sp"
        " LEFT JOIN qiita.sequenced_sample ss ON ss.sequenced_pool_idx = sp.idx"
        " LEFT JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx"
        " WHERE sp.idx = $1"
        " GROUP BY sp.idx, sp.sequencing_run_idx, sp.run_preflight_filename,"
        " sp.extra_metadata, sp.created_by_idx, sp.created_at",
        sequenced_pool_idx,
    )


async def fetch_sequenced_pool_sample_qc_reports(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    sequenced_pool_idx: int,
) -> list[asyncpg.Record]:
    """Return one row per NON-retired sequenced_sample in the pool, carrying the
    two persisted QC-report JSONBs (raw / filtered), the prep_sample_idx, and the
    per-pool item id — the per-sample detail the merged pool QC report aggregates.

    Excludes retired samples (`ps.retired IS NOT TRUE`) to match the read-metric
    rollup's sample set, so `sample_count` there and the length of this list agree
    on a fully-processed pool. Ordered by prep_sample_idx for a stable response.
    Returns `[]` for a pool with no (non-retired) samples; the caller still 404s a
    missing pool via require_sequenced_pool_in_run + the rollup fetch. asyncpg
    returns JSONB as text, so the caller decodes each blob."""
    return await pool_or_conn.fetch(
        "SELECT ss.prep_sample_idx, ss.sequenced_pool_item_id,"
        " ss.raw_qc_report, ss.filtered_qc_report"
        " FROM qiita.sequenced_sample ss"
        " JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx"
        " WHERE ss.sequenced_pool_idx = $1 AND ps.retired IS NOT TRUE"
        " ORDER BY ss.prep_sample_idx",
        sequenced_pool_idx,
    )


async def fetch_sequenced_pool_completion(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    sequenced_pool_idx: int,
) -> asyncpg.Record:
    """Return the pool's host-masking completion rollup: counts of its
    non-retired sequenced_samples bucketed by the state of their read-mask
    work tickets (any version). Always returns one row (aggregate over zero rows
    is a single all-zero row), so a zero-sample / missing pool reads as all-zero
    counts — the caller still 404s a missing pool via require_sequenced_pool_in_run.
    (The demux/bcl-convert stage is reported separately by
    fetch_sequenced_pool_demux_state.)

    Per-sample classification mirrors qiita_common.models.PoolCompletionStatus
    (precedence completed > in_flight > no_data > failed > not_submitted),
    computed in two layers:

      `sample_state` LEFT-JOINs each non-retired sample to its read-mask
      tickets and folds them with bool_or aggregates (NULL over a sample with no
      ticket, so `ticket_count = 0` cleanly identifies not_submitted). The outer
      aggregate then tallies the mutually-exclusive buckets with FILTERs that
      encode the precedence, so every sample lands in exactly one — and the five
      buckets sum to `sample_count`.

    no_data outranks failed: a sample with both a NO_DATA and a stale FAILED
    ticket counts as no_data, so an empty well that was retried-then-superseded
    doesn't get stuck in the failed bucket (and `complete` can still fire).

    The action_id is matched on its bare id (passed as a bound param from the
    shared READ_MASK_ACTION_ID, the same constant the submit-host-filter-pool
    gesture mints against), NOT pinned to a version: the submitter chooses the
    read-mask version, and "this sample got masked" holds regardless of which
    version produced it (consistent with the read-metric / QC rollups, which read
    persisted columns irrespective of the writing version). The inlined state
    literals are pinned to qiita_common.models.WorkTicketState ('completed' /
    'pending' / 'queued' / 'processing' / 'no_data' / 'failed' — its full closed
    set); keep them in lockstep if that enum changes. Retired samples are
    excluded (`ps.retired IS NOT TRUE`) to match the other pool rollups' sample
    set."""
    return await pool_or_conn.fetchrow(
        "WITH sample_state AS ("
        "  SELECT ss.prep_sample_idx,"
        "    bool_or(wt.state = 'completed') AS has_completed,"
        "    bool_or(wt.state IN ('pending', 'queued', 'processing')) AS has_inflight,"
        "    bool_or(wt.state = 'no_data') AS has_no_data,"
        "    bool_or(wt.state = 'failed') AS has_failed,"
        "    count(wt.work_ticket_idx) AS ticket_count"
        "  FROM qiita.sequenced_sample ss"
        "  JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx"
        "  LEFT JOIN qiita.work_ticket wt"
        "    ON wt.prep_sample_idx = ss.prep_sample_idx"
        "   AND wt.action_id = $2"
        "  WHERE ss.sequenced_pool_idx = $1 AND ps.retired IS NOT TRUE"
        "  GROUP BY ss.prep_sample_idx"
        ")"
        " SELECT"
        "   count(*) AS sample_count,"
        "   count(*) FILTER (WHERE has_completed) AS samples_completed,"
        "   count(*) FILTER (WHERE NOT has_completed AND has_inflight)"
        "     AS samples_in_flight,"
        "   count(*) FILTER (WHERE NOT has_completed AND NOT has_inflight AND has_no_data)"
        "     AS samples_no_data,"
        "   count(*) FILTER ("
        "     WHERE NOT has_completed AND NOT has_inflight AND NOT has_no_data AND has_failed)"
        "     AS samples_failed,"
        "   count(*) FILTER (WHERE ticket_count = 0) AS samples_not_submitted"
        " FROM sample_state",
        sequenced_pool_idx,
        READ_MASK_ACTION_ID,
    )


async def fetch_sequenced_pool_demux_state(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    sequenced_pool_idx: int,
) -> str:
    """Return the pool's demux (bcl-convert) stage state, one of
    'completed' / 'in_flight' / 'no_data' / 'failed' / 'not_submitted'.

    bcl-convert is sequenced_pool-scoped (one ticket per pool, occasionally more
    if a force re-submit landed a second), so this folds every bcl-convert ticket
    on the pool with bool_or aggregates and applies the same precedence the
    per-sample read-mask rollup uses (completed > in_flight > no_data > failed),
    falling back to 'not_submitted' when the pool has no bcl-convert ticket at
    all. Matched on the bare BCL_CONVERT_ACTION_ID (version-agnostic — "did this
    pool's demux finish?" holds regardless of which version produced it). The
    state literals are pinned to qiita_common.models.WorkTicketState; keep them
    in lockstep if that enum changes."""
    row = await pool_or_conn.fetchrow(
        "SELECT"
        "  bool_or(state = 'completed') AS has_completed,"
        "  bool_or(state IN ('pending', 'queued', 'processing')) AS has_in_flight,"
        "  bool_or(state = 'no_data') AS has_no_data,"
        "  bool_or(state = 'failed') AS has_failed,"
        "  count(*) AS ticket_count"
        " FROM qiita.work_ticket"
        " WHERE sequenced_pool_idx = $1 AND action_id = $2",
        sequenced_pool_idx,
        BCL_CONVERT_ACTION_ID,
    )
    if row["has_completed"]:
        return "completed"
    if row["has_in_flight"]:
        return "in_flight"
    if row["has_no_data"]:
        return "no_data"
    if row["has_failed"]:
        return "failed"
    return "not_submitted"


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


async def update_sequenced_pool_preflight_blob(
    conn: asyncpg.Connection,
    *,
    sequenced_pool_idx: int,
    new_blob: bytes,
) -> None:
    """Overwrite a sequenced_pool's run_preflight_blob with edited bytes.

    The filename is left untouched (an in-place edit of the same preflight
    file), so only the BYTEA column is rewritten and the
    sequenced_pool_run_preflight_pair_consistent CHECK still holds (its
    co-populated filename partner is unchanged). Takes a connection, never a
    pool: the caller runs this inside the same transaction as the editability
    re-check (`assert_pool_preflight_editable`), so the write is rejected if that
    re-check found the run already processed. Note that under READ COMMITTED the
    gate and write are not fully serialized against a submission committing
    concurrently mid-edit — see the route docstring for that residual race."""
    await conn.execute(
        "UPDATE qiita.sequenced_pool SET run_preflight_blob = $1 WHERE idx = $2",
        new_blob,
        sequenced_pool_idx,
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
