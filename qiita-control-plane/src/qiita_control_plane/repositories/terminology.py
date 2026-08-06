"""Repository functions and composer for the qiita.terminology,
qiita.terminology_term, and qiita.terminology_closure tables.

Read functions accept an asyncpg.Pool or asyncpg.Connection, so they
stand alone or compose inside an open transaction. Write and composer
functions take an asyncpg.Connection and never acquire their own or open
a top-level transaction; composers additionally guard at entry, so a
caller cannot forget to wrap the multi-statement workflow atomically.
"""

from __future__ import annotations

from dataclasses import dataclass

import asyncpg
from qiita_common.models import TerminologyStatus, TerminologyTermObsoletionKind

from . import require_transaction

# Caller-visible column projection for the qiita.terminology row
_TERMINOLOGY_COLUMNS = "idx AS terminology_idx, name, version, loaded_at, status"


class TerminologyImportAnomaly(Exception):
    """Raised when the staged release violates a structural invariant
    the import cannot silently resolve. At least one attribute is
    populated; each is a list of offending rows.

    `silently_dropped_term_ids`: in the DB but absent from the batch.
    `unresolved_replaced_by`: (term_id, target) — target absent from batch.
    `misaligned_replaced_by`: (term_id, target) — non-obsolete with target set."""

    def __init__(
        self,
        *,
        silently_dropped_term_ids: list[str] | None = None,
        unresolved_replaced_by: list[tuple[str, str]] | None = None,
        misaligned_replaced_by: list[tuple[str, str]] | None = None,
    ) -> None:
        self.silently_dropped_term_ids = silently_dropped_term_ids or []
        self.unresolved_replaced_by = unresolved_replaced_by or []
        self.misaligned_replaced_by = misaligned_replaced_by or []
        parts: list[str] = []
        if self.silently_dropped_term_ids:
            parts.append(f"silently_dropped_term_ids={self.silently_dropped_term_ids!r}")
        if self.unresolved_replaced_by:
            parts.append(f"unresolved_replaced_by={self.unresolved_replaced_by!r}")
        if self.misaligned_replaced_by:
            parts.append(f"misaligned_replaced_by={self.misaligned_replaced_by!r}")
        super().__init__("; ".join(parts) or "TerminologyImportAnomaly")


@dataclass(frozen=True)
class ParsedTerm:
    """One term in an incoming release batch."""

    term_id: str
    label: str
    is_obsolete: bool
    replaced_by_term_id: str | None
    obsoletion_kind: TerminologyTermObsoletionKind | None


@dataclass(frozen=True)
class PriorTermState:
    """Pre-import snapshot of one terminology_term row."""

    label: str
    is_obsolete: bool
    obsoletion_kind: TerminologyTermObsoletionKind | None


@dataclass(frozen=True)
class TerminologyImportResult:
    """Holds counts describing what changed in this load.

    `terms_inserted` — rows newly added to terminology_term.
    `terms_label_updated` — existing rows whose label changed.
    `terms_newly_obsoleted` — rows that became obsolete on this load,
    counting both rows that flipped is_obsolete=false → true (including
    silent drops auto-obsoleted under tolerate_anomalies=True) and
    first-time inserts of terms arriving already obsolete (regardless
    of obsoletion_kind).
    `terms_newly_merged` — rows whose obsoletion_kind became
    source_merged on this load (subset of terms_newly_obsoleted when the
    obsoletion is fresh; also counts kind-only transitions on a row that
    was already obsolete from a prior load).
    `terms_silently_dropped` — rows auto-obsoleted because the term_id
    was present in the database but absent from the incoming batch with
    no explicit deprecation marker. Always zero unless the load ran
    with tolerate_anomalies=True (fail mode raises before returning).
    `closure_rows` — terminology_closure rows present for this
    terminology after the rebuild (includes self-rows).
    """

    terminology_idx: int
    terms_inserted: int
    terms_label_updated: int
    terms_newly_obsoleted: int
    terms_newly_merged: int
    terms_silently_dropped: int
    closure_rows: int


async def fetch_terminology(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    terminology_idx: int,
) -> asyncpg.Record | None:
    """Return the qiita.terminology row for the given idx, or None on miss."""
    return await pool_or_conn.fetchrow(
        f"SELECT {_TERMINOLOGY_COLUMNS} FROM qiita.terminology WHERE idx = $1",
        terminology_idx,
    )


async def fetch_terminology_idx_by_name(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    name: str,
) -> int | None:
    """Return only the idx for the qiita.terminology row with this name,
    or None on miss.
    """
    return await pool_or_conn.fetchval(
        "SELECT idx FROM qiita.terminology WHERE name = $1",
        name,
    )


async def update_terminology_status(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    terminology_idx: int,
    target: TerminologyStatus,
    valid_sources: list[str],
) -> asyncpg.Record | None:
    """Atomically transition a terminology row's status, conditional on
    the current status being one of `valid_sources`.

    Returns the post-UPDATE row on success, None when no row matched —
    either the idx does not exist or the row is in a state not present
    in `valid_sources`. The conditional UPDATE is what makes the
    transition TOCTOU-safe against concurrent writers; `valid_sources`
    is the caller's derivation from VALID_TERMINOLOGY_STATUS_TRANSITIONS.
    """
    return await pool_or_conn.fetchrow(
        "UPDATE qiita.terminology"
        " SET status = $1::qiita.terminology_status"
        " WHERE idx = $2"
        "   AND status = ANY($3::qiita.terminology_status[])"
        f" RETURNING {_TERMINOLOGY_COLUMNS}",
        str(target),
        terminology_idx,
        valid_sources,
    )


async def import_terminology_release(
    conn: asyncpg.Connection,
    *,
    name: str,
    version: str,
    parsed_terms: list[ParsedTerm],
    parsed_closure: list[tuple[str, str, int]],
    tolerate_anomalies: bool = False,
    unresolved_replaced_by_pairs: list[tuple[str, str]] | None = None,
) -> TerminologyImportResult:
    """Apply one complete staged terminology release to the DB.

    Caller must wrap this in `async with conn.transaction():` — the
    multi-statement workflow is not idempotent against partial commit.

    `tolerate_anomalies=False` (default) raises TerminologyImportAnomaly
    when the database already carries term_ids absent from the incoming
    term batch with no explicit obsoletion marker; the outer transaction
    rolls back so terminology.status, .version, and .loaded_at stay at
    their pre-import values.

    `tolerate_anomalies=True` instead auto-obsoletes silent drops as
    synthetic ParsedTerm rows (kind=silently_dropped, replaced_by NULL,
    label carried forward) and appends a notes line per entry in
    `unresolved_replaced_by_pairs` recording the attempted CURIE."""
    require_transaction(conn)

    # Find-or-create the qiita.terminology row in 'loading'.
    terminology_idx = await _ensure_loading_row(conn, name=name, version=version)

    # Snapshot the pre-import term state before any writes.
    prior_state = await _fetch_prior_term_state(conn, terminology_idx)

    # Silent drops: fail mode raises here; tolerate mode returns synthetic
    # rows to append to the upsert batch.
    silent_drop_synthetics = _handle_silent_drops(
        parsed_terms, prior_state, tolerate_anomalies=tolerate_anomalies
    )
    effective_terms = parsed_terms + silent_drop_synthetics

    # Pass 1: upsert every incoming term with replaced_by=NULL.
    await _upsert_terms_without_replaced_by(
        conn,
        terminology_idx=terminology_idx,
        parsed_terms=effective_terms,
        loading_version=version,
    )

    # Pass 2: populate replaced_by from the in-batch term_id → idx mapping.
    await _resolve_replaced_by(conn, terminology_idx=terminology_idx, parsed_terms=effective_terms)

    # Tolerate mode: stamp the attempted-but-unresolvable CURIE on each
    # affected row's notes column (append, newline-separated).
    if unresolved_replaced_by_pairs:
        await _append_unresolved_replaced_by_notes(
            conn,
            terminology_idx=terminology_idx,
            unresolved_pairs=unresolved_replaced_by_pairs,
            loading_version=version,
        )

    # Rebuild closure scoped to this terminology_idx.
    closure_rows = await _rebuild_closure(
        conn, terminology_idx=terminology_idx, parsed_closure=parsed_closure
    )

    counts = _compute_counts(prior_state, effective_terms)

    # Final transition: LOADING → ACTIVE.
    transitioned = await update_terminology_status(
        conn,
        terminology_idx,
        TerminologyStatus.ACTIVE,
        [TerminologyStatus.LOADING.value],
    )
    if transitioned is None:
        # Since the LOADING transition above succeeded, an empty UPDATE here
        # would mean another writer concurrently mutated the row, which is a
        # bug worth surfacing rather than silently completing.
        raise RuntimeError(
            f"qiita.terminology row {terminology_idx} was not in LOADING after"
            " the import; concurrent mutation suspected."
        )

    return TerminologyImportResult(
        terminology_idx=terminology_idx,
        terms_inserted=counts["inserted"],
        terms_label_updated=counts["label_updated"],
        terms_newly_obsoleted=counts["newly_obsoleted"],
        terms_newly_merged=counts["newly_merged"],
        terms_silently_dropped=len(silent_drop_synthetics),
        closure_rows=closure_rows,
    )


async def _ensure_loading_row(
    conn: asyncpg.Connection,
    *,
    name: str,
    version: str,
) -> int:
    """Return the terminology_idx with the row in 'loading' state and
    its version + loaded_at advanced to the new release.

    Raises RuntimeError if the row exists in a state other than ACTIVE
    or FAILED — concurrent load or a prior crash mid-flight."""
    existing_idx = await fetch_terminology_idx_by_name(conn, name)
    if existing_idx is None:
        try:
            return await conn.fetchval(
                "INSERT INTO qiita.terminology (name, version, loaded_at, status)"
                " VALUES ($1, $2, NOW(), $3::qiita.terminology_status) RETURNING idx",
                name,
                version,
                TerminologyStatus.LOADING.value,
            )
        except asyncpg.exceptions.UniqueViolationError as exc:
            raise RuntimeError(
                f"qiita.terminology {name!r} was created concurrently;"
                " retry after the other load completes."
            ) from exc

    transitioned = await update_terminology_status(
        conn,
        existing_idx,
        TerminologyStatus.LOADING,
        [TerminologyStatus.ACTIVE.value, TerminologyStatus.FAILED.value],
    )
    if transitioned is None:
        raise RuntimeError(
            f"qiita.terminology {name!r} could not transition to LOADING;"
            " the row exists but is not in ACTIVE or FAILED state."
        )
    await conn.execute(
        "UPDATE qiita.terminology SET version = $1, loaded_at = NOW() WHERE idx = $2",
        version,
        existing_idx,
    )
    return existing_idx


async def _fetch_prior_term_state(
    conn: asyncpg.Connection, terminology_idx: int
) -> dict[str, PriorTermState]:
    """Snapshot term_id → PriorTermState for every term currently in
    this terminology."""
    rows = await conn.fetch(
        "SELECT term_id, label, is_obsolete, obsoletion_kind"
        "  FROM qiita.terminology_term WHERE terminology_idx = $1",
        terminology_idx,
    )
    return {
        row["term_id"]: PriorTermState(
            label=row["label"],
            is_obsolete=row["is_obsolete"],
            obsoletion_kind=(
                TerminologyTermObsoletionKind(row["obsoletion_kind"])
                if row["obsoletion_kind"] is not None
                else None
            ),
        )
        for row in rows
    }


def _handle_silent_drops(
    parsed_terms: list[ParsedTerm],
    prior_state: dict[str, PriorTermState],
    *,
    tolerate_anomalies: bool,
) -> list[ParsedTerm]:
    """A silent drop is a non-obsolete term in the DB whose term_id is
    absent from the incoming term batch with no explicit deprecation
    marker. Already-obsolete terms absent from the batch are benign.

    Fail mode raises TerminologyImportAnomaly. Tolerate mode returns
    synthetic ParsedTerm rows for the dropped term_ids — label carried
    forward from prior DB state, is_obsolete=True, replaced_by_term_id
    None, obsoletion_kind=SILENTLY_DROPPED. Returns the empty list when
    no silent drops are present."""
    incoming_term_ids = {term.term_id for term in parsed_terms}
    silently_dropped = sorted(
        term_id
        for term_id, prior in prior_state.items()
        if term_id not in incoming_term_ids and not prior.is_obsolete
    )
    if not silently_dropped:
        return []
    if not tolerate_anomalies:
        raise TerminologyImportAnomaly(silently_dropped_term_ids=silently_dropped)
    return [
        ParsedTerm(
            term_id=term_id,
            label=prior_state[term_id].label,
            is_obsolete=True,
            replaced_by_term_id=None,
            obsoletion_kind=TerminologyTermObsoletionKind.SILENTLY_DROPPED,
        )
        for term_id in silently_dropped
    ]


async def _upsert_terms_without_replaced_by(
    conn: asyncpg.Connection,
    *,
    terminology_idx: int,
    parsed_terms: list[ParsedTerm],
    loading_version: str,
) -> None:
    """Insert-or-update every incoming term, setting replaced_by=NULL. The
    obsoleted_in_version set-once and un-obsoletion-clear rules live
    inside the UPSERT itself so the invariants are enforced by the same
    statement that writes them."""
    if not parsed_terms:
        return
    term_ids = [term.term_id for term in parsed_terms]
    labels = [term.label for term in parsed_terms]
    is_obsoletes = [term.is_obsolete for term in parsed_terms]
    obsoletion_kinds = [
        str(term.obsoletion_kind) if term.obsoletion_kind is not None else None
        for term in parsed_terms
    ]

    # One INSERT ... ON CONFLICT DO UPDATE call keyed on
    # (terminology_idx, term_id) drives both new-row and update paths
    # from the same row source: unnesting four parallel arrays
    # ($2 term_ids, $3 labels, $4 is_obsoletes, $5 obsoletion_kinds)
    # zips into one tuple per incoming term.
    # The INSERT side stamps obsoleted_in_version with loading_version ($6)
    # iff the row arrives obsolete, else NULL; NB: replaced_by is always
    # NULL because the in-batch term_id -> idx map is not yet known.
    # The ON CONFLICT side overwrites label, is_obsolete, and
    # obsoletion_kind unconditionally; obsoleted_in_version uses
    # COALESCE(existing, EXCLUDED) when still obsolete so the first
    # version that obsoleted the term sticks across reloads, and clears
    # to NULL on un-obsoletion; NB: replaced_by is wiped on every update so
    # the later replaced_by-setting step starts from a clean slate.
    await conn.execute(
        "INSERT INTO qiita.terminology_term"
        "   (terminology_idx, term_id, label, is_obsolete,"
        "    obsoletion_kind, obsoleted_in_version, replaced_by)"
        " SELECT $1, t, l, o,"
        "        k::qiita.terminology_term_obsoletion_kind,"
        "        CASE WHEN o THEN $6 ELSE NULL END,"
        "        NULL"
        "   FROM unnest($2::text[], $3::text[], $4::bool[], $5::text[]) AS s(t, l, o, k)"
        " ON CONFLICT (terminology_idx, term_id) DO UPDATE"
        "   SET label = EXCLUDED.label,"
        "       is_obsolete = EXCLUDED.is_obsolete,"
        "       obsoletion_kind = EXCLUDED.obsoletion_kind,"
        "       obsoleted_in_version = CASE"
        "           WHEN EXCLUDED.is_obsolete"
        "           THEN COALESCE("
        "               qiita.terminology_term.obsoleted_in_version,"
        "               EXCLUDED.obsoleted_in_version"
        "           )"
        "           ELSE NULL"
        "       END,"
        "       replaced_by = NULL",
        terminology_idx,
        term_ids,
        labels,
        is_obsoletes,
        obsoletion_kinds,
        loading_version,
    )


async def _resolve_replaced_by(
    conn: asyncpg.Connection,
    *,
    terminology_idx: int,
    parsed_terms: list[ParsedTerm],
) -> None:
    """With all term rows currently present at known idxs, populate replaced_by
    for obsolete rows whose incoming entry names within this terminology
    show they have been replaced. Precondition: every term in the
    batch is already present in terminology_term."""
    pairs = [
        (term.term_id, term.replaced_by_term_id)
        for term in parsed_terms
        if term.is_obsolete and term.replaced_by_term_id is not None
    ]
    if not pairs:
        return
    obsolete_term_ids = [p[0] for p in pairs]
    survivor_term_ids = [p[1] for p in pairs]
    await conn.execute(
        "UPDATE qiita.terminology_term AS tt SET replaced_by = survivor.idx"
        "   FROM unnest($2::text[], $3::text[]) AS s(obsolete_term_id, survivor_term_id)"
        "   JOIN qiita.terminology_term AS survivor"
        "     ON survivor.terminology_idx = $1"
        "    AND survivor.term_id = s.survivor_term_id"
        "  WHERE tt.terminology_idx = $1 AND tt.term_id = s.obsolete_term_id",
        terminology_idx,
        obsolete_term_ids,
        survivor_term_ids,
    )


async def _append_unresolved_replaced_by_notes(
    conn: asyncpg.Connection,
    *,
    terminology_idx: int,
    unresolved_pairs: list[tuple[str, str]],
    loading_version: str,
) -> None:
    """Append a per-event audit line to qiita.terminology_term.notes for
    each (obsolete_term_id, attempted_curie) pair, scoped to this
    terminology. Existing notes content is preserved; the new line is
    newline-separated when prior content exists.

    The notes column is shared between the loader and operator content,
    so entries accumulate across reloads; the version stamp in each line
    is what distinguishes one tolerate run from another."""
    if not unresolved_pairs:
        return
    obsolete_term_ids = [p[0] for p in unresolved_pairs]
    new_lines = [
        f"v{loading_version}: attempted replaced_by={attempted} unresolved"
        for _, attempted in unresolved_pairs
    ]
    await conn.execute(
        "UPDATE qiita.terminology_term AS tt"
        "   SET notes = CASE"
        "       WHEN tt.notes IS NULL THEN s.new_line"
        "       ELSE tt.notes || E'\\n' || s.new_line"
        "   END"
        "   FROM unnest($2::text[], $3::text[]) AS s(obsolete_term_id, new_line)"
        " WHERE tt.terminology_idx = $1 AND tt.term_id = s.obsolete_term_id",
        terminology_idx,
        obsolete_term_ids,
        new_lines,
    )


async def _rebuild_closure(
    conn: asyncpg.Connection,
    *,
    terminology_idx: int,
    parsed_closure: list[tuple[str, str, int]],
) -> int:
    """Replace every closure row scoped to this terminology and return
    the DB-side inserted count. Closure tuples that reference a term_id
    not present in terminology_term are silently dropped by the inner
    JOINs, so the returned count may be less than len(parsed_closure)."""
    await conn.execute(
        "DELETE FROM qiita.terminology_closure WHERE terminology_idx = $1",
        terminology_idx,
    )
    if not parsed_closure:
        return 0
    ancestors = [c[0] for c in parsed_closure]
    descendants = [c[1] for c in parsed_closure]
    distances = [c[2] for c in parsed_closure]
    return await conn.fetchval(
        "WITH ins AS ("
        "  INSERT INTO qiita.terminology_closure"
        "       (terminology_idx, ancestor_term_idx, descendant_term_idx, distance)"
        "     SELECT $1, a.idx, d.idx, s.distance"
        "       FROM unnest($2::text[], $3::text[], $4::int[]) AS s(a_id, d_id, distance)"
        "       JOIN qiita.terminology_term a"
        "         ON a.terminology_idx = $1 AND a.term_id = s.a_id"
        "       JOIN qiita.terminology_term d"
        "         ON d.terminology_idx = $1 AND d.term_id = s.d_id"
        "     RETURNING 1"
        ") SELECT count(*)::int FROM ins",
        terminology_idx,
        ancestors,
        descendants,
        distances,
    )


def _compute_counts(
    prior_state: dict[str, PriorTermState],
    parsed_terms: list[ParsedTerm],
) -> dict[str, int]:
    """Walk `parsed_terms` (the effective batch — caller-supplied terms
    plus any synthetic silent-drop entries appended in tolerate mode)
    against the pre-import DB snapshot to compute the
    TerminologyImportResult counters. terms_newly_merged catches both
    fresh obsoletions whose kind is source_merged and existing-obsolete
    rows whose kind flips to source_merged on this import."""
    merged_kind = TerminologyTermObsoletionKind.SOURCE_MERGED
    inserted = 0
    label_updated = 0
    newly_obsoleted = 0
    newly_merged = 0
    for term in parsed_terms:
        prior = prior_state.get(term.term_id)
        if prior is None:
            inserted += 1
            if term.is_obsolete:
                newly_obsoleted += 1
                if term.obsoletion_kind == merged_kind:
                    newly_merged += 1
            continue
        if prior.label != term.label:
            label_updated += 1
        # Was-not-obsolete → is-obsolete is a fresh obsoletion event;
        # the kind-only flip from a prior obsolete state to source_merged
        # is a separate event that still counts toward newly_merged.
        if not prior.is_obsolete and term.is_obsolete:
            newly_obsoleted += 1
            if term.obsoletion_kind == merged_kind:
                newly_merged += 1
        elif (
            prior.is_obsolete
            and term.is_obsolete
            and term.obsoletion_kind == merged_kind
            and prior.obsoletion_kind != merged_kind
        ):
            newly_merged += 1
    return {
        "inserted": inserted,
        "label_updated": label_updated,
        "newly_obsoleted": newly_obsoleted,
        "newly_merged": newly_merged,
    }
