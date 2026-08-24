"""Repository functions and composer for the qiita.terminology,
qiita.terminology_term, and qiita.terminology_closure tables.

Read functions accept an asyncpg.Pool or asyncpg.Connection, so they
stand alone or compose inside an open transaction. Write and composer
functions take an asyncpg.Connection and never acquire their own or open
a top-level transaction; composers additionally guard at entry, so a
caller cannot forget to wrap the multi-statement workflow atomically.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace

import asyncpg
from qiita_common.models import (
    VALID_TERMINOLOGY_STATUS_TRANSITIONS,
    TerminologyStatus,
    TerminologyTermObsoletionKind,
)

from . import require_transaction

# Caller-visible column projection for the qiita.terminology row
_TERMINOLOGY_COLUMNS = "idx AS terminology_idx, name, version, loaded_at, status"

# How many offending values an error names before it summarizes the rest. A
# release runs to millions of terms, so a check that named every offender would
# render an error no operator can read.
MAX_REPORTED_OFFENDERS = 20


def capped_offenders(values: Sequence[object]) -> tuple[int, list[object]]:
    """Return how many values offended and the at-most-MAX_REPORTED_OFFENDERS
    sample a report names.

    Which offenders a report names is decided here alone, so a prose error and
    a structured one describe the same subset of the same failure.
    """
    total = len(values)
    sample = list(values[:MAX_REPORTED_OFFENDERS])
    return total, sample


def format_offenders(values: Sequence[object]) -> str:
    """Render offending values for an error, naming at most
    MAX_REPORTED_OFFENDERS of them and stating the total when any go unnamed.

    A collection within the cap renders as its own repr, so an error about a
    handful of rows reads exactly as it would with no cap in place.
    """
    total, sample = capped_offenders(values)
    if total <= MAX_REPORTED_OFFENDERS:
        return repr(sample)
    return f"{total} total, first {MAX_REPORTED_OFFENDERS}: {sample!r}"


class TerminologyImportAnomaly(Exception):
    """Raised when the staged release violates a structural invariant
    the import cannot silently resolve. Each attribute holds a list of
    offending rows, and at least one is non-empty.

    `silently_dropped_term_ids`: in the DB but absent from the batch.
    `unresolved_replaced_by`: (term_id, target) — target absent from batch.
    `misaligned_replaced_by`: (term_id, target) — non-obsolete with target set.
    `unresolved_closure_endpoints`: (ancestor, descendant) — an endpoint
    absent from the batch."""

    def __init__(
        self,
        *,
        silently_dropped_term_ids: list[str] | None = None,
        unresolved_replaced_by: list[tuple[str, str]] | None = None,
        misaligned_replaced_by: list[tuple[str, str]] | None = None,
        unresolved_closure_endpoints: list[tuple[str, str]] | None = None,
    ) -> None:
        self.silently_dropped_term_ids = silently_dropped_term_ids or []
        self.unresolved_replaced_by = unresolved_replaced_by or []
        self.misaligned_replaced_by = misaligned_replaced_by or []
        self.unresolved_closure_endpoints = unresolved_closure_endpoints or []

        # The message names only populated kinds, so it says nothing about a
        # kind the release did not violate.
        parts = [
            f"{attribute}={format_offenders(rows)}"
            for attribute, rows in self.reported_anomalies()
            if rows
        ]
        super().__init__("; ".join(parts) or "TerminologyImportAnomaly")

    def reported_anomalies(self) -> tuple[tuple[str, Sequence[object]], ...]:
        """Return (attribute name, offending rows) per anomaly kind, populated
        or not, in one fixed order; a kind added later joins the end."""
        return (
            ("silently_dropped_term_ids", self.silently_dropped_term_ids),
            ("unresolved_replaced_by", self.unresolved_replaced_by),
            ("misaligned_replaced_by", self.misaligned_replaced_by),
            ("unresolved_closure_endpoints", self.unresolved_closure_endpoints),
        )


@dataclass(frozen=True)
class ParsedTerm:
    """One term in an incoming release batch.

    A `label` of None says the source supplies no name for the term, which
    a source can do for a term id it retired without ever naming. The import
    decides what to store in its place against the pre-import snapshot.

    `alternate_label` is the second name the source supplies for the term,
    None when it supplies none. A source that names its terms only one way
    leaves it None throughout.

    Every text value arrives stripped, and one carrying nothing arrives as
    None rather than as the empty string. An empty term_id is not rejected
    here.
    """

    term_id: str
    label: str | None
    alternate_label: str | None
    is_obsolete: bool
    replaced_by_term_id: str | None
    obsoletion_kind: TerminologyTermObsoletionKind | None

    def __post_init__(self) -> None:
        # Frozen forbids plain assignment, so normalization writes through
        # object.__setattr__.
        object.__setattr__(self, "term_id", self.term_id.strip())
        for optional_field in ("label", "alternate_label", "replaced_by_term_id"):
            raw_value = getattr(self, optional_field)
            settled = raw_value.strip() or None if raw_value is not None else None
            object.__setattr__(self, optional_field, settled)

    @classmethod
    def retired(
        cls,
        term_id: str,
        *,
        replaced_by_term_id: str | None,
        obsoletion_kind: TerminologyTermObsoletionKind,
    ) -> ParsedTerm:
        """Build the row for a term id the release retires without naming it.

        Leaves both names unset, which says the release asserts nothing about
        them rather than that the term has none. A retirement that does carry
        a name is an ordinary construction, not this.
        """
        return cls(
            term_id=term_id,
            label=None,
            alternate_label=None,
            is_obsolete=True,
            replaced_by_term_id=replaced_by_term_id,
            obsoletion_kind=obsoletion_kind,
        )


@dataclass(frozen=True)
class PriorTermState:
    """Pre-import snapshot of one terminology_term row."""

    label: str
    alternate_label: str | None
    is_obsolete: bool
    obsoletion_kind: TerminologyTermObsoletionKind | None


@dataclass(frozen=True)
class TerminologyImportResult:
    """Holds counts describing what changed in this load.

    `terms_inserted` — rows newly added to terminology_term.
    `terms_label_updated` — existing rows whose label changed.
    `terms_alternate_label_updated` — existing rows whose alternate_label
    changed, counting a change to or from no second name at all. Independent
    of terms_label_updated: a row whose two names both changed counts once
    in each.
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
    terms_alternate_label_updated: int
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
) -> asyncpg.Record | None:
    """Atomically transition a terminology row's status, conditional on the
    current status being one VALID_TERMINOLOGY_STATUS_TRANSITIONS admits as
    a source for `target`.

    Returns the post-UPDATE row on success, None when no row matched —
    either the idx does not exist or the row is in a state that cannot
    reach `target`. The conditional UPDATE makes the transition TOCTOU-safe
    against concurrent writers.
    """
    # Derive the source states that can reach `target` rather than taking
    # them from the caller, so the transition table lives in one place.
    valid_sources = [
        str(src)
        for src, targets in VALID_TERMINOLOGY_STATUS_TRANSITIONS.items()
        if target in targets
    ]
    updated_row = await pool_or_conn.fetchrow(
        "UPDATE qiita.terminology"
        " SET status = $1::qiita.terminology_status"
        " WHERE idx = $2"
        "   AND status = ANY($3::qiita.terminology_status[])"
        f" RETURNING {_TERMINOLOGY_COLUMNS}",
        str(target),
        terminology_idx,
        valid_sources,
    )
    return updated_row


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
    # Settle what an unnamed term stores before anything reads a name, so the
    # upsert and the counters see the same values.
    effective_terms = _resolve_missing_names(parsed_terms + silent_drop_synthetics, prior_state)

    # Pass 1: upsert every incoming term with replaced_by=NULL.
    await _upsert_terms_without_replaced_by(
        conn,
        terminology_idx=terminology_idx,
        parsed_terms=effective_terms,
        loading_version=version,
    )

    # Statistics for the rows just written, so every statement below plans the
    # terminology_idx filter it carries against real selectivity instead of the
    # default, which on a release of any size is orders of magnitude off. Runs
    # here rather than after the commit because the statements that need it are
    # in this transaction, and ANALYZE counts rows its own transaction inserted.
    await conn.execute("ANALYZE qiita.terminology_term")

    # Pass 2: populate replaced_by from the in-batch term_id → idx mapping.
    # Two row versions per replaced row per load: pass 1 wipes the pointer and
    # this restores it, changed or not, so a reload leaves twice the merge count
    # in dead tuples. The wipe lets a release withdraw a replacement, and
    # skipping it would need the term_id -> idx map that pass 1 produces. This
    # covers all replacements, each time, not just the new release's own.
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
    transitioned = await update_terminology_status(conn, terminology_idx, TerminologyStatus.ACTIVE)
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
        terms_alternate_label_updated=counts["alternate_label_updated"],
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

    transitioned = await update_terminology_status(conn, existing_idx, TerminologyStatus.LOADING)
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
        "SELECT term_id, label, alternate_label, is_obsolete, obsoletion_kind"
        "  FROM qiita.terminology_term WHERE terminology_idx = $1",
        terminology_idx,
    )
    return {
        row["term_id"]: PriorTermState(
            label=row["label"],
            alternate_label=row["alternate_label"],
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
    synthetic ParsedTerm rows for the dropped term_ids — unnamed, so they keep
    both stored names, with is_obsolete=True, replaced_by_term_id None, and
    obsoletion_kind=SILENTLY_DROPPED. Returns the empty list when the release
    drops nothing silently."""
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
        ParsedTerm.retired(
            term_id,
            replaced_by_term_id=None,
            obsoletion_kind=TerminologyTermObsoletionKind.SILENTLY_DROPPED,
        )
        for term_id in silently_dropped
    ]


def _resolve_missing_names(
    parsed_terms: list[ParsedTerm],
    prior_state: dict[str, PriorTermState],
) -> list[ParsedTerm]:
    """Settle both names of every term the source left unnamed: the names
    already stored for it, or its own term_id and no second name when the
    database holds nothing.

    An absent label is the source saying nothing about the term rather than
    asserting it has no names, so that release may clear neither name — hence
    settling both together. A term the source does name passes through, and
    the release stays authoritative for its second name.

    The label column cannot be empty, and whatever built the batch cannot see
    what the database holds, so falling back to the term_id keeps the row
    honest: it asserts only what the source said, and a later release naming
    the term overwrites it like any other label."""
    resolved: list[ParsedTerm] = []
    for term in parsed_terms:
        if term.label is not None:
            resolved.append(term)
            continue
        # No label makes the record's names malformed, so discard any
        # alternate_label it supplied along with the label.
        prior = prior_state.get(term.term_id)
        stored_label = prior.label if prior is not None else term.term_id
        stored_alternate_label = prior.alternate_label if prior is not None else None
        resolved.append(replace(term, label=stored_label, alternate_label=stored_alternate_label))
    return resolved


async def _upsert_terms_without_replaced_by(
    conn: asyncpg.Connection,
    *,
    terminology_idx: int,
    parsed_terms: list[ParsedTerm],
    loading_version: str,
) -> None:
    """Insert-or-update every incoming term, setting replaced_by=NULL.

    The version that first obsoletes a term stamps obsoleted_in_version, and
    un-obsoleting clears it. A term whose stored values already match the
    incoming ones passes through unwritten, so a reload costs row versions
    only for what actually moved."""
    if not parsed_terms:
        return
    term_ids = [term.term_id for term in parsed_terms]
    labels = [term.label for term in parsed_terms]
    alternate_labels = [term.alternate_label for term in parsed_terms]
    is_obsoletes = [term.is_obsolete for term in parsed_terms]
    obsoletion_kinds = [
        str(term.obsoletion_kind) if term.obsoletion_kind is not None else None
        for term in parsed_terms
    ]

    # One INSERT ... ON CONFLICT DO UPDATE call keyed on
    # (terminology_idx, term_id) drives both new-row and update paths
    # from the same row source: unnesting five parallel arrays
    # ($2 term_ids, $3 labels, $4 alternate_labels, $5 is_obsoletes,
    # $6 obsoletion_kinds) zips into one tuple per incoming term.
    # The INSERT side stamps obsoleted_in_version with loading_version ($7)
    # iff the row arrives obsolete, else NULL; NB: replaced_by is always NULL
    # because pass 1 has not yet produced the term_id -> idx map.
    # The ON CONFLICT side overwrites label, alternate_label, is_obsolete,
    # and obsoletion_kind unconditionally, so the release is authoritative
    # for each of them and one supplying no second name clears a stored one;
    # obsoleted_in_version uses COALESCE(existing, EXCLUDED) when still
    # obsolete so the first version that obsoleted the term sticks across
    # reloads, and clears to NULL on un-obsoletion; NB: every update wipes
    # replaced_by so the later replaced_by-setting step starts clean.
    # The WHERE on the update branch skips a row in which nothing would
    # move, because Postgres stores a new row version per UPDATE without
    # comparing values and a reload would otherwise leave one dead tuple
    # per term whether it changed or not.
    # obsoleted_in_version needs no clause of its own: the alignment CHECK
    # ties its nullness to is_obsolete, so it can only move when is_obsolete
    # does. A row carrying a replaced_by must be rewritten whatever else
    # matches, since the wipe above lets the next step drop a pointer the new
    # release no longer asserts.
    await conn.execute(
        "INSERT INTO qiita.terminology_term"
        "   (terminology_idx, term_id, label, alternate_label, is_obsolete,"
        "    obsoletion_kind, obsoleted_in_version, replaced_by)"
        " SELECT $1, t, l, al, o,"
        "        k::qiita.terminology_term_obsoletion_kind,"
        "        CASE WHEN o THEN $7 ELSE NULL END,"
        "        NULL"
        "   FROM unnest($2::text[], $3::text[], $4::text[], $5::bool[], $6::text[])"
        "        AS s(t, l, al, o, k)"
        " ON CONFLICT (terminology_idx, term_id) DO UPDATE"
        "   SET label = EXCLUDED.label,"
        "       alternate_label = EXCLUDED.alternate_label,"
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
        "       replaced_by = NULL"
        " WHERE qiita.terminology_term.label IS DISTINCT FROM EXCLUDED.label"
        "    OR qiita.terminology_term.alternate_label"
        "       IS DISTINCT FROM EXCLUDED.alternate_label"
        "    OR qiita.terminology_term.is_obsolete IS DISTINCT FROM EXCLUDED.is_obsolete"
        "    OR qiita.terminology_term.obsoletion_kind"
        "       IS DISTINCT FROM EXCLUDED.obsoletion_kind"
        "    OR qiita.terminology_term.replaced_by IS NOT NULL",
        terminology_idx,
        term_ids,
        labels,
        alternate_labels,
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
    """Populate replaced_by for every obsolete row whose incoming entry names a
    replacement within this terminology. Precondition: terminology_term
    already holds every term in the batch.

    Resolution stays scoped to this terminology: a pointer naming another
    vocabulary's term — which sources can emit — drops out before reaching
    here, so every pointer in a batch resolves in-terminology by construction.
    The db itself could accept a cross-terminology target; supporting that
    would mean revisiting this."""
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
    terminology. Preserves existing notes content, separating the new line
    with a newline when prior content exists.

    The notes column carries both loader and operator content, so entries
    accumulate across reloads; the version stamp in each line distinguishes
    one tolerate run from another."""
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
    """Replace every closure row scoped to this terminology and return the
    DB-side inserted count. The inner JOINs drop a tuple naming a term_id
    terminology_term does not hold, so the returned count may be less than
    len(parsed_closure); an earlier check settles whether such a tuple is
    acceptable at all."""
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
    alternate_label_updated = 0
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
        if prior.alternate_label != term.alternate_label:
            alternate_label_updated += 1
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
        "alternate_label_updated": alternate_label_updated,
        "newly_obsoleted": newly_obsoleted,
        "newly_merged": newly_merged,
    }
