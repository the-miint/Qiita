"""Cross-route helpers shared by sibling route modules.

Centralizing them keeps response wording consistent across parallel
endpoints — same input shape, same on-the-wire output.
"""

from collections.abc import Awaitable, Callable
from datetime import datetime

import asyncpg
from fastapi import HTTPException
from qiita_common.models import MissingReasonRef, TerminologyTermRef

from ..repositories._sample_helpers import (
    ConflictingValueDifferentStudyError,
    ConflictingValueSameStudyError,
    DuplicateValueDifferentStudyError,
    DuplicateValueSameStudyError,
    MetadataChecklistUnknownError,
    SlotOccupiedByMissingReasonError,
    SlotOccupiedByTypedValueError,
    SlotOccupiedError,
    TransientWriteRaceError,
    fetch_metadata_checklist_idx_by_name,
)


def _attempted_label(value: object) -> str:
    """Render the 'what was attempted' noun for a slot-collision message.

    Returns "missing-reason marker" for MissingReasonRef, "terminology
    term" for TerminologyTermRef, and "value" for bare typed scalars.
    """
    if isinstance(value, MissingReasonRef):
        return "missing-reason marker"
    if isinstance(value, TerminologyTermRef):
        return "terminology term"
    return "value"


# Shared 422-detail string for a foreign-key violation whose constraint
# is not in a route's specific message map. Lifted here so the wording
# stays identical across every route that falls back to it.
GENERIC_FK_VIOLATION = "references a row that does not exist"


def raise_for_unique_violation(
    exc: asyncpg.UniqueViolationError,
    *,
    constraint_messages: dict[str, str],
    generic: str,
) -> None:
    """Translate a UNIQUE-constraint violation into a 409 response.

    Looks up `exc.constraint_name` against `constraint_messages`; an
    unknown name yields `generic` as the detail. Never returns; always
    raises HTTPException.
    """
    detail = constraint_messages.get(exc.constraint_name, generic)
    raise HTTPException(status_code=409, detail=detail)


async def resolve_metadata_checklist_idx(
    conn: asyncpg.Connection,
    name: str | None,
) -> int | None:
    """Resolve a caller-supplied checklist name to its idx for a write.

    None passes through as None. An unknown name is mapped to a 422 so
    every create/patch surface reports it identically rather than letting
    it surface as a downstream FK violation.
    """
    try:
        return await fetch_metadata_checklist_idx_by_name(conn, name)
    except MetadataChecklistUnknownError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"metadata_checklist_name {exc.name!r} does not reference an existing checklist",  # noqa: E501
        )


def etag_for_updated_at(updated_at: datetime) -> str:
    """Build the quoted ETag header value from a row's updated_at timestamp.

    The surrounding double-quotes are required by RFC 7232's entity-tag
    grammar — the on-the-wire value is `"<iso8601>"`, not `<iso8601>`.
    The inner ISO 8601 timestamp is opaque to clients; only its
    byte-for-byte equality with a subsequent If-Match header matters.
    """
    return f'"{updated_at.isoformat()}"'


def require_if_match(if_match: str | None) -> str:
    """Raise 428 when the caller did not send an If-Match header.

    All patching requires optimistic-concurrency control; routing
    the 428 through this helper keeps the wording identical."""
    if if_match is None:
        raise HTTPException(status_code=428, detail="If-Match header required")
    return if_match


def require_etag_match(
    row: asyncpg.Record | None,
    *,
    if_match: str,
    label: str,
    row_idx: int,
) -> None:
    """Run the post-FOR-UPDATE-preflight 404 / 412 checks for a PATCH route.

    Called after `fetch_<entity>(conn, idx, for_update=True)` to fold
    "row absent (404)" and "ETag stale (412)" into one site so every
    PATCH endpoint emits the same wording. `label` is the entity noun
    embedded in the 404 detail (e.g. "study", "biosample").
    """
    if row is None:
        raise HTTPException(status_code=404, detail=f"{label} {row_idx} not found")
    if if_match != etag_for_updated_at(row["updated_at"]):
        raise HTTPException(status_code=412, detail="If-Match did not match")


async def detail_for_slot_collision(
    conn: asyncpg.Connection,
    exc: SlotOccupiedError,
) -> str:
    """Build the HTTP-409 detail string for a metadata slot collision.

    Dispatches on the SlotOccupiedError subclass to produce a sub-case-
    specific message; both metadata-writing routes (biosample import,
    sequenced-sample create) call this so wording stays consistent
    across endpoints. Both the global-write and local-write paths route
    through this dispatcher: exc.global_field_idx is non-None for the
    global path and None for the local path, selecting the slot
    identifier embedded in the message. For the missing-reason sub-case
    the helper resolves the missing_value_reason row's name with one
    extra SELECT against the spec-known table — actionability over
    terseness, so the caller learns what reason occupies the slot.

    Returns the bare string; the caller wraps it in HTTPException with
    status 409. Per the project decision, all six sub-cases return 409
    (Conflict). Contributing study idx is surfaced so callers can
    correlate; study name is intentionally not joined (caller may not
    have read access to that study).
    """
    # The same/different subclasses cover typed-vs-typed,
    # missing-vs-missing, and terminology-vs-terminology equality; the
    # attempted_value's kind selects the wording noun per branch via
    # _attempted_label.
    what = _attempted_label(exc.attempted_value)
    # Slot identifier: global path is keyed by global_field_idx, local
    # path by the entity-scoped study_field_idx. The non-None-ness of
    # exc.global_field_idx discriminates without a separate flag.
    slot_id = (
        f"global_field_idx={exc.global_field_idx}"
        if exc.global_field_idx is not None
        else f"{exc.entity_kind}_study_field_idx={exc.study_field_idx}"
    )
    # Match on the concrete subclass to pick the right wording. The
    # generic SlotOccupiedError fallback covers any future subclass
    # added without a wording branch here; reading the catch-all
    # message in production points the maintainer at this dispatch.
    if isinstance(exc, DuplicateValueSameStudyError):
        return (
            f"your study already wrote this same {what} for field"
            f" {exc.display_name!r} on {exc.entity_kind}_idx={exc.entity_idx}"
            f" ({slot_id}); no new row was created"
        )
    if isinstance(exc, ConflictingValueSameStudyError):
        return (
            f"your study previously wrote a different {what} for field"
            f" {exc.display_name!r} on {exc.entity_kind}_idx={exc.entity_idx}"
            f" ({slot_id});"
            f" correct it via PATCH or DELETE+INSERT, not INSERT"
        )
    if isinstance(exc, DuplicateValueDifferentStudyError):
        return (
            f"the {what} you attempted is already present for field"
            f" {exc.display_name!r} on {exc.entity_kind}_idx={exc.entity_idx}"
            f" ({slot_id}), contributed by"
            f" study_idx={exc.contributing_study_idx}; your study does"
            f" not own the row"
        )
    if isinstance(exc, ConflictingValueDifferentStudyError):
        return (
            f"another study (study_idx={exc.contributing_study_idx}) has"
            f" written a different {what} for field"
            f" {exc.display_name!r} on {exc.entity_kind}_idx={exc.entity_idx}"
            f" ({slot_id});"
            f" the global field's canonical value is in dispute"
        )
    if isinstance(exc, SlotOccupiedByMissingReasonError):
        # One extra SELECT to resolve the human-readable reason name so the
        # caller knows what reason occupies the slot; the missing_value_reason
        # table is shared across all entity kinds (no spec dispatch needed).
        # existing_missing_reason_idx is non-None whenever this subclass fires
        # (the diagnose path only constructs it when the missing-reason FK is
        # populated); the assert documents the invariant for asyncpg's binder.
        assert exc.existing_missing_reason_idx is not None
        reason_name = await conn.fetchval(
            "SELECT name FROM qiita.missing_value_reason WHERE idx = $1",
            exc.existing_missing_reason_idx,
        )
        return (
            f"the value for field {exc.display_name!r} on"
            f" {exc.entity_kind}_idx={exc.entity_idx} ({slot_id}) is"
            f" recorded as intentionally missing (reason: {reason_name});"
            f" the missing-reason row must be deleted before a typed"
            f" value can be written"
        )
    if isinstance(exc, SlotOccupiedByTypedValueError):
        # Existing typed value travels on the exception payload — no DB
        # roundtrip needed for str/Decimal/date. A terminology-term slot
        # carries an int FK (qiita.terminology_term.idx); resolve it to
        # the human-readable term_id + label with one extra SELECT so the
        # caller sees what term occupies the slot rather than a bare idx.
        # str values render via repr() (quoting distinguishes "123" from 123);
        # Decimal / date render via str() so the body shows "1.5" / "2024-01-02"
        # instead of "Decimal('1.5')" / "datetime.date(2024, 1, 2)".
        if isinstance(exc.existing_value, int) and not isinstance(exc.existing_value, bool):
            term_row = await conn.fetchrow(
                "SELECT term_id, label FROM qiita.terminology_term WHERE idx = $1",
                exc.existing_value,
            )
            rendered_existing = (
                f"terminology term {term_row['term_id']!r} ({term_row['label']!r})"
                if term_row is not None
                else f"terminology_term_idx={exc.existing_value}"
            )
        elif isinstance(exc.existing_value, str):
            rendered_existing = repr(exc.existing_value)
        else:
            rendered_existing = str(exc.existing_value)
        return (
            f"the value for field {exc.display_name!r} on"
            f" {exc.entity_kind}_idx={exc.entity_idx} ({slot_id}) is"
            f" already recorded as a typed value ({rendered_existing});"
            f" the typed row must be deleted before a missing-reason"
            f" marker can be written"
        )
    # Fallback: an unrecognised subclass means the exception hierarchy
    # grew without the dispatch above being extended. Surface a generic
    # message rather than crashing the route; the maintainer can find
    # the missing branch via this string.
    return (
        f"{exc.entity_kind}_metadata slot for {exc.display_name!r} is"
        f" already occupied (entity_idx={exc.entity_idx}, {slot_id})"
    )


def parse_kv_detail(detail: str | None) -> dict[str, str]:
    """Parse a Postgres error ``DETAIL`` of comma-separated ``key=value`` pairs.

    A trigger that needs to hand structured data to a route puts it in
    the error's DETAIL field as ``k1=v1, k2=v2`` rather than embedding it
    in the human-readable MESSAGE: regex-parsing MESSAGE for data couples
    route code to migration string literals and breaks silently when the
    wording is edited. Chunks without an ``=`` are skipped; a None or
    empty detail yields an empty dict.

    Splitting is on a bare ``,`` — values must be comma-free (the current
    callers emit integers and schema identifiers, both safe). A value
    that can contain a comma needs a different encoding.
    """
    fields: dict[str, str] = {}
    if not detail:
        return fields
    for chunk in detail.split(","):
        key, sep, value = chunk.partition("=")
        if sep:
            fields[key.strip()] = value.strip()
    return fields


def detail_for_biosample_link_rejection(detail_fields: dict[str, str]) -> str:
    """Build the HTTP-422 detail for a rejected prep_sample_to_study link.

    Takes the already-parsed DETAIL fields (see parse_kv_detail) emitted
    by the prep_sample_to_study_reject_without_biosample_link trigger in
    db/migrations/20260501000011_prep_sample.sql. That trigger fires once
    per link row the sequenced-sample composer inserts — a primary study
    plus zero or more secondaries — so "the requested study" is ambiguous
    when a body lists several; this helper names the exact study that
    lacks a biosample link. Missing keys degrade to ``?`` so a trigger
    that ever stops emitting DETAIL produces a vague message instead of
    crashing the route.
    """
    study_idx = detail_fields.get("study_idx", "?")
    biosample_idx = detail_fields.get("biosample_idx", "?")
    return (
        f"prep_sample cannot be linked to study_idx={study_idx}:"
        f" biosample_idx={biosample_idx} is not linked to that study"
        " (or the link is retired)"
    )


# Retry-After is advisory; the race self-resolves the instant the
# concurrent delete commits, so a 1-second hint is generous. Sent as a
# string because that is the on-the-wire header value.
_TRANSIENT_WRITE_RACE_RETRY_AFTER = "1"


def raise_for_transient_write_race(exc: TransientWriteRaceError) -> None:
    """Translate a lost write race into a 503 retry response.

    Both metadata-writing routes call this so the status, wording, and
    Retry-After hint stay identical across endpoints. The occupant that
    triggered the unique violation was concurrently deleted before it
    could be diagnosed, so the slot is free again and the same request
    will succeed on resubmission — 503 (transient) with Retry-After, not
    409 (the state is not actually in conflict) and not 500.

    Never returns; always raises HTTPException.
    """
    raise HTTPException(
        status_code=503,
        detail=(
            f"a concurrent delete raced your {exc.row_label} write"
            f" ({exc.slot_summary}); the slot is now free —"
            f" resubmit the identical request"
        ),
        headers={"Retry-After": _TRANSIENT_WRITE_RACE_RETRY_AFTER},
    )


async def resolve_idxs_by_natural_key(
    *,
    values: list[str],
    fetcher: Callable[[list[str]], Awaitable[dict[str, int]]],
) -> tuple[dict[str, int], list[str]]:
    """Dedup `values` in input order, resolve survivors via `fetcher`, and
    return `(resolved, missing)`.

    The caller supplies `fetcher` already bound to its pool and any per-key
    SQL details so this helper stays table-agnostic. `missing` is the
    input-order deduped list of values that did not resolve.
    """
    # Dedup while preserving input order so `missing` is deterministic.
    dedup_ordered: list[str] = []
    seen: set[str] = set()
    for v in values:
        if v in seen:
            continue
        seen.add(v)
        dedup_ordered.append(v)

    resolved = await fetcher(dedup_ordered)
    missing = [v for v in dedup_ordered if v not in resolved]
    return resolved, missing
