"""Sample-family (biosample + prep_sample) cross-entity helpers.

Holds the shapes, exceptions, and write-and-diagnose machinery that the
biosample and prep_sample repository modules share, so the parallel
implementations stay coordinated without duplicating logic.
"""

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Literal, NamedTuple

import asyncpg
from qiita_common.models import (
    MISSING_REASON_VALUE_COLUMN,
    TERMINOLOGY_TERM_VALUE_COLUMN,
    FieldDataType,
    MissingReasonRef,
    TerminologyTermRef,
    Tier,
)

from . import require_transaction


class SampleEntityKind(StrEnum):
    """Discriminator for the entity domain a sample-family operation targets.
    Values match the table-name prefix (biosample_*, prep_sample_*) used
    throughout the schema.
    """

    BIOSAMPLE = "biosample"
    PREP_SAMPLE = "prep_sample"


# One globally-linked metadata value as it travels through this module:
# a parsed scalar, an intentionally-missing marker, or a terminology term.
# Mirrors the wire-side qiita_common.models.GlobalMetadataEntry.value union.
type GlobalMetadataValue = str | Decimal | date | MissingReasonRef | TerminologyTermRef


# ---------------------------------------------------------------------------
# Shared *_global_field lookup row shape
# ---------------------------------------------------------------------------


class GlobalFieldRow(NamedTuple):
    """Subset of *_global_field columns used by metadata pre-flight reads.

    terminology_idx is non-None iff data_type is TERMINOLOGY (enforced by
    the *_global_field CHECK), and identifies the terminology that scopes
    any term-id lookup against the field.
    """

    idx: int
    display_name: str
    data_type: FieldDataType
    terminology_idx: int | None


# ---------------------------------------------------------------------------
# Shared globally-linked metadata read shape and value-column dispatch
# ---------------------------------------------------------------------------


class GlobalMetadataRow(NamedTuple):
    """One row of globally-linked metadata: the field's stable internal_name,
    the cosmetic display_name and description from the *_global_field row
    (not study-scoped), its data_type, and the value extracted from the row
    — either the typed Python value from the matching value_* column, a
    MissingReasonRef carrying an intentionally-missing reason's idx + name,
    or a TerminologyTermRef carrying a terminology-term's idx + term_id + label.
    """

    internal_name: str
    display_name: str
    description: str | None
    data_type: FieldDataType
    value: GlobalMetadataValue


# Closed set of data_types currently decoded. BOOLEAN is intentionally
# absent so adding it requires updating both the value_* column mapping
# and any sibling write-side parsers together.
GLOBAL_METADATA_VALUE_COLUMN: dict[FieldDataType, str] = {
    FieldDataType.TEXT: "value_text",
    FieldDataType.NUMERIC: "value_numeric",
    FieldDataType.DATE: "value_date",
    FieldDataType.TERMINOLOGY: TERMINOLOGY_TERM_VALUE_COLUMN,
}


# ---------------------------------------------------------------------------
# Shared metadata parse error and text-to-typed coercion
# ---------------------------------------------------------------------------


class MetadataUnknownFieldsError(Exception):
    """Raised when metadata input names display_names with no matching
    {entity_kind}_global_field row. Carries every unknown name in one list
    so the full set can be surfaced together.
    """

    def __init__(self, entity_kind: SampleEntityKind, unknown_display_names: list[str]) -> None:
        self.unknown_display_names = unknown_display_names
        super().__init__(
            f"unknown {entity_kind} global field display_names: {unknown_display_names!r}"
        )


class MetadataChecklistUnknownError(Exception):
    """Raised when a metadata_checklist name has no matching
    qiita.metadata_checklist row. Carries the unknown name.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"unknown metadata_checklist name: {name!r}")


class StudyFieldConflictError(Exception):
    """Raised when a {entity_kind}_study_field row at (study_idx, display_name)
    already exists but is purely-local (found_global_field_idx is None) or is
    globally linked to a different global field than the one requested.
    """

    def __init__(
        self,
        entity_kind: SampleEntityKind,
        study_idx: int,
        display_name: str,
        expected_global_field_idx: int,
        found_global_field_idx: int | None,
    ) -> None:
        self.entity_kind = entity_kind
        self.study_idx = study_idx
        self.display_name = display_name
        self.expected_global_field_idx = expected_global_field_idx
        self.found_global_field_idx = found_global_field_idx
        super().__init__(
            f"{entity_kind}_study_field at study_idx={study_idx},"
            f" display_name={display_name!r} is bound to global"
            f" {found_global_field_idx!r}, expected {expected_global_field_idx!r}"
        )


class MetadataParseError(Exception):
    """Raised when a metadata text value cannot be coerced into the Python
    type matching its global field's data_type. Carries the failing
    display_name plus the raw inputs for field-scoped diagnostics.
    """

    def __init__(
        self,
        display_name: str,
        data_type: FieldDataType,
        text_value: str,
        reason: str,
    ) -> None:
        self.display_name = display_name
        self.data_type = data_type
        self.text_value = text_value
        self.reason = reason
        super().__init__(
            f"could not parse {display_name!r} value {text_value!r} as {data_type}: {reason}"
        )


class TransientWriteRaceError(Exception):
    """Raised when an INSERT-then-diagnostic-SELECT pair lost the row it
    expected to inspect: a concurrent transaction deleted-and-committed
    the colliding occupant between the unique-violation signal and the
    follow-up SELECT. The slot is free again — a benign lost race, not
    schema corruption — and the right answer is to retry the identical
    request. row_label and slot_summary are caller-supplied so this class
    stays agnostic to which table or slot kind it describes.
    """

    def __init__(
        self,
        *,
        row_label: str,
        slot_summary: str,
    ) -> None:
        self.row_label = row_label
        self.slot_summary = slot_summary
        super().__init__(
            f"{row_label} write raced a concurrent delete on slot"
            f" {slot_summary}; the occupant vanished before it could be"
            f" diagnosed — retry"
        )


def parse_text_for_data_type(
    display_name: str,
    data_type: FieldDataType,
    text_value: str,
) -> str | Decimal | date:
    """Coerce a text input into the Python type matching data_type.

    Outer whitespace is stripped before parsing. TEXT returns the stripped
    string; NUMERIC returns Decimal; DATE returns datetime.date. BOOLEAN
    and TERMINOLOGY raise NotImplementedError. Conversion failures raise
    MetadataParseError carrying display_name, data_type, raw text, and
    reason.
    """
    # Normalize once; all parse arms see the stripped value.
    stripped = text_value.strip()
    if data_type is FieldDataType.TEXT:
        return stripped
    if data_type is FieldDataType.NUMERIC:
        try:
            return Decimal(stripped)
        except InvalidOperation as exc:
            raise MetadataParseError(
                display_name=display_name,
                data_type=data_type,
                text_value=text_value,
                reason="not a valid decimal number",
            ) from exc
    if data_type is FieldDataType.DATE:
        try:
            return date.fromisoformat(stripped)
        except ValueError as exc:
            raise MetadataParseError(
                display_name=display_name,
                data_type=data_type,
                text_value=text_value,
                reason="not a valid ISO date (YYYY-MM-DD)",
            ) from exc
    # Closed-set fallback: BOOLEAN and TERMINOLOGY land here and raise.
    raise NotImplementedError(
        f"text-to-typed parsing for data_type={data_type} is not yet implemented"
    )


# ---------------------------------------------------------------------------
# Cross-entity metadata-write dispatch: spec + shared read/write helpers
# ---------------------------------------------------------------------------


class SampleMetadataWriteResult(NamedTuple):
    """Successful sample-family metadata write outcome: the new metadata
    row's idx, the study_field idx the value was attached to, and whether
    that study_field row was created (versus reused from a get-or-create
    lookup).
    """

    metadata_idx: int
    study_field_idx: int
    study_field_created: bool


@dataclass(frozen=True)
class EntityMetadataSpec:
    """Per-entity SQL-identifier binding for the sample-family helpers.

    Holds the entity discriminator plus the table, column, and constraint
    identifiers that differ between the biosample and prep_sample stacks,
    so the shared helpers stay agnostic. Bindings cover the metadata table,
    the global-field and study-field tables, the constraint names the
    diagnostic paths key on, and the per-study link table.
    """

    entity_kind: SampleEntityKind
    metadata_table: str
    # The *_global_field table (biosample_global_field / prep_sample_global_field)
    # the globally-linked reads resolve display_name and data_type against.
    global_field_table: str
    entity_key_column: str
    study_field_table: str
    study_field_idx_column: str
    # The FK column on study_field_table pointing at the *_global_field
    # table (biosample_global_field_idx / prep_sample_global_field_idx).
    # NULL on a purely-local row, non-NULL on a globally-linked one.
    study_field_global_fk_column: str
    global_field_unique_index_name: str
    local_unique_per_field_index_name: str
    # The per-study link table (biosample_to_study / prep_sample_to_study)
    # and the entity-id column on it (biosample_idx / prep_sample_idx).
    link_table: str
    link_entity_key_column: str


async def fetch_global_fields_by_display_names(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    display_names: Iterable[str],
) -> dict[str, GlobalFieldRow]:
    """Return a dict of display_name -> GlobalFieldRow for the matching
    rows in the *_global_field table named by spec.global_field_table.

    Display names that have no matching row are absent from the returned
    dict; callers detect "unknown field" by checking dict membership for
    each requested name. Empty input short-circuits with no DB call.
    """
    # Materialize so emptiness is detectable and the param can be passed as ANY.
    names = list(display_names)
    if not names:
        return {}

    # f-string interpolation of the table identifier is safe: spec fields
    # are frozen module-level constants, never reached by caller input.
    rows = await pool_or_conn.fetch(
        f"SELECT idx, display_name, data_type, terminology_idx"
        f" FROM {spec.global_field_table}"
        f" WHERE display_name = ANY($1::text[])",
        names,
    )

    # Wrap each row in the typed tuple, keyed on display_name.
    return {
        r["display_name"]: GlobalFieldRow(
            idx=r["idx"],
            display_name=r["display_name"],
            data_type=FieldDataType(r["data_type"]),
            terminology_idx=r["terminology_idx"],
        )
        for r in rows
    }


async def fetch_missing_value_reason_idxs_by_names(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    names: Iterable[str],
) -> dict[str, int]:
    """Return a dict of name -> idx for every qiita.missing_value_reason row
    whose name appears in `names`.

    Names absent from the table are absent from the returned dict. Empty
    input short-circuits with no DB call. No is_obsolete filter — any row
    in the table is treated as a valid marker; the obsoletion lifecycle is
    not yet exercised.
    """
    # Materialize so emptiness is detectable and the param can be passed as ANY.
    candidate_names = list(names)
    if not candidate_names:
        return {}

    # Single batch SELECT keyed on name; the column is UNIQUE NOT NULL so
    # the row count is bounded by len(candidate_names).
    rows = await pool_or_conn.fetch(
        "SELECT idx, name FROM qiita.missing_value_reason WHERE name = ANY($1::text[])",
        candidate_names,
    )
    return {r["name"]: r["idx"] for r in rows}


async def fetch_terminology_term_idxs_by_term_ids(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    terminology_idx: int,
    term_ids: Iterable[str],
) -> dict[str, tuple[int, str]]:
    """Return a dict of term_id -> (idx, label) for every qiita.terminology_term
    row whose term_id appears in `term_ids` AND whose terminology_idx matches.

    Term ids absent from the table are absent from the returned dict. Empty
    input short-circuits with no DB call. No is_obsolete filter — any row in
    the table scoped to this terminology is treated as a valid marker; the
    obsoletion lifecycle is not yet exercised. Scoped to one terminology_idx
    because (terminology_idx, term_id) is the table's unique key — the same
    term_id can recur across different terminologies.
    """
    # Materialize so emptiness is detectable and the param can be passed as ANY.
    candidate_term_ids = list(term_ids)
    if not candidate_term_ids:
        return {}

    # Single batch SELECT keyed on term_id and scoped to terminology_idx.
    # The (terminology_idx, term_id) UNIQUE constraint bounds the row count
    # by len(candidate_term_ids).
    rows = await pool_or_conn.fetch(
        "SELECT idx, term_id, label FROM qiita.terminology_term"
        " WHERE terminology_idx = $1 AND term_id = ANY($2::text[])",
        terminology_idx,
        candidate_term_ids,
    )
    return {r["term_id"]: (r["idx"], r["label"]) for r in rows}


async def fetch_metadata_checklist_idx_by_name(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    name: str | None,
) -> int | None:
    """Resolve a metadata_checklist name to its idx.

    None passes through as None (no checklist requested). A non-null name
    with no matching row raises MetadataChecklistUnknownError so the caller
    surfaces a clean error instead of a downstream FK violation.
    metadata_checklist.name is UNIQUE, so at most one row matches.
    """
    if name is None:
        return None
    idx = await pool_or_conn.fetchval(
        "SELECT idx FROM qiita.metadata_checklist WHERE name = $1", name
    )
    if idx is None:
        raise MetadataChecklistUnknownError(name)
    return idx


async def fetch_global_metadata(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
) -> dict[str, GlobalMetadataRow]:
    """Return internal_name -> GlobalMetadataRow for every globally-linked
    metadata value the entity carries.

    Filters on global_field_idx IS NOT NULL (purely-local rows are
    excluded). Intentionally-missing entries (value_missing_reason_idx
    populated) surface as MissingReasonRef in the row's `value`;
    terminology-term entries (value_terminology_term_idx populated)
    surface as TerminologyTermRef. Both Ref kinds supersede
    data_type-driven decoding. Other typed rows require data_type in
    {TEXT, NUMERIC, DATE}; unsupported data_types raise
    NotImplementedError. Not study-scoped: the canonical global value
    persists across link retirement.
    """
    # f-string interpolation of the table identifiers is safe: all
    # (including spec fields) are frozen constants, never reached by caller input.
    # LEFT JOINs on qiita.missing_value_reason and qiita.terminology_term so
    # a Ref-surfaced row's display payload comes back in one round trip; typed
    # rows have both join keys NULL.
    rows = await pool_or_conn.fetch(
        f"SELECT gf.internal_name, gf.display_name, gf.description, gf.data_type,"
        f" m.value_text, m.value_numeric, m.value_date,"
        f" m.{MISSING_REASON_VALUE_COLUMN}, mvr.name AS missing_reason_name,"
        f" m.{TERMINOLOGY_TERM_VALUE_COLUMN},"
        f" tt.term_id AS terminology_term_id,"
        f" tt.label AS terminology_term_label"
        f" FROM {spec.metadata_table} m"
        f" JOIN {spec.global_field_table} gf ON gf.idx = m.global_field_idx"
        f" LEFT JOIN qiita.missing_value_reason mvr"
        f"   ON mvr.idx = m.{MISSING_REASON_VALUE_COLUMN}"
        f" LEFT JOIN qiita.terminology_term tt"
        f"   ON tt.idx = m.{TERMINOLOGY_TERM_VALUE_COLUMN}"
        f" WHERE m.{spec.entity_key_column} = $1"
        f"   AND m.global_field_idx IS NOT NULL",
        entity_idx,
    )

    # Walk rows. Ref kinds take precedence over data_type: a row with
    # value_missing_reason_idx populated surfaces as MissingReasonRef and
    # a row with value_terminology_term_idx populated surfaces as
    # TerminologyTermRef, regardless of data_type. Other typed rows
    # dispatch to the value column the data_type names; the unsupported
    # branch raises so an out-of-set data_type cannot silently surface
    # a NULL value.
    result: dict[str, GlobalMetadataRow] = {}
    for r in rows:
        data_type = FieldDataType(r["data_type"])
        missing_reason_idx = r[MISSING_REASON_VALUE_COLUMN]
        terminology_term_idx = r[TERMINOLOGY_TERM_VALUE_COLUMN]
        value: GlobalMetadataValue
        if missing_reason_idx is not None:
            value = MissingReasonRef(idx=missing_reason_idx, name=r["missing_reason_name"])
        elif terminology_term_idx is not None:
            value = TerminologyTermRef(
                idx=terminology_term_idx,
                term_id=r["terminology_term_id"],
                label=r["terminology_term_label"],
            )
        else:
            column = GLOBAL_METADATA_VALUE_COLUMN.get(data_type)
            if column is None:
                raise NotImplementedError(
                    f"global metadata read for data_type={data_type} is not yet implemented"
                )
            value = r[column]
        result[r["internal_name"]] = GlobalMetadataRow(
            internal_name=r["internal_name"],
            display_name=r["display_name"],
            description=r["description"],
            data_type=data_type,
            value=value,
        )
    return result


# ---------------------------------------------------------------------------
# Metadata slot-collision exception family
# ---------------------------------------------------------------------------
#
# SlotOccupiedError is a plain Exception, not an
# asyncpg.UniqueViolationError subclass: it is raised only after the
# triggering UniqueViolationError has been caught and the slot occupant
# diagnosed, so it carries diagnostic payload rather than raw Postgres
# attributes. Both globally-linked and purely-local writes raise from
# this hierarchy; global_field_idx is non-None for the global-path
# discriminator and None for the local path.
# ---------------------------------------------------------------------------


class SlotOccupiedError(Exception):
    """Base class: a *_metadata write failed because the entity already has
    a row for the same field. global_field_idx is non-None when the slot
    was rejected by the cross-study partial unique index; None when it was
    rejected by the per-field unique constraint on a purely-local field.
    The concrete subclass names which sub-case applies; subclass bodies
    are empty by design — the discriminator is the type. For local-path
    writes attempted_study_idx and contributing_study_idx are equal by
    construction, so the *DifferentStudy leaves are unreachable.
    """

    def __init__(
        self,
        *,
        entity_kind: SampleEntityKind,
        entity_idx: int,
        display_name: str,
        study_field_idx: int,
        attempted_study_idx: int,
        contributing_study_idx: int,
        attempted_value: GlobalMetadataValue,
        data_type: FieldDataType,
        existing_metadata_idx: int,
        # int arm is the terminology_term.idx FK (read from the typed value
        # column of a TERMINOLOGY-typed row); the scalar arms cover str/Decimal/date.
        existing_value: str | Decimal | date | int | None,
        existing_missing_reason_idx: int | None,
        global_field_idx: int | None = None,
    ) -> None:
        self.entity_kind = entity_kind
        self.entity_idx = entity_idx
        self.display_name = display_name
        self.study_field_idx = study_field_idx
        self.attempted_study_idx = attempted_study_idx
        self.contributing_study_idx = contributing_study_idx
        self.attempted_value = attempted_value
        self.data_type = data_type
        self.existing_metadata_idx = existing_metadata_idx
        self.existing_value = existing_value
        self.existing_missing_reason_idx = existing_missing_reason_idx
        self.global_field_idx = global_field_idx
        # Lead with the caller-facing display_name; idxs follow as
        # parenthetical operator context. The slot identifier varies by
        # path: the global path is keyed by global_field_idx, the local
        # path by the entity-scoped study_field_idx.
        slot_id = (
            f"global_field_idx={global_field_idx}"
            if global_field_idx is not None
            else f"{entity_kind}_study_field_idx={study_field_idx}"
        )
        super().__init__(
            f"{entity_kind}_metadata slot for {display_name!r}"
            f" ({entity_kind}_idx={entity_idx}, {slot_id})"
            f" is already occupied by"
            f" {entity_kind}_metadata_idx={existing_metadata_idx}"
        )


class DuplicateValueSameStudyError(SlotOccupiedError):
    """Existing row's value equals the attempted value; the caller's study is
    the contributing study. Idempotent confirm — no write was performed."""


class ConflictingValueSameStudyError(SlotOccupiedError):
    """Existing row's value differs from the attempted value; the caller's
    study is the contributing study. The caller asked to INSERT but a row
    already exists; correction requires an explicit PATCH or DELETE+INSERT."""


class DuplicateValueDifferentStudyError(SlotOccupiedError):
    """Existing row's value equals the attempted value; another study
    contributed it. The desired global state already exists — but the
    caller's study does not own the row. Unreachable from the local
    write path (single-study by construction)."""


class ConflictingValueDifferentStudyError(SlotOccupiedError):
    """Existing row's value differs from the attempted value; another study
    contributed it. The real cross-study conflict — the global field's
    canonical value is in dispute. Unreachable from the local write
    path (single-study by construction)."""


class SlotOccupiedByMissingReasonError(SlotOccupiedError):
    """The slot holds a row recorded as intentionally missing
    (value_missing_reason_idx populated); the caller attempted to write
    something other than a missing-reason marker (a typed value or a
    terminology term). The missing-reason row must be deleted before a
    non-missing value can be written."""


class SlotOccupiedByTypedValueError(SlotOccupiedError):
    """The slot holds a typed value (incl. a terminology-term idx); the
    caller attempted to record an intentionally-missing marker. The
    typed row must be deleted before a missing-reason can be written."""


# ---------------------------------------------------------------------------
# Diagnostic helpers (private)
# ---------------------------------------------------------------------------


def _resolve_typed_value_column(data_type: FieldDataType) -> str:
    """Map data_type to the qiita.*_metadata.value_* column holding its typed
    value, via GLOBAL_METADATA_VALUE_COLUMN. Raises NotImplementedError for
    data_types absent from the mapping (BOOLEAN).
    """
    column = GLOBAL_METADATA_VALUE_COLUMN.get(data_type)
    if column is None:
        raise NotImplementedError(
            f"global-metadata write diagnostic for data_type={data_type} is not yet implemented"
        )
    return column


def _compare_slot_occupant(
    value_column: str,
    existing_row: Mapping[str, object],
    attempted_value: GlobalMetadataValue,
) -> Literal["same", "different", "occupied_by_missing", "occupied_by_typed"]:
    """Classify the existing slot occupant vs the attempted write.

    - "same" / "different" — both sides typed (incl. terminology-term-idx
      equality), or both missing-reason; discriminator is value equality
      (scalar equality, terminology-term idx equality, or missing-reason
      idx equality).
    - "occupied_by_missing" — slot holds missing-reason; attempted is typed
      or terminology-term.
    - "occupied_by_typed" — slot holds typed (incl. terminology-term);
      attempted is missing-reason.

    The cross-kind cases trump same/different because no typed value can
    be compared across kinds.
    """
    existing_missing_reason_idx = existing_row[MISSING_REASON_VALUE_COLUMN]
    attempted_is_missing = isinstance(attempted_value, MissingReasonRef)
    # Terminology-term writes carry a Ref, not the raw idx; extract the
    # idx so the typed-vs-typed equality below compares int-to-int.
    attempted_comparable = (
        attempted_value.idx if isinstance(attempted_value, TerminologyTermRef) else attempted_value
    )

    # Existing-missing slot: both sides may agree on missing kind, or the
    # caller attempted a non-missing write against a missing slot.
    if existing_missing_reason_idx is not None:
        if attempted_is_missing:
            return "same" if existing_missing_reason_idx == attempted_value.idx else "different"
        return "occupied_by_missing"

    # Existing-typed slot (incl. terminology-term-idx): a missing-reason
    # attempt cannot compare against a typed value, so it is the symmetric
    # twin of the case above.
    if attempted_is_missing:
        return "occupied_by_typed"

    # Both sides typed (or both terminology-term-idx); equality is
    # well-defined on the comparable scalar.
    return "same" if existing_row[value_column] == attempted_comparable else "different"


async def _fetch_slot_occupant(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    global_field_idx: int | None = None,
    study_field_idx: int | None = None,
) -> Mapping[str, object]:
    """Read the existing row occupying the metadata slot rejected by the
    unique constraint, joined to its source study_field to recover the
    contributing study. Exactly one of global_field_idx / study_field_idx
    must be passed: the non-None one selects the WHERE column (the global
    partial-unique-index path filters by m.global_field_idx; the
    per-field unique-constraint path filters by m.{study_field_idx_column})
    and the slot identifier embedded in any TransientWriteRaceError raised
    when the occupant has been concurrently deleted. Returns all six value
    columns; value_boolean is read ahead of BOOLEAN support being wired
    in coordinated with the value-column map.
    """
    # XOR check: exactly one of the two idx kwargs must be passed.
    if (global_field_idx is None) == (study_field_idx is None):
        raise ValueError("exactly one of global_field_idx / study_field_idx must be passed")

    # The non-None idx selects both the WHERE column and the slot label
    # used in any TransientWriteRaceError raised below.
    if global_field_idx is not None:
        filter_column = "global_field_idx"
        slot_value: int = global_field_idx
    else:
        filter_column = spec.study_field_idx_column
        slot_value = study_field_idx  # type: ignore[assignment]

    # f-string interpolation of identifiers is safe: spec fields are frozen
    # module-level constants and filter_column is a closed in-code choice,
    # never reached by caller input.
    sql = (
        f"SELECT m.idx AS existing_metadata_idx,"
        f" m.value_text, m.value_numeric, m.value_boolean,"
        f" m.value_date, m.value_terminology_term_idx,"
        f" m.{MISSING_REASON_VALUE_COLUMN},"
        f" f.study_idx AS contributing_study_idx"
        f" FROM {spec.metadata_table} m"
        f" JOIN {spec.study_field_table} f"
        f" ON f.idx = m.{spec.study_field_idx_column}"
        f" WHERE m.{spec.entity_key_column} = $1"
        f" AND m.{filter_column} = $2"
    )
    row = await conn.fetchrow(sql, entity_idx, slot_value)
    if row is None:
        # The unique constraint rejected the INSERT, yet the occupant is
        # gone: a concurrent transaction deleted-and-committed it in the
        # window between the savepoint rollback and this read. The slot is
        # free again — a benign lost race, not schema corruption — so signal
        # a retry rather than masquerading it as an invariant violation.
        raise TransientWriteRaceError(
            row_label=f"{spec.entity_kind}_metadata",
            slot_summary=(f"{spec.entity_kind}_idx={entity_idx}, {filter_column}={slot_value}"),
        )
    return row


def _diagnose_slot_occupant(
    data_type: FieldDataType,
    existing_row: Mapping[str, object],
    attempted_value: GlobalMetadataValue,
) -> tuple[
    Literal["same", "different", "occupied_by_missing", "occupied_by_typed"],
    str | Decimal | date | int | None,
    int | None,
]:
    """Resolve the typed value column, classify the slot occupant vs the
    attempted write, and extract the existing typed value (or None when
    the slot holds a missing reason). Returns (compare_result,
    existing_value, existing_missing_reason_idx). For a terminology-typed
    field the existing_value is the int FK to qiita.terminology_term.
    """
    # Resolve the value_* column once; reused for the typed compare and
    # the existing-value extraction below.
    existing_value_column = _resolve_typed_value_column(data_type)
    compare_result = _compare_slot_occupant(existing_value_column, existing_row, attempted_value)

    # Typed slot surfaces the typed column; missing-reason slot has no
    # typed value (None).
    missing_reason_idx = existing_row[MISSING_REASON_VALUE_COLUMN]
    existing_value = None if missing_reason_idx is not None else existing_row[existing_value_column]
    return compare_result, existing_value, missing_reason_idx


def _make_collision_error(
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    display_name: str,
    study_field_idx: int,
    attempted_study_idx: int,
    attempted_value: GlobalMetadataValue,
    data_type: FieldDataType,
    existing_row: Mapping[str, object],
    global_field_idx: int | None = None,
) -> SlotOccupiedError:
    """Pick the right SlotOccupiedError subclass given the diagnostic
    SELECT row and the attempted write's identity. global_field_idx
    discriminates global-path (non-None) vs local-path (None) callers;
    contributing_study_idx is read off existing_row and, on the local
    path, equals attempted_study_idx by construction so the same-study
    leaves are the only reachable subset.

    Caller is expected to `raise` the returned instance; this function
    does not raise itself so the call site stays readable.
    """
    compare_result, existing_value, existing_missing_reason_idx = _diagnose_slot_occupant(
        data_type, existing_row, attempted_value
    )
    contributing_study_idx = existing_row["contributing_study_idx"]
    same_study = contributing_study_idx == attempted_study_idx

    # Common kwargs for whichever subclass fires.
    kwargs = {
        "entity_kind": spec.entity_kind,
        "entity_idx": entity_idx,
        "display_name": display_name,
        "study_field_idx": study_field_idx,
        "attempted_study_idx": attempted_study_idx,
        "contributing_study_idx": contributing_study_idx,
        "attempted_value": attempted_value,
        "data_type": data_type,
        "existing_metadata_idx": existing_row["existing_metadata_idx"],
        "existing_value": existing_value,
        "existing_missing_reason_idx": existing_missing_reason_idx,
        "global_field_idx": global_field_idx,
    }

    # Cross-kind cases trump the same/different axis (no comparable values).
    if compare_result == "occupied_by_missing":
        return SlotOccupiedByMissingReasonError(**kwargs)
    if compare_result == "occupied_by_typed":
        return SlotOccupiedByTypedValueError(**kwargs)
    if same_study and compare_result == "same":
        return DuplicateValueSameStudyError(**kwargs)
    if same_study and compare_result == "different":
        return ConflictingValueSameStudyError(**kwargs)
    if compare_result == "same":
        return DuplicateValueDifferentStudyError(**kwargs)
    return ConflictingValueDifferentStudyError(**kwargs)


# ---------------------------------------------------------------------------
# Globally-linked study-field upsert (private)
# ---------------------------------------------------------------------------


async def _get_or_create_globally_linked_study_field(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    study_idx: int,
    global_field_idx: int,
    display_name: str,
    created_by_idx: int,
    description: str | None = None,
) -> tuple[int, bool]:
    """Find a {entity}_study_field linked to global_field_idx; create on miss.

    Returns (idx, created). created is True when this call inserted the row;
    False on the lookup fallback. A created row populates the global FK
    column and leaves the inheritance columns NULL per the
    *_study_field_inheritance_consistent CHECK.

    Raises StudyFieldConflictError when an existing row at
    (study_idx, display_name) is purely-local or bound to a different
    global field — silently reusing it would attach the value to the wrong
    field. Race-free under READ COMMITTED via INSERT ... ON CONFLICT DO
    NOTHING + fallback SELECT.
    """
    # Both branches must observe the same snapshot; require a wrapping
    # transaction so the INSERT and the fallback SELECT cannot straddle
    # an implicit-commit boundary.
    require_transaction(conn)

    # f-string interpolation of identifiers is safe: spec fields are frozen
    # module-level constants, never reached by caller input.
    fk_column = spec.study_field_global_fk_column

    # Create branch — globally-linked row leaves the inherited columns NULL.
    # ON CONFLICT DO NOTHING absorbs the unique-constraint hit so the
    # concurrent loser of the race does not raise.
    idx = await conn.fetchval(
        f"INSERT INTO {spec.study_field_table} ("
        f"    study_idx, {fk_column},"
        f"    display_name, description, created_by_idx"
        f") VALUES ($1, $2, $3, $4, $5)"
        f" ON CONFLICT (study_idx, display_name) DO NOTHING"
        f" RETURNING idx",
        study_idx,
        global_field_idx,
        display_name,
        description,
        created_by_idx,
    )
    if idx is not None:
        return idx, True

    # Fallback branch — existing row at (study_idx, display_name). Verify
    # its global link matches what the caller asked for; otherwise the row
    # is bound to a different global field (or none) and reusing it would
    # attach the value to the wrong field. The SELECT aliases the global
    # FK column so the Python access stays stable across entities.
    row = await conn.fetchrow(
        f"SELECT idx, {fk_column} AS found_global_field_idx"
        f" FROM {spec.study_field_table}"
        f" WHERE study_idx = $1 AND display_name = $2",
        study_idx,
        display_name,
    )
    if row is None:
        # ON CONFLICT fired against a row that was then deleted-and-
        # committed before this SELECT ran. The slot is free again —
        # benign race, not schema corruption — so signal a retry.
        raise TransientWriteRaceError(
            row_label=f"{spec.entity_kind}_study_field",
            slot_summary=(f"study_idx={study_idx}, display_name={display_name!r}"),
        )
    if row["found_global_field_idx"] != global_field_idx:
        raise StudyFieldConflictError(
            entity_kind=spec.entity_kind,
            study_idx=study_idx,
            display_name=display_name,
            expected_global_field_idx=global_field_idx,
            found_global_field_idx=row["found_global_field_idx"],
        )
    return row["idx"], False


# ---------------------------------------------------------------------------
# Typed metadata insert (private)
# ---------------------------------------------------------------------------


async def _insert_metadata(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    study_field_idx: int,
    data_type: FieldDataType,
    value: GlobalMetadataValue,
    created_by_idx: int,
) -> int:
    """Insert one metadata row into spec.metadata_table and return its idx.

    Populates exactly one value column: the typed value column for a bare
    typed value (via GLOBAL_METADATA_VALUE_COLUMN), value_missing_reason_idx
    for a MissingReasonRef, or value_terminology_term_idx for a
    TerminologyTermRef. global_field_idx is populated by trigger from the
    source field row. BOOLEAN typed values raise NotImplementedError.
    """
    # Dispatch on the value's kind: a resolved Ref (missing-reason or
    # terminology-term) names its own target column and binds its idx;
    # a bare typed value resolves the column via GLOBAL_METADATA_VALUE_COLUMN
    # and binds the value itself.
    if isinstance(value, (MissingReasonRef, TerminologyTermRef)):
        value_column = value.value_column
        bound_value: int | str | Decimal | date = value.idx
    else:
        # Closed-set guard: unsupported data_types raise rather than silently
        # write NULL into every value column.
        resolved_column = GLOBAL_METADATA_VALUE_COLUMN.get(data_type)
        if resolved_column is None:
            raise NotImplementedError(
                f"typed metadata insert for data_type={data_type} is not yet implemented"
            )
        value_column = resolved_column
        bound_value = value

    # f-string interpolation of identifiers is safe: spec fields and
    # GLOBAL_METADATA_VALUE_COLUMN values are frozen module-level
    # constants, never reached by caller input.
    return await conn.fetchval(
        f"INSERT INTO {spec.metadata_table} ("
        f"    {spec.entity_key_column}, {spec.study_field_idx_column},"
        f"    {value_column}, created_by_idx"
        f") VALUES ($1, $2, $3, $4)"
        f" RETURNING idx",
        entity_idx,
        study_field_idx,
        bound_value,
        created_by_idx,
    )


# ---------------------------------------------------------------------------
# write_global_metadata_or_diagnose (public)
# ---------------------------------------------------------------------------


async def write_global_metadata_or_diagnose(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    study_idx: int,
    global_field_idx: int,
    display_name: str,
    data_type: FieldDataType,
    value: GlobalMetadataValue,
    caller_idx: int,
) -> SampleMetadataWriteResult:
    """Write one globally-linked metadata row; on cross-study slot collision,
    diagnose the existing occupant and raise a typed exception.

    Returns SampleMetadataWriteResult on success. The caller owns the outer
    transaction: any study_field row created here rolls back with it on a
    raised exception. UniqueViolations whose constraint_name is NOT
    spec.global_field_unique_index_name propagate unchanged.
    StudyFieldConflictError and TransientWriteRaceError also propagate.
    """
    # Fail-fast: the caller must own the transaction so the typed exception
    # rolls back any study_field row this function created before raising.
    require_transaction(conn)

    # Step 1: resolve the caller's study_field bound to global_field_idx.
    study_field_idx, study_field_created = await _get_or_create_globally_linked_study_field(
        conn,
        spec=spec,
        study_idx=study_idx,
        global_field_idx=global_field_idx,
        display_name=display_name,
        created_by_idx=caller_idx,
    )

    # Step 2: typed INSERT inside a SAVEPOINT. Postgres aborts the whole
    # transaction on any statement error, so without a savepoint the
    # diagnostic SELECT below would fail. Nested conn.transaction() issues
    # SAVEPOINT on enter and ROLLBACK TO SAVEPOINT on exception, leaving
    # the outer transaction alive. Get-or-create above stays outside the
    # savepoint so it rolls back with the caller's outer transaction.
    try:
        async with conn.transaction():
            metadata_idx = await _insert_metadata(
                conn,
                spec=spec,
                entity_idx=entity_idx,
                study_field_idx=study_field_idx,
                data_type=data_type,
                value=value,
                created_by_idx=caller_idx,
            )
        return SampleMetadataWriteResult(
            metadata_idx=metadata_idx,
            study_field_idx=study_field_idx,
            study_field_created=study_field_created,
        )
    except asyncpg.UniqueViolationError as exc:
        # Only the cross-study partial unique index drives the diagnostic
        # path; any other UniqueViolation (e.g., unique_per_field) is the
        # caller's problem and propagates unchanged.
        if exc.constraint_name != spec.global_field_unique_index_name:
            raise

    # Step 3: diagnose the slot occupant and raise the right subclass.
    # Reached only via the controlled path above (UniqueViolation on the
    # partial unique index); the outer transaction is alive because the
    # savepoint rolled back. The raise propagates up and the caller's
    # transaction rolls back.
    existing_row = await _fetch_slot_occupant(
        conn,
        spec=spec,
        entity_idx=entity_idx,
        global_field_idx=global_field_idx,
    )
    raise _make_collision_error(
        spec=spec,
        entity_idx=entity_idx,
        display_name=display_name,
        study_field_idx=study_field_idx,
        attempted_study_idx=study_idx,
        attempted_value=value,
        data_type=data_type,
        existing_row=existing_row,
        global_field_idx=global_field_idx,
    )


# ---------------------------------------------------------------------------
# Local-write strict-mode guard
# ---------------------------------------------------------------------------
#
# LocalWriteOnGloballyLinkedFieldError fires pre-INSERT when the resolved
# study_field turns out to be globally linked — strict-mode refuses to
# write local-typed semantics through a global-typed field.
# ---------------------------------------------------------------------------


class LocalWriteOnGloballyLinkedFieldError(Exception):
    """Raised when a local-only write resolves a {entity_kind}_study_field
    at (study_idx, display_name) that is currently bound to a global field.
    Writing through it would let the value compete in the cross-study
    global slot, which is the opposite of local-only intent.
    """

    def __init__(
        self,
        *,
        entity_kind: SampleEntityKind,
        study_idx: int,
        display_name: str,
        study_field_idx: int,
        found_global_field_idx: int,
    ) -> None:
        self.entity_kind = entity_kind
        self.study_idx = study_idx
        self.display_name = display_name
        self.study_field_idx = study_field_idx
        self.found_global_field_idx = found_global_field_idx
        super().__init__(
            f"{entity_kind}_study_field at study_idx={study_idx},"
            f" display_name={display_name!r} is bound to global field"
            f" {found_global_field_idx}; cannot write a local-only value"
            f" through it"
        )


# ---------------------------------------------------------------------------
# Local study-field upsert (private)
# ---------------------------------------------------------------------------


async def _get_or_create_local_study_field(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    study_idx: int,
    display_name: str,
    created_by_idx: int,
    description: str | None = None,
    data_type: FieldDataType = FieldDataType.TEXT,
    required: bool = False,
    terminology_idx: int | None = None,
    tier_override: Tier | None = None,
) -> tuple[int, bool, int | None]:
    """Find a {entity}_study_field by (study_idx, display_name); create
    purely-local on miss.

    Returns (idx, created, global_field_idx). created is True on the insert
    branch (always purely-local). global_field_idx is None for a purely-local
    row and non-None when the lookup branch resolved an existing row that
    turned out to be globally linked, so callers that require strict
    local-only semantics can reject that resolution instead of silently
    writing through it. Race-free under READ COMMITTED via INSERT ...
    ON CONFLICT DO NOTHING + fallback SELECT.
    """
    # Both branches must observe the same snapshot; require a wrapping
    # transaction so the INSERT and the fallback SELECT cannot straddle
    # an implicit-commit boundary.
    require_transaction(conn)

    # f-string interpolation of identifiers is safe: spec fields are frozen
    # module-level constants, never reached by caller input.
    fk_column = spec.study_field_global_fk_column

    # Create branch — purely-local row, FK column left NULL. ON CONFLICT
    # DO NOTHING absorbs the unique-constraint hit so the concurrent loser
    # of the race does not raise.
    idx = await conn.fetchval(
        f"INSERT INTO {spec.study_field_table} ("
        f"    study_idx, display_name, description,"
        f"    data_type, required, terminology_idx, tier_override,"
        f"    created_by_idx"
        f") VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
        f" ON CONFLICT (study_idx, display_name) DO NOTHING"
        f" RETURNING idx",
        study_idx,
        display_name,
        description,
        data_type,
        required,
        terminology_idx,
        tier_override,
        created_by_idx,
    )
    if idx is not None:
        # Create branch — the row is purely-local by construction.
        return idx, True, None

    # Lookup branch — fallback fires only on conflict; takes a fresh
    # snapshot under READ COMMITTED so it sees the row the concurrent
    # winner committed. Surface the FK column so callers can detect a
    # globally-linked resolution; alias to a stable name so the Python
    # access stays independent of the entity-specific column.
    row = await conn.fetchrow(
        f"SELECT idx, {fk_column} AS found_global_field_idx"
        f" FROM {spec.study_field_table}"
        f" WHERE study_idx = $1 AND display_name = $2",
        study_idx,
        display_name,
    )
    if row is None:
        # ON CONFLICT fired against a row that was then deleted-and-
        # committed before this SELECT ran. The slot is free again —
        # benign race, not schema corruption — so signal a retry.
        raise TransientWriteRaceError(
            row_label=f"{spec.entity_kind}_study_field",
            slot_summary=(f"study_idx={study_idx}, display_name={display_name!r}"),
        )
    return row["idx"], False, row["found_global_field_idx"]


# ---------------------------------------------------------------------------
# write_local_metadata_or_diagnose (public)
# ---------------------------------------------------------------------------


async def write_local_metadata_or_diagnose(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    study_idx: int,
    display_name: str,
    data_type: FieldDataType,
    value: GlobalMetadataValue,
    caller_idx: int,
    required: bool = False,
    terminology_idx: int | None = None,
    tier_override: Tier | None = None,
) -> SampleMetadataWriteResult:
    """Write one local (non-globally-linked) metadata row; on collision,
    diagnose the existing occupant and raise a typed exception.

    Returns SampleMetadataWriteResult on success. The caller owns the
    outer transaction: any study_field row created here rolls back with
    it on a raised exception. required, terminology_idx, and tier_override
    are forwarded to the study_field create branch only. UniqueViolations
    whose constraint_name is NOT spec.local_unique_per_field_index_name
    propagate unchanged. LocalWriteOnGloballyLinkedFieldError and
    TransientWriteRaceError also propagate.
    """
    # Fail-fast: the caller must own the transaction so the typed exception
    # rolls back any study_field row this function created before raising.
    require_transaction(conn)

    # Step 1: get-or-create the local study_field. The third tuple element
    # is the resolved row's global_field_idx; non-None means the row is
    # globally linked, which contradicts the caller's local-only intent
    # and triggers the strict-mode guard.
    (
        study_field_idx,
        study_field_created,
        resolved_global_field_idx,
    ) = await _get_or_create_local_study_field(
        conn,
        spec=spec,
        study_idx=study_idx,
        display_name=display_name,
        created_by_idx=caller_idx,
        data_type=data_type,
        required=required,
        terminology_idx=terminology_idx,
        tier_override=tier_override,
    )
    if resolved_global_field_idx is not None:
        # Strict-mode: the caller asked for local-only, but the resolved
        # row is an existing field that is globally linked.
        # Refuse the write before any metadata INSERT.
        raise LocalWriteOnGloballyLinkedFieldError(
            entity_kind=spec.entity_kind,
            study_idx=study_idx,
            display_name=display_name,
            study_field_idx=study_field_idx,
            found_global_field_idx=resolved_global_field_idx,
        )

    # Step 2: typed INSERT inside a SAVEPOINT. Postgres aborts the whole
    # transaction on any statement error, so without a savepoint the
    # diagnostic SELECT below would fail. Nested conn.transaction()
    # issues SAVEPOINT on enter and ROLLBACK TO SAVEPOINT on exception,
    # leaving the outer transaction alive.
    try:
        async with conn.transaction():
            metadata_idx = await _insert_metadata(
                conn,
                spec=spec,
                entity_idx=entity_idx,
                study_field_idx=study_field_idx,
                data_type=data_type,
                value=value,
                created_by_idx=caller_idx,
            )
        return SampleMetadataWriteResult(
            metadata_idx=metadata_idx,
            study_field_idx=study_field_idx,
            study_field_created=study_field_created,
        )
    except asyncpg.UniqueViolationError as exc:
        # Only the unique-per-field constraint drives the diagnostic path;
        # any other UniqueViolation propagates unchanged.
        if exc.constraint_name != spec.local_unique_per_field_index_name:
            raise

    # Step 3: diagnose the slot occupant and raise the right subclass.
    # Reached only via the controlled path above; the outer transaction
    # is alive because the savepoint rolled back. The raise propagates
    # up and the caller's transaction rolls back.
    existing_row = await _fetch_slot_occupant(
        conn,
        spec=spec,
        entity_idx=entity_idx,
        study_field_idx=study_field_idx,
    )
    # global_field_idx defaults to None: the local-path discriminator
    # the shared dispatcher uses to skip the global-only message bits.
    raise _make_collision_error(
        spec=spec,
        entity_idx=entity_idx,
        display_name=display_name,
        study_field_idx=study_field_idx,
        attempted_study_idx=study_idx,
        attempted_value=value,
        data_type=data_type,
        existing_row=existing_row,
    )


# ---------------------------------------------------------------------------
# Sample-import composer building blocks
# ---------------------------------------------------------------------------


def validate_primary_secondary_studies(
    primary_study_idx: int,
    secondary_study_idxs: Sequence[int],
) -> None:
    """Reject when primary_study_idx also appears in secondary_study_idxs.
    Raises ValueError.
    """
    # Single membership test; secondary_study_idxs is small so the linear
    # scan is cheaper than building a set.
    if primary_study_idx in secondary_study_idxs:
        raise ValueError(
            f"primary_study_idx ({primary_study_idx}) must not appear in secondary_study_idxs"
        )


async def preflight_global_metadata(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    metadata: Mapping[str, str],
    known_missing_reasons: Mapping[str, int] | None = None,
) -> list[tuple[GlobalFieldRow, GlobalMetadataValue]]:
    """Resolve every metadata display_name against spec.global_field_table
    and parse each text value into a typed Python value, a MissingReasonRef
    (if the text matches a known missing-reason name) or a TerminologyTermRef
    (if the text matches a qiita.terminology_term row scoped to the field's
    terminology_idx). Returns (GlobalFieldRow, parsed_value) pairs in input
    order.

    known_missing_reasons maps reason name -> idx; a text value matching a
    key (after outer-whitespace stripping) is emitted as MissingReasonRef
    and skips typed parsing. None or empty disables marker recognition.
    Raises MetadataUnknownFieldsError (carrying every unknown name) before
    parsing, then MetadataParseError on the first typed-parse failure or
    unresolved terminology term.
    """
    # Resolve all requested display_names in one round trip; the helper
    # short-circuits on empty input so this is free for metadata-less callers.
    global_field_rows = await fetch_global_fields_by_display_names(
        conn, spec=spec, display_names=metadata.keys()
    )

    # Collect every unknown name (not first-only) so the caller can surface
    # them all in one 422.
    unknown = [name for name in metadata if name not in global_field_rows]
    if unknown:
        raise MetadataUnknownFieldsError(spec.entity_kind, unknown)

    # Marker lookup is keyed on stripped text so it aligns with
    # parse_text_for_data_type's whitespace handling; an empty mapping
    # disables marker recognition.
    reason_lookup: Mapping[str, int] = known_missing_reasons or {}

    # Group terminology candidates by terminology_idx so we can batch the
    # lookups per terminology, then resolve into a (terminology_idx,
    # term_id) -> (idx, label) map. Missing-reason markers take precedence
    # over the terminology lookup, so a text already matching a known
    # missing-reason name is excluded from the candidate set.
    terminology_candidates: dict[int, set[str]] = {}
    for display_name, text_value in metadata.items():
        global_row = global_field_rows[display_name]
        if global_row.data_type is not FieldDataType.TERMINOLOGY:
            continue
        stripped = text_value.strip()
        if stripped in reason_lookup:
            continue
        # terminology_idx is non-None for TERMINOLOGY-typed rows by the
        # *_global_field CHECK; assert rather than guard so a CHECK violation
        # surfaces loudly instead of silently dropping the row.
        assert global_row.terminology_idx is not None
        terminology_candidates.setdefault(global_row.terminology_idx, set()).add(stripped)

    # One round trip per distinct terminology_idx; the helper short-circuits
    # on empty inputs so a no-terminology import pays nothing.
    terminology_lookup: dict[tuple[int, str], tuple[int, str]] = {}
    for terminology_idx, term_ids in terminology_candidates.items():
        resolved = await fetch_terminology_term_idxs_by_term_ids(
            conn, terminology_idx=terminology_idx, term_ids=term_ids
        )
        for term_id, idx_label in resolved.items():
            terminology_lookup[(terminology_idx, term_id)] = idx_label

    # Parse each text value: missing-reason markers route to MissingReasonRef
    # first; TERMINOLOGY-typed fields then route to TerminologyTermRef on hit
    # or raise MetadataParseError on miss; other values dispatch to the
    # typed parser.
    parsed: list[tuple[GlobalFieldRow, GlobalMetadataValue]] = []
    for display_name, text_value in metadata.items():
        global_row = global_field_rows[display_name]
        stripped = text_value.strip()
        if stripped in reason_lookup:
            parsed.append(
                (global_row, MissingReasonRef(idx=reason_lookup[stripped], name=stripped))
            )
            continue
        if global_row.data_type is FieldDataType.TERMINOLOGY:
            # terminology_idx is non-None for TERMINOLOGY-typed rows by the
            # *_global_field CHECK; assert rather than guard so a CHECK violation
            # surfaces loudly instead of silently dropping the row.
            assert global_row.terminology_idx is not None
            resolved_idx_label = terminology_lookup.get((global_row.terminology_idx, stripped))
            if resolved_idx_label is None:
                raise MetadataParseError(
                    display_name=display_name,
                    data_type=global_row.data_type,
                    text_value=text_value,
                    reason="no matching terminology term",
                )
            term_idx, term_label = resolved_idx_label
            parsed.append(
                (
                    global_row,
                    TerminologyTermRef(idx=term_idx, term_id=stripped, label=term_label),
                )
            )
            continue
        parsed_value = parse_text_for_data_type(display_name, global_row.data_type, text_value)
        parsed.append((global_row, parsed_value))
    return parsed


async def insert_entity_to_study(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    study_idx: int,
    created_by_idx: int,
) -> None:
    """Insert one (entity, study) link row into spec.link_table.

    Retirement columns are CHECK-pinned to NULL/false on a fresh row, so
    they have no place in a create call; created_at defaults to now().
    Prep-sample inserts may be rejected (asyncpg.RaiseError) if the
    underlying biosample is not linked to the same study.

    Raises asyncpg.UniqueViolationError if (entity_idx, study_idx) already
    exists, asyncpg.ForeignKeyViolationError on bad refs.
    """
    # f-string interpolation of identifiers is safe: spec fields are frozen
    # module-level constants, never reached by caller input.
    await conn.execute(
        f"INSERT INTO {spec.link_table} ("
        f"    {spec.link_entity_key_column}, study_idx, created_by_idx"
        f") VALUES ($1, $2, $3)",
        entity_idx,
        study_idx,
        created_by_idx,
    )


async def link_entity_to_studies(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    primary_study_idx: int,
    secondary_study_idxs: Sequence[int],
    caller_idx: int,
) -> None:
    """Link entity_idx to primary_study_idx plus every entry in
    secondary_study_idxs.

    Deduplicates secondary_study_idxs before iterating so a caller that
    passes a repeated study idx does not trip the link table's primary
    key. Primary first so its link row carries the smallest created_at
    ordering; secondaries sorted ascending so a failing study idx is
    reproducible if any per-row trigger fires.
    """
    unique_secondaries = list(dict.fromkeys(secondary_study_idxs))

    # Primary first; sorted secondaries after, for deterministic ordering.
    for study_idx in [primary_study_idx, *sorted(unique_secondaries)]:
        await insert_entity_to_study(
            conn,
            spec=spec,
            entity_idx=entity_idx,
            study_idx=study_idx,
            created_by_idx=caller_idx,
        )


async def write_global_metadata_entries(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    study_idx: int,
    caller_idx: int,
    parsed_metadata: Sequence[tuple[GlobalFieldRow, GlobalMetadataValue]],
) -> None:
    """Drive write_global_metadata_or_diagnose over every preflight-parsed
    entry, writing each value against study_idx (the field-owning study).

    The first collision or rollback signal write_global_metadata_or_diagnose
    raises propagates; subsequent entries are not attempted because the
    caller's outer transaction is the right place to roll partial state back.
    """
    # One write per entry, sequentially; the outer transaction is the
    # atomicity boundary.
    for global_row, parsed_value in parsed_metadata:
        await write_global_metadata_or_diagnose(
            conn,
            spec=spec,
            entity_idx=entity_idx,
            study_idx=study_idx,
            global_field_idx=global_row.idx,
            display_name=global_row.display_name,
            data_type=global_row.data_type,
            value=parsed_value,
            caller_idx=caller_idx,
        )
