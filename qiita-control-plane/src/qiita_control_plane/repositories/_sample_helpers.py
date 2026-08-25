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
    FieldWriteOutcome,
    MetadataFieldScope,
    MissingReasonRef,
    SampleMetadataValue,
    TerminologyTermRef,
    Tier,
    derive_metadata_field_scope,
)

from . import require_transaction

# Whether a colliding metadata write overwrites the existing value or raises.
type MetadataConflictMode = Literal["raise", "upsert"]

# How the existing occupant of a metadata slot relates to an attempted write:
# the two same-kind verdicts, then the two cross-kind ones (a typed value
# attempted against a missing-reason slot and the reverse), which are not
# comparable and so carry no same/different answer.
type SlotCompareResult = Literal["same", "different", "occupied_by_missing", "occupied_by_typed"]

# The only two texts a BOOLEAN-typed metadata value accepts, compared after
# outer-whitespace stripping and case folding. Anything else is a parse error:
# the stored value is a real boolean, so admitting a wider vocabulary would
# rewrite the caller's token into one of these two on read.
BOOLEAN_TRUE_TEXT = "true"
BOOLEAN_FALSE_TEXT = "false"

# The widest unconstrained NUMERIC Postgres stores: past either bound it
# rejects the value as overflowing the numeric format, so a value beyond them
# has nowhere to land.
MAX_PG_NUMERIC_INTEGER_DIGITS = 131072
MAX_PG_NUMERIC_SCALE_DIGITS = 16383

# Maps each data_type to the qiita.*_metadata.value_* column carrying its typed
# value. Every member of FieldDataType must appear: a data_type absent here has
# no column to read or write, so it reaches the closed-set guards as a
# NotImplementedError instead of silently writing or reading NULL.
GLOBAL_METADATA_VALUE_COLUMN: dict[FieldDataType, str] = {
    FieldDataType.TEXT: "value_text",
    FieldDataType.NUMERIC: "value_numeric",
    FieldDataType.DATE: "value_date",
    FieldDataType.BOOLEAN: "value_boolean",
    FieldDataType.TERMINOLOGY: TERMINOLOGY_TERM_VALUE_COLUMN,
}

# SELECT fragment naming every value_* column a metadata row may populate,
# aliased to `m`. Derived from the column map so a data_type added there reaches
# every read that decodes a row; a hand-listed copy is how a column goes
# missing from one read and not another.
METADATA_VALUE_COLUMNS_SELECT = ", ".join(
    f"m.{column}"
    for column in (*GLOBAL_METADATA_VALUE_COLUMN.values(), MISSING_REASON_VALUE_COLUMN)
)

# The display payload for a row whose value is a missing-reason or a terminology
# term (the reason name; the term's term_id, label, and alternate label), and the
# two LEFT JOINs supplying it. Split because the two halves sit in different
# clauses of the statement, and neither works without the other.
METADATA_REF_PAYLOAD_SELECT = (
    "mvr.name AS missing_reason_name,"
    " tt.term_id AS terminology_term_id,"
    " tt.label AS terminology_term_label,"
    " tt.alternate_label AS terminology_term_alternate_label"
)
METADATA_REF_JOIN_CLAUSES = (
    f" LEFT JOIN qiita.missing_value_reason mvr"
    f"   ON mvr.idx = m.{MISSING_REASON_VALUE_COLUMN}"
    f" LEFT JOIN qiita.terminology_term tt"
    f"   ON tt.idx = m.{TERMINOLOGY_TERM_VALUE_COLUMN}"
)


class SampleEntityKind(StrEnum):
    """Discriminator for the entity domain a sample-family operation targets.
    Values match the table-name prefix (biosample_*, prep_sample_*) used
    throughout the schema.
    """

    BIOSAMPLE = "biosample"
    PREP_SAMPLE = "prep_sample"


def _study_field_location(study_idx: int, display_name: str) -> str:
    """Render the (study_idx, display_name) pair that uniquely keys one
    *_study_field row, for embedding in a diagnostic message.
    """
    return f"study_idx={study_idx}, display_name={display_name!r}"


def _assert_global_link_consistent(global_field_idx: int | None, internal_name: str | None) -> None:
    """Reject a field row whose two global-linkage markers disagree.

    A globally-linked field carries both the FK to its global field and that
    field's internal_name; a purely-local field carries neither. One populated
    without the other means the row was assembled from mismatched sources, and
    every scope derived from it downstream would be arbitrary.
    """
    if (global_field_idx is None) != (internal_name is None):
        raise ValueError(
            "global_field_idx and internal_name must both be set or both be None;"
            f" got global_field_idx={global_field_idx!r}, internal_name={internal_name!r}"
        )


@dataclass(frozen=True, kw_only=True)
class FieldRow:
    """Subset of field columns used by metadata pre-flight reads, for both a
    *_global_field row and a study-local *_study_field row.

    data_type is the field's effective type (a globally-linked study-local row
    inherits it from the global field). terminology_idx is non-None iff
    data_type is TERMINOLOGY and scopes any term-id lookup. global_field_idx is
    the global field this row represents or links to: its own idx for a
    *_global_field row, the FK for a *_study_field row (None on a purely-local
    field). internal_name is that global field's stable identifier — its own for
    a *_global_field row, the linked global's for a globally-linked *_study_field
    row, and None on a purely-local field.
    """

    idx: int
    display_name: str
    data_type: FieldDataType
    terminology_idx: int | None
    # Nullness decides globally-linked vs purely-local. Never test that with
    # idx == global_field_idx: the two tables number independently, so a study
    # field can equal its own FK by coincidence.
    global_field_idx: int | None
    internal_name: str | None

    def __post_init__(self) -> None:
        _assert_global_link_consistent(self.global_field_idx, self.internal_name)


@dataclass(frozen=True, kw_only=True)
class ResolvedField:
    """One metadata column resolved to the field it will be written against.

    caller_key is the key the caller keyed the column on: a display_name, or a
    global field's internal_name under global-internal-name resolution. scope is
    the write path — "global" writes through global_field_idx, "local" against
    study_field_idx. study_field_idx is the existing study-local row when one was
    matched, None on a direct global match (the row is minted at write under
    canonical_display). canonical_display is that study-local field's
    display_name, equal to caller_key except when a global was keyed by
    internal_name. internal_name is the resolved global field's stable
    identifier, None for a purely-local field, and is the key a globally-linked
    value reads back under. data_type / terminology_idx are the field's
    effective type.
    """

    caller_key: str
    global_field_idx: int | None
    study_field_idx: int | None
    canonical_display: str
    data_type: FieldDataType
    terminology_idx: int | None
    internal_name: str | None

    def __post_init__(self) -> None:
        _assert_global_link_consistent(self.global_field_idx, self.internal_name)

    @property
    def scope(self) -> MetadataFieldScope:
        """Which write path this column takes. Derived from global_field_idx —
        the value the global path writes through — so it can't disagree with it."""
        return derive_metadata_field_scope(self.global_field_idx)


# One resolved metadata column paired with the parsed value to write into it.
type ResolvedFieldValue = tuple[ResolvedField, SampleMetadataValue]


@dataclass(frozen=True)
class MetadataRow:
    """One row of resolved metadata for a biosample or prep_sample: the
    cosmetic display_name and description, its data_type, and the value
    extracted from the row — either the typed Python value from the matching
    value_* column, a MissingReasonRef carrying an intentionally-missing
    reason's idx + name, or a TerminologyTermRef carrying the resolved
    terminology term.
    """

    display_name: str
    description: str | None
    data_type: FieldDataType
    value: SampleMetadataValue


@dataclass(frozen=True)
class GlobalMetadataRow(MetadataRow):
    """A globally-linked MetadataRow that also carries the field's stable
    internal_name from the *_global_field row. display_name and description
    are the canonical global-field values, not study-scoped. internal_name is
    retained on the row (not only as the dict key) so a value separated from
    its keyed dict still identifies its field.
    """

    internal_name: str


# ---------------------------------------------------------------------------
# Shared metadata parse error and text-to-typed coercion
# ---------------------------------------------------------------------------


class MetadataUnknownFieldsError(Exception):
    """Raised when metadata input names field keys with no matching
    {entity_kind} field. Carries every unknown key in one list so the full set
    can be surfaced together. A key is whatever the caller keyed the column on
    (a display_name, or a global field's internal_name under internal-name
    resolution), so this stays namespace-neutral.
    """

    def __init__(self, entity_kind: SampleEntityKind, field_keys: list[str]) -> None:
        self.field_keys = field_keys
        super().__init__(f"unknown {entity_kind} metadata field keys: {field_keys!r}")


class MetadataMissingRequiredFieldsError(Exception):
    """Raised when an import omits a global field marked `required`.

    Carries every missing display_name in one list, so the caller fixes the whole
    set at once rather than discovering them one 422 at a time.
    """

    def __init__(self, entity_kind: SampleEntityKind, missing_display_names: list[str]) -> None:
        self.missing_display_names = missing_display_names
        super().__init__(
            f"missing required {entity_kind} global field(s): {missing_display_names!r}"
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
            f"{entity_kind}_study_field at {_study_field_location(study_idx, display_name)}"
            f" is bound to global {found_global_field_idx!r},"
            f" expected {expected_global_field_idx!r}"
        )


class StudyFieldAlreadyExistsError(Exception):
    """A create collided with a {entity_kind}_study_field row already present
    at (study_idx, display_name); the create does not reuse that row, so it
    reports the name as taken rather than minting a duplicate.

    A globally-linked create instead raises StudyFieldConflictError when the
    colliding row is bound to a *different* global field than the create requested
    (or is purely-local) since reusing that one would attach the value to the wrong field.
    """

    def __init__(
        self,
        entity_kind: SampleEntityKind,
        study_idx: int,
        display_name: str,
    ) -> None:
        self.entity_kind = entity_kind
        self.study_idx = study_idx
        self.display_name = display_name
        super().__init__(
            f"{entity_kind}_study_field at {_study_field_location(study_idx, display_name)}"
            f" already exists"
        )


class DuplicateGlobalFieldTargetError(Exception):
    """Raised when two or more input metadata columns resolve to the same
    global field — directly, or through a study-local alias. One entity's
    value for a single global field cannot come from multiple columns. Carries
    the shared global_field_idx and the colliding field keys (each whatever the
    caller keyed that column on).
    """

    def __init__(
        self,
        entity_kind: SampleEntityKind,
        global_field_idx: int,
        field_keys: list[str],
    ) -> None:
        self.entity_kind = entity_kind
        self.global_field_idx = global_field_idx
        self.field_keys = field_keys
        super().__init__(
            f"multiple {entity_kind} columns target global field {global_field_idx}: {field_keys!r}"
        )


class MetadataParseError(Exception):
    """Raised when a metadata text value cannot be coerced into the Python
    type matching its field's data_type. Carries the failing field key
    (whatever the caller keyed the column on) plus the raw inputs for
    field-scoped diagnostics.
    """

    def __init__(
        self,
        field_key: str,
        data_type: FieldDataType,
        text_value: str,
        reason: str,
    ) -> None:
        self.field_key = field_key
        self.data_type = data_type
        self.text_value = text_value
        self.reason = reason
        super().__init__(
            f"could not parse {field_key!r} value {text_value!r} as {data_type}: {reason}"
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


def _fits_pg_numeric(value: Decimal) -> bool:
    """Report whether a Decimal falls within the digit bounds Postgres stores.

    Callers must clear a value through here before rendering it: the rendered
    form carries one character per digit, and the exponent setting that count
    arrives as caller text, so a value of a few bytes can name a width of
    gigabytes.
    """
    # Infinity and every NaN flavour render to a fixed token, and carry a
    # string exponent that no digit bound can be compared against.
    if not value.is_finite():
        return True
    exponent = value.as_tuple().exponent
    integer_digits = value.adjusted() + 1
    scale_digits = -exponent if exponent < 0 else 0
    return (
        integer_digits <= MAX_PG_NUMERIC_INTEGER_DIGITS
        and scale_digits <= MAX_PG_NUMERIC_SCALE_DIGITS
    )


def _pg_numeric_text(value: Decimal) -> str:
    """Render a Decimal in the exact text form an unconstrained Postgres
    NUMERIC column stores it as. The value must already have cleared
    _fits_pg_numeric, which bounds how wide this render can run.

    Postgres expands exponent notation into plain digits and keeps whatever
    scale those digits carry, so fixed-point formatting matches it — except
    that every NaN flavour collapses to one, and a negative zero loses its
    sign while keeping its scale.
    """
    # Quiet, signalling, and signed NaN all store as the one unsigned form.
    if value.is_nan():
        return "NaN"
    text = format(value, "f")
    # is_zero rather than == 0: a signalling NaN raises on comparison.
    if value.is_signed() and value.is_zero():
        return text.lstrip("-")
    return text


def parse_text_for_data_type(
    field_key: str,
    data_type: FieldDataType,
    text_value: str,
) -> str | Decimal | date | bool:
    """Coerce a text input into the Python type matching data_type.

    Outer whitespace is stripped before parsing. TEXT returns the stripped
    string; NUMERIC returns a Decimal in the representation Postgres stores,
    so the value written, compared, and reported back are one and the same
    form; DATE returns datetime.date; BOOLEAN accepts only the two boolean
    texts, case-insensitively. TERMINOLOGY has no text coercion — its value is
    a terminology-term lookup — and raises NotImplementedError. Conversion
    failures raise MetadataParseError carrying field_key, data_type, raw text,
    and reason.
    """
    # Normalize once; all parse arms see the stripped value.
    stripped = text_value.strip()
    if data_type is FieldDataType.TEXT:
        return stripped
    if data_type is FieldDataType.NUMERIC:
        try:
            parsed_decimal = Decimal(stripped)
        except InvalidOperation as exc:
            raise MetadataParseError(
                field_key=field_key,
                data_type=data_type,
                text_value=text_value,
                reason="not a valid decimal number",
            ) from exc
        # Turn away what the database has no room for before rendering it,
        # since rendering is what pays for every digit the exponent names.
        if not _fits_pg_numeric(parsed_decimal):
            raise MetadataParseError(
                field_key=field_key,
                data_type=data_type,
                text_value=text_value,
                reason="more digits than a numeric column can store",
            )
        # Adopt the stored representation here, so no later step has to know
        # which of the two forms it is holding.
        stored_form = _pg_numeric_text(parsed_decimal)
        return Decimal(stored_form)
    if data_type is FieldDataType.DATE:
        try:
            return date.fromisoformat(stripped)
        except ValueError as exc:
            raise MetadataParseError(
                field_key=field_key,
                data_type=data_type,
                text_value=text_value,
                reason="not a valid ISO date (YYYY-MM-DD)",
            ) from exc
    if data_type is FieldDataType.BOOLEAN:
        folded = stripped.casefold()
        if folded == BOOLEAN_TRUE_TEXT:
            return True
        if folded == BOOLEAN_FALSE_TEXT:
            return False
        raise MetadataParseError(
            field_key=field_key,
            data_type=data_type,
            text_value=text_value,
            reason=f"not {BOOLEAN_TRUE_TEXT!r} or {BOOLEAN_FALSE_TEXT!r} (case-insensitive)",
        )
    # Closed-set fallback: no arm above matched. TERMINOLOGY is the one member
    # that never will — its value is a term lookup, not a coercion — and a
    # member added without an arm lands here too, loudly.
    raise NotImplementedError(f"no text coercion for data_type={data_type}")


# ---------------------------------------------------------------------------
# Cross-entity metadata-write dispatch: spec + shared read/write helpers
# ---------------------------------------------------------------------------


class SampleMetadataWriteResult(NamedTuple):
    """Successful sample-family metadata write: the metadata row's idx, the
    study_field idx the value was attached to, whether that study_field row
    was created (versus reused from a get-or-create lookup), and what the
    write did to the value slot (inserted a new row, overwrote an existing
    one, or left an already-identical value untouched). On an UNCHANGED
    upsert metadata_idx is the pre-existing row's idx and nothing was written.
    """

    metadata_idx: int
    study_field_idx: int
    study_field_created: bool
    outcome: FieldWriteOutcome


class SampleMetadataFieldResult(NamedTuple):
    """Per-field outcome of one sample-metadata write: the key the caller
    keyed the value on (its display_name, or a global's internal_name under
    global-internal-name resolution), whether it resolved to a globally-linked
    or a purely-local field, what the write did to the slot, and the value that
    now occupies it. Emitted one per input field in the caller's input order.

    internal_name is the resolved global field's stable identifier, None for a
    purely-local field. It is the key a globally-linked value reads back under,
    which differs from field_key unless the caller keyed on it.
    """

    field_key: str
    outcome: FieldWriteOutcome
    value: SampleMetadataValue
    internal_name: str | None

    @property
    def scope(self) -> MetadataFieldScope:
        """Whether the value landed in a globally-linked or a purely-local slot.
        Derived from internal_name, the only global-linkage marker this type
        carries, so it can't disagree with it."""
        return derive_metadata_field_scope(self.internal_name)


@dataclass(frozen=True)
class EntityMetadataSpec:
    """Per-entity SQL-identifier binding for the sample-family helpers.

    Holds the entity discriminator plus the table, column, and constraint
    identifiers that differ between the biosample and prep_sample stacks,
    so the shared helpers stay agnostic. Bindings cover the metadata table,
    the global-field and study-field tables, the constraint names the
    diagnostic paths key on, the per-study link table, and — for an entity
    that has one — the boolean column flagging an owner-sample-id row.
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
    # Name of the DB function the metadata table's retired-link triggers run.
    # It tags its error DETAIL with `trigger=<this value>`, which is how a
    # caller tells that rejection from any other trigger sharing its SQLSTATE.
    # Renaming the function in a migration means changing this in lockstep.
    metadata_retired_link_trigger: str
    # The boolean *_metadata column marking an owner-sample-id row, for
    # entities that carry one (is_owner_biosample_id on biosample); None when
    # the entity has no owner-sample-id concept. When set, generic metadata
    # writes refuse to target a field holding such a row.
    owner_sample_id_flag_column: str | None = None


# SQL identifiers are interpolated into the queries below only from frozen
# constants and closed in-code mappings, never from caller input; values always
# bind as $N placeholders.

# The name a study-field read aliases its entity-specific global-FK column to,
# so the Python access is the same whichever entity's table was queried. Shared
# by the SELECT that emits it and every read that consumes it: a rename that
# reached only one side would fail at runtime with a missing record key.
FOUND_GLOBAL_FIELD_IDX_ALIAS = "found_global_field_idx"

# The columns a global-field lookup may key on. A closed mapping from the
# public Literal to its SQL column name, so the interpolated identifier is
# never reached by caller input (the value is unbindable as a placeholder).
type FieldKeyColumn = Literal["display_name", "internal_name"]
_GLOBAL_FIELD_KEY_COLUMN: dict[FieldKeyColumn, str] = {
    "display_name": "display_name",
    "internal_name": "internal_name",
}


def _field_rows_by_key(
    rows: list[asyncpg.Record], *, key_column: str = "display_name"
) -> dict[str, FieldRow]:
    """Key field-lookup rows by the named column, wrapping each in FieldRow.
    Each row must carry key_column plus idx, display_name, data_type,
    terminology_idx, global_field_idx, and internal_name columns.
    """
    return {
        r[key_column]: FieldRow(
            idx=r["idx"],
            display_name=r["display_name"],
            data_type=FieldDataType(r["data_type"]),
            terminology_idx=r["terminology_idx"],
            global_field_idx=r["global_field_idx"],
            internal_name=r["internal_name"],
        )
        for r in rows
    }


async def fetch_global_fields_by_keys(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    keys: Iterable[str],
    key_column: FieldKeyColumn = "display_name",
) -> dict[str, FieldRow]:
    """Return a dict of key -> FieldRow for the matching rows in the
    *_global_field table named by spec.global_field_table.

    key_column selects which column the keys match against — display_name
    (default) or the machine-facing internal_name; the returned dict is keyed
    on that same column while FieldRow.display_name always carries the row's
    canonical display_name. Keys with no matching row are absent from the
    returned dict; callers detect "unknown field" by checking dict membership.
    Empty input short-circuits with no DB call.
    """
    # Materialize so emptiness is detectable and the param can be passed as ANY.
    lookup_keys = list(keys)
    if not lookup_keys:
        return {}

    # A global field is its own reference, so idx doubles as global_field_idx;
    # internal_name is selected so it can serve as the dict key.
    key_sql_column = _GLOBAL_FIELD_KEY_COLUMN[key_column]
    rows = await pool_or_conn.fetch(
        f"SELECT idx, internal_name, display_name, data_type, terminology_idx,"
        f" idx AS global_field_idx"
        f" FROM {spec.global_field_table}"
        f" WHERE {key_sql_column} = ANY($1::text[])",
        lookup_keys,
    )
    return _field_rows_by_key(rows, key_column=key_sql_column)


async def fetch_study_fields_by_display_names(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    study_idx: int,
    display_names: Iterable[str],
) -> dict[str, FieldRow]:
    """Return a dict of display_name -> FieldRow for the study's *_study_field
    rows matching the given display_names.

    Scoped to study_idx (the *_study_field unique key is
    (study_idx, display_name)). data_type / terminology_idx are the effective
    values: a globally-linked row stores them NULL and inherits from its global
    field, so the query COALESCEs against *_global_field. global_field_idx is
    the row's FK, None on a purely-local field, and internal_name comes from the
    linked global field, None for the same reason. Names with no matching row are
    absent from the returned dict; empty input short-circuits with no DB call.
    """
    # Materialize so emptiness is detectable and the param can be passed as ANY.
    names = list(display_names)
    if not names:
        return {}

    # The LEFT JOIN resolves the inherited data_type / terminology_idx for
    # globally-linked rows, which store those columns NULL, and carries the
    # linked global's internal_name (NULL for a purely-local row).
    fk_column = spec.study_field_global_fk_column
    rows = await pool_or_conn.fetch(
        f"SELECT sf.idx, sf.display_name,"
        f" COALESCE(sf.data_type, gf.data_type) AS data_type,"
        f" COALESCE(sf.terminology_idx, gf.terminology_idx) AS terminology_idx,"
        f" sf.{fk_column} AS global_field_idx, gf.internal_name"
        f" FROM {spec.study_field_table} sf"
        f" LEFT JOIN {spec.global_field_table} gf ON gf.idx = sf.{fk_column}"
        f" WHERE sf.study_idx = $1 AND sf.display_name = ANY($2::text[])",
        study_idx,
        names,
    )
    return _field_rows_by_key(rows, key_column="display_name")


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
) -> dict[str, tuple[int, str, str | None]]:
    """Return a dict of term_id -> (idx, label, alternate_label) for every
    qiita.terminology_term row whose term_id appears in `term_ids` AND whose
    terminology_idx matches. alternate_label is None for a term the source
    supplies no second name for.

    Term ids absent from the table are absent from the returned dict. Empty
    input short-circuits with no DB call. No is_obsolete filter — any row
    scoped to this terminology counts as a valid marker; nothing exercises the
    obsoletion lifecycle yet. Scoped to one terminology_idx because
    (terminology_idx, term_id) is the table's unique key — the same term_id can
    recur across different terminologies.
    """
    # Materialize so emptiness is detectable and the param can be passed as ANY.
    candidate_term_ids = list(term_ids)
    if not candidate_term_ids:
        return {}

    # Single batch SELECT keyed on term_id and scoped to terminology_idx.
    # The (terminology_idx, term_id) UNIQUE constraint bounds the row count
    # by len(candidate_term_ids).
    rows = await pool_or_conn.fetch(
        "SELECT idx, term_id, label, alternate_label FROM qiita.terminology_term"
        " WHERE terminology_idx = $1 AND term_id = ANY($2::text[])",
        terminology_idx,
        candidate_term_ids,
    )
    return {r["term_id"]: (r["idx"], r["label"], r["alternate_label"]) for r in rows}


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


def _decode_metadata_value(
    row: Mapping[str, object], data_type: FieldDataType, *, read_label: str
) -> SampleMetadataValue:
    """Decode one metadata row's value-column set into a SampleMetadataValue.

    A populated value_missing_reason_idx yields a MissingReasonRef and a
    populated value_terminology_term_idx a TerminologyTermRef, both superseding
    data_type-driven decoding; otherwise the value_* column the data_type names
    is read. A data_type absent from GLOBAL_METADATA_VALUE_COLUMN raises
    NotImplementedError (labelled by read_label) so it cannot silently surface
    a NULL value. The row must carry the METADATA_VALUE_COLUMNS_SELECT columns
    plus the METADATA_REF_PAYLOAD_SELECT aliases.
    """
    # Ref kinds take precedence over data_type; the typed branch is reached
    # only when neither Ref column is populated.
    missing_reason_idx = row[MISSING_REASON_VALUE_COLUMN]
    if missing_reason_idx is not None:
        return MissingReasonRef(idx=missing_reason_idx, name=row["missing_reason_name"])
    terminology_term_idx = row[TERMINOLOGY_TERM_VALUE_COLUMN]
    if terminology_term_idx is not None:
        return TerminologyTermRef(
            idx=terminology_term_idx,
            term_id=row["terminology_term_id"],
            label=row["terminology_term_label"],
            alternate_label=row["terminology_term_alternate_label"],
        )
    column = GLOBAL_METADATA_VALUE_COLUMN.get(data_type)
    if column is None:
        raise NotImplementedError(f"{read_label} for data_type={data_type} is not yet implemented")
    return row[column]


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
    data_type-driven decoding. Any other row is decoded from the value
    column its data_type maps to. Not study-scoped: the canonical global
    value persists across link retirement.
    """
    # f-string interpolation of the table identifiers is safe: all
    # (including spec fields) are frozen constants, never reached by caller input.
    # LEFT JOINs on qiita.missing_value_reason and qiita.terminology_term so a
    # missing-reason or terminology-term row's display payload comes back in one
    # round trip; rows carrying a scalar value have both join keys NULL.
    rows = await pool_or_conn.fetch(
        f"SELECT gf.internal_name, gf.display_name, gf.description, gf.data_type,"
        f" {METADATA_VALUE_COLUMNS_SELECT}, {METADATA_REF_PAYLOAD_SELECT}"
        f" FROM {spec.metadata_table} m"
        f" JOIN {spec.global_field_table} gf ON gf.idx = m.global_field_idx"
        f"{METADATA_REF_JOIN_CLAUSES}"
        f" WHERE m.{spec.entity_key_column} = $1"
        f"   AND m.global_field_idx IS NOT NULL",
        entity_idx,
    )

    # One GlobalMetadataRow per row, keyed by internal_name; the shared decoder
    # resolves the value column (Ref kinds supersede data_type).
    result: dict[str, GlobalMetadataRow] = {}
    for r in rows:
        data_type = FieldDataType(r["data_type"])
        value = _decode_metadata_value(r, data_type, read_label="global metadata read")
        result[r["internal_name"]] = GlobalMetadataRow(
            internal_name=r["internal_name"],
            display_name=r["display_name"],
            description=r["description"],
            data_type=data_type,
            value=value,
        )
    return result


async def fetch_local_metadata(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    study_idx: int,
) -> dict[str, MetadataRow]:
    """Return display_name -> MetadataRow for every purely-local metadata value
    the entity carries on study_idx.

    Filters on global_field_idx IS NULL (globally-linked rows are excluded) and
    on the value's study_field belonging to study_idx, so the read is scoped to
    one study's local fields; display_name, description, and data_type come from
    that study_field (a purely-local field owns its own type). Intentionally-
    missing and terminology-term entries surface as MissingReasonRef /
    TerminologyTermRef; any other row is decoded from the value column its
    data_type maps to. An owner-sample-id row (where the spec has one) is a
    study-local value like any other and is included; this read applies no
    visibility gating of its own.
    """
    # The study_field join scopes to one study and supplies the field's
    # display_name + data_type; the LEFT JOINs recover a missing-reason or
    # terminology-term row's display payload in one round trip.
    # m.global_field_idx IS NULL selects only purely-local values.
    rows = await pool_or_conn.fetch(
        f"SELECT sf.display_name, sf.description, sf.data_type,"
        f" {METADATA_VALUE_COLUMNS_SELECT}, {METADATA_REF_PAYLOAD_SELECT}"
        f" FROM {spec.metadata_table} m"
        f" JOIN {spec.study_field_table} sf ON sf.idx = m.{spec.study_field_idx_column}"
        f"{METADATA_REF_JOIN_CLAUSES}"
        f" WHERE m.{spec.entity_key_column} = $1"
        f"   AND m.global_field_idx IS NULL"
        f"   AND sf.study_idx = $2",
        entity_idx,
        study_idx,
    )

    # One MetadataRow per row, keyed by display_name; the shared decoder
    # resolves the value column (Ref kinds supersede data_type).
    result: dict[str, MetadataRow] = {}
    for r in rows:
        data_type = FieldDataType(r["data_type"])
        value = _decode_metadata_value(r, data_type, read_label="local metadata read")
        result[r["display_name"]] = MetadataRow(
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
        attempted_value: SampleMetadataValue,
        data_type: FieldDataType,
        existing_metadata_idx: int,
        # int arm is the terminology_term.idx FK (read from the typed value
        # column of a TERMINOLOGY-typed row); the scalar arms cover
        # str/Decimal/date/bool.
        existing_value: str | Decimal | date | bool | int | None,
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
    value, via GLOBAL_METADATA_VALUE_COLUMN. Raises NotImplementedError for a
    data_type absent from the mapping, which has no column to read or write.
    """
    column = GLOBAL_METADATA_VALUE_COLUMN.get(data_type)
    if column is None:
        raise NotImplementedError(
            f"metadata value column for data_type={data_type} is not yet implemented"
        )
    return column


def _resolve_value_column_and_bind(
    value: SampleMetadataValue, data_type: FieldDataType
) -> tuple[str, int | str | Decimal | date | bool]:
    """Resolve the value_* column and the parameter to bind for one metadata
    value. A resolved Ref (missing-reason or terminology-term) names its own
    target column and binds its idx; a bare typed value takes the column its
    data_type maps to and binds the value itself. A data_type absent from the
    map raises NotImplementedError via _resolve_typed_value_column.
    """
    if isinstance(value, (MissingReasonRef, TerminologyTermRef)):
        return value.value_column, value.idx
    return _resolve_typed_value_column(data_type), value


def _compare_slot_occupant(
    value_column: str,
    existing_row: Mapping[str, object],
    attempted_value: SampleMetadataValue,
) -> SlotCompareResult:
    """Classify the existing slot occupant vs the attempted write.

    - "same" / "different" — both sides typed (incl. terminology-term-idx
      equality), or both missing-reason; discriminator is value equality
      (scalar equality, terminology-term idx equality, or missing-reason
      idx equality). A NUMERIC slot is the exception: it discriminates on
      the stored representation, so a rewrite differing only in scale is
      "different".
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

    # Both sides typed (or both terminology-term-idx). A NUMERIC slot
    # discriminates on the stored representation: scale carries measurement
    # precision, so a 5.0-over-5 rewrite is a real change that numeric equality
    # would call identical. Both sides already hold the stored form, which also
    # makes NaN compare equal to NaN as the database does. Every other type has
    # one representation per value, so equality is the whole story.
    existing_comparable = existing_row[value_column]
    if isinstance(attempted_comparable, Decimal):
        stored_matches = _pg_numeric_text(existing_comparable) == _pg_numeric_text(
            attempted_comparable
        )
    else:
        stored_matches = existing_comparable == attempted_comparable
    return "same" if stored_matches else "different"


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
    contributing study and that field's own idx. Exactly one of
    global_field_idx / study_field_idx must be passed: the non-None one
    selects the WHERE column (the global partial-unique-index path filters
    by m.global_field_idx; the per-field unique-constraint path filters by
    m.{study_field_idx_column}) and the slot identifier embedded in any
    TransientWriteRaceError raised when the occupant has been concurrently
    deleted. Returns every value column, so the caller can diagnose the
    occupant whatever its data_type.
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
        f" {METADATA_VALUE_COLUMNS_SELECT},"
        f" f.study_idx AS contributing_study_idx,"
        f" f.idx AS contributing_study_field_idx"
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
    attempted_value: SampleMetadataValue,
) -> tuple[SlotCompareResult, str | Decimal | date | bool | int | None, int | None]:
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
    attempted_value: SampleMetadataValue,
    data_type: FieldDataType,
    compare_result: SlotCompareResult,
    existing_metadata_idx: int,
    existing_value: str | Decimal | date | bool | int | None,
    existing_missing_reason_idx: int | None,
    contributing_study_idx: int,
    global_field_idx: int | None = None,
) -> SlotOccupiedError:
    """Pick the right SlotOccupiedError subclass from an already-computed slot
    diagnosis and the attempted write's identity. global_field_idx
    discriminates global-path (non-None) vs local-path (None) callers; on the
    local path contributing_study_idx equals attempted_study_idx by
    construction, so only the same-study leaves are reachable there.

    Caller is expected to `raise` the returned instance; this function
    does not raise itself so the call site stays readable.
    """
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
        "existing_metadata_idx": existing_metadata_idx,
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


async def _refetch_conflicting_study_field(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    study_idx: int,
    display_name: str,
) -> asyncpg.Record:
    """Re-read the *_study_field row at (study_idx, display_name) whose presence
    made an ON CONFLICT DO NOTHING insert return nothing. Carries idx plus the
    global FK under FOUND_GLOBAL_FIELD_IDX_ALIAS.

    Raises TransientWriteRaceError when the row is already gone: a concurrent
    transaction deleted-and-committed it between the conflict and this read, so
    the slot is free again — a benign lost race, not schema corruption.
    """
    # f-string interpolation of identifiers is safe: spec fields are frozen
    # module-level constants, never reached by caller input.
    row = await conn.fetchrow(
        f"SELECT idx, {spec.study_field_global_fk_column} AS {FOUND_GLOBAL_FIELD_IDX_ALIAS}"
        f" FROM {spec.study_field_table}"
        f" WHERE study_idx = $1 AND display_name = $2",
        study_idx,
        display_name,
    )
    if row is None:
        raise TransientWriteRaceError(
            row_label=f"{spec.entity_kind}_study_field",
            slot_summary=_study_field_location(study_idx, display_name),
        )
    return row


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

    Resolution keys on (study_idx, display_name), not on global_field_idx: a
    new display_name bound to an already-linked global field mints an
    additional study-local field for that global rather than reusing the
    existing one. This multi-field-per-global aliasing capability within
    a single study is intentional; see docs/architecture.md.

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
    # attach the value to the wrong field.
    row = await _refetch_conflicting_study_field(
        conn, spec=spec, study_idx=study_idx, display_name=display_name
    )
    found_global_field_idx = row[FOUND_GLOBAL_FIELD_IDX_ALIAS]
    if found_global_field_idx != global_field_idx:
        raise StudyFieldConflictError(
            entity_kind=spec.entity_kind,
            study_idx=study_idx,
            display_name=display_name,
            expected_global_field_idx=global_field_idx,
            found_global_field_idx=found_global_field_idx,
        )
    return row["idx"], False


async def _insert_metadata(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    study_field_idx: int,
    data_type: FieldDataType,
    value: SampleMetadataValue,
    created_by_idx: int,
) -> int:
    """Insert one metadata row into spec.metadata_table and return its idx.

    Populates exactly one value column: the typed value column for a bare
    typed value (via GLOBAL_METADATA_VALUE_COLUMN), value_missing_reason_idx
    for a MissingReasonRef, or value_terminology_term_idx for a
    TerminologyTermRef. global_field_idx is populated by trigger from the
    source field row.
    """
    # Resolve which value column to populate and what to bind into it; the
    # shared resolver handles the Ref-vs-typed dispatch and the closed-set guard.
    value_column, bound_value = _resolve_value_column_and_bind(value, data_type)

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


async def _update_metadata(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    metadata_idx: int,
    data_type: FieldDataType,
    value: SampleMetadataValue,
    existing_value_column: str,
) -> None:
    """Overwrite the value of an existing metadata row in place.

    existing_value_column is the value_* column the row currently populates
    (the caller derives it from the occupant diagnosis). The new value's
    target column is resolved from `value`; when it differs from the existing
    one the old column is set NULL in the same statement so the
    exactly-one-value CHECK still holds across a kind change (missing↔typed).
    The (entity, study_field) key columns are immutable and untouched.
    Raises TransientWriteRaceError if no row matched — a concurrent delete
    removed the occupant between the diagnostic read and this overwrite.
    """
    new_column, bound_value = _resolve_value_column_and_bind(value, data_type)

    # Same kind: overwrite the one column. Kind change: clear the old column
    # and set the new one so exactly one value column stays populated.
    if new_column == existing_value_column:
        set_clause = f"{new_column} = $1"
    else:
        set_clause = f"{existing_value_column} = NULL, {new_column} = $1"

    updated_idx = await conn.fetchval(
        f"UPDATE {spec.metadata_table} SET {set_clause} WHERE idx = $2 RETURNING idx",
        bound_value,
        metadata_idx,
    )
    if updated_idx is None:
        raise TransientWriteRaceError(
            row_label=f"{spec.entity_kind}_metadata",
            slot_summary=f"{spec.entity_kind}_metadata_idx={metadata_idx}",
        )


async def _insert_metadata_or_diagnose(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    study_idx: int,
    study_field_idx: int | None = None,
    study_field_created: bool = False,
    display_name: str,
    data_type: FieldDataType,
    value: SampleMetadataValue,
    caller_idx: int,
    global_field_idx: int | None = None,
    on_conflict: MetadataConflictMode = "raise",
) -> SampleMetadataWriteResult:
    """Insert one metadata row inside a SAVEPOINT. On the collision-diagnostic
    unique violation, diagnose the slot occupant, then either overwrite it
    (on_conflict="upsert", when the caller's study is the slot's contributing
    study) or raise the typed SlotOccupiedError subclass (on_conflict="raise",
    or a foreign contributing study even under upsert). The nested transaction
    isolates the INSERT so the caller's outer transaction survives to run the
    diagnostic SELECT and any overwrite. global_field_idx discriminates the two
    write paths: non-None routes the diagnostic constraint and the slot lookup
    through the cross-study partial index and mints the globally-linked
    study_field here; None routes through the per-field constraint and requires
    study_field_idx, the row the caller already resolved. Returns
    SampleMetadataWriteResult carrying the outcome: INSERTED on a clean insert,
    UPDATED on an upsert overwrite, UNCHANGED when the same-study slot already
    held exactly this value (no write performed). Every collision result names
    the study_field the value is attached to, which on the global path may be a
    different alias of that global field than the caller keyed on.

    A foreign study's value is never overwritten: its cross-study conflict
    raises even under upsert, so one study cannot claim or clobber another's
    contribution to a global field.
    """
    # global_field_idx selects the path: the slot lookup key follows from it,
    # as does which unique violation(s) route into diagnosis.
    if global_field_idx is not None:
        slot_kwargs: dict[str, int] = {"global_field_idx": global_field_idx}
        # The global path's diagnostic trigger is the cross-study slot index.
        # Re-writing through the *same* study_field (same display_name) instead
        # trips the per-field constraint; under upsert that must also route
        # into diagnosis so the caller's own value is overwritten, but under
        # raise it stays a raw propagation.
        diagnostic_constraint_names = {spec.global_field_unique_index_name}
        if on_conflict == "upsert":
            diagnostic_constraint_names.add(spec.local_unique_per_field_index_name)
    else:
        slot_kwargs = {"study_field_idx": study_field_idx}
        diagnostic_constraint_names = {spec.local_unique_per_field_index_name}

    # Typed INSERT inside a SAVEPOINT so a unique violation rolls back only the
    # nested savepoint, leaving the caller's outer transaction alive to
    # diagnose and (under upsert) overwrite the occupant below.
    try:
        async with conn.transaction():
            # The global path mints its study_field here, inside the savepoint:
            # the mint resolves on display_name while the slot is keyed on the
            # global field, so a mint that turns out to be unnecessary rolls
            # back with the rejected INSERT below instead of surviving as a
            # value-less alias. The purely-local path arrives with the row it
            # already resolved, which is itself the slot.
            if global_field_idx is not None:
                (
                    target_field_idx,
                    study_field_created,
                ) = await _get_or_create_globally_linked_study_field(
                    conn,
                    spec=spec,
                    study_idx=study_idx,
                    global_field_idx=global_field_idx,
                    display_name=display_name,
                    created_by_idx=caller_idx,
                )
            elif study_field_idx is None:
                raise ValueError("study_field_idx is required when global_field_idx is None")
            else:
                target_field_idx = study_field_idx

            metadata_idx = await _insert_metadata(
                conn,
                spec=spec,
                entity_idx=entity_idx,
                study_field_idx=target_field_idx,
                data_type=data_type,
                value=value,
                created_by_idx=caller_idx,
            )
        return SampleMetadataWriteResult(
            metadata_idx=metadata_idx,
            study_field_idx=target_field_idx,
            study_field_created=study_field_created,
            outcome=FieldWriteOutcome.INSERTED,
        )
    except asyncpg.UniqueViolationError as exc:
        # Only a diagnostic constraint drives the diagnostic path; any other
        # UniqueViolation is the caller's problem and propagates unchanged.
        if exc.constraint_name not in diagnostic_constraint_names:
            raise

    # Diagnose the occupant once; both the upsert decision and any raised error
    # read from this single diagnosis. Reached only via the controlled path
    # above; the outer transaction is alive because the savepoint rolled back.
    existing_row = await _fetch_slot_occupant(
        conn,
        spec=spec,
        entity_idx=entity_idx,
        **slot_kwargs,
    )
    compare_result, existing_value, existing_missing_reason_idx = _diagnose_slot_occupant(
        data_type, existing_row, value
    )
    contributing_study_idx = existing_row["contributing_study_idx"]
    existing_metadata_idx = existing_row["existing_metadata_idx"]
    # The value is attached to the occupant's own study_field, which the global
    # path's mint has just rolled back past: report that row, never "created".
    occupant_field_idx = existing_row["contributing_study_field_idx"]

    # Upsert overwrites only the caller's own study's value; a foreign study's
    # value falls through to the raised cross-study conflict below.
    if on_conflict == "upsert" and contributing_study_idx == study_idx:
        if compare_result == "same":
            # Slot already holds exactly this value — no write; report untouched.
            return SampleMetadataWriteResult(
                metadata_idx=existing_metadata_idx,
                study_field_idx=occupant_field_idx,
                study_field_created=False,
                outcome=FieldWriteOutcome.UNCHANGED,
            )
        # Different value, or a missing↔typed kind change: overwrite in place.
        # The occupant's populated column is the missing-reason column when it
        # holds a missing reason, otherwise the field's typed column.
        existing_value_column = (
            MISSING_REASON_VALUE_COLUMN
            if existing_missing_reason_idx is not None
            else _resolve_typed_value_column(data_type)
        )
        await _update_metadata(
            conn,
            spec=spec,
            metadata_idx=existing_metadata_idx,
            data_type=data_type,
            value=value,
            existing_value_column=existing_value_column,
        )
        return SampleMetadataWriteResult(
            metadata_idx=existing_metadata_idx,
            study_field_idx=occupant_field_idx,
            study_field_created=False,
            outcome=FieldWriteOutcome.UPDATED,
        )

    raise _make_collision_error(
        spec=spec,
        entity_idx=entity_idx,
        display_name=display_name,
        study_field_idx=occupant_field_idx,
        attempted_study_idx=study_idx,
        attempted_value=value,
        data_type=data_type,
        compare_result=compare_result,
        existing_metadata_idx=existing_metadata_idx,
        existing_value=existing_value,
        existing_missing_reason_idx=existing_missing_reason_idx,
        contributing_study_idx=contributing_study_idx,
        global_field_idx=global_field_idx,
    )


async def write_global_metadata_or_diagnose(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    study_idx: int,
    global_field_idx: int,
    display_name: str,
    data_type: FieldDataType,
    value: SampleMetadataValue,
    caller_idx: int,
    on_conflict: MetadataConflictMode = "raise",
) -> SampleMetadataWriteResult:
    """Write one globally-linked metadata row; on cross-study slot collision,
    diagnose the existing occupant and either overwrite it (on_conflict=
    "upsert", caller's study owns the slot) or raise a typed exception.

    Returns SampleMetadataWriteResult (carrying the write outcome) on success.
    The caller owns the outer transaction. A study_field minted for this write
    never outlives a write that does not use it: it is created inside the same
    savepoint as the INSERT, so it rolls back both on a raised exception and on
    an upsert that resolves to a value held through another alias of the same
    global field. UniqueViolations whose constraint_name is NOT
    spec.global_field_unique_index_name propagate unchanged.
    StudyFieldConflictError and TransientWriteRaceError also propagate. Under
    upsert a foreign study's value still raises rather than being overwritten.
    """
    # Fail-fast: the caller must own the transaction so the whole write, mint
    # included, rolls back atomically.
    require_transaction(conn)

    # The study_field bound to global_field_idx is resolved (and minted on
    # first use) inside the insert-and-diagnose savepoint.
    return await _insert_metadata_or_diagnose(
        conn,
        spec=spec,
        entity_idx=entity_idx,
        study_idx=study_idx,
        display_name=display_name,
        data_type=data_type,
        value=value,
        caller_idx=caller_idx,
        global_field_idx=global_field_idx,
        on_conflict=on_conflict,
    )


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

    # Create branch — purely-local row, FK column left NULL. ON CONFLICT
    # DO NOTHING absorbs the unique-constraint hit so the concurrent loser
    # of the race does not raise. f-string interpolation of identifiers is
    # safe: spec fields are frozen module-level constants, never reached by
    # caller input.
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
    # winner committed. The resolved row's FK travels back to the caller so
    # it can detect a globally-linked resolution.
    row = await _refetch_conflicting_study_field(
        conn, spec=spec, study_idx=study_idx, display_name=display_name
    )
    return row["idx"], False, row[FOUND_GLOBAL_FIELD_IDX_ALIAS]


def _study_field_read_sql(spec: EntityMetadataSpec) -> str:
    """Compose the SELECT/FROM/JOIN prefix every {entity}_study_field read shares,
    for the caller to append its own WHERE to.

    A globally-linked row leaves data_type / required / terminology_idx NULL
    (they live on the global-field row), so the read LEFT JOINs the global field
    and COALESCEs those three into their effective values. tier_override has no
    global counterpart (the global row carries default_tier, a distinct concept),
    so it comes through as stored (NULL on a linked row). The global FK column
    keeps its entity-specific name (spec.study_field_global_fk_column);
    the row's own idx comes through as `idx`.
    """
    # f-string interpolation of identifiers is safe: spec fields are frozen
    # module-level constants, never reached by caller input.
    fk_column = spec.study_field_global_fk_column
    return (
        f"SELECT"
        f"    sf.idx,"
        f"    sf.study_idx,"
        f"    sf.{fk_column},"
        f"    sf.display_name,"
        f"    sf.description,"
        f"    COALESCE(sf.data_type, gf.data_type) AS data_type,"
        f"    COALESCE(sf.required, gf.required) AS required,"
        f"    COALESCE(sf.terminology_idx, gf.terminology_idx) AS terminology_idx,"
        f"    sf.tier_override,"
        f"    sf.created_by_idx,"
        f"    sf.created_at"
        f" FROM {spec.study_field_table} sf"
        f" LEFT JOIN {spec.global_field_table} gf"
        f"     ON gf.idx = sf.{fk_column}"
    )


async def fetch_study_field(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    idx: int,
) -> asyncpg.Record | None:
    """Return one {entity}_study_field row by idx, or None on miss.

    Values inherited from a linked global field arrive already resolved. The row
    names the global FK by its entity-specific column and the row's own idx as
    `idx`. Accepts either a pool or a connection.
    """
    sql = f"{_study_field_read_sql(spec)} WHERE sf.idx = $1"
    row = await pool_or_conn.fetchrow(sql, idx)
    return row


async def fetch_study_fields_for_study(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    study_idx: int,
) -> list[asyncpg.Record]:
    """Return every {entity}_study_field row on study_idx, ordered by display_name.

    Values inherited from a linked global field arrive already resolved; each row
    names the global FK by its entity-specific column and its own idx as `idx`. A
    study with no fields yields an empty list. Unbounded by design: the row count
    is the study's curated field vocabulary, not a per-sample roster. Accepts
    either a pool or a connection.
    """
    sql = f"{_study_field_read_sql(spec)} WHERE sf.study_idx = $1 ORDER BY sf.display_name"
    rows = await pool_or_conn.fetch(sql, study_idx)
    return rows


async def fetch_global_fields(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
) -> list[asyncpg.Record]:
    """Return every row of the *_global_field table named by
    spec.global_field_table, ordered by internal_name.

    The stored row names its own idx as `idx`. Unbounded by design
    since the row count is the deployment's curated field vocabulary, not a
    per-sample roster. Accepts either a pool or a connection.
    """
    rows = await pool_or_conn.fetch(
        "SELECT idx, internal_name, display_name, description, data_type,"
        " default_tier, required, terminology_idx, created_by_idx, created_at"
        f" FROM {spec.global_field_table}"
        " ORDER BY internal_name"
    )
    return rows


async def create_study_field(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    study_idx: int,
    display_name: str,
    created_by_idx: int,
    description: str | None = None,
    global_field_idx: int | None = None,
    data_type: FieldDataType | None = None,
    required: bool | None = None,
    terminology_idx: int | None = None,
    tier_override: Tier | None = None,
) -> int:
    """Create one {entity}_study_field and return its idx, failing if the name
    is already used on the study.

    Dispatches on global_field_idx: non-None mints a globally-linked row
    (inherited columns must be omitted); None mints a purely-local row and
    requires data_type. Unlike the get-or-create write entry points, a row
    already at (study_idx, display_name) is not reused — it raises
    StudyFieldAlreadyExistsError. StudyFieldConflictError (a globally-linked
    create hitting a same-name row bound to a different global field) and
    TransientWriteRaceError propagate from the underlying primitive. The
    caller owns the transaction; a raised exception rolls the row back.
    """
    # Both branches INSERT-then-diagnose across two statements; require a
    # wrapping transaction so a raised exception rolls the row back.
    require_transaction(conn)

    # Globally-linked mode: the inherited columns live on the global-field row,
    # so the caller must omit them here — reject rather than silently drop.
    if global_field_idx is not None:
        if any(x is not None for x in (data_type, required, terminology_idx, tier_override)):
            raise ValueError(
                "data_type/required/terminology_idx/tier_override must be omitted "
                "for a globally-linked study field"
            )
        idx, created = await _get_or_create_globally_linked_study_field(
            conn,
            spec=spec,
            study_idx=study_idx,
            global_field_idx=global_field_idx,
            display_name=display_name,
            created_by_idx=created_by_idx,
            description=description,
        )
    else:
        # Purely-local mode: data_type is mandatory here (the DB defaults it,
        # but this create requires the caller to state it); required
        # defaults to False.
        if data_type is None:
            raise ValueError("data_type is required for a purely-local study field")
        # A display_name that already names a global field is accepted here on
        # purpose; metadata writes keyed on that name are then refused as a
        # shadowed field, an accepted cost for the study that chose it. A delete
        # surface for study fields is not built yet.
        idx, created, _ = await _get_or_create_local_study_field(
            conn,
            spec=spec,
            study_idx=study_idx,
            display_name=display_name,
            created_by_idx=created_by_idx,
            description=description,
            data_type=data_type,
            required=required if required is not None else False,
            terminology_idx=terminology_idx,
            tier_override=tier_override,
        )

    # A row already at this name is an error here, not a silent reuse.
    if not created:
        raise StudyFieldAlreadyExistsError(
            entity_kind=spec.entity_kind,
            study_idx=study_idx,
            display_name=display_name,
        )
    return idx


async def create_study_field_and_read_back(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    study_idx: int,
    display_name: str,
    created_by_idx: int,
    description: str | None = None,
    global_field_idx: int | None = None,
    data_type: FieldDataType | None = None,
    required: bool | None = None,
    terminology_idx: int | None = None,
    tier_override: Tier | None = None,
) -> asyncpg.Record:
    """Create one {entity}_study_field and return the stored row, failing if
    the name is already used on the study.

    Mints the row via create_study_field (which rejects a name already in use
    and enforces the local/linked mode rules), then re-reads it through
    fetch_study_field so the returned Record carries the resolved,
    inheritance-aware column values. Returns a Record. The caller owns the
    transaction; StudyFieldAlreadyExistsError / StudyFieldConflictError /
    TransientWriteRaceError propagate from the create primitive.
    """
    # The mint and the read-back span two statements against the same row;
    # require a wrapping transaction so the read sees the just-minted row.
    require_transaction(conn)
    created_idx = await create_study_field(
        conn,
        spec=spec,
        study_idx=study_idx,
        display_name=display_name,
        created_by_idx=created_by_idx,
        description=description,
        global_field_idx=global_field_idx,
        data_type=data_type,
        required=required,
        terminology_idx=terminology_idx,
        tier_override=tier_override,
    )
    created_row = await fetch_study_field(conn, spec=spec, idx=created_idx)
    # The row was just inserted inside this transaction, so a miss here means
    # corruption, not an ordinary absence; fail loud rather than returning None
    # up a Record-typed contract.
    if created_row is None:
        raise RuntimeError(
            f"{spec.study_field_table} idx={created_idx} vanished immediately after insert"
        )
    return created_row


async def write_local_metadata_or_diagnose(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    study_idx: int,
    display_name: str,
    data_type: FieldDataType,
    value: SampleMetadataValue,
    caller_idx: int,
    required: bool = False,
    terminology_idx: int | None = None,
    tier_override: Tier | None = None,
    on_conflict: MetadataConflictMode = "raise",
) -> SampleMetadataWriteResult:
    """Write one local (non-globally-linked) metadata row; on collision,
    diagnose the existing occupant and either overwrite it (on_conflict=
    "upsert") or raise a typed exception.

    Returns SampleMetadataWriteResult (carrying the write outcome) on success.
    The caller owns the outer transaction: any study_field row created here
    rolls back with it on a raised exception. required, terminology_idx, and
    tier_override are forwarded to the study_field create branch only.
    UniqueViolations whose constraint_name is NOT
    spec.local_unique_per_field_index_name propagate unchanged.
    LocalWriteOnGloballyLinkedFieldError and TransientWriteRaceError also
    propagate. A purely-local slot is single-study by construction, so upsert
    always overwrites the caller's own value here (no foreign-study case).
    """
    # Fail-fast: the caller must own the transaction so the typed exception
    # rolls back any study_field row this function created before raising.
    require_transaction(conn)

    # Get-or-create the local study_field. The third tuple element is the
    # resolved row's global_field_idx; non-None means the row is globally
    # linked, which contradicts the caller's local-only intent and triggers
    # the strict-mode guard.
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

    # Insert-and-diagnose; the local path has no cross-study slot key, so
    # global_field_idx stays None (which selects the per-field constraint).
    return await _insert_metadata_or_diagnose(
        conn,
        spec=spec,
        entity_idx=entity_idx,
        study_idx=study_idx,
        study_field_idx=study_field_idx,
        study_field_created=study_field_created,
        display_name=display_name,
        data_type=data_type,
        value=value,
        caller_idx=caller_idx,
        on_conflict=on_conflict,
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


# The `internal_name`s whose `required` flag this import path actually enforces.
# Narrow ON PURPOSE — see assert_required_global_fields_supplied. Adding an entry
# here is the whole change needed to start enforcing another field.
_ENFORCED_REQUIRED_FIELDS = frozenset({"host_taxon_id"})


# Slated for removal: the premise behind this gate is a misunderstanding, so it is
# deliberately not being extended to cover the cases it gets wrong. Notably, a required
# field whose value is supplied through a study-local alias of that field is not
# recognized as supplied, and the import is rejected as missing.
async def assert_required_global_fields_supplied(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    metadata: Mapping[str, str],
    global_internal_names: bool = False,
) -> None:
    """Reject an import that omits an ENFORCED required global field.

    `biosample_global_field.required` has been in the schema since the first
    migration and was never enforced anywhere — a field could be declared required
    and simply not supplied. That is how `host_taxon_id` came to be marked required
    and yet be absent from every one of the samples we hold.

    It matters now because host filtering is resolved FROM that field: a sample
    without it resolves UNRESOLVED and aborts its pool at submit. Unenforced, every
    newly-ingested pool would arrive broken and the backfill would be a treadmill
    rather than a one-off.

    DELIBERATELY NARROW. This enforces only `_ENFORCED_REQUIRED_FIELDS`, not every
    field the schema marks `required` — because the other required fields
    (collection_date, the geo/ENVO set, taxon_id) were ALSO never enforced, and
    turning them all on at once would reject historical-shaped imports this arc has
    no reason to touch. `host_taxon_id` is enforced because this is the PR that makes
    its absence a hard failure downstream; the rest is a separate decision about
    intake strictness, made when someone owns it. Widening the set is the whole
    change required to enforce another field.

    A missing-value marker ('not applicable', 'missing: control sample', …) COUNTS
    as supplied — declining to give a value is a decision, and it is one the
    resolver understands. What is rejected is silence.

    global_internal_names matches the caller's key namespace: the supplied
    metadata keys are compared against the required fields' internal_names when
    set, and their display_names otherwise, so the check stays accurate under
    either resolution mode.

    Raises before any DB write, with the whole missing set at once.
    """
    rows = await conn.fetch(
        f"SELECT internal_name, display_name FROM {spec.global_field_table}"  # noqa: S608
        " WHERE required AND internal_name = ANY($1::text[])",
        sorted(_ENFORCED_REQUIRED_FIELDS),
    )
    # Compare against whichever name the caller keyed on, so a required field
    # supplied by internal_name is not falsely reported missing under the flag.
    key_column = "internal_name" if global_internal_names else "display_name"
    required = {r[key_column] for r in rows}
    supplied = set(metadata)
    missing = sorted(required - supplied)
    if missing:
        raise MetadataMissingRequiredFieldsError(spec.entity_kind, missing)


async def _resolve_and_parse_metadata_values(
    conn: asyncpg.Connection,
    *,
    metadata: Mapping[str, str],
    fields: Mapping[str, ResolvedField],
    reason_lookup: Mapping[str, int],
) -> dict[str, SampleMetadataValue]:
    """Parse each metadata text value against its already-resolved field,
    returning caller_key -> parsed value.

    fields maps every caller_key in metadata to its ResolvedField
    (data_type + terminology_idx). A value matching a reason_lookup key
    (compared after outer-whitespace stripping) becomes a MissingReasonRef; a
    TERMINOLOGY-typed field routes to a TerminologyTermRef on hit or raises
    MetadataParseError on miss; any other value goes through the typed parser.
    An empty reason_lookup disables marker recognition.
    """
    # Group terminology candidates by terminology_idx so the lookups batch per
    # terminology. Missing-reason markers take precedence, so a text already
    # matching a known reason name is excluded from the candidate set.
    terminology_candidates: dict[int, set[str]] = {}
    for caller_key, text_value in metadata.items():
        resolved_field = fields[caller_key]
        if resolved_field.data_type is not FieldDataType.TERMINOLOGY:
            continue
        stripped = text_value.strip()
        if stripped in reason_lookup:
            continue
        # terminology_idx is non-None for TERMINOLOGY-typed rows by the
        # *_global_field CHECK; assert rather than guard so a CHECK violation
        # surfaces loudly instead of silently dropping the row.
        assert resolved_field.terminology_idx is not None
        terminology_candidates.setdefault(resolved_field.terminology_idx, set()).add(stripped)

    # One round trip per distinct terminology_idx; the helper short-circuits
    # on empty inputs so a no-terminology import pays nothing.
    terminology_lookup: dict[tuple[int, str], tuple[int, str, str | None]] = {}
    for terminology_idx, term_ids in terminology_candidates.items():
        resolved = await fetch_terminology_term_idxs_by_term_ids(
            conn, terminology_idx=terminology_idx, term_ids=term_ids
        )
        for term_id, resolved_term in resolved.items():
            terminology_lookup[(terminology_idx, term_id)] = resolved_term

    # Parse each text value: missing-reason markers route to MissingReasonRef
    # first; TERMINOLOGY-typed fields then route to TerminologyTermRef on hit
    # or raise MetadataParseError on miss; other values dispatch to the typed
    # parser.
    parsed: dict[str, SampleMetadataValue] = {}
    for caller_key, text_value in metadata.items():
        resolved_field = fields[caller_key]
        stripped = text_value.strip()
        if stripped in reason_lookup:
            parsed[caller_key] = MissingReasonRef(idx=reason_lookup[stripped], name=stripped)
            continue
        if resolved_field.data_type is FieldDataType.TERMINOLOGY:
            # terminology_idx is non-None for TERMINOLOGY-typed rows by the
            # *_global_field CHECK; assert rather than guard so a CHECK violation
            # surfaces loudly instead of silently dropping the row.
            assert resolved_field.terminology_idx is not None
            resolved_term = terminology_lookup.get((resolved_field.terminology_idx, stripped))
            if resolved_term is None:
                raise MetadataParseError(
                    field_key=caller_key,
                    data_type=resolved_field.data_type,
                    text_value=text_value,
                    reason="no matching terminology term",
                )
            term_idx, term_label, term_alternate_label = resolved_term
            parsed[caller_key] = TerminologyTermRef(
                idx=term_idx,
                term_id=stripped,
                label=term_label,
                alternate_label=term_alternate_label,
            )
            continue
        parsed[caller_key] = parse_text_for_data_type(
            caller_key, resolved_field.data_type, text_value
        )
    return parsed


class OwnerSampleIdMetadataWriteError(Exception):
    """Raised when metadata input names a display_name that resolves to a
    study-local field serving as an owner-sample-id field (a metadata row
    flagged via spec.owner_sample_id_flag_column). That value is written and
    changed only through its dedicated import/admin path, never as ordinary
    metadata, so a generic write to it is refused.
    """

    def __init__(
        self,
        *,
        entity_kind: SampleEntityKind,
        display_name: str,
        study_field_idx: int,
    ) -> None:
        self.entity_kind = entity_kind
        self.display_name = display_name
        self.study_field_idx = study_field_idx
        super().__init__(
            f"{entity_kind} metadata field {display_name!r}"
            f" ({entity_kind}_study_field_idx={study_field_idx}) is an"
            f" owner-sample-id field; it cannot be written as ordinary metadata"
        )


async def _fetch_owner_sample_id_study_field_idxs(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    study_field_idxs: Sequence[int],
) -> set[int]:
    """Return the subset of study_field_idxs that serve as owner-sample-id
    fields — those carrying at least one metadata row flagged via
    spec.owner_sample_id_flag_column.

    A spec without that column, or empty input, short-circuits with no DB call.
    Study-field-scoped, not entity-scoped: a field used as an owner-sample-id
    field for any entity in the study counts, since that field name is reserved
    for the owner-sample-id and must not take ordinary values for any entity.
    """
    if spec.owner_sample_id_flag_column is None or not study_field_idxs:
        return set()
    idxs = list(study_field_idxs)
    # f-string interpolation of identifiers is safe: spec fields are frozen
    # module-level constants, never reached by caller input.
    rows = await conn.fetch(
        f"SELECT DISTINCT {spec.study_field_idx_column} AS study_field_idx"
        f" FROM {spec.metadata_table}"
        f" WHERE {spec.study_field_idx_column} = ANY($1::bigint[])"
        f"   AND {spec.owner_sample_id_flag_column} = true",
        idxs,
    )
    return {r["study_field_idx"] for r in rows}


async def preflight_sample_metadata(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    study_idx: int,
    metadata: Mapping[str, str],
    known_missing_reasons: Mapping[str, int] | None = None,
    allow_local: bool = False,
    global_internal_names: bool = False,
) -> list[ResolvedFieldValue]:
    """Resolve and parse metadata into one ordered list of (ResolvedField,
    parsed_value) in caller input order; each entry's scope selects its write
    path.

    Global fields are matched on display_name by default, or on internal_name
    when global_internal_names is set (globals are the only fields with an
    internal_name). With allow_local False only global fields are accepted;
    with allow_local True a name may also resolve to an existing study-local
    field on study_idx (never created here). known_missing_reasons maps reason
    name -> idx; None or empty disables marker recognition. Raises
    MetadataUnknownFieldsError (name matches no field), StudyFieldConflictError
    (a display-name-keyed name is a global field but also a study-local field
    bound elsewhere — suppressed under global_internal_names, where a global's
    internal_name and a study field's display_name are distinct namespaces),
    DuplicateGlobalFieldTargetError (two columns target one global field),
    OwnerSampleIdMetadataWriteError (a name resolves to a study-local field
    serving as an owner-sample-id field, for a spec that defines one), or
    MetadataParseError (value will not coerce to its field's data_type).
    """
    # Resolve names against global fields first, on internal_name when the
    # caller keyed globals that way, else on display_name.
    global_key_column = "internal_name" if global_internal_names else "display_name"
    global_field_rows = await fetch_global_fields_by_keys(
        conn, spec=spec, keys=metadata.keys(), key_column=global_key_column
    )
    non_global_names = [name for name in metadata if name not in global_field_rows]

    # Strict mode (no local writes): reject any non-global names up front,
    # collecting every name into one error.
    if not allow_local and non_global_names:
        raise MetadataUnknownFieldsError(spec.entity_kind, non_global_names)

    # Local write mode: resolve names against the study's *existing* study-local fields.
    study_field_rows = await fetch_study_fields_by_display_names(
        conn, spec=spec, study_idx=study_idx, display_names=metadata.keys()
    )

    # Partition each name into a ResolvedField carrying its write path. A direct
    # global match routes through the global (study_field minted at write, so
    # study_field_idx stays None); a study-local alias routes through its global
    # but records the existing alias row; a purely-local field writes locally.
    resolved: dict[str, ResolvedField] = {}
    unknown: list[str] = []
    for curr_name in metadata:
        curr_global_field = global_field_rows.get(curr_name)
        curr_study_field = study_field_rows.get(curr_name)
        if curr_global_field is not None:
            # A coincidental same-display study field bound to a different global
            # is a real ambiguity only when the caller keyed by display_name;
            # under internal-name keying the two live in distinct namespaces, so
            # the internal-name match is authoritative and the check is skipped.
            if (
                not global_internal_names
                and curr_study_field is not None
                and curr_study_field.global_field_idx != curr_global_field.global_field_idx
            ):
                raise StudyFieldConflictError(
                    entity_kind=spec.entity_kind,
                    study_idx=study_idx,
                    display_name=curr_name,
                    expected_global_field_idx=curr_global_field.global_field_idx,
                    found_global_field_idx=curr_study_field.global_field_idx,
                )
            resolved[curr_name] = ResolvedField(
                caller_key=curr_name,
                global_field_idx=curr_global_field.global_field_idx,
                study_field_idx=None,
                canonical_display=curr_global_field.display_name,
                data_type=curr_global_field.data_type,
                terminology_idx=curr_global_field.terminology_idx,
                internal_name=curr_global_field.internal_name,
            )
        elif curr_study_field is not None:
            resolved[curr_name] = ResolvedField(
                caller_key=curr_name,
                # A globally-linked alias writes through its global field; a
                # purely-local field (global_field_idx None) writes locally, and
                # the derived scope follows that same nullness.
                global_field_idx=curr_study_field.global_field_idx,
                study_field_idx=curr_study_field.idx,
                canonical_display=curr_study_field.display_name,
                data_type=curr_study_field.data_type,
                terminology_idx=curr_study_field.terminology_idx,
                internal_name=curr_study_field.internal_name,
            )
        else:
            unknown.append(curr_name)

    if unknown:
        raise MetadataUnknownFieldsError(spec.entity_kind, unknown)

    # Refuse a write that targets an owner-sample-id field: that value is owned
    # by the dedicated import/admin path, never written as ordinary metadata.
    # The lookup short-circuits when the spec has no owner-sample-id flag column
    # or no local fields were resolved.
    local_field_idxs = [rf.study_field_idx for rf in resolved.values() if rf.scope == "local"]
    owner_id_field_idxs = await _fetch_owner_sample_id_study_field_idxs(
        conn, spec=spec, study_field_idxs=local_field_idxs
    )
    for curr_name, rf in resolved.items():
        if rf.scope == "local" and rf.study_field_idx in owner_id_field_idxs:
            raise OwnerSampleIdMetadataWriteError(
                entity_kind=spec.entity_kind,
                display_name=curr_name,
                study_field_idx=rf.study_field_idx,
            )

    # No two input columns may target the same global field: one entity's value
    # for a global field cannot come from multiple columns.
    global_field_sources: dict[int, list[str]] = {}
    for name, rf in resolved.items():
        if rf.scope == "global":
            global_field_sources.setdefault(rf.global_field_idx, []).append(name)
    for target_global_field_idx, source_names in global_field_sources.items():
        if len(source_names) > 1:
            raise DuplicateGlobalFieldTargetError(
                entity_kind=spec.entity_kind,
                global_field_idx=target_global_field_idx,
                field_keys=source_names,
            )

    parsed_values = await _resolve_and_parse_metadata_values(
        conn,
        metadata=metadata,
        fields=resolved,
        reason_lookup=known_missing_reasons or {},
    )

    # Emit one list in caller input order; scope on each ResolvedField carries
    # the global-vs-local discriminator.
    return [(resolved[name], parsed_values[name]) for name in metadata]


async def fetch_entity_is_linked_to_study(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    study_idx: int,
) -> bool:
    """Return True iff entity_idx has a non-retired link to study_idx.

    Checks only spec.link_table's own retirement (retired = false); the
    entity's own retirement is a separate row the caller checks itself.
    Accepts either a pool or a connection so the read composes inside an
    open transaction or stands alone.
    """
    # f-string interpolation of identifiers is safe: spec fields are frozen
    # module-level constants, never reached by caller input.
    return await pool_or_conn.fetchval(
        f"SELECT EXISTS ("
        f"    SELECT 1 FROM {spec.link_table}"
        f"     WHERE {spec.link_entity_key_column} = $1"
        f"       AND study_idx = $2"
        f"       AND retired = false"
        f")",
        entity_idx,
        study_idx,
    )


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


async def write_resolved_metadata_entries(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    study_idx: int,
    caller_idx: int,
    resolved_metadata: Sequence[ResolvedFieldValue],
    on_conflict: MetadataConflictMode = "raise",
) -> list[SampleMetadataFieldResult]:
    """Write each resolved entry against study_idx, dispatched by scope, and
    return one SampleMetadataFieldResult per entry in caller input order.

    A "global" entry writes through its global field (its study-local row minted
    on first use); a "local" entry writes against its existing study_field. Each
    result is keyed on the caller's key. on_conflict: "raise" fails on a slot
    collision, "upsert" overwrites the caller's own study's value. The first
    collision or rollback signal propagates, and the caller's outer transaction
    rolls partial state back.
    """
    # One write per entry, sequentially; the outer transaction is the atomicity
    # boundary. The global path writes through the global field (minting the
    # study-local row as needed); the local path writes against the resolved
    # study_field with global_field_idx omitted, selecting the per-field slot.
    results: list[SampleMetadataFieldResult] = []
    for resolved_field, parsed_value in resolved_metadata:
        if resolved_field.scope == "global":
            write_result = await write_global_metadata_or_diagnose(
                conn,
                spec=spec,
                entity_idx=entity_idx,
                study_idx=study_idx,
                global_field_idx=resolved_field.global_field_idx,
                display_name=resolved_field.canonical_display,
                data_type=resolved_field.data_type,
                value=parsed_value,
                caller_idx=caller_idx,
                on_conflict=on_conflict,
            )
        else:
            write_result = await _insert_metadata_or_diagnose(
                conn,
                spec=spec,
                entity_idx=entity_idx,
                study_idx=study_idx,
                study_field_idx=resolved_field.study_field_idx,
                study_field_created=False,
                display_name=resolved_field.canonical_display,
                data_type=resolved_field.data_type,
                value=parsed_value,
                caller_idx=caller_idx,
                on_conflict=on_conflict,
            )
        results.append(
            SampleMetadataFieldResult(
                field_key=resolved_field.caller_key,
                internal_name=resolved_field.internal_name,
                outcome=write_result.outcome,
                value=parsed_value,
            )
        )
    return results


async def write_sample_metadata(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    study_idx: int,
    metadata: Mapping[str, str],
    caller_idx: int,
    allow_local: bool,
    known_missing_reasons: Mapping[str, int] | None = None,
    on_conflict: MetadataConflictMode = "raise",
    global_internal_names: bool = False,
) -> list[SampleMetadataFieldResult]:
    """Resolve, validate, and write one entity's metadata against study_idx.

    Writes globally-linked entries always, and — when allow_local — values into
    *existing* study-local fields; no study field is created here beyond the
    study-local row a global write mints on first use. Returns one
    SampleMetadataFieldResult per input field in the caller's metadata key
    order, each carrying whether the field resolved global vs local and what the
    write did to its slot.

    known_missing_reasons pre-supplies the marker set when the caller has
    already resolved a superset for other text; None resolves it here from
    the metadata values alone. allow_local False accepts only global fields,
    True also writes existing local ones. global_internal_names keys global
    fields on internal_name rather than display_name (globals only; local fields
    are always keyed on display_name). on_conflict: "raise" (default) fails on a
    slot collision, "upsert" overwrites the caller's own study's value.

    Caller owns the transaction (require_transaction raises otherwise) so
    every validation and write error propagates and rolls partial state back.
    """
    # Fail-fast: the batched writes must share one transaction so a partial
    # failure rolls back atomically.
    require_transaction(conn)

    # Resolve missing-reason markers from the metadata values unless the
    # caller already resolved a superset for its own text (e.g. an owner-id).
    if known_missing_reasons is None:
        candidate_texts = {v.strip() for v in metadata.values()}
        known_missing_reasons = await fetch_missing_value_reason_idxs_by_names(
            conn, candidate_texts
        )

    # Validate + parse every entry against the study's global and existing
    # study-local fields; any unresolvable/conflicting case raises pre-write.
    # The returned list is already in caller input order.
    resolved_metadata = await preflight_sample_metadata(
        conn,
        spec=spec,
        study_idx=study_idx,
        metadata=metadata,
        known_missing_reasons=known_missing_reasons,
        allow_local=allow_local,
        global_internal_names=global_internal_names,
    )

    return await write_resolved_metadata_entries(
        conn,
        spec=spec,
        entity_idx=entity_idx,
        study_idx=study_idx,
        caller_idx=caller_idx,
        resolved_metadata=resolved_metadata,
        on_conflict=on_conflict,
    )
