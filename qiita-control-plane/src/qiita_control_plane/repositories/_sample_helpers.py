"""Sample-family (biosample + prep_sample) cross-entity helpers.

Holds the shapes, exceptions, and write-and-diagnose machinery that both
biosample and prep_sample repository modules share, so the parallel
implementations stay coordinated without duplicating logic.
Callers inside the repositories package import from
here directly; outside callers should usually reach the per-entity
modules (biosample_metadata, prep_sample_metadata) and pull the shapes
they need transitively.
"""

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Literal, NamedTuple

import asyncpg
from qiita_common.models import FieldDataType, Tier

from . import require_transaction


class SampleEntityKind(StrEnum):
    """Discriminator passed to the shared metadata exceptions so each error
    message names its domain. Values match the table-name prefix used
    throughout the schema (biosample_*, prep_sample_*), so the f-string
    interpolation in the exception messages reads naturally.
    """

    BIOSAMPLE = "biosample"
    PREP_SAMPLE = "prep_sample"


# ---------------------------------------------------------------------------
# Shared *_global_field lookup row shape
# ---------------------------------------------------------------------------


class GlobalFieldRow(NamedTuple):
    """Subset of *_global_field columns the import / composer pre-flight
    needs. Shared by biosample_metadata and prep_sample_metadata because
    both fetch the same three columns from structurally-parallel tables.
    """

    idx: int
    display_name: str
    data_type: FieldDataType


# ---------------------------------------------------------------------------
# Shared globally-linked metadata read shape and value-column dispatch
# ---------------------------------------------------------------------------


class GlobalMetadataRow(NamedTuple):
    """One row from the globally-linked metadata reads on either entity.

    Carries the global field's stable internal_name (which doubles as the
    dict key returned by the *_global_metadata_for_* reads), the cosmetic
    display_name and description (taken from the *_global_field row, not
    from any per-study *_study_field override, because the reads are not
    study-scoped), the field's data_type, and the typed Python value
    extracted from the matching *_metadata.value_* column.
    """

    internal_name: str
    display_name: str
    description: str | None
    data_type: FieldDataType
    value: str | Decimal | date


# Closed set of data_types the globally-linked metadata reads currently
# decode. BOOLEAN and TERMINOLOGY are intentionally absent so a future
# addition is a coordinated extension across read and write paths (both
# sides need to learn the new value_* column at the same time).
GLOBAL_METADATA_VALUE_COLUMN: dict[FieldDataType, str] = {
    FieldDataType.TEXT: "value_text",
    FieldDataType.NUMERIC: "value_numeric",
    FieldDataType.DATE: "value_date",
}


# ---------------------------------------------------------------------------
# Shared metadata parse error and text-to-typed coercion
# ---------------------------------------------------------------------------


class MetadataUnknownFieldsError(Exception):
    """Raised when import metadata names display_names that have no matching
    {entity_kind}_global_field row. Carries every unknown name in one list
    so the caller can surface them all in a single 422.
    """

    def __init__(self, entity_kind: SampleEntityKind, unknown_display_names: list[str]) -> None:
        self.unknown_display_names = unknown_display_names
        super().__init__(
            f"unknown {entity_kind} global field display_names: {unknown_display_names!r}"
        )


class StudyFieldConflictError(Exception):
    """Raised by the globally-linked study-field upsert when a
    {entity_kind}_study_field row already exists at (study_idx,
    display_name) that is purely-local (found_global_field_idx is None)
    or globally linked to a different global field than the one the
    caller requested.
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
    display_name plus the raw inputs so the route can build a field-scoped
    422 detail. Raised by parse_text_for_data_type and caught by the
    routes that drive metadata-bearing imports.
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
    the colliding row in the window between the unique-violation signal
    and the follow-up SELECT.

    The slot is therefore free again — a benign lost race, not schema
    corruption — and the right answer is for the caller to resubmit the
    identical request. Routes map it to a 503 with a Retry-After hint
    rather than a 500.

    row_label names the row type that lost the race (e.g.,
    "biosample_metadata", "prep_sample_study_field"); slot_summary names
    the slot key textually. Both are caller-supplied so this class stays
    agnostic to which table or slot kind it describes.
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
    and TERMINOLOGY are not yet supported and raise NotImplementedError.

    Conversion failures raise MetadataParseError carrying the display_name,
    data_type, raw text, and a friendly reason so the route can build a
    field-scoped 422 message.
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
#
# The two metadata tables (qiita.biosample_metadata, qiita.prep_sample_metadata)
# share a partial unique index on (entity_idx, global_field_idx) that lets at
# most one globally-linked metadata row per (entity, global field) pair exist
# across all studies. write_global_metadata_or_diagnose performs the typed
# INSERT against the right table, and on collision with that partial index
# runs a typed-value diagnostic SELECT to determine which of five sub-cases
# is happening, then raises a typed exception describing it.
# write_local_metadata_or_diagnose follows the same shape against the
# *_metadata_unique_per_field constraint with three local sub-cases plus
# the LocalWriteOnGloballyLinkedFieldError pre-INSERT guard.
#
# Per-entity differences (table and column identifiers, constraint names)
# are captured in EntityMetadataSpec. The two specs (BIOSAMPLE_METADATA_SPEC,
# PREP_SAMPLE_METADATA_SPEC) live in the per-entity repository modules; the
# shared functions in this module never import them directly — callers pass
# the matching spec in.
# ---------------------------------------------------------------------------


class SampleMetadataWriteResult(NamedTuple):
    """Return shape of write_global_metadata_or_diagnose and
    write_local_metadata_or_diagnose on success.

    Carries the new metadata row's idx plus the study_field idx the value
    was attached to and whether that study_field row was created by this
    call (versus reused via the get-or-create lookup branch). Callers that
    only need confirmation of the write can ignore study_field_idx /
    study_field_created; the prep_sample composer threads them into its
    own per-display-name tracking dict.
    """

    metadata_idx: int
    study_field_idx: int
    study_field_created: bool


@dataclass(frozen=True)
class EntityMetadataSpec:
    """Per-entity SQL-identifier binding consumed by the shared sample-family
    helpers in this module.

    Holds the entity discriminator plus the table, column, and constraint
    identifiers that differ between the biosample and prep_sample stacks
    so the shared functions stay agnostic. Carries both metadata-side
    bindings (metadata table, global-field table, study-field table, and
    the constraint names the diagnostic paths key on) and per-study link
    bindings (link_table, link_entity_key_column) consumed by
    insert_entity_to_study and link_entity_to_studies. Constructed once
    per entity at module-load time in the matching *_metadata repository
    module.
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
    # NULL on a purely-local row, non-NULL on a globally-linked one;
    # _get_or_create_globally_linked_study_field writes and verifies it.
    study_field_global_fk_column: str
    global_field_unique_index_name: str
    local_unique_per_field_index_name: str
    # The per-study link table (biosample_to_study / prep_sample_to_study)
    # and the entity-id column on it (biosample_idx / prep_sample_idx).
    # Consumed by insert_entity_to_study so one parameterised INSERT
    # covers both link tables.
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
        f"SELECT idx, display_name, data_type"
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
        )
        for r in rows
    }


async def fetch_global_metadata(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
) -> dict[str, GlobalMetadataRow]:
    """Return a dict of internal_name -> GlobalMetadataRow for every
    globally-linked metadata value the entity carries.

    Filters on <metadata>.global_field_idx IS NOT NULL: purely-local rows
    (including the biosample owner-id row) are excluded. The read is not
    study-scoped — the canonical global value persists across
    <entity>_to_study link retirement, and per-study read access is
    governed by the study_access predicate at the caller's auth boundary,
    not here. Missing-reason rows (value_missing_reason_idx populated) are
    also excluded -- they have no typed value to surface and the import
    path does not currently write them.

    Currently supports data_type in {TEXT, NUMERIC, DATE} (matching the
    import path's closed set); rows of other data_types raise
    NotImplementedError so a future addition is a coordinated extension of
    read + write paths.
    """
    # f-string interpolation of the table identifiers is safe: spec fields
    # are frozen module-level constants, never reached by caller input.
    # Pull every globally-linked, non-missing-reason row for the entity in
    # one round trip; carry every typed value column the closed set covers.
    rows = await pool_or_conn.fetch(
        f"SELECT gf.internal_name, gf.display_name, gf.description, gf.data_type,"
        f" m.value_text, m.value_numeric, m.value_date"
        f" FROM {spec.metadata_table} m"
        f" JOIN {spec.global_field_table} gf ON gf.idx = m.global_field_idx"
        f" WHERE m.{spec.entity_key_column} = $1"
        f"   AND m.global_field_idx IS NOT NULL"
        f"   AND m.value_missing_reason_idx IS NULL",
        entity_idx,
    )

    # Walk rows, dispatch each to the value column the data_type names.
    # The unsupported branch raises so an out-of-set data_type cannot
    # silently surface a NULL value.
    result: dict[str, GlobalMetadataRow] = {}
    for r in rows:
        data_type = FieldDataType(r["data_type"])
        column = GLOBAL_METADATA_VALUE_COLUMN.get(data_type)
        if column is None:
            raise NotImplementedError(
                f"global metadata read for data_type={data_type} is not yet implemented"
            )
        result[r["internal_name"]] = GlobalMetadataRow(
            internal_name=r["internal_name"],
            display_name=r["display_name"],
            description=r["description"],
            data_type=data_type,
            value=r[column],
        )
    return result


# ---------------------------------------------------------------------------
# Global-field collision exception family
# ---------------------------------------------------------------------------
#
# GlobalFieldSlotOccupiedError is a plain Exception, not an
# asyncpg.UniqueViolationError subclass. It is raised by this module only
# after the triggering UniqueViolationError has already been caught and the
# slot occupant diagnosed, so it carries diagnostic payload rather than raw
# asyncpg Postgres-message attributes. Routes catch it with its own `except`
# clause, independent of any `except asyncpg.UniqueViolationError`.
# ---------------------------------------------------------------------------


class GlobalFieldSlotOccupiedError(Exception):
    """Base class: the partial unique index on (entity_idx, global_field_idx)
    rejected the write because the slot is already occupied. The concrete
    subclass names which of five sub-cases applies; all subclasses carry the
    same payload.

    Subclasses with empty bodies are intentional: the discriminator IS the
    type, and `match exc: case DuplicateValueSameStudyError(): ...` keeps
    route-side response shaping declarative.
    """

    def __init__(
        self,
        *,
        entity_kind: SampleEntityKind,
        entity_idx: int,
        global_field_idx: int,
        attempted_study_idx: int,
        attempted_value: str | Decimal | date,
        data_type: FieldDataType,
        existing_metadata_idx: int,
        existing_value: str | Decimal | date | None,
        existing_missing_reason_idx: int | None,
        contributing_study_idx: int,
    ) -> None:
        self.entity_kind = entity_kind
        self.entity_idx = entity_idx
        self.global_field_idx = global_field_idx
        self.attempted_study_idx = attempted_study_idx
        self.attempted_value = attempted_value
        self.data_type = data_type
        self.existing_metadata_idx = existing_metadata_idx
        self.existing_value = existing_value
        self.existing_missing_reason_idx = existing_missing_reason_idx
        self.contributing_study_idx = contributing_study_idx
        super().__init__(
            f"{entity_kind}_metadata slot ({entity_kind}_idx={entity_idx},"
            f" global_field_idx={global_field_idx}) is already occupied"
            f" by {entity_kind}_metadata_idx={existing_metadata_idx}"
            f" contributed via study_idx={contributing_study_idx}"
        )


class DuplicateValueSameStudyError(GlobalFieldSlotOccupiedError):
    """Existing row's value equals the attempted value; the caller's study is
    the contributing study. Idempotent confirm — no write was performed."""


class ConflictingValueSameStudyError(GlobalFieldSlotOccupiedError):
    """Existing row's value differs from the attempted value; the caller's
    study is the contributing study. The caller asked to INSERT but a row
    already exists; correction requires an explicit PATCH or DELETE+INSERT."""


class DuplicateValueDifferentStudyError(GlobalFieldSlotOccupiedError):
    """Existing row's value equals the attempted value; another study
    contributed it. The desired global state already exists — but the
    caller's study does not own the row."""


class ConflictingValueDifferentStudyError(GlobalFieldSlotOccupiedError):
    """Existing row's value differs from the attempted value; another study
    contributed it. The real cross-study conflict — the global field's
    canonical value is in dispute."""


class SlotOccupiedByMissingReasonError(GlobalFieldSlotOccupiedError):
    """The slot holds a row recorded as intentionally missing
    (value_missing_reason_idx populated); no typed value to compare against."""


# ---------------------------------------------------------------------------
# Diagnostic helpers (private)
# ---------------------------------------------------------------------------


def _resolve_typed_value_column(data_type: FieldDataType) -> str:
    """Map data_type to the qiita.*_metadata.value_* column holding its
    typed value, via GLOBAL_METADATA_VALUE_COLUMN.

    Single source of the lookup-and-guard so the collision classifiers and
    the value comparison cannot diverge on which data_types are decodable.
    Raises NotImplementedError for data_types the diagnostic path does not
    yet decode (BOOLEAN, TERMINOLOGY).
    """
    column = GLOBAL_METADATA_VALUE_COLUMN.get(data_type)
    if column is None:
        raise NotImplementedError(
            f"global-metadata write diagnostic for data_type={data_type} is not yet implemented"
        )
    return column


def _compare_typed_value(
    value_column: str,
    existing_row: Mapping[str, object],
    attempted_value: str | Decimal | date,
) -> Literal["same", "different", "missing_reason"]:
    """Classify the relationship between the existing slot occupant's value
    and the value the caller attempted to write.

    Returns "missing_reason" when the existing row has value_missing_reason_idx
    populated (no typed value column to compare); otherwise compares the
    caller-resolved value_column for equality.
    """
    # Missing-reason rows have no typed value; the comparison is undefined.
    if existing_row["value_missing_reason_idx"] is not None:
        return "missing_reason"

    # Single typed comparison; Decimal/date/str equality is well-defined.
    return "same" if existing_row[value_column] == attempted_value else "different"


async def _fetch_global_field_slot_occupant(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    global_field_idx: int,
) -> Mapping[str, object]:
    """Read the existing row occupying the (entity_idx, global_field_idx)
    slot, joined to its source study_field row to recover the contributing
    study_idx. Returns all six value columns so the caller can dispatch on
    data_type to pick the right one. value_boolean and
    value_terminology_term_idx are fetched ahead of need: the current
    classifier cannot surface them, but BOOLEAN/TERMINOLOGY support is a
    planned coordinated extension and selecting them now keeps this read
    stable when that lands.

    The spec parameterises which table and key column are read. The SQL
    identifiers come from a frozen spec constructed at module-load time, so
    f-string interpolation is safe here (no caller-controlled input).
    """
    # f-string interpolation of identifiers is safe: spec fields are frozen
    # module-level constants, never reached by caller input.
    sql = (
        f"SELECT m.idx AS existing_metadata_idx,"
        f" m.value_text, m.value_numeric, m.value_boolean,"
        f" m.value_date, m.value_terminology_term_idx,"
        f" m.value_missing_reason_idx,"
        f" f.study_idx AS contributing_study_idx"
        f" FROM {spec.metadata_table} m"
        f" JOIN {spec.study_field_table} f"
        f" ON f.idx = m.{spec.study_field_idx_column}"
        f" WHERE m.{spec.entity_key_column} = $1"
        f" AND m.global_field_idx = $2"
    )
    row = await conn.fetchrow(sql, entity_idx, global_field_idx)
    if row is None:
        # The partial unique index rejected the INSERT, yet the occupant is
        # gone: a concurrent transaction deleted-and-committed it in the
        # window between the savepoint rollback and this read. The slot is
        # free again — a benign lost race, not schema corruption — so signal
        # a retry rather than masquerading it as an invariant violation.
        raise TransientWriteRaceError(
            row_label=f"{spec.entity_kind}_metadata",
            slot_summary=(
                f"{spec.entity_kind}_idx={entity_idx}, global_field_idx={global_field_idx}"
            ),
        )
    return row


def _make_global_field_collision_error(
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    global_field_idx: int,
    attempted_study_idx: int,
    attempted_value: str | Decimal | date,
    data_type: FieldDataType,
    existing_row: Mapping[str, object],
) -> GlobalFieldSlotOccupiedError:
    """Pick the right GlobalFieldSlotOccupiedError subclass given the
    diagnostic SELECT row and the attempted write's identity.

    Caller is expected to `raise` the returned instance; this function does
    not raise itself so the call site stays readable and the exception is
    constructed without try/except gymnastics.
    """
    # Resolve the value_* column once; reused for both the typed compare
    # and the existing-value extraction below.
    existing_value_column = _resolve_typed_value_column(data_type)
    compare_result = _compare_typed_value(existing_value_column, existing_row, attempted_value)
    contributing_study_idx = existing_row["contributing_study_idx"]
    same_study = contributing_study_idx == attempted_study_idx

    # Common kwargs for whichever subclass fires.
    kwargs = {
        "entity_kind": spec.entity_kind,
        "entity_idx": entity_idx,
        "global_field_idx": global_field_idx,
        "attempted_study_idx": attempted_study_idx,
        "attempted_value": attempted_value,
        "data_type": data_type,
        "existing_metadata_idx": existing_row["existing_metadata_idx"],
        "existing_value": (
            existing_row[existing_value_column] if compare_result != "missing_reason" else None
        ),
        "existing_missing_reason_idx": existing_row["value_missing_reason_idx"],
        "contributing_study_idx": contributing_study_idx,
    }

    # Missing-reason rows trump the same/different axis: no typed value
    # exists to compare against.
    if compare_result == "missing_reason":
        return SlotOccupiedByMissingReasonError(**kwargs)
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

    The {entity}_study_field row carries spec.study_field_global_fk_column
    pointing at the {entity}_global_field row this study_field binds to;
    one parameterised body drives both the biosample and prep_sample
    stacks via spec.study_field_table interpolation.

    Returns (idx, created): created is True when this call inserted the
    row; False when the fallback SELECT branch resolved against a row a
    concurrent caller had already committed (or that pre-existed entirely).

    The created row populates the global FK column and leaves
    data_type / required / terminology_idx / tier_override NULL per the
    *_study_field_inheritance_consistent CHECK; the global field owns
    those fields.

    Raises StudyFieldConflictError(entity_kind=spec.entity_kind, ...) when
    a row at (study_idx, display_name) already exists but is purely-local
    (the global FK column IS NULL) or is bound to a different global
    field. Both cases mean the caller is trying to write metadata against
    a field that is not the global field they think it is; silently
    returning the existing idx would attach the value to the wrong field.

    Concurrency: INSERT ... ON CONFLICT DO NOTHING RETURNING idx + a
    fallback SELECT on miss. Race-free under READ COMMITTED (the project
    default, set in qiita_control_plane.db).
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


async def _insert_typed_metadata(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    study_field_idx: int,
    data_type: FieldDataType,
    value: str | Decimal | date,
    created_by_idx: int,
) -> int:
    """Insert one typed metadata row into spec.metadata_table and return
    its idx.

    Looks up the value column via GLOBAL_METADATA_VALUE_COLUMN and
    populates exactly that one value column; the other five value
    columns stay NULL so the *_metadata_exactly_one_value CHECK is
    satisfied. global_field_idx is populated by trigger from the source
    field row and is not a parameter here.

    The *_metadata_apply_field_contract trigger rejects writes whose
    value column does not match the source field's resolved data_type;
    the *_metadata_reject_if_link_retired trigger rejects writes against
    retired *_to_study links. Both surface as asyncpg.PostgresError
    subclasses. The cross-study partial unique index and the
    unique-per-field constraint surface as asyncpg.UniqueViolationError;
    write_global_metadata_or_diagnose / write_local_metadata_or_diagnose
    drive their diagnostic SELECTs off those.

    BOOLEAN and TERMINOLOGY land in the closed-set fallback and raise
    NotImplementedError so a future addition is a coordinated extension
    across GLOBAL_METADATA_VALUE_COLUMN and this function.
    """
    # Closed-set guard: unsupported data_types raise rather than silently
    # write NULL into every value column.
    value_column = GLOBAL_METADATA_VALUE_COLUMN.get(data_type)
    if value_column is None:
        raise NotImplementedError(
            f"typed metadata insert for data_type={data_type} is not yet implemented"
        )

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
        value,
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
    value: str | Decimal | date,
    caller_idx: int,
) -> SampleMetadataWriteResult:
    """Write one globally-linked metadata row; on cross-study slot collision,
    diagnose the existing occupant and raise a typed exception.

    The flow is: get-or-create the caller's study_field linked to
    global_field_idx, attempt the typed INSERT, and on a UniqueViolationError
    against the partial unique index named in `spec.global_field_unique_index_name`,
    read the existing row and raise one of the GlobalFieldSlotOccupiedError
    subclasses describing which of five sub-cases applies (same- vs different-
    study, same- vs different-value, or slot-held-by-missing-reason).

    Returns a SampleMetadataWriteResult on success (carrying the new
    metadata_idx, the study_field_idx the value was attached to, and
    whether that study_field row was created by this call). The caller
    controls the outer transaction: failure here propagates the typed
    exception up, and any study_field row newly created by the
    get-or-create branch rolls back with the caller's transaction.

    StudyFieldConflictError (raised by the get-or-create on natural-key
    collision against a study_field bound to a different global field)
    and any UniqueViolationError whose constraint_name is NOT
    spec.global_field_unique_index_name (e.g., unique_per_field, accession
    uniqueness) propagate unchanged. TransientWriteRaceError
    propagates when the colliding occupant was concurrently deleted
    before the diagnostic read could inspect it.
    """
    # Fail-fast: the caller must own the transaction so the typed exception
    # rolls back any study_field row this function created before raising.
    require_transaction(conn)

    # Step 1: resolve the caller's study_field bound to global_field_idx,
    # creating one on miss. StudyFieldConflictError propagates if an
    # existing row at (study_idx, display_name) is bound to a different
    # global field.
    study_field_idx, study_field_created = await _get_or_create_globally_linked_study_field(
        conn,
        spec=spec,
        study_idx=study_idx,
        global_field_idx=global_field_idx,
        display_name=display_name,
        created_by_idx=caller_idx,
    )

    # Step 2: attempt the typed INSERT inside a SAVEPOINT.
    #
    # SAVEPOINT around the INSERT. Postgres aborts the entire transaction
    # on any statement error, and every subsequent statement on that
    # transaction fails with InFailedSQLTransactionError until a ROLLBACK
    # or ROLLBACK TO SAVEPOINT. Without this savepoint, the diagnostic
    # SELECT below would fail rather than return the colliding row.
    # asyncpg's nested conn.transaction() issues SAVEPOINT on enter and
    # ROLLBACK TO SAVEPOINT on exception, leaving the outer transaction
    # alive and continuable. The savepoint scope is intentionally just
    # the INSERT; the get-or-create above runs in the outer transaction
    # so the outer rollback that fires when this function's typed
    # exception propagates also undoes any new study_field row created
    # there.
    try:
        async with conn.transaction():
            metadata_idx = await _insert_typed_metadata(
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
    existing_row = await _fetch_global_field_slot_occupant(
        conn,
        spec=spec,
        entity_idx=entity_idx,
        global_field_idx=global_field_idx,
    )
    raise _make_global_field_collision_error(
        spec=spec,
        entity_idx=entity_idx,
        global_field_idx=global_field_idx,
        attempted_study_idx=study_idx,
        attempted_value=value,
        data_type=data_type,
        existing_row=existing_row,
    )


# ---------------------------------------------------------------------------
# Local-write strict-mode and collision exception family
# ---------------------------------------------------------------------------
#
# Local writes target a study-local field row. The schema's
# *_metadata_unique_per_field UNIQUE constraint on
# (entity_idx, study_field_idx) rejects a second write through the same
# study_field; write_local_metadata_or_diagnose diagnoses three sub-cases
# (duplicate value, conflicting value, slot held by a missing-reason row).
# A separate LocalWriteOnGloballyLinkedFieldError fires before the INSERT
# when the get-or-create resolved a study_field that turned out to be
# globally linked — strict-mode rejects silently writing local-typed
# semantics through a global-typed field.
#
# LocalSlotOccupiedError and LocalWriteOnGloballyLinkedFieldError are both
# plain Exceptions, not asyncpg.UniqueViolationError subclasses.
# LocalSlotOccupiedError is raised only after the triggering
# UniqueViolationError has already been caught and the slot occupant
# diagnosed; LocalWriteOnGloballyLinkedFieldError fires pre-INSERT, before
# any unique-constraint violation. Routes catch each with its own `except`
# clause, independent of any `except asyncpg.UniqueViolationError`.
# ---------------------------------------------------------------------------


class LocalWriteOnGloballyLinkedFieldError(Exception):
    """Raised by write_local_metadata_or_diagnose when the get-or-create
    resolved a study_field at (study_idx, display_name) that is currently
    bound to a global field. The caller declared a local-only write but
    the resolved field is globally linked; silently writing through it
    would let the value compete in the cross-study global slot, which is
    the opposite of what a local-only caller asked for. Caller must
    either switch to write_global_metadata_or_diagnose or pick a
    different display_name.
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


class LocalSlotOccupiedError(Exception):
    """Base class: the unique-per-field constraint rejected the write
    because the (entity_idx, study_field_idx) slot is already occupied.
    The concrete subclass names which of three sub-cases applies; all
    subclasses carry the same payload.

    Subclasses with empty bodies are intentional: the discriminator IS
    the type, and `match exc: case LocalDuplicateValueError(): ...` keeps
    route-side response shaping declarative.
    """

    def __init__(
        self,
        *,
        entity_kind: SampleEntityKind,
        entity_idx: int,
        study_idx: int,
        study_field_idx: int,
        display_name: str,
        attempted_value: str | Decimal | date,
        data_type: FieldDataType,
        existing_metadata_idx: int,
        existing_value: str | Decimal | date | None,
        existing_missing_reason_idx: int | None,
    ) -> None:
        self.entity_kind = entity_kind
        self.entity_idx = entity_idx
        self.study_idx = study_idx
        self.study_field_idx = study_field_idx
        self.display_name = display_name
        self.attempted_value = attempted_value
        self.data_type = data_type
        self.existing_metadata_idx = existing_metadata_idx
        self.existing_value = existing_value
        self.existing_missing_reason_idx = existing_missing_reason_idx
        super().__init__(
            f"{entity_kind}_metadata slot ({entity_kind}_idx={entity_idx},"
            f" {entity_kind}_study_field_idx={study_field_idx}) is already"
            f" occupied by {entity_kind}_metadata_idx={existing_metadata_idx}"
        )


class LocalDuplicateValueError(LocalSlotOccupiedError):
    """Existing row's value equals the attempted value. Idempotent
    confirm — no write was performed."""


class LocalConflictingValueError(LocalSlotOccupiedError):
    """Existing row's value differs from the attempted value. The caller
    asked to INSERT but a row already exists; correction requires an
    explicit PATCH or DELETE+INSERT."""


class LocalSlotOccupiedByMissingReasonError(LocalSlotOccupiedError):
    """The slot holds a row recorded as intentionally missing
    (value_missing_reason_idx populated); no typed value to compare
    against."""


# ---------------------------------------------------------------------------
# Local-write diagnostic helpers (private)
# ---------------------------------------------------------------------------


async def _fetch_local_slot_occupant(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    study_field_idx: int,
) -> Mapping[str, object]:
    """Read the existing row occupying the (entity_idx, study_field_idx)
    slot enforced by the *_metadata_unique_per_field UNIQUE constraint.
    Returns all six value columns so the caller can dispatch on data_type
    to pick the right one. value_boolean and value_terminology_term_idx
    are fetched ahead of need: the current classifier cannot surface them,
    but BOOLEAN/TERMINOLOGY support is a planned coordinated extension and
    selecting them now keeps this read stable when that lands.

    Similar to _fetch_global_field_slot_occupant; differs in the WHERE
    key (study_field_idx instead of global_field_idx) and the absence of
    a join (a local row's contributing study is always the caller's, so
    no separate column needs surfacing).
    """
    # f-string interpolation of identifiers is safe: spec fields are frozen
    # module-level constants, never reached by caller input.
    sql = (
        f"SELECT m.idx AS existing_metadata_idx,"
        f" m.value_text, m.value_numeric, m.value_boolean,"
        f" m.value_date, m.value_terminology_term_idx,"
        f" m.value_missing_reason_idx"
        f" FROM {spec.metadata_table} m"
        f" WHERE m.{spec.entity_key_column} = $1"
        f" AND m.{spec.study_field_idx_column} = $2"
    )
    row = await conn.fetchrow(sql, entity_idx, study_field_idx)
    if row is None:
        # The unique-per-field constraint rejected the INSERT, yet the
        # occupant is gone: a concurrent transaction deleted-and-committed
        # it between the savepoint rollback and this read. The slot is free
        # again — a benign lost race, not schema corruption — so signal a
        # retry rather than masquerading it as an invariant violation.
        raise TransientWriteRaceError(
            row_label=f"{spec.entity_kind}_metadata",
            slot_summary=(
                f"{spec.entity_kind}_idx={entity_idx},"
                f" {spec.entity_kind}_study_field_idx={study_field_idx}"
            ),
        )
    return row


def _make_local_collision_error(
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    study_idx: int,
    study_field_idx: int,
    display_name: str,
    attempted_value: str | Decimal | date,
    data_type: FieldDataType,
    existing_row: Mapping[str, object],
) -> LocalSlotOccupiedError:
    """Pick the right LocalSlotOccupiedError subclass given the diagnostic
    SELECT row and the attempted write's identity.

    Caller is expected to `raise` the returned instance; this function
    does not raise itself so the call site stays readable.
    """
    # Resolve the value_* column once; reused for both the typed compare
    # and the existing-value extraction below.
    existing_value_column = _resolve_typed_value_column(data_type)
    compare_result = _compare_typed_value(existing_value_column, existing_row, attempted_value)

    # Common kwargs for whichever subclass fires.
    kwargs = {
        "entity_kind": spec.entity_kind,
        "entity_idx": entity_idx,
        "study_idx": study_idx,
        "study_field_idx": study_field_idx,
        "display_name": display_name,
        "attempted_value": attempted_value,
        "data_type": data_type,
        "existing_metadata_idx": existing_row["existing_metadata_idx"],
        "existing_value": (
            existing_row[existing_value_column] if compare_result != "missing_reason" else None
        ),
        "existing_missing_reason_idx": existing_row["value_missing_reason_idx"],
    }

    # Missing-reason rows trump the same/different axis: no typed value
    # exists to compare against.
    if compare_result == "missing_reason":
        return LocalSlotOccupiedByMissingReasonError(**kwargs)
    if compare_result == "same":
        return LocalDuplicateValueError(**kwargs)
    return LocalConflictingValueError(**kwargs)


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

    Parameterised by spec.study_field_table and
    spec.study_field_global_fk_column; one parameterised body drives
    both the biosample and prep_sample stacks.

    Returns (idx, created, global_field_idx). created is True on the
    insert branch (always a purely-local row, so global_field_idx is
    None), False on the fallback SELECT branch. The third element
    surfaces the resolved row's FK to the global field — None for a
    purely-local row, non-None when the existing row turned out to be
    globally linked (only possible from the lookup branch; the create
    branch always produces a purely-local row).

    Surfacing the link status in the return tuple lets callers that need
    strict local-only semantics (write_local_metadata_or_diagnose) reject
    a globally-linked resolution rather than silently writing through it.

    The create branch produces a purely-local row (FK column NULL);
    creating a globally-linked row is a separate operation
    (_get_or_create_globally_linked_study_field) because its non-null
    inputs are the inverse set per the *_study_field_inheritance_consistent
    CHECK.

    Concurrency: same INSERT ... ON CONFLICT DO NOTHING RETURNING idx +
    fallback SELECT pattern as the globally-linked sibling. Race-free
    under READ COMMITTED (the project default, set in
    qiita_control_plane.db).
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
    value: str | Decimal | date,
    caller_idx: int,
    required: bool = False,
    terminology_idx: int | None = None,
    tier_override: Tier | None = None,
) -> SampleMetadataWriteResult:
    """Write one local (non-globally-linked) metadata row; on collision,
    diagnose the existing occupant and raise a typed exception.

    The flow is: get-or-create the caller's local study_field at
    (study_idx, display_name), reject (LocalWriteOnGloballyLinkedFieldError)
    if the resolved row turns out to be globally linked, attempt the typed
    INSERT, and on a UniqueViolationError against the
    *_metadata_unique_per_field constraint named in
    `spec.local_unique_per_field_index_name`, read the existing row and
    raise one of the LocalSlotOccupiedError subclasses describing which of
    three sub-cases applies (duplicate value, conflicting value, or slot
    held by a missing-reason row).

    `required`, `terminology_idx`, and `tier_override` are forwarded to
    the get-or-create on the create branch; they have no effect when the
    lookup branch returns an existing row.

    Returns a SampleMetadataWriteResult on success (carrying the new
    metadata_idx, the study_field_idx the value was attached to, and
    whether that study_field row was created by this call). The caller
    controls the outer transaction: failure here propagates the typed
    exception up, and any study_field row newly created by the
    get-or-create branch rolls back with the caller's transaction.

    Any UniqueViolationError whose constraint_name is NOT
    spec.local_unique_per_field_index_name propagates unchanged.
    TransientWriteRaceError propagates when the colliding
    occupant was concurrently deleted before the diagnostic read could
    inspect it.
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

    # Step 2: attempt the typed INSERT inside a SAVEPOINT.
    #
    # SAVEPOINT around the INSERT for the same reason as the global-
    # write path: Postgres aborts the entire transaction on any statement
    # error, so the diagnostic SELECT below would fail without one.
    # asyncpg's nested conn.transaction() issues SAVEPOINT on enter and
    # ROLLBACK TO SAVEPOINT on exception, leaving the outer transaction
    # alive and continuable.
    try:
        async with conn.transaction():
            metadata_idx = await _insert_typed_metadata(
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
    existing_row = await _fetch_local_slot_occupant(
        conn,
        spec=spec,
        entity_idx=entity_idx,
        study_field_idx=study_field_idx,
    )
    raise _make_local_collision_error(
        spec=spec,
        entity_idx=entity_idx,
        study_idx=study_idx,
        study_field_idx=study_field_idx,
        display_name=display_name,
        attempted_value=value,
        data_type=data_type,
        existing_row=existing_row,
    )


# ---------------------------------------------------------------------------
# Sample-import composer building blocks
# ---------------------------------------------------------------------------
#
# The biosample and sequenced-prep-sample composers share the same
# transaction-guarded shape: validate the primary/secondary study split,
# preflight the metadata dict against the global-field table, link the
# new entity to every requested study, then write the globally-linked
# metadata rows. The four helpers below capture each shared step in a
# spec-parameterised form so the two composers route through one code
# path and cannot drift on details like dedup or error payload.
# ---------------------------------------------------------------------------


def validate_primary_secondary_studies(
    primary_study_idx: int,
    secondary_study_idxs: Sequence[int],
) -> None:
    """Reject a sample-import call whose primary study also appears in
    secondaries, raises ValueError.
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
) -> list[tuple[GlobalFieldRow, str | Decimal | date]]:
    """Resolve every metadata display_name against spec.global_field_table
    and parse every text value into the typed Python value matching its
    data_type. Returns the resolved (GlobalFieldRow, parsed_value) pairs
    in input order; the caller passes them to write_global_metadata_entries
    to drive the actual INSERTs.

    Raises MetadataUnknownFieldsError(spec.entity_kind, ...) carrying every
    unknown name in one list, before parsing. Raises MetadataParseError on
    first parse failure after the unknown-name check passes. Both errors
    fire before any DB write, so a partial-input request never reaches the
    insert path.
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

    # Parse each text value into its typed Python form; MetadataParseError
    # propagates with the failing display_name on the first bad input.
    parsed: list[tuple[GlobalFieldRow, str | Decimal | date]] = []
    for display_name, text_value in metadata.items():
        global_row = global_field_rows[display_name]
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

    The four retirement columns on both link tables are CHECK-pinned to
    NULL/false on a fresh row so they have no place in a create call;
    created_at defaults to now(). The prep_sample side carries a
    reject_without_biosample_link trigger that fires before INSERT and
    raises asyncpg.RaiseError if the underlying biosample is not linked
    (non-retired) to the same study; the biosample side has no analogous
    trigger.

    Raises asyncpg.UniqueViolationError if the (entity_idx, study_idx)
    pair already exists, asyncpg.ForeignKeyViolationError on bad refs.
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
    parsed_metadata: Sequence[tuple[GlobalFieldRow, str | Decimal | date]],
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
