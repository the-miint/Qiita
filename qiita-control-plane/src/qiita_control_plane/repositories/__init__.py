"""Repository modules holding the SQL for each resource.

Cross-module helpers shared by sibling repositories also live here:
require_transaction (the composer-boundary guard), validate_patch_fields
(the PATCH composer-boundary input guard), GlobalFieldRow (the
*_global_field lookup row shape), GlobalMetadataRow plus
GLOBAL_METADATA_VALUE_COLUMN (the globally-linked metadata read shape
plus the data_type -> value-column dispatch dict), MetadataParseError /
MetadataUnknownFieldsError / StudyFieldConflictError (the structured
exceptions raised by the metadata import paths), and the
text-to-typed-value coercion parse_text_for_data_type used by both the
biosample and prep_sample import paths.

Each shared exception class takes a SampleEntityKind member that the
caller passes at the raise site so the message names the domain. Routes
catch the shared class directly; they do not need to discriminate on
entity_kind because each route already knows its own domain.
"""

from datetime import date
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import NamedTuple

import asyncpg
from qiita_common.models import FieldDataType


class SampleEntityKind(StrEnum):
    """Discriminator passed to the shared metadata exceptions so each error
    message names its domain. Values match the table-name prefix used
    throughout the schema (biosample_*, prep_sample_*), so the f-string
    interpolation in the exception messages reads naturally.
    """

    BIOSAMPLE = "biosample"
    PREP_SAMPLE = "prep_sample"


def require_transaction(conn: asyncpg.Connection) -> None:
    """Raise RuntimeError if conn is not currently inside a transaction.

    Use as the first call in any repository function whose writes must roll
    back atomically on partial failure. asyncpg has no static type that
    expresses the transactional contract, so this is enforced at runtime;
    the offending function appears in the traceback frame above.
    """
    if not conn.is_in_transaction():
        raise RuntimeError(
            "repository function called outside a transaction;"
            " wrap the call in `async with conn.transaction(): ...`"
            " — its writes must roll back atomically on partial failure."
        )


def validate_patch_fields(
    fields: dict[str, object],
    *,
    allowlist: frozenset[str],
    repo_name: str,
) -> None:
    """Reject empty / unknown-column PATCH inputs at the repo boundary.

    Every update_X composer that takes a column-keyed `fields` dict shares
    the same two failure modes: an empty dict yields an UPDATE with no
    SET clause (SQL error), and an unknown column name reaches the f-string
    SQL builder. Both are misuse at the repo boundary and surface as
    ValueError with messages naming `repo_name` so the traceback points
    at the offending composer. The route layer's Pydantic extra="forbid"
    already covers unknown columns from external callers; this guard
    catches bypass paths (tests, future internal callers).
    """
    # Empty dict — the SQL builder would emit an UPDATE with no SET clause.
    if not fields:
        raise ValueError(f"{repo_name} requires at least one field")

    # Unknown column name — the SQL builder would interpolate it into SET.
    unknown = set(fields) - allowlist
    if unknown:
        raise ValueError(f"{repo_name} rejects non-patchable column(s): {sorted(unknown)}")


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
    or globally linked to a different concept than the one the caller
    requested.
    """

    def __init__(
        self,
        entity_kind: SampleEntityKind,
        study_idx: int,
        display_name: str,
        expected_global_field_idx: int,
        found_global_field_idx: int | None,
    ) -> None:
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
