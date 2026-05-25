"""Biosample-side metadata: the entity-specific spec consumed by the
shared writers, plus the inserter for the purely-local
owner-biosample-id row (the one metadata row not handled through that
spec) and its associated structured exceptions.

Functions take an asyncpg.Connection as their first positional argument,
never acquire their own connection, and never open their own top-level
transaction; the caller controls transaction scope.
"""

import asyncpg

from ._sample_helpers import (
    EntityMetadataSpec,
    SampleEntityKind,
)

# ---------------------------------------------------------------------------
# Structured exceptions
# ---------------------------------------------------------------------------


class BiosampleOwnerIdFieldCollisionError(Exception):
    """Raised when a metadata entry's display_name collides with the
    owner_biosample_id_field_name passed by the caller. The
    owner-biosample-id row is purely-local and flagged; the same
    display_name cannot also identify a globally-linked metadata entry.
    """

    def __init__(self, display_name: str) -> None:
        self.display_name = display_name
        super().__init__(
            f"metadata key {display_name!r} collides with owner_biosample_id_field_name"
        )


class BiosampleOwnerIdMissingValueError(Exception):
    """Raised when the owner_biosample_id_value text matches the name of a
    qiita.missing_value_reason row. The owner-biosample-id row carries an
    identifier (PII) for the biosample; an intentionally-missing marker is
    incompatible with that contract — the field cannot be both "the
    sample's owner-side identifier" and "no value was given."
    """

    def __init__(self, owner_biosample_id_value: str, reason_idx: int) -> None:
        self.owner_biosample_id_value = owner_biosample_id_value
        self.reason_idx = reason_idx
        super().__init__(
            f"owner_biosample_id_value {owner_biosample_id_value!r} matches"
            f" missing_value_reason idx={reason_idx}; the owner-biosample-id"
            f" field cannot hold a missing-value marker"
        )


async def insert_owner_biosample_id_metadata(
    conn: asyncpg.Connection,
    *,
    biosample_idx: int,
    biosample_study_field_idx: int,
    value_text: str,
    created_by_idx: int,
) -> int:
    """Insert the purely-local owner-biosample-id metadata row and return
    its idx. The is_owner_biosample_id flag is set TRUE as a SQL literal
    because this function exclusively writes that flagged row; non-flagged
    rows go through a different writer.
    """
    # global_field_idx is filled in by a DB trigger from the referenced
    # study-field row; the other four value_* columns stay NULL.
    return await conn.fetchval(
        "INSERT INTO qiita.biosample_metadata ("
        "    biosample_idx, biosample_study_field_idx,"
        "    value_text, is_owner_biosample_id, created_by_idx"
        ") VALUES ($1, $2, $3, TRUE, $4)"
        " RETURNING idx",
        biosample_idx,
        biosample_study_field_idx,
        value_text,
        created_by_idx,
    )


# ---------------------------------------------------------------------------
# Biosample EntityMetadataSpec
# ---------------------------------------------------------------------------

BIOSAMPLE_METADATA_SPEC = EntityMetadataSpec(
    entity_kind=SampleEntityKind.BIOSAMPLE,
    metadata_table="qiita.biosample_metadata",
    global_field_table="qiita.biosample_global_field",
    entity_key_column="biosample_idx",
    study_field_table="qiita.biosample_study_field",
    study_field_idx_column="biosample_study_field_idx",
    study_field_global_fk_column="biosample_global_field_idx",
    global_field_unique_index_name="biosample_metadata_one_value_per_global_field",
    local_unique_per_field_index_name="biosample_metadata_unique_per_field",
    link_table="qiita.biosample_to_study",
    link_entity_key_column="biosample_idx",
)
