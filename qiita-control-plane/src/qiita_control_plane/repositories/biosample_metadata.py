"""Biosample arm of the shared metadata stack.

Holds BIOSAMPLE_METADATA_SPEC (consumed by the cross-entity helpers in
_sample_helpers) plus the biosample-only owner-id-flagged metadata
inserter and the owner-id-collision exception, neither of which has a
prep_sample analogue. The typed-value INSERT path for every non-owner-id
metadata row is driven by this spec via the shared writers.

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
# Structured exceptions raised by the import path
# ---------------------------------------------------------------------------
# BiosampleOwnerIdFieldCollisionError has no prep_sample analog and stays
# in this entity-specific module rather than the cross-entity helpers.


class BiosampleOwnerIdFieldCollisionError(Exception):
    """Raised when import metadata carries an entry whose key equals the
    request's owner_biosample_id_field_name. The owner-biosample-id row is
    purely-local and flagged; allowing the same display_name as a globally
    linked metadata entry would conflict with that contract.
    """

    def __init__(self, display_name: str) -> None:
        self.display_name = display_name
        super().__init__(
            f"metadata key {display_name!r} collides with owner_biosample_id_field_name"
        )


async def insert_owner_biosample_id_metadata(
    conn: asyncpg.Connection,
    *,
    biosample_idx: int,
    biosample_study_field_idx: int,
    value_text: str,
    created_by_idx: int,
) -> int:
    """Insert the purely-local owner-biosample-id metadata row, flagged
    is_owner_biosample_id=TRUE in the database, and return its idx.

    Writes one specific kind of row — the PII-tier-pinned owner-id record
    the biosample composer attaches in Step e. is_owner_biosample_id is a
    SQL literal here (not a Python parameter) because the function's
    name carries that contract: every row this writes is the flagged
    one. Non-owner-id metadata rows use the shared typed-value writer
    driven by BIOSAMPLE_METADATA_SPEC.

    The biosample_metadata_unique_owner_biosample_id partial unique index
    rejects a second is_owner_biosample_id=TRUE row for the same
    biosample. The biosample_metadata_reject_if_link_retired trigger
    rejects writes against retired biosample_to_study links. Both
    surface as asyncpg.PostgresError subclasses. global_field_idx is
    populated by trigger from the source field row; the other five
    value columns stay NULL so biosample_metadata_exactly_one_value is
    satisfied.
    """
    # Single INSERT; value_text is the only value column populated and
    # is_owner_biosample_id is the SQL literal TRUE.
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
# EntityMetadataSpec for biosample (consumed by _sample_helpers shared writers)
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
