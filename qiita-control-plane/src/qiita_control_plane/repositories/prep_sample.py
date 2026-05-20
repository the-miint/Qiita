"""Single-table writer for the qiita.prep_sample supertype.

Covers the supertype row insert. The per-study link table
(qiita.prep_sample_to_study) is written through the shared
insert_entity_to_study / link_entity_to_studies helpers in
_sample_helpers, driven by PREP_SAMPLE_METADATA_SPEC. The
sequencing-pathway subtype rows and the per-sample composer that ties
the supertype, subtype, study links, and metadata together live in the
sibling sequenced_sample module.

Write functions take an asyncpg.Connection as their first positional
argument, never acquire their own connection, and never open their own
top-level transaction; the caller controls transaction scope so multiple
calls compose atomically on one connection.
"""

import asyncpg


async def insert_prep_sample(
    conn: asyncpg.Connection,
    *,
    biosample_idx: int,
    owner_idx: int,
    prep_protocol_idx: int,
    processing_kind: str,
    created_by_idx: int,
    metadata_checklist_idx: int | None = None,
) -> int:
    """Insert a row into qiita.prep_sample and return the generated idx.

    Exposes every column the caller may legitimately set on a fresh row:
    the three NOT-NULL FKs (biosample, owner, prep_protocol), the
    processing_kind enum, the optional metadata_checklist link, and the
    creator principal. Subtype-only columns (sequencing-pool linkage, ENA
    accessions, submission tracking) live on the subtype tables and have
    no place in a supertype create call. Retirement and metadata-touch
    timestamps are populated by triggers, defaults, or schema CHECKs.

    Raises asyncpg.PostgresError on FK violation or constraint failure.
    """
    # Single INSERT carrying every supertype caller-settable column.
    return await conn.fetchval(
        "INSERT INTO qiita.prep_sample ("
        "    biosample_idx, owner_idx, prep_protocol_idx, metadata_checklist_idx,"
        "    processing_kind, created_by_idx"
        ") VALUES ($1, $2, $3, $4, $5::qiita.processing_kind, $6)"
        " RETURNING idx",
        biosample_idx,
        owner_idx,
        prep_protocol_idx,
        metadata_checklist_idx,
        processing_kind,
        created_by_idx,
    )
