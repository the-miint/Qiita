"""Reads of `qiita.definition_settings_edit`, the audit of a definition's settings
edits (`definition_settings` writes it)."""

import asyncpg
from qiita_common.models import DefinitionKind

# `edit_kind` of an audit row written by `qiita.edit_definition_settings` (the admin
# edit). The other value, `adapter_hash_rekey`, is the lock trigger's record of a mask
# re-key, which moves `adapter_set_hash` onto another derivation of the same adapter
# set (`backfill/mask_adapter_hash.py`): the adapters a run filtered with are
# unchanged, so a re-key does not fail a resume.
ADMIN_EDIT = "admin_edit"


async def fetch_settings_edit_since_ticket(
    conn: asyncpg.Pool | asyncpg.Connection, work_ticket_idx: int
) -> asyncpg.Record | None:
    """The earliest admin edit of a definition this ticket records (`mask_idx`,
    `alignment_idx` or `processing_idx`) made after the ticket was created, as
    (definition_kind, definition_ref, occurred_at), or None."""
    return await conn.fetchrow(
        "SELECT e.definition_kind, e.definition_ref, e.occurred_at"
        "  FROM qiita.work_ticket wt"
        "  JOIN qiita.definition_settings_edit e"
        "    ON e.edit_kind = $2 AND e.occurred_at > wt.created_at"
        "   AND ((e.definition_kind = $3 AND e.definition_ref = wt.mask_idx)"
        "     OR (e.definition_kind = $4 AND e.definition_ref = wt.alignment_idx)"
        "     OR (e.definition_kind = $5 AND e.definition_ref = wt.processing_idx))"
        " WHERE wt.work_ticket_idx = $1"
        " ORDER BY e.occurred_at, e.definition_settings_edit_idx"
        " LIMIT 1",
        work_ticket_idx,
        ADMIN_EDIT,
        DefinitionKind.MASK.value,
        DefinitionKind.ALIGNMENT.value,
        DefinitionKind.PROCESSING.value,
    )
