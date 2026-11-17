"""Reads of `qiita.definition_settings_edit`, the audit of a definition's settings
edits (`definition_settings` writes it)."""

import asyncpg
from qiita_common.models import DefinitionKind, DefinitionSettingsEditKind


async def fetch_settings_edit_since_ticket(
    conn: asyncpg.Pool | asyncpg.Connection,
    work_ticket_idx: int,
    *,
    definition: tuple[DefinitionKind, int] | None = None,
) -> asyncpg.Record | None:
    """The earliest admin edit made after the ticket was created, as
    (definition_kind, definition_ref, occurred_at), or None. With `definition`, of
    that (kind, idx) only, so an edit of another definition the ticket records does
    not stand in for it; without, of any definition the ticket records (`mask_idx`,
    `alignment_idx` or `processing_idx`).

    Only an admin edit counts. A mask re-key (`ADAPTER_HASH_REKEY`) moves
    `adapter_set_hash` onto another derivation of the same adapter set
    (`backfill/mask_adapter_hash.py`): the adapters a run filtered with are
    unchanged, so a re-key does not fail a resume."""
    if definition is None:
        match = (
            "((e.definition_kind = $3 AND e.definition_ref = wt.mask_idx)"
            " OR (e.definition_kind = $4 AND e.definition_ref = wt.alignment_idx)"
            " OR (e.definition_kind = $5 AND e.definition_ref = wt.processing_idx))"
        )
        args: tuple = (
            DefinitionKind.MASK.value,
            DefinitionKind.ALIGNMENT.value,
            DefinitionKind.PROCESSING.value,
        )
    else:
        kind, definition_idx = definition
        match = "e.definition_kind = $3 AND e.definition_ref = $4"
        args = (kind.value, definition_idx)
    return await conn.fetchrow(
        "SELECT e.definition_kind, e.definition_ref, e.occurred_at"
        "  FROM qiita.work_ticket wt"
        "  JOIN qiita.definition_settings_edit e"
        "    ON e.edit_kind = $2 AND e.occurred_at > wt.created_at"
        f"   AND {match}"
        " WHERE wt.work_ticket_idx = $1"
        " ORDER BY e.occurred_at, e.definition_settings_edit_idx"
        " LIMIT 1",
        work_ticket_idx,
        DefinitionSettingsEditKind.ADMIN_EDIT.value,
        *args,
    )
