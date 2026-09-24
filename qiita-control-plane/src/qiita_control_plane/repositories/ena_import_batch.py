"""Repository functions for qiita.ena_import_batch / qiita.ena_import_batch_item.

Writes take an asyncpg.Connection, never acquire their own connection, and
never open a top-level transaction -- `ena_import.batch` owns the per-batch
insert transaction and the state-rollup logic that reads these rows. Reads
take a Pool or a Connection so they compose either way.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg
from qiita_common.models.ena_import import BatchItemState

from . import require_transaction


async def insert_ena_import_batch(
    conn: asyncpg.Connection, *, submitted_by_principal_idx: int
) -> int:
    """Insert one qiita.ena_import_batch row and return its idx."""
    require_transaction(conn)
    return await conn.fetchval(
        "INSERT INTO qiita.ena_import_batch (submitted_by_principal_idx) VALUES ($1) RETURNING idx",
        submitted_by_principal_idx,
    )


async def insert_ena_import_batch_item(
    conn: asyncpg.Connection, *, batch_idx: int, ena_study_accession: str
) -> int:
    """Insert one 'pending' qiita.ena_import_batch_item row and return its idx."""
    require_transaction(conn)
    return await conn.fetchval(
        "INSERT INTO qiita.ena_import_batch_item (batch_idx, ena_study_accession)"
        " VALUES ($1, $2) RETURNING idx",
        batch_idx,
        ena_study_accession,
    )


async def update_ena_import_batch_item_state(
    conn: asyncpg.Connection,
    *,
    item_idx: int,
    state: str,
    failure_reason: str | None = None,
) -> None:
    """Set an item's state and failure_reason (NULL unless passed)."""
    await conn.execute(
        "UPDATE qiita.ena_import_batch_item SET state = $2, failure_reason = $3 WHERE idx = $1",
        item_idx,
        state,
        failure_reason,
    )


async def update_ena_import_batch_item_study_created(
    conn: asyncpg.Connection, *, item_idx: int, study_idx: int
) -> None:
    """Record that this item created `study_idx`.

    Must commit with the study INSERT: an item interrupted between the two would
    leave a study no import is recorded as creating, refused on every re-import.
    """
    require_transaction(conn)
    await conn.execute(
        "UPDATE qiita.ena_import_batch_item SET study_idx = $2, study_created = true"
        " WHERE idx = $1",
        item_idx,
        study_idx,
    )


async def update_ena_import_batch_item_registered(
    conn: asyncpg.Connection,
    *,
    item_idx: int,
    study_idx: int,
    ena_run_outcomes: list[dict[str, Any]],
) -> None:
    """Record a study's registration on its batch item, state -> 'registered'.

    Clears `download_work_ticket_idxs`: the driver re-collects them from the
    pools, so a re-drive drops a ticket it has since replaced. asyncpg has no
    default jsonb codec, so `ena_run_outcomes` is written as a string + `::jsonb`.
    """
    await conn.execute(
        "UPDATE qiita.ena_import_batch_item"
        " SET state = $2, study_idx = $3, ena_run_outcomes = $4::jsonb, failure_reason = NULL,"
        "     download_work_ticket_idxs = '{}'"
        " WHERE idx = $1",
        item_idx,
        BatchItemState.REGISTERED.value,
        study_idx,
        json.dumps(ena_run_outcomes),
    )


async def append_ena_import_batch_item_download_ticket(
    conn: asyncpg.Connection, *, item_idx: int, ticket_idx: int
) -> None:
    """Append `ticket_idx` to the item's download_work_ticket_idxs if absent.

    The CASE/ANY guard makes this idempotent -- a re-drive that reuses an
    existing ticket does not duplicate the idx.
    """
    await conn.execute(
        "UPDATE qiita.ena_import_batch_item"
        " SET download_work_ticket_idxs = CASE"
        "   WHEN $2 = ANY(download_work_ticket_idxs) THEN download_work_ticket_idxs"
        "   ELSE array_append(download_work_ticket_idxs, $2) END"
        " WHERE idx = $1",
        item_idx,
        ticket_idx,
    )


async def ena_import_created_study(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection, study_idx: int
) -> bool:
    """True iff some qiita.ena_import_batch_item created `study_idx`.

    A batch deleted after its import (CASCADE) takes its items' rows with it,
    so this reads False again for a study whose creating item no longer exists.
    """
    return await pool_or_conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM qiita.ena_import_batch_item"
        " WHERE study_idx = $1 AND study_created)",
        study_idx,
    )


async def fetch_sequenced_pool_download_states(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    sequencing_run_idx: int,
    action_id: str,
    action_version: str,
) -> list[asyncpg.Record]:
    """Every sequenced_pool on `sequencing_run_idx`, oldest first, with
    `has_sequenced_sample` (any active one) and its latest ticket for the action
    (`work_ticket_idx` / `work_ticket_state`, NULL when it has none)."""
    return await pool_or_conn.fetch(
        "SELECT sp.idx AS sequenced_pool_idx,"
        "       EXISTS (SELECT 1 FROM qiita.sequenced_sample ss"
        "               JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx"
        "               WHERE ss.sequenced_pool_idx = sp.idx AND ps.retired = false)"
        "         AS has_sequenced_sample,"
        "       wt.work_ticket_idx, wt.state::text AS work_ticket_state"
        " FROM qiita.sequenced_pool sp"
        " LEFT JOIN LATERAL ("
        "   SELECT work_ticket_idx, state FROM qiita.work_ticket"
        "   WHERE action_id = $2 AND action_version = $3 AND sequenced_pool_idx = sp.idx"
        "   ORDER BY work_ticket_idx DESC LIMIT 1"
        " ) wt ON true"
        " WHERE sp.sequencing_run_idx = $1"
        " ORDER BY sp.idx",
        sequencing_run_idx,
        action_id,
        action_version,
    )


async def fetch_inflight_ena_import_batch_items(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
) -> list[asyncpg.Record]:
    """Every ena_import_batch_item row still pending/resolving/registered, with
    its batch's submitting principal, ordered by (batch_idx, idx)."""
    return await pool_or_conn.fetch(
        "SELECT bi.idx, bi.ena_study_accession, bi.batch_idx,"
        "       b.submitted_by_principal_idx"
        " FROM qiita.ena_import_batch_item bi"
        " JOIN qiita.ena_import_batch b ON b.idx = bi.batch_idx"
        " WHERE bi.state = ANY($1::text[])"
        " ORDER BY bi.batch_idx, bi.idx",
        [
            BatchItemState.PENDING.value,
            BatchItemState.RESOLVING.value,
            BatchItemState.REGISTERED.value,
        ],
    )


async def ena_import_batch_exists(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection, batch_idx: int
) -> bool:
    """True iff qiita.ena_import_batch has a row with the given idx."""
    return (
        await pool_or_conn.fetchval(
            "SELECT 1 FROM qiita.ena_import_batch WHERE idx = $1", batch_idx
        )
        is not None
    )


async def fetch_ena_import_batch_items(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection, batch_idx: int
) -> list[asyncpg.Record]:
    """All qiita.ena_import_batch_item rows for `batch_idx`, ordered by idx."""
    return await pool_or_conn.fetch(
        "SELECT idx, ena_study_accession, state, failure_reason, study_idx,"
        "       download_work_ticket_idxs, ena_run_outcomes"
        " FROM qiita.ena_import_batch_item"
        " WHERE batch_idx = $1"
        " ORDER BY idx",
        batch_idx,
    )


async def fetch_work_ticket_states_for_idxs(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection, ticket_idxs: list[int]
) -> list[asyncpg.Record]:
    """(work_ticket_idx, state) rows for the given qiita.work_ticket idxs."""
    return await pool_or_conn.fetch(
        "SELECT work_ticket_idx, state FROM qiita.work_ticket"
        " WHERE work_ticket_idx = ANY($1::bigint[])",
        ticket_idxs,
    )
