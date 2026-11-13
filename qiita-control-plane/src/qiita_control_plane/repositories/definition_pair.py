"""SQL for `qiita.definition_pair` / `qiita.definition_pair_event`, the reads of
masks, alignments and processings that the group rules take as input, and the group
lock.

The rules themselves (what a change is, how groups form, what is refused) are in
`qiita_control_plane.definition_similarity`; nothing here decides anything.
"""

import asyncio
import json
from dataclasses import dataclass
from typing import Any

import asyncpg
from qiita_common.models import (
    AlignmentDefinitionStatus,
    DefinitionKind,
    DefinitionPairEventType,
    DefinitionPairVerdict,
    MaskDefinitionStatus,
    ProcessingStatus,
)

from . import require_transaction

# pg_advisory_xact_lock(class, key) class for the group lock; allocated in
# repositories/__init__'s registry, distinct from sequencing_run's, sequence_range's
# and fanout_dispatch's. One key: the lock covers every kind's groups at once.
DEFINITION_GROUP_LOCK_CLASS = 0x0DEF_0001
DEFINITION_GROUP_LOCK_KEY = 0

# The exclusive side is taken by try-lock in a loop and never queued: a queued
# exclusive request blocks every shared request made after it, so one decision
# waiting behind a long reader would stall every other reader too. 20 tries 50 ms
# apart give up after about one second, under the CLI's HTTP timeout.
GROUP_LOCK_EXCLUSIVE_ATTEMPTS = 20
GROUP_LOCK_EXCLUSIVE_INTERVAL_S = 0.05


class GroupLockBusy(Exception):
    """The exclusive group lock was not free within the bounded retry. The same
    request can be retried."""

    def __init__(self) -> None:
        super().__init__("definition groups are being read by another transaction; retry shortly")


async def lock_groups_shared(conn: asyncpg.Connection) -> None:
    """Take the group lock SHARED until the transaction ends.

    A deprecation takes this before reading the `similar` pairs that name its
    definition and before any definition row lock: no decision commits while it is
    held, so a `similar` decision and the deprecation of one of its sides cannot
    both commit. It waits for a decision in progress, in a READ COMMITTED
    transaction whose next statement then sees that decision. Decisions never queue
    behind it (`lock_groups_exclusive`)."""
    require_transaction(conn)
    await conn.execute(
        "SELECT pg_advisory_xact_lock_shared($1, $2)",
        DEFINITION_GROUP_LOCK_CLASS,
        DEFINITION_GROUP_LOCK_KEY,
    )


async def lock_groups_exclusive(conn: asyncpg.Connection) -> None:
    """Take the group lock EXCLUSIVE until the transaction ends, or raise
    GroupLockBusy.

    Tries `GROUP_LOCK_EXCLUSIVE_ATTEMPTS` times, `GROUP_LOCK_EXCLUSIVE_INTERVAL_S`
    apart, with `pg_try_advisory_xact_lock`; the reason it never waits in the
    lock queue is on those constants. Read at call time, so a test can shorten
    them."""
    require_transaction(conn)
    for attempt in range(GROUP_LOCK_EXCLUSIVE_ATTEMPTS):
        if await conn.fetchval(
            "SELECT pg_try_advisory_xact_lock($1, $2)",
            DEFINITION_GROUP_LOCK_CLASS,
            DEFINITION_GROUP_LOCK_KEY,
        ):
            return
        if attempt + 1 < GROUP_LOCK_EXCLUSIVE_ATTEMPTS:
            await asyncio.sleep(GROUP_LOCK_EXCLUSIVE_INTERVAL_S)
    raise GroupLockBusy()


# ---------------------------------------------------------------------------
# Definitions
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DefinitionRow:
    """What the group rules read of one definition. `workflow` is the mask's
    filter_workflow or the processing's workflow; an alignment has none outside
    its params."""

    kind: DefinitionKind
    idx: int
    params: dict[str, Any]
    deprecated: bool
    workflow: str | None


# Per kind: the table, its id column, the column the comparison scope reads, and
# the kind's deprecated status. The first three are interpolated into SQL, so the
# map is closed.
_DEFINITION_TABLES: dict[DefinitionKind, tuple[str, str, str, str]] = {
    DefinitionKind.MASK: (
        "qiita.mask_definition",
        "mask_idx",
        "filter_workflow",
        MaskDefinitionStatus.DEPRECATED.value,
    ),
    DefinitionKind.ALIGNMENT: (
        "qiita.alignment_definition",
        "alignment_idx",
        "NULL::text",
        AlignmentDefinitionStatus.DEPRECATED.value,
    ),
    DefinitionKind.PROCESSING: (
        "qiita.processing",
        "processing_idx",
        "workflow",
        ProcessingStatus.DEPRECATED.value,
    ),
}


def _decode(value: Any) -> Any:
    """A JSONB column, decoded: asyncpg returns it as str under the default codec,
    or already decoded when a codec is registered."""
    return json.loads(value) if isinstance(value, str) else value


def _definition_row(kind: DefinitionKind, row: asyncpg.Record) -> DefinitionRow:
    return DefinitionRow(
        kind=kind,
        idx=row["idx"],
        params=_decode(row["params"]),
        deprecated=row["status"] == _DEFINITION_TABLES[kind][3],
        workflow=row["workflow"],
    )


async def fetch_definitions(conn: asyncpg.Connection) -> list[DefinitionRow]:
    """Every mask, alignment and processing definition, deprecated ones included."""
    out: list[DefinitionRow] = []
    for kind, (table, id_col, workflow_col, _) in _DEFINITION_TABLES.items():
        rows = await conn.fetch(
            f"SELECT {id_col} AS idx, params, status, {workflow_col} AS workflow FROM {table}"
        )
        out.extend(_definition_row(kind, r) for r in rows)
    return out


async def fetch_definition(
    conn: asyncpg.Connection, kind: DefinitionKind, idx: int
) -> DefinitionRow | None:
    """One definition of `kind`, or None when no row has `idx`."""
    table, id_col, workflow_col, _ = _DEFINITION_TABLES[kind]
    row = await conn.fetchrow(
        f"SELECT {id_col} AS idx, params, status, {workflow_col} AS workflow"
        f" FROM {table} WHERE {id_col} = $1",
        idx,
    )
    return None if row is None else _definition_row(kind, row)


# ---------------------------------------------------------------------------
# Pairs
# ---------------------------------------------------------------------------

_PAIR_COLUMNS = (
    "definition_pair_idx, definition_kind,"
    " low_definition_ref AS low_definition_idx, high_definition_ref AS high_definition_idx,"
    " detached_at, change, derived_only, verdict, decided_by_idx, decided_at,"
    " decision_reason, opened_by_idx, opened_at"
)

_TYPED_COLUMNS: dict[DefinitionKind, tuple[str, str]] = {
    DefinitionKind.MASK: ("low_mask_idx", "high_mask_idx"),
    DefinitionKind.ALIGNMENT: ("low_alignment_idx", "high_alignment_idx"),
    DefinitionKind.PROCESSING: ("low_processing_idx", "high_processing_idx"),
}


def pair_record_fields(row: asyncpg.Record) -> dict[str, Any]:
    """A pair row as the `DefinitionPair` wire model's fields."""
    return {**dict(row), "change": _decode(row["change"])}


async def insert_pair(
    conn: asyncpg.Connection,
    *,
    kind: DefinitionKind,
    low_idx: int,
    high_idx: int,
    change: dict[str, list[Any]],
    derived_only: bool,
    opened_by_idx: int,
) -> tuple[asyncpg.Record, bool]:
    """Open a pending pair and its `opened` event, or return the pair already
    recorded for these two definitions. Returns `(row, created)`."""
    require_transaction(conn)
    low_col, high_col = _TYPED_COLUMNS[kind]
    row = await conn.fetchrow(
        "INSERT INTO qiita.definition_pair ("
        f"  definition_kind, {low_col}, {high_col}, low_definition_ref, high_definition_ref,"
        "  change, derived_only, opened_by_idx"
        ") VALUES ($1, $2, $3, $2, $3, $4::jsonb, $5, $6)"
        " ON CONFLICT DO NOTHING"
        f" RETURNING {_PAIR_COLUMNS}",
        kind.value,
        low_idx,
        high_idx,
        json.dumps(change),
        derived_only,
        opened_by_idx,
    )
    if row is None:
        existing = await conn.fetchrow(
            f"SELECT {_PAIR_COLUMNS} FROM qiita.definition_pair"
            " WHERE definition_kind = $1 AND low_definition_ref = $2 AND high_definition_ref = $3",
            kind.value,
            low_idx,
            high_idx,
        )
        return existing, False
    await _insert_event(
        conn,
        definition_pair_idx=row["definition_pair_idx"],
        event=DefinitionPairEventType.OPENED,
        verdict=DefinitionPairVerdict.PENDING,
        actor_principal_idx=opened_by_idx,
        reason=None,
    )
    return row, True


async def fetch_pair(
    conn: asyncpg.Pool | asyncpg.Connection, definition_pair_idx: int, *, for_update: bool = False
) -> asyncpg.Record | None:
    """One pair, optionally locked FOR UPDATE (inside a transaction)."""
    if for_update:
        if isinstance(conn, asyncpg.Pool):
            raise RuntimeError("a row lock needs a connection inside a transaction")
        require_transaction(conn)
    return await conn.fetchrow(
        f"SELECT {_PAIR_COLUMNS} FROM qiita.definition_pair WHERE definition_pair_idx = $1"
        + (" FOR UPDATE" if for_update else ""),
        definition_pair_idx,
    )


async def list_pairs(
    conn: asyncpg.Pool | asyncpg.Connection,
    *,
    kinds: list[DefinitionKind],
    verdict: DefinitionPairVerdict | None,
    limit: int,
) -> list[asyncpg.Record]:
    """Pairs of the given kinds, optionally of one verdict, oldest first."""
    return list(
        await conn.fetch(
            f"SELECT {_PAIR_COLUMNS} FROM qiita.definition_pair"
            " WHERE definition_kind = ANY($1::text[])"
            "   AND ($2::text IS NULL OR verdict = $2)"
            " ORDER BY definition_pair_idx LIMIT $3",
            [k.value for k in kinds],
            None if verdict is None else verdict.value,
            limit,
        )
    )


async def fetch_live_pairs(conn: asyncpg.Connection) -> list[asyncpg.Record]:
    """Every pair that is not detached, of every kind and verdict."""
    return list(
        await conn.fetch(
            f"SELECT {_PAIR_COLUMNS} FROM qiita.definition_pair WHERE detached_at IS NULL"
        )
    )


async def fetch_pair_events(
    conn: asyncpg.Pool | asyncpg.Connection, definition_pair_idx: int
) -> list[asyncpg.Record]:
    """A pair's history, oldest first."""
    return list(
        await conn.fetch(
            "SELECT definition_pair_event_idx, event, verdict, actor_principal_idx, reason,"
            " occurred_at FROM qiita.definition_pair_event"
            " WHERE definition_pair_idx = $1 ORDER BY definition_pair_event_idx",
            definition_pair_idx,
        )
    )


async def fetch_similar_pairs_naming(
    conn: asyncpg.Connection, kind: DefinitionKind, idx: int
) -> list[int]:
    """The live `similar` pairs of `kind` with `idx` on either side."""
    return [
        r["definition_pair_idx"]
        for r in await conn.fetch(
            "SELECT definition_pair_idx FROM qiita.definition_pair"
            " WHERE definition_kind = $1 AND verdict = $3 AND detached_at IS NULL"
            "   AND $2 IN (low_definition_ref, high_definition_ref)"
            " ORDER BY definition_pair_idx",
            kind.value,
            idx,
            DefinitionPairVerdict.SIMILAR.value,
        )
    ]


async def record_decision(
    conn: asyncpg.Connection,
    *,
    definition_pair_idx: int,
    verdict: DefinitionPairVerdict,
    reason: str,
    principal_idx: int,
) -> asyncpg.Record:
    """Write a decision onto the pair and its `decided` event; return the pair."""
    require_transaction(conn)
    row = await conn.fetchrow(
        "UPDATE qiita.definition_pair"
        "   SET verdict = $2, decided_by_idx = $3, decided_at = now(), decision_reason = $4"
        " WHERE definition_pair_idx = $1"
        f" RETURNING {_PAIR_COLUMNS}",
        definition_pair_idx,
        verdict.value,
        principal_idx,
        reason,
    )
    await _insert_event(
        conn,
        definition_pair_idx=definition_pair_idx,
        event=DefinitionPairEventType.DECIDED,
        verdict=verdict,
        actor_principal_idx=principal_idx,
        reason=reason,
    )
    return row


async def record_revoke(
    conn: asyncpg.Connection, *, definition_pair_idx: int, reason: str, principal_idx: int
) -> asyncpg.Record:
    """Return the pair to pending, clearing its decision, and write the `revoked`
    event; return the pair."""
    require_transaction(conn)
    row = await conn.fetchrow(
        "UPDATE qiita.definition_pair"
        "   SET verdict = $2, decided_by_idx = NULL, decided_at = NULL,"
        "       decision_reason = NULL"
        " WHERE definition_pair_idx = $1"
        f" RETURNING {_PAIR_COLUMNS}",
        definition_pair_idx,
        DefinitionPairVerdict.PENDING.value,
    )
    await _insert_event(
        conn,
        definition_pair_idx=definition_pair_idx,
        event=DefinitionPairEventType.REVOKED,
        verdict=DefinitionPairVerdict.PENDING,
        actor_principal_idx=principal_idx,
        reason=reason,
    )
    return row


async def _insert_event(
    conn: asyncpg.Connection,
    *,
    definition_pair_idx: int,
    event: DefinitionPairEventType,
    verdict: DefinitionPairVerdict,
    actor_principal_idx: int,
    reason: str | None,
) -> None:
    await conn.execute(
        "INSERT INTO qiita.definition_pair_event"
        " (definition_pair_idx, event, verdict, actor_principal_idx, reason)"
        " VALUES ($1, $2, $3, $4, $5)",
        definition_pair_idx,
        event.value,
        verdict.value,
        actor_principal_idx,
        reason,
    )
