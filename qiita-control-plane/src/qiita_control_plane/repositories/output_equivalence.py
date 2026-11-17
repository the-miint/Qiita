"""SQL for `qiita.output_equivalence` / `qiita.output_equivalence_event`, the reads of
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
    MaskDefinitionStatus,
    OutputEquivalenceEventType,
    OutputEquivalenceVerdict,
    ProcessingStatus,
)

from . import READ_COMMITTED, require_transaction, transaction_isolation

# pg_advisory_xact_lock(class, key) class for the group lock; allocated in
# repositories/__init__'s registry, distinct from sequencing_run's, sequence_range's
# and fanout_dispatch's. One key: the lock covers every kind's groups at once.
DEFINITION_GROUP_LOCK_CLASS = 0x0DEF_0001
DEFINITION_GROUP_LOCK_KEY = 0

# The exclusive side is taken by try-lock in a loop and never queued: a queued
# exclusive request blocks every shared request made after it, so one decision
# waiting behind a deprecation would stall every later deprecation too. 20 tries 50 ms
# apart give up after about one second, under the CLI's HTTP timeout.
GROUP_LOCK_EXCLUSIVE_ATTEMPTS = 20
GROUP_LOCK_EXCLUSIVE_INTERVAL_S = 0.05


class GroupLockBusy(Exception):
    """The exclusive group lock was not free within the bounded retry. The same
    request can be retried."""

    def __init__(self) -> None:
        super().__init__(
            "another output-equivalence decision, revoke or deprecation is in progress;"
            " retry shortly"
        )


async def lock_groups_shared(conn: asyncpg.Connection) -> None:
    """Take the group lock SHARED until the transaction ends.

    A deprecation takes this before reading the joining output equivalences that name
    its definition and before any definition row lock: no decision commits while it is
    held, so a joining decision and the deprecation of one of its sides cannot
    both commit. It waits for a decision in progress. The caller's transaction must
    be READ COMMITTED, so that the statement after the wait sees that decision; at
    REPEATABLE READ the waiting statement takes a snapshot from before that decision
    commits.
    Decisions never queue behind it (`lock_groups_exclusive`)."""
    require_transaction(conn)
    isolation = await transaction_isolation(conn)
    if isolation != READ_COMMITTED:
        raise RuntimeError(
            f"lock_groups_shared needs a READ COMMITTED transaction; this one is {isolation!r}"
        )
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
    them.

    The caller's transaction must be READ COMMITTED, so that the statements after
    the lock see a decision that committed while this one retried; at REPEATABLE
    READ the first try-lock fixes the snapshot, and the caller would check its
    decision against the groups as they were before that one."""
    require_transaction(conn)
    isolation = await transaction_isolation(conn)
    if isolation != READ_COMMITTED:
        raise RuntimeError(
            f"lock_groups_exclusive needs a READ COMMITTED transaction; this one is {isolation!r}"
        )
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


async def holds_group_lock_exclusive(conn: asyncpg.Connection) -> bool:
    """True when this session holds the group lock EXCLUSIVE."""
    return await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM pg_locks"
        " WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND granted"
        "   AND mode = 'ExclusiveLock' AND classid = $1 AND objid = $2 AND objsubid = 2)",
        DEFINITION_GROUP_LOCK_CLASS,
        DEFINITION_GROUP_LOCK_KEY,
    )


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


@dataclass(frozen=True)
class DefinitionTable:
    """One kind's table: its name, id column, workflow and version columns (None for
    an alignment, which keeps both inside its params; the workflow column is what the
    comparison scope reads), and its deprecated status value. The names are
    interpolated into SQL, so `DEFINITION_TABLES` is closed."""

    table: str
    id_column: str
    workflow_column: str | None
    version_column: str | None
    deprecated_status: str

    @property
    def workflow_expr(self) -> str:
        return self.workflow_column or "NULL::text"


DEFINITION_TABLES: dict[DefinitionKind, DefinitionTable] = {
    DefinitionKind.MASK: DefinitionTable(
        "qiita.mask_definition",
        "mask_idx",
        "filter_workflow",
        "filter_version",
        MaskDefinitionStatus.DEPRECATED.value,
    ),
    DefinitionKind.ALIGNMENT: DefinitionTable(
        "qiita.alignment_definition",
        "alignment_idx",
        None,
        None,
        AlignmentDefinitionStatus.DEPRECATED.value,
    ),
    DefinitionKind.PROCESSING: DefinitionTable(
        "qiita.processing",
        "processing_idx",
        "workflow",
        "version",
        ProcessingStatus.DEPRECATED.value,
    ),
}


def decode_jsonb(value: Any) -> Any:
    """A JSONB column, decoded: asyncpg returns it as str under the default codec,
    or already decoded when a codec is registered."""
    return json.loads(value) if isinstance(value, str) else value


def _definition_row(kind: DefinitionKind, row: asyncpg.Record) -> DefinitionRow:
    return DefinitionRow(
        kind=kind,
        idx=row["idx"],
        params=decode_jsonb(row["params"]),
        deprecated=row["status"] == DEFINITION_TABLES[kind].deprecated_status,
        workflow=row["workflow"],
    )


async def fetch_definitions(conn: asyncpg.Connection) -> list[DefinitionRow]:
    """Every mask, alignment and processing definition, deprecated ones included."""
    out: list[DefinitionRow] = []
    for kind, t in DEFINITION_TABLES.items():
        rows = await conn.fetch(
            f"SELECT {t.id_column} AS idx, params, status, {t.workflow_expr} AS workflow"
            f" FROM {t.table}"
        )
        out.extend(_definition_row(kind, r) for r in rows)
    return out


async def fetch_definition(
    conn: asyncpg.Connection, kind: DefinitionKind, idx: int
) -> DefinitionRow | None:
    """One definition of `kind`, or None when no row has `idx`."""
    t = DEFINITION_TABLES[kind]
    row = await conn.fetchrow(
        f"SELECT {t.id_column} AS idx, params, status, {t.workflow_expr} AS workflow"
        f" FROM {t.table} WHERE {t.id_column} = $1",
        idx,
    )
    return None if row is None else _definition_row(kind, row)


# ---------------------------------------------------------------------------
# Equivalences
# ---------------------------------------------------------------------------

_EQUIVALENCE_COLUMNS = (
    "output_equivalence_idx, definition_kind,"
    " low_definition_ref AS low_definition_idx, high_definition_ref AS high_definition_idx,"
    " detached_at, change, derived_only, verdict, decided_by_idx, decided_at,"
    " decision_reason, opened_by_idx, opened_at"
)

_TYPED_COLUMNS: dict[DefinitionKind, tuple[str, str]] = {
    DefinitionKind.MASK: ("low_mask_idx", "high_mask_idx"),
    DefinitionKind.ALIGNMENT: ("low_alignment_idx", "high_alignment_idx"),
    DefinitionKind.PROCESSING: ("low_processing_idx", "high_processing_idx"),
}


def equivalence_record_fields(row: asyncpg.Record) -> dict[str, Any]:
    """An equivalence row as the `OutputEquivalence` wire model's fields."""
    return {**dict(row), "change": decode_jsonb(row["change"])}


async def insert_equivalence(
    conn: asyncpg.Connection,
    *,
    kind: DefinitionKind,
    low_idx: int,
    high_idx: int,
    change: dict[str, list[Any]],
    derived_only: bool,
    opened_by_idx: int,
) -> tuple[asyncpg.Record, bool]:
    """Open a pending equivalence and its `opened` event, or return the equivalence already
    recorded for these two definitions. Returns `(row, created)`."""
    require_transaction(conn)
    low_col, high_col = _TYPED_COLUMNS[kind]
    row = await conn.fetchrow(
        "INSERT INTO qiita.output_equivalence ("
        f"  definition_kind, {low_col}, {high_col}, low_definition_ref, high_definition_ref,"
        "  change, derived_only, opened_by_idx"
        ") VALUES ($1, $2, $3, $2, $3, $4::jsonb, $5, $6)"
        " ON CONFLICT DO NOTHING"
        f" RETURNING {_EQUIVALENCE_COLUMNS}",
        kind.value,
        low_idx,
        high_idx,
        json.dumps(change),
        derived_only,
        opened_by_idx,
    )
    if row is None:
        existing = await conn.fetchrow(
            f"SELECT {_EQUIVALENCE_COLUMNS} FROM qiita.output_equivalence"
            " WHERE definition_kind = $1 AND low_definition_ref = $2 AND high_definition_ref = $3",
            kind.value,
            low_idx,
            high_idx,
        )
        return existing, False
    await _insert_event(
        conn,
        output_equivalence_idx=row["output_equivalence_idx"],
        event=OutputEquivalenceEventType.OPENED,
        verdict=OutputEquivalenceVerdict.PENDING,
        actor_principal_idx=opened_by_idx,
        reason=None,
    )
    return row, True


async def fetch_equivalence(
    conn: asyncpg.Pool | asyncpg.Connection,
    output_equivalence_idx: int,
    *,
    for_update: bool = False,
) -> asyncpg.Record | None:
    """One equivalence, optionally locked FOR UPDATE (inside a transaction)."""
    if for_update:
        if isinstance(conn, asyncpg.Pool):
            raise RuntimeError("a row lock needs a connection inside a transaction")
        require_transaction(conn)
    return await conn.fetchrow(
        f"SELECT {_EQUIVALENCE_COLUMNS} FROM qiita.output_equivalence"
        " WHERE output_equivalence_idx = $1" + (" FOR UPDATE" if for_update else ""),
        output_equivalence_idx,
    )


async def list_equivalences(
    conn: asyncpg.Pool | asyncpg.Connection,
    *,
    kinds: list[DefinitionKind],
    verdict: OutputEquivalenceVerdict | None,
    before_output_equivalence_idx: int | None,
    limit: int,
) -> list[asyncpg.Record]:
    """Up to `limit` equivalences of the given kinds, optionally of one verdict, newest
    first, with an id below `before_output_equivalence_idx` when it is given."""
    return list(
        await conn.fetch(
            f"SELECT {_EQUIVALENCE_COLUMNS} FROM qiita.output_equivalence"
            " WHERE definition_kind = ANY($1::text[])"
            "   AND ($2::text IS NULL OR verdict = $2)"
            "   AND ($3::bigint IS NULL OR output_equivalence_idx < $3)"
            " ORDER BY output_equivalence_idx DESC LIMIT $4",
            [k.value for k in kinds],
            None if verdict is None else verdict.value,
            before_output_equivalence_idx,
            limit,
        )
    )


async def fetch_live_equivalences(conn: asyncpg.Connection) -> list[asyncpg.Record]:
    """Every equivalence that is not detached, of every kind and verdict."""
    return list(
        await conn.fetch(
            f"SELECT {_EQUIVALENCE_COLUMNS} FROM qiita.output_equivalence WHERE detached_at IS NULL"
        )
    )


async def fetch_equivalence_events(
    conn: asyncpg.Pool | asyncpg.Connection, output_equivalence_idx: int
) -> list[asyncpg.Record]:
    """An equivalence's history, oldest first."""
    return list(
        await conn.fetch(
            "SELECT output_equivalence_event_idx, event, verdict, actor_principal_idx, reason,"
            " occurred_at FROM qiita.output_equivalence_event"
            " WHERE output_equivalence_idx = $1 ORDER BY output_equivalence_event_idx",
            output_equivalence_idx,
        )
    )


async def fetch_joining_equivalences_naming(
    conn: asyncpg.Connection, kind: DefinitionKind, idx: int
) -> list[int]:
    """The live joining output equivalences of `kind` with `idx` on either side, by
    the SQL twin of `JOINING_VERDICTS`."""
    return [
        r["output_equivalence_idx"]
        for r in await conn.fetch(
            "SELECT output_equivalence_idx FROM qiita.output_equivalence"
            " WHERE definition_kind = $1 AND qiita.output_equivalence_verdict_joins(verdict)"
            "   AND detached_at IS NULL"
            "   AND $2 IN (low_definition_ref, high_definition_ref)"
            " ORDER BY output_equivalence_idx",
            kind.value,
            idx,
        )
    ]


async def record_decision(
    conn: asyncpg.Connection,
    *,
    output_equivalence_idx: int,
    verdict: OutputEquivalenceVerdict,
    reason: str,
    principal_idx: int,
) -> asyncpg.Record:
    """Write a decision onto the equivalence and its `decided` event; return the equivalence."""
    require_transaction(conn)
    row = await conn.fetchrow(
        "UPDATE qiita.output_equivalence"
        "   SET verdict = $2, decided_by_idx = $3, decided_at = now(), decision_reason = $4"
        " WHERE output_equivalence_idx = $1"
        f" RETURNING {_EQUIVALENCE_COLUMNS}",
        output_equivalence_idx,
        verdict.value,
        principal_idx,
        reason,
    )
    await _insert_event(
        conn,
        output_equivalence_idx=output_equivalence_idx,
        event=OutputEquivalenceEventType.DECIDED,
        verdict=verdict,
        actor_principal_idx=principal_idx,
        reason=reason,
    )
    return row


async def record_revoke(
    conn: asyncpg.Connection, *, output_equivalence_idx: int, reason: str, principal_idx: int
) -> asyncpg.Record:
    """Return the equivalence to pending, clearing its decision, and write the `revoked`
    event; return the equivalence."""
    require_transaction(conn)
    row = await conn.fetchrow(
        "UPDATE qiita.output_equivalence"
        "   SET verdict = $2, decided_by_idx = NULL, decided_at = NULL,"
        "       decision_reason = NULL"
        " WHERE output_equivalence_idx = $1"
        f" RETURNING {_EQUIVALENCE_COLUMNS}",
        output_equivalence_idx,
        OutputEquivalenceVerdict.PENDING.value,
    )
    await _insert_event(
        conn,
        output_equivalence_idx=output_equivalence_idx,
        event=OutputEquivalenceEventType.REVOKED,
        verdict=OutputEquivalenceVerdict.PENDING,
        actor_principal_idx=principal_idx,
        reason=reason,
    )
    return row


async def _insert_event(
    conn: asyncpg.Connection,
    *,
    output_equivalence_idx: int,
    event: OutputEquivalenceEventType,
    verdict: OutputEquivalenceVerdict,
    actor_principal_idx: int,
    reason: str | None,
) -> None:
    await conn.execute(
        "INSERT INTO qiita.output_equivalence_event"
        " (output_equivalence_idx, event, verdict, actor_principal_idx, reason)"
        " VALUES ($1, $2, $3, $4, $5)",
        output_equivalence_idx,
        event.value,
        verdict.value,
        actor_principal_idx,
        reason,
    )
