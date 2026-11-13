"""SQL for what the group resolver reads of a definition's runs: the per-sample gate
rows (`mask_sample`, `alignment_sample`, `assembly_sample`) and the work tickets that
produce them.

Nothing here decides a prep_sample's state; `qiita_control_plane.definition_resolver`
does, from these rows.
"""

import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import asyncpg
from qiita_common.actions import (
    ALIGN_ACTION_ID,
    ALIGN_DENOVO_ACTION_ID,
    BLOCK_MASK_ACTION_ID,
    LONG_READ_ASSEMBLY_ACTION_ID,
    PER_SAMPLE_MASK_ACTION_IDS,
)
from qiita_common.models import DefinitionKind, WorkTicketState

from .block import BLOCK_ABANDONED

# Where a row came from: a gate row, a prep_sample-scoped ticket, or the ticket of a
# block that covers the prep_sample.
SOURCE_GATE = "gate"
SOURCE_SAMPLE_TICKET = "sample_ticket"
SOURCE_BLOCK_TICKET = "block_ticket"


@dataclass(frozen=True)
class RunRow:
    """One gate row or one producing ticket of a `(definition, prep_sample)`.
    `state` is the gate's state for a gate row and the ticket's otherwise;
    `work_ticket_idx` is None for a gate row."""

    source: str
    definition_idx: int
    prep_sample_idx: int
    state: str
    work_ticket_idx: int | None


@dataclass(frozen=True)
class _KindTables:
    gate_table: str
    gate_column: str
    # The work_ticket column naming the definition a producing ticket writes, and the
    # actions that produce this kind's runs per prep_sample and per block. A mask's column
    # is also written by tickets that consume a mask (assembly, align, align-denovo),
    # so the action filter is what keeps those out.
    ticket_column: str
    sample_actions: tuple[str, ...]
    block_actions: tuple[str, ...]


# Interpolated into SQL, so the map is closed. A processing's runs are produced by
# tickets that record no processing_idx; `fetch_assembly_ticket_candidates` reads
# those.
_KIND_TABLES: dict[DefinitionKind, _KindTables] = {
    DefinitionKind.MASK: _KindTables(
        gate_table="qiita.mask_sample",
        gate_column="mask_idx",
        ticket_column="mask_idx",
        sample_actions=PER_SAMPLE_MASK_ACTION_IDS,
        block_actions=(BLOCK_MASK_ACTION_ID,),
    ),
    DefinitionKind.ALIGNMENT: _KindTables(
        gate_table="qiita.alignment_sample",
        gate_column="alignment_idx",
        ticket_column="alignment_idx",
        sample_actions=(ALIGN_DENOVO_ACTION_ID,),
        block_actions=(ALIGN_ACTION_ID,),
    ),
}

_PROCESSING_GATE_TABLE = "qiita.assembly_sample"


async def fetch_run_rows(
    conn: asyncpg.Connection,
    kind: DefinitionKind,
    definition_idxs: Sequence[int],
    prep_sample_idxs: Sequence[int],
) -> list[RunRow]:
    """Every gate row of `kind` for these definitions and prep_samples, and every
    producing ticket that is not cancelled: prep_sample-scoped tickets naming the
    definition, and the tickets of blocks that cover the prep_sample and are not
    abandoned. One statement, so gates and tickets are read from one snapshot.

    For `processing`, gate rows only; the tickets come from
    `fetch_assembly_ticket_candidates`, a second statement (`resolve_group` says
    which isolation that needs)."""
    definitions = sorted(set(definition_idxs))
    samples = sorted(set(prep_sample_idxs))
    if not definitions or not samples:
        return []
    if kind is DefinitionKind.PROCESSING:
        rows = await conn.fetch(
            f"SELECT $3::text AS source, processing_idx AS definition_idx, prep_sample_idx,"
            f"       state, NULL::bigint AS work_ticket_idx"
            f"  FROM {_PROCESSING_GATE_TABLE}"
            f" WHERE processing_idx = ANY($1::bigint[]) AND prep_sample_idx = ANY($2::bigint[])",
            definitions,
            samples,
            SOURCE_GATE,
        )
        return [RunRow(**dict(r)) for r in rows]
    t = _KIND_TABLES[kind]
    rows = await conn.fetch(
        f"SELECT $5::text AS source, g.{t.gate_column} AS definition_idx, g.prep_sample_idx,"
        f"       g.state, NULL::bigint AS work_ticket_idx"
        f"  FROM {t.gate_table} g"
        f" WHERE g.{t.gate_column} = ANY($1::bigint[]) AND g.prep_sample_idx = ANY($2::bigint[])"
        f" UNION ALL"
        f" SELECT $6::text, wt.{t.ticket_column}, wt.prep_sample_idx, wt.state::text,"
        f"        wt.work_ticket_idx"
        f"   FROM qiita.work_ticket wt"
        f"  WHERE wt.action_id = ANY($3::text[])"
        f"    AND wt.{t.ticket_column} = ANY($1::bigint[])"
        f"    AND wt.prep_sample_idx = ANY($2::bigint[])"
        f"    AND wt.state <> $8::qiita.work_ticket_state"
        f" UNION ALL"
        f" SELECT $7::text, wt.{t.ticket_column}, bm.prep_sample_idx, wt.state::text,"
        f"        wt.work_ticket_idx"
        f"   FROM qiita.work_ticket wt"
        f"   JOIN qiita.block b ON b.work_ticket_idx = wt.work_ticket_idx"
        f"   JOIN qiita.block_member bm ON bm.block_idx = b.block_idx"
        f"  WHERE wt.action_id = ANY($4::text[])"
        f"    AND wt.{t.ticket_column} = ANY($1::bigint[])"
        f"    AND bm.prep_sample_idx = ANY($2::bigint[])"
        f"    AND wt.state <> $8::qiita.work_ticket_state"
        f"    AND b.state <> $9",
        definitions,
        samples,
        list(t.sample_actions),
        list(t.block_actions),
        SOURCE_GATE,
        SOURCE_SAMPLE_TICKET,
        SOURCE_BLOCK_TICKET,
        WorkTicketState.CANCELLED.value,
        BLOCK_ABANDONED,
    )
    return [RunRow(**dict(r)) for r in rows]


@dataclass(frozen=True)
class AssemblyTicketCandidate:
    """A long-read-assembly ticket that the runner has started (its consumed
    `mask_idx` is recorded), with what is needed to compute the processing params
    that it mints."""

    work_ticket_idx: int
    prep_sample_idx: int
    state: str
    action_id: str
    action_version: str
    action_context: dict[str, Any]
    mask_idx: int
    context_schema: dict[str, Any]


def _decode(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


async def fetch_assembly_ticket_candidates(
    conn: asyncpg.Connection, prep_sample_idxs: Sequence[int]
) -> list[AssemblyTicketCandidate]:
    """The long-read-assembly tickets of these prep_samples that are not cancelled and
    that the runner has started: the runner records a ticket's `mask_idx` before it
    mints the processing, so a ticket with none is not started and is not
    returned."""
    samples = sorted(set(prep_sample_idxs))
    if not samples:
        return []
    rows = await conn.fetch(
        "SELECT wt.work_ticket_idx, wt.prep_sample_idx, wt.state,"
        "       wt.action_id, wt.action_version, wt.action_context, wt.mask_idx,"
        "       a.context_schema"
        "  FROM qiita.work_ticket wt"
        "  JOIN qiita.action a ON a.action_id = wt.action_id AND a.version = wt.action_version"
        " WHERE wt.action_id = $1 AND wt.prep_sample_idx = ANY($2::bigint[])"
        "   AND wt.mask_idx IS NOT NULL AND wt.state <> $3::qiita.work_ticket_state",
        LONG_READ_ASSEMBLY_ACTION_ID,
        samples,
        WorkTicketState.CANCELLED.value,
    )
    return [
        AssemblyTicketCandidate(
            **{
                **dict(r),
                "action_context": _decode(r["action_context"]) or {},
                "context_schema": _decode(r["context_schema"]) or {},
            }
        )
        for r in rows
    ]
