"""The audited edit of a mask's, alignment's or processing's locked settings.

The settings are locked after the mint by triggers (`qiita.definition_settings_columns`
lists the columns); this module is the one path that changes them. In the caller's
READ COMMITTED transaction, holding the group lock exclusive (an edit changes what the
groups are computed from):

  1. the definition exists (`DefinitionNotFound`);
  2. no live `similar` pair names it (`PairRefused`, `similar_pairs`): the groups that
     pair joins were decided over the old settings, so it is revoked first, as for a
     deprecation or a delete;
  2a. no unfinished work ticket runs under it (`live_work`): its output would be
     recorded under settings it did not run with. The definition row is held
     `FOR UPDATE`, which the writers of `work_ticket.mask_idx` / `alignment_idx` wait
     on (`lock_active_mask`, `lock_active_alignment`); no writer of
     `work_ticket.processing_idx` takes it, so a processing's check is a read. The
     deprecation lookups (`refuse_deprecating_*_with_live_work`) are not reused:
     they skip a deprecated definition, and a deprecated one's settings are locked too;
  2b. no public handle that is not retired cites it (`published`): an
     `exported_processing` row naming it or an `exported_identifier` minted under it.
     A published manifest cites the settings it was built under. Only masks and
     alignments have handles; a processing has none;
  3. the edit changes something, and no other definition of the kind has the new
     params (`SettingsEditRefused`, `no_change` / `params_taken`; a concurrent mint
     of the same params that the check misses hits the `params_hash` UNIQUE
     constraint, which is refused the same way);
  4. each live pending or not_similar pair naming it gets its `change` and
     `derived_only` recomputed from the new params, by the rule a pair is opened
     with (`check_open`), which refuses an edit that would leave such a pair across
     comparison scopes or with no difference. Their verdicts stay: a not_similar
     decision joins nothing, so a stale one cannot merge groups;
  5. `qiita.edit_definition_settings` records the edit (actor, old and new values,
     reason) and makes it.

The database itself enforces only that a locked change is audited and that no live
`similar` pair names the row (the lock trigger); steps 2a, 2b, 3 and 4 are checks of
this path, as deprecation's live-work check is of its route.

Before any of these, the request itself is checked (`SettingsEditInvalid`): an
alignment has no workflow or version column, and a mask's or processing's workflow and
version columns must equal the same-named keys in `params` when `params` carries them,
as the mint writes both. params_hash is recomputed from `params` with
`canonical_params_hash`, as the mint does, after checking the params survive jsonb
storage unchanged.
"""

import json
from dataclasses import dataclass
from typing import Any

import asyncpg
from qiita_common.hashing import canonical_params_hash
from qiita_common.models import (
    NON_TERMINAL_WORK_TICKET_STATES,
    DefinitionKind,
    DefinitionPairRefusal,
    DefinitionPairVerdict,
    DefinitionSettingsRefusal,
)

from .definition_similarity import (
    DefinitionNotFound,
    PairRefused,
    check_open,
    is_derived_only,
)
from .repositories import require_transaction
from .repositories.definition_pair import (
    DEFINITION_TABLES,
    DefinitionRow,
    DefinitionTable,
    fetch_definition,
    fetch_similar_pairs_naming,
    lock_groups_exclusive,
)
from .repositories.params_storage import assert_params_survive_storage

__all__ = [
    "SettingsEditInvalid",
    "SettingsEditRefused",
    "SettingsEditResult",
    "edit_definition_settings",
]


class SettingsEditInvalid(Exception):
    """A request that cannot describe the definition: a workflow or version for an
    alignment, or a workflow / version column that disagrees with `params`."""


class SettingsEditRefused(Exception):
    """An edit refused by a rule of this module, not a pair rule."""

    def __init__(self, reason: DefinitionSettingsRefusal, message: str) -> None:
        super().__init__(message)
        self.reason = reason


@dataclass(frozen=True)
class SettingsEditResult:
    edit: asyncpg.Record
    repaired_pair_idx: list[int]


async def edit_definition_settings(
    conn: asyncpg.Connection,
    *,
    kind: DefinitionKind,
    idx: int,
    params: dict[str, Any],
    workflow: str | None,
    version: str | None,
    reason: str,
    principal_idx: int,
) -> SettingsEditResult:
    """Edit `kind` `idx`'s settings by the steps in the module docstring. Raises
    DefinitionNotFound, SettingsEditInvalid, PairRefused, SettingsEditRefused,
    GroupLockBusy or ParamsDoNotSurviveStorageError."""
    require_transaction(conn)
    t = DEFINITION_TABLES[kind]
    if t.workflow_column is None and (workflow is not None or version is not None):
        raise SettingsEditInvalid(
            f"an {kind.value} has no workflow or version column; edit its params"
        )
    await lock_groups_exclusive(conn)
    current = await fetch_definition(conn, kind, idx)
    if current is None:
        raise DefinitionNotFound(kind, idx)
    columns = await _columns_after_edit(conn, t, idx, workflow=workflow, version=version)
    disagree = {
        col: (value, params[col])
        for col, value in columns.items()
        if col in params and params[col] != value
    }
    if disagree:
        raise SettingsEditInvalid(
            f"{kind.value} {idx}: "
            + "; ".join(
                f"{col} would be {value!r} but params[{col!r}] is {in_params!r}"
                for col, (value, in_params) in sorted(disagree.items())
            )
            + ". The column and the params key are written together at the mint; give"
            " both the same value"
        )
    similar = await fetch_similar_pairs_naming(conn, kind, idx)
    if similar:
        raise PairRefused(
            DefinitionPairRefusal.SIMILAR_PAIRS,
            f"{kind.value} {idx} is a side of similar pair(s) {similar}; revoke them first",
            definition_pair_idx=similar,
        )
    await _refuse_live_work(conn, t, kind, idx)
    await _refuse_published(conn, t, kind, idx)

    params_hash = canonical_params_hash(params)
    await assert_params_survive_storage(conn, params, params_hash)
    patch: dict[str, Any] = {"params": params, "params_hash": "\\x" + params_hash.hex()}
    if workflow is not None:
        patch[t.workflow_column] = workflow
    if version is not None:
        patch[t.version_column] = version
    taken = await _params_taken_by(conn, t, params_hash, idx)
    if taken is not None:
        raise _params_taken(kind, idx, f"{kind.value} {taken}")

    edited = DefinitionRow(
        kind=kind,
        idx=idx,
        params=params,
        deprecated=current.deprecated,
        workflow=workflow if workflow is not None else current.workflow,
    )
    repaired = await _recompute_pairs(conn, edited)

    try:
        edit_idx = await conn.fetchval(
            "SELECT qiita.edit_definition_settings($1, $2, $3::jsonb, $4, $5)",
            kind.value,
            idx,
            json.dumps(patch),
            principal_idx,
            reason,
        )
    except asyncpg.UniqueViolationError as exc:
        if exc.constraint_name != f"{t.table.removeprefix('qiita.')}_params_hash_key":
            raise
        raise _params_taken(kind, idx, f"another {kind.value}") from exc
    except asyncpg.CheckViolationError as exc:
        if exc.constraint_name != "definition_settings_edit_changes":
            raise
        raise SettingsEditRefused(
            DefinitionSettingsRefusal.NO_CHANGE,
            f"{kind.value} {idx} already has these settings",
        ) from exc
    edit = await conn.fetchrow(
        "SELECT * FROM qiita.definition_settings_edit WHERE definition_settings_edit_idx = $1",
        edit_idx,
    )
    return SettingsEditResult(edit=edit, repaired_pair_idx=repaired)


# The tables whose live rows cite a definition by the kind's id column.
_HANDLE_TABLES = {
    DefinitionKind.MASK: ("exported_processing", "exported_identifier"),
    DefinitionKind.ALIGNMENT: ("exported_processing", "exported_identifier"),
    DefinitionKind.PROCESSING: (),
}


async def _refuse_live_work(
    conn: asyncpg.Connection, t: DefinitionTable, kind: DefinitionKind, idx: int
) -> None:
    """Step 2a of the module docstring."""
    await conn.execute(f"SELECT 1 FROM {t.table} WHERE {t.id_column} = $1 FOR UPDATE", idx)
    live = [
        r["work_ticket_idx"]
        for r in await conn.fetch(
            "SELECT work_ticket_idx FROM qiita.work_ticket"
            f" WHERE {t.id_column} = $1 AND state::text = ANY($2::text[])"
            " ORDER BY work_ticket_idx",
            idx,
            list(NON_TERMINAL_WORK_TICKET_STATES),
        )
    ]
    if live:
        raise SettingsEditRefused(
            DefinitionSettingsRefusal.LIVE_WORK,
            f"{kind.value} {idx} has {len(live)} unfinished work ticket(s): {live}. Wait"
            " for them to finish, or cancel them, then edit its settings",
        )


async def _refuse_published(
    conn: asyncpg.Connection, t: DefinitionTable, kind: DefinitionKind, idx: int
) -> None:
    """Step 2b of the module docstring."""
    cited: list[str] = []
    for table in _HANDLE_TABLES[kind]:
        n = await conn.fetchval(
            f"SELECT count(*) FROM qiita.{table} WHERE {t.id_column} = $1 AND NOT retired",
            idx,
        )
        if n:
            cited.append(f"{n} {table} row(s)")
    if cited:
        raise SettingsEditRefused(
            DefinitionSettingsRefusal.PUBLISHED,
            f"{kind.value} {idx} is cited by public handles that are not retired"
            f" ({', '.join(cited)}); a published output names the settings it was built"
            " under, so they are not edited",
        )


async def _columns_after_edit(
    conn: asyncpg.Connection,
    t: DefinitionTable,
    idx: int,
    *,
    workflow: str | None,
    version: str | None,
) -> dict[str, str]:
    """The workflow and version columns as the edit would leave them: the value given,
    else the stored one. Empty for an alignment."""
    if t.workflow_column is None or t.version_column is None:
        return {}
    row = await conn.fetchrow(
        f"SELECT {t.workflow_column} AS workflow, {t.version_column} AS version"
        f" FROM {t.table} WHERE {t.id_column} = $1",
        idx,
    )
    return {
        t.workflow_column: workflow if workflow is not None else row["workflow"],
        t.version_column: version if version is not None else row["version"],
    }


async def _params_taken_by(
    conn: asyncpg.Connection, t: DefinitionTable, params_hash: bytes, idx: int
) -> int | None:
    """Another definition of the table whose params_hash is `params_hash`, or None."""
    return await conn.fetchval(
        f"SELECT {t.id_column} FROM {t.table} WHERE params_hash = $1 AND {t.id_column} <> $2",
        params_hash,
        idx,
    )


def _params_taken(kind: DefinitionKind, idx: int, holder: str) -> SettingsEditRefused:
    return SettingsEditRefused(
        DefinitionSettingsRefusal.PARAMS_TAKEN,
        f"{holder} already has these params; {kind.value} {idx} cannot take them",
    )


async def _recompute_pairs(conn: asyncpg.Connection, edited: DefinitionRow) -> list[int]:
    """Recompute `change` and `derived_only` of every live pending or not_similar pair
    naming `edited`, from its new params. Returns the pairs, ascending."""
    rows = await conn.fetch(
        "SELECT definition_pair_idx, low_definition_ref, high_definition_ref"
        "  FROM qiita.definition_pair"
        " WHERE definition_kind = $1 AND detached_at IS NULL AND verdict <> $3"
        "   AND $2 IN (low_definition_ref, high_definition_ref)"
        " ORDER BY definition_pair_idx",
        edited.kind.value,
        edited.idx,
        DefinitionPairVerdict.SIMILAR.value,
    )
    repaired = []
    for row in rows:
        other_idx = (
            row["high_definition_ref"]
            if row["low_definition_ref"] == edited.idx
            else row["low_definition_ref"]
        )
        other = await fetch_definition(conn, edited.kind, other_idx)
        if other is None:
            raise RuntimeError(
                f"definition_pair {row['definition_pair_idx']} is live but"
                f" {edited.kind.value} {other_idx} is gone"
            )
        low, high = (edited, other) if edited.idx < other_idx else (other, edited)
        try:
            change = check_open(low, high)
        except PairRefused as exc:
            raise PairRefused(
                exc.reason,
                f"the edit would leave definition_pair {row['definition_pair_idx']} invalid: {exc}",
                definition_pair_idx=[row["definition_pair_idx"]],
            ) from exc
        await conn.execute(
            "UPDATE qiita.definition_pair SET change = $2::jsonb, derived_only = $3"
            " WHERE definition_pair_idx = $1",
            row["definition_pair_idx"],
            json.dumps(change),
            is_derived_only(edited.kind, low.params, high.params, change),
        )
        repaired.append(row["definition_pair_idx"])
    return repaired
