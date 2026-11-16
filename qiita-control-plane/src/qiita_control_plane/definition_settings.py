"""The audited edit of a mask's, alignment's or processing's locked settings.

The settings are locked after the mint by triggers (migration
`20261116000004_definition_settings_lock.sql` lists the columns); this module is the
one path that changes them. In the caller's READ COMMITTED transaction, holding the
group lock exclusive (an edit changes what the groups are computed from):

  1. the definition exists (`DefinitionNotFound`);
  2. no live `similar` pair names it (`PairRefused`, `similar_pairs`): the groups that
     pair joins were decided over the old settings, so it is revoked first, as for a
     deprecation or a delete;
  3. the edit changes something, and no other definition of the kind has the new
     params (`SettingsEditRefused`, `no_change` / `params_taken`);
  4. each live pending or not_similar pair naming it gets its `change` and
     `derived_only` recomputed from the new params, by the rule a pair is opened
     with (`check_open`), which refuses an edit that would leave such a pair across
     comparison scopes or with no difference. Their verdicts stay: a not_similar
     decision joins nothing, so a stale one cannot merge groups;
  5. `qiita.edit_definition_settings` records the edit (actor, old and new values,
     reason) and makes it.

params_hash is recomputed from `params` with `canonical_params_hash`, as the mint does,
after checking the params survive jsonb storage unchanged.
"""

import json
from dataclasses import dataclass
from typing import Any

import asyncpg
from qiita_common.hashing import canonical_params_hash
from qiita_common.models import (
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
from .repositories.alignment_definition import assert_params_survive_storage
from .repositories.definition_pair import (
    DefinitionRow,
    fetch_definition,
    fetch_similar_pairs_naming,
    lock_groups_exclusive,
)

__all__ = ["SettingsEditRefused", "SettingsEditResult", "edit_definition_settings"]

# Per kind: the table, its id column, and its workflow / version columns (None for
# an alignment, which keeps both inside its params). Interpolated into SQL.
_KIND_COLUMNS: dict[DefinitionKind, tuple[str, str, str | None, str | None]] = {
    DefinitionKind.MASK: ("qiita.mask_definition", "mask_idx", "filter_workflow", "filter_version"),
    DefinitionKind.ALIGNMENT: ("qiita.alignment_definition", "alignment_idx", None, None),
    DefinitionKind.PROCESSING: ("qiita.processing", "processing_idx", "workflow", "version"),
}


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
    DefinitionNotFound, PairRefused, SettingsEditRefused, GroupLockBusy, ValueError
    (a workflow or version given for an alignment) or
    ParamsDoNotSurviveStorageError."""
    require_transaction(conn)
    table, id_col, workflow_col, version_col = _KIND_COLUMNS[kind]
    if workflow_col is None and (workflow is not None or version is not None):
        raise ValueError(f"an {kind.value} has no workflow or version column; edit its params")
    await lock_groups_exclusive(conn)
    current = await fetch_definition(conn, kind, idx)
    if current is None:
        raise DefinitionNotFound(kind, idx)
    similar = await fetch_similar_pairs_naming(conn, kind, idx)
    if similar:
        raise PairRefused(
            DefinitionPairRefusal.SIMILAR_PAIRS,
            f"{kind.value} {idx} is a side of similar pair(s) {similar}; revoke them first",
            definition_pair_idx=similar,
        )

    params_hash = canonical_params_hash(params)
    await assert_params_survive_storage(conn, params, params_hash)
    patch: dict[str, Any] = {"params": params, "params_hash": "\\x" + params_hash.hex()}
    if workflow is not None:
        patch[workflow_col] = workflow
    if version is not None:
        patch[version_col] = version
    taken = await conn.fetchval(
        f"SELECT {id_col} FROM {table} WHERE params_hash = $1 AND {id_col} <> $2",
        params_hash,
        idx,
    )
    if taken is not None:
        raise SettingsEditRefused(
            DefinitionSettingsRefusal.PARAMS_TAKEN,
            f"{kind.value} {taken} already has these params; {kind.value} {idx} cannot take them",
        )

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
