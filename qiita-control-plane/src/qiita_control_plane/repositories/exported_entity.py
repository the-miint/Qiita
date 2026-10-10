"""Reads of `qiita.exported_entity`."""

from collections.abc import Mapping
from typing import Any, Literal

import asyncpg

from ._exported_identifier_helpers import missing_from

EntityKind = Literal["study", "biosample"]

# The entity column per kind. A column name cannot be bound as a parameter, so the
# kind selects one from this closed mapping rather than from caller input.
_ENTITY_COLUMN: dict[EntityKind, str] = {"study": "study_idx", "biosample": "biosample_idx"}

# Every caller-visible column, both kinds in one pass. Study entries come first
# ascending, then biosample entries ascending: `study_idx IS NULL` is false on a
# study row and false sorts first, and a biosample row's NULL study_idx ties the
# second group so the trailing key orders it.
_SELECT = (
    "SELECT study_idx, biosample_idx, export_entity_id"
    "  FROM qiita.exported_entity"
    " WHERE study_idx = ANY($1::bigint[]) OR biosample_idx = ANY($2::bigint[])"
    " ORDER BY study_idx IS NULL, study_idx, biosample_idx"
)


class MissingExportedEntityError(RuntimeError):
    """Requested entities that have no export_entity_id.

    Carries the missing identifiers and the kind they belong to. Every existing
    study and biosample holds a handle, so each one names either no entity at all
    or an entity whose handle is unexpectedly absent.
    """

    def __init__(self, missing: list[int], *, kind: str) -> None:
        self.missing = missing
        self.kind = kind
        super().__init__(
            f"{len(missing)} {kind}(s) have no export_entity_id or do not exist: {missing}"
        )


def export_entity_id_select(kind: EntityKind, *, alias: str) -> str:
    """Return the select-list expression for an entity's `export_entity_id`, where
    `alias` names the entity table in the enclosing query.

    A scalar subquery, so an entity without a row reads NULL rather than dropping
    out of the result.
    """
    column = _ENTITY_COLUMN[kind]
    expression = (
        "(SELECT export_entity_id FROM qiita.exported_entity ee"
        f" WHERE ee.{column} = {alias}.idx) AS export_entity_id"
    )
    return expression


def require_export_entity_id(row: Mapping[str, Any], *, kind: EntityKind) -> str:
    """Return the `export_entity_id` of an entity row, which also carries `idx`.

    Raises `MissingExportedEntityError` naming the entity when the value is NULL.
    """
    export_entity_id = row["export_entity_id"]
    if export_entity_id is None:
        raise MissingExportedEntityError([row["idx"]], kind=kind)
    return export_entity_id


async def fetch_exported_entities(
    pool: asyncpg.Pool | asyncpg.Connection,
    *,
    study_idx: list[int],
    biosample_idx: list[int],
) -> list[asyncpg.Record]:
    """Return the handle of every named study and biosample — study entries first
    ascending, then biosample entries ascending.

    Raises `MissingExportedEntityError` rather than returning a short list.
    """
    requested = {"study": study_idx, "biosample": biosample_idx}
    rows = await pool.fetch(_SELECT, study_idx, biosample_idx)

    # Per kind, so a gap names the entities it is actually about.
    for kind, column in _ENTITY_COLUMN.items():
        missing = missing_from(rows, requested[kind], key=column)
        if missing:
            raise MissingExportedEntityError(missing, kind=kind)
    return rows
