"""Reads of `qiita.exported_entity`."""

from collections.abc import Mapping
from typing import Any, Literal, get_args

import asyncpg

EntityKind = Literal["study", "biosample"]

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
    study and biosample holds an export_entity_id, so each missing identifier names
    either no entity at all or an entity whose export_entity_id is unexpectedly
    absent.
    """

    def __init__(self, missing: list[int], *, kind: str) -> None:
        self.missing = missing
        self.kind = kind
        super().__init__(
            f"{len(missing)} {kind}(s) have no export_entity_id or do not exist: {missing}"
        )


def entity_idx_column(kind: EntityKind) -> str:
    """Return the exported_entity column that references a `kind` entity's idx.

    `kind` is also the entity's table name; both reach SQL text, so a value outside
    EntityKind is refused.
    """
    if kind not in get_args(EntityKind):
        raise ValueError(f"invalid entity kind: {kind!r}")
    column = f"{kind}_idx"
    return column


def export_entity_id_select(kind: EntityKind) -> str:
    """Return the select-list expression for an entity's `export_entity_id`.

    A scalar subquery, so an entity without a row reads NULL rather than dropping
    out of the result. The enclosing query must refer to the entity table by its
    own name, unaliased.
    """
    column = entity_idx_column(kind)
    expression = (
        "(SELECT export_entity_id FROM qiita.exported_entity ee"
        f" WHERE ee.{column} = {kind}.idx) AS export_entity_id"
    )
    return expression


def export_entity_id_join(kind: EntityKind) -> str:
    """Return a JOIN clause that brings an entity's exported_entity row in as `ee`.

    The enclosing query must refer to the entity table by its own name, unaliased.
    """
    column = entity_idx_column(kind)
    clause = f" JOIN qiita.exported_entity ee ON ee.{column} = {kind}.idx"
    return clause


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

    # Per kind, so a gap names the entities it is actually about. A row of the
    # other kind carries NULL in this kind's column, so it matches nothing here.
    for kind in get_args(EntityKind):
        column = entity_idx_column(kind)
        returned = {row[column] for row in rows}
        missing = sorted(idx for idx in set(requested[kind]) if idx not in returned)
        if missing:
            raise MissingExportedEntityError(missing, kind=kind)
    return rows
