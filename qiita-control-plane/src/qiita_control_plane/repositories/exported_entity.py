"""Reads and the idempotent mint for `qiita.exported_entity`."""

import asyncpg

from ._exported_identifier_helpers import IncompleteMintError, missing_from

# The entity column per kind. A column name cannot be bound as a parameter, so the
# kind selects one from this closed mapping rather than from caller input.
_ENTITY_COLUMN = {"study": "study_idx", "biosample": "biosample_idx"}

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


async def mint_exported_entities(
    pool: asyncpg.Pool,
    *,
    study_idx: list[int],
    biosample_idx: list[int],
    created_by_idx: int,
) -> list[asyncpg.Record]:
    """Ensure a handle exists for every named study and biosample, and return them
    all — study entries first ascending, then biosample entries ascending.

    **Idempotent, and that is the contract rather than an optimization**: a handle
    is published, so asking twice for one entity must answer the same way both
    times. Concurrent callers need no lock — the per-kind unique constraint admits
    one row and `DO NOTHING` swallows the rest. Raises `IncompleteMintError` rather
    than returning a short list.
    """
    requested = {"study": study_idx, "biosample": biosample_idx}

    async with pool.acquire() as conn, conn.transaction():
        # One statement per kind, from one template: a kind varies only the column
        # name, which cannot be bound.
        for kind, column in _ENTITY_COLUMN.items():
            if not requested[kind]:
                continue
            await conn.execute(
                "INSERT INTO qiita.exported_entity"
                f"       ({column}, created_by_idx)"
                " SELECT entity_idx, $2"
                "   FROM unnest($1::bigint[]) AS entity_idx"
                f" ON CONFLICT ({column}) DO NOTHING",
                requested[kind],
                created_by_idx,
            )
        rows = await conn.fetch(_SELECT, study_idx, biosample_idx)

    # The read-back takes its own snapshot, so completeness is checked rather than
    # assumed. Per kind, so a gap names the entities it is actually about.
    for kind, column in _ENTITY_COLUMN.items():
        missing = missing_from(rows, requested[kind], key=column)
        if missing:
            raise IncompleteMintError(missing, kind=kind)
    return rows
