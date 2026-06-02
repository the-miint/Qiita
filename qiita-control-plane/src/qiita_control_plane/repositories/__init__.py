"""Repository modules holding the SQL for each resource.

Repo-wide helpers used by every kind of repository module — guards,
input validators, and the parameterized UPDATE composer shared across
study, biosample, prep_sample, and the rest — live in this package init.
"""

import json
from typing import Literal, get_args

import asyncpg

# Tables that expose a PATCH route. The set is 1:1 with the tables
# whose ETag is read from `updated_at`: a table that supports PATCH
# exposes the matching ETag, and a table that does not is also not
# ETag-readable. The table name is interpolated into the SQL, so the
# set is a closed Literal — never widen by accepting caller input
# directly. The runtime get_args() check inside each consumer rejects
# any string the Literal does not cover, since Python does not enforce
# Literal at runtime on its own.
UpdatableTable = Literal["biosample", "study"]


def require_transaction(conn: asyncpg.Connection) -> None:
    """Raise RuntimeError if conn is not currently inside a transaction.

    Use as the first call in any repository function whose writes must roll
    back atomically on partial failure. asyncpg has no static type that
    expresses the transactional contract, so this is enforced at runtime;
    the offending function appears in the traceback frame above.
    """
    if not conn.is_in_transaction():
        raise RuntimeError(
            "repository function called outside a transaction;"
            " wrap the call in `async with conn.transaction(): ...`"
            " — its writes must roll back atomically on partial failure."
        )


def validate_patch_fields(
    fields: dict[str, object],
    *,
    allowlist: frozenset[str],
    repo_name: str,
) -> None:
    """Reject empty / unknown-column PATCH inputs at the repo boundary.

    Every update_X composer that takes a column-keyed `fields` dict shares
    the same two failure modes: an empty dict yields an UPDATE with no
    SET clause (SQL error), and an unknown column name reaches the f-string
    SQL builder. Both are misuse at the repo boundary and surface as
    ValueError with messages naming `repo_name` so the traceback points
    at the offending composer. The route layer's Pydantic extra="forbid"
    already covers unknown columns from external callers; this guard
    catches bypass paths (tests, future internal callers).
    """
    # Empty dict — the SQL builder would emit an UPDATE with no SET clause.
    if not fields:
        raise ValueError(f"{repo_name} requires at least one field")

    # Unknown column name — the SQL builder would interpolate it into SET.
    unknown = set(fields) - allowlist
    if unknown:
        raise ValueError(f"{repo_name} rejects non-patchable column(s): {sorted(unknown)}")


async def update_row(
    conn: asyncpg.Connection,
    *,
    table: UpdatableTable,
    row_idx: int,
    fields: dict[str, object],
    allowlist: frozenset[str],
    returning_cols: str,
    jsonb_cols: frozenset[str] = frozenset(),
    repo_name: str,
) -> asyncpg.Record | None:
    """Update the named columns on qiita.<table> row idx=row_idx, return the post-UPDATE row.

    `fields` maps column name -> new value; only the listed keys are
    written, and explicit None sets the column to NULL. Unknown keys
    and an empty dict raise ValueError. Columns named in `jsonb_cols`
    are bound with a `::jsonb` cast and their values pre-serialized
    with `json.dumps`; everything else binds as a plain positional
    parameter. `returning_cols` is the comma-separated column list
    each per-repo wrapper already keeps as a constant so the post-
    UPDATE shape stays identical to that wrapper's fetch_X shape.
    Returns None when no row matches `row_idx` (possible even after
    a passing preflight: READ COMMITTED snapshots are per-statement).

    Raises asyncpg.PostgresError on FK violation or constraint failure.
    """
    # Runtime guard for the interpolated table name; the Literal alone
    # is a static hint, so reject any string outside its closed set
    # before building SQL.
    if table not in get_args(UpdatableTable):
        raise ValueError(f"{repo_name} rejects non-updatable table: {table!r}")

    validate_patch_fields(fields, allowlist=allowlist, repo_name=repo_name)

    # Build the parameterized SET clause. Column names come from the
    # caller's allowlist (frozen at module load) and are accepted only
    # after validate_patch_fields rejects anything outside it, so the
    # f-string interpolation of `col` is safe; per-column values are
    # passed as positional asyncpg parameters. jsonb_cols members get
    # a ::jsonb cast and the value is pre-serialized so asyncpg ships
    # it as text + the cast.
    columns = sorted(fields.keys())
    set_chunks: list[str] = []
    values: list[object] = []
    for i, col in enumerate(columns, start=1):
        if col in jsonb_cols:
            set_chunks.append(f"{col} = ${i}::jsonb")
            raw = fields[col]
            values.append(json.dumps(raw) if raw is not None else None)
        else:
            set_chunks.append(f"{col} = ${i}")
            values.append(fields[col])
    set_clause = ", ".join(set_chunks)
    row_param = f"${len(columns) + 1}"

    # Single round trip: UPDATE ... RETURNING with the same column list
    # the per-repo fetch wrapper selects.
    return await conn.fetchrow(
        f"UPDATE qiita.{table} SET {set_clause} WHERE idx = {row_param} RETURNING {returning_cols}",
        *values,
        row_idx,
    )
