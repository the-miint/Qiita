"""Repository modules holding the SQL for each resource.

Repo-wide helpers used by every kind of repository module — guards and
input validators shared across study, biosample, prep_sample, and the
rest — live in this package init.
"""

import asyncpg


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
