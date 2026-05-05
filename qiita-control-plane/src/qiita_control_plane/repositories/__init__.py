"""Repository modules holding the SQL for each resource."""

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
