"""qiita-admin CLI — shared direct-DB constants and the pool every direct-DB
subcommand opens.

Split out of the former single-file ``cli.admin`` module.
"""

import asyncpg

# Direct-DB connection timeout for the bootstrap subcommand. Short because the
# DB is expected to be reachable on the operator's network; a multi-second
# stall here masks misconfiguration.
_DB_CONNECT_TIMEOUT_SECONDS = 5


async def open_admin_pool(database_url: str) -> asyncpg.Pool:
    """Open the small direct-DB pool the admin/backfill subcommands run against.

    Wraps every connect failure — bad URL, wrong credentials, host unreachable —
    in a RuntimeError naming the underlying exception, which is the one error type
    the handlers map to exit 1.
    """
    try:
        return await asyncpg.create_pool(
            database_url, timeout=_DB_CONNECT_TIMEOUT_SECONDS, min_size=1, max_size=4
        )
    except Exception as exc:  # noqa: BLE001 — show full reason, including OS errors
        raise RuntimeError(
            f"could not connect to DATABASE_URL: {type(exc).__name__}: {exc}"
        ) from exc
