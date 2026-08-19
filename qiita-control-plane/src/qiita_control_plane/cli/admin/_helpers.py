"""qiita-admin CLI — shared direct-DB constants and the pool every direct-DB
subcommand opens.

Split out of the former single-file ``cli.admin`` module.
"""

import argparse
import functools
import os
import sys
from collections.abc import Callable

import asyncpg

# Direct-DB connection timeout for the bootstrap subcommand. Short because the
# DB is expected to be reachable on the operator's network; a multi-second
# stall here masks misconfiguration.
_DB_CONNECT_TIMEOUT_SECONDS = 5

# Exit code for a precondition the operator must fix before the command can run
# at all — an unset environment variable, an unusable argument — as opposed to
# a command that ran and failed, which is 1.
EXIT_PRECONDITION_FAILED = 2


def requires_database_url(
    handler: Callable[[argparse.Namespace, argparse.ArgumentParser, str], int],
) -> Callable[[argparse.Namespace, argparse.ArgumentParser], int]:
    """Run `handler` only when DATABASE_URL holds a value, passing it that value.

    Reports the absence on stderr and returns the precondition exit code
    otherwise, so a subcommand declares that it needs a direct-DB connection
    rather than restating the check, the message, and the code.
    """

    @functools.wraps(handler)
    def run(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            print("error: DATABASE_URL not set", file=sys.stderr)
            return EXIT_PRECONDITION_FAILED
        return handler(args, parser, database_url)

    return run


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
