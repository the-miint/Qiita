"""In-process sweeper that reclaims plaintext PATs left in cli_login_code.

`cli_login_code` is the only place qiita stores a plaintext token at rest. Its
plaintext is scrubbed the instant an ot_code is redeemed (see
POST /auth/cli-exchange), but a login the user *abandons* — code minted, never
redeemed — leaves the row, and its still-usable plaintext PAT, behind until it
is removed. Once per `CLI_LOGIN_CODE_SWEEP_INTERVAL_SECONDS` this sweeper
deletes every consumed or expired row: consumed rows already served their
purpose (their plaintext is NULL), and an expired-unconsumed code can never be
redeemed, so its plaintext is pure liability. This bounds how long any plaintext
lingers at rest to roughly one sweep interval past the (short) ot_code TTL.

Mirrors the notify sweeper's advisory-lock discipline so a second CP process
can't double-run it, but is far simpler: the lock is held only for a single
fast DELETE, so it borrows the shared request pool rather than a dedicated
connection.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from .db import rows_affected

if TYPE_CHECKING:
    import asyncpg

    from ..config import Settings

_log = logging.getLogger(__name__)

# Fixed advisory-lock key, chosen distinct from every other in-process sweeper's
# key so the sweepers never block each other. Only this sweeper uses it; a second
# CP process fails to acquire it and skips its pass.
_CLI_LOGIN_CODE_SWEEP_LOCK_KEY = 4_310_290_148


async def sweep_cli_login_codes_once(pool: asyncpg.Pool) -> int:
    """Delete every consumed or expired cli_login_code row; return the count.

    Advisory-locked so two CP processes don't both scan. The lock is held only
    for the DELETE (sub-millisecond), so briefly borrowing the request pool
    never starves request handling.
    """
    async with pool.acquire() as conn:
        acquired = await conn.fetchval(
            "SELECT pg_try_advisory_lock($1)", _CLI_LOGIN_CODE_SWEEP_LOCK_KEY
        )
        if not acquired:
            _log.debug("cli_login_code sweep: advisory lock held elsewhere; skipping pass")
            return 0
        try:
            tag = await conn.execute(
                "DELETE FROM qiita.cli_login_code"
                " WHERE consumed_at IS NOT NULL OR expires_at <= now()"
            )
        finally:
            await conn.fetchval("SELECT pg_advisory_unlock($1)", _CLI_LOGIN_CODE_SWEEP_LOCK_KEY)
    deleted = rows_affected(tag)
    if deleted:
        _log.info("cli_login_code sweep: deleted %d dead row(s)", deleted)
    return deleted


async def run_cli_login_code_sweeper(pool: asyncpg.Pool, settings: Settings) -> None:
    """Long-lived loop: one sweep per `CLI_LOGIN_CODE_SWEEP_INTERVAL_SECONDS`.

    A bad pass logs and continues — the loop must outlive any single failure.
    Cancelled at shutdown (the CancelledError propagates out to end the task).
    """
    _log.info(
        "cli_login_code sweeper started (interval=%ds)",
        settings.cli_login_code_sweep_interval_seconds,
    )
    while True:
        try:
            await sweep_cli_login_codes_once(pool)
        except asyncio.CancelledError:
            raise
        except Exception:
            _log.exception("cli_login_code sweep pass failed; continuing")
        await asyncio.sleep(settings.cli_login_code_sweep_interval_seconds)
