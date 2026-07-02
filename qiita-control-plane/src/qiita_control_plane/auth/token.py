"""Opaque API token mint / verify / last-used coalescing.

The DB stores SHA-256(plaintext) in qiita.api_token.token_hash (BYTEA UNIQUE).
Plaintext is shown exactly once at mint time and never logged. Verification
is a hash lookup with side checks for revocation, expiry, and principal
disabled/retired status. last_used_at writes are fire-and-forget and
coalesced to ≤1/min/token at the predicate level.
"""

import asyncio
import hashlib
import logging
import secrets
from dataclasses import dataclass
from datetime import datetime

import asyncpg
from qiita_common.auth_constants import LAST_USED_AT_COALESCE_INTERVAL

from . import TOKEN_BODY_BYTES, TOKEN_HASH_BYTES, TOKEN_PREFIX, TOKEN_TOTAL_LEN
from .scopes import VALID_SCOPES

log = logging.getLogger(__name__)

# Strong references to the fire-and-forget last_used_at tasks. asyncio holds
# only a weak reference to a bare create_task, so without this the task can be
# garbage-collected mid-flight before the last_used_at write lands. Each task
# removes itself on completion.
_background_tasks: set[asyncio.Task] = set()


class TokenHashCollision(RuntimeError):
    """SHA-256 collision on `qiita.api_token.token_hash`. Effectively
    impossible with 256 bits of entropy from `secrets.token_urlsafe(32)`
    plus a cryptographic digest; surfaced loudly rather than silently
    shadowing the colliding token. Inherits from RuntimeError so blanket
    handlers still catch it; named so tests / audit-replay tools can
    target it specifically.
    """


@dataclass(frozen=True, slots=True)
class VerifiedToken:
    principal_idx: int
    token_idx: int
    scopes: frozenset[str]


def _generate_token() -> tuple[str, bytes]:
    """Generate a fresh (plaintext, sha256) pair. Pure; no I/O."""
    plaintext = TOKEN_PREFIX + secrets.token_urlsafe(TOKEN_BODY_BYTES)
    digest = hashlib.sha256(plaintext.encode("ascii")).digest()
    assert len(digest) == TOKEN_HASH_BYTES, "sha256 must produce 32 bytes"
    return plaintext, digest


async def mint_api_token(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    principal_idx: int,
    label: str,
    scopes: list[str],
    expires_at: datetime | None = None,
) -> tuple[str, int]:
    """Mint a new opaque API token.

    Returns (plaintext, token_idx). Plaintext must be returned to the caller
    exactly once and never logged. Raises ValueError on unknown scopes;
    raises TokenHashCollision (a RuntimeError subclass) on the
    extraordinarily unlikely event of a token_hash collision — surfaced
    rather than silently shadowing another token.

    `pool_or_conn` accepts either an asyncpg.Pool or a Connection — the
    same `.fetchval(...)` API works for both, so callers can write inside
    an existing transaction by passing the connection.
    """
    unknown = set(scopes) - VALID_SCOPES
    if unknown:
        raise ValueError(f"Unknown scopes: {sorted(unknown)}")

    plaintext, token_hash = _generate_token()

    try:
        token_idx = await pool_or_conn.fetchval(
            "INSERT INTO qiita.api_token"
            "  (principal_idx, token_hash, label, scopes, expires_at)"
            " VALUES ($1, $2, $3, $4, $5)"
            " RETURNING token_idx",
            principal_idx,
            token_hash,
            label,
            list(scopes),
            expires_at,
        )
    except asyncpg.UniqueViolationError as exc:
        # 256 bits of entropy + cryptographic hash → effectively impossible.
        # Surfacing rather than silently overwriting protects against an
        # attacker-engineered collision (also impossible, but: principle).
        raise TokenHashCollision("token_hash collision — refusing to shadow") from exc

    return plaintext, token_idx


async def verify_api_token(pool: asyncpg.Pool, plaintext: str) -> VerifiedToken | None:
    """Verify a presented opaque token. Returns None on any rejection.

    Rejection conditions (all return None):
    - Plaintext doesn't start with `qk_` (malformed prefix)
    - Plaintext length wrong
    - No matching active (revoked_at IS NULL) row
    - Token expired (expires_at < now())
    - Owning principal is disabled or retired

    On success, schedules a fire-and-forget last_used_at update.
    """
    if not plaintext.startswith(TOKEN_PREFIX):
        return None
    if len(plaintext) != TOKEN_TOTAL_LEN:
        return None

    token_hash = hashlib.sha256(plaintext.encode("ascii")).digest()

    # Every rejection condition lives in the WHERE clause so we get a
    # single row-or-None answer with one DB clock reading. Splitting
    # `revoked_at IS NULL` from `expires_at < now()` between SQL and
    # Python would make verification depend on whether the DB and Python
    # process clocks agree.
    row = await pool.fetchrow(
        "SELECT t.token_idx, t.principal_idx, t.scopes"
        " FROM qiita.api_token t"
        " JOIN qiita.principal p ON p.idx = t.principal_idx"
        " WHERE t.token_hash = $1"
        "   AND t.revoked_at IS NULL"
        "   AND (t.expires_at IS NULL OR t.expires_at > now())"
        "   AND NOT p.disabled"
        "   AND NOT p.retired",
        token_hash,
    )
    if row is None:
        return None

    task = asyncio.create_task(record_token_use(pool, row["token_idx"]))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)

    return VerifiedToken(
        principal_idx=row["principal_idx"],
        token_idx=row["token_idx"],
        scopes=frozenset(row["scopes"]),
    )


async def record_token_use(pool: asyncpg.Pool, token_idx: int) -> None:
    """Coalesce last_used_at writes to once-per-minute per token.

    Skipped on the row level (predicate) and on the connection level
    (try/except). last_used_at is observability, not auth state — never
    block verify on it.
    """
    try:
        await pool.execute(
            "UPDATE qiita.api_token SET last_used_at = now()"
            " WHERE token_idx = $1"
            "   AND (last_used_at IS NULL"
            f"        OR last_used_at < now() - interval '{LAST_USED_AT_COALESCE_INTERVAL}')",
            token_idx,
        )
    except asyncpg.PostgresError:
        # Swallowed because we're invoked via asyncio.create_task — no caller
        # to receive a raise. `warning` (not `error`) because last_used_at is
        # observability-only and the auth flow already succeeded; if Prometheus
        # lands, a counter on this branch is the right escalation path.
        log.warning("last_used_at update failed token_idx=%s", token_idx, exc_info=True)
