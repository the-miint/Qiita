"""Opaque API token mint / verify / last-used coalescing.

The DB stores SHA-256(plaintext) in qiita.api_tokens.token_hash (BYTEA UNIQUE).
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
from datetime import UTC, datetime

import asyncpg
from qiita_common.auth_constants import LAST_USED_AT_COALESCE_INTERVAL

from . import TOKEN_BODY_BYTES, TOKEN_HASH_BYTES, TOKEN_PREFIX, TOKEN_TOTAL_LEN
from .scopes import VALID_SCOPES

log = logging.getLogger(__name__)


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
    pool: asyncpg.Pool,
    *,
    principal_idx: int,
    label: str,
    scopes: list[str],
    expires_at: datetime | None = None,
) -> tuple[str, int]:
    """Mint a new opaque API token.

    Returns (plaintext, token_idx). Plaintext must be returned to the caller
    exactly once and never logged. Raises ValueError on unknown scopes; raises
    RuntimeError on token_hash collision (extraordinarily unlikely with 256
    bits of entropy — surfaced rather than silently shadowing another token).
    """
    unknown = set(scopes) - VALID_SCOPES
    if unknown:
        raise ValueError(f"Unknown scopes: {sorted(unknown)}")

    plaintext, token_hash = _generate_token()

    try:
        token_idx = await pool.fetchval(
            "INSERT INTO qiita.api_tokens"
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
        raise RuntimeError("Token hash collision — refusing to shadow") from exc

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

    row = await pool.fetchrow(
        "SELECT t.token_idx, t.principal_idx, t.scopes, t.expires_at,"
        "  p.disabled, p.retired"
        " FROM qiita.api_tokens t"
        " JOIN qiita.principal p ON p.idx = t.principal_idx"
        " WHERE t.token_hash = $1 AND t.revoked_at IS NULL",
        token_hash,
    )
    if row is None:
        return None
    if row["expires_at"] is not None and row["expires_at"] < datetime.now(UTC):
        return None
    if row["disabled"] or row["retired"]:
        return None

    asyncio.create_task(record_token_use(pool, row["token_idx"]))

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
            "UPDATE qiita.api_tokens SET last_used_at = now()"
            " WHERE token_idx = $1"
            "   AND (last_used_at IS NULL"
            f"        OR last_used_at < now() - interval '{LAST_USED_AT_COALESCE_INTERVAL}')",
            token_idx,
        )
    except asyncpg.PostgresError:
        log.warning("last_used_at update failed token_idx=%s", token_idx, exc_info=True)
