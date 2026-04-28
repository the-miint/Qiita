"""Integration tests for mint_api_token / verify_api_token / record_token_use.

These need a real Postgres because verify joins qiita.api_tokens against
qiita.principal to enforce disabled/retired status.
"""

import asyncio
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest


# ---------------------------------------------------------------------------
# Per-test principal fixture
# ---------------------------------------------------------------------------


@pytest.fixture
async def principal_idx(postgres_pool):
    """Create a fresh active principal for the test, clean up after."""
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.principal"
        "  (display_name, system_role, created_by_idx)"
        " VALUES ('token-test', 'user', 1) RETURNING idx"
    )
    yield idx
    # Tokens FK to principal; delete tokens before principal.
    await postgres_pool.execute(
        "DELETE FROM qiita.api_tokens WHERE principal_idx = $1", idx
    )
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", idx)


# ---------------------------------------------------------------------------
# mint_api_token
# ---------------------------------------------------------------------------


async def test_mint_returns_plaintext_and_token_idx(postgres_pool, principal_idx):
    from qiita_control_plane.auth.tokens import mint_api_token

    plaintext, token_idx = await mint_api_token(
        postgres_pool,
        principal_idx=principal_idx,
        label="test-token",
        scopes=["references:read"],
    )
    assert plaintext.startswith("qk_")
    assert len(plaintext) == 46
    assert token_idx > 0


async def test_mint_validates_scopes_against_valid_set(postgres_pool, principal_idx):
    from qiita_control_plane.auth.tokens import mint_api_token

    with pytest.raises(ValueError, match="Unknown scopes"):
        await mint_api_token(
            postgres_pool,
            principal_idx=principal_idx,
            label="bad-scope",
            scopes=["references:read", "this:does:not:exist"],
        )


async def test_mint_persists_to_db(postgres_pool, principal_idx):
    """The minted row should be visible in qiita.api_tokens."""
    from qiita_control_plane.auth.tokens import mint_api_token

    _, token_idx = await mint_api_token(
        postgres_pool,
        principal_idx=principal_idx,
        label="persist-check",
        scopes=["references:read", "self:profile"],
    )
    row = await postgres_pool.fetchrow(
        "SELECT principal_idx, label, scopes, revoked_at, expires_at"
        " FROM qiita.api_tokens WHERE token_idx = $1",
        token_idx,
    )
    assert row["principal_idx"] == principal_idx
    assert row["label"] == "persist-check"
    assert set(row["scopes"]) == {"references:read", "self:profile"}
    assert row["revoked_at"] is None


async def test_mint_persists_expires_at(postgres_pool, principal_idx):
    from qiita_control_plane.auth.tokens import mint_api_token

    expires = datetime.now(UTC) + timedelta(days=90)
    _, token_idx = await mint_api_token(
        postgres_pool,
        principal_idx=principal_idx,
        label="expiring",
        scopes=[],
        expires_at=expires,
    )
    stored = await postgres_pool.fetchval(
        "SELECT expires_at FROM qiita.api_tokens WHERE token_idx = $1",
        token_idx,
    )
    # asyncpg returns aware UTC; allow microsecond precision drift
    assert abs((stored - expires).total_seconds()) < 1


async def test_mint_raises_on_hash_collision(postgres_pool, principal_idx, monkeypatch):
    """If the random body collides with an existing token_hash, mint raises."""
    from qiita_control_plane.auth import tokens

    # Fixed hash so we can pre-insert a matching row.
    fixed_plaintext = "qk_" + "A" * 43
    fixed_hash = __import__("hashlib").sha256(fixed_plaintext.encode("ascii")).digest()

    # Pre-insert a row with this hash.
    await postgres_pool.execute(
        "INSERT INTO qiita.api_tokens"
        "  (principal_idx, token_hash, label, scopes)"
        " VALUES ($1, $2, 'pre-existing', '{}'::text[])",
        principal_idx,
        fixed_hash,
    )

    monkeypatch.setattr(
        tokens, "_generate_token", lambda: (fixed_plaintext, fixed_hash)
    )
    with pytest.raises(RuntimeError, match="collision"):
        await tokens.mint_api_token(
            postgres_pool,
            principal_idx=principal_idx,
            label="will-collide",
            scopes=[],
        )


# ---------------------------------------------------------------------------
# verify_api_token
# ---------------------------------------------------------------------------


async def test_verify_valid_token_returns_principal_idx(postgres_pool, principal_idx):
    from qiita_control_plane.auth.tokens import mint_api_token, verify_api_token

    plaintext, token_idx = await mint_api_token(
        postgres_pool,
        principal_idx=principal_idx,
        label="verify-success",
        scopes=["references:read", "self:profile"],
    )
    verified = await verify_api_token(postgres_pool, plaintext)
    assert verified is not None
    assert verified.principal_idx == principal_idx
    assert verified.token_idx == token_idx
    assert verified.scopes == frozenset({"references:read", "self:profile"})


async def test_verify_rejects_unknown_token(postgres_pool):
    from qiita_control_plane.auth.tokens import verify_api_token

    fake = "qk_" + "Z" * 43
    assert await verify_api_token(postgres_pool, fake) is None


async def test_verify_rejects_revoked_token(postgres_pool, principal_idx):
    from qiita_control_plane.auth.tokens import mint_api_token, verify_api_token

    plaintext, token_idx = await mint_api_token(
        postgres_pool,
        principal_idx=principal_idx,
        label="will-revoke",
        scopes=[],
    )
    await postgres_pool.execute(
        "UPDATE qiita.api_tokens SET revoked_at = now() WHERE token_idx = $1",
        token_idx,
    )
    assert await verify_api_token(postgres_pool, plaintext) is None


async def test_verify_rejects_expired_token(postgres_pool, principal_idx):
    from qiita_control_plane.auth.tokens import mint_api_token, verify_api_token

    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=principal_idx,
        label="expired",
        scopes=[],
        expires_at=datetime.now(UTC) - timedelta(seconds=1),
    )
    assert await verify_api_token(postgres_pool, plaintext) is None


async def test_verify_rejects_token_for_disabled_principal(
    postgres_pool, principal_idx
):
    from qiita_control_plane.auth.tokens import mint_api_token, verify_api_token

    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=principal_idx,
        label="for-disabled",
        scopes=[],
    )
    await postgres_pool.execute(
        "UPDATE qiita.principal SET"
        "  disabled = true, disabled_at = now(), disabled_by_idx = 1"
        " WHERE idx = $1",
        principal_idx,
    )
    assert await verify_api_token(postgres_pool, plaintext) is None


async def test_verify_rejects_token_for_retired_principal(postgres_pool, principal_idx):
    from qiita_control_plane.auth.tokens import mint_api_token, verify_api_token

    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=principal_idx,
        label="for-retired",
        scopes=[],
    )
    # Retiring also auto-revokes via the `tg_revoke_tokens_on_retire`
    # trigger — verify still rejects, but the rejection reason is "revoked",
    # not "retired". Either way, verify returns None.
    await postgres_pool.execute(
        "UPDATE qiita.principal SET"
        "  retired = true, retired_at = now(), retired_by_idx = 1"
        " WHERE idx = $1",
        principal_idx,
    )
    assert await verify_api_token(postgres_pool, plaintext) is None


@pytest.mark.parametrize(
    "malformed",
    [
        "",
        "not_a_token",
        "qk_short",  # right prefix, wrong length
        "qk_" + "X" * 42,  # one short of body
        "qk_" + "X" * 44,  # one over body
        "QK_" + "X" * 43,  # wrong-case prefix
        "ey" + "X" * 44,  # JWT-shape, not qk_
    ],
)
async def test_verify_rejects_malformed(postgres_pool, malformed):
    from qiita_control_plane.auth.tokens import verify_api_token

    assert await verify_api_token(postgres_pool, malformed) is None


# ---------------------------------------------------------------------------
# record_token_use coalescing & error swallowing
# ---------------------------------------------------------------------------


async def test_record_token_use_advances_last_used_at(postgres_pool, principal_idx):
    from qiita_control_plane.auth.tokens import mint_api_token, record_token_use

    _, token_idx = await mint_api_token(
        postgres_pool,
        principal_idx=principal_idx,
        label="last-used",
        scopes=[],
    )
    before = await postgres_pool.fetchval(
        "SELECT last_used_at FROM qiita.api_tokens WHERE token_idx = $1",
        token_idx,
    )
    assert before is None
    await record_token_use(postgres_pool, token_idx)
    after = await postgres_pool.fetchval(
        "SELECT last_used_at FROM qiita.api_tokens WHERE token_idx = $1",
        token_idx,
    )
    assert after is not None


async def test_record_token_use_coalesces_within_one_minute(
    postgres_pool, principal_idx
):
    """Two consecutive calls within 60s should advance last_used_at only once."""
    from qiita_control_plane.auth.tokens import mint_api_token, record_token_use

    _, token_idx = await mint_api_token(
        postgres_pool,
        principal_idx=principal_idx,
        label="coalesce",
        scopes=[],
    )
    await record_token_use(postgres_pool, token_idx)
    first = await postgres_pool.fetchval(
        "SELECT last_used_at FROM qiita.api_tokens WHERE token_idx = $1",
        token_idx,
    )
    await record_token_use(postgres_pool, token_idx)
    second = await postgres_pool.fetchval(
        "SELECT last_used_at FROM qiita.api_tokens WHERE token_idx = $1",
        token_idx,
    )
    assert first == second, "second call within 60s must not advance last_used_at"


async def test_record_token_use_swallows_db_error(monkeypatch, principal_idx):
    """A DB error inside record_token_use must not propagate."""
    from qiita_control_plane.auth.tokens import record_token_use

    class _BoomPool:
        async def execute(self, *args, **kwargs):
            raise asyncpg.PostgresError("simulated outage")

    # Should not raise.
    await record_token_use(_BoomPool(), token_idx=1)


async def test_verify_does_not_block_on_last_used_at(
    postgres_pool, principal_idx, monkeypatch
):
    """If record_token_use raises, verify still returns success."""
    from qiita_control_plane.auth import tokens

    plaintext, _ = await tokens.mint_api_token(
        postgres_pool,
        principal_idx=principal_idx,
        label="non-blocking",
        scopes=[],
    )

    async def _boom(*args, **kwargs):
        raise asyncpg.PostgresError("simulated")

    monkeypatch.setattr(tokens, "record_token_use", _boom)
    verified = await tokens.verify_api_token(postgres_pool, plaintext)
    assert verified is not None
    # Allow any pending tasks to settle so the test doesn't leak a failed task.
    await asyncio.sleep(0)
