"""Tests for auth schema — user / service_account subtypes of principal,
api_token, auth_event, plus the disabled flag added to principal.

The auth migration adds:
- ALTER TABLE qiita.principal: disabled flag + audit cols + CHECK constraints
- Seed: system principal at idx=1
- New tables: qiita.user, qiita.user_identity, qiita.service_account,
  qiita.api_token, qiita.auth_event
- Triggers: subtype mutual exclusion, auth_event immutability,
  token revocation on retirement
"""

import asyncpg
import pytest
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX, SystemRole

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _insert_principal(
    conn: asyncpg.Connection,
    *,
    display_name: str,
    system_role: str = SystemRole.USER,
    created_by_idx: int = SYSTEM_PRINCIPAL_IDX,
) -> int:
    """Insert a fresh principal scoped to the caller's transaction."""
    return await conn.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, $3) RETURNING idx",
        display_name,
        system_role,
        created_by_idx,
    )


# ---------------------------------------------------------------------------
# Table existence
# ---------------------------------------------------------------------------


EXPECTED_AUTH_TABLES = [
    "user",
    "user_identity",
    "service_account",
    "api_token",
    "auth_event",
]


@pytest.mark.parametrize("table", EXPECTED_AUTH_TABLES)
async def test_auth_table_exists(postgres_pool, table):
    exists = await postgres_pool.fetchval(
        "SELECT EXISTS("
        "  SELECT 1 FROM information_schema.tables"
        "  WHERE table_schema = 'qiita' AND table_name = $1"
        ")",
        table,
    )
    assert exists, f"Table qiita.{table} does not exist"


# ---------------------------------------------------------------------------
# disabled flag on principal
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("column", ["disabled", "disabled_at", "disabled_by_idx", "disable_reason"])
async def test_principal_disabled_columns_added(postgres_pool, column):
    exists = await postgres_pool.fetchval(
        "SELECT EXISTS("
        "  SELECT 1 FROM information_schema.columns"
        "  WHERE table_schema = 'qiita' AND table_name = 'principal'"
        "  AND column_name = $1"
        ")",
        column,
    )
    assert exists, f"qiita.principal.{column} does not exist"


async def test_principal_disabled_consistent_check_rejects_partial(postgres_pool):
    """disabled=true with NULL audit cols must be rejected by the CHECK."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            idx = await _insert_principal(conn, display_name="partial-disable-test")
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(
                    "UPDATE qiita.principal SET disabled = true WHERE idx = $1",
                    idx,
                )
        finally:
            await tr.rollback()


async def test_principal_not_both_disabled_and_retired_check(postgres_pool):
    """A principal cannot be simultaneously disabled and retired."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            actor = await _insert_principal(conn, display_name="actor-mutex")
            target = await _insert_principal(conn, display_name="target-mutex")
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(
                    "UPDATE qiita.principal SET"
                    "  disabled = true, disabled_at = now(), disabled_by_idx = $2,"
                    "  retired = true, retired_at = now(), retired_by_idx = $2"
                    " WHERE idx = $1",
                    target,
                    actor,
                )
        finally:
            await tr.rollback()


async def test_principal_disabled_round_trip(postgres_pool):
    """A consistent disabled state (all cols set or all unset) round-trips."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            actor = await _insert_principal(conn, display_name="actor-disable-rt")
            target = await _insert_principal(conn, display_name="target-disable-rt")
            await conn.execute(
                "UPDATE qiita.principal SET"
                "  disabled = true, disabled_at = now(),"
                "  disabled_by_idx = $2, disable_reason = 'investigation'"
                " WHERE idx = $1",
                target,
                actor,
            )
            row = await conn.fetchrow(
                "SELECT disabled, disabled_at, disabled_by_idx, disable_reason"
                " FROM qiita.principal WHERE idx = $1",
                target,
            )
            assert row["disabled"] is True
            assert row["disabled_at"] is not None
            assert row["disabled_by_idx"] == actor
            assert row["disable_reason"] == "investigation"
        finally:
            await tr.rollback()


# ---------------------------------------------------------------------------
# System principal seed
# ---------------------------------------------------------------------------


async def test_system_principal_seeded_at_idx_1(postgres_pool):
    row = await postgres_pool.fetchrow(
        "SELECT display_name, system_role, created_by_idx FROM qiita.principal WHERE idx = $1",
        SYSTEM_PRINCIPAL_IDX,
    )
    assert row is not None, "System principal not seeded at idx=1"
    assert row["display_name"] == "system"
    assert row["system_role"] == SystemRole.SYSTEM_ADMIN
    assert row["created_by_idx"] == SYSTEM_PRINCIPAL_IDX, "System principal must self-reference"


async def test_system_principal_has_no_subtype_rows(postgres_pool):
    user_row = await postgres_pool.fetchval("SELECT 1 FROM qiita.user WHERE principal_idx = 1")
    service_row = await postgres_pool.fetchval(
        "SELECT 1 FROM qiita.service_account WHERE principal_idx = 1"
    )
    assert user_row is None
    assert service_row is None


async def test_principal_idx_sequence_advanced_past_1(postgres_pool):
    """A default INSERT after seed must yield idx >= 2."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            idx = await _insert_principal(conn, display_name="post-seed-test")
            assert idx >= 2
        finally:
            await tr.rollback()


async def test_system_principal_cannot_be_disabled(postgres_pool):
    """The principal_system_principal_always_active CHECK forbids it."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            actor = await _insert_principal(conn, display_name="actor-sys-disable")
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(
                    "UPDATE qiita.principal SET"
                    "  disabled = true, disabled_at = now(), disabled_by_idx = $1,"
                    "  disable_reason = 'cannot disable system'"
                    " WHERE idx = 1",
                    actor,
                )
        finally:
            await tr.rollback()


async def test_system_principal_cannot_be_retired(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            actor = await _insert_principal(conn, display_name="actor-sys-retire")
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(
                    "UPDATE qiita.principal SET"
                    "  retired = true, retired_at = now(), retired_by_idx = $1,"
                    "  retire_reason = 'cannot retire system'"
                    " WHERE idx = 1",
                    actor,
                )
        finally:
            await tr.rollback()


# ---------------------------------------------------------------------------
# user subtype
# ---------------------------------------------------------------------------


async def test_user_subtype_rejects_sentinel_principal(postgres_pool):
    """Cannot insert a user row for principal_idx = 1 (the system principal)."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(
                    "INSERT INTO qiita.user (principal_idx, email) VALUES (1, 'system@example.com')"
                )
        finally:
            await tr.rollback()


async def test_email_uniqueness_is_case_insensitive(postgres_pool):
    """CITEXT column means 'Alice@example.com' == 'alice@example.com'."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            p1 = await _insert_principal(conn, display_name="alice")
            p2 = await _insert_principal(conn, display_name="alice2")
            await conn.execute(
                "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, 'Alice@Example.com')",
                p1,
            )
            with pytest.raises(asyncpg.UniqueViolationError):
                await conn.execute(
                    "INSERT INTO qiita.user (principal_idx, email)"
                    " VALUES ($1, 'alice@example.com')",
                    p2,
                )
        finally:
            await tr.rollback()


@pytest.mark.parametrize(
    "orcid",
    [
        "0000-0002-1825-0097",
        "0000-0002-1694-233X",
        None,
    ],
)
async def test_orcid_check_accepts_valid(postgres_pool, orcid):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            idx = await _insert_principal(conn, display_name=f"orcid-{orcid}")
            await conn.execute(
                "INSERT INTO qiita.user (principal_idx, email, orcid) VALUES ($1, $2, $3)",
                idx,
                f"u{idx}@example.com",
                orcid,
            )
        finally:
            await tr.rollback()


@pytest.mark.parametrize(
    "orcid",
    [
        "not-an-orcid",
        "0000-0002-1825-009",  # too short
        "abcd-efgh-ijkl-mnop",
        # NOTE: "too long" is rejected by VARCHAR(19) before reaching the CHECK,
        # so it raises StringDataRightTruncationError, not CheckViolationError —
        # not exercising the regex. Skipped here.
    ],
)
async def test_orcid_check_rejects_invalid(postgres_pool, orcid):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            idx = await _insert_principal(conn, display_name=f"badorcid-{orcid}")
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(
                    "INSERT INTO qiita.user (principal_idx, email, orcid) VALUES ($1, $2, $3)",
                    idx,
                    f"u{idx}@example.com",
                    orcid,
                )
        finally:
            await tr.rollback()


async def test_profile_complete_generated_column(postgres_pool):
    """profile_complete is True iff affiliation, address, phone all nonempty."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            idx = await _insert_principal(conn, display_name="profile-test")
            await conn.execute(
                "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
                idx,
                f"u{idx}@example.com",
            )
            row = await conn.fetchrow(
                "SELECT profile_complete FROM qiita.user WHERE principal_idx = $1",
                idx,
            )
            assert row["profile_complete"] is False

            await conn.execute(
                "UPDATE qiita.user SET affiliation = 'A', address = 'B', phone = 'C'"
                " WHERE principal_idx = $1",
                idx,
            )
            row = await conn.fetchrow(
                "SELECT profile_complete FROM qiita.user WHERE principal_idx = $1",
                idx,
            )
            assert row["profile_complete"] is True
        finally:
            await tr.rollback()


async def test_profile_complete_cannot_be_written_directly(postgres_pool):
    """GENERATED ALWAYS column rejects explicit writes."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            idx = await _insert_principal(conn, display_name="profile-write-test")
            # Postgres rejects writes to GENERATED ALWAYS columns with SQLSTATE
            # 428C9 (generated_always). asyncpg surfaces this under the
            # PostgresError hierarchy; the exact subclass varies by version,
            # so match on the base class and assert the error message.
            with pytest.raises(asyncpg.PostgresError) as exc:
                await conn.execute(
                    "INSERT INTO qiita.user (principal_idx, email, profile_complete)"
                    " VALUES ($1, $2, true)",
                    idx,
                    f"u{idx}@example.com",
                )
            # Sanity: this is the generated_always sqlstate (428C9), not
            # something else like a missing column or syntax error.
            assert exc.value.sqlstate == "428C9"
        finally:
            await tr.rollback()


async def test_user_updated_at_uses_shared_trigger(postgres_pool):
    """qiita.set_updated_at() is wired to qiita.user.

    We assert the trigger is registered (not its empirical behavior), because
    set_updated_at uses now(), which returns the transaction start time —
    within a single test transaction, multiple INSERT/UPDATE calls all see
    the same timestamp. The trigger advances updated_at correctly across
    real (committed) transactions; that's the production path.
    """
    row = await postgres_pool.fetchrow(
        "SELECT t.tgname, p.proname"
        " FROM pg_trigger t"
        " JOIN pg_class c ON c.oid = t.tgrelid"
        " JOIN pg_namespace n ON n.oid = c.relnamespace"
        " JOIN pg_proc p ON p.oid = t.tgfoid"
        " WHERE n.nspname = 'qiita' AND c.relname = 'user'"
        "   AND t.tgname = 'user_set_updated_at'"
        "   AND NOT t.tgisinternal"
    )
    assert row is not None, "user_set_updated_at trigger not registered on qiita.user"
    assert row["proname"] == "set_updated_at", (
        f"expected qiita.set_updated_at, got {row['proname']!r}"
    )


# ---------------------------------------------------------------------------
# service_account subtype
# ---------------------------------------------------------------------------


async def test_service_account_subtype_rejects_sentinel_principal(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(
                    "INSERT INTO qiita.service_account (principal_idx, name)"
                    " VALUES (1, 'system-as-service')"
                )
        finally:
            await tr.rollback()


async def test_service_account_name_unique(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            p1 = await _insert_principal(conn, display_name="svc-a")
            p2 = await _insert_principal(conn, display_name="svc-b")
            await conn.execute(
                "INSERT INTO qiita.service_account (principal_idx, name)"
                " VALUES ($1, 'orchestrator')",
                p1,
            )
            with pytest.raises(asyncpg.UniqueViolationError):
                await conn.execute(
                    "INSERT INTO qiita.service_account (principal_idx, name)"
                    " VALUES ($1, 'orchestrator')",
                    p2,
                )
        finally:
            await tr.rollback()


# ---------------------------------------------------------------------------
# Subtype mutual exclusion
# ---------------------------------------------------------------------------


async def test_subtype_mutual_exclusion_user_then_service(postgres_pool):
    """A principal that is already a user cannot also become a service_account."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            idx = await _insert_principal(conn, display_name="user-then-svc")
            await conn.execute(
                "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
                idx,
                f"u{idx}@example.com",
            )
            with pytest.raises(asyncpg.RaiseError):
                await conn.execute(
                    "INSERT INTO qiita.service_account (principal_idx, name)"
                    " VALUES ($1, 'cannot-also-be-svc')",
                    idx,
                )
        finally:
            await tr.rollback()


async def test_subtype_mutual_exclusion_service_then_user(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            idx = await _insert_principal(conn, display_name="svc-then-user")
            await conn.execute(
                "INSERT INTO qiita.service_account (principal_idx, name)"
                " VALUES ($1, 'svc-block-test')",
                idx,
            )
            with pytest.raises(asyncpg.RaiseError):
                await conn.execute(
                    "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
                    idx,
                    f"u{idx}@example.com",
                )
        finally:
            await tr.rollback()


async def test_subtype_exclusion_trigger_acquires_advisory_lock(postgres_pool):
    """Defends against silent removal of the pg_advisory_xact_lock that
    serializes concurrent INSERTs across the two subtype tables. Without
    the lock, two parallel INSERTs (one per table, same principal_idx)
    each see an empty other table in their EXISTS check and both succeed.
    """
    src = await postgres_pool.fetchval(
        "SELECT p.prosrc FROM pg_proc p"
        " JOIN pg_namespace n ON n.oid = p.pronamespace"
        " WHERE n.nspname = 'qiita'"
        "   AND p.proname = 'tg_principal_subtype_exclusion'"
    )
    assert src is not None, "trigger function tg_principal_subtype_exclusion missing"
    assert "pg_advisory_xact_lock" in src, (
        "subtype-exclusion trigger missing TOCTOU defense — "
        "pg_advisory_xact_lock must be the first call in the function body"
    )


async def test_subtype_independent_principals_ok(postgres_pool):
    """Two different principals — one user, one service — both succeed."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            p_user = await _insert_principal(conn, display_name="indep-user")
            p_svc = await _insert_principal(conn, display_name="indep-svc")
            await conn.execute(
                "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
                p_user,
                f"u{p_user}@example.com",
            )
            await conn.execute(
                "INSERT INTO qiita.service_account (principal_idx, name)"
                " VALUES ($1, 'indep-svc-name')",
                p_svc,
            )
        finally:
            await tr.rollback()


# ---------------------------------------------------------------------------
# user_identity
# ---------------------------------------------------------------------------


async def test_user_identity_pk_uniqueness(postgres_pool):
    """(issuer, subject) is the PK; same pair cannot be inserted twice."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            p1 = await _insert_principal(conn, display_name="ident-pk-1")
            p2 = await _insert_principal(conn, display_name="ident-pk-2")
            for p in (p1, p2):
                await conn.execute(
                    "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
                    p,
                    f"u{p}@example.com",
                )
            await conn.execute(
                "INSERT INTO qiita.user_identity (principal_idx, issuer, subject)"
                " VALUES ($1, 'iss-A', 'sub-A')",
                p1,
            )
            with pytest.raises(asyncpg.UniqueViolationError):
                await conn.execute(
                    "INSERT INTO qiita.user_identity"
                    "  (principal_idx, issuer, subject)"
                    " VALUES ($1, 'iss-A', 'sub-A')",
                    p2,
                )
        finally:
            await tr.rollback()


async def test_user_identity_fk_to_user_subtype(postgres_pool):
    """A user_identity row must reference an existing user (not just a principal)."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            # Bare principal — no user row.
            bare = await _insert_principal(conn, display_name="ident-fk-bare")
            with pytest.raises(asyncpg.ForeignKeyViolationError):
                await conn.execute(
                    "INSERT INTO qiita.user_identity"
                    "  (principal_idx, issuer, subject)"
                    " VALUES ($1, 'iss', 'sub')",
                    bare,
                )
        finally:
            await tr.rollback()


async def test_user_identity_on_delete_restrict(postgres_pool):
    """user_identity -> user uses ON DELETE RESTRICT (no CASCADE)."""
    rule = await postgres_pool.fetchval(
        "SELECT rc.delete_rule"
        " FROM information_schema.table_constraints tc"
        " JOIN information_schema.referential_constraints rc"
        "   ON tc.constraint_name = rc.constraint_name"
        "   AND tc.table_schema = rc.constraint_schema"
        " WHERE tc.table_schema = 'qiita'"
        "   AND tc.table_name = 'user_identity'"
        "   AND tc.constraint_type = 'FOREIGN KEY'"
    )
    assert rule in ("NO ACTION", "RESTRICT"), f"got {rule!r}"


# ---------------------------------------------------------------------------
# api_token
# ---------------------------------------------------------------------------


async def test_api_token_check_no_sentinel_principal(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(
                    "INSERT INTO qiita.api_token"
                    "  (principal_idx, token_hash, label) VALUES (1, $1, 'x')",
                    b"\x00" * 32,
                )
        finally:
            await tr.rollback()


async def test_api_token_hash_active_index_partial(postgres_pool):
    """The api_token_hash_active index must be partial on revoked_at IS NULL."""
    pred = await postgres_pool.fetchval(
        "SELECT indexdef FROM pg_indexes"
        " WHERE schemaname = 'qiita' AND indexname = 'api_token_hash_active'"
    )
    assert pred is not None, "api_token_hash_active index missing"
    assert "WHERE" in pred and "revoked_at IS NULL" in pred


async def test_api_token_hash_unique(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            p = await _insert_principal(conn, display_name="token-hash-test")
            h = b"\x42" * 32
            await conn.execute(
                "INSERT INTO qiita.api_token"
                "  (principal_idx, token_hash, label) VALUES ($1, $2, 'a')",
                p,
                h,
            )
            with pytest.raises(asyncpg.UniqueViolationError):
                await conn.execute(
                    "INSERT INTO qiita.api_token"
                    "  (principal_idx, token_hash, label) VALUES ($1, $2, 'b')",
                    p,
                    h,
                )
        finally:
            await tr.rollback()


# ---------------------------------------------------------------------------
# Retirement / disable token revocation
# ---------------------------------------------------------------------------


async def test_retirement_revokes_tokens_trigger(postgres_pool):
    """Setting retired=true revokes all active tokens for that principal."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            actor = await _insert_principal(conn, display_name="retire-actor")
            target = await _insert_principal(conn, display_name="retire-target")
            for i in range(2):
                await conn.execute(
                    "INSERT INTO qiita.api_token"
                    "  (principal_idx, token_hash, label) VALUES ($1, $2, $3)",
                    target,
                    bytes([i + 1]) * 32,
                    f"tok-{i}",
                )
            await conn.execute(
                "UPDATE qiita.principal SET"
                "  retired = true, retired_at = now(), retired_by_idx = $2"
                " WHERE idx = $1",
                target,
                actor,
            )
            n_active = await conn.fetchval(
                "SELECT count(*) FROM qiita.api_token"
                " WHERE principal_idx = $1 AND revoked_at IS NULL",
                target,
            )
            n_total = await conn.fetchval(
                "SELECT count(*) FROM qiita.api_token WHERE principal_idx = $1",
                target,
            )
            assert n_active == 0
            assert n_total == 2
        finally:
            await tr.rollback()


async def test_retirement_does_not_touch_other_principals_tokens(postgres_pool):
    """Retiring principal A must not affect principal B's tokens."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            actor = await _insert_principal(conn, display_name="retire-cross-actor")
            target = await _insert_principal(conn, display_name="retire-cross-target")
            other = await _insert_principal(conn, display_name="retire-cross-bystander")
            await conn.execute(
                "INSERT INTO qiita.api_token"
                "  (principal_idx, token_hash, label) VALUES ($1, $2, 'survivor')",
                other,
                b"\x33" * 32,
            )
            await conn.execute(
                "UPDATE qiita.principal SET"
                "  retired = true, retired_at = now(), retired_by_idx = $2"
                " WHERE idx = $1",
                target,
                actor,
            )
            other_revoked = await conn.fetchval(
                "SELECT revoked_at FROM qiita.api_token WHERE principal_idx = $1",
                other,
            )
            assert other_revoked is None
        finally:
            await tr.rollback()


async def test_retirement_preserves_already_revoked_at(postgres_pool):
    """The trigger's WHERE revoked_at IS NULL clause must not overwrite."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            actor = await _insert_principal(conn, display_name="retire-preserve-actor")
            target = await _insert_principal(conn, display_name="retire-preserve-target")
            await conn.execute(
                "INSERT INTO qiita.api_token"
                "  (principal_idx, token_hash, label, revoked_at)"
                " VALUES ($1, $2, 'pre-revoked', '2020-01-01T00:00:00Z')",
                target,
                b"\x44" * 32,
            )
            await conn.execute(
                "UPDATE qiita.principal SET"
                "  retired = true, retired_at = now(), retired_by_idx = $2"
                " WHERE idx = $1",
                target,
                actor,
            )
            preserved = await conn.fetchval(
                "SELECT revoked_at FROM qiita.api_token WHERE principal_idx = $1",
                target,
            )
            assert preserved.year == 2020
        finally:
            await tr.rollback()


async def test_disabling_does_not_revoke_tokens(postgres_pool):
    """Setting disabled=true does NOT revoke tokens (admin can bulk-revoke)."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            actor = await _insert_principal(conn, display_name="disable-actor")
            target = await _insert_principal(conn, display_name="disable-target")
            await conn.execute(
                "INSERT INTO qiita.api_token"
                "  (principal_idx, token_hash, label) VALUES ($1, $2, 'live')",
                target,
                b"\x09" * 32,
            )
            await conn.execute(
                "UPDATE qiita.principal SET"
                "  disabled = true, disabled_at = now(), disabled_by_idx = $2"
                " WHERE idx = $1",
                target,
                actor,
            )
            n_active = await conn.fetchval(
                "SELECT count(*) FROM qiita.api_token"
                " WHERE principal_idx = $1 AND revoked_at IS NULL",
                target,
            )
            assert n_active == 1
        finally:
            await tr.rollback()


# ---------------------------------------------------------------------------
# auth_event immutability
# ---------------------------------------------------------------------------


async def test_auth_event_insert_succeeds(postgres_pool):
    """Append-only means INSERT works; only UPDATE/DELETE are blocked.

    Without this positive test, a regression that broke
    tg_auth_event_immutable to fire on INSERT would slip past the suite.
    """
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            actor = await _insert_principal(conn, display_name="evt-insert-actor")
            event_idx = await conn.fetchval(
                "INSERT INTO qiita.auth_event"
                "  (event_type, principal_idx, actor_principal_idx, detail)"
                "  VALUES ('token_mint', $1, $1, '{\"ip\":\"127.0.0.1\"}'::jsonb)"
                " RETURNING event_idx",
                actor,
            )
            assert event_idx is not None
        finally:
            await tr.rollback()


async def test_auth_event_immutable_update_raises(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            actor = await _insert_principal(conn, display_name="evt-actor")
            event_idx = await conn.fetchval(
                "INSERT INTO qiita.auth_event"
                "  (event_type, principal_idx, actor_principal_idx, detail)"
                "  VALUES ('token_mint', $1, $1, '{}'::jsonb)"
                " RETURNING event_idx",
                actor,
            )
            with pytest.raises(asyncpg.RaiseError):
                await conn.execute(
                    "UPDATE qiita.auth_event SET event_type = 'tampered' WHERE event_idx = $1",
                    event_idx,
                )
        finally:
            await tr.rollback()


async def test_auth_event_immutable_delete_raises(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            actor = await _insert_principal(conn, display_name="evt-del-actor")
            event_idx = await conn.fetchval(
                "INSERT INTO qiita.auth_event"
                "  (event_type, principal_idx, actor_principal_idx, detail)"
                "  VALUES ('token_use', $1, $1, '{}'::jsonb)"
                " RETURNING event_idx",
                actor,
            )
            with pytest.raises(asyncpg.RaiseError):
                await conn.execute(
                    "DELETE FROM qiita.auth_event WHERE event_idx = $1",
                    event_idx,
                )
        finally:
            await tr.rollback()


# ---------------------------------------------------------------------------
# No CASCADE on auth-table FKs
# ---------------------------------------------------------------------------


AUTH_TABLES_SET = {
    "user",
    "user_identity",
    "service_account",
    "api_token",
    "auth_event",
}


async def test_no_cascade_on_auth_table_fks(postgres_pool):
    """All FKs originating in the auth tables use NO ACTION or RESTRICT."""
    rows = await postgres_pool.fetch(
        "SELECT tc.table_name, tc.constraint_name, rc.delete_rule"
        " FROM information_schema.table_constraints tc"
        " JOIN information_schema.referential_constraints rc"
        "   ON tc.constraint_name = rc.constraint_name"
        "   AND tc.table_schema = rc.constraint_schema"
        " WHERE tc.table_schema = 'qiita'"
        "   AND tc.constraint_type = 'FOREIGN KEY'"
        "   AND tc.table_name = ANY($1::text[])",
        list(AUTH_TABLES_SET),
    )
    assert rows, "expected at least one FK across auth tables"
    bad = [
        (r["table_name"], r["constraint_name"], r["delete_rule"])
        for r in rows
        if r["delete_rule"] not in ("NO ACTION", "RESTRICT")
    ]
    assert not bad, f"non-NO-ACTION/RESTRICT FKs found: {bad}"
