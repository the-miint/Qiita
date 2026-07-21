"""DB-bound trigger tests for qiita.study.

Exercises the role-typed FK triggers attached to qiita.study(owner_idx)
and qiita.study(principal_investigator_idx), plus the
qiita.user-delete-blocking trigger that fires when a study still
references a user.

Tests use Pattern 1 (transaction-rollback per test): all seed and
assertions happen inside a single transaction that is rolled back at
the end. No shared fixture, no FK-reverse cleanup. This pattern fits
trigger tests because triggers fire per-statement and the test does not
need to commit. Tests that exercise commit-time behavior or
cross-transaction scenarios use Pattern 2 (committed fixture +
FK-reverse cleanup) — see tests/repositories/test_biosample.py.
"""

import json
import secrets

import asyncpg
import pytest
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX, SystemRole
from qiita_common.models import Tier

from qiita_control_plane.repositories.study import (
    create_study,
    fetch_study,
    fetch_study_exists,
    fetch_study_idxs_by_accession,
    get_or_create_study_by_ena_accessions,
    update_study,
)

pytestmark = pytest.mark.db


def _suffix(label: str) -> str:
    return f"{label}-{secrets.token_hex(4)}"


async def _create_user(conn) -> int:
    """Return principal_idx of a freshly-minted user-kind principal."""
    name = _suffix("user")
    pidx = await conn.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, $3) RETURNING idx",
        name,
        SystemRole.USER,
        SYSTEM_PRINCIPAL_IDX,
    )
    await conn.execute(
        "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
        pidx,
        f"{name}@example.com",
    )
    return pidx


async def _create_service_account(conn) -> int:
    name = _suffix("svc")
    pidx = await conn.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, $3) RETURNING idx",
        name,
        SystemRole.USER,
        SYSTEM_PRINCIPAL_IDX,
    )
    await conn.execute(
        "INSERT INTO qiita.service_account (principal_idx, name) VALUES ($1, $2)",
        pidx,
        name,
    )
    return pidx


async def _create_bare_principal(conn) -> int:
    """Principal with no subtype row — represents an actor that cannot
    authenticate (e.g., a PI imported from an external system before
    they've logged in)."""
    return await conn.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, $3) RETURNING idx",
        _suffix("bare"),
        SystemRole.USER,
        SYSTEM_PRINCIPAL_IDX,
    )


async def _insert_study(
    conn,
    *,
    owner_idx: int,
    pi_idx: int | None = None,
    created_by_idx: int = SYSTEM_PRINCIPAL_IDX,
) -> int:
    return await conn.fetchval(
        "INSERT INTO qiita.study"
        "  (owner_idx, principal_investigator_idx, title, created_by_idx)"
        " VALUES ($1, $2, $3, $4) RETURNING idx",
        owner_idx,
        pi_idx,
        _suffix("study"),
        created_by_idx,
    )


# ---------------------------------------------------------------------------
# tg_principal_must_be_user — happy paths
# ---------------------------------------------------------------------------


async def test_study_insert_with_user_owner_and_pi_succeeds(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            pi = await _create_user(conn)
            study_idx = await _insert_study(conn, owner_idx=owner, pi_idx=pi)
            assert study_idx is not None
        finally:
            await tr.rollback()


async def test_study_insert_with_null_pi_succeeds(postgres_pool):
    """principal_investigator_idx is nullable; the trigger short-circuits on NULL."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            study_idx = await _insert_study(conn, owner_idx=owner, pi_idx=None)
            assert study_idx is not None
        finally:
            await tr.rollback()


# ---------------------------------------------------------------------------
# tg_principal_must_be_user — rejection paths
# ---------------------------------------------------------------------------


async def test_study_insert_with_service_account_owner_raises(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            svc = await _create_service_account(conn)
            with pytest.raises(asyncpg.RaiseError, match="must reference a user-kind principal"):
                await _insert_study(conn, owner_idx=svc)
        finally:
            await tr.rollback()


async def test_study_insert_with_bare_principal_owner_raises(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            bare = await _create_bare_principal(conn)
            with pytest.raises(asyncpg.RaiseError, match="must reference a user-kind principal"):
                await _insert_study(conn, owner_idx=bare)
        finally:
            await tr.rollback()


async def test_study_insert_with_service_account_pi_raises(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            svc = await _create_service_account(conn)
            with pytest.raises(asyncpg.RaiseError, match="must reference a user-kind principal"):
                await _insert_study(conn, owner_idx=owner, pi_idx=svc)
        finally:
            await tr.rollback()


async def test_study_update_owner_to_service_account_raises(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            svc = await _create_service_account(conn)
            study_idx = await _insert_study(conn, owner_idx=owner)
            with pytest.raises(asyncpg.RaiseError, match="must reference a user-kind principal"):
                await conn.execute(
                    "UPDATE qiita.study SET owner_idx = $1 WHERE idx = $2",
                    svc,
                    study_idx,
                )
        finally:
            await tr.rollback()


# ---------------------------------------------------------------------------
# tg_user_role_ref_blocks_delete — happy and rejection paths
# ---------------------------------------------------------------------------


async def test_user_delete_blocked_when_referenced_as_owner(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            await _insert_study(conn, owner_idx=owner)
            with pytest.raises(asyncpg.RaiseError, match="cannot delete qiita.user"):
                await conn.execute("DELETE FROM qiita.user WHERE principal_idx = $1", owner)
        finally:
            await tr.rollback()


async def test_user_delete_blocked_when_referenced_as_pi(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            pi = await _create_user(conn)
            await _insert_study(conn, owner_idx=owner, pi_idx=pi)
            with pytest.raises(asyncpg.RaiseError, match="cannot delete qiita.user"):
                await conn.execute("DELETE FROM qiita.user WHERE principal_idx = $1", pi)
        finally:
            await tr.rollback()


async def test_user_delete_succeeds_when_no_inbound_refs(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            unused = await _create_user(conn)
            await conn.execute("DELETE FROM qiita.user WHERE principal_idx = $1", unused)
            still_there = await conn.fetchval(
                "SELECT 1 FROM qiita.user WHERE principal_idx = $1", unused
            )
            assert still_there is None
        finally:
            await tr.rollback()


# ---------------------------------------------------------------------------
# create_study composer — happy and rejection paths
# ---------------------------------------------------------------------------


async def test_create_study_minimum_body_inserts_row_and_owner_admin_grant(postgres_pool):
    """A bare-minimum call inserts a study row with the schema defaults
    applied and adds an ADMIN study_access row for the owner with
    granted_by_idx = caller. All assertions inside one rolled-back
    transaction."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            title = _suffix("min-study")

            row = await create_study(
                conn,
                owner_idx=owner,
                created_by_idx=owner,
                title=title,
            )

            # Study row carries the requested title, defaulted columns NULL,
            # default_tier=member from the schema default.
            assert row["title"] == title
            assert row["owner_idx"] == owner
            assert row["created_by_idx"] == owner
            assert row["principal_investigator_idx"] is None
            assert row["alias"] is None
            assert row["description"] is None
            assert row["abstract"] is None
            assert row["funding"] is None
            assert row["ena_study_accession"] is None
            assert row["notes"] is None
            assert row["extra_metadata"] is None
            assert row["default_tier"] == "member"

            # Auto-grant: owner has an ADMIN access row, granted_by_idx=caller.
            grant = await conn.fetchrow(
                "SELECT access_tier, granted_by_idx"
                " FROM qiita.study_access"
                " WHERE study_idx = $1 AND principal_idx = $2",
                row["idx"],
                owner,
            )
            assert grant is not None
            assert grant["access_tier"] == "admin"
            assert grant["granted_by_idx"] == owner
        finally:
            await tr.rollback()


async def test_create_study_full_body_round_trips_every_field(postgres_pool):
    """Every settable column on StudyCreate, including the JSONB
    extra_metadata and a non-default default_tier, lands on the row."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            pi = await _create_user(conn)
            title = _suffix("full-study")
            extra = {"site": "ucsd", "season": "spring", "vamps_id": "VAMPS-1"}

            row = await create_study(
                conn,
                owner_idx=owner,
                created_by_idx=owner,
                title=title,
                principal_investigator_idx=pi,
                alias="alias-1",
                description="desc",
                abstract="abs",
                funding="NIH-R01",
                ena_study_accession="ERP000001",
                bioproject_accession="PRJNA000001",
                notes="notes-1",
                extra_metadata=extra,
                default_tier=Tier.VIEWER,
            )

            assert row["title"] == title
            assert row["principal_investigator_idx"] == pi
            assert row["alias"] == "alias-1"
            assert row["description"] == "desc"
            assert row["abstract"] == "abs"
            assert row["funding"] == "NIH-R01"
            assert row["ena_study_accession"] == "ERP000001"
            assert row["bioproject_accession"] == "PRJNA000001"
            assert row["notes"] == "notes-1"
            # asyncpg returns JSONB as a string; decode for comparison.
            assert json.loads(row["extra_metadata"]) == extra
            assert row["default_tier"] == "viewer"
        finally:
            await tr.rollback()


async def test_create_study_admin_on_behalf_grants_owner_not_caller(postgres_pool):
    """When the caller and the owner differ, the auto-grant ADMIN row
    targets the owner; granted_by_idx records the caller. The caller
    receives no study_access row."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            caller = await _create_user(conn)

            row = await create_study(
                conn,
                owner_idx=owner,
                created_by_idx=caller,
                title=_suffix("on-behalf"),
            )

            assert row["owner_idx"] == owner
            assert row["created_by_idx"] == caller

            # Owner has the ADMIN row, granted_by_idx=caller.
            owner_grant = await conn.fetchrow(
                "SELECT access_tier, granted_by_idx"
                " FROM qiita.study_access"
                " WHERE study_idx = $1 AND principal_idx = $2",
                row["idx"],
                owner,
            )
            assert owner_grant["access_tier"] == "admin"
            assert owner_grant["granted_by_idx"] == caller

            # Caller has no auto-granted row.
            caller_grant = await conn.fetchval(
                "SELECT 1 FROM qiita.study_access WHERE study_idx = $1 AND principal_idx = $2",
                row["idx"],
                caller,
            )
            assert caller_grant is None
        finally:
            await tr.rollback()


async def test_create_study_unknown_pi_idx_raises_user_kind_trigger(postgres_pool):
    """A principal_investigator_idx past the highest existing idx hits
    tg_principal_must_be_user before the FK check runs (BEFORE INSERT
    triggers fire ahead of constraint validation), so all bad-PI inputs
    surface as the same RaiseError regardless of root cause."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            max_idx = await conn.fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.principal")

            with pytest.raises(asyncpg.RaiseError, match="must reference a user-kind principal"):
                await create_study(
                    conn,
                    owner_idx=owner,
                    created_by_idx=owner,
                    title=_suffix("bad-pi"),
                    principal_investigator_idx=max_idx + 100_000,
                )
        finally:
            await tr.rollback()


async def test_create_study_service_account_owner_raises_user_kind_trigger(postgres_pool):
    """tg_principal_must_be_user fires when owner_idx points at a
    service-account-kind principal."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            svc = await _create_service_account(conn)

            with pytest.raises(asyncpg.RaiseError, match="must reference a user-kind principal"):
                await create_study(
                    conn,
                    owner_idx=svc,
                    created_by_idx=svc,
                    title=_suffix("svc-owner"),
                )
        finally:
            await tr.rollback()


async def test_create_study_outside_transaction_raises(postgres_pool):
    """create_study guards on conn.is_in_transaction() — calling it on a
    bare connection must surface the helpful RuntimeError, not a
    half-committed study row without its access grant."""
    async with postgres_pool.acquire() as conn:
        with pytest.raises(RuntimeError, match="conn.transaction"):
            await create_study(
                conn,
                owner_idx=SYSTEM_PRINCIPAL_IDX,
                created_by_idx=SYSTEM_PRINCIPAL_IDX,
                title=_suffix("no-tx"),
            )


async def test_create_study_duplicate_ena_accession_raises_unique_error(postgres_pool):
    """Tests the case where two studies attempt the same non-null
    ena_study_accession: the second create trips the
    study_ena_study_accession_unique constraint and surfaces as
    asyncpg.UniqueViolationError. The route layer maps this to 409."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            shared_accession = f"ERP{secrets.token_hex(4)}"

            # First study claims the accession.
            await create_study(
                conn,
                owner_idx=owner,
                created_by_idx=owner,
                title=_suffix("dup-first"),
                ena_study_accession=shared_accession,
            )

            with pytest.raises(
                asyncpg.UniqueViolationError,
                match="study_ena_study_accession_unique",
            ):
                await create_study(
                    conn,
                    owner_idx=owner,
                    created_by_idx=owner,
                    title=_suffix("dup-second"),
                    ena_study_accession=shared_accession,
                )
        finally:
            await tr.rollback()


# ---------------------------------------------------------------------------
# fetch_study_exists
# ---------------------------------------------------------------------------


async def test_fetch_study_exists_returns_true_for_existing(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            study_idx = await _insert_study(conn, owner_idx=owner)

            # Pass the same connection so the fetch sees the uncommitted row.
            assert await fetch_study_exists(conn, study_idx) is True
        finally:
            await tr.rollback()


async def test_fetch_study_exists_returns_false_for_missing(postgres_pool):
    # A negative idx is guaranteed to miss because the IDENTITY column
    # only ever issues positive values.
    assert await fetch_study_exists(postgres_pool, -1) is False


# ---------------------------------------------------------------------------
# fetch_study
# ---------------------------------------------------------------------------


async def test_fetch_study_returns_full_row_for_existing_idx(postgres_pool):
    """fetch_study returns every column listed in _STUDY_RETURNING_COLS for
    a row created via the composer; round-tripping confirms the SELECT
    list matches the INSERT RETURNING list."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            pi = await _create_user(conn)
            title = _suffix("fetch-full")
            extra = {"site": "ucsd", "season": "spring", "vamps_id": "VAMPS-1"}

            created_row = await create_study(
                conn,
                owner_idx=owner,
                created_by_idx=owner,
                title=title,
                principal_investigator_idx=pi,
                alias="alias-1",
                description="desc",
                abstract="abs",
                funding="NIH-R01",
                ena_study_accession="ERP000001",
                bioproject_accession="PRJNA000001",
                notes="notes-1",
                extra_metadata=extra,
                default_tier=Tier.VIEWER,
            )

            fetched = await fetch_study(conn, created_row["idx"])

            # Every caller-visible column round-trips identically.
            assert fetched is not None
            assert fetched["idx"] == created_row["idx"]
            assert fetched["owner_idx"] == owner
            assert fetched["principal_investigator_idx"] == pi
            assert fetched["title"] == title
            assert fetched["alias"] == "alias-1"
            assert fetched["description"] == "desc"
            assert fetched["abstract"] == "abs"
            assert fetched["funding"] == "NIH-R01"
            assert fetched["ena_study_accession"] == "ERP000001"
            assert fetched["bioproject_accession"] == "PRJNA000001"
            assert fetched["notes"] == "notes-1"
            assert json.loads(fetched["extra_metadata"]) == extra
            assert fetched["default_tier"] == "viewer"
            assert fetched["created_by_idx"] == owner
            assert fetched["created_at"] == created_row["created_at"]
            assert fetched["updated_at"] == created_row["updated_at"]
        finally:
            await tr.rollback()


async def test_fetch_study_returns_none_for_missing_idx(postgres_pool):
    # A negative idx is guaranteed to miss because the IDENTITY column
    # only ever issues positive values.
    assert await fetch_study(postgres_pool, -1) is None


# ---------------------------------------------------------------------------
# update_study — study-specific behaviors only. The shared update_row
# composer's structural behaviors (empty-dict / unknown-key ValueError,
# None on missing row, updated_at bump, explicit-null, single- and
# multi-field writes) are already covered by test_update_biosample_* in
# tests/repositories/test_biosample.py and are not re-tested here. What
# belongs here is the JSONB-cast carve-out (no biosample column uses
# jsonb_cols today) and the constraints / triggers that live on
# qiita.study.
# ---------------------------------------------------------------------------


async def test_update_study_jsonb_extra_metadata_round_trips(postgres_pool):
    """Tests the case where the JSONB-cast carve-out in update_row
    serializes dict input and the column round-trips through asyncpg as
    JSON text."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            study_idx = await _insert_study(conn, owner_idx=owner)
            extra = {"vamps_id": "VAMPS-42", "tags": ["a", "b"]}

            row = await update_study(conn, study_idx, fields={"extra_metadata": extra})

            assert row is not None
            assert json.loads(row["extra_metadata"]) == extra
        finally:
            await tr.rollback()


async def test_update_study_jsonb_extra_metadata_explicit_null_clears_column(postgres_pool):
    """Tests the case where the JSONB column is set to SQL NULL via
    explicit None — the carve-out must not turn None into
    'null'::jsonb."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            study_idx = await _insert_study(conn, owner_idx=owner)
            await update_study(conn, study_idx, fields={"extra_metadata": {"k": "v"}})

            row = await update_study(conn, study_idx, fields={"extra_metadata": None})

            assert row is not None
            assert row["extra_metadata"] is None
        finally:
            await tr.rollback()


async def test_update_study_duplicate_ena_accession_raises_unique_error(postgres_pool):
    """Tests the case where the ena_study_accession uniqueness constraint
    fires on a PATCH; the route layer translates this into 409 via the
    shared raise_for_unique_violation helper."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            study_a = await _insert_study(conn, owner_idx=owner)
            study_b = await _insert_study(conn, owner_idx=owner)
            accession = _suffix("ERP")
            await update_study(conn, study_a, fields={"ena_study_accession": accession})

            with pytest.raises(asyncpg.UniqueViolationError) as excinfo:
                await update_study(conn, study_b, fields={"ena_study_accession": accession})
            assert excinfo.value.constraint_name == "study_ena_study_accession_unique"
        finally:
            await tr.rollback()


async def test_update_study_duplicate_bioproject_accession_raises_unique_error(postgres_pool):
    """Tests the case where two studies are updated to the same non-NULL
    bioproject_accession: the second update trips the
    study_bioproject_accession_unique constraint. Both seed studies leave
    the column NULL, confirming NULLs coexist freely until a value is
    written."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            study_a = await _insert_study(conn, owner_idx=owner)
            study_b = await _insert_study(conn, owner_idx=owner)
            accession = _suffix("PRJNA")
            await update_study(conn, study_a, fields={"bioproject_accession": accession})

            with pytest.raises(asyncpg.UniqueViolationError) as excinfo:
                await update_study(conn, study_b, fields={"bioproject_accession": accession})
            assert excinfo.value.constraint_name == "study_bioproject_accession_unique"
        finally:
            await tr.rollback()


# ---------------------------------------------------------------------------
# get_or_create_study_by_ena_accessions (T02-1, ena_import.registration)
#
# Known coverage gap, documented rather than papered over with a synthetic
# test: exercising the UniqueViolationError-catch-and-refetch branch needs a
# genuinely concurrent second writer racing between this function's own
# pre-check and its create_study attempt -- single-threaded, the pre-check
# always sees a row this same function created moments earlier, so the
# except branch is never reached from a single caller. test__sample_helpers.py
# documents the identical gap for write_global_metadata_or_diagnose's
# equivalent savepoint race.
# ---------------------------------------------------------------------------


async def test_get_or_create_study_by_ena_accessions_creates_on_miss(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            bioproject = _suffix("PRJNA")
            ena = _suffix("ERP")

            row, created = await get_or_create_study_by_ena_accessions(
                conn,
                bioproject_accession=bioproject,
                ena_study_accession=ena,
                owner_idx=owner,
                created_by_idx=owner,
                title="a new ena study",
            )

            assert created is True
            assert row["bioproject_accession"] == bioproject
            assert row["ena_study_accession"] == ena
            assert row["owner_idx"] == owner
        finally:
            await tr.rollback()


async def test_get_or_create_study_by_ena_accessions_reuses_on_hit(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            bioproject = _suffix("PRJNA")
            ena = _suffix("ERP")

            first_row, first_created = await get_or_create_study_by_ena_accessions(
                conn,
                bioproject_accession=bioproject,
                ena_study_accession=ena,
                owner_idx=owner,
                created_by_idx=owner,
                title="first call",
            )
            second_row, second_created = await get_or_create_study_by_ena_accessions(
                conn,
                bioproject_accession=bioproject,
                ena_study_accession=ena,
                owner_idx=owner,
                created_by_idx=owner,
                title="second call -- title is ignored on the reuse path",
            )

            assert first_created is True
            assert second_created is False
            assert first_row["idx"] == second_row["idx"]
            # The reuse path never re-attempts create_study, so the second
            # call's title argument never reaches the row.
            assert second_row["title"] == "first call"

            count = await conn.fetchval(
                "SELECT count(*) FROM qiita.study WHERE bioproject_accession = $1", bioproject
            )
            assert count == 1
        finally:
            await tr.rollback()


async def test_get_or_create_study_by_ena_accessions_null_secondary_accession(postgres_pool):
    """ena_study_accession is optional on EnaStudyHeader (a study may lack a
    secondary accession); None must round-trip as NULL, not a stringified
    'None'."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            bioproject = _suffix("PRJNA")

            row, created = await get_or_create_study_by_ena_accessions(
                conn,
                bioproject_accession=bioproject,
                ena_study_accession=None,
                owner_idx=owner,
                created_by_idx=owner,
                title="no secondary accession",
            )

            assert created is True
            assert row["ena_study_accession"] is None
        finally:
            await tr.rollback()


# ---------------------------------------------------------------------------
# fetch_study_idxs_by_accession — selectable accession column
# ---------------------------------------------------------------------------


async def test_fetch_study_idxs_by_accession_resolves_by_bioproject_default(postgres_pool):
    """Tests the case where accession_field is omitted: the default keys on
    bioproject_accession, so a study's bioproject value resolves and a
    bogus value lands in the unresolved set (absent from the map)."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            bioproject = _suffix("PRJNA")
            row = await create_study(
                conn,
                owner_idx=owner,
                created_by_idx=owner,
                title=_suffix("lookup-bioproject"),
                bioproject_accession=bioproject,
            )

            resolved = await fetch_study_idxs_by_accession(
                conn, values=[bioproject, "PRJNA-absent"]
            )

            assert resolved == {bioproject: row["idx"]}
        finally:
            await tr.rollback()


async def test_fetch_study_idxs_by_accession_resolves_by_ena_when_specified(postgres_pool):
    """Tests the case where accession_field selects ena_study_accession: a
    bioproject value no longer resolves, only the ena value does."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            ena = _suffix("ERP")
            row = await create_study(
                conn,
                owner_idx=owner,
                created_by_idx=owner,
                title=_suffix("lookup-ena"),
                ena_study_accession=ena,
            )

            resolved = await fetch_study_idxs_by_accession(
                conn, values=[ena], accession_field="ena_study_accession"
            )

            assert resolved == {ena: row["idx"]}
        finally:
            await tr.rollback()


async def test_fetch_study_idxs_by_accession_invalid_field_raises(postgres_pool):
    """Tests the case where accession_field is outside StudyAccessionField:
    the guard rejects it before any column name reaches the SQL."""
    with pytest.raises(ValueError, match="invalid study accession field"):
        await fetch_study_idxs_by_accession(postgres_pool, values=["x"], accession_field="title")


async def test_update_study_bad_pi_idx_raises_role_typed_error(postgres_pool):
    """Tests the case where a non-user-kind candidate for
    principal_investigator_idx trips the role-typed FK trigger; the
    route layer translates this into 422 with the disambiguated PI
    message."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            study_idx = await _insert_study(conn, owner_idx=owner)
            svc = await _create_service_account(conn)

            with pytest.raises(asyncpg.RaiseError, match="user-kind principal"):
                await update_study(conn, study_idx, fields={"principal_investigator_idx": svc})
        finally:
            await tr.rollback()
