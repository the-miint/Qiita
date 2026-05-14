"""Session-scoped fixtures that mint PATs against the test database.

`human_admin_session` provisions a system_admin human and returns a token
carrying the full admin scope ceiling. `regular_user_session` provisions a
non-admin user with a user-ceiling token (for negative-case tests).
`compute_worker_service_account` provisions a service-account principal,
mints a worker-scope token, and writes it to a tmp file path that mirrors
the production `/etc/qiita/orchestrator.token` location.

The principal rows persist across pytest sessions because qiita.auth_event
references them via FK and is append-only by design. Each fixture looks up
its principal by display_name / service-account name and only creates the
row when absent.
"""

import pytest_asyncio
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX, Scope, SystemRole


@pytest_asyncio.fixture(scope="session")
async def human_admin_session(postgres_pool):
    """A session-scoped system_admin human with a complete profile and a
    PAT carrying the full admin scope ceiling. Tests use this token to
    drive routes that require human + admin authority (POST /references,
    POST /admin/*, PATCH /users/me).
    """
    from qiita_control_plane.auth.token import mint_api_token

    display_name = "test-human-admin"
    # Look up an existing principal first; auth_events FK keeps rows around
    # across pytest sessions, so re-creation would fail.
    idx = await postgres_pool.fetchval(
        "SELECT idx FROM qiita.principal WHERE display_name = $1",
        display_name,
    )
    if idx is None:
        async with postgres_pool.acquire() as conn:
            async with conn.transaction():
                idx = await conn.fetchval(
                    "INSERT INTO qiita.principal"
                    "  (display_name, system_role, created_by_idx)"
                    " VALUES ($1, $2, $3) RETURNING idx",
                    display_name,
                    SystemRole.SYSTEM_ADMIN,
                    SYSTEM_PRINCIPAL_IDX,
                )
                await conn.execute(
                    "INSERT INTO qiita.user"
                    "  (principal_idx, email, affiliation, address, phone)"
                    " VALUES ($1, $2, 'UCSD', '9500 Gilman', '555-0001')",
                    idx,
                    f"{display_name}@example.com",
                )
    # Make sure existing rows have a complete profile (idempotent).
    await postgres_pool.execute(
        "UPDATE qiita.user SET affiliation = 'UCSD', address = '9500 Gilman',"
        " phone = '555-0001'"
        " WHERE principal_idx = $1",
        idx,
    )
    # Make sure principal isn't disabled / retired from a prior partial run.
    await postgres_pool.execute(
        "UPDATE qiita.principal SET"
        "  disabled = false, disabled_at = NULL, disabled_by_idx = NULL,"
        "  disable_reason = NULL"
        " WHERE idx = $1 AND retired = false",
        idx,
    )
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=idx,
        label="session-admin",
        scopes=[
            Scope.SELF_PROFILE,
            Scope.SELF_TOKEN,
            Scope.REFERENCE_READ,
            Scope.REFERENCE_WRITE,
            Scope.BIOSAMPLE_READ,
            Scope.BIOSAMPLE_WRITE,
            Scope.PREP_SAMPLE_READ,
            Scope.PREP_SAMPLE_WRITE,
            Scope.STUDY_READ,
            Scope.STUDY_WRITE,
            Scope.ADMIN_USER,
            Scope.ADMIN_SERVICE_ACCOUNT,
            Scope.ADMIN_AUDIT_READ,
        ],
    )
    return {
        "principal_idx": idx,
        "token": plaintext,
        "email": f"{display_name}@example.com",
        "display_name": display_name,
    }


@pytest_asyncio.fixture(scope="session")
async def wet_lab_admin_session(postgres_pool):
    """A session-scoped wet_lab_admin human with a complete profile and a
    PAT carrying the wet_lab_admin scope ceiling. Used for tests that need
    a caller authorized to act on behalf of another user (e.g., importing
    a biosample where body.owner_idx names a different principal)."""
    from qiita_control_plane.auth.token import mint_api_token

    display_name = "test-wet-lab-admin"
    # Look up an existing principal first; auth_events FK keeps rows around
    # across pytest sessions, so re-creation would fail.
    idx = await postgres_pool.fetchval(
        "SELECT idx FROM qiita.principal WHERE display_name = $1",
        display_name,
    )
    if idx is None:
        async with postgres_pool.acquire() as conn:
            async with conn.transaction():
                idx = await conn.fetchval(
                    "INSERT INTO qiita.principal"
                    "  (display_name, system_role, created_by_idx)"
                    " VALUES ($1, $2, $3) RETURNING idx",
                    display_name,
                    SystemRole.WET_LAB_ADMIN,
                    SYSTEM_PRINCIPAL_IDX,
                )
                await conn.execute(
                    "INSERT INTO qiita.user"
                    "  (principal_idx, email, affiliation, address, phone)"
                    " VALUES ($1, $2, 'UCSD', '9500 Gilman', '555-0002')",
                    idx,
                    f"{display_name}@example.com",
                )
    # Make sure existing rows have a complete profile (idempotent).
    await postgres_pool.execute(
        "UPDATE qiita.user SET affiliation = 'UCSD', address = '9500 Gilman',"
        " phone = '555-0002'"
        " WHERE principal_idx = $1",
        idx,
    )
    # Make sure principal isn't disabled / retired from a prior partial run.
    await postgres_pool.execute(
        "UPDATE qiita.principal SET"
        "  disabled = false, disabled_at = NULL, disabled_by_idx = NULL,"
        "  disable_reason = NULL"
        " WHERE idx = $1 AND retired = false",
        idx,
    )
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=idx,
        label="session-wet-lab-admin",
        scopes=[
            Scope.SELF_PROFILE,
            Scope.SELF_TOKEN,
            Scope.REFERENCE_READ,
            Scope.REFERENCE_WRITE,
            Scope.BIOSAMPLE_READ,
            Scope.BIOSAMPLE_WRITE,
            Scope.PREP_SAMPLE_READ,
            Scope.PREP_SAMPLE_WRITE,
            Scope.STUDY_READ,
            Scope.STUDY_WRITE,
        ],
    )
    return {
        "principal_idx": idx,
        "token": plaintext,
        "email": f"{display_name}@example.com",
        "display_name": display_name,
    }


@pytest_asyncio.fixture(scope="session")
async def regular_user_session(postgres_pool):
    """A session-scoped 'user'-role human with a complete profile and a
    PAT scoped to the user ceiling. Used for negative-case tests that
    need a non-admin caller (e.g., 403 on admin endpoints)."""
    from qiita_control_plane.auth.token import mint_api_token

    display_name = "test-regular-user"
    idx = await postgres_pool.fetchval(
        "SELECT idx FROM qiita.principal WHERE display_name = $1",
        display_name,
    )
    if idx is None:
        async with postgres_pool.acquire() as conn:
            async with conn.transaction():
                idx = await conn.fetchval(
                    "INSERT INTO qiita.principal"
                    "  (display_name, system_role, created_by_idx)"
                    " VALUES ($1, $2, $3) RETURNING idx",
                    display_name,
                    SystemRole.USER,
                    SYSTEM_PRINCIPAL_IDX,
                )
                await conn.execute(
                    "INSERT INTO qiita.user"
                    "  (principal_idx, email, affiliation, address, phone)"
                    " VALUES ($1, $2, 'UCSD', 'X', 'Y')",
                    idx,
                    f"{display_name}@example.com",
                )
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=idx,
        label="session-user",
        scopes=[
            Scope.SELF_PROFILE,
            Scope.SELF_TOKEN,
            Scope.REFERENCE_READ,
            Scope.BIOSAMPLE_READ,
            Scope.BIOSAMPLE_WRITE,
            Scope.PREP_SAMPLE_READ,
            Scope.STUDY_READ,
            Scope.STUDY_WRITE,
        ],
    )
    return {
        "principal_idx": idx,
        "token": plaintext,
        "email": f"{display_name}@example.com",
        "display_name": display_name,
    }


@pytest_asyncio.fixture(scope="session")
async def compute_worker_service_account(postgres_pool, tmp_path_factory):
    """Provision a service-account-kind principal with worker scopes and
    write its token to a tmp file. Reused by the orchestrator-auth tests;
    the file path is the canonical drop-in for the production
    `/etc/qiita/orchestrator.token` location.

    Idempotent across pytest sessions: if a previous run created the
    service_account row (auth_event FK keeps principals around), look it
    up by name instead of re-creating. Always mints a fresh token so each
    session starts with a known-good credential.

    Returns a dict with `principal_idx`, `token_path` (Path), `token` (str).
    """
    from qiita_control_plane.auth.token import mint_api_token

    SVC_NAME = "compute-worker-fixture"
    pidx = await postgres_pool.fetchval(
        "SELECT principal_idx FROM qiita.service_account WHERE name = $1",
        SVC_NAME,
    )
    if pidx is None:
        async with postgres_pool.acquire() as conn:
            async with conn.transaction():
                pidx = await conn.fetchval(
                    "INSERT INTO qiita.principal"
                    "  (display_name, system_role, created_by_idx)"
                    " VALUES ($1, $2, $3) RETURNING idx",
                    SVC_NAME,
                    SystemRole.USER,
                    SYSTEM_PRINCIPAL_IDX,
                )
                await conn.execute(
                    "INSERT INTO qiita.service_account"
                    "  (principal_idx, name, description)"
                    " VALUES ($1, $2, 'orchestrator service-account fixture')",
                    pidx,
                    SVC_NAME,
                )
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=pidx,
        label="orchestrator-fixture",
        scopes=[
            Scope.FEATURE_MINT,
            Scope.REFERENCE_WRITE,
            Scope.REFERENCE_REGISTER_FILES,
            Scope.REFERENCE_READ,
            Scope.TICKET_DOGET,
        ],
    )
    token_path = tmp_path_factory.mktemp("orchestrator-token") / "token"
    token_path.write_text(plaintext)
    token_path.chmod(0o400)
    return {"principal_idx": pidx, "token_path": token_path, "token": plaintext}
