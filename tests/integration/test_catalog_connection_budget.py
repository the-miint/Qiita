"""The catalog-connections queries in `deploy/_common.sh`, run against the test
Postgres as a role with and without `pg_read_all_stats`.

`qiita_catalog_connection_budget` reads the settings row's grant flag and the
backends rows, and `wait_for_data_plane_idle` the idle count; these pin what
Postgres returns to each query on either side of the grant. The roles are created
once for the module, under a random suffix, and dropped after the probe.
"""

import asyncio
import secrets
import subprocess
from pathlib import Path
from urllib.parse import urlparse

import asyncpg
import pytest
from qiita_control_plane.testing.postgres import resolve_postgres_url

_COMMON = Path(__file__).parent.parent.parent / "deploy" / "_common.sh"


def _common_sql(var: str) -> str:
    """The value of `var` once `deploy/_common.sh` is sourced."""
    proc = subprocess.run(
        ["bash", "-c", 'source "$1"; printf %s "${!2}"', "_", str(_COMMON), var],
        capture_output=True,
        text=True,
        check=True,
    )
    assert proc.stdout, f"{var} is empty after sourcing {_COMMON}"
    return proc.stdout


_OWN_ROLE_LINE = "\n    AND usename = current_user"


def _as_role(url: str, role: str, password: str) -> str:
    parsed = urlparse(url)
    netloc = f"{role}:{password}@{parsed.hostname}:{parsed.port}"
    return parsed._replace(netloc=netloc).geturl()


async def _settings_and_backends(url: str) -> tuple[asyncpg.Record, list[asyncpg.Record]]:
    conn = await asyncpg.connect(url)
    try:
        settings = await conn.fetchrow(_common_sql("QIITA_CATALOG_SETTINGS_SQL"))
        backends = await conn.fetch(_common_sql("QIITA_CATALOG_BACKENDS_SQL"))
    finally:
        await conn.close()
    return settings, backends


async def _idle_counts(url: str) -> tuple[int, int]:
    """QIITA_CATALOG_IDLE_SQL's count, then the same query without its own-role filter."""
    sql = _common_sql("QIITA_CATALOG_IDLE_SQL")
    unfiltered = sql.replace(_OWN_ROLE_LINE, "")
    assert unfiltered != sql, f"{_OWN_ROLE_LINE!r} not found in QIITA_CATALOG_IDLE_SQL"
    conn = await asyncpg.connect(url)
    try:
        return await conn.fetchval(sql), await conn.fetchval(unfiltered)
    finally:
        await conn.close()


async def _probe(url: str) -> dict[str, object]:
    admin = await asyncpg.connect(url)
    try:
        if not await admin.fetchval("SELECT rolsuper FROM pg_roles WHERE rolname = current_user"):
            pytest.skip("creating roles and granting pg_read_all_stats needs a superuser")
        suffix = secrets.token_hex(4)
        check, other = f"budget_check_{suffix}", f"budget_other_{suffix}"
        password = secrets.token_hex(16)
        for role in (check, other):
            await admin.execute(f"CREATE ROLE {role} LOGIN PASSWORD '{password}'")
        other_conn = None
        try:
            # Another role's session over TCP, idle in a transaction.
            other_conn = await asyncpg.connect(
                _as_role(url, other, password),
                server_settings={"application_name": "budget-other"},
            )
            tx = other_conn.transaction()
            await tx.start()

            without = await _settings_and_backends(_as_role(url, check, password))
            await admin.execute(f"GRANT pg_read_all_stats TO {check}")
            with_grant = await _settings_and_backends(_as_role(url, check, password))
            idle = await _idle_counts(_as_role(url, check, password))
            await tx.rollback()
        finally:
            if other_conn is not None:
                await other_conn.close()
            for role in (check, other):
                await admin.execute(f"DROP ROLE IF EXISTS {role}")
    finally:
        await admin.close()
    return {"check": check, "other": other, "without": without, "with": with_grant, "idle": idle}


@pytest.fixture(scope="module")
def probe() -> dict[str, object]:
    return asyncio.run(_probe(resolve_postgres_url()))


def test_catalog_queries_see_other_roles_sessions_only_with_pg_read_all_stats(
    probe: dict[str, object],
) -> None:
    result = probe
    check, other = result["check"], result["other"]

    settings, backends = result["without"]
    assert settings[3] is False
    assert settings[4] == check
    assert [row for row in backends if row[2] == other] == []

    settings, backends = result["with"]
    assert settings[3] is True
    assert settings[4] == check
    rows = [row for row in backends if row[2] == other]
    assert len(rows) == 1, backends
    host, count, _usename, appname = rows[0]
    assert host not in ("", "local"), rows[0]
    assert count == 1
    assert appname == "budget-other"


def test_idle_wait_does_not_count_another_roles_open_transaction(
    probe: dict[str, object],
) -> None:
    """With pg_read_all_stats granted and another role idle in a transaction, the
    idle-wait count is 0; the control, the same query without its own-role filter,
    counts that transaction."""
    filtered, unfiltered = probe["idle"]
    assert filtered == 0
    assert unfiltered >= 1
