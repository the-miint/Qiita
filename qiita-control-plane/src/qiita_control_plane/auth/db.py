"""Small DB helpers shared across auth routes."""

import re

from qiita_common.auth_constants import SystemRole

# `pool.execute(...)` returns a Postgres command tag like "UPDATE 1",
# "INSERT 0 3", "DELETE 0". We need the *trailing* row count.
_COMMAND_TAG_ROWS_RE = re.compile(r"\b(\d+)\s*$")


def rows_affected(command_tag: str) -> int:
    """Parse the trailing row count out of an asyncpg command tag.

    Replaces the brittle `tag.endswith("0")` idiom: the substring match
    happens to work for "UPDATE 1" vs "UPDATE 0" but silently misclassifies
    "UPDATE 10" or "UPDATE 100" as zero rows. Single-row UPDATEs in this
    codebase never see double digits, but we'd rather have the right helper
    than rely on that invariant.
    """
    match = _COMMAND_TAG_ROWS_RE.search(command_tag)
    if match is None:
        raise ValueError(f"could not parse row count from command tag {command_tag!r}")
    return int(match.group(1))


async def insert_principal(
    conn,
    *,
    display_name: str,
    created_by_idx: int,
    system_role: SystemRole = SystemRole.USER,
) -> int:
    """Insert a principal row and return its idx.

    Centralises the (display_name, system_role, created_by_idx) INSERT used
    by the OIDC first-login path, admin user creation, and admin
    service-account creation.
    """
    return await conn.fetchval(
        "INSERT INTO qiita.principal"
        "  (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, $3) RETURNING idx",
        display_name,
        system_role,
        created_by_idx,
    )
