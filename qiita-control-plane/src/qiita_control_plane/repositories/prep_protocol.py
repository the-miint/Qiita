"""Read-only repository for qiita.prep_protocol -- the system-admin curated
registry of prep protocols. No write surface: protocols are curated out-of-band
(seed migration), not minted by any import path -- `ena_import.registration`
resolves a run's protocol name and looks it up here.
"""

import asyncpg


class PrepProtocolUnknownError(Exception):
    """Raised when a prep_protocol name has no matching, non-retired row.
    Carries the offending name so the caller fails loud with it rather than a
    bare FK violation deeper in an import composer.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"unknown prep_protocol name: {name!r}")


async def fetch_prep_protocol_idx_by_name(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    name: str,
) -> int:
    """Resolve a prep_protocol name to its idx.

    Only non-retired rows resolve -- a retired protocol is not a valid target
    for a fresh import. Raises PrepProtocolUnknownError on a miss (unknown or
    retired name) so the caller fails loud before any downstream FK violation.
    """
    idx = await pool_or_conn.fetchval(
        "SELECT idx FROM qiita.prep_protocol WHERE name = $1 AND retired = false",
        name,
    )
    if idx is None:
        raise PrepProtocolUnknownError(name)
    return idx
