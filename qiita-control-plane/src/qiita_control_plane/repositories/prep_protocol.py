"""Read-only repository for qiita.prep_protocol -- the system-admin curated
registry of prep protocols (db/migrations/20260501000010_prep_protocol_
prep_sample_field.sql). No write surface lives here: prep protocols are
curated out-of-band (seed migration), not created by any import path --
see `ena_import.registration`, which resolves a run's protocol name via
`ena_import.protocol_mapping` and looks it up here rather than minting one.
"""

import asyncpg


class PrepProtocolUnknownError(Exception):
    """Raised when a prep_protocol name has no matching, non-retired row.

    Carries the offending name so the caller can surface which protocol
    name failed to resolve rather than a bare FK-violation deeper in an
    import composer.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"unknown prep_protocol name: {name!r}")


async def fetch_prep_protocol_idx_by_name(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    name: str,
) -> int:
    """Resolve a prep_protocol name to its idx.

    Only non-retired rows resolve -- a retired protocol is not a valid
    target for a fresh import. Raises PrepProtocolUnknownError on a miss
    (unknown name, or a name that exists but is retired) so the caller
    fails loud before any downstream FK violation.
    """
    idx = await pool_or_conn.fetchval(
        "SELECT idx FROM qiita.prep_protocol WHERE name = $1 AND retired = false",
        name,
    )
    if idx is None:
        raise PrepProtocolUnknownError(name)
    return idx
