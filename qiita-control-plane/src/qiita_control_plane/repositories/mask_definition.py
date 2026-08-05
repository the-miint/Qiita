"""Repository functions for the qiita.mask_definition table.

A mask's identity is its read-filtering CONFIG (filter workflow + version +
host references + QC params). The mint path is a thin wrapper around the
qiita.mint_mask_definition plpgsql function, which upserts on params_hash so
the same config always resolves to the same mask_idx fleet-wide (idempotent).

The params_hash is computed control-plane-side via
qiita_common.hashing.canonical_params_hash (SHA-256 of the canonical config
JSON) — no pgcrypto dependency on the database. The function only enforces the
dedup and returns the row; asyncpg.ForeignKeyViolationError (unknown
principal_idx) and asyncpg.InvalidParameterValueError (SQLSTATE 22023, a
non-32-byte hash — unreachable via this helper) propagate to the caller.

A config carrying an adapter set hashes under two derivations while the adapter
identity migration is in its expand phase: the current one (the reference's
sorted qiita.feature.sequence_hash values) and the legacy one (SHA-256 of the
serialized adapter Parquet). The mint looks the current hash up first and falls
back to the legacy hash, re-keying the row it finds onto the current one. Full
rationale and the removal plan: the 20260804000000 migration header.
"""

import json

import asyncpg
from qiita_common.hashing import canonical_params_hash

# Value stamped on qiita.mask_definition.adapter_hash_scheme for a row whose
# params.resolved_qc.adapter_set_hash came from the reference's sorted
# qiita.feature.sequence_hash values (reference_membership.reference_sequence_set_hash).
# A NULL column is the legacy serialized-Parquet-bytes derivation. Mirrors the
# CHECK constraint in the 20260804000000 migration; adding a value changes both.
ADAPTER_HASH_SCHEME_SEQUENCE_HASH = "sequence_hash_v1"

# The two `params` keys addressing the adapter identity inside the blob
# `runner._mask._build_mask_params` produces. They live here, not next to that
# builder, because this module and the re-key backfill both address the path
# without going through it — the scheme stamp below, the backfill's SQL, and its
# params rewrite — and the repository is what every one of them already imports.
# A rename in the builder has to reach these.
RESOLVED_QC_KEY = "resolved_qc"
ADAPTER_SET_HASH_KEY = "adapter_set_hash"
# The same path as a Postgres jsonb text accessor:
# `params->'resolved_qc'->>'adapter_set_hash'`.
ADAPTER_SET_HASH_JSON_PATH = f"'{RESOLVED_QC_KEY}'->>'{ADAPTER_SET_HASH_KEY}'"


async def mint_mask_definition(
    conn: asyncpg.Connection,
    *,
    filter_workflow: str,
    filter_version: str,
    params: dict,
    principal_idx: int,
    legacy_params: dict | None = None,
    adapter_hash_scheme: str | None = None,
) -> asyncpg.Record:
    """Mint (or return the existing) mask_definition row for a config.

    Deduplicates on the canonical-JSON SHA-256 of `params` alone — the
    dedup key is the config blob, so the same config resolves to the same
    `mask_idx` fleet-wide. `filter_workflow` / `filter_version` are stored as
    descriptive columns; they are expected to also appear inside `params`
    so the hash covers them, but the hash is over `params` so two callers
    that pass the same `params` collapse to one row regardless.

    `legacy_params` is the same config carrying the legacy
    `resolved_qc.adapter_set_hash` (see `runner._mask._adapter_set_hash_legacy`).
    When `params` misses and `legacy_params` hits, that row is this config and is
    re-keyed onto the current hash in place, keeping its mask_idx. Pass None for a
    config that carries no adapter set: the two derivations agree on None, and a
    None legacy hash makes the SQL skip the lookup.

    `adapter_hash_scheme` is the caller stating which derivation produced the
    `resolved_qc.adapter_set_hash` inside `params`. It is NOT inferred from the
    blob: only a caller that derived the hash knows that, and the public
    `POST /mask-definition` route mints whatever `params` a client sends. Left
    None there, such a row stays unstamped and shows up in `qiita-admin backfill
    mask-adapter-hash`'s report — which is where a hash nobody can attribute
    belongs. Inferring it from "params carries a hash" would instead mark it
    converted and let the contract phase's gate read green over an unknown
    derivation. Pass None for a config with no adapter set: there is no
    derivation to record.

    Returns the qiita.mask_definition row as an asyncpg.Record. Raises
    asyncpg.ForeignKeyViolationError when principal_idx does not exist.

    No `require_transaction(conn)` guard: the qiita.mint_mask_definition
    plpgsql body (the SELECT/UPDATE/INSERT upsert loop) executes as a single SQL
    statement, so Postgres wraps it in one transaction either way.
    """
    params_hash = canonical_params_hash(params)
    legacy_params_hash = canonical_params_hash(legacy_params) if legacy_params is not None else None
    # asyncpg encodes a dict bound to a jsonb parameter via the JSON codec; pass
    # the serialized string explicitly so the behaviour is independent of
    # whether a JSON codec is registered on the connection.
    return await conn.fetchrow(
        "SELECT mask_idx, params_hash, filter_workflow, filter_version,"
        "       params, created_by_idx, created_at, adapter_hash_scheme"
        "  FROM qiita.mint_mask_definition($1, $2, $3, $4::jsonb, $5, $6, $7)",
        params_hash,
        filter_workflow,
        filter_version,
        json.dumps(params),
        principal_idx,
        legacy_params_hash,
        adapter_hash_scheme,
    )


async def fetch_mask_definition_by_idx(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    mask_idx: int,
) -> asyncpg.Record | None:
    """Return the qiita.mask_definition row for mask_idx, or None.

    Accepts either a pool or a connection so the helper composes inside an
    open transaction or stands alone.
    """
    return await pool_or_conn.fetchrow(
        "SELECT mask_idx, params_hash, filter_workflow, filter_version,"
        "       params, created_by_idx, created_at, adapter_hash_scheme"
        "  FROM qiita.mask_definition"
        " WHERE mask_idx = $1",
        mask_idx,
    )
