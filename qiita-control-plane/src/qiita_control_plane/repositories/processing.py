"""Repository for the qiita.processing_method and processed_prep_sample tables.

A processing method's identity is its config; minting upserts on params_hash so the
same config always resolves to the same processing_idx fleet-wide. params_hash is a
SHA-256 of the canonical config JSON computed CP-side, so there is no pgcrypto dependency.
"""

import json

import asyncpg
from qiita_common.hashing import canonical_params_hash


async def mint_processing_method(
    conn: asyncpg.Connection,
    *,
    workflow_name: str,
    workflow_version: str,
    params: dict,
    principal_idx: int,
) -> asyncpg.Record:
    """Mint or return the existing processing_method row for a config.

    Deduplicates on the canonical-JSON SHA-256 of `params` alone, so two callers passing the
    same `params` collapse to one row; `workflow_name` and `workflow_version` are descriptive.
    """
    params_hash = canonical_params_hash(params)
    # pass the serialized JSON explicitly so behaviour is independent of whether a JSON
    # codec is registered on the connection.
    return await conn.fetchrow(
        "SELECT processing_idx, params_hash, workflow_name, workflow_version,"
        "       params, created_by_idx, created_at"
        "  FROM qiita.mint_processing_method($1, $2, $3, $4::jsonb, $5)",
        params_hash,
        workflow_name,
        workflow_version,
        json.dumps(params),
        principal_idx,
    )


async def mint_processed_prep_samples(
    conn: asyncpg.Connection,
    *,
    processing_idx: int,
    prep_sample_idxs: list[int],
) -> dict[int, int]:
    """Mint or return processed_prep_sample rows for a cohort, keyed by prep_sample_idx.

    Idempotent over (processing_idx, prep_sample_idx): a re-run returns the same values.
    """
    rows = await conn.fetch(
        "SELECT prep_sample_idx, processed_prep_sample_idx"
        "  FROM qiita.mint_processed_prep_samples($1, $2::bigint[])",
        processing_idx,
        prep_sample_idxs,
    )
    return {r["prep_sample_idx"]: r["processed_prep_sample_idx"] for r in rows}


async def lookup_processing_idx_by_params(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    params: dict,
) -> int | None:
    """Return the processing_idx whose params_hash matches ``params``, or None; never mints."""
    params_hash = canonical_params_hash(params)
    return await pool_or_conn.fetchval(
        "SELECT processing_idx FROM qiita.processing_method WHERE params_hash = $1",
        params_hash,
    )
