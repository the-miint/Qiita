"""Repository functions for the qiita.processing identity table.

A processing_idx is the identity of a per-sample processing RUN — its canonical
parameters (workflow + version + result-affecting knobs like the assembler). The
mint path wraps the qiita.mint_processing plpgsql function, which upserts on
params_hash so the same params always resolve to the same processing_idx
fleet-wide (idempotent re-run) and different params get a distinct id (run
disambiguation). The params_hash is computed control-plane-side via
qiita_common.hashing.canonical_params_hash (SHA-256 of the canonical params
JSON) — no pgcrypto dependency on the database. Mirrors repositories.mask_definition.
"""

import json

import asyncpg
from qiita_common.hashing import canonical_params_hash


async def mint_processing(
    conn: asyncpg.Connection,
    *,
    workflow: str,
    version: str,
    params: dict,
) -> asyncpg.Record:
    """Mint (or return the existing) qiita.processing row for a params set.

    Deduplicates on the canonical-JSON SHA-256 of `params` — the dedup key is the
    full params blob, so the same params resolve to the same `processing_idx`.
    `workflow` / `version` are stored as descriptive columns; they are expected to
    also appear inside `params` so the hash covers them.

    Returns the qiita.processing row as an asyncpg.Record. No
    `require_transaction(conn)` guard: the plpgsql SELECT/INSERT upsert loop runs
    as a single statement, so Postgres wraps it in one transaction either way.
    """
    params_hash = canonical_params_hash(params)
    return await conn.fetchrow(
        "SELECT processing_idx, params_hash, workflow, version, params, created_at"
        "  FROM qiita.mint_processing($1, $2, $3, $4::jsonb)",
        params_hash,
        workflow,
        version,
        json.dumps(params),
    )
