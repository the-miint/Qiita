"""Schema-level invariants for qiita.mask_definition and its mint function.

Locks in the columns, the UNIQUE params_hash dedup key, the 32-byte hash CHECK,
the adapter_hash_scheme CHECK, the FK to qiita.principal, and the
qiita.mint_mask_definition signature/return type. These fail with a clear "does
not exist" message until the migration lands.
"""

import secrets

import asyncpg
import pytest
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX

from qiita_control_plane.repositories.mask_definition import ADAPTER_HASH_SCHEME_SEQUENCE_HASH

pytestmark = pytest.mark.db


async def test_mask_definition_table_columns(postgres_pool):
    rows = await postgres_pool.fetch(
        "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS type,"
        "       a.attnotnull"
        "  FROM pg_attribute a"
        "  JOIN pg_class c ON c.oid = a.attrelid"
        "  JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE n.nspname = 'qiita'"
        "   AND c.relname = 'mask_definition'"
        "   AND a.attnum > 0"
        "   AND NOT a.attisdropped"
        " ORDER BY a.attnum"
    )
    cols = {r["attname"]: (r["type"], r["attnotnull"]) for r in rows}
    assert cols, "qiita.mask_definition is missing"
    assert cols["mask_idx"] == ("bigint", True)
    assert cols["params_hash"] == ("bytea", True)
    assert cols["filter_workflow"] == ("text", True)
    assert cols["filter_version"] == ("text", True)
    assert cols["params"] == ("jsonb", True)
    assert cols["created_by_idx"] == ("bigint", True)
    assert cols["created_at"][0].startswith("timestamp with time zone")
    assert cols["created_at"][1] is True
    # Nullable by design: NULL means either the config carries no adapter set or
    # the row still holds the legacy byte-derived hash. A NOT NULL here would make
    # the contract phase's gate unsatisfiable for adapter-less masks.
    assert cols["adapter_hash_scheme"] == ("text", False)


async def test_mask_definition_adapter_hash_scheme_check(postgres_pool):
    """The scheme is bounded to derivations the code can reproduce. A row minted
    with an unknown scheme would claim an identity nothing can re-derive, so the
    DB rejects it — and the Python constant must be a value it accepts."""
    (check,) = [
        d["def"]
        for d in await postgres_pool.fetch(
            "SELECT pg_get_constraintdef(c.oid) AS def"
            "  FROM pg_constraint c"
            "  JOIN pg_class ct ON ct.oid = c.conrelid"
            "  JOIN pg_namespace cn ON cn.oid = ct.relnamespace"
            " WHERE c.contype = 'c' AND cn.nspname = 'qiita'"
            "   AND ct.relname = 'mask_definition'"
            "   AND c.conname = 'mask_definition_adapter_hash_scheme_known'"
        )
    ]
    assert ADAPTER_HASH_SCHEME_SEQUENCE_HASH in check, check

    with pytest.raises(asyncpg.CheckViolationError):
        await postgres_pool.execute(
            "INSERT INTO qiita.mask_definition"
            " (params_hash, filter_workflow, filter_version, params, created_by_idx,"
            "  adapter_hash_scheme)"
            " VALUES ($1, 'read-mask', '1.0', '{}'::jsonb, $2, 'not-a-scheme')",
            secrets.token_bytes(32),
            SYSTEM_PRINCIPAL_IDX,
        )


async def test_mask_definition_params_hash_unique(postgres_pool):
    """params_hash must carry a UNIQUE constraint — the dedup key."""
    defs = await postgres_pool.fetch(
        "SELECT pg_get_constraintdef(c.oid) AS def"
        "  FROM pg_constraint c"
        "  JOIN pg_class ct ON ct.oid = c.conrelid"
        "  JOIN pg_namespace cn ON cn.oid = ct.relnamespace"
        " WHERE c.contype = 'u'"
        "   AND cn.nspname = 'qiita' AND ct.relname = 'mask_definition'"
    )
    assert any("params_hash" in d["def"] for d in defs), [d["def"] for d in defs]


async def test_mask_definition_params_hash_len_check(postgres_pool):
    defs = await postgres_pool.fetch(
        "SELECT c.conname, pg_get_constraintdef(c.oid) AS def"
        "  FROM pg_constraint c"
        "  JOIN pg_class ct ON ct.oid = c.conrelid"
        "  JOIN pg_namespace cn ON cn.oid = ct.relnamespace"
        " WHERE c.contype = 'c'"
        "   AND cn.nspname = 'qiita' AND ct.relname = 'mask_definition'"
    )
    by_name = {r["conname"]: r["def"] for r in defs}
    assert "mask_definition_params_hash_len" in by_name, by_name
    assert "32" in by_name["mask_definition_params_hash_len"]


async def test_mask_definition_fk_to_principal(postgres_pool):
    confdeltype = await postgres_pool.fetchval(
        "SELECT c.confdeltype"
        "  FROM pg_constraint c"
        "  JOIN pg_class ct ON ct.oid = c.conrelid"
        "  JOIN pg_namespace cn ON cn.oid = ct.relnamespace"
        "  JOIN pg_class pt ON pt.oid = c.confrelid"
        "  JOIN pg_namespace pn ON pn.oid = pt.relnamespace"
        " WHERE c.contype = 'f'"
        "   AND cn.nspname = 'qiita' AND ct.relname = 'mask_definition'"
        "   AND pn.nspname = 'qiita' AND pt.relname = 'principal'"
    )
    assert confdeltype is not None, "mask_definition is missing its FK to qiita.principal"
    # 'r' = RESTRICT — a principal with masks can't be hard-deleted out from
    # under them (same posture as sequence_range.created_by_idx).
    assert confdeltype == b"r", (
        f"created_by_idx FK must be ON DELETE RESTRICT (got {confdeltype!r})"
    )


async def test_mint_mask_definition_function_exists(postgres_pool):
    row = await postgres_pool.fetchrow(
        "SELECT p.pronargs, p.pronargdefaults,"
        "       p.proargtypes::regtype[] AS argtypes,"
        "       pg_get_function_result(p.oid) AS result"
        "  FROM pg_proc p"
        "  JOIN pg_namespace n ON n.oid = p.pronamespace"
        " WHERE n.nspname = 'qiita' AND p.proname = 'mint_mask_definition'"
    )
    assert row is not None, "qiita.mint_mask_definition is missing"
    assert row["pronargs"] == 7, row["pronargs"]
    assert list(row["argtypes"]) == [
        "bytea",
        "text",
        "text",
        "jsonb",
        "bigint",
        "bytea",
        "text",
    ], row["argtypes"]
    # The last two carry DEFAULT NULL, which is what lets the pre-existing
    # 5-argument call still resolve — the property the deploy leans on while the
    # migration has run but the service has not restarted.
    assert row["pronargdefaults"] == 2, row["pronargdefaults"]
    assert row["result"] == "mask_definition", row["result"]
