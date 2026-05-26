"""Pytest seed and state-change helpers for DB-row fixtures.

Plain async functions (not pytest fixtures) so callers can pass test-local
arguments. Helpers fall into three groups: seeders that insert rows and
return the new idx, state-changers that update existing rows (disabling,
retiring, etc.), and lookup helpers for migration-seeded reference data
that every test DB carries. Cleanup is the caller's responsibility
(route tests do FK-reverse cleanup against a per-test `created` tracker;
integration tests may rely on a session-scoped truncate). Helpers are
pool-based and commit their writes — for repository-layer trigger tests
that roll back, build the SQL inline against the open connection instead.
"""

import secrets

import asyncpg
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX, SystemRole
from qiita_common.models import FieldDataType

# Seeded NCBI Taxonomy fixture data — must match the seed migration at
# qiita-control-plane/db/migrations/20260525000000_seed_ncbi_taxonomy.sql.
NCBI_TAXONOMY_NAME = "NCBI Taxonomy"
NCBI_TAXONOMY_METAGENOME_TERM_ID = "256318"


async def fetch_seeded_metagenome_term(pool: asyncpg.Pool) -> asyncpg.Record:
    """Return the seeded NCBI Taxonomy metagenome term row (idx, term_id,
    label, terminology_idx)."""
    return await pool.fetchrow(
        "SELECT tt.idx, tt.term_id, tt.label, tt.terminology_idx"
        " FROM qiita.terminology_term tt"
        " JOIN qiita.terminology t ON t.idx = tt.terminology_idx"
        " WHERE t.name = $1 AND tt.term_id = $2",
        NCBI_TAXONOMY_NAME,
        NCBI_TAXONOMY_METAGENOME_TERM_ID,
    )


async def seed_user_principal(
    pool: asyncpg.Pool,
    *,
    prefix: str,
    suffix: str,
    profile_complete: bool = True,
    system_role: SystemRole = SystemRole.USER,
) -> int:
    """Insert a principal + qiita.user row; return the principal_idx.

    `prefix` and `suffix` form the display_name as f"{prefix}-{suffix}-{token}";
    the token defends against name collisions across re-runs. With
    profile_complete=True the user row carries email + affiliation + address
    + phone, which the schema's profile_complete computed column treats as a
    complete profile. With profile_complete=False only email is populated, so
    the flag stays false. `system_role` defaults to USER; pass an elevated
    role for tests that need a wet_lab_admin / system_admin caller (the
    qiita.user row makes this a user-kind, not service-account, principal
    regardless of role).
    """
    name = f"{prefix}-{suffix}-{secrets.token_hex(4)}"
    async with pool.acquire() as conn:
        async with conn.transaction():
            pidx = await conn.fetchval(
                "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
                " VALUES ($1, $2, $3) RETURNING idx",
                name,
                system_role,
                SYSTEM_PRINCIPAL_IDX,
            )
            if profile_complete:
                await conn.execute(
                    "INSERT INTO qiita.user"
                    "  (principal_idx, email, affiliation, address, phone)"
                    " VALUES ($1, $2, 'UCSD', 'X', 'Y')",
                    pidx,
                    f"{name}@test.local",
                )
            else:
                await conn.execute(
                    "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
                    pidx,
                    f"{name}@test.local",
                )
    return pidx


async def seed_service_principal(
    pool: asyncpg.Pool,
    *,
    prefix: str,
    suffix: str,
) -> int:
    """Insert a principal + qiita.service_account row; return the principal_idx.

    `prefix` and `suffix` form the display_name as f"{prefix}-{suffix}-{token}";
    the token defends against name collisions across re-runs. The service
    account row uses the principal's display_name verbatim as its `name`.
    """
    name = f"{prefix}-{suffix}-{secrets.token_hex(4)}"
    async with pool.acquire() as conn:
        async with conn.transaction():
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


async def disable_principal(pool: asyncpg.Pool, principal_idx: int) -> None:
    """Mark a principal disabled, populating the audit columns the
    qiita.principal disabled-consistency CHECK requires."""
    await pool.execute(
        "UPDATE qiita.principal SET"
        "  disabled = true, disabled_at = now(), disabled_by_idx = $2"
        " WHERE idx = $1",
        principal_idx,
        SYSTEM_PRINCIPAL_IDX,
    )


async def retire_principal(pool: asyncpg.Pool, principal_idx: int) -> None:
    """Mark a principal retired, populating the audit columns the
    qiita.principal retired-consistency CHECK requires."""
    await pool.execute(
        "UPDATE qiita.principal SET"
        "  retired = true, retired_at = now(), retired_by_idx = $2"
        " WHERE idx = $1",
        principal_idx,
        SYSTEM_PRINCIPAL_IDX,
    )


async def seed_biosample(
    pool: asyncpg.Pool,
    *,
    owner_idx: int,
    created_by_idx: int,
) -> int:
    """Insert a minimal qiita.biosample row; return its idx.

    Only the two NOT-NULL principal references are populated; every
    other column carries its schema default. Sufficient for tests that
    need a biosample idx without exercising accessions, metadata
    checklists, or the import composer.
    """
    return await pool.fetchval(
        "INSERT INTO qiita.biosample (owner_idx, created_by_idx) VALUES ($1, $2) RETURNING idx",
        owner_idx,
        created_by_idx,
    )


async def seed_sequenced_prep_sample(
    pool: asyncpg.Pool,
    *,
    biosample_idx: int,
    owner_idx: int,
    protocol_name: str = "short_read_metagenomics",
) -> int:
    """Insert a minimal qiita.prep_sample row with processing_kind='sequenced';
    return its idx. The prep_protocol is resolved by name (seeded by
    migration 20260501000010); callers that need a different protocol
    pass `protocol_name`. Sufficient for tests that need a sequenced
    prep_sample idx without exercising the sequencing-run / pool surface.
    """
    protocol_idx = await pool.fetchval(
        "SELECT idx FROM qiita.prep_protocol WHERE name = $1",
        protocol_name,
    )
    if protocol_idx is None:
        raise RuntimeError(f"prep_protocol {protocol_name!r} not seeded")
    return await pool.fetchval(
        "INSERT INTO qiita.prep_sample"
        " (biosample_idx, owner_idx, prep_protocol_idx, processing_kind, created_by_idx)"
        " VALUES ($1, $2, $3, 'sequenced'::qiita.processing_kind, $2)"
        " RETURNING idx",
        biosample_idx,
        owner_idx,
        protocol_idx,
    )


async def seed_biosample_with_sequenced_prep_sample(
    pool: asyncpg.Pool,
    *,
    owner_idx: int,
    protocol_name: str = "short_read_metagenomics",
) -> tuple[int, int]:
    """Seed a biosample + sequenced prep_sample owned by `owner_idx`;
    return `(biosample_idx, prep_sample_idx)`.

    Composes `seed_biosample` (owner + created_by both = owner_idx) and
    `seed_sequenced_prep_sample`. Use this from fixtures that need a
    sequenced prep_sample to scope a work_ticket or a sequence_range
    against and want to track both rows for FK-reverse cleanup. Callers
    that need a non-default prep_protocol pass `protocol_name`; the
    underlying helper resolves it by lookup against the seeded protocols
    (qiita.prep_protocol, populated by migration 20260501000010).
    """
    biosample_idx = await seed_biosample(pool, owner_idx=owner_idx, created_by_idx=owner_idx)
    prep_sample_idx = await seed_sequenced_prep_sample(
        pool,
        biosample_idx=biosample_idx,
        owner_idx=owner_idx,
        protocol_name=protocol_name,
    )
    return biosample_idx, prep_sample_idx


async def seed_sequenced_sample_subtype(
    pool: asyncpg.Pool,
    *,
    prep_sample_idx: int,
    owner_idx: int,
    sequenced_pool_item_id: str,
) -> tuple[int, int, int]:
    """Seed the run -> pool -> sequenced_sample subtype chain for an
    existing sequenced prep_sample; return
    `(sequencing_run_idx, sequenced_pool_idx, sequenced_sample_idx)`.

    `prep_sample_idx` must already name a supertype prep_sample row with
    processing_kind='sequenced' (see seed_sequenced_prep_sample). This
    helper attaches the 1:1 sequenced_sample subtype plus the
    sequenced_pool it references, so `sequenced_pool_item_id` is
    populated — the sequenced_sample_pool_pair_consistent CHECK requires
    the pool idx and item id to be set together. Use this from fixtures
    that need a prep_sample carrying a pool item id (e.g. the
    work_ticket fastq-filename-prefix gate). Caller does FK-reverse
    cleanup: sequenced_sample, then sequenced_pool, then sequencing_run.
    """
    run_idx = await pool.fetchval(
        "INSERT INTO qiita.sequencing_run"
        "  (instrument_run_id, platform, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, $2) RETURNING idx",
        f"seed-run-{secrets.token_hex(4)}",
        owner_idx,
    )
    pool_idx = await pool.fetchval(
        "INSERT INTO qiita.sequenced_pool (sequencing_run_idx, created_by_idx)"
        " VALUES ($1, $2) RETURNING idx",
        run_idx,
        owner_idx,
    )
    sequenced_sample_idx = await pool.fetchval(
        "INSERT INTO qiita.sequenced_sample"
        "  (prep_sample_idx, sequenced_pool_idx, sequenced_pool_item_id, created_by_idx)"
        " VALUES ($1, $2, $3, $4) RETURNING idx",
        prep_sample_idx,
        pool_idx,
        sequenced_pool_item_id,
        owner_idx,
    )
    return run_idx, pool_idx, sequenced_sample_idx


async def seed_biosample_global_field(
    pool: asyncpg.Pool,
    *,
    internal_name: str,
    display_name: str,
    data_type: FieldDataType,
    created_by_idx: int,
    terminology_idx: int | None = None,
) -> int:
    """Insert a qiita.biosample_global_field row and return its idx.

    Mirrors the column subset the seven-row migration seed populates:
    internal_name, display_name, data_type, plus the principal that
    created the row. required and default_tier rely on schema defaults.
    description is intentionally omitted -- callers that need a non-null
    description set it via UPDATE so the helper surface stays small.
    asyncpg coerces the StrEnum value to text for the
    qiita.field_data_type cast. terminology_idx must be supplied for
    data_type=TERMINOLOGY (the CHECK enforces the iff coupling) and
    omitted otherwise.
    """
    return await pool.fetchval(
        "INSERT INTO qiita.biosample_global_field"
        "  (internal_name, display_name, data_type, terminology_idx, created_by_idx)"
        " VALUES ($1, $2, $3, $4, $5) RETURNING idx",
        internal_name,
        display_name,
        data_type,
        terminology_idx,
        created_by_idx,
    )


async def seed_prep_sample_global_field(
    pool: asyncpg.Pool,
    *,
    internal_name: str,
    display_name: str,
    data_type: FieldDataType,
    created_by_idx: int,
    terminology_idx: int | None = None,
) -> int:
    """Insert a qiita.prep_sample_global_field row and return its idx.

    Parallel to seed_biosample_global_field; mirrors the same column
    subset (internal_name, display_name, data_type, plus the creating
    principal). required and default_tier rely on schema defaults;
    description is intentionally omitted -- callers that need a non-null
    description set it via UPDATE so the helper surface stays small.
    asyncpg coerces the StrEnum value to text for the
    qiita.field_data_type cast. terminology_idx must be supplied for
    data_type=TERMINOLOGY (the CHECK enforces the iff coupling) and
    omitted otherwise.
    """
    return await pool.fetchval(
        "INSERT INTO qiita.prep_sample_global_field"
        "  (internal_name, display_name, data_type, terminology_idx, created_by_idx)"
        " VALUES ($1, $2, $3, $4, $5) RETURNING idx",
        internal_name,
        display_name,
        data_type,
        terminology_idx,
        created_by_idx,
    )


async def seed_biosample_to_study_link(
    pool: asyncpg.Pool,
    *,
    biosample_idx: int,
    study_idx: int,
    created_by_idx: int,
) -> None:
    """Insert a qiita.biosample_to_study link row at the active retirement state.

    The four retirement columns are CHECK-pinned to NULL/false on a
    fresh row, so they have no place in a create call; created_at
    defaults to now().
    """
    await pool.execute(
        "INSERT INTO qiita.biosample_to_study"
        "  (biosample_idx, study_idx, created_by_idx)"
        " VALUES ($1, $2, $3)",
        biosample_idx,
        study_idx,
        created_by_idx,
    )


async def retire_biosample_to_study_link(
    pool: asyncpg.Pool,
    *,
    biosample_idx: int,
    study_idx: int,
    retired_by_idx: int,
) -> None:
    """UPDATE qiita.biosample_to_study to retire the (biosample, study) link.

    Populates retired, retired_at, and retired_by_idx together so the
    biosample_to_study_retirement_consistent CHECK passes; retire_reason
    is left NULL (the CHECK allows it). Caller supplies retired_by_idx
    explicitly so the helper does not need to know which test fixture
    owns the action.
    """
    await pool.execute(
        "UPDATE qiita.biosample_to_study"
        " SET retired = true, retired_at = now(), retired_by_idx = $3"
        " WHERE biosample_idx = $1 AND study_idx = $2",
        biosample_idx,
        study_idx,
        retired_by_idx,
    )


async def retire_prep_sample_to_study_link(
    pool: asyncpg.Pool,
    *,
    prep_sample_idx: int,
    study_idx: int,
    retired_by_idx: int,
) -> None:
    """UPDATE qiita.prep_sample_to_study to retire the (prep_sample, study)
    link.

    Parallel to retire_biosample_to_study_link: populates retired,
    retired_at, and retired_by_idx together so the
    prep_sample_to_study_retirement_consistent CHECK passes; retire_reason
    is left NULL (the CHECK allows it). Caller supplies retired_by_idx
    explicitly so the helper does not need to know which test fixture
    owns the action.
    """
    await pool.execute(
        "UPDATE qiita.prep_sample_to_study"
        " SET retired = true, retired_at = now(), retired_by_idx = $3"
        " WHERE prep_sample_idx = $1 AND study_idx = $2",
        prep_sample_idx,
        study_idx,
        retired_by_idx,
    )


async def retire_biosample(
    pool: asyncpg.Pool,
    *,
    biosample_idx: int,
    retired_by_idx: int,
) -> None:
    """UPDATE qiita.biosample to retire the biosample entity-wide.

    Populates retired, retired_at, and retired_by_idx together so the
    biosample_retirement_consistent CHECK passes; retire_reason is left
    NULL (the CHECK allows it). Distinct from retiring a single
    biosample_to_study link — this withdraws the sample everywhere.
    """
    await pool.execute(
        "UPDATE qiita.biosample"
        " SET retired = true, retired_at = now(), retired_by_idx = $2"
        " WHERE idx = $1",
        biosample_idx,
        retired_by_idx,
    )
