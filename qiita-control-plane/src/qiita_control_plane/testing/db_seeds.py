"""Pytest seed and state-change helpers for DB-row fixtures.

Plain async functions (not pytest fixtures) so callers can pass test-local
arguments. Helpers fall into four groups: seeders that insert rows and
return the new idx, state-changers that update existing rows (disabling,
retiring, etc.), lookup helpers for migration-seeded reference data that
every test DB carries, and a cleanup-tracking helper that records
import-created rows in a test's `created` dict. Cleanup is the caller's
responsibility
(route tests do FK-reverse cleanup against a per-test `created` tracker;
integration tests may rely on a session-scoped truncate). Helpers are
pool-based and commit their writes — for repository-layer trigger tests
that roll back, build the SQL inline against the open connection instead.
"""

import json
import secrets
import uuid

import asyncpg
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX, SystemRole
from qiita_common.chunking import canonical_sequence_hash_expr
from qiita_common.hashing import canonical_params_hash

# Re-exported (redundant-alias form, so the linter keeps what looks unused here).
# The taxonomy constants live in qiita_common.models now — production code must
# not reach into `testing/` for them — but the suite imports them alongside these
# seed helpers, so they stay reachable from here.
from qiita_common.models import NCBI_TAXONOMY_HUMAN_TERM_ID as NCBI_TAXONOMY_HUMAN_TERM_ID
from qiita_common.models import NCBI_TAXONOMY_NAME as NCBI_TAXONOMY_NAME
from qiita_common.models import FieldDataType, ReferenceStatus

from qiita_control_plane.miint import connect_with_miint
from qiita_control_plane.repositories.host_filter_profile import insert_host_filter_profile

from ..repositories._sample_helpers import (
    EntityMetadataSpec,
    _get_or_create_globally_linked_study_field,
    _get_or_create_local_study_field,
    write_local_metadata_or_diagnose,
)

# Seeded NCBI Taxonomy fixture data — must match the seed migration at
# qiita-control-plane/db/migrations/20260525000000_seed_ncbi_taxonomy.sql.
NCBI_TAXONOMY_METAGENOME_TERM_ID = "256318"


async def fetch_ncbi_taxonomy_term(pool: asyncpg.Pool, term_id: str) -> asyncpg.Record | None:
    """Return a seeded NCBI Taxonomy term row (idx, term_id, label,
    terminology_idx) by its term_id, or None when the migration did not seed it.
    """
    return await pool.fetchrow(
        "SELECT tt.idx, tt.term_id, tt.label, tt.terminology_idx"
        " FROM qiita.terminology_term tt"
        " JOIN qiita.terminology t ON t.idx = tt.terminology_idx"
        " WHERE t.name = $1 AND tt.term_id = $2",
        NCBI_TAXONOMY_NAME,
        term_id,
    )


async def fetch_seeded_metagenome_term(pool: asyncpg.Pool) -> asyncpg.Record:
    """Return the seeded NCBI Taxonomy metagenome term row (idx, term_id,
    label, terminology_idx)."""
    return await fetch_ncbi_taxonomy_term(pool, NCBI_TAXONOMY_METAGENOME_TERM_ID)


async def fetch_missing_value_reason_idx(pool: asyncpg.Pool, name: str) -> int | None:
    """Return the idx of a seeded qiita.missing_value_reason by name, or None.

    The names are INSDC vocabulary seeded by the terminology-lifecycle
    migration ('not applicable', 'not collected', 'missing: control sample', …).
    """
    return await pool.fetchval("SELECT idx FROM qiita.missing_value_reason WHERE name = $1", name)


async def seed_host_reference(
    pool: asyncpg.Pool,
    *,
    name: str,
    created_by_idx: int,
    version: str = "1.0",
) -> int:
    """Insert a host qiita.reference row (is_host=true), return its reference_idx.

    A bare row with no reference_index children — enough to be the target of a
    FK. Tests that need a *built* index register one separately.
    """
    return await pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, is_host, created_by_idx)"
        " VALUES ($1, $2, 'sequence_reference', true, $3)"
        " RETURNING reference_idx",
        name,
        version,
        created_by_idx,
    )


def canonical_sequence_hashes(sequences: list[str]) -> list[uuid.UUID]:
    """The `qiita.feature.sequence_hash` each sequence mints under.

    Evaluates `qiita_common.chunking.canonical_sequence_hash_expr` on a
    miint-loaded DuckDB connection — the expression that module declares every
    minter must use. Returns UUIDs, deduplicated on the canonical hash the way
    `qiita.feature`'s UNIQUE does, so a strand pair yields one entry.

    Uses the client connect path (INSTALL-then-LOAD): tests run off the deploy,
    with no staged `MIINT_EXTENSION_DIRECTORY` and a writable `$HOME`.
    """
    if not sequences:
        return []
    with connect_with_miint() as conn:
        conn.execute("CREATE TABLE _seq (sequence VARCHAR)")
        conn.executemany("INSERT INTO _seq VALUES (?)", [(s,) for s in sequences])
        # The hash expression embeds its argument several times, so it reads a
        # column rather than a bare placeholder.
        rows = conn.execute(
            f"SELECT DISTINCT {canonical_sequence_hash_expr('sequence')} FROM _seq"
        ).fetchall()
    return [row[0] for row in rows]


async def seed_reference_with_sequences(
    pool: asyncpg.Pool,
    *,
    name: str,
    created_by_idx: int,
    sequences: list[str],
    kind: str = "artifact_sequence_set",
    version: str = "1.0",
) -> int:
    """Insert a qiita.reference plus one member feature per sequence; return its
    reference_idx.

    Feature hashes come from `qiita_common.chunking.canonical_sequence_hash_expr`
    on a miint connection — the SAME expression every production minter uses, so
    the seeded features dedup across references the way real ones do. That
    expression is strand-canonical (`LEAST` of the md5 of each strand) and calls
    miint's `sequence_dna_reverse_complement`, so a sequence and its reverse
    complement seed ONE feature: pick sequences accordingly when a test needs a
    given member count. Postgres cannot compute this on its own — a plain
    `md5(sequence)` here would key the seed differently from production and split
    a strand pair into two features.

    Enough for anything that reads a reference's sequence SET (the read mask's
    adapter identity). No DuckLake rows, so it is not enough for anything that
    reads the sequence BYTES.
    """
    reference_idx = await pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, $2, $3, $4, $5)"
        " RETURNING reference_idx",
        name,
        version,
        kind,
        ReferenceStatus.ACTIVE.value,
        created_by_idx,
    )
    sequence_hashes = canonical_sequence_hashes(sequences)
    # Two statements, not one data-modifying CTE: a CTE's INSERT is invisible to
    # the enclosing query's snapshot, so the membership SELECT would find none of
    # the features the CTE just minted.
    await pool.execute(
        "INSERT INTO qiita.feature (sequence_hash)"
        " SELECT unnest($1::uuid[])"
        " ON CONFLICT (sequence_hash) DO NOTHING",
        sequence_hashes,
    )
    await pool.execute(
        "INSERT INTO qiita.reference_membership (reference_idx, feature_idx)"
        " SELECT $1, f.feature_idx FROM qiita.feature f"
        " WHERE f.sequence_hash = ANY($2::uuid[])"
        " ON CONFLICT DO NOTHING",
        reference_idx,
        sequence_hashes,
    )
    return reference_idx


async def delete_reference_with_sequences(pool: asyncpg.Pool, reference_idx: int) -> None:
    """Drop a `seed_reference_with_sequences` reference and its membership.

    The features themselves stay: they are content-addressed and shared across
    references, so deleting them here could strip another reference's members.
    The reference row is what holds the FK to qiita.principal (ON DELETE
    RESTRICT), so removing it is what lets a test's principal cleanup succeed.
    """
    await pool.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", reference_idx
    )
    await pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx)


async def seed_legacy_mask_definition(
    pool: asyncpg.Pool,
    *,
    params: dict,
    created_by_idx: int,
) -> int:
    """Insert a qiita.mask_definition row as it looked before the adapter-identity
    migration: `adapter_hash_scheme` NULL, whatever `adapter_set_hash` `params`
    carries. Returns its mask_idx.

    Raw INSERT rather than `mint_mask_definition`, which stamps the scheme on any
    adapter-bearing config — the point of this helper is the unstamped row, so
    minting through the current path cannot produce it.
    """
    return await pool.fetchval(
        "INSERT INTO qiita.mask_definition"
        " (params_hash, filter_workflow, filter_version, params, created_by_idx)"
        " VALUES ($1, $2, $3, $4::jsonb, $5)"
        " RETURNING mask_idx",
        canonical_params_hash(params),
        params["filter_workflow"],
        params["filter_version"],
        json.dumps(params),
        created_by_idx,
    )


async def seed_host_filter_profile(
    pool: asyncpg.Pool,
    *,
    host_term_idx: int,
    platform: str,
    rype_reference_idx: int,
    created_by_idx: int,
    minimap2_reference_idx: int | None = None,
) -> int:
    """Insert one qiita.host_filter_profile row; return its idx.

    Unlike its siblings here this seeds through the repository rather than inline
    SQL, because the profile's insert is a single plain INSERT with no composition
    — reproducing it here would be a second copy of the same statement to keep in
    step with the table. Tests of the repository itself call the repository
    directly; seeding those through this helper would be circular.
    """
    async with pool.acquire() as conn:
        profile = await insert_host_filter_profile(
            conn,
            host_term_idx=host_term_idx,
            platform=platform,
            rype_reference_idx=rype_reference_idx,
            minimap2_reference_idx=minimap2_reference_idx,
            principal_idx=created_by_idx,
        )
    return profile.idx


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
    sequencing_run_idx: int | None = None,
    sequenced_pool_idx: int | None = None,
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

    Pass `sequencing_run_idx` + `sequenced_pool_idx` (both, as returned by an
    earlier call) to attach this sample to an EXISTING pool instead of standing
    up a fresh run and pool. Without it a multi-sample pool is unreachable
    through this helper, which is why fixtures that needed one used to seed the
    first sample here and hand-write the raw INSERT for the rest.
    """
    if (sequencing_run_idx is None) != (sequenced_pool_idx is None):
        raise ValueError("pass both sequencing_run_idx and sequenced_pool_idx, or neither")
    run_idx = sequencing_run_idx
    pool_idx = sequenced_pool_idx
    if pool_idx is None:
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


async def seed_local_study_field(
    pool: asyncpg.Pool,
    *,
    spec: EntityMetadataSpec,
    study_idx: int,
    display_name: str,
    created_by_idx: int,
    data_type: FieldDataType = FieldDataType.TEXT,
    required: bool = False,
) -> int:
    """Create a purely-local study field for spec's entity and return its idx.

    Delegates to the repository get-or-create so the study-field INSERT and
    inheritance rules stay single-sourced; the caller supplies display_name and
    tracks the returned idx for cleanup. Runs inside an acquired transaction
    because the underlying upsert requires one.
    """
    async with pool.acquire() as conn, conn.transaction():
        idx, _, _ = await _get_or_create_local_study_field(
            conn,
            spec=spec,
            study_idx=study_idx,
            display_name=display_name,
            created_by_idx=created_by_idx,
            data_type=data_type,
            required=required,
        )
    return idx


async def seed_local_metadata_value(
    pool: asyncpg.Pool,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    study_idx: int,
    display_name: str,
    value: str,
    created_by_idx: int,
    data_type: FieldDataType = FieldDataType.TEXT,
) -> tuple[int, int]:
    """Write one purely-local metadata value for spec's entity and return
    (metadata_idx, study_field_idx) for cleanup.

    Delegates to the repository local writer so the study-field get-or-create
    and the value insert stay single-sourced; the caller tracks both returned
    idxs for FK-reverse teardown. Runs inside an acquired transaction because
    the underlying writer requires one.
    """
    async with pool.acquire() as conn, conn.transaction():
        result = await write_local_metadata_or_diagnose(
            conn,
            spec=spec,
            entity_idx=entity_idx,
            study_idx=study_idx,
            display_name=display_name,
            data_type=data_type,
            value=value,
            caller_idx=created_by_idx,
        )
    return result.metadata_idx, result.study_field_idx


async def seed_globally_linked_study_field(
    pool: asyncpg.Pool,
    *,
    spec: EntityMetadataSpec,
    study_idx: int,
    global_field_idx: int,
    display_name: str,
    created_by_idx: int,
) -> int:
    """Create a study field linked to global_field_idx for spec's entity and
    return its idx.

    Delegates to the repository get-or-create so the study-field INSERT and
    inheritance rules stay single-sourced; the caller supplies display_name and
    tracks the returned idx for cleanup. Runs inside an acquired transaction
    because the underlying upsert requires one.
    """
    async with pool.acquire() as conn, conn.transaction():
        idx, _ = await _get_or_create_globally_linked_study_field(
            conn,
            spec=spec,
            study_idx=study_idx,
            global_field_idx=global_field_idx,
            display_name=display_name,
            created_by_idx=created_by_idx,
        )
    return idx


async def track_biosample_metadata_outputs(
    pool: asyncpg.Pool,
    created: dict,
    biosample_idx: int,
    study_idx: int,
    global_field_idxs: list[int],
) -> None:
    """Record, in a test's `created` tracker, the study-field and metadata rows a
    biosample import produced, so FK-reverse cleanup sweeps them.

    Appends every globally-linked biosample_study_field at study_idx tied to one
    of global_field_idxs, plus every non-owner-id biosample_metadata row for
    biosample_idx. An idx already present is not re-appended.
    """
    # Pick up every globally-linked study field row at this study tied to
    # one of the supplied global fields.
    field_rows = await pool.fetch(
        "SELECT idx FROM qiita.biosample_study_field"
        " WHERE study_idx = $1 AND biosample_global_field_idx = ANY($2::bigint[])",
        study_idx,
        list(global_field_idxs),
    )
    for r in field_rows:
        if r["idx"] not in created["biosample_study_field"]:
            created["biosample_study_field"].append(r["idx"])

    # Pick up every non-owner-id metadata row for this biosample.
    meta_rows = await pool.fetch(
        "SELECT idx FROM qiita.biosample_metadata"
        " WHERE biosample_idx = $1 AND is_owner_biosample_id = false",
        biosample_idx,
    )
    for r in meta_rows:
        if r["idx"] not in created["biosample_metadata"]:
            created["biosample_metadata"].append(r["idx"])


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


async def seed_prep_sample_to_study_link(
    pool: asyncpg.Pool,
    *,
    prep_sample_idx: int,
    study_idx: int,
    created_by_idx: int,
) -> None:
    """INSERT the (prep_sample, study) link row.

    The missing sibling of `seed_biosample_to_study_link` and
    `retire_prep_sample_to_study_link` — without it every fixture that needs a
    prep_sample in a study hand-writes this INSERT.
    """
    await pool.execute(
        "INSERT INTO qiita.prep_sample_to_study"
        " (prep_sample_idx, study_idx, created_by_idx) VALUES ($1, $2, $3)",
        prep_sample_idx,
        study_idx,
        created_by_idx,
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


async def seed_action_if_absent(
    pool: asyncpg.Pool, *, action_id: str, version: str, target_kind: str = "block"
) -> bool:
    """Ensure a `qiita.action` row exists; return True iff we made it.

    For the REAL action ids (`qiita_common.actions.BLOCK_MASK_ACTION_ID` /
    `ALIGN_ACTION_ID` / `READ_MASK_ACTION_ID`), which several DB-tier fixtures need
    because a ticket's kind is its action_id and the dispatch pump, the read-mask
    finalize gate, and the mask roster all key on it. A throwaway id would make
    those queries match nothing.

    Unlike a per-test random id, `(action_id, version)` is a FIXED PK several test
    modules share, so a fixture must neither collide with a row another test owns nor
    delete one it did not create. Not a concurrency problem — each xdist worker gets
    its own database and runs its tests serially — but an ORDERING one, across test
    files in a session and across sessions (a crashed prior run, or a persistent
    `QIITA_USE_HOST_POSTGRES=1` host DB, leaves the row behind). Hence
    insert-if-absent plus a did-I-create-it answer: pass the return value to
    `delete_action_if_created` in teardown and the row's lifetime matches its
    creator's.
    """
    created = await pool.fetchval(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status)"
        " VALUES ($1, $2, $3::qiita.scope_target_kind, '{}'::text[], $4::jsonb,"
        "         '{}'::jsonb, '[]'::jsonb, 1, 1, '1 minute', 'active', 'failed')"
        " ON CONFLICT (action_id, version) DO NOTHING"
        " RETURNING action_id",
        action_id,
        version,
        target_kind,
        '{"service": false, "human_roles": ["system_admin"]}',
    )
    return created is not None


async def delete_action_if_created(
    pool: asyncpg.Pool, *, action_id: str, version: str, created: bool
) -> None:
    """Teardown twin of `seed_action_if_absent`: drop the row only if that
    call created it. A no-op when the row pre-existed, so a fixture never deletes
    another test's action out from under it."""
    if not created:
        return
    await pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, version
    )


async def seed_bare_reference(pool: asyncpg.Pool, *, label: str, created_by_idx: int = 1) -> int:
    """Insert a minimal `sequence_reference` with no members; return its idx.

    The no-sequences counterpart of `seed_reference_with_sequences` above, for
    tests that build their own membership rows because they care about the
    feature/genome graph rather than about sequence hashing. `label` is
    suffixed with a uuid so parallel tests never collide on `(name, version)`.
    """
    return await pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', $2) RETURNING reference_idx",
        f"{label}-{uuid.uuid4()}",
        created_by_idx,
    )


async def seed_bare_feature(pool: asyncpg.Pool) -> int:
    """Insert a `qiita.feature` on a random hash; return its feature_idx.

    Random rather than content-derived, so a caller that wants two distinct
    features gets them without minding what sequence would produce that. Use
    `seed_reference_with_sequences` when the hash itself is under test.
    """
    return await pool.fetchval(
        "INSERT INTO qiita.feature (sequence_hash) VALUES (gen_random_uuid()) RETURNING feature_idx"
    )


async def seed_genome(pool: asyncpg.Pool, *, source: str = "refseq") -> tuple[int, str]:
    """Insert a `qiita.genome`; return `(genome_idx, source_id)`.

    The source_id is returned because `qiita.genome`'s uniqueness is the
    composite `(source, source_id)` — a caller asserting on provenance needs the
    generated accession, and generating it here is what keeps it unique.
    """
    source_id = f"GCF_{uuid.uuid4().hex[:12]}"
    genome_idx = await pool.fetchval(
        "INSERT INTO qiita.genome (source, source_id) VALUES ($1, $2) RETURNING genome_idx",
        source,
        source_id,
    )
    return genome_idx, source_id


async def seed_reference_membership(
    pool: asyncpg.Pool, *, reference_idx: int, feature_idx: int, accession: str | None = None
) -> None:
    """Put one feature in one reference. `accession` is the reference's own
    FASTA-header accession for it, nullable (a non-FASTA ingest path has none)."""
    await pool.execute(
        "INSERT INTO qiita.reference_membership (reference_idx, feature_idx, accession)"
        " VALUES ($1, $2, $3)",
        reference_idx,
        feature_idx,
        accession,
    )


async def seed_feature_genome(pool: asyncpg.Pool, *, feature_idx: int, genome_idx: int) -> None:
    """Associate a feature with a genome. Many-to-many since the standalone
    UNIQUE(feature_idx) was dropped, so call it twice to seed a shared plasmid."""
    await pool.execute(
        "INSERT INTO qiita.feature_genome (feature_idx, genome_idx) VALUES ($1, $2)",
        feature_idx,
        genome_idx,
    )


async def cleanup_reference_graph(
    pool: asyncpg.Pool, *, reference_idx: int, feature_idxs=(), genome_idxs=()
) -> None:
    """FK-reverse teardown for the seeders above: feature_genome, membership,
    feature, genome, then the reference itself."""
    if feature_idxs:
        await pool.execute(
            "DELETE FROM qiita.feature_genome WHERE feature_idx = ANY($1::bigint[])",
            list(feature_idxs),
        )
        await pool.execute(
            "DELETE FROM qiita.reference_membership WHERE feature_idx = ANY($1::bigint[])",
            list(feature_idxs),
        )
        await pool.execute(
            "DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])", list(feature_idxs)
        )
    if genome_idxs:
        await pool.execute(
            "DELETE FROM qiita.genome WHERE genome_idx = ANY($1::bigint[])", list(genome_idxs)
        )
    await pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx)
