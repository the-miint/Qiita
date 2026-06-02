"""Integration tests for the core biosample repository surface and the import composer.

Covers the direct repository functions on the qiita.biosample and
biosample_to_study tables, the import_biosample_from_owner_biosample_id
composer (which orchestrates writes across both surfaces), and the
role-typed FK / user-delete-blocking triggers attached to qiita.biosample.

Metadata-shaped functions (biosample_global_field / biosample_study_field /
biosample_metadata helpers and the parser) are tested in
test_biosample_metadata.py; the conftest in this directory hosts the
shared `ctx` fixture and the seed/setup/cleanup helpers both files use.
"""

import secrets
from datetime import UTC, date, datetime
from decimal import Decimal

import asyncpg
import pytest
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX, SystemRole
from qiita_common.models import FieldDataType

from qiita_control_plane.repositories._sample_helpers import (
    LocalWriteOnGloballyLinkedFieldError,
    MetadataParseError,
    MetadataUnknownFieldsError,
    insert_entity_to_study,
)
from qiita_control_plane.repositories.biosample import (
    BiosampleImportResult,
    fetch_biosample,
    fetch_biosample_idxs_by_natural_key,
    fetch_biosample_idxs_for_study,
    fetch_caller_has_biosample_access,
    import_biosample_from_owner_biosample_id,
    insert_biosample,
    update_biosample,
)
from qiita_control_plane.repositories.biosample_metadata import (
    BIOSAMPLE_METADATA_SPEC,
    BiosampleOwnerIdFieldCollisionError,
    BiosampleOwnerIdMissingValueError,
)
from qiita_control_plane.testing.db_seeds import (
    retire_biosample,
    retire_biosample_to_study_link,
    seed_biosample_global_field,
)
from qiita_control_plane.testing.unique_names import (
    unique_accession,
    unique_field_name,
    unique_matrix_tube_id,
)

from .conftest import (
    _create_biosample_with_link,
    _seed_study,
)

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# insert_biosample
# ---------------------------------------------------------------------------


async def test_insert_biosample_minimal(ctx):
    # Insert with only the required principal references; everything else
    # NULL. owner_idx and created_by_idx are distinct principals so the
    # round-trip assertion exercises the admin-creates-on-behalf-of-owner case.
    async with ctx["pool"].acquire() as conn:
        idx = await insert_biosample(
            conn,
            owner_idx=ctx["biosample_owner_idx"],
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample"].append(idx)

    # Verify the full row matches the expected default-laden shape.
    row = await ctx["pool"].fetchrow(
        "SELECT owner_idx, created_by_idx, metadata_checklist_idx,"
        " biosample_accession, ena_sample_accession, matrix_tube_id, retired"
        " FROM qiita.biosample WHERE idx = $1",
        idx,
    )
    expected = {
        "owner_idx": ctx["biosample_owner_idx"],
        "created_by_idx": ctx["principal_idx"],
        "metadata_checklist_idx": None,
        "biosample_accession": None,
        "ena_sample_accession": None,
        "matrix_tube_id": None,
        "retired": False,
    }
    assert dict(row) == expected


async def test_insert_biosample_full_columns(ctx):
    # Unique-per-test accessions avoid the UNIQUE constraint on bare values.
    bs_acc = unique_accession("BS")
    ena_acc = unique_accession("ENA")
    tube_id = unique_matrix_tube_id()

    # Insert with every caller-settable column populated. owner_idx and
    # created_by_idx are distinct principals so the round-trip assertion
    # exercises the admin-creates-on-behalf-of-owner case.
    async with ctx["pool"].acquire() as conn:
        idx = await insert_biosample(
            conn,
            owner_idx=ctx["biosample_owner_idx"],
            created_by_idx=ctx["principal_idx"],
            metadata_checklist_idx=ctx["checklist_idx"],
            biosample_accession=bs_acc,
            ena_sample_accession=ena_acc,
            matrix_tube_id=tube_id,
        )
    ctx["created"]["biosample"].append(idx)

    # Confirm the populated columns round-trip onto the inserted row;
    # matrix_tube_id round-trips with its leading zero intact.
    row = await ctx["pool"].fetchrow(
        "SELECT owner_idx, created_by_idx, metadata_checklist_idx,"
        " biosample_accession, ena_sample_accession, matrix_tube_id"
        " FROM qiita.biosample WHERE idx = $1",
        idx,
    )
    expected = {
        "owner_idx": ctx["biosample_owner_idx"],
        "created_by_idx": ctx["principal_idx"],
        "metadata_checklist_idx": ctx["checklist_idx"],
        "biosample_accession": bs_acc,
        "ena_sample_accession": ena_acc,
        "matrix_tube_id": tube_id,
    }
    assert dict(row) == expected


# ---------------------------------------------------------------------------
# import_biosample_from_owner_biosample_id (composer)
# ---------------------------------------------------------------------------


async def _track_composer_outputs(ctx, bs_idx, study_idx, field_name):
    """Look up the rows the composer created on top of bs_idx and track them.

    The composer returns only the new biosample idx; the dependent link,
    field, and owner-biosample-id metadata rows are looked up by their natural keys
    so the cleanup fixture can sweep them in FK-reverse order.
    """
    # Biosample and link are addressable from the composer's inputs.
    ctx["created"]["biosample"].append(bs_idx)
    ctx["created"]["biosample_to_study"].append((bs_idx, study_idx))

    # Find the local field by (study_idx, display_name) and dedupe — multiple
    # composer calls may share the same field, and we only want to delete it once.
    field_idx = await ctx["pool"].fetchval(
        "SELECT idx FROM qiita.biosample_study_field WHERE study_idx = $1 AND display_name = $2",
        study_idx,
        field_name,
    )
    if field_idx is not None and field_idx not in ctx["created"]["biosample_study_field"]:
        ctx["created"]["biosample_study_field"].append(field_idx)

    # Find the owner-biosample-id metadata row for this biosample.
    meta_idx = await ctx["pool"].fetchval(
        "SELECT idx FROM qiita.biosample_metadata"
        " WHERE biosample_idx = $1 AND is_owner_biosample_id = true",
        bs_idx,
    )
    if meta_idx is not None:
        ctx["created"]["biosample_metadata"].append(meta_idx)


async def test_import_biosample_from_owner_biosample_id_creates_full_chain(ctx):
    field_name = unique_field_name()

    # Compose the import inside a transaction (route layer's responsibility
    # in production; the test plays that role here). owner_idx and
    # caller_idx are distinct principals so the test exercises the
    # admin-imports-on-behalf-of-owner case.
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            result = await import_biosample_from_owner_biosample_id(
                conn,
                primary_study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=field_name,
                owner_biosample_id_value="OWNER-XYZ-1",
                caller_idx=ctx["principal_idx"],
                metadata={},
            )
    bs_idx = result.biosample_idx
    await _track_composer_outputs(ctx, bs_idx, ctx["study_idx"], field_name)

    # Verify the four rows exist with the expected shape.
    bs_row = await ctx["pool"].fetchrow(
        "SELECT owner_idx, created_by_idx, metadata_checklist_idx"
        " FROM qiita.biosample WHERE idx = $1",
        bs_idx,
    )
    link_row = await ctx["pool"].fetchrow(
        "SELECT biosample_idx, study_idx, created_by_idx"
        " FROM qiita.biosample_to_study"
        " WHERE biosample_idx = $1 AND study_idx = $2",
        bs_idx,
        ctx["study_idx"],
    )
    field_row = await ctx["pool"].fetchrow(
        "SELECT idx, study_idx, display_name, data_type, required,"
        " tier_override, biosample_global_field_idx"
        " FROM qiita.biosample_study_field"
        " WHERE study_idx = $1 AND display_name = $2",
        ctx["study_idx"],
        field_name,
    )
    meta_row = await ctx["pool"].fetchrow(
        "SELECT biosample_idx, biosample_study_field_idx, value_text,"
        " is_owner_biosample_id, created_by_idx"
        " FROM qiita.biosample_metadata"
        " WHERE biosample_idx = $1 AND is_owner_biosample_id = true",
        bs_idx,
    )
    field_idx = field_row["idx"]

    # Composer's structured return must report the new field idx and that
    # this call created it (single-call test, no concurrent winner possible).
    assert result == BiosampleImportResult(
        biosample_idx=bs_idx,
        owner_id_biosample_study_field_idx=field_idx,
        owner_id_biosample_study_field_created=True,
    )
    actual = {
        "biosample": dict(bs_row),
        "link": dict(link_row),
        "field": dict(field_row),
        "metadata": dict(meta_row),
    }
    expected = {
        "biosample": {
            "owner_idx": ctx["biosample_owner_idx"],
            "created_by_idx": ctx["principal_idx"],
            "metadata_checklist_idx": None,
        },
        "link": {
            "biosample_idx": bs_idx,
            "study_idx": ctx["study_idx"],
            "created_by_idx": ctx["principal_idx"],
        },
        "field": {
            "idx": field_idx,
            "study_idx": ctx["study_idx"],
            "display_name": field_name,
            "data_type": "text",
            "required": True,
            "tier_override": "member",
            "biosample_global_field_idx": None,
        },
        "metadata": {
            "biosample_idx": bs_idx,
            "biosample_study_field_idx": field_idx,
            "value_text": "OWNER-XYZ-1",
            "is_owner_biosample_id": True,
            "created_by_idx": ctx["principal_idx"],
        },
    }
    assert actual == expected


async def test_import_biosample_from_owner_biosample_id_with_explicit_checklist(ctx):
    field_name = unique_field_name()
    bs_acc = unique_accession("BS")

    # Pass through metadata_checklist_idx and biosample_accession on the composer.
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            result = await import_biosample_from_owner_biosample_id(
                conn,
                primary_study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=field_name,
                owner_biosample_id_value="OWNER-CL-1",
                caller_idx=ctx["principal_idx"],
                metadata={},
                metadata_checklist_idx=ctx["checklist_idx"],
                biosample_accession=bs_acc,
            )
    bs_idx = result.biosample_idx
    await _track_composer_outputs(ctx, bs_idx, ctx["study_idx"], field_name)

    # Confirm the optional pass-throughs round-tripped onto the biosample row.
    row = await ctx["pool"].fetchrow(
        "SELECT metadata_checklist_idx, biosample_accession FROM qiita.biosample WHERE idx = $1",
        bs_idx,
    )
    expected = {
        "metadata_checklist_idx": ctx["checklist_idx"],
        "biosample_accession": bs_acc,
    }
    assert dict(row) == expected


async def test_import_biosample_from_owner_biosample_id_reuses_local_field_for_same_name(ctx):
    field_name = unique_field_name()

    # Two imports against the same study with the same owner-biosample-id field name —
    # the second must reuse the field row rather than creating a new one.
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            result1 = await import_biosample_from_owner_biosample_id(
                conn,
                primary_study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=field_name,
                owner_biosample_id_value="A",
                caller_idx=ctx["principal_idx"],
                metadata={},
            )
            result2 = await import_biosample_from_owner_biosample_id(
                conn,
                primary_study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=field_name,
                owner_biosample_id_value="B",
                caller_idx=ctx["principal_idx"],
                metadata={},
            )
    bs1 = result1.biosample_idx
    bs2 = result2.biosample_idx
    await _track_composer_outputs(ctx, bs1, ctx["study_idx"], field_name)
    await _track_composer_outputs(ctx, bs2, ctx["study_idx"], field_name)

    # Composer-level created-flag contract: first call inserts the field
    # (created=True); second call resolves it from the existing row
    # (created=False) and both report the same field idx.
    assert result1.owner_id_biosample_study_field_created is True
    assert result2.owner_id_biosample_study_field_created is False
    assert result1.owner_id_biosample_study_field_idx == result2.owner_id_biosample_study_field_idx

    # Exactly one field row exists for this (study, name); the two metadata
    # rows point at it.
    field_rows = await ctx["pool"].fetch(
        "SELECT idx FROM qiita.biosample_study_field WHERE study_idx = $1 AND display_name = $2",
        ctx["study_idx"],
        field_name,
    )
    assert len(field_rows) == 1
    field_idx = field_rows[0]["idx"]

    meta_field_idxs = await ctx["pool"].fetch(
        "SELECT biosample_study_field_idx FROM qiita.biosample_metadata"
        " WHERE biosample_idx = ANY($1::bigint[]) AND is_owner_biosample_id = true"
        " ORDER BY biosample_idx",
        [bs1, bs2],
    )
    assert [r["biosample_study_field_idx"] for r in meta_field_idxs] == [
        field_idx,
        field_idx,
    ]


async def test_import_biosample_from_owner_biosample_id_creates_distinct_fields_for_distinct_names(
    ctx,
):
    name_a = unique_field_name("owner_biosample_id_a")
    name_b = unique_field_name("owner_biosample_id_b")

    # Two imports against the same study with different field names produce
    # two separate field rows.
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            result1 = await import_biosample_from_owner_biosample_id(
                conn,
                primary_study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=name_a,
                owner_biosample_id_value="A",
                caller_idx=ctx["principal_idx"],
                metadata={},
            )
            result2 = await import_biosample_from_owner_biosample_id(
                conn,
                primary_study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=name_b,
                owner_biosample_id_value="B",
                caller_idx=ctx["principal_idx"],
                metadata={},
            )
    bs1 = result1.biosample_idx
    bs2 = result2.biosample_idx
    await _track_composer_outputs(ctx, bs1, ctx["study_idx"], name_a)
    await _track_composer_outputs(ctx, bs2, ctx["study_idx"], name_b)

    # Distinct field names → both calls hit the insert branch → both report created=True.
    assert result1.owner_id_biosample_study_field_created is True
    assert result2.owner_id_biosample_study_field_created is True

    # Two field rows, one per name.
    rows = await ctx["pool"].fetch(
        "SELECT display_name FROM qiita.biosample_study_field"
        " WHERE study_idx = $1 AND display_name = ANY($2::text[])"
        " ORDER BY display_name",
        ctx["study_idx"],
        [name_a, name_b],
    )
    expected = [{"display_name": name_a}, {"display_name": name_b}]
    assert [dict(r) for r in rows] == expected


async def test_import_biosample_from_owner_biosample_id_rolls_back_on_failed_step(ctx):
    bs_acc = unique_accession("BS-rollback")
    bad_study_idx = -1  # nonexistent; the link insert FK will reject it

    # The composer succeeds at step a (biosample insert) and fails at step b
    # (link insert). Wrapping in a transaction must roll back the biosample.
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            async with conn.transaction():
                await import_biosample_from_owner_biosample_id(
                    conn,
                    primary_study_idx=bad_study_idx,
                    owner_idx=ctx["biosample_owner_idx"],
                    owner_biosample_id_field_name=unique_field_name(),
                    owner_biosample_id_value="DOOMED",
                    caller_idx=ctx["principal_idx"],
                    metadata={},
                    biosample_accession=bs_acc,
                )

    # No biosample row should have persisted under our marker accession.
    found = await ctx["pool"].fetchval(
        "SELECT idx FROM qiita.biosample WHERE biosample_accession = $1",
        bs_acc,
    )
    assert found is None


async def test_import_biosample_from_owner_biosample_id_uses_independent_field_per_study(ctx):
    field_name = unique_field_name()

    # Seed a second study owned by the same principal so the same display_name
    # can appear in both. The biosample_study_field UNIQUE (study_idx,
    # display_name) constraint is study-scoped, so two rows must result.
    second_study_idx = await _seed_study(
        ctx["pool"], ctx["principal_idx"], f"bs-extra-{secrets.token_hex(4)}"
    )
    ctx["created"]["studies"].append(second_study_idx)

    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            result1 = await import_biosample_from_owner_biosample_id(
                conn,
                primary_study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=field_name,
                owner_biosample_id_value="STUDY1-1",
                caller_idx=ctx["principal_idx"],
                metadata={},
            )
            result2 = await import_biosample_from_owner_biosample_id(
                conn,
                primary_study_idx=second_study_idx,
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=field_name,
                owner_biosample_id_value="STUDY2-1",
                caller_idx=ctx["principal_idx"],
                metadata={},
            )
    bs1 = result1.biosample_idx
    bs2 = result2.biosample_idx
    await _track_composer_outputs(ctx, bs1, ctx["study_idx"], field_name)
    await _track_composer_outputs(ctx, bs2, second_study_idx, field_name)

    # Independent (study_idx, display_name) keys → both hit the insert branch.
    assert result1.owner_id_biosample_study_field_created is True
    assert result2.owner_id_biosample_study_field_created is True

    # Two distinct field rows, one per study, sharing the same display_name.
    rows = await ctx["pool"].fetch(
        "SELECT study_idx, display_name FROM qiita.biosample_study_field"
        " WHERE display_name = $1 AND study_idx = ANY($2::bigint[])"
        " ORDER BY study_idx",
        field_name,
        sorted([ctx["study_idx"], second_study_idx]),
    )
    expected = [
        {"study_idx": s, "display_name": field_name}
        for s in sorted([ctx["study_idx"], second_study_idx])
    ]
    assert [dict(r) for r in rows] == expected


async def test_import_biosample_from_owner_biosample_id_rejects_non_transactional_connection(ctx):
    # The composer's writes must roll back atomically on partial failure;
    # without a transaction wrapper, a mid-flight failure leaves orphan rows.
    # The fail-fast guard rejects the call before any write happens.
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(RuntimeError, match="transaction"):
            await import_biosample_from_owner_biosample_id(
                conn,
                primary_study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=unique_field_name(),
                owner_biosample_id_value="GUARD-CHECK",
                caller_idx=ctx["principal_idx"],
                metadata={},
            )


async def _track_global_metadata_outputs(ctx, bs_idx, study_idx, global_idxs):
    """Track globally-linked study fields (by global field idx) and every
    non-owner-id metadata row written for this biosample. Use after
    `_track_composer_outputs` in tests that exercised the metadata dict
    path so the FK-reverse cleanup picks the new rows up.
    """
    # Pick up every globally-linked study field row at this study tied to
    # one of the supplied global fields.
    rows = await ctx["pool"].fetch(
        "SELECT idx FROM qiita.biosample_study_field"
        " WHERE study_idx = $1 AND biosample_global_field_idx = ANY($2::bigint[])",
        study_idx,
        list(global_idxs),
    )
    for r in rows:
        if r["idx"] not in ctx["created"]["biosample_study_field"]:
            ctx["created"]["biosample_study_field"].append(r["idx"])

    # Pick up every non-owner-id metadata row for this biosample. The
    # owner-id row is already tracked by _track_composer_outputs.
    meta_rows = await ctx["pool"].fetch(
        "SELECT idx FROM qiita.biosample_metadata"
        " WHERE biosample_idx = $1 AND is_owner_biosample_id = false",
        bs_idx,
    )
    for r in meta_rows:
        if r["idx"] not in ctx["created"]["biosample_metadata"]:
            ctx["created"]["biosample_metadata"].append(r["idx"])


async def test_import_biosample_from_owner_biosample_id_writes_global_metadata(ctx):
    suffix = secrets.token_hex(4)

    # Two global fields with distinct typed columns to round-trip.
    date_global = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"date_{suffix}",
        display_name=f"Collection Date {suffix}",
        data_type=FieldDataType.DATE,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    num_global = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"num_{suffix}",
        display_name=f"Latitude {suffix}",
        data_type=FieldDataType.NUMERIC,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"]["biosample_global_field"].extend([date_global, num_global])

    field_name = unique_field_name()
    metadata_payload = {
        f"Collection Date {suffix}": "2026-05-06",
        f"Latitude {suffix}": "32.7",
    }

    # Compose the import with metadata covering both global fields.
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            result = await import_biosample_from_owner_biosample_id(
                conn,
                primary_study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=field_name,
                owner_biosample_id_value="OWNER-WG-1",
                caller_idx=ctx["principal_idx"],
                metadata=metadata_payload,
            )
    bs_idx = result.biosample_idx
    await _track_composer_outputs(ctx, bs_idx, ctx["study_idx"], field_name)
    await _track_global_metadata_outputs(ctx, bs_idx, ctx["study_idx"], [date_global, num_global])

    # Verify two globally-linked study field rows landed under the seeded globals.
    field_rows = await ctx["pool"].fetch(
        "SELECT biosample_global_field_idx, display_name"
        " FROM qiita.biosample_study_field"
        " WHERE study_idx = $1 AND biosample_global_field_idx = ANY($2::bigint[])"
        " ORDER BY biosample_global_field_idx",
        ctx["study_idx"],
        sorted([date_global, num_global]),
    )
    expected_fields = sorted(
        [
            {
                "biosample_global_field_idx": date_global,
                "display_name": f"Collection Date {suffix}",
            },
            {
                "biosample_global_field_idx": num_global,
                "display_name": f"Latitude {suffix}",
            },
        ],
        key=lambda r: r["biosample_global_field_idx"],
    )
    assert [dict(r) for r in field_rows] == expected_fields

    # Verify the typed values landed in the matching value_* columns.
    meta_rows = await ctx["pool"].fetch(
        "SELECT global_field_idx, value_text, value_numeric, value_date"
        " FROM qiita.biosample_metadata"
        " WHERE biosample_idx = $1 AND is_owner_biosample_id = false"
        " ORDER BY global_field_idx",
        bs_idx,
    )
    expected_meta = sorted(
        [
            {
                "global_field_idx": date_global,
                "value_text": None,
                "value_numeric": None,
                "value_date": date(2026, 5, 6),
            },
            {
                "global_field_idx": num_global,
                "value_text": None,
                "value_numeric": Decimal("32.7"),
                "value_date": None,
            },
        ],
        key=lambda r: r["global_field_idx"],
    )
    assert [dict(r) for r in meta_rows] == expected_meta


async def test_import_biosample_from_owner_biosample_id_rejects_globally_linked_owner_field(ctx):
    suffix = secrets.token_hex(4)
    linked_name = f"Globally Linked {suffix}"

    # Seed a global field and write a metadata value against it so a
    # globally-linked biosample_study_field row exists at
    # (study_idx, linked_name) for the owner-id step to later resolve.
    global_idx = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"glob_{suffix}",
        display_name=linked_name,
        data_type=FieldDataType.TEXT,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"]["biosample_global_field"].append(global_idx)

    seed_owner_field = unique_field_name()
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            seed_result = await import_biosample_from_owner_biosample_id(
                conn,
                primary_study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=seed_owner_field,
                owner_biosample_id_value="SEED-OWNER",
                caller_idx=ctx["principal_idx"],
                metadata={linked_name: "seed-value"},
            )
    await _track_composer_outputs(
        ctx, seed_result.biosample_idx, ctx["study_idx"], seed_owner_field
    )
    await _track_global_metadata_outputs(
        ctx, seed_result.biosample_idx, ctx["study_idx"], [global_idx]
    )

    # Reuse the globally-linked display_name AS the owner-id field. The
    # pre-flight collision check only inspects the metadata dict (empty
    # here), so the call proceeds to step d, where get-or-create resolves
    # the existing globally-linked row instead of creating a local one —
    # strict-mode must reject rather than write PII through a global slot.
    bs_acc = unique_accession("BS-glob-owner")
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(LocalWriteOnGloballyLinkedFieldError):
            async with conn.transaction():
                await import_biosample_from_owner_biosample_id(
                    conn,
                    primary_study_idx=ctx["study_idx"],
                    owner_idx=ctx["biosample_owner_idx"],
                    owner_biosample_id_field_name=linked_name,
                    owner_biosample_id_value="DOOMED",
                    caller_idx=ctx["principal_idx"],
                    metadata={},
                    biosample_accession=bs_acc,
                )

    # The rejection fires after the second call's biosample/link writes;
    # the caller's transaction must roll all of it back.
    found = await ctx["pool"].fetchval(
        "SELECT idx FROM qiita.biosample WHERE biosample_accession = $1",
        bs_acc,
    )
    assert found is None


async def test_import_biosample_from_owner_biosample_id_with_empty_metadata(ctx):
    field_name = unique_field_name()

    # Empty metadata dict — composer must skip the global-metadata block
    # entirely and write only the owner-biosample-id metadata row.
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            result = await import_biosample_from_owner_biosample_id(
                conn,
                primary_study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=field_name,
                owner_biosample_id_value="OWNER-EM-1",
                caller_idx=ctx["principal_idx"],
                metadata={},
            )
    bs_idx = result.biosample_idx
    await _track_composer_outputs(ctx, bs_idx, ctx["study_idx"], field_name)

    # Exactly one biosample_metadata row exists; it is the owner-id row.
    rows = await ctx["pool"].fetch(
        "SELECT is_owner_biosample_id FROM qiita.biosample_metadata WHERE biosample_idx = $1",
        bs_idx,
    )
    assert [dict(r) for r in rows] == [{"is_owner_biosample_id": True}]


async def test_import_biosample_from_owner_biosample_id_raises_on_unknown_metadata_field(ctx):
    field_name = unique_field_name()
    suffix = secrets.token_hex(4)
    unknown_a = f"Unknown A {suffix}"
    unknown_b = f"Unknown B {suffix}"

    # Two metadata keys that have no matching biosample_global_field row.
    # The composer must collect both into one MetadataUnknownFieldsError
    # before any writes.
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(MetadataUnknownFieldsError) as excinfo:
            async with conn.transaction():
                await import_biosample_from_owner_biosample_id(
                    conn,
                    primary_study_idx=ctx["study_idx"],
                    owner_idx=ctx["biosample_owner_idx"],
                    owner_biosample_id_field_name=field_name,
                    owner_biosample_id_value="UNKNOWN",
                    caller_idx=ctx["principal_idx"],
                    metadata={unknown_a: "x", unknown_b: "y"},
                )
    assert sorted(excinfo.value.unknown_display_names) == sorted([unknown_a, unknown_b])


async def test_import_biosample_from_owner_biosample_id_raises_on_metadata_parse_failure(ctx):
    suffix = secrets.token_hex(4)
    numeric_field_name = f"item_number_{suffix}"
    global_idx = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"item_{suffix}",
        display_name=numeric_field_name,
        data_type=FieldDataType.NUMERIC,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"]["biosample_global_field"].append(global_idx)

    field_name = unique_field_name()

    # Numeric global field, garbage value — composer raises pre-write.
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(MetadataParseError) as excinfo:
            async with conn.transaction():
                await import_biosample_from_owner_biosample_id(
                    conn,
                    primary_study_idx=ctx["study_idx"],
                    owner_idx=ctx["biosample_owner_idx"],
                    owner_biosample_id_field_name=field_name,
                    owner_biosample_id_value="PARSE-FAIL",
                    caller_idx=ctx["principal_idx"],
                    metadata={numeric_field_name: "not-a-number"},
                )
    assert excinfo.value.display_name == numeric_field_name
    assert excinfo.value.data_type == FieldDataType.NUMERIC


async def test_import_biosample_from_owner_biosample_id_raises_on_owner_id_field_collision(ctx):
    # The owner-biosample-id field name appears as a metadata key. The
    # owner-id row must remain purely-local; sharing the display_name with
    # a globally-linked metadata entry is rejected pre-write.
    shared_name = unique_field_name("collide")

    async with ctx["pool"].acquire() as conn:
        with pytest.raises(BiosampleOwnerIdFieldCollisionError) as excinfo:
            async with conn.transaction():
                await import_biosample_from_owner_biosample_id(
                    conn,
                    primary_study_idx=ctx["study_idx"],
                    owner_idx=ctx["biosample_owner_idx"],
                    owner_biosample_id_field_name=shared_name,
                    owner_biosample_id_value="OWNER-COLL",
                    caller_idx=ctx["principal_idx"],
                    metadata={shared_name: "x"},
                )
    assert excinfo.value.display_name == shared_name


async def test_import_biosample_from_owner_biosample_id_owner_id_missing_value_raises(ctx):
    """Tests the case where owner_biosample_id_value matches a known
    missing_value_reason name: the composer raises
    BiosampleOwnerIdMissingValueError before any DB write commits.
    """
    reason_name = f"reason_{secrets.token_hex(4)}"
    reason_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.missing_value_reason (name) VALUES ($1) RETURNING idx",
        reason_name,
    )
    ctx["created"]["missing_value_reason"].append(reason_idx)
    field_name = unique_field_name("owner_missing")

    # Run the composer in a transaction so partial state rolls back; the
    # caller-facing 422 path on the route also relies on the same rollback.
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(BiosampleOwnerIdMissingValueError) as excinfo:
            async with conn.transaction():
                await import_biosample_from_owner_biosample_id(
                    conn,
                    primary_study_idx=ctx["study_idx"],
                    owner_idx=ctx["biosample_owner_idx"],
                    owner_biosample_id_field_name=field_name,
                    owner_biosample_id_value=reason_name,
                    caller_idx=ctx["principal_idx"],
                    metadata={},
                )
    assert excinfo.value.owner_biosample_id_value == reason_name
    assert excinfo.value.reason_idx == reason_idx

    # No biosample row was created: the rejection fired before any INSERT.
    bs_count = await ctx["pool"].fetchval(
        "SELECT COUNT(*) FROM qiita.biosample WHERE owner_idx = $1",
        ctx["biosample_owner_idx"],
    )
    assert bs_count == 0


async def test_import_biosample_from_owner_biosample_id_owner_id_padded_marker_raises(ctx):
    """Tests the case where owner_biosample_id_value matches a known
    missing_value_reason name surrounded by whitespace: the composer strips
    the value for marker recognition and still raises
    BiosampleOwnerIdMissingValueError, carrying the raw padded value.
    """
    reason_name = f"reason_{secrets.token_hex(4)}"
    reason_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.missing_value_reason (name) VALUES ($1) RETURNING idx",
        reason_name,
    )
    ctx["created"]["missing_value_reason"].append(reason_idx)
    field_name = unique_field_name("owner_padded_missing")
    padded_value = f"  {reason_name}  "

    # Run the composer in a transaction so partial state rolls back.
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(BiosampleOwnerIdMissingValueError) as excinfo:
            async with conn.transaction():
                await import_biosample_from_owner_biosample_id(
                    conn,
                    primary_study_idx=ctx["study_idx"],
                    owner_idx=ctx["biosample_owner_idx"],
                    owner_biosample_id_field_name=field_name,
                    owner_biosample_id_value=padded_value,
                    caller_idx=ctx["principal_idx"],
                    metadata={},
                )
    # The raw padded value is preserved on the error so the user sees what they sent.
    assert excinfo.value.owner_biosample_id_value == padded_value
    assert excinfo.value.reason_idx == reason_idx

    # No biosample row was created: the rejection fired before any INSERT.
    bs_count = await ctx["pool"].fetchval(
        "SELECT COUNT(*) FROM qiita.biosample WHERE owner_idx = $1",
        ctx["biosample_owner_idx"],
    )
    assert bs_count == 0


async def test_import_biosample_from_owner_biosample_id_metadata_missing_value_persists(ctx):
    """Tests the case where a metadata text value matches a known
    missing_value_reason name: the composer routes the value to
    value_missing_reason_idx and the resulting row carries every typed
    value column NULL.
    """
    suffix = secrets.token_hex(4)
    reason_name = f"reason_{suffix}"
    reason_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.missing_value_reason (name) VALUES ($1) RETURNING idx",
        reason_name,
    )
    ctx["created"]["missing_value_reason"].append(reason_idx)

    # NUMERIC global field; a literal reason name would fail typed parsing
    # without the missing-reason routing, so the test pinpoints that
    # routing fires.
    global_idx = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"num_{suffix}",
        display_name=f"Latitude {suffix}",
        data_type=FieldDataType.NUMERIC,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"]["biosample_global_field"].append(global_idx)

    field_name = unique_field_name("owner_id")
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            result = await import_biosample_from_owner_biosample_id(
                conn,
                primary_study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=field_name,
                owner_biosample_id_value="OWNER-MV-1",
                caller_idx=ctx["principal_idx"],
                metadata={f"Latitude {suffix}": reason_name},
            )
    bs_idx = result.biosample_idx
    await _track_composer_outputs(ctx, bs_idx, ctx["study_idx"], field_name)
    await _track_global_metadata_outputs(ctx, bs_idx, ctx["study_idx"], [global_idx])

    # Assert one non-owner-id metadata row exists, with
    # value_missing_reason_idx populated and every typed column NULL.
    rows = await ctx["pool"].fetch(
        "SELECT global_field_idx, value_text, value_numeric, value_date,"
        " value_missing_reason_idx"
        " FROM qiita.biosample_metadata"
        " WHERE biosample_idx = $1 AND is_owner_biosample_id = false",
        bs_idx,
    )
    assert [dict(r) for r in rows] == [
        {
            "global_field_idx": global_idx,
            "value_text": None,
            "value_numeric": None,
            "value_date": None,
            "value_missing_reason_idx": reason_idx,
        }
    ]


# ---------------------------------------------------------------------------
# fetch_biosample_idxs_for_study
# ---------------------------------------------------------------------------


async def test_fetch_biosample_idxs_for_study_returns_empty_for_no_links(ctx):
    # The fixture's study has no biosample links yet; the read returns [].
    result = await fetch_biosample_idxs_for_study(ctx["pool"], study_idx=ctx["study_idx"], limit=10)
    assert result == []


async def test_fetch_biosample_idxs_for_study_orders_newest_link_first(ctx):
    # Three links inserted sequentially have monotonically increasing
    # created_at; the read orders by (created_at DESC, idx DESC) so the
    # last-inserted comes first.
    bs_idxs = []
    for _ in range(3):
        async with ctx["pool"].acquire() as conn:
            bs_idx = await insert_biosample(
                conn,
                owner_idx=ctx["principal_idx"],
                created_by_idx=ctx["principal_idx"],
            )
            await insert_entity_to_study(
                conn,
                spec=BIOSAMPLE_METADATA_SPEC,
                entity_idx=bs_idx,
                study_idx=ctx["study_idx"],
                created_by_idx=ctx["principal_idx"],
            )
        ctx["created"]["biosample"].append(bs_idx)
        ctx["created"]["biosample_to_study"].append((bs_idx, ctx["study_idx"]))
        bs_idxs.append(bs_idx)

    result = await fetch_biosample_idxs_for_study(ctx["pool"], study_idx=ctx["study_idx"], limit=10)
    # Newest-linked first: the third insert is the head of the list.
    assert result == list(reversed(bs_idxs))


async def test_fetch_biosample_idxs_for_study_excludes_retired_link(ctx):
    # Two links: one active, one retired at the link level. The retired
    # link is filtered out; the underlying biosample row is unaffected.
    active_idx = await _create_biosample_with_link(ctx)
    retired_link_idx = await _create_biosample_with_link(ctx)
    await retire_biosample_to_study_link(
        ctx["pool"],
        biosample_idx=retired_link_idx,
        study_idx=ctx["study_idx"],
        retired_by_idx=ctx["principal_idx"],
    )

    result = await fetch_biosample_idxs_for_study(ctx["pool"], study_idx=ctx["study_idx"], limit=10)
    assert result == [active_idx]


async def test_fetch_biosample_idxs_for_study_excludes_retired_biosample(ctx):
    # Two links with active link rows; the underlying biosample of the
    # second is retired entity-wide. The active-link/active-biosample row
    # is the only one returned.
    active_idx = await _create_biosample_with_link(ctx)
    retired_bs_idx = await _create_biosample_with_link(ctx)
    await retire_biosample(
        ctx["pool"],
        biosample_idx=retired_bs_idx,
        retired_by_idx=ctx["principal_idx"],
    )

    result = await fetch_biosample_idxs_for_study(ctx["pool"], study_idx=ctx["study_idx"], limit=10)
    assert result == [active_idx]


async def test_fetch_biosample_idxs_for_study_respects_limit(ctx):
    # Insert three links; ask for two. The DB returns exactly the two
    # newest under the documented sort order.
    bs_idxs = []
    for _ in range(3):
        bs_idxs.append(await _create_biosample_with_link(ctx))

    result = await fetch_biosample_idxs_for_study(ctx["pool"], study_idx=ctx["study_idx"], limit=2)
    # The two newest are the last two appended; reversed to put newest first.
    assert result == [bs_idxs[2], bs_idxs[1]]


# ---------------------------------------------------------------------------
# fetch_biosample
# ---------------------------------------------------------------------------


async def test_fetch_biosample_returns_row(ctx):
    # Seed a biosample with the full set of caller-settable columns so the
    # round-trip exercise covers every value the read surfaces.
    bs_acc = unique_accession("BS")
    ena_acc = unique_accession("ENA")
    tube_id = unique_matrix_tube_id()
    async with ctx["pool"].acquire() as conn:
        bs_idx = await insert_biosample(
            conn,
            owner_idx=ctx["biosample_owner_idx"],
            created_by_idx=ctx["principal_idx"],
            metadata_checklist_idx=ctx["checklist_idx"],
            biosample_accession=bs_acc,
            ena_sample_accession=ena_acc,
            matrix_tube_id=tube_id,
        )
    ctx["created"]["biosample"].append(bs_idx)

    row = await fetch_biosample(ctx["pool"], bs_idx)
    assert row is not None
    actual = dict(row)

    expected = {
        "idx": bs_idx,
        "owner_idx": ctx["biosample_owner_idx"],
        "metadata_checklist_idx": ctx["checklist_idx"],
        "biosample_accession": bs_acc,
        "ena_sample_accession": ena_acc,
        "matrix_tube_id": tube_id,
        "last_submission_at": None,
        "submission_error": None,
        "last_metadata_change_at": None,
        "created_by_idx": ctx["principal_idx"],
        # Auto-generated by the DB; copy the actual values into expected so
        # the equality confirms column presence without pinning timestamps.
        "created_at": actual["created_at"],
        "updated_at": actual["updated_at"],
        "retired": False,
        "retired_by_idx": None,
        "retired_at": None,
        "retire_reason": None,
    }
    assert actual == expected


async def test_fetch_biosample_returns_none_when_missing(ctx):
    # No biosample at idx=-1; the read returns None rather than raising.
    row = await fetch_biosample(ctx["pool"], -1)
    assert row is None


# ---------------------------------------------------------------------------
# fetch_caller_has_biosample_access
# ---------------------------------------------------------------------------


async def _seed_study_access_row(ctx, *, study_idx, principal_idx, access_tier):
    """Insert a qiita.study_access row and track it for fixture cleanup."""
    sa_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.study_access"
        "  (study_idx, principal_idx, access_tier)"
        " VALUES ($1, $2, $3) RETURNING idx",
        study_idx,
        principal_idx,
        access_tier,
    )
    ctx["created"]["study_access"].append(sa_idx)
    return sa_idx


async def test_fetch_caller_has_biosample_access_owner(ctx):
    # The biosample's owner has a path regardless of any study-side access.
    # No biosample_to_study link is needed and no study_access row exists.
    bs_idx = await _create_biosample_with_link(ctx)

    has_access = await fetch_caller_has_biosample_access(
        ctx["pool"],
        principal_idx=ctx["principal_idx"],
        biosample_idx=bs_idx,
    )
    assert has_access is True


async def test_fetch_caller_has_biosample_access_via_study_access_row(ctx):
    # The peer principal is NOT the owner; access must come through a
    # qiita.study_access row on the (active) biosample_to_study link.
    bs_idx = await _create_biosample_with_link(ctx)
    await _seed_study_access_row(
        ctx,
        study_idx=ctx["study_idx"],
        principal_idx=ctx["biosample_owner_idx"],
        access_tier="viewer",
    )

    has_access = await fetch_caller_has_biosample_access(
        ctx["pool"],
        principal_idx=ctx["biosample_owner_idx"],
        biosample_idx=bs_idx,
    )
    assert has_access is True


async def test_fetch_caller_has_biosample_access_no_access(ctx):
    # The peer principal is neither the biosample's owner nor on a
    # study_access row of the linked study; the predicate returns False.
    bs_idx = await _create_biosample_with_link(ctx)

    has_access = await fetch_caller_has_biosample_access(
        ctx["pool"],
        principal_idx=ctx["biosample_owner_idx"],
        biosample_idx=bs_idx,
    )
    assert has_access is False


async def test_fetch_caller_has_biosample_access_excludes_retired_link(ctx):
    # The peer principal has a study_access row, but the only link tying
    # the biosample to that study has been retired. The predicate must
    # treat the retired link as no path at all.
    bs_idx = await _create_biosample_with_link(ctx)
    await _seed_study_access_row(
        ctx,
        study_idx=ctx["study_idx"],
        principal_idx=ctx["biosample_owner_idx"],
        access_tier="viewer",
    )
    await retire_biosample_to_study_link(
        ctx["pool"],
        biosample_idx=bs_idx,
        study_idx=ctx["study_idx"],
        retired_by_idx=ctx["principal_idx"],
    )

    has_access = await fetch_caller_has_biosample_access(
        ctx["pool"],
        principal_idx=ctx["biosample_owner_idx"],
        biosample_idx=bs_idx,
    )
    assert has_access is False


# ---------------------------------------------------------------------------
# update_biosample
# ---------------------------------------------------------------------------


async def _seed_full_biosample(ctx, *, owner_idx, bs_acc, ena_acc, matrix_tube_id=None):
    """Seed a biosample with every caller-settable column populated.

    `matrix_tube_id` is optional so existing call sites that pre-date the
    column keep working with the column NULL; tests targeting the column
    pass an explicit value.

    Returns the new biosample_idx; caller is responsible for the
    cleanup tracking via ctx['created']['biosample'].
    """
    async with ctx["pool"].acquire() as conn:
        bs_idx = await insert_biosample(
            conn,
            owner_idx=owner_idx,
            created_by_idx=ctx["principal_idx"],
            metadata_checklist_idx=ctx["checklist_idx"],
            biosample_accession=bs_acc,
            ena_sample_accession=ena_acc,
            matrix_tube_id=matrix_tube_id,
        )
    ctx["created"]["biosample"].append(bs_idx)
    return bs_idx


async def test_update_biosample_writes_single_field(ctx):
    # PATCH biosample_accession only; verify the returned row reflects the
    # new accession and every other column matches the seed values. The
    # full-row equality covers both the RETURNING column shape and the
    # non-touched-column invariant in one assertion.
    seed_acc = unique_accession("BS-old")
    seed_ena = unique_accession("ENA-keep")
    new_acc = unique_accession("BS-new")
    bs_idx = await _seed_full_biosample(
        ctx, owner_idx=ctx["biosample_owner_idx"], bs_acc=seed_acc, ena_acc=seed_ena
    )

    async with ctx["pool"].acquire() as conn:
        row = await update_biosample(conn, bs_idx, fields={"biosample_accession": new_acc})
    actual = dict(row)

    expected = {
        "idx": bs_idx,
        "owner_idx": ctx["biosample_owner_idx"],
        "metadata_checklist_idx": ctx["checklist_idx"],
        "biosample_accession": new_acc,
        "ena_sample_accession": seed_ena,
        "matrix_tube_id": None,
        "last_submission_at": None,
        "submission_error": None,
        "last_metadata_change_at": None,
        "created_by_idx": ctx["principal_idx"],
        # Auto-generated by the DB; copy the actual values into expected so
        # the equality confirms column presence without pinning timestamps.
        "created_at": actual["created_at"],
        "updated_at": actual["updated_at"],
        "retired": False,
        "retired_by_idx": None,
        "retired_at": None,
        "retire_reason": None,
    }
    assert actual == expected


async def test_update_biosample_writes_all_editable_fields(ctx):
    # PATCH every editable column at once (including switching ownership
    # to the other user-kind principal); the returned row must reflect
    # all new values.
    seed_acc = unique_accession("BS-old")
    seed_ena = unique_accession("ENA-old")
    seed_tube = unique_matrix_tube_id()
    new_acc = unique_accession("BS-new")
    new_ena = unique_accession("ENA-new")
    new_tube = unique_matrix_tube_id()
    new_submission_at = datetime.now(UTC).replace(microsecond=0)
    bs_idx = await _seed_full_biosample(
        ctx,
        owner_idx=ctx["biosample_owner_idx"],
        bs_acc=seed_acc,
        ena_acc=seed_ena,
        matrix_tube_id=seed_tube,
    )

    async with ctx["pool"].acquire() as conn:
        row = await update_biosample(
            conn,
            bs_idx,
            fields={
                "metadata_checklist_idx": None,
                "owner_idx": ctx["principal_idx"],
                "biosample_accession": new_acc,
                "ena_sample_accession": new_ena,
                "matrix_tube_id": new_tube,
                "last_submission_at": new_submission_at,
                "submission_error": "NCBI rejected: bad collection_date",
            },
        )
    actual = dict(row)

    expected = {
        "idx": bs_idx,
        "owner_idx": ctx["principal_idx"],
        "metadata_checklist_idx": None,
        "biosample_accession": new_acc,
        "ena_sample_accession": new_ena,
        "matrix_tube_id": new_tube,
        "last_submission_at": new_submission_at,
        "submission_error": "NCBI rejected: bad collection_date",
        "last_metadata_change_at": None,
        "created_by_idx": ctx["principal_idx"],
        "created_at": actual["created_at"],
        "updated_at": actual["updated_at"],
        "retired": False,
        "retired_by_idx": None,
        "retired_at": None,
        "retire_reason": None,
    }
    assert actual == expected


async def test_update_biosample_explicit_null_clears_nullable_column(ctx):
    # An explicit None in the fields dict must reach the column as a
    # SQL NULL rather than being treated as "absent" and skipped.
    seed_acc = unique_accession("BS")
    bs_idx = await _seed_full_biosample(
        ctx,
        owner_idx=ctx["biosample_owner_idx"],
        bs_acc=seed_acc,
        ena_acc=unique_accession("ENA"),
    )

    async with ctx["pool"].acquire() as conn:
        row = await update_biosample(conn, bs_idx, fields={"metadata_checklist_idx": None})
    assert row["metadata_checklist_idx"] is None
    # The non-targeted accession must survive untouched so we know the
    # NULL write didn't widen into a blanket clear.
    assert row["biosample_accession"] == seed_acc


async def test_update_biosample_empty_fields_raises_value_error(ctx):
    # No SQL should be issued; the function fails at its boundary.
    bs_idx = await _seed_full_biosample(
        ctx,
        owner_idx=ctx["biosample_owner_idx"],
        bs_acc=unique_accession("BS"),
        ena_acc=unique_accession("ENA"),
    )
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(ValueError, match="at least one"):
            await update_biosample(conn, bs_idx, fields={})


async def test_update_biosample_unknown_key_raises_value_error(ctx):
    # `retired` is not in the patch allowlist (managed by retirement
    # endpoints); the function rejects it before reaching SQL.
    bs_idx = await _seed_full_biosample(
        ctx,
        owner_idx=ctx["biosample_owner_idx"],
        bs_acc=unique_accession("BS"),
        ena_acc=unique_accession("ENA"),
    )
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(ValueError, match="retired"):
            await update_biosample(conn, bs_idx, fields={"retired": True})


async def test_update_biosample_missing_row_returns_none(ctx):
    # An idx past the highest existing biosample matches zero rows;
    # UPDATE ... RETURNING then yields no row and fetchrow returns None.
    # Pins the contract that callers (the PATCH route) must surface this
    # as 404 rather than dereferencing a None Record. The route's
    # static missing-row case is caught earlier by the preflight; this
    # function only sees None when the row is deleted between preflight
    # and UPDATE (READ COMMITTED snapshots are per-statement).
    missing_idx = (
        await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.biosample")
    ) + 100_000

    async with ctx["pool"].acquire() as conn:
        result = await update_biosample(
            conn, missing_idx, fields={"submission_error": "should not land"}
        )
    assert result is None


async def test_update_biosample_bad_metadata_checklist_idx_raises_fk_error(ctx):
    # FK violation on metadata_checklist_idx surfaces as
    # asyncpg.ForeignKeyViolationError; the route maps this to 422.
    bs_idx = await _seed_full_biosample(
        ctx,
        owner_idx=ctx["biosample_owner_idx"],
        bs_acc=unique_accession("BS"),
        ena_acc=unique_accession("ENA"),
    )
    bad_checklist = (
        await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.metadata_checklist")
    ) + 100_000

    async with ctx["pool"].acquire() as conn:
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await update_biosample(conn, bs_idx, fields={"metadata_checklist_idx": bad_checklist})


async def test_update_biosample_bad_owner_idx_raises_role_typed_error(ctx):
    # The role-typed FK trigger on biosample.owner_idx fires before the
    # underlying FK constraint, so an owner_idx that does not name a
    # user-kind principal surfaces as asyncpg.RaiseError rather than
    # asyncpg.ForeignKeyViolationError. The route must map both surfaces
    # to 422; this test pins the trigger arm.
    bs_idx = await _seed_full_biosample(
        ctx,
        owner_idx=ctx["biosample_owner_idx"],
        bs_acc=unique_accession("BS"),
        ena_acc=unique_accession("ENA"),
    )
    bad_owner = (
        await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.principal")
    ) + 100_000

    async with ctx["pool"].acquire() as conn:
        with pytest.raises(asyncpg.RaiseError, match="user-kind principal"):
            await update_biosample(conn, bs_idx, fields={"owner_idx": bad_owner})


@pytest.mark.parametrize(
    "column,make_value",
    [
        ("biosample_accession", lambda: unique_accession("BS-A")),
        ("matrix_tube_id", unique_matrix_tube_id),
    ],
)
async def test_update_biosample_duplicate_unique_column_raises_unique_error(
    ctx, column, make_value
):
    """Tests the case where a PATCH to a unique-constrained column would
    collide with another row's value: asyncpg.UniqueViolationError fires
    regardless of which unique-constrained column triggers it. The route
    layer maps both to 409.
    """
    conflicting_value = make_value()
    # Seed A with the conflicting value populated in the target column.
    a_kwargs = {
        "owner_idx": ctx["biosample_owner_idx"],
        "bs_acc": unique_accession("BS-A"),
        "ena_acc": unique_accession("ENA-A"),
    }
    if column == "matrix_tube_id":
        a_kwargs["matrix_tube_id"] = conflicting_value
    else:
        a_kwargs["bs_acc"] = conflicting_value
    bs_a = await _seed_full_biosample(ctx, **a_kwargs)
    # Seed B without the conflicting value; the PATCH below sets it.
    bs_b = await _seed_full_biosample(
        ctx,
        owner_idx=ctx["biosample_owner_idx"],
        bs_acc=unique_accession("BS-B"),
        ena_acc=unique_accession("ENA-B"),
    )
    # A is referenced only via the seed value; the lookup keeps the idx
    # in scope so the cleanup sweep removes both biosamples in order.
    assert bs_a != bs_b

    async with ctx["pool"].acquire() as conn:
        with pytest.raises(asyncpg.UniqueViolationError):
            await update_biosample(conn, bs_b, fields={column: conflicting_value})


async def test_update_biosample_bad_matrix_tube_id_raises_check_error(ctx):
    """Tests the case where a PATCH sets matrix_tube_id to a non-digit
    value: the column-level CHECK fires and
    asyncpg.CheckViolationError propagates.
    """
    bs_idx = await _seed_full_biosample(
        ctx,
        owner_idx=ctx["biosample_owner_idx"],
        bs_acc=unique_accession("BS"),
        ena_acc=unique_accession("ENA"),
    )
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            await update_biosample(conn, bs_idx, fields={"matrix_tube_id": "ABC"})


async def test_update_biosample_overlong_matrix_tube_id_raises_string_truncation(ctx):
    """Tests the case where a PATCH sets matrix_tube_id to a value
    longer than the VARCHAR(50) cap: the column type fires and
    asyncpg.StringDataRightTruncationError propagates.
    """
    bs_idx = await _seed_full_biosample(
        ctx,
        owner_idx=ctx["biosample_owner_idx"],
        bs_acc=unique_accession("BS"),
        ena_acc=unique_accession("ENA"),
    )
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(asyncpg.StringDataRightTruncationError):
            await update_biosample(conn, bs_idx, fields={"matrix_tube_id": "1" * 51})


async def test_update_biosample_advances_updated_at(ctx):
    # The schema's biosample_set_updated_at trigger must run on every
    # UPDATE; the returned row's updated_at must therefore strictly exceed
    # the pre-update value.
    bs_idx = await _seed_full_biosample(
        ctx,
        owner_idx=ctx["biosample_owner_idx"],
        bs_acc=unique_accession("BS"),
        ena_acc=unique_accession("ENA"),
    )
    initial_updated_at = await ctx["pool"].fetchval(
        "SELECT updated_at FROM qiita.biosample WHERE idx = $1", bs_idx
    )

    async with ctx["pool"].acquire() as conn:
        row = await update_biosample(conn, bs_idx, fields={"submission_error": "transient retry"})
    assert row["updated_at"] > initial_updated_at


# ---------------------------------------------------------------------------
# fetch_biosample_idxs_by_natural_key
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key,make_value",
    [
        ("biosample_accession", lambda: unique_accession("LK")),
        ("matrix_tube_id", unique_matrix_tube_id),
    ],
)
async def test_fetch_biosample_idxs_by_natural_key_returns_resolved(ctx, key, make_value):
    """Tests the case where the named natural-key column carries values
    that resolve to non-retired biosamples: the function returns a
    `{value: biosample_idx}` map for the hits and omits the misses. The
    parameterization exercises both natural-key surfaces (accession and
    matrix_tube_id) through one definition.
    """
    hit_value = make_value()
    miss_value = make_value()
    # Seed one biosample carrying hit_value in the target column; the
    # paired accession/ena_accession use a separate generator so the
    # uniqueness constraints on the other unique columns never collide.
    kwargs = {
        "owner_idx": ctx["biosample_owner_idx"],
        "bs_acc": unique_accession("LK-OTHER"),
        "ena_acc": unique_accession("LK-ENA"),
    }
    if key == "matrix_tube_id":
        kwargs["matrix_tube_id"] = hit_value
    else:
        kwargs["bs_acc"] = hit_value
    bs_idx = await _seed_full_biosample(ctx, **kwargs)

    result = await fetch_biosample_idxs_by_natural_key(
        ctx["pool"], key=key, values=[hit_value, miss_value]
    )
    assert result == {hit_value: bs_idx}


async def test_fetch_biosample_idxs_by_natural_key_empty_values_short_circuits(ctx):
    """Tests the case where `values` is empty: the function returns an
    empty dict without executing any SQL (the no-op early-return path).
    """
    result = await fetch_biosample_idxs_by_natural_key(
        ctx["pool"], key="biosample_accession", values=[]
    )
    assert result == {}


async def test_fetch_biosample_idxs_by_natural_key_invalid_key_raises(ctx):
    """Tests the case where `key` is a string outside BiosampleLookupKey:
    the function raises ValueError before issuing any SQL, blocking the
    column-name interpolation path.
    """
    with pytest.raises(ValueError, match="invalid biosample lookup key"):
        await fetch_biosample_idxs_by_natural_key(
            ctx["pool"], key="idx; DROP TABLE qiita.biosample; --", values=["x"]
        )


# ===========================================================================
# Role-typed FK triggers — qiita.biosample.owner_idx
# ===========================================================================
#
# Tests below use Pattern 1 (transaction-rollback per test): all seed and
# assertions happen inside a single transaction that is rolled back at the
# end, with no shared fixture and no FK-reverse cleanup. The rest of this
# file uses Pattern 2 (committed `ctx` fixture + FK-reverse cleanup).
# Pattern 1 fits trigger tests because triggers fire per-statement and the
# test does not need to commit; Pattern 2 is needed elsewhere — notably
# `test_import_biosample_from_owner_biosample_id_rejects_non_transactional_connection`,
# which calls the composer outside a transaction (impossible in Pattern 1).
# Helpers below are conn-style and prefixed `_create_*` / `_insert_*`,
# distinct from the pool-style `_seed_*` helpers used by Pattern-2 tests.


def _trigger_test_suffix(label: str) -> str:
    return f"{label}-{secrets.token_hex(4)}"


async def _create_user(conn) -> int:
    """Pattern-1 helper: create a user-kind principal via one connection."""
    name = _trigger_test_suffix("user")
    pidx = await conn.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, $3) RETURNING idx",
        name,
        SystemRole.USER,
        SYSTEM_PRINCIPAL_IDX,
    )
    await conn.execute(
        "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
        pidx,
        f"{name}@example.com",
    )
    return pidx


async def _create_service_account(conn) -> int:
    name = _trigger_test_suffix("svc")
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


async def _create_bare_principal(conn) -> int:
    """Pattern-1 helper: principal with no subtype row."""
    return await conn.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, $3) RETURNING idx",
        _trigger_test_suffix("bare"),
        SystemRole.USER,
        SYSTEM_PRINCIPAL_IDX,
    )


async def _insert_biosample_row(
    conn,
    *,
    owner_idx: int,
    created_by_idx: int = SYSTEM_PRINCIPAL_IDX,
) -> int:
    """Pattern-1 helper: raw INSERT into qiita.biosample bypassing the
    repository function. The local name avoids collision with the
    imported `insert_biosample` repository function used elsewhere."""
    return await conn.fetchval(
        "INSERT INTO qiita.biosample (owner_idx, created_by_idx) VALUES ($1, $2) RETURNING idx",
        owner_idx,
        created_by_idx,
    )


async def test_biosample_owner_must_be_user_accepts_user(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            bs_idx = await _insert_biosample_row(conn, owner_idx=owner)
            assert bs_idx is not None
        finally:
            await tr.rollback()


async def test_biosample_owner_must_be_user_rejects_service_account(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            svc = await _create_service_account(conn)
            with pytest.raises(asyncpg.RaiseError, match="must reference a user-kind principal"):
                await _insert_biosample_row(conn, owner_idx=svc)
        finally:
            await tr.rollback()


async def test_biosample_owner_must_be_user_rejects_bare_principal(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            bare = await _create_bare_principal(conn)
            with pytest.raises(asyncpg.RaiseError, match="must reference a user-kind principal"):
                await _insert_biosample_row(conn, owner_idx=bare)
        finally:
            await tr.rollback()


async def test_biosample_update_owner_to_service_account_raises(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            svc = await _create_service_account(conn)
            bs_idx = await _insert_biosample_row(conn, owner_idx=owner)
            with pytest.raises(asyncpg.RaiseError, match="must reference a user-kind principal"):
                await conn.execute(
                    "UPDATE qiita.biosample SET owner_idx = $1 WHERE idx = $2",
                    svc,
                    bs_idx,
                )
        finally:
            await tr.rollback()


async def test_user_delete_blocked_when_referenced_as_biosample_owner(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            await _insert_biosample_row(conn, owner_idx=owner)
            with pytest.raises(asyncpg.RaiseError, match="cannot delete qiita.user"):
                await conn.execute("DELETE FROM qiita.user WHERE principal_idx = $1", owner)
        finally:
            await tr.rollback()


async def test_user_delete_succeeds_after_biosample_gone(postgres_pool):
    # Once the biosample referencing the user has been removed, the trigger
    # has nothing to block on and the user delete proceeds.
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            bs_idx = await _insert_biosample_row(conn, owner_idx=owner)
            await conn.execute("DELETE FROM qiita.biosample WHERE idx = $1", bs_idx)
            await conn.execute("DELETE FROM qiita.user WHERE principal_idx = $1", owner)
            still_there = await conn.fetchval(
                "SELECT 1 FROM qiita.user WHERE principal_idx = $1", owner
            )
            assert still_there is None
        finally:
            await tr.rollback()


async def test_biosample_created_by_idx_accepts_service_account(postgres_pool):
    # created_by_idx is intentionally NOT registered in the role-typed FK
    # trigger system: bulk imports and admin tools legitimately set it to
    # a service account or the system principal. Asserts the carve-out so
    # an accidental future registration would fail the suite.
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            svc = await _create_service_account(conn)
            bs_idx = await _insert_biosample_row(conn, owner_idx=owner, created_by_idx=svc)
            assert bs_idx is not None
        finally:
            await tr.rollback()
