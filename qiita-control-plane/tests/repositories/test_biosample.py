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
from datetime import date
from decimal import Decimal

import asyncpg
import pytest
from qiita_common.auth_constants import SystemRole
from qiita_common.models import FieldDataType

from qiita_control_plane.repositories.biosample import (
    BiosampleImportResult,
    fetch_biosample_idxs_for_study,
    import_biosample_from_owner_biosample_id,
    insert_biosample,
    insert_biosample_to_study,
)
from qiita_control_plane.repositories.biosample_metadata import (
    BiosampleMetadataParseError,
    BiosampleMetadataUnknownFieldsError,
    BiosampleOwnerIdFieldCollisionError,
)
from qiita_control_plane.testing.db_seeds import (
    retire_biosample,
    retire_biosample_to_study_link,
)

from .conftest import (
    _create_biosample_with_link,
    _seed_biosample_global_field,
    _seed_study,
    _unique_accession,
    _unique_field_name,
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
        " biosample_accession, ena_sample_accession, retired"
        " FROM qiita.biosample WHERE idx = $1",
        idx,
    )
    expected = {
        "owner_idx": ctx["biosample_owner_idx"],
        "created_by_idx": ctx["principal_idx"],
        "metadata_checklist_idx": None,
        "biosample_accession": None,
        "ena_sample_accession": None,
        "retired": False,
    }
    assert dict(row) == expected


async def test_insert_biosample_full_columns(ctx):
    # Unique-per-test accessions avoid the UNIQUE constraint on bare values.
    bs_acc = _unique_accession("BS")
    ena_acc = _unique_accession("ENA")

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
        )
    ctx["created"]["biosample"].append(idx)

    # Confirm the populated columns round-trip onto the inserted row.
    row = await ctx["pool"].fetchrow(
        "SELECT owner_idx, created_by_idx, metadata_checklist_idx,"
        " biosample_accession, ena_sample_accession"
        " FROM qiita.biosample WHERE idx = $1",
        idx,
    )
    expected = {
        "owner_idx": ctx["biosample_owner_idx"],
        "created_by_idx": ctx["principal_idx"],
        "metadata_checklist_idx": ctx["checklist_idx"],
        "biosample_accession": bs_acc,
        "ena_sample_accession": ena_acc,
    }
    assert dict(row) == expected


# ---------------------------------------------------------------------------
# insert_biosample_to_study
# ---------------------------------------------------------------------------


async def test_insert_biosample_to_study_links_biosample(ctx):
    # Seed a biosample directly so the test owns a known idx.
    async with ctx["pool"].acquire() as conn:
        bs_idx = await insert_biosample(
            conn,
            owner_idx=ctx["principal_idx"],
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample"].append(bs_idx)

    # Link the biosample to the seeded study.
    async with ctx["pool"].acquire() as conn:
        await insert_biosample_to_study(
            conn,
            biosample_idx=bs_idx,
            study_idx=ctx["study_idx"],
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample_to_study"].append((bs_idx, ctx["study_idx"]))

    # Verify the link row matches the expected non-retired shape.
    row = await ctx["pool"].fetchrow(
        "SELECT biosample_idx, study_idx, created_by_idx, retired"
        " FROM qiita.biosample_to_study WHERE biosample_idx = $1 AND study_idx = $2",
        bs_idx,
        ctx["study_idx"],
    )
    expected = {
        "biosample_idx": bs_idx,
        "study_idx": ctx["study_idx"],
        "created_by_idx": ctx["principal_idx"],
        "retired": False,
    }
    assert dict(row) == expected


async def test_insert_biosample_to_study_rejects_duplicate(ctx):
    async with ctx["pool"].acquire() as conn:
        bs_idx = await insert_biosample(
            conn,
            owner_idx=ctx["principal_idx"],
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample"].append(bs_idx)

    # First link succeeds.
    async with ctx["pool"].acquire() as conn:
        await insert_biosample_to_study(
            conn,
            biosample_idx=bs_idx,
            study_idx=ctx["study_idx"],
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample_to_study"].append((bs_idx, ctx["study_idx"]))

    # Second insert of the same (biosample, study) pair must raise on the PK.
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(asyncpg.UniqueViolationError):
            await insert_biosample_to_study(
                conn,
                biosample_idx=bs_idx,
                study_idx=ctx["study_idx"],
                created_by_idx=ctx["principal_idx"],
            )


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
    field_name = _unique_field_name()

    # Compose the import inside a transaction (route layer's responsibility
    # in production; the test plays that role here). owner_idx and
    # caller_idx are distinct principals so the test exercises the
    # admin-imports-on-behalf-of-owner case.
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            result = await import_biosample_from_owner_biosample_id(
                conn,
                study_idx=ctx["study_idx"],
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
        biosample_study_field_idx=field_idx,
        biosample_study_field_created=True,
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
    field_name = _unique_field_name()
    bs_acc = _unique_accession("BS")

    # Pass through metadata_checklist_idx and biosample_accession on the composer.
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            result = await import_biosample_from_owner_biosample_id(
                conn,
                study_idx=ctx["study_idx"],
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
    field_name = _unique_field_name()

    # Two imports against the same study with the same owner-biosample-id field name —
    # the second must reuse the field row rather than creating a new one.
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            result1 = await import_biosample_from_owner_biosample_id(
                conn,
                study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=field_name,
                owner_biosample_id_value="A",
                caller_idx=ctx["principal_idx"],
                metadata={},
            )
            result2 = await import_biosample_from_owner_biosample_id(
                conn,
                study_idx=ctx["study_idx"],
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
    assert result1.biosample_study_field_created is True
    assert result2.biosample_study_field_created is False
    assert result1.biosample_study_field_idx == result2.biosample_study_field_idx

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
    name_a = _unique_field_name("owner_biosample_id_a")
    name_b = _unique_field_name("owner_biosample_id_b")

    # Two imports against the same study with different field names produce
    # two separate field rows.
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            result1 = await import_biosample_from_owner_biosample_id(
                conn,
                study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=name_a,
                owner_biosample_id_value="A",
                caller_idx=ctx["principal_idx"],
                metadata={},
            )
            result2 = await import_biosample_from_owner_biosample_id(
                conn,
                study_idx=ctx["study_idx"],
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
    assert result1.biosample_study_field_created is True
    assert result2.biosample_study_field_created is True

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
    bs_acc = _unique_accession("BS-rollback")
    bad_study_idx = -1  # nonexistent; the link insert FK will reject it

    # The composer succeeds at step a (biosample insert) and fails at step b
    # (link insert). Wrapping in a transaction must roll back the biosample.
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            async with conn.transaction():
                await import_biosample_from_owner_biosample_id(
                    conn,
                    study_idx=bad_study_idx,
                    owner_idx=ctx["biosample_owner_idx"],
                    owner_biosample_id_field_name=_unique_field_name(),
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
    field_name = _unique_field_name()

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
                study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=field_name,
                owner_biosample_id_value="STUDY1-1",
                caller_idx=ctx["principal_idx"],
                metadata={},
            )
            result2 = await import_biosample_from_owner_biosample_id(
                conn,
                study_idx=second_study_idx,
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
    assert result1.biosample_study_field_created is True
    assert result2.biosample_study_field_created is True

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
                study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=_unique_field_name(),
                owner_biosample_id_value="GUARD-CHECK",
                caller_idx=ctx["principal_idx"],
                metadata={},
            )


async def _track_global_metadata_outputs(ctx, bs_idx, study_idx, global_idxs):
    """Track globally-linked study fields (by global concept idx) and every
    non-owner-id metadata row written for this biosample. Use after
    `_track_composer_outputs` in tests that exercised the metadata dict
    path so the FK-reverse cleanup picks the new rows up.
    """
    # Pick up every globally-linked study field row at this study tied to
    # one of the supplied global concepts.
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
    date_global = await _seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"date_{suffix}",
        display_name=f"Collection Date {suffix}",
        data_type=FieldDataType.DATE,
        created_by_idx=1,
    )
    num_global = await _seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"num_{suffix}",
        display_name=f"Latitude {suffix}",
        data_type=FieldDataType.NUMERIC,
        created_by_idx=1,
    )
    ctx["created"]["biosample_global_field"].extend([date_global, num_global])

    field_name = _unique_field_name()
    metadata_payload = {
        f"Collection Date {suffix}": "2026-05-06",
        f"Latitude {suffix}": "32.7",
    }

    # Compose the import with metadata covering both global concepts.
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            result = await import_biosample_from_owner_biosample_id(
                conn,
                study_idx=ctx["study_idx"],
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


async def test_import_biosample_from_owner_biosample_id_with_empty_metadata(ctx):
    field_name = _unique_field_name()

    # Empty metadata dict — composer must skip the global-metadata block
    # entirely and write only the owner-biosample-id metadata row.
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            result = await import_biosample_from_owner_biosample_id(
                conn,
                study_idx=ctx["study_idx"],
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
    field_name = _unique_field_name()
    suffix = secrets.token_hex(4)
    unknown_a = f"Unknown A {suffix}"
    unknown_b = f"Unknown B {suffix}"

    # Two metadata keys that have no matching biosample_global_field row.
    # The composer must collect both into one BiosampleMetadataUnknownFieldsError
    # before any writes.
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(BiosampleMetadataUnknownFieldsError) as excinfo:
            async with conn.transaction():
                await import_biosample_from_owner_biosample_id(
                    conn,
                    study_idx=ctx["study_idx"],
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
    global_idx = await _seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"item_{suffix}",
        display_name=numeric_field_name,
        data_type=FieldDataType.NUMERIC,
        created_by_idx=1,
    )
    ctx["created"]["biosample_global_field"].append(global_idx)

    field_name = _unique_field_name()

    # Numeric global field, garbage value — composer raises pre-write.
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(BiosampleMetadataParseError) as excinfo:
            async with conn.transaction():
                await import_biosample_from_owner_biosample_id(
                    conn,
                    study_idx=ctx["study_idx"],
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
    shared_name = _unique_field_name("collide")

    async with ctx["pool"].acquire() as conn:
        with pytest.raises(BiosampleOwnerIdFieldCollisionError) as excinfo:
            async with conn.transaction():
                await import_biosample_from_owner_biosample_id(
                    conn,
                    study_idx=ctx["study_idx"],
                    owner_idx=ctx["biosample_owner_idx"],
                    owner_biosample_id_field_name=shared_name,
                    owner_biosample_id_value="OWNER-COLL",
                    caller_idx=ctx["principal_idx"],
                    metadata={shared_name: "x"},
                )
    assert excinfo.value.display_name == shared_name


# ---------------------------------------------------------------------------
# fetch_biosample_idxs_for_study
# ---------------------------------------------------------------------------


async def test_fetch_biosample_idxs_for_study_returns_empty_for_no_links(ctx):
    # The fixture's study has no biosample links yet; the read returns [].
    result = await fetch_biosample_idxs_for_study(
        ctx["pool"], study_idx=ctx["study_idx"], limit=10
    )
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
            await insert_biosample_to_study(
                conn,
                biosample_idx=bs_idx,
                study_idx=ctx["study_idx"],
                created_by_idx=ctx["principal_idx"],
            )
        ctx["created"]["biosample"].append(bs_idx)
        ctx["created"]["biosample_to_study"].append((bs_idx, ctx["study_idx"]))
        bs_idxs.append(bs_idx)

    result = await fetch_biosample_idxs_for_study(
        ctx["pool"], study_idx=ctx["study_idx"], limit=10
    )
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

    result = await fetch_biosample_idxs_for_study(
        ctx["pool"], study_idx=ctx["study_idx"], limit=10
    )
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

    result = await fetch_biosample_idxs_for_study(
        ctx["pool"], study_idx=ctx["study_idx"], limit=10
    )
    assert result == [active_idx]


async def test_fetch_biosample_idxs_for_study_respects_limit(ctx):
    # Insert three links; ask for two. The DB returns exactly the two
    # newest under the documented sort order.
    bs_idxs = []
    for _ in range(3):
        bs_idxs.append(await _create_biosample_with_link(ctx))

    result = await fetch_biosample_idxs_for_study(
        ctx["pool"], study_idx=ctx["study_idx"], limit=2
    )
    # The two newest are the last two appended; reversed to put newest first.
    assert result == [bs_idxs[2], bs_idxs[1]]


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


_SYSTEM_PRINCIPAL_IDX = 1


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
        _SYSTEM_PRINCIPAL_IDX,
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
        _SYSTEM_PRINCIPAL_IDX,
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
        _SYSTEM_PRINCIPAL_IDX,
    )


async def _insert_biosample_row(
    conn,
    *,
    owner_idx: int,
    created_by_idx: int = _SYSTEM_PRINCIPAL_IDX,
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
