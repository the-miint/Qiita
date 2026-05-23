"""Integration tests for biosample-metadata repository functions.

Mirrors the qiita.biosample_metadata / biosample_study_field /
biosample_global_field surface in the schema. The composer test suite
lives in test_biosample.py because the composer itself is in
repositories.biosample; cross-suite fixtures and seed/cleanup helpers
live in tests/repositories/conftest.py.
"""

import secrets

import asyncpg
import pytest
from qiita_common.models import FieldDataType

from qiita_control_plane.repositories._sample_helpers import (
    _get_or_create_local_study_field,
    _insert_typed_metadata,
)
from qiita_control_plane.repositories.biosample_metadata import (
    BIOSAMPLE_METADATA_SPEC,
    insert_owner_biosample_id_metadata,
)

from .conftest import (
    _create_biosample_with_link,
    _create_local_field,
    _unique_field_name,
)

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# biosample_metadata_check_data_type_and_set_global_field_idx trigger
# ---------------------------------------------------------------------------


async def test_biosample_metadata_rejects_value_text_when_data_type_numeric(ctx):
    bs_idx = await _create_biosample_with_link(ctx)

    # Create a purely-local numeric-typed field. Mismatching the field's
    # data_type with the populated value_* column must be rejected by the
    # check_data_type half of the trigger.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _, _ = await _get_or_create_local_study_field(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            study_idx=ctx["study_idx"],
            display_name=_unique_field_name("num"),
            created_by_idx=ctx["principal_idx"],
            data_type=FieldDataType.NUMERIC,
            required=True,
        )
    ctx["created"]["biosample_study_field"].append(field_idx)

    # Numeric field, value_text populated — trigger raises.
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(asyncpg.RaiseError):
            await conn.execute(
                "INSERT INTO qiita.biosample_metadata"
                " (biosample_idx, biosample_study_field_idx, value_text, created_by_idx)"
                " VALUES ($1, $2, $3, $4)",
                bs_idx,
                field_idx,
                "should-not-fit",
                ctx["principal_idx"],
            )


async def test_biosample_metadata_accepts_value_missing_reason_for_any_data_type(ctx):
    bs_idx = await _create_biosample_with_link(ctx)

    # Create a numeric field, but write a missing-reason row instead of a
    # value_numeric row. Missing-reason rows are exempt from the data_type match.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _, _ = await _get_or_create_local_study_field(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            study_idx=ctx["study_idx"],
            display_name=_unique_field_name("num"),
            created_by_idx=ctx["principal_idx"],
            data_type=FieldDataType.NUMERIC,
            required=True,
        )
    ctx["created"]["biosample_study_field"].append(field_idx)

    # Seed a missing-value reason so the metadata row has something to point at.
    reason_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.missing_value_reason (name) VALUES ($1) RETURNING idx",
        f"reason_{secrets.token_hex(4)}",
    )
    ctx["created"]["missing_value_reason"].append(reason_idx)

    # Insert the missing-reason metadata row; trigger must permit it.
    async with ctx["pool"].acquire() as conn:
        meta_idx = await conn.fetchval(
            "INSERT INTO qiita.biosample_metadata"
            " (biosample_idx, biosample_study_field_idx, value_missing_reason_idx,"
            "  created_by_idx)"
            " VALUES ($1, $2, $3, $4) RETURNING idx",
            bs_idx,
            field_idx,
            reason_idx,
            ctx["principal_idx"],
        )
    ctx["created"]["biosample_metadata"].append(meta_idx)

    # Verify the row landed and points at the reason.
    row = await ctx["pool"].fetchrow(
        "SELECT value_numeric, value_missing_reason_idx"
        " FROM qiita.biosample_metadata WHERE idx = $1",
        meta_idx,
    )
    assert dict(row) == {"value_numeric": None, "value_missing_reason_idx": reason_idx}


async def test_biosample_metadata_resolves_data_type_via_global_link(ctx):
    bs_idx = await _create_biosample_with_link(ctx)

    # Seed a global field with data_type=text, then a study field linked to it.
    # The linked study field's own data_type is NULL per the inheritance CHECK;
    # the trigger must resolve data_type via COALESCE to the global side.
    global_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.biosample_global_field"
        "  (internal_name, display_name, data_type, created_by_idx)"
        " VALUES ($1, $2, 'text', $3) RETURNING idx",
        f"gf_{secrets.token_hex(4)}",
        "Linked Text Field",
        ctx["principal_idx"],
    )
    ctx["created"]["biosample_global_field"].append(global_idx)

    field_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.biosample_study_field"
        "  (study_idx, biosample_global_field_idx, display_name, created_by_idx)"
        " VALUES ($1, $2, $3, $4) RETURNING idx",
        ctx["study_idx"],
        global_idx,
        _unique_field_name("linked"),
        ctx["principal_idx"],
    )
    ctx["created"]["biosample_study_field"].append(field_idx)

    # value_numeric on a text-typed (linked) field — trigger raises.
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(asyncpg.RaiseError):
            await conn.execute(
                "INSERT INTO qiita.biosample_metadata"
                " (biosample_idx, biosample_study_field_idx, value_numeric,"
                "  created_by_idx)"
                " VALUES ($1, $2, $3, $4)",
                bs_idx,
                field_idx,
                42,
                ctx["principal_idx"],
            )

    # value_text on the same field — trigger permits it; round-trip the
    # denormalized global_field_idx the trigger sets in the same step.
    async with ctx["pool"].acquire() as conn:
        meta_idx = await conn.fetchval(
            "INSERT INTO qiita.biosample_metadata"
            " (biosample_idx, biosample_study_field_idx, value_text,"
            "  created_by_idx)"
            " VALUES ($1, $2, $3, $4) RETURNING idx",
            bs_idx,
            field_idx,
            "ok",
            ctx["principal_idx"],
        )
    ctx["created"]["biosample_metadata"].append(meta_idx)

    row = await ctx["pool"].fetchrow(
        "SELECT value_text, global_field_idx FROM qiita.biosample_metadata WHERE idx = $1",
        meta_idx,
    )
    assert dict(row) == {"value_text": "ok", "global_field_idx": global_idx}


async def test_biosample_metadata_rejects_value_text_when_data_type_terminology(ctx):
    bs_idx = await _create_biosample_with_link(ctx)

    # Seed a terminology + one term so the field has a valid terminology_idx
    # and there is a term_idx available for the success-path insert. This is
    # the only data_type whose match arm has an FK to a separate vocabulary,
    # so it is the only one that requires this much auxiliary seeding.
    terminology_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.terminology (name, version, loaded_at)"
        " VALUES ($1, $2, now()) RETURNING idx",
        f"term_{secrets.token_hex(4)}",
        "v1",
    )
    ctx["created"]["terminology"].append(terminology_idx)

    term_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.terminology_term (terminology_idx, term_id, label)"
        " VALUES ($1, $2, $3) RETURNING idx",
        terminology_idx,
        f"TERM:{secrets.token_hex(4)}",
        "label",
    )
    ctx["created"]["terminology_term"].append(term_idx)

    # Create the field via the repository function so the field-table CHECK
    # `(data_type = 'terminology') = (terminology_idx IS NOT NULL)` is exercised
    # alongside the trigger.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _, _ = await _get_or_create_local_study_field(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            study_idx=ctx["study_idx"],
            display_name=_unique_field_name("term"),
            created_by_idx=ctx["principal_idx"],
            data_type=FieldDataType.TERMINOLOGY,
            terminology_idx=terminology_idx,
            required=True,
        )
    ctx["created"]["biosample_study_field"].append(field_idx)

    # value_text on a terminology-typed field — trigger raises.
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(asyncpg.RaiseError):
            await conn.execute(
                "INSERT INTO qiita.biosample_metadata"
                " (biosample_idx, biosample_study_field_idx, value_text,"
                "  created_by_idx)"
                " VALUES ($1, $2, $3, $4)",
                bs_idx,
                field_idx,
                "should-not-fit",
                ctx["principal_idx"],
            )

    # value_terminology_term_idx on the same field — trigger permits it.
    async with ctx["pool"].acquire() as conn:
        meta_idx = await conn.fetchval(
            "INSERT INTO qiita.biosample_metadata"
            " (biosample_idx, biosample_study_field_idx,"
            "  value_terminology_term_idx, created_by_idx)"
            " VALUES ($1, $2, $3, $4) RETURNING idx",
            bs_idx,
            field_idx,
            term_idx,
            ctx["principal_idx"],
        )
    ctx["created"]["biosample_metadata"].append(meta_idx)

    row = await ctx["pool"].fetchrow(
        "SELECT value_terminology_term_idx FROM qiita.biosample_metadata WHERE idx = $1",
        meta_idx,
    )
    assert dict(row) == {"value_terminology_term_idx": term_idx}


# ---------------------------------------------------------------------------
# insert_owner_biosample_id_metadata (biosample-only: prep_sample has no
# owner-id flag, and the partial unique index it cooperates with is
# biosample-specific)
# ---------------------------------------------------------------------------


async def test_insert_owner_biosample_id_metadata_inserts_flagged_row(ctx):
    bs_idx = await _create_biosample_with_link(ctx)
    field_idx = await _create_local_field(ctx)

    # Insert one text-valued metadata row flagged as the owner's identifier.
    async with ctx["pool"].acquire() as conn:
        meta_idx = await insert_owner_biosample_id_metadata(
            conn,
            biosample_idx=bs_idx,
            biosample_study_field_idx=field_idx,
            value_text="OWNER-SAMPLE-42",
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample_metadata"].append(meta_idx)

    # Verify the SQL literal TRUE landed and the value matches the input.
    row = await ctx["pool"].fetchrow(
        "SELECT biosample_idx, biosample_study_field_idx, value_text,"
        " is_owner_biosample_id, created_by_idx"
        " FROM qiita.biosample_metadata WHERE idx = $1",
        meta_idx,
    )
    expected = {
        "biosample_idx": bs_idx,
        "biosample_study_field_idx": field_idx,
        "value_text": "OWNER-SAMPLE-42",
        "is_owner_biosample_id": True,
        "created_by_idx": ctx["principal_idx"],
    }
    assert dict(row) == expected


async def test_insert_owner_biosample_id_metadata_rejects_second_flagged_row(ctx):
    bs_idx = await _create_biosample_with_link(ctx)
    field1_idx = await _create_local_field(ctx, "a")
    field2_idx = await _create_local_field(ctx, "b")

    # First owner-biosample-id row succeeds.
    async with ctx["pool"].acquire() as conn:
        meta_idx = await insert_owner_biosample_id_metadata(
            conn,
            biosample_idx=bs_idx,
            biosample_study_field_idx=field1_idx,
            value_text="FIRST-OWNER-ID",
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample_metadata"].append(meta_idx)

    # Second flagged row for the same biosample (different field) must fail
    # the biosample_metadata_unique_owner_biosample_id partial unique index.
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(asyncpg.UniqueViolationError):
            await insert_owner_biosample_id_metadata(
                conn,
                biosample_idx=bs_idx,
                biosample_study_field_idx=field2_idx,
                value_text="SECOND-OWNER-ID",
                created_by_idx=ctx["principal_idx"],
            )


async def test__insert_typed_metadata_allows_many_non_owner_id_rows_per_biosample(ctx):
    """Verify the partial unique index does not over-restrict non-flagged
    rows: the shared typed inserter never touches is_owner_biosample_id,
    so the DB default keeps the column FALSE and multiple rows for the
    same biosample succeed. The partial unique index is biosample-only,
    so this assertion is biosample-only.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    field1_idx = await _create_local_field(ctx, "a")
    field2_idx = await _create_local_field(ctx, "b")

    async with ctx["pool"].acquire() as conn:
        m1 = await _insert_typed_metadata(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            entity_idx=bs_idx,
            study_field_idx=field1_idx,
            data_type=FieldDataType.TEXT,
            value="VAL-A",
            created_by_idx=ctx["principal_idx"],
        )
        m2 = await _insert_typed_metadata(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            entity_idx=bs_idx,
            study_field_idx=field2_idx,
            data_type=FieldDataType.TEXT,
            value="VAL-B",
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample_metadata"].extend([m1, m2])

    # Both rows are present; the column the shared inserter does not write
    # carries the DB default (FALSE) for both.
    rows = await ctx["pool"].fetch(
        "SELECT idx, is_owner_biosample_id FROM qiita.biosample_metadata"
        " WHERE biosample_idx = $1 ORDER BY idx",
        bs_idx,
    )
    expected = [
        {"idx": m1, "is_owner_biosample_id": False},
        {"idx": m2, "is_owner_biosample_id": False},
    ]
    assert [dict(r) for r in rows] == expected
