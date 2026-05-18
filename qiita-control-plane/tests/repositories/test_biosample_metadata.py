"""Integration tests for biosample-metadata repository functions.

Mirrors the qiita.biosample_metadata / biosample_study_field /
biosample_global_field surface in the schema. The composer test suite
lives in test_biosample.py because the composer itself is in
repositories.biosample; cross-suite fixtures and seed/cleanup helpers
live in tests/repositories/conftest.py.
"""

import secrets
from datetime import date
from decimal import Decimal

import asyncpg
import pytest
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX
from qiita_common.models import FieldDataType

from qiita_control_plane.repositories._sample_helpers import (
    GlobalFieldRow,
    GlobalMetadataRow,
    StudyFieldConflictError,
)
from qiita_control_plane.repositories.biosample_metadata import (
    fetch_biosample_global_fields_by_display_names,
    fetch_global_metadata_for_biosample,
    get_or_create_globally_linked_biosample_study_field,
    get_or_create_local_biosample_study_field,
    insert_biosample_metadata_date,
    insert_biosample_metadata_numeric,
    insert_biosample_metadata_text,
)
from qiita_control_plane.testing.db_seeds import (
    retire_biosample_to_study_link,
    seed_biosample_global_field,
)

from .conftest import (
    _create_biosample_with_link,
    _create_local_field,
    _unique_field_name,
)

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# get_or_create_local_biosample_study_field
# ---------------------------------------------------------------------------


async def test_get_or_create_local_biosample_study_field_creates_purely_local(ctx):
    field_name = _unique_field_name()

    # Create a new local field with required=True (composer's intended use).
    async with ctx["pool"].acquire() as conn, conn.transaction():
        idx, created, resolved_global_field_idx = await get_or_create_local_biosample_study_field(
            conn,
            study_idx=ctx["study_idx"],
            display_name=field_name,
            created_by_idx=ctx["principal_idx"],
            required=True,
        )
    ctx["created"]["biosample_study_field"].append(idx)

    # First call inserts the row; created flag reports True and the resolved
    # global_field_idx is None because the create branch always produces a
    # purely-local row.
    assert created is True
    assert resolved_global_field_idx is None

    # Verify the row reflects the local-field defaults plus the explicit required.
    row = await ctx["pool"].fetchrow(
        "SELECT study_idx, biosample_global_field_idx, display_name, description,"
        " data_type, required, terminology_idx, tier_override, created_by_idx"
        " FROM qiita.biosample_study_field WHERE idx = $1",
        idx,
    )
    expected = {
        "study_idx": ctx["study_idx"],
        "biosample_global_field_idx": None,
        "display_name": field_name,
        "description": None,
        "data_type": "text",
        "required": True,
        "terminology_idx": None,
        "tier_override": None,
        "created_by_idx": ctx["principal_idx"],
    }
    assert dict(row) == expected


async def test_get_or_create_local_biosample_study_field_returns_existing(ctx):
    field_name = _unique_field_name()

    # First call inserts; second call with the same (study_idx, display_name)
    # must return the same idx without inserting a new row.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        (
            first_idx,
            first_created,
            first_global_field_idx,
        ) = await get_or_create_local_biosample_study_field(
            conn,
            study_idx=ctx["study_idx"],
            display_name=field_name,
            created_by_idx=ctx["principal_idx"],
        )
        (
            second_idx,
            second_created,
            second_global_field_idx,
        ) = await get_or_create_local_biosample_study_field(
            conn,
            study_idx=ctx["study_idx"],
            display_name=field_name,
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample_study_field"].append(first_idx)

    # First call inserts (created=True); second call resolves via the
    # fallback SELECT branch (created=False) and converges on the same idx.
    # Both calls resolve to a purely-local row, so the global_field_idx
    # element is None in both.
    assert first_created is True
    assert second_created is False
    assert first_idx == second_idx
    assert first_global_field_idx is None
    assert second_global_field_idx is None

    # Confirm the DB only has one row for this (study, display_name).
    count = await ctx["pool"].fetchval(
        "SELECT count(*) FROM qiita.biosample_study_field"
        " WHERE study_idx = $1 AND display_name = $2",
        ctx["study_idx"],
        field_name,
    )
    assert count == 1


# ---------------------------------------------------------------------------
# fetch_biosample_global_fields_by_display_names
# ---------------------------------------------------------------------------


async def test_fetch_biosample_global_fields_by_display_names_returns_matching(ctx):
    # Seed two global fields with collision-resistant names.
    suffix = secrets.token_hex(4)
    name_a = f"Test Field A {suffix}"
    name_b = f"Test Field B {suffix}"
    idx_a = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"tfa_{suffix}",
        display_name=name_a,
        data_type=FieldDataType.TEXT,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    idx_b = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"tfb_{suffix}",
        display_name=name_b,
        data_type=FieldDataType.NUMERIC,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"]["biosample_global_field"].extend([idx_a, idx_b])

    # Fetch both names; verify the dict carries both rows with correct fields.
    async with ctx["pool"].acquire() as conn:
        result = await fetch_biosample_global_fields_by_display_names(conn, [name_a, name_b])

    expected = {
        name_a: GlobalFieldRow(idx=idx_a, display_name=name_a, data_type=FieldDataType.TEXT),
        name_b: GlobalFieldRow(idx=idx_b, display_name=name_b, data_type=FieldDataType.NUMERIC),
    }
    assert result == expected


async def test_fetch_biosample_global_fields_by_display_names_omits_unknown(ctx):
    # Seed one global field; ask for it plus a name that does not exist.
    suffix = secrets.token_hex(4)
    known_name = f"Known Field {suffix}"
    unknown_name = f"Unknown Field {suffix}"
    idx = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"kf_{suffix}",
        display_name=known_name,
        data_type=FieldDataType.TEXT,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"]["biosample_global_field"].append(idx)

    async with ctx["pool"].acquire() as conn:
        result = await fetch_biosample_global_fields_by_display_names(
            conn, [known_name, unknown_name]
        )

    # Only the known name appears; unknown is silently absent.
    expected = {
        known_name: GlobalFieldRow(idx=idx, display_name=known_name, data_type=FieldDataType.TEXT),
    }
    assert result == expected


# ---------------------------------------------------------------------------
# get_or_create_globally_linked_biosample_study_field
# ---------------------------------------------------------------------------


async def test_get_or_create_globally_linked_biosample_study_field_creates_new_row(ctx):
    # Seed a global field the new field will be linked to.
    suffix = secrets.token_hex(4)
    display_name = f"Linked Field {suffix}"
    global_idx = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"link_{suffix}",
        display_name=display_name,
        data_type=FieldDataType.NUMERIC,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"]["biosample_global_field"].append(global_idx)

    # Upsert a globally-linked study field at the same display_name.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        idx, created = await get_or_create_globally_linked_biosample_study_field(
            conn,
            study_idx=ctx["study_idx"],
            global_field_idx=global_idx,
            display_name=display_name,
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample_study_field"].append(idx)

    # First call inserts the row; created flag must report True.
    assert created is True

    # Verify the inheritance-CHECK-compliant row shape: global link populated,
    # data_type / required / terminology_idx / tier_override all NULL.
    row = await ctx["pool"].fetchrow(
        "SELECT study_idx, biosample_global_field_idx, display_name, description,"
        " data_type, required, terminology_idx, tier_override, created_by_idx"
        " FROM qiita.biosample_study_field WHERE idx = $1",
        idx,
    )
    expected = {
        "study_idx": ctx["study_idx"],
        "biosample_global_field_idx": global_idx,
        "display_name": display_name,
        "description": None,
        "data_type": None,
        "required": None,
        "terminology_idx": None,
        "tier_override": None,
        "created_by_idx": ctx["principal_idx"],
    }
    assert dict(row) == expected


async def test_get_or_create_globally_linked_biosample_study_field_returns_existing(ctx):
    # Seed a global field and call the upsert twice with the same args.
    suffix = secrets.token_hex(4)
    display_name = f"Linked Field {suffix}"
    global_idx = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"link_{suffix}",
        display_name=display_name,
        data_type=FieldDataType.TEXT,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"]["biosample_global_field"].append(global_idx)

    async with ctx["pool"].acquire() as conn, conn.transaction():
        first_idx, first_created = await get_or_create_globally_linked_biosample_study_field(
            conn,
            study_idx=ctx["study_idx"],
            global_field_idx=global_idx,
            display_name=display_name,
            created_by_idx=ctx["principal_idx"],
        )
        second_idx, second_created = await get_or_create_globally_linked_biosample_study_field(
            conn,
            study_idx=ctx["study_idx"],
            global_field_idx=global_idx,
            display_name=display_name,
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample_study_field"].append(first_idx)

    # First call inserts; second call resolves via the fallback SELECT branch.
    assert first_created is True
    assert second_created is False
    assert first_idx == second_idx


async def test_get_or_create_globally_linked_biosample_study_field_raises_on_local_collision(ctx):
    # Pre-seed a purely-local row at (study_idx, display_name) by calling the
    # local sibling. Then ask for a globally-linked row at the same key.
    suffix = secrets.token_hex(4)
    display_name = f"Collision Field {suffix}"
    async with ctx["pool"].acquire() as conn, conn.transaction():
        local_idx, _, _ = await get_or_create_local_biosample_study_field(
            conn,
            study_idx=ctx["study_idx"],
            display_name=display_name,
            created_by_idx=ctx["principal_idx"],
            required=True,
        )
    ctx["created"]["biosample_study_field"].append(local_idx)

    # Seed a global field the caller wants to link to.
    global_idx = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"col_{suffix}",
        display_name=f"Global {suffix}",
        data_type=FieldDataType.TEXT,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"]["biosample_global_field"].append(global_idx)

    # The upsert must detect the existing row is purely-local and raise.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        with pytest.raises(StudyFieldConflictError) as excinfo:
            await get_or_create_globally_linked_biosample_study_field(
                conn,
                study_idx=ctx["study_idx"],
                global_field_idx=global_idx,
                display_name=display_name,
                created_by_idx=ctx["principal_idx"],
            )
    assert excinfo.value.found_global_field_idx is None
    assert excinfo.value.expected_global_field_idx == global_idx
    assert excinfo.value.display_name == display_name


async def test_get_or_create_globally_linked_biosample_study_field_raises_on_global_mismatch(ctx):
    # Two distinct global fields; pre-seed a study field bound to the first.
    suffix = secrets.token_hex(4)
    global_a = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"ma_{suffix}",
        display_name=f"Global A {suffix}",
        data_type=FieldDataType.TEXT,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    global_b = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"mb_{suffix}",
        display_name=f"Global B {suffix}",
        data_type=FieldDataType.TEXT,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"]["biosample_global_field"].extend([global_a, global_b])

    # Shared study-local display_name pointing at global_a.
    display_name = f"Field {suffix}"
    async with ctx["pool"].acquire() as conn, conn.transaction():
        existing_idx, _ = await get_or_create_globally_linked_biosample_study_field(
            conn,
            study_idx=ctx["study_idx"],
            global_field_idx=global_a,
            display_name=display_name,
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample_study_field"].append(existing_idx)

    # Asking for the same display_name with global_b must raise; the row
    # already binds to global_a.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        with pytest.raises(StudyFieldConflictError) as excinfo:
            await get_or_create_globally_linked_biosample_study_field(
                conn,
                study_idx=ctx["study_idx"],
                global_field_idx=global_b,
                display_name=display_name,
                created_by_idx=ctx["principal_idx"],
            )
    assert excinfo.value.found_global_field_idx == global_a
    assert excinfo.value.expected_global_field_idx == global_b


# ---------------------------------------------------------------------------
# biosample_metadata_check_data_type_and_set_global_field_idx trigger
# ---------------------------------------------------------------------------


async def test_biosample_metadata_rejects_value_text_when_data_type_numeric(ctx):
    bs_idx = await _create_biosample_with_link(ctx)

    # Create a purely-local numeric-typed field. Mismatching the field's
    # data_type with the populated value_* column must be rejected by the
    # check_data_type half of the trigger.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _, _ = await get_or_create_local_biosample_study_field(
            conn,
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
        field_idx, _, _ = await get_or_create_local_biosample_study_field(
            conn,
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
        field_idx, _, _ = await get_or_create_local_biosample_study_field(
            conn,
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
# insert_biosample_metadata_text
# ---------------------------------------------------------------------------


async def test_insert_biosample_metadata_text_inserts_owner_biosample_id_row(ctx):
    bs_idx = await _create_biosample_with_link(ctx)
    field_idx = await _create_local_field(ctx)

    # Insert one text-valued metadata row flagged as the owner's identifier.
    async with ctx["pool"].acquire() as conn:
        meta_idx = await insert_biosample_metadata_text(
            conn,
            biosample_idx=bs_idx,
            biosample_study_field_idx=field_idx,
            value_text="OWNER-SAMPLE-42",
            created_by_idx=ctx["principal_idx"],
            is_owner_biosample_id=True,
        )
    ctx["created"]["biosample_metadata"].append(meta_idx)

    # Verify the row carries the flag and the expected value.
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


async def test_insert_biosample_metadata_text_rejects_second_owner_biosample_id(ctx):
    bs_idx = await _create_biosample_with_link(ctx)
    field1_idx = await _create_local_field(ctx, "a")
    field2_idx = await _create_local_field(ctx, "b")

    # First owner-biosample-id row succeeds.
    async with ctx["pool"].acquire() as conn:
        meta_idx = await insert_biosample_metadata_text(
            conn,
            biosample_idx=bs_idx,
            biosample_study_field_idx=field1_idx,
            value_text="FIRST-OWNER-ID",
            created_by_idx=ctx["principal_idx"],
            is_owner_biosample_id=True,
        )
    ctx["created"]["biosample_metadata"].append(meta_idx)

    # Second owner-biosample-id row for the same biosample (different field) must fail
    # the biosample_metadata_unique_owner_biosample_id partial unique index.
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(asyncpg.UniqueViolationError):
            await insert_biosample_metadata_text(
                conn,
                biosample_idx=bs_idx,
                biosample_study_field_idx=field2_idx,
                value_text="SECOND-OWNER-ID",
                created_by_idx=ctx["principal_idx"],
                is_owner_biosample_id=True,
            )


async def test_insert_biosample_metadata_text_allows_many_non_owner_rows(ctx):
    bs_idx = await _create_biosample_with_link(ctx)
    field1_idx = await _create_local_field(ctx, "a")
    field2_idx = await _create_local_field(ctx, "b")

    # Two non-owner-biosample-id rows for the same biosample succeed; the partial
    # unique index must not over-restrict when is_owner_biosample_id is false.
    async with ctx["pool"].acquire() as conn:
        m1 = await insert_biosample_metadata_text(
            conn,
            biosample_idx=bs_idx,
            biosample_study_field_idx=field1_idx,
            value_text="VAL-A",
            created_by_idx=ctx["principal_idx"],
        )
        m2 = await insert_biosample_metadata_text(
            conn,
            biosample_idx=bs_idx,
            biosample_study_field_idx=field2_idx,
            value_text="VAL-B",
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample_metadata"].extend([m1, m2])

    # Both rows should be present and both flagged false.
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


# ---------------------------------------------------------------------------
# insert_biosample_metadata_numeric / insert_biosample_metadata_date
# ---------------------------------------------------------------------------


async def test_insert_biosample_metadata_numeric_inserts_value_numeric(ctx):
    bs_idx = await _create_biosample_with_link(ctx)

    # Numeric-typed local field so the field-contract trigger accepts a
    # value_numeric write.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _, _ = await get_or_create_local_biosample_study_field(
            conn,
            study_idx=ctx["study_idx"],
            display_name=_unique_field_name("num"),
            created_by_idx=ctx["principal_idx"],
            data_type=FieldDataType.NUMERIC,
            required=True,
        )
    ctx["created"]["biosample_study_field"].append(field_idx)

    # Insert a numeric metadata row via the typed helper.
    async with ctx["pool"].acquire() as conn:
        meta_idx = await insert_biosample_metadata_numeric(
            conn,
            biosample_idx=bs_idx,
            biosample_study_field_idx=field_idx,
            value_numeric=Decimal("3.14"),
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample_metadata"].append(meta_idx)

    # Verify the row carries value_numeric and nothing else.
    row = await ctx["pool"].fetchrow(
        "SELECT biosample_idx, biosample_study_field_idx,"
        " value_numeric, value_text, value_date,"
        " is_owner_biosample_id, created_by_idx"
        " FROM qiita.biosample_metadata WHERE idx = $1",
        meta_idx,
    )
    expected = {
        "biosample_idx": bs_idx,
        "biosample_study_field_idx": field_idx,
        "value_numeric": Decimal("3.14"),
        "value_text": None,
        "value_date": None,
        "is_owner_biosample_id": False,
        "created_by_idx": ctx["principal_idx"],
    }
    assert dict(row) == expected


async def test_insert_biosample_metadata_date_inserts_value_date(ctx):
    bs_idx = await _create_biosample_with_link(ctx)

    # Date-typed local field so the field-contract trigger accepts a
    # value_date write.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _, _ = await get_or_create_local_biosample_study_field(
            conn,
            study_idx=ctx["study_idx"],
            display_name=_unique_field_name("dt"),
            created_by_idx=ctx["principal_idx"],
            data_type=FieldDataType.DATE,
            required=True,
        )
    ctx["created"]["biosample_study_field"].append(field_idx)

    # Insert a date metadata row via the typed helper.
    sample_date = date(2026, 5, 6)
    async with ctx["pool"].acquire() as conn:
        meta_idx = await insert_biosample_metadata_date(
            conn,
            biosample_idx=bs_idx,
            biosample_study_field_idx=field_idx,
            value_date=sample_date,
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample_metadata"].append(meta_idx)

    # Verify the row carries value_date and nothing else.
    row = await ctx["pool"].fetchrow(
        "SELECT biosample_idx, biosample_study_field_idx,"
        " value_numeric, value_text, value_date,"
        " is_owner_biosample_id, created_by_idx"
        " FROM qiita.biosample_metadata WHERE idx = $1",
        meta_idx,
    )
    expected = {
        "biosample_idx": bs_idx,
        "biosample_study_field_idx": field_idx,
        "value_numeric": None,
        "value_text": None,
        "value_date": sample_date,
        "is_owner_biosample_id": False,
        "created_by_idx": ctx["principal_idx"],
    }
    assert dict(row) == expected


# ---------------------------------------------------------------------------
# fetch_global_metadata_for_biosample
# ---------------------------------------------------------------------------


async def _seed_globally_linked_metadata(
    ctx,
    *,
    biosample_idx: int,
    internal_name: str,
    display_name: str,
    description: str | None,
    data_type: FieldDataType,
    value,
):
    """Test helper: seed a global field, link a study field to it, write
    one metadata row of the right typed-column flavor, and track the
    biosample_global_field / biosample_study_field / biosample_metadata
    idxs for fixture cleanup. Returns the global field idx for callers
    that want to inspect or further extend the row.
    """
    # Seed the global field; biosample_global_field rows persist beyond
    # the test so they go on the cleanup tracker.
    global_idx = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=internal_name,
        display_name=display_name,
        data_type=data_type,
        created_by_idx=ctx["principal_idx"],
    )
    ctx["created"]["biosample_global_field"].append(global_idx)
    # Patch description if the seed helper does not support it directly;
    # the seeded row always carries display_name and data_type, but the
    # description column is set via UPDATE so the helper surface stays small.
    if description is not None:
        await ctx["pool"].execute(
            "UPDATE qiita.biosample_global_field SET description = $2 WHERE idx = $1",
            global_idx,
            description,
        )

    # Link a per-study field to the global field and dispatch the value
    # write to the matching typed insert; the field-contract trigger
    # rejects mismatches so the dispatch must agree with data_type.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _ = await get_or_create_globally_linked_biosample_study_field(
            conn,
            study_idx=ctx["study_idx"],
            global_field_idx=global_idx,
            display_name=display_name,
            created_by_idx=ctx["principal_idx"],
        )
        if data_type is FieldDataType.TEXT:
            meta_idx = await insert_biosample_metadata_text(
                conn,
                biosample_idx=biosample_idx,
                biosample_study_field_idx=field_idx,
                value_text=value,
                created_by_idx=ctx["principal_idx"],
            )
        elif data_type is FieldDataType.NUMERIC:
            meta_idx = await insert_biosample_metadata_numeric(
                conn,
                biosample_idx=biosample_idx,
                biosample_study_field_idx=field_idx,
                value_numeric=value,
                created_by_idx=ctx["principal_idx"],
            )
        else:
            meta_idx = await insert_biosample_metadata_date(
                conn,
                biosample_idx=biosample_idx,
                biosample_study_field_idx=field_idx,
                value_date=value,
                created_by_idx=ctx["principal_idx"],
            )
    ctx["created"]["biosample_study_field"].append(field_idx)
    ctx["created"]["biosample_metadata"].append(meta_idx)
    return global_idx


async def test_fetch_global_metadata_for_biosample_text_numeric_date(ctx):
    bs_idx = await _create_biosample_with_link(ctx)
    suffix = secrets.token_hex(4)

    # Three globally-linked rows, one per supported data_type, on the same
    # biosample. Names are tagged with a suffix to dodge UNIQUE collisions.
    await _seed_globally_linked_metadata(
        ctx,
        biosample_idx=bs_idx,
        internal_name=f"host_subject_id_{suffix}",
        display_name=f"Host Subject ID {suffix}",
        description="Host's stable identifier",
        data_type=FieldDataType.TEXT,
        value="HOST-7",
    )
    await _seed_globally_linked_metadata(
        ctx,
        biosample_idx=bs_idx,
        internal_name=f"latitude_{suffix}",
        display_name=f"Latitude {suffix}",
        description=None,
        data_type=FieldDataType.NUMERIC,
        value=Decimal("32.7"),
    )
    await _seed_globally_linked_metadata(
        ctx,
        biosample_idx=bs_idx,
        internal_name=f"collection_date_{suffix}",
        display_name=f"Collection Date {suffix}",
        description="Date the sample was collected",
        data_type=FieldDataType.DATE,
        value=date(2026, 5, 6),
    )

    result = await fetch_global_metadata_for_biosample(ctx["pool"], bs_idx)

    expected = {
        f"host_subject_id_{suffix}": GlobalMetadataRow(
            internal_name=f"host_subject_id_{suffix}",
            display_name=f"Host Subject ID {suffix}",
            description="Host's stable identifier",
            data_type=FieldDataType.TEXT,
            value="HOST-7",
        ),
        f"latitude_{suffix}": GlobalMetadataRow(
            internal_name=f"latitude_{suffix}",
            display_name=f"Latitude {suffix}",
            description=None,
            data_type=FieldDataType.NUMERIC,
            value=Decimal("32.7"),
        ),
        f"collection_date_{suffix}": GlobalMetadataRow(
            internal_name=f"collection_date_{suffix}",
            display_name=f"Collection Date {suffix}",
            description="Date the sample was collected",
            data_type=FieldDataType.DATE,
            value=date(2026, 5, 6),
        ),
    }
    assert result == expected


async def test_fetch_global_metadata_for_biosample_excludes_purely_local_rows(ctx):
    bs_idx = await _create_biosample_with_link(ctx)
    suffix = secrets.token_hex(4)

    # One globally-linked row to confirm appears in the result.
    await _seed_globally_linked_metadata(
        ctx,
        biosample_idx=bs_idx,
        internal_name=f"global_only_{suffix}",
        display_name=f"Global Only {suffix}",
        description=None,
        data_type=FieldDataType.TEXT,
        value="KEEP",
    )

    # One purely-local field plus a metadata row (not flagged as
    # owner-id) and one purely-local field flagged as owner-id. Both
    # are biosample_metadata rows with global_field_idx IS NULL via the
    # field-contract trigger; both must be filtered out by the read.
    local_field_idx = await _create_local_field(ctx, suffix=f"plain_{suffix}")
    owner_id_field_idx = await _create_local_field(ctx, suffix=f"owner_{suffix}")
    async with ctx["pool"].acquire() as conn:
        local_meta = await insert_biosample_metadata_text(
            conn,
            biosample_idx=bs_idx,
            biosample_study_field_idx=local_field_idx,
            value_text="LOCAL-VAL",
            created_by_idx=ctx["principal_idx"],
        )
        owner_meta = await insert_biosample_metadata_text(
            conn,
            biosample_idx=bs_idx,
            biosample_study_field_idx=owner_id_field_idx,
            value_text="OWNER-ID-VAL",
            created_by_idx=ctx["principal_idx"],
            is_owner_biosample_id=True,
        )
    ctx["created"]["biosample_metadata"].extend([local_meta, owner_meta])

    result = await fetch_global_metadata_for_biosample(ctx["pool"], bs_idx)

    # Only the globally-linked row appears; both purely-local rows are filtered.
    expected = {
        f"global_only_{suffix}": GlobalMetadataRow(
            internal_name=f"global_only_{suffix}",
            display_name=f"Global Only {suffix}",
            description=None,
            data_type=FieldDataType.TEXT,
            value="KEEP",
        ),
    }
    assert result == expected


async def test_fetch_global_metadata_for_biosample_preserves_link_retired_rows(ctx):
    bs_idx = await _create_biosample_with_link(ctx)
    suffix = secrets.token_hex(4)

    # Seed one globally-linked metadata row, then retire the underlying
    # biosample_to_study link. The retirement does not touch
    # global_field_idx on the metadata row — the canonical global value
    # is preserved so studies other than the retiring one (and admins)
    # continue to read it through the global field. Per-study read
    # access on the retired link is governed by the study_access
    # predicate at the route boundary, not by schema mutation here.
    await _seed_globally_linked_metadata(
        ctx,
        biosample_idx=bs_idx,
        internal_name=f"preserved_{suffix}",
        display_name=f"Preserved {suffix}",
        description=None,
        data_type=FieldDataType.TEXT,
        value="PRESERVED",
    )
    await retire_biosample_to_study_link(
        ctx["pool"],
        biosample_idx=bs_idx,
        study_idx=ctx["study_idx"],
        retired_by_idx=ctx["principal_idx"],
    )

    result = await fetch_global_metadata_for_biosample(ctx["pool"], bs_idx)

    # The row still satisfies global_field_idx IS NOT NULL and surfaces
    # unchanged through the cross-study read.
    expected = {
        f"preserved_{suffix}": GlobalMetadataRow(
            internal_name=f"preserved_{suffix}",
            display_name=f"Preserved {suffix}",
            description=None,
            data_type=FieldDataType.TEXT,
            value="PRESERVED",
        ),
    }
    assert result == expected


async def test_fetch_global_metadata_for_biosample_empty_when_none_exist(ctx):
    # A biosample with no metadata rows of any kind returns an empty dict.
    bs_idx = await _create_biosample_with_link(ctx)

    result = await fetch_global_metadata_for_biosample(ctx["pool"], bs_idx)
    assert result == {}


# ---------------------------------------------------------------------------
# biosample_study_field_propagate_global_link trigger: transition rules
# ---------------------------------------------------------------------------


async def test_propagate_link_upgrade_null_to_non_null_propagates_to_metadata(ctx):
    # NULL -> non-NULL transition (upgrade local to global): the UPDATE on
    # biosample_study_field succeeds and the trigger denormalizes the new
    # global_field_idx into any existing metadata rows through this field.
    bs_idx = await _create_biosample_with_link(ctx)
    suffix = secrets.token_hex(4)

    # Seed a TEXT global field the field will be upgraded to.
    gf_idx = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"upgrade_{suffix}",
        display_name=f"Upgrade {suffix}",
        data_type=FieldDataType.TEXT,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"]["biosample_global_field"].append(gf_idx)

    # Create a purely-local TEXT field and write one metadata row through it.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _, _ = await get_or_create_local_biosample_study_field(
            conn,
            study_idx=ctx["study_idx"],
            display_name=_unique_field_name("upgrade"),
            created_by_idx=ctx["principal_idx"],
            data_type=FieldDataType.TEXT,
            required=False,
        )
        meta_idx = await insert_biosample_metadata_text(
            conn,
            biosample_idx=bs_idx,
            biosample_study_field_idx=field_idx,
            value_text="kept",
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample_study_field"].append(field_idx)
    ctx["created"]["biosample_metadata"].append(meta_idx)

    # Upgrade the field to global: also clear the inherited columns so the
    # biosample_study_field_inheritance_consistent CHECK passes after the
    # UPDATE (the CHECK requires data_type / required NULL on linked rows).
    await ctx["pool"].execute(
        "UPDATE qiita.biosample_study_field"
        " SET biosample_global_field_idx = $1,"
        "     data_type = NULL,"
        "     required = NULL,"
        "     terminology_idx = NULL,"
        "     tier_override = NULL"
        " WHERE idx = $2",
        gf_idx,
        field_idx,
    )

    # The pre-existing metadata row's global_field_idx now reflects the
    # upgrade; the typed value column is untouched.
    row = await ctx["pool"].fetchrow(
        "SELECT global_field_idx, value_text FROM qiita.biosample_metadata WHERE idx = $1",
        meta_idx,
    )
    assert dict(row) == {"global_field_idx": gf_idx, "value_text": "kept"}


async def test_propagate_link_unlink_with_no_metadata_succeeds(ctx):
    # non-NULL -> NULL transition (unlink) with no metadata through the
    # field: the UPDATE succeeds because the unlink has no rows to strand.
    suffix = secrets.token_hex(4)
    gf_idx = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"unlink_empty_{suffix}",
        display_name=f"Unlink Empty {suffix}",
        data_type=FieldDataType.TEXT,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"]["biosample_global_field"].append(gf_idx)

    # Create a globally-linked study_field with no metadata rows yet.
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            field_idx, _ = await get_or_create_globally_linked_biosample_study_field(
                conn,
                study_idx=ctx["study_idx"],
                global_field_idx=gf_idx,
                display_name=_unique_field_name("unlink_empty"),
                created_by_idx=ctx["principal_idx"],
            )
    ctx["created"]["biosample_study_field"].append(field_idx)

    # Unlink the field. The propagate trigger has nothing to update; the
    # CHECK requires data_type / required non-NULL once unlinked, so the
    # UPDATE supplies both alongside the unlink.
    await ctx["pool"].execute(
        "UPDATE qiita.biosample_study_field"
        " SET biosample_global_field_idx = NULL,"
        "     data_type = 'text',"
        "     required = false"
        " WHERE idx = $1",
        field_idx,
    )

    row = await ctx["pool"].fetchrow(
        "SELECT biosample_global_field_idx, data_type, required"
        " FROM qiita.biosample_study_field WHERE idx = $1",
        field_idx,
    )
    assert dict(row) == {
        "biosample_global_field_idx": None,
        "data_type": "text",
        "required": False,
    }


async def test_propagate_link_unlink_with_metadata_raises(ctx):
    # non-NULL -> NULL transition (unlink) with at least one metadata row
    # through the field: the trigger raises rather than silently strand
    # the globally-linked rows.
    bs_idx = await _create_biosample_with_link(ctx)
    suffix = secrets.token_hex(4)
    gf_idx = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"unlink_full_{suffix}",
        display_name=f"Unlink Full {suffix}",
        data_type=FieldDataType.TEXT,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"]["biosample_global_field"].append(gf_idx)

    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            field_idx, _ = await get_or_create_globally_linked_biosample_study_field(
                conn,
                study_idx=ctx["study_idx"],
                global_field_idx=gf_idx,
                display_name=_unique_field_name("unlink_full"),
                created_by_idx=ctx["principal_idx"],
            )
            meta_idx = await insert_biosample_metadata_text(
                conn,
                biosample_idx=bs_idx,
                biosample_study_field_idx=field_idx,
                value_text="published",
                created_by_idx=ctx["principal_idx"],
            )
    ctx["created"]["biosample_study_field"].append(field_idx)
    ctx["created"]["biosample_metadata"].append(meta_idx)

    # Attempt to unlink — trigger refuses, the UPDATE rolls back.
    with pytest.raises(asyncpg.RaiseError, match="cannot unlink"):
        await ctx["pool"].execute(
            "UPDATE qiita.biosample_study_field"
            " SET biosample_global_field_idx = NULL,"
            "     data_type = 'text',"
            "     required = false"
            " WHERE idx = $1",
            field_idx,
        )

    # Field row remains globally-linked; metadata row is untouched.
    row = await ctx["pool"].fetchrow(
        "SELECT biosample_global_field_idx, data_type, required"
        " FROM qiita.biosample_study_field WHERE idx = $1",
        field_idx,
    )
    assert dict(row) == {
        "biosample_global_field_idx": gf_idx,
        "data_type": None,
        "required": None,
    }
    meta_row = await ctx["pool"].fetchrow(
        "SELECT global_field_idx, value_text FROM qiita.biosample_metadata WHERE idx = $1",
        meta_idx,
    )
    assert dict(meta_row) == {"global_field_idx": gf_idx, "value_text": "published"}


async def test_propagate_link_rebind_raises_unconditionally(ctx):
    # non-NULL -> different non-NULL transition (rebind): trigger rejects
    # regardless of metadata presence, because rebinding mutates the
    # field's identity rather than evolving it. This test exercises the
    # no-metadata case so the rejection is provably unconditional.
    suffix = secrets.token_hex(4)
    gf_a = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"rebind_a_{suffix}",
        display_name=f"Rebind A {suffix}",
        data_type=FieldDataType.TEXT,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    gf_b = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"rebind_b_{suffix}",
        display_name=f"Rebind B {suffix}",
        data_type=FieldDataType.TEXT,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"]["biosample_global_field"].extend([gf_a, gf_b])

    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            field_idx, _ = await get_or_create_globally_linked_biosample_study_field(
                conn,
                study_idx=ctx["study_idx"],
                global_field_idx=gf_a,
                display_name=_unique_field_name("rebind"),
                created_by_idx=ctx["principal_idx"],
            )
    ctx["created"]["biosample_study_field"].append(field_idx)

    # Attempt to rebind from gf_a to gf_b. Trigger raises even though no
    # metadata exists through this field.
    with pytest.raises(asyncpg.RaiseError, match="cannot rebind"):
        await ctx["pool"].execute(
            "UPDATE qiita.biosample_study_field SET biosample_global_field_idx = $1 WHERE idx = $2",
            gf_b,
            field_idx,
        )

    # Field row remains bound to the original global field.
    bound = await ctx["pool"].fetchval(
        "SELECT biosample_global_field_idx FROM qiita.biosample_study_field WHERE idx = $1",
        field_idx,
    )
    assert bound == gf_a
