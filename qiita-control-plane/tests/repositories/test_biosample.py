"""Integration tests for biosample repository functions and the import composer."""

import secrets

import asyncpg
import pytest
import pytest_asyncio
from qiita_common.models import FieldDataType

from qiita_control_plane.repositories.biosample import (
    get_or_create_local_biosample_study_field,
    import_biosample_from_owner_biosample_id,
    insert_biosample,
    insert_biosample_metadata_text,
    insert_biosample_to_study,
)

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# Seed and unique-name helpers
# ---------------------------------------------------------------------------


async def _seed_principal(pool, display_name, *, created_by_idx):
    """Insert a qiita.principal row with the given parent, return its idx.

    The parent is required so callers cannot accidentally seed a root
    principal; the system principal at idx=1 is the standard root for
    test fixtures.
    """
    return await pool.fetchval(
        "INSERT INTO qiita.principal (display_name, created_by_idx) VALUES ($1, $2) RETURNING idx",
        display_name,
        created_by_idx,
    )


async def _seed_user(pool, principal_idx, email):
    """Promote a principal to user-kind by inserting a qiita.user row.

    Required so the principal can serve as study.owner_idx (and similar
    role-typed FK columns); the trigger on those columns rejects bare
    principals. Only the required columns are populated; all other
    qiita.user columns carry NOT NULL DEFAULT '' or are nullable.
    """
    return await pool.fetchval(
        "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2) RETURNING principal_idx",
        principal_idx,
        email,
    )


async def _seed_study(pool, owner_idx, title):
    """Insert a minimal qiita.study row, return its idx."""
    return await pool.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        owner_idx,
        title,
    )


async def _seed_metadata_checklist(pool, name):
    """Insert a minimal qiita.metadata_checklist row, return its idx."""
    return await pool.fetchval(
        "INSERT INTO qiita.metadata_checklist (name) VALUES ($1) RETURNING idx",
        name,
    )


def _unique_field_name(prefix: str = "owner_biosample_id") -> str:
    """Return prefix + '_' + 8 hex chars; collision-resistant across re-runs."""
    return f"{prefix}_{secrets.token_hex(4)}"


def _unique_accession(prefix: str = "BS") -> str:
    """Return prefix + '-' + 8 hex chars; for biosample/ENA accession columns."""
    return f"{prefix}-{secrets.token_hex(4)}"


# ---------------------------------------------------------------------------
# Fixture cleanup helper
# ---------------------------------------------------------------------------


async def _delete_idxs(pool, table, idxs):
    """Delete rows by idx from qiita.<table>.

    `idxs` may be a scalar int or an iterable of ints; an empty iterable
    is a no-op. The scalar form is normalised so callers can pass a single
    auto-seeded idx without wrapping in a list.
    """
    # Normalize a bare int into a one-element list so callers can pass either.
    if isinstance(idxs, int):
        idxs = [idxs]
    if not idxs:
        return
    await pool.execute(
        f"DELETE FROM qiita.{table} WHERE idx = ANY($1::bigint[])",
        idxs,
    )


async def _cleanup_tracked(pool, created):
    """FK-reverse cleanup of every row tracked in `created`.

    The order encodes FK dependencies; do not reorder. biosample_to_study
    is composite-keyed so it is handled separately from the idx-keyed sweep.
    Empty lists for tables a given test does not seed are no-ops via
    `_delete_idxs`, so the sweep is free for tests that only touch the
    common biosample surface.
    """
    # Sweep the EAV value rows first; they reference everything else.
    await _delete_idxs(pool, "biosample_metadata", created["biosample_metadata"])
    # Field rows reference biosample_global_field and terminology.
    await _delete_idxs(pool, "biosample_study_field", created["biosample_study_field"])
    for bs, st in created["biosample_to_study"]:
        await pool.execute(
            "DELETE FROM qiita.biosample_to_study WHERE biosample_idx = $1 AND study_idx = $2",
            bs,
            st,
        )
    await _delete_idxs(pool, "biosample", created["biosample"])
    # biosample_global_field and terminology_term both reference terminology;
    # missing_value_reason has no inbound refs left after biosample_metadata.
    await _delete_idxs(pool, "biosample_global_field", created["biosample_global_field"])
    await _delete_idxs(pool, "terminology_term", created["terminology_term"])
    await _delete_idxs(pool, "missing_value_reason", created["missing_value_reason"])
    await _delete_idxs(pool, "terminology", created["terminology"])
    await _delete_idxs(pool, "study", created["studies"])


# ---------------------------------------------------------------------------
# Per-test fixture
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def ctx(postgres_pool):
    """Seed two principals, a user, a study, and a metadata checklist.

    Each test gets fresh seed rows (suffixed with a token to avoid collisions
    across re-runs) plus an empty `created` dict the test populates with idxs
    of any rows it inserts. Cleanup runs in FK-reverse order after the test.

    principal_idx is promoted to user-kind via a qiita.user row so it can
    serve as study.owner_idx (the role-typed FK trigger on that column
    rejects bare principals). biosample_owner_idx stays bare;
    biosample.owner_idx is not in the role-typed-FK registry.
    """
    # Token-suffixed names avoid UNIQUE collisions if a prior run leaked rows.
    # Two principals are seeded so composer tests can exercise the case where
    # the biosample owner is a different principal than the one running the
    # call (e.g., an admin importing on behalf of an owner). principal_idx
    # is the caller / study owner; biosample_owner_idx is a peer principal.
    token = secrets.token_hex(4)
    principal_idx = await _seed_principal(postgres_pool, f"bs-{token}", created_by_idx=1)
    await _seed_user(postgres_pool, principal_idx, f"bs-{token}@test.local")
    biosample_owner_idx = await _seed_principal(
        postgres_pool, f"bs-owner-{token}", created_by_idx=principal_idx
    )
    study_idx = await _seed_study(postgres_pool, principal_idx, f"bs-{token}")
    checklist_idx = await _seed_metadata_checklist(postgres_pool, f"bs-{token}")

    # Test-populated tracking dict; lists hold idxs (or (bs, st) tuples).
    # `studies` holds idxs of any extra studies the test seeds beyond the
    # one auto-seeded above; they are deleted after the biosample-side rows
    # are swept and before the auto-seeded study row is dropped.
    created: dict = {
        "biosample_metadata": [],
        "biosample_study_field": [],
        "biosample_to_study": [],
        "biosample": [],
        "biosample_global_field": [],
        "terminology_term": [],
        "missing_value_reason": [],
        "terminology": [],
        "studies": [],
    }

    yield {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "biosample_owner_idx": biosample_owner_idx,
        "study_idx": study_idx,
        "checklist_idx": checklist_idx,
        "created": created,
    }

    # Sweep test-populated rows then the auto-seeded support rows.
    await _cleanup_tracked(postgres_pool, created)
    await _delete_idxs(postgres_pool, "metadata_checklist", checklist_idx)
    await _delete_idxs(postgres_pool, "study", study_idx)
    # qiita.user → qiita.principal is ON DELETE RESTRICT, so the user row
    # must go before the principal it references. The role-typed
    # user_no_delete_if_study_owner trigger already passes because the
    # study row above has been removed.
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    # principal FK is DEFERRABLE INITIALLY DEFERRED, so deleting both rows in
    # one statement is fine — the biosample_owner_idx → principal_idx
    # reference is checked at commit, after both rows are gone.
    await _delete_idxs(postgres_pool, "principal", [biosample_owner_idx, principal_idx])


# ---------------------------------------------------------------------------
# In-test setup helpers
# ---------------------------------------------------------------------------


async def _create_biosample(ctx):
    """Helper: create a biosample owned by ctx['principal_idx'], track for cleanup."""
    async with ctx["pool"].acquire() as conn:
        idx = await insert_biosample(
            conn,
            owner_idx=ctx["principal_idx"],
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample"].append(idx)
    return idx


async def _create_biosample_with_link(ctx):
    """Helper: create a biosample, link it to ctx['study_idx'], track both."""
    bs_idx = await _create_biosample(ctx)
    async with ctx["pool"].acquire() as conn:
        await insert_biosample_to_study(
            conn,
            biosample_idx=bs_idx,
            study_idx=ctx["study_idx"],
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample_to_study"].append((bs_idx, ctx["study_idx"]))
    return bs_idx


async def _create_local_field(ctx, suffix=""):
    """Helper: create a purely-local biosample_study_field, track for cleanup."""
    field_name = f"{_unique_field_name()}_{suffix}"
    async with ctx["pool"].acquire() as conn:
        idx = await get_or_create_local_biosample_study_field(
            conn,
            study_idx=ctx["study_idx"],
            display_name=field_name,
            created_by_idx=ctx["principal_idx"],
            required=True,
        )
    ctx["created"]["biosample_study_field"].append(idx)
    return idx


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
    bs_idx = await _create_biosample(ctx)

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
    bs_idx = await _create_biosample(ctx)

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
# get_or_create_local_biosample_study_field
# ---------------------------------------------------------------------------


async def test_get_or_create_local_biosample_study_field_creates_purely_local(ctx):
    field_name = _unique_field_name()

    # Create a new local field with required=True (composer's intended use).
    async with ctx["pool"].acquire() as conn:
        idx = await get_or_create_local_biosample_study_field(
            conn,
            study_idx=ctx["study_idx"],
            display_name=field_name,
            created_by_idx=ctx["principal_idx"],
            required=True,
        )
    ctx["created"]["biosample_study_field"].append(idx)

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
    async with ctx["pool"].acquire() as conn:
        first_idx = await get_or_create_local_biosample_study_field(
            conn,
            study_idx=ctx["study_idx"],
            display_name=field_name,
            created_by_idx=ctx["principal_idx"],
        )
        second_idx = await get_or_create_local_biosample_study_field(
            conn,
            study_idx=ctx["study_idx"],
            display_name=field_name,
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample_study_field"].append(first_idx)

    assert first_idx == second_idx

    # Confirm the DB only has one row for this (study, display_name).
    count = await ctx["pool"].fetchval(
        "SELECT count(*) FROM qiita.biosample_study_field"
        " WHERE study_idx = $1 AND display_name = $2",
        ctx["study_idx"],
        field_name,
    )
    assert count == 1


# ---------------------------------------------------------------------------
# biosample_metadata_check_data_type_and_set_global_field_idx trigger
# ---------------------------------------------------------------------------


async def test_biosample_metadata_rejects_value_text_when_data_type_numeric(ctx):
    bs_idx = await _create_biosample_with_link(ctx)

    # Create a purely-local numeric-typed field. Mismatching the field's
    # data_type with the populated value_* column must be rejected by the
    # check_data_type half of the trigger.
    async with ctx["pool"].acquire() as conn:
        field_idx = await get_or_create_local_biosample_study_field(
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
    async with ctx["pool"].acquire() as conn:
        field_idx = await get_or_create_local_biosample_study_field(
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
    async with ctx["pool"].acquire() as conn:
        field_idx = await get_or_create_local_biosample_study_field(
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
            bs_idx = await import_biosample_from_owner_biosample_id(
                conn,
                study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=field_name,
                owner_biosample_id_value="OWNER-XYZ-1",
                caller_idx=ctx["principal_idx"],
            )
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
        "SELECT idx, study_idx, display_name, data_type, required, biosample_global_field_idx"
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
            bs_idx = await import_biosample_from_owner_biosample_id(
                conn,
                study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=field_name,
                owner_biosample_id_value="OWNER-CL-1",
                caller_idx=ctx["principal_idx"],
                metadata_checklist_idx=ctx["checklist_idx"],
                biosample_accession=bs_acc,
            )
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
            bs1 = await import_biosample_from_owner_biosample_id(
                conn,
                study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=field_name,
                owner_biosample_id_value="A",
                caller_idx=ctx["principal_idx"],
            )
            bs2 = await import_biosample_from_owner_biosample_id(
                conn,
                study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=field_name,
                owner_biosample_id_value="B",
                caller_idx=ctx["principal_idx"],
            )
    await _track_composer_outputs(ctx, bs1, ctx["study_idx"], field_name)
    await _track_composer_outputs(ctx, bs2, ctx["study_idx"], field_name)

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
            bs1 = await import_biosample_from_owner_biosample_id(
                conn,
                study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=name_a,
                owner_biosample_id_value="A",
                caller_idx=ctx["principal_idx"],
            )
            bs2 = await import_biosample_from_owner_biosample_id(
                conn,
                study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=name_b,
                owner_biosample_id_value="B",
                caller_idx=ctx["principal_idx"],
            )
    await _track_composer_outputs(ctx, bs1, ctx["study_idx"], name_a)
    await _track_composer_outputs(ctx, bs2, ctx["study_idx"], name_b)

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
            bs1 = await import_biosample_from_owner_biosample_id(
                conn,
                study_idx=ctx["study_idx"],
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=field_name,
                owner_biosample_id_value="STUDY1-1",
                caller_idx=ctx["principal_idx"],
            )
            bs2 = await import_biosample_from_owner_biosample_id(
                conn,
                study_idx=second_study_idx,
                owner_idx=ctx["biosample_owner_idx"],
                owner_biosample_id_field_name=field_name,
                owner_biosample_id_value="STUDY2-1",
                caller_idx=ctx["principal_idx"],
            )
    await _track_composer_outputs(ctx, bs1, ctx["study_idx"], field_name)
    await _track_composer_outputs(ctx, bs2, second_study_idx, field_name)

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
            )
