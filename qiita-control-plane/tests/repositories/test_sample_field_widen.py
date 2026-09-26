"""Tests for widening a study-local field to text: the type flip, the value
move that must accompany it, and the shapes the function refuses.

Parameterized over SPECS so one statement of a behaviour covers both
sample-family stacks; pytest reports ids as [biosample] / [prep_sample].
"""

import secrets
from datetime import date
from decimal import Decimal

import asyncpg
import pytest
from qiita_common.models import FieldDataType

from qiita_control_plane.repositories._sample_helpers import (
    _insert_metadata,
    fetch_study_field,
    parse_text_for_data_type,
    widen_study_field_to_text,
)
from qiita_control_plane.routes._helpers import parse_kv_detail
from qiita_control_plane.testing.db_seeds import seed_terminology

from .conftest import (
    METADATA_WRITE_LOCK_TIMEOUT,
    SPECS,
    _create_linked_entity_for_spec,
    _create_plain_field,
    _metadata_tracking_key,
    _seed_global_field_for_spec,
    _set_unique_in_study,
    _spec_id,
    _write_value,
)

pytestmark = pytest.mark.db


# Every value column a metadata row may populate, so an assertion states the
# whole value slot rather than the one column it expects to find filled.
_VALUE_COLUMNS = (
    "value_text",
    "value_numeric",
    "value_boolean",
    "value_date",
    "value_terminology_term_idx",
    "value_missing_reason_idx",
)

# The widenable types, each with a stored value and the text it must become.
# The expected text is the form the write path stores, so a widened value
# re-parses to itself -- asserted below rather than assumed.
_WIDENABLE_CASES = [
    (FieldDataType.NUMERIC, Decimal("1.50"), "1.50"),
    (FieldDataType.BOOLEAN, True, "true"),
    (FieldDataType.DATE, date(2026, 3, 7), "2026-03-07"),
]

# Short enough that a widen held off by a writer this test never releases fails
# quickly rather than stalling the suite.
CONTENDED_WIDEN_LOCK_TIMEOUT = "250ms"

# A date's rendering must not follow the session's date formatting, so a case
# asserting the text a date moved as widens under a setting that would show up
# in it.
NON_ISO_DATE_STYLE = "SQL, DMY"


async def _read_value_slot(ctx, spec, meta_idx):
    """Return every value column of one metadata row as a dict."""
    row = await ctx["pool"].fetchrow(
        f"SELECT {', '.join(_VALUE_COLUMNS)} FROM {spec.metadata_table} WHERE idx = $1",
        meta_idx,
    )
    return dict(row)


def _empty_value_slot():
    """Return a value slot with every column unpopulated, for a caller to fill
    in the one column it expects.
    """
    return dict.fromkeys(_VALUE_COLUMNS)


async def _widen(
    ctx, spec, field_idx, *, lock_timeout=METADATA_WRITE_LOCK_TIMEOUT, date_style=None
):
    """Widen one field in its own transaction and return the rows moved.

    lock_timeout=None leaves the session's bound unset, which is how a caller
    that has bounded nothing appears to the function. date_style sets the
    session's date formatting for the move, for a caller asserting the text a
    date moves as.
    """
    async with ctx["pool"].acquire() as conn, conn.transaction():
        if lock_timeout is not None:
            await conn.execute(f"SET LOCAL lock_timeout = '{lock_timeout}'")
        if date_style is not None:
            await conn.execute(f"SET LOCAL DateStyle = '{date_style}'")
        return await widen_study_field_to_text(conn, spec=spec, study_field_idx=field_idx)


async def _widen_refusal(ctx, spec, field_idx, *, lock_timeout=METADATA_WRITE_LOCK_TIMEOUT):
    """Widen one field expecting refusal, and return the parsed error DETAIL."""
    with pytest.raises(asyncpg.RaiseError) as excinfo:
        await _widen(ctx, spec, field_idx, lock_timeout=lock_timeout)
    return parse_kv_detail(excinfo.value.detail)


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
@pytest.mark.parametrize("data_type,stored,expected_text", _WIDENABLE_CASES, ids=lambda v: str(v))
async def test_widen_study_field_to_text(ctx, spec, data_type, stored, expected_text):
    """Tests the case where a field carrying one stored value is widened: the
    declaration becomes text and the value arrives in value_text in the form
    the write path would have stored, leaving no other value column populated.
    """
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    field_idx = await _create_plain_field(ctx, spec, suffix="widen", data_type=data_type)
    meta_idx = await _write_value(
        ctx, spec, entity_idx=entity_idx, field_idx=field_idx, data_type=data_type, value=stored
    )

    moved = await _widen(ctx, spec, field_idx, date_style=NON_ISO_DATE_STYLE)

    assert moved == 1
    assert await _read_value_slot(ctx, spec, meta_idx) == _empty_value_slot() | {
        "value_text": expected_text
    }

    field_row = await fetch_study_field(ctx["pool"], spec=spec, idx=field_idx)
    assert field_row["data_type"] == FieldDataType.TEXT
    # The moved text parses back to the value the field held before the move, so
    # the widen changed which column carries the value and not the value itself.
    assert parse_text_for_data_type("widened", data_type, expected_text) == stored


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_widen_study_field_to_text_bumps_updated_at(ctx, spec):
    """Tests the case where a widened field's optimistic-concurrency tag is
    read afterwards: the flip counts as a change to the row, so a tag taken
    before it is stale.
    """
    field_idx = await _create_plain_field(ctx, spec, suffix="bump", data_type=FieldDataType.NUMERIC)
    before = await fetch_study_field(ctx["pool"], spec=spec, idx=field_idx)

    await _widen(ctx, spec, field_idx)

    after = await fetch_study_field(ctx["pool"], spec=spec, idx=field_idx)
    assert after["updated_at"] > before["updated_at"]


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_widen_study_field_to_text_leaves_missing_marker(ctx, spec):
    """Tests the case where a field holds both a value and a missing-value
    marker: the marker carries no typed value, so it is passed over and only
    the typed row counts as moved.
    """
    valued_entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    marker_entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    field_idx = await _create_plain_field(
        ctx, spec, suffix="marker", data_type=FieldDataType.NUMERIC
    )

    reason_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.missing_value_reason (name) VALUES ($1) RETURNING idx",
        f"reason_{secrets.token_hex(4)}",
    )
    ctx["created"]["missing_value_reason"].append(reason_idx)

    valued_meta_idx = await _write_value(
        ctx,
        spec,
        entity_idx=valued_entity_idx,
        field_idx=field_idx,
        data_type=FieldDataType.NUMERIC,
        value=Decimal("7"),
    )
    marker_meta_idx = await ctx["pool"].fetchval(
        f"INSERT INTO {spec.metadata_table}"
        f" ({spec.entity_key_column}, {spec.study_field_idx_column},"
        f"  value_missing_reason_idx, created_by_idx)"
        f" VALUES ($1, $2, $3, $4) RETURNING idx",
        marker_entity_idx,
        field_idx,
        reason_idx,
        ctx["principal_idx"],
    )
    ctx["created"][_metadata_tracking_key(spec)].append(marker_meta_idx)

    moved = await _widen(ctx, spec, field_idx)

    assert moved == 1
    assert await _read_value_slot(ctx, spec, valued_meta_idx) == _empty_value_slot() | {
        "value_text": "7"
    }
    assert await _read_value_slot(ctx, spec, marker_meta_idx) == _empty_value_slot() | {
        "value_missing_reason_idx": reason_idx
    }


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_widen_study_field_to_text_keeps_unique_in_study(ctx, spec):
    """Tests the case where a widened field declares its values unique in the
    study: enforcement passes from the numeric partial index to the text one,
    so values distinct as numbers stay distinct as text and a later collision
    is still refused.
    """
    first_entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    second_entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    third_entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    field_idx = await _create_plain_field(
        ctx, spec, suffix="unique", data_type=FieldDataType.NUMERIC
    )
    await _set_unique_in_study(ctx, spec, field_idx, True)

    for entity_idx, value in (
        (first_entity_idx, Decimal("1.5")),
        (second_entity_idx, Decimal("2.5")),
    ):
        await _write_value(
            ctx,
            spec,
            entity_idx=entity_idx,
            field_idx=field_idx,
            data_type=FieldDataType.NUMERIC,
            value=value,
        )

    moved = await _widen(ctx, spec, field_idx)

    assert moved == 2
    stored = await ctx["pool"].fetch(
        f"SELECT value_text FROM {spec.metadata_table}"
        f" WHERE {spec.study_field_idx_column} = $1 ORDER BY value_text",
        field_idx,
    )
    assert [r["value_text"] for r in stored] == ["1.5", "2.5"]

    with pytest.raises(asyncpg.UniqueViolationError):
        await _write_value(
            ctx,
            spec,
            entity_idx=third_entity_idx,
            field_idx=field_idx,
            data_type=FieldDataType.TEXT,
            value="1.5",
        )


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_widen_study_field_to_text_already_text(ctx, spec):
    """Tests the case where the field is already declared text: the state the
    caller asked for already holds, so nothing is written, nothing is counted,
    and the tag a caller is holding stays good.
    """
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    field_idx = await _create_plain_field(ctx, spec, suffix="astext", data_type=FieldDataType.TEXT)
    meta_idx = await _write_value(
        ctx,
        spec,
        entity_idx=entity_idx,
        field_idx=field_idx,
        data_type=FieldDataType.TEXT,
        value="already here",
    )
    before = await fetch_study_field(ctx["pool"], spec=spec, idx=field_idx)

    moved = await _widen(ctx, spec, field_idx)

    assert moved == 0
    after = await fetch_study_field(ctx["pool"], spec=spec, idx=field_idx)
    assert after["data_type"] == FieldDataType.TEXT
    assert after["updated_at"] == before["updated_at"]
    assert await _read_value_slot(ctx, spec, meta_idx) == _empty_value_slot() | {
        "value_text": "already here"
    }


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_widen_study_field_to_text_globally_linked(ctx, spec):
    """Tests the case where the field inherits its type from a global field:
    the type belongs to the registry every linked study reads through, so one
    study may not change it.
    """
    global_field = await _seed_global_field_for_spec(ctx, spec, data_type=FieldDataType.NUMERIC)
    field_idx = await _create_plain_field(ctx, spec, suffix="linked", data_type=FieldDataType.TEXT)
    # Upgrade to linked, clearing the columns a linked row inherits so the
    # inheritance CHECK holds; data_type going NULL is what marks the link.
    await ctx["pool"].execute(
        f"UPDATE {spec.study_field_table}"
        f" SET {spec.study_field_global_fk_column} = $1,"
        f"     data_type = NULL,"
        f"     required = NULL,"
        f"     terminology_idx = NULL,"
        f"     tier_override = NULL"
        f" WHERE idx = $2",
        global_field.idx,
        field_idx,
    )

    detail = await _widen_refusal(ctx, spec, field_idx)

    assert detail == {"widen": "globally_linked", "study_field_idx": str(field_idx)}


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_widen_study_field_to_text_terminology(ctx, spec):
    """Tests the case where the field's values are references into a
    controlled vocabulary: there is no text form that is not either an opaque
    id or a revisable label, so the type is refused rather than guessed at.
    """
    terminology_idx = await seed_terminology(ctx["pool"], name=f"widen-{secrets.token_hex(4)}")
    ctx["created"]["terminology"].append(terminology_idx)
    field_idx = await _create_plain_field(
        ctx,
        spec,
        suffix="term",
        data_type=FieldDataType.TERMINOLOGY,
        terminology_idx=terminology_idx,
    )

    detail = await _widen_refusal(ctx, spec, field_idx)

    assert detail == {
        "widen": "unwidenable_type",
        "study_field_idx": str(field_idx),
        "data_type": "terminology",
    }


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_widen_study_field_to_text_absent_field(ctx, spec):
    """Tests the case where the idx names no field at all: the refusal is
    tagged like the others, so a caller can tell a precondition it broke from a
    refusal it could pass on to an end user.
    """
    absent_idx = await ctx["pool"].fetchval(
        f"SELECT coalesce(max(idx), 0) + 1 FROM {spec.study_field_table}"
    )

    detail = await _widen_refusal(ctx, spec, absent_idx)

    assert detail == {"widen": "not_found", "study_field_idx": str(absent_idx)}


# =============================================================================
# The metadata-table lock a widen takes, and the bound it requires
# =============================================================================


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_widen_study_field_to_text_refused_without_lock_timeout(ctx, spec):
    """Tests the case where a widenable field is widened in a session with no
    lock_timeout: the move takes a table lock, and an unbounded wait for it
    would queue every metadata write, so the widen refuses instead.
    """
    field_idx = await _create_plain_field(
        ctx, spec, suffix="nobound", data_type=FieldDataType.NUMERIC
    )
    before = await fetch_study_field(ctx["pool"], spec=spec, idx=field_idx)

    # Pinned to the SQLSTATE, not the class: the lock-timeout error subclasses
    # this one, so the class alone would not separate no bound from a bound
    # that ran out.
    with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError) as excinfo:
        await _widen(ctx, spec, field_idx, lock_timeout=None)
    assert excinfo.value.sqlstate == "55000"

    after = await fetch_study_field(ctx["pool"], spec=spec, idx=field_idx)
    assert after["data_type"] == FieldDataType.NUMERIC
    assert after["updated_at"] == before["updated_at"]


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_widen_study_field_to_text_no_lock_before_early_returns(ctx, spec):
    """Tests the case where each answer the function reaches before the move is
    given in a session with no lock_timeout: an already-text no-op and the two
    refusals decide from the field row alone, so none of them takes the table
    lock and none is turned into the missing-bound refusal.
    """
    text_field_idx = await _create_plain_field(
        ctx, spec, suffix="earlytext", data_type=FieldDataType.TEXT
    )

    global_field = await _seed_global_field_for_spec(ctx, spec, data_type=FieldDataType.NUMERIC)
    linked_field_idx = await _create_plain_field(
        ctx, spec, suffix="earlylink", data_type=FieldDataType.TEXT
    )
    await ctx["pool"].execute(
        f"UPDATE {spec.study_field_table}"
        f" SET {spec.study_field_global_fk_column} = $1,"
        f"     data_type = NULL,"
        f"     required = NULL,"
        f"     terminology_idx = NULL,"
        f"     tier_override = NULL"
        f" WHERE idx = $2",
        global_field.idx,
        linked_field_idx,
    )

    terminology_idx = await seed_terminology(ctx["pool"], name=f"early-{secrets.token_hex(4)}")
    ctx["created"]["terminology"].append(terminology_idx)
    term_field_idx = await _create_plain_field(
        ctx,
        spec,
        suffix="earlyterm",
        data_type=FieldDataType.TERMINOLOGY,
        terminology_idx=terminology_idx,
    )

    outcomes = {
        "already_text": await _widen(ctx, spec, text_field_idx, lock_timeout=None),
        "globally_linked": await _widen_refusal(ctx, spec, linked_field_idx, lock_timeout=None),
        "terminology": await _widen_refusal(ctx, spec, term_field_idx, lock_timeout=None),
    }

    assert outcomes == {
        "already_text": 0,
        "globally_linked": {
            "widen": "globally_linked",
            "study_field_idx": str(linked_field_idx),
        },
        "terminology": {
            "widen": "unwidenable_type",
            "study_field_idx": str(term_field_idx),
            "data_type": "terminology",
        },
    }


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_widen_study_field_to_text_gives_up_on_in_flight_write(ctx, spec):
    """Tests the case where a metadata write is in flight when a field is
    widened: the move waits for the table rather than running past a row the
    writer has not committed, and gives up within its bound, leaving the field
    at its original type rather than committing a declaration that row escaped.
    """
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    written_field_idx = await _create_plain_field(
        ctx, spec, suffix="holder", data_type=FieldDataType.TEXT
    )
    widen_field_idx = await _create_plain_field(
        ctx, spec, suffix="contended", data_type=FieldDataType.NUMERIC
    )

    async with ctx["pool"].acquire() as writer:
        # Held open: the INSERT takes ROW EXCLUSIVE on the metadata table at
        # statement start and keeps it until this transaction ends.
        writer_tx = writer.transaction()
        await writer_tx.start()
        try:
            await _insert_metadata(
                writer,
                spec=spec,
                entity_idx=entity_idx,
                study_field_idx=written_field_idx,
                data_type=FieldDataType.TEXT,
                value="Sample 1",
                created_by_idx=ctx["principal_idx"],
            )

            with pytest.raises(asyncpg.LockNotAvailableError):
                await _widen(ctx, spec, widen_field_idx, lock_timeout=CONTENDED_WIDEN_LOCK_TIMEOUT)
        finally:
            await writer_tx.rollback()

    after = await fetch_study_field(ctx["pool"], spec=spec, idx=widen_field_idx)
    assert after["data_type"] == FieldDataType.NUMERIC
