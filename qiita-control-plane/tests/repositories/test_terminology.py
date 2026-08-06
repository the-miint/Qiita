"""Tests for the terminology repository layer: the atomic row operations and
the import_terminology_release composer.

Scope is the connection-level contract — the transaction guard, a conditional
UPDATE that matches nothing returning None instead of raising, and the closure
rebuild's tolerance of tuples naming a term the batch never supplied. The
status-transition rules those primitives back are exercised against the
action-layer entry point instead.
"""

import pytest
from qiita_common.models import TerminologyStatus

from qiita_control_plane.repositories.terminology import (
    ParsedTerm,
    TerminologyImportResult,
    fetch_terminology,
    fetch_terminology_idx_by_name,
    import_terminology_release,
    update_terminology_status,
)
from qiita_control_plane.testing.db_seeds import (
    SEEDED_TERMINOLOGY_LOADED_AT,
    seed_terminology,
)

pytestmark = pytest.mark.db

# Far above the identity sequence start, so it can never collide with a real row.
_ABSENT_TERMINOLOGY_IDX = 2**40


def _term(term_id: str, label: str) -> ParsedTerm:
    """A non-obsolete ParsedTerm, the shape most closure fixtures need."""
    return ParsedTerm(
        term_id=term_id,
        label=label,
        is_obsolete=False,
        replaced_by_term_id=None,
        obsoletion_kind=None,
    )


# ---------------------------------------------------------------------------
# import_terminology_release
# ---------------------------------------------------------------------------


async def test_import_terminology_release_requires_transaction(postgres_pool):
    """Tests the case where the composer runs on a connection with no open
    transaction: the entry guard rejects the call before any write lands."""
    async with postgres_pool.acquire() as conn:
        with pytest.raises(RuntimeError, match="outside a transaction"):
            await import_terminology_release(
                conn,
                name="tr_no_transaction",
                version="1.0.0",
                parsed_terms=[_term("TR:1", "one")],
                parsed_closure=[],
            )

    # The guard fires ahead of find-or-create, so no terminology row exists.
    assert await fetch_terminology_idx_by_name(postgres_pool, "tr_no_transaction") is None


async def test_import_terminology_release_row_already_loading(postgres_pool, created_terminologies):
    """Tests the case where the terminology row is already in 'loading':
    only 'active' or 'failed' can enter a load, so the composer refuses
    rather than joining an in-flight one."""
    terminology_idx = await seed_terminology(
        postgres_pool, name="tr_already_loading", status=TerminologyStatus.LOADING
    )
    created_terminologies.append(terminology_idx)

    async with postgres_pool.acquire() as conn, conn.transaction():
        with pytest.raises(RuntimeError, match="not in ACTIVE or FAILED"):
            await import_terminology_release(
                conn,
                name="tr_already_loading",
                version="2.0.0",
                parsed_terms=[_term("TR:1", "one")],
                parsed_closure=[],
            )

    # Version and status are untouched: the refusal precedes every write.
    row = await fetch_terminology(postgres_pool, terminology_idx)
    expected_row = {
        "terminology_idx": terminology_idx,
        "name": "tr_already_loading",
        "version": "1.0.0",
        "loaded_at": SEEDED_TERMINOLOGY_LOADED_AT,
        "status": TerminologyStatus.LOADING.value,
    }
    assert dict(row) == expected_row


async def test_import_terminology_release_closure_tuple_with_unknown_term_id(
    postgres_pool, created_terminologies
):
    """Tests the case where a closure tuple names a term the batch never
    supplied: the inner JOINs drop that tuple, so the reported closure count
    is lower than the number of tuples handed in."""
    parsed_terms = [_term("TR:1", "one"), _term("TR:2", "two")]
    parsed_closure = [("TR:1", "TR:1", 0), ("TR:1", "TR:2", 1), ("TR:1", "TR:404", 1)]

    async with postgres_pool.acquire() as conn, conn.transaction():
        result = await import_terminology_release(
            conn,
            name="tr_closure_unknown",
            version="1.0.0",
            parsed_terms=parsed_terms,
            parsed_closure=parsed_closure,
        )
    created_terminologies.append(result.terminology_idx)

    expected = TerminologyImportResult(
        terminology_idx=result.terminology_idx,
        terms_inserted=2,
        terms_label_updated=0,
        terms_newly_obsoleted=0,
        terms_newly_merged=0,
        terms_silently_dropped=0,
        closure_rows=2,
    )
    assert result == expected


# ---------------------------------------------------------------------------
# update_terminology_status
# ---------------------------------------------------------------------------


async def test_update_terminology_status_not_found(postgres_pool):
    """Tests the case where the idx names no row: the conditional UPDATE
    matches nothing and returns None instead of raising, leaving the caller
    to distinguish absence from an illegal source state."""
    row = await update_terminology_status(
        postgres_pool,
        _ABSENT_TERMINOLOGY_IDX,
        TerminologyStatus.ACTIVE,
        [TerminologyStatus.LOADING.value],
    )
    assert row is None


async def test_update_terminology_status_invalid_source(postgres_pool, created_terminologies):
    """Tests the case where the row exists but its status is not among the
    permitted sources: the UPDATE matches nothing, returns None, and leaves
    the row exactly as it was."""
    terminology_idx = await seed_terminology(
        postgres_pool, name="tr_invalid_source", status=TerminologyStatus.ACTIVE
    )
    created_terminologies.append(terminology_idx)

    row = await update_terminology_status(
        postgres_pool,
        terminology_idx,
        TerminologyStatus.ACTIVE,
        [TerminologyStatus.LOADING.value],
    )
    assert row is None

    unchanged = await fetch_terminology(postgres_pool, terminology_idx)
    expected_row = {
        "terminology_idx": terminology_idx,
        "name": "tr_invalid_source",
        "version": "1.0.0",
        "loaded_at": SEEDED_TERMINOLOGY_LOADED_AT,
        "status": TerminologyStatus.ACTIVE.value,
    }
    assert dict(unchanged) == expected_row


# ---------------------------------------------------------------------------
# fetch_terminology / fetch_terminology_idx_by_name
# ---------------------------------------------------------------------------


async def test_fetch_terminology_not_found(postgres_pool):
    """Tests the case where the idx names no row: the read returns None."""
    assert await fetch_terminology(postgres_pool, _ABSENT_TERMINOLOGY_IDX) is None


async def test_fetch_terminology_idx_by_name_not_found(postgres_pool):
    """Tests the case where no terminology carries the given name: the read
    returns None rather than an empty record."""
    assert await fetch_terminology_idx_by_name(postgres_pool, "tr_no_such_name") is None
