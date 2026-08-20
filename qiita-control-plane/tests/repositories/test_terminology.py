"""Tests for the terminology repository layer: its offender reporting, the
atomic row operations, and the import_terminology_release composer.

Most of the scope is the connection-level contract — the transaction guard, a
conditional UPDATE that matches nothing returning None instead of raising, and
the closure rebuild's tolerance of tuples naming a term the batch never
supplied. The status-transition rules those primitives back get their coverage
at the action-layer entry point instead.

The `db` marker is per test rather than module-wide, so the reporting tests,
which need no database, stay in the pure-unit tier.
"""

from dataclasses import replace

import asyncpg
import pytest
from qiita_common.models import TerminologyStatus, TerminologyTermObsoletionKind

from qiita_control_plane.repositories.terminology import (
    MAX_REPORTED_OFFENDERS,
    TerminologyImportAnomaly,
    TerminologyImportResult,
    fetch_terminology,
    fetch_terminology_idx_by_name,
    format_offenders,
    import_terminology_release,
    update_terminology_status,
)
from qiita_control_plane.testing.db_seeds import (
    SEEDED_TERMINOLOGY_LOADED_AT,
    seed_terminology,
)
from qiita_control_plane.testing.terminology import parsed_term

# Far above the identity sequence start, so it can never collide with a real row.
_ABSENT_TERMINOLOGY_IDX = 2**40

# Unqualified, for reads against the catalog (which stores the bare name) and
# the count it is compared against.
_TERM_TABLE = "terminology_term"


async def _insert_term(
    pool: asyncpg.Pool,
    terminology_idx: int,
    term_id: str,
    label: str,
    alternate_label: str | None,
) -> None:
    """Insert one term row directly, bypassing the load path, so a value lands
    on a row without a release supplying it."""
    await pool.execute(
        "INSERT INTO qiita.terminology_term (terminology_idx, term_id, label, alternate_label)"
        " VALUES ($1, $2, $3, $4)",
        terminology_idx,
        term_id,
        label,
        alternate_label,
    )


async def _load_release(
    pool: asyncpg.Pool, *, name: str, version: str, terms: list
) -> TerminologyImportResult:
    """Apply one release through the composer and return what it reports."""
    async with pool.acquire() as conn, conn.transaction():
        result = await import_terminology_release(
            conn,
            name=name,
            version=version,
            parsed_terms=terms,
            parsed_closure=[],
        )
    return result


async def _read_row_versions(pool: asyncpg.Pool, terminology_idx: int) -> dict[str, str]:
    """Return term_id -> the row's transaction stamp, which changes only when
    Postgres physically rewrites the row. Comparing two of these detects a
    write that stored the value a row already held."""
    rows = await pool.fetch(
        "SELECT term_id, xmin::text AS row_version FROM qiita.terminology_term"
        " WHERE terminology_idx = $1",
        terminology_idx,
    )
    return {row["term_id"]: row["row_version"] for row in rows}


# ---------------------------------------------------------------------------
# ParsedTerm
# ---------------------------------------------------------------------------


def test_ParsedTerm_settles_padded_and_empty_values():
    """Tests the case where a term is built from cells carrying padding or
    nothing at all: every text value arrives stripped, and one holding nothing
    arrives as None, so no reader has to spell that rule itself."""
    result = parsed_term(
        "  UBERON:0001  ",
        "  mouth  ",
        alternate_label="   ",
        replaced_by_term_id="",
    )

    expected = parsed_term("UBERON:0001", "mouth")
    assert result == expected


def test_ParsedTerm_settles_values_on_replace():
    """Tests the case where replace derives a row from another: it settles the
    new values like any others, since replace rebuilds the row through the
    constructor."""
    term = parsed_term("UBERON:0001", "mouth")

    result = replace(term, label="  molar  ", replaced_by_term_id="   ")

    expected = parsed_term("UBERON:0001", "molar")
    assert result == expected


# ---------------------------------------------------------------------------
# format_offenders / TerminologyImportAnomaly
# ---------------------------------------------------------------------------


def test_format_offenders_within_cap():
    """Tests the case where the offending values fit under the cap: they render
    as their own repr, so a message about a handful of rows reads exactly as it
    would with no cap in place."""
    values = ["UBERON:0001", "UBERON:0002"]

    result = format_offenders(values)

    assert result == "['UBERON:0001', 'UBERON:0002']"


def test_format_offenders_at_cap():
    """Tests the case where the offending values exactly fill the cap: the
    message summarizes nothing, because no value went unnamed."""
    values = [f"UBERON:{i:04d}" for i in range(MAX_REPORTED_OFFENDERS)]

    result = format_offenders(values)

    assert result == repr(values)


def test_format_offenders_over_cap():
    """Tests the case where more offending values arrive than the cap allows:
    the message states the total and names only the capped sample."""
    over_cap_count = MAX_REPORTED_OFFENDERS + 5
    values = [f"UBERON:{i:04d}" for i in range(over_cap_count)]

    result = format_offenders(values)

    named = values[:MAX_REPORTED_OFFENDERS]
    assert result == (f"{over_cap_count} total, first {MAX_REPORTED_OFFENDERS}: {named!r}")


def test_format_offenders_pairs():
    """Tests the case where the offending values are (term, target) pairs rather
    than single ids: each renders as the pair it is."""
    values = [("UBERON:0001", "UBERON:9999")]

    result = format_offenders(values)

    assert result == "[('UBERON:0001', 'UBERON:9999')]"


def test_TerminologyImportAnomaly_every_kind():
    """Tests the case where every anomaly kind is populated: one message names
    each, in the order the anomaly declares them."""
    exc = TerminologyImportAnomaly(
        silently_dropped_term_ids=["UBERON:0003"],
        unresolved_replaced_by=[("UBERON:0001", "UBERON:9999")],
        misaligned_replaced_by=[("UBERON:0002", "UBERON:0004")],
        unresolved_closure_endpoints=[("UBERON:0005", "UBERON:0006")],
    )

    assert str(exc) == (
        "silently_dropped_term_ids=['UBERON:0003'];"
        " unresolved_replaced_by=[('UBERON:0001', 'UBERON:9999')];"
        " misaligned_replaced_by=[('UBERON:0002', 'UBERON:0004')];"
        " unresolved_closure_endpoints=[('UBERON:0005', 'UBERON:0006')]"
    )


def test_TerminologyImportAnomaly_over_cap():
    """Tests the case where more term ids are dropped than the message names:
    the message states the total and a sample, while the attribute still carries
    every id so a caller can report on all of them."""
    over_cap_count = MAX_REPORTED_OFFENDERS + 3
    dropped = [f"UBERON:{i:04d}" for i in range(over_cap_count)]

    exc = TerminologyImportAnomaly(silently_dropped_term_ids=dropped)

    named = dropped[:MAX_REPORTED_OFFENDERS]
    assert str(exc) == (
        f"silently_dropped_term_ids={over_cap_count} total,"
        f" first {MAX_REPORTED_OFFENDERS}: {named!r}"
    )
    assert exc.silently_dropped_term_ids == dropped


# ---------------------------------------------------------------------------
# import_terminology_release
# ---------------------------------------------------------------------------


@pytest.mark.db
async def test_import_terminology_release_requires_transaction(postgres_pool):
    """Tests the case where the composer runs on a connection with no open
    transaction: the entry guard rejects the call before any write lands."""
    async with postgres_pool.acquire() as conn:
        with pytest.raises(RuntimeError, match="outside a transaction"):
            await import_terminology_release(
                conn,
                name="tr_no_transaction",
                version="1.0.0",
                parsed_terms=[parsed_term("TR:1", "one")],
                parsed_closure=[],
            )

    # The guard fires ahead of find-or-create, so no terminology row exists.
    assert await fetch_terminology_idx_by_name(postgres_pool, "tr_no_transaction") is None


@pytest.mark.db
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
                parsed_terms=[parsed_term("TR:1", "one")],
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


@pytest.mark.db
async def test_import_terminology_release_closure_tuple_with_unknown_term_id(
    postgres_pool, created_terminologies
):
    """Tests the case where a closure tuple names a term the batch never
    supplied: the inner JOINs drop that tuple, so the reported closure count
    is lower than the number of tuples handed in."""
    parsed_terms = [parsed_term("TR:1", "one"), parsed_term("TR:2", "two")]
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
        terms_alternate_label_updated=0,
        terms_newly_obsoleted=0,
        terms_newly_merged=0,
        terms_silently_dropped=0,
        closure_rows=2,
    )
    assert result == expected


@pytest.mark.db
async def test_import_terminology_release_analyzes_terms(postgres_pool, created_terminologies):
    """Tests the case where a release has just been applied: the planner's row
    estimate for the term table matches what it holds, so the planner does not
    size statements filtering on terminology_idx at a default selectivity.

    The estimate is exact at this size because ANALYZE reads every page of a
    small table; a table never analyzed reports -1 instead of a count.
    """
    parsed_terms = [parsed_term("AN:1", "one"), parsed_term("AN:2", "two")]
    parsed_closure = [("AN:1", "AN:1", 0), ("AN:1", "AN:2", 1)]

    async with postgres_pool.acquire() as conn, conn.transaction():
        result = await import_terminology_release(
            conn,
            name="an_analyze_terms",
            version="1.0.0",
            parsed_terms=parsed_terms,
            parsed_closure=parsed_closure,
        )
    created_terminologies.append(result.terminology_idx)

    # Table-wide, not scoped to this terminology: reltuples describes the whole
    # table, so the row count compared against it must too.
    estimate = await postgres_pool.fetchval(
        "SELECT c.reltuples FROM pg_class c"
        "  JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE n.nspname = 'qiita' AND c.relname = $1",
        _TERM_TABLE,
    )
    term_count = await postgres_pool.fetchval(f"SELECT count(*) FROM qiita.{_TERM_TABLE}")

    assert int(estimate) == term_count


@pytest.mark.db
async def test_import_terminology_release_alternate_label_only_change(
    postgres_pool, created_terminologies
):
    """Tests the case where a reload changes only a term's second name: the
    second-name counter moves while the label counter stays at zero, so the
    reported counts show that something changed."""
    first_load = await _load_release(
        postgres_pool,
        name="tr_alt_only",
        version="1.0.0",
        terms=[parsed_term("TR:1", "one", alternate_label="uno")],
    )
    created_terminologies.append(first_load.terminology_idx)

    second_load = await _load_release(
        postgres_pool,
        name="tr_alt_only",
        version="2.0.0",
        terms=[parsed_term("TR:1", "one", alternate_label="ein")],
    )

    expected = TerminologyImportResult(
        terminology_idx=first_load.terminology_idx,
        terms_inserted=0,
        terms_label_updated=0,
        terms_alternate_label_updated=1,
        terms_newly_obsoleted=0,
        terms_newly_merged=0,
        terms_silently_dropped=0,
        closure_rows=0,
    )
    assert second_load == expected


@pytest.mark.db
async def test_import_terminology_release_both_names_change(postgres_pool, created_terminologies):
    """Tests the case where a reload changes both of a term's names: the row
    counts once against each name's counter, rather than against only one."""
    first_load = await _load_release(
        postgres_pool,
        name="tr_both_names",
        version="1.0.0",
        terms=[parsed_term("TR:1", "one", alternate_label="uno")],
    )
    created_terminologies.append(first_load.terminology_idx)

    second_load = await _load_release(
        postgres_pool,
        name="tr_both_names",
        version="2.0.0",
        terms=[parsed_term("TR:1", "ONE", alternate_label="ein")],
    )

    expected = TerminologyImportResult(
        terminology_idx=first_load.terminology_idx,
        terms_inserted=0,
        terms_label_updated=1,
        terms_alternate_label_updated=1,
        terms_newly_obsoleted=0,
        terms_newly_merged=0,
        terms_silently_dropped=0,
        closure_rows=0,
    )
    assert second_load == expected


# ---------------------------------------------------------------------------
# update_terminology_status
# ---------------------------------------------------------------------------


@pytest.mark.db
async def test_update_terminology_status_not_found(postgres_pool):
    """Tests the case where the idx names no row: the conditional UPDATE
    matches nothing and returns None instead of raising, leaving the caller
    to distinguish absence from an illegal source state."""
    row = await update_terminology_status(
        postgres_pool,
        _ABSENT_TERMINOLOGY_IDX,
        TerminologyStatus.ACTIVE,
    )
    assert row is None


@pytest.mark.db
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


@pytest.mark.db
async def test_update_terminology_status_derives_sources(postgres_pool, created_terminologies):
    """Tests the case where the target is reachable from some state but not
    from this row's: FAILED is a source for LOADING only, so a FAILED row
    cannot go straight to ACTIVE."""
    terminology_idx = await seed_terminology(
        postgres_pool, name="tr_derived_sources", status=TerminologyStatus.FAILED
    )
    created_terminologies.append(terminology_idx)

    row = await update_terminology_status(
        postgres_pool,
        terminology_idx,
        TerminologyStatus.ACTIVE,
    )
    assert row is None

    promoted = await update_terminology_status(
        postgres_pool,
        terminology_idx,
        TerminologyStatus.LOADING,
    )
    expected_row = {
        "terminology_idx": terminology_idx,
        "name": "tr_derived_sources",
        "version": "1.0.0",
        "loaded_at": SEEDED_TERMINOLOGY_LOADED_AT,
        "status": TerminologyStatus.LOADING.value,
    }
    assert dict(promoted) == expected_row


# ---------------------------------------------------------------------------
# fetch_terminology / fetch_terminology_idx_by_name
# ---------------------------------------------------------------------------


@pytest.mark.db
async def test_fetch_terminology_not_found(postgres_pool):
    """Tests the case where the idx names no row: the read returns None."""
    assert await fetch_terminology(postgres_pool, _ABSENT_TERMINOLOGY_IDX) is None


@pytest.mark.db
async def test_fetch_terminology_idx_by_name_not_found(postgres_pool):
    """Tests the case where no terminology carries the given name: the read
    returns None rather than an empty record."""
    assert await fetch_terminology_idx_by_name(postgres_pool, "tr_no_such_name") is None


# ---------------------------------------------------------------------------
# terminology_term.alternate_label
# ---------------------------------------------------------------------------


@pytest.mark.db
async def test_terminology_term_alternate_label_column(postgres_pool):
    """Tests the case where the catalog reports the column's declared shape: a
    nullable VARCHAR(500), the same width as label because it holds a name
    rather than a definition."""
    row = await postgres_pool.fetchrow(
        "SELECT format_type(a.atttypid, a.atttypmod) AS type, a.attnotnull"
        "  FROM pg_attribute a"
        "  JOIN pg_class c ON c.oid = a.attrelid"
        "  JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE n.nspname = 'qiita'"
        "   AND c.relname = 'terminology_term'"
        "   AND a.attname = 'alternate_label'"
        "   AND NOT a.attisdropped"
    )
    expected = {"type": "character varying(500)", "attnotnull": False}
    assert dict(row) == expected


@pytest.mark.db
async def test_terminology_term_alternate_label_rejects_empty(postgres_pool, created_terminologies):
    """Tests the case where a write supplies an empty string: the CHECK
    rejects it, leaving NULL as the only spelling of absence."""
    terminology_idx = await seed_terminology(postgres_pool, name="tr_alt_empty")
    created_terminologies.append(terminology_idx)

    with pytest.raises(asyncpg.exceptions.CheckViolationError):
        await _insert_term(postgres_pool, terminology_idx, "TR:1", "one", "")


@pytest.mark.db
async def test_terminology_term_alternate_label_null_and_value(
    postgres_pool, created_terminologies
):
    """Tests the case where one term carries a second name and another does
    not: the database accepts both rows, and each reads back as written."""
    terminology_idx = await seed_terminology(postgres_pool, name="tr_alt_roundtrip")
    created_terminologies.append(terminology_idx)

    await _insert_term(postgres_pool, terminology_idx, "TR:1", "Homo sapiens", "human")
    await _insert_term(postgres_pool, terminology_idx, "TR:2", "Mus musculus", None)

    rows = await postgres_pool.fetch(
        "SELECT term_id, label, alternate_label FROM qiita.terminology_term"
        " WHERE terminology_idx = $1 ORDER BY term_id",
        terminology_idx,
    )
    expected = [
        {"term_id": "TR:1", "label": "Homo sapiens", "alternate_label": "human"},
        {"term_id": "TR:2", "label": "Mus musculus", "alternate_label": None},
    ]
    assert [dict(row) for row in rows] == expected


@pytest.mark.db
async def test_import_terminology_release_clears_unsupplied_alternate_label(
    postgres_pool, created_terminologies
):
    """Tests the case where a second name reached a row outside any load and
    the next release supplies none: the load clears the value.

    The release is authoritative for alternate_label, so the column is not a
    place to keep content the source does not carry — a value put there by hand
    survives only until the next load.
    """
    async with postgres_pool.acquire() as conn, conn.transaction():
        first = await import_terminology_release(
            conn,
            name="tr_alt_cleared",
            version="1.0.0",
            parsed_terms=[parsed_term("TR:1", "Homo sapiens")],
            parsed_closure=[],
        )
    created_terminologies.append(first.terminology_idx)

    await postgres_pool.execute(
        "UPDATE qiita.terminology_term SET alternate_label = $1"
        " WHERE terminology_idx = $2 AND term_id = $3",
        "human",
        first.terminology_idx,
        "TR:1",
    )

    async with postgres_pool.acquire() as conn, conn.transaction():
        await import_terminology_release(
            conn,
            name="tr_alt_cleared",
            version="2.0.0",
            parsed_terms=[parsed_term("TR:1", "Homo sapiens sapiens")],
            parsed_closure=[],
        )

    row = await postgres_pool.fetchrow(
        "SELECT label, alternate_label FROM qiita.terminology_term"
        " WHERE terminology_idx = $1 AND term_id = $2",
        first.terminology_idx,
        "TR:1",
    )
    expected = {"label": "Homo sapiens sapiens", "alternate_label": None}
    assert dict(row) == expected


# ---------------------------------------------------------------------------
# import_terminology_release — rewriting only what changed
# ---------------------------------------------------------------------------


@pytest.mark.db
async def test_import_terminology_release_unchanged_rows_are_not_rewritten(
    postgres_pool, created_terminologies
):
    """Tests the case where a release is applied twice with identical content:
    the second load rewrites no row.

    Postgres stores a new row version on every UPDATE without comparing the
    incoming values to the stored ones, so an upsert that assigns
    unconditionally leaves one dead tuple per term on a reload that changed
    nothing. At a few terms that is invisible; at a few million it is the
    dominant cost of the load.
    """
    terms = [
        parsed_term("TR:1", "one"),
        parsed_term("TR:2", "two", alternate_label="second"),
    ]
    load = await _load_release(postgres_pool, name="tr_no_op_reload", version="1.0.0", terms=terms)
    terminology_idx = load.terminology_idx
    created_terminologies.append(terminology_idx)
    before = await _read_row_versions(postgres_pool, terminology_idx)

    await _load_release(postgres_pool, name="tr_no_op_reload", version="2.0.0", terms=terms)

    after = await _read_row_versions(postgres_pool, terminology_idx)
    assert after == before


@pytest.mark.db
async def test_import_terminology_release_changed_row_is_rewritten(
    postgres_pool, created_terminologies
):
    """Tests the case where a reload changes one term's label and leaves
    another untouched: the load rewrites only the changed row, and it carries
    the new value."""
    load = await _load_release(
        postgres_pool,
        name="tr_partial_reload",
        version="1.0.0",
        terms=[parsed_term("TR:1", "one"), parsed_term("TR:2", "two")],
    )
    terminology_idx = load.terminology_idx
    created_terminologies.append(terminology_idx)
    before = await _read_row_versions(postgres_pool, terminology_idx)

    await _load_release(
        postgres_pool,
        name="tr_partial_reload",
        version="2.0.0",
        terms=[parsed_term("TR:1", "uno"), parsed_term("TR:2", "two")],
    )

    after = await _read_row_versions(postgres_pool, terminology_idx)
    assert after["TR:1"] != before["TR:1"]
    assert after["TR:2"] == before["TR:2"]

    rows = await postgres_pool.fetch(
        "SELECT term_id, label FROM qiita.terminology_term"
        " WHERE terminology_idx = $1 ORDER BY term_id",
        terminology_idx,
    )
    expected = [
        {"term_id": "TR:1", "label": "uno"},
        {"term_id": "TR:2", "label": "two"},
    ]
    assert [dict(row) for row in rows] == expected


@pytest.mark.db
async def test_import_terminology_release_withdrawn_replaced_by_is_cleared(
    postgres_pool, created_terminologies
):
    """Tests the case where a reload keeps a term obsolete but withdraws its
    replacement pointer: replaced_by returns to NULL.

    Every other column on the row is unchanged between the two releases, so a
    change-detecting upsert would skip this case. A later step populates the
    pointer rather than the upsert itself, so the row must be rewritten on the
    strength of its stored pointer alone.
    """
    merged_term = parsed_term(
        "TR:1",
        "one",
        is_obsolete=True,
        replaced_by_term_id="TR:2",
        obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_MERGED,
    )
    orphaned_term = parsed_term(
        "TR:1",
        "one",
        is_obsolete=True,
        obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_MERGED,
    )
    survivor = parsed_term("TR:2", "two")

    load = await _load_release(
        postgres_pool,
        name="tr_withdrawn_pointer",
        version="1.0.0",
        terms=[merged_term, survivor],
    )
    terminology_idx = load.terminology_idx
    created_terminologies.append(terminology_idx)

    await _load_release(
        postgres_pool,
        name="tr_withdrawn_pointer",
        version="2.0.0",
        terms=[orphaned_term, survivor],
    )

    row = await postgres_pool.fetchrow(
        "SELECT is_obsolete, replaced_by FROM qiita.terminology_term"
        " WHERE terminology_idx = $1 AND term_id = $2",
        terminology_idx,
        "TR:1",
    )
    expected = {"is_obsolete": True, "replaced_by": None}
    assert dict(row) == expected


# ---------------------------------------------------------------------------
# import_terminology_release — resolving a term the source does not name
# ---------------------------------------------------------------------------


@pytest.mark.db
async def test_import_terminology_release_unnamed_term_keeps_stored_names(
    postgres_pool, created_terminologies
):
    """Tests the case where a release merges away a term the database already
    holds and supplies no name for it: both stored names survive.

    A source that retires a term id often stops naming it, so the release
    carries nothing for either name column. An absent label is the source
    saying nothing about the term at all rather than asserting it has no
    second name, so neither stored name is the release's to clear. is_obsolete,
    obsoletion_kind, and the replaced_by pointer already record the merge — the
    names are the only part a reader cannot reconstruct.
    """
    load = await _load_release(
        postgres_pool,
        name="tr_unnamed_keeps_label",
        version="1.0.0",
        terms=[
            parsed_term("TR:1", "mouth", alternate_label="gob"),
            parsed_term("TR:2", "oral opening"),
        ],
    )
    terminology_idx = load.terminology_idx
    created_terminologies.append(terminology_idx)

    await _load_release(
        postgres_pool,
        name="tr_unnamed_keeps_label",
        version="2.0.0",
        terms=[
            parsed_term(
                "TR:1",
                None,
                is_obsolete=True,
                replaced_by_term_id="TR:2",
                obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_MERGED,
            ),
            parsed_term("TR:2", "oral opening"),
        ],
    )

    row = await postgres_pool.fetchrow(
        "SELECT tt.label, tt.alternate_label, tt.is_obsolete,"
        "       tt.obsoletion_kind::text AS obsoletion_kind,"
        "       survivor.term_id AS replaced_by_term_id"
        "  FROM qiita.terminology_term tt"
        "  LEFT JOIN qiita.terminology_term survivor ON survivor.idx = tt.replaced_by"
        " WHERE tt.terminology_idx = $1 AND tt.term_id = $2",
        terminology_idx,
        "TR:1",
    )
    expected = {
        "label": "mouth",
        "alternate_label": "gob",
        "is_obsolete": True,
        "obsoletion_kind": TerminologyTermObsoletionKind.SOURCE_MERGED.value,
        "replaced_by_term_id": "TR:2",
    }
    assert dict(row) == expected


@pytest.mark.db
async def test_import_terminology_release_unnamed_new_term_falls_back_to_term_id(
    postgres_pool, created_terminologies
):
    """Tests the case where no prior load holds a term the source does not
    name: its own term id becomes its label, since that is the only thing
    anyone knows about it and the column cannot be empty."""
    load = await _load_release(
        postgres_pool,
        name="tr_unnamed_new_term",
        version="1.0.0",
        terms=[
            parsed_term(
                "TR:1",
                None,
                is_obsolete=True,
                replaced_by_term_id="TR:2",
                obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_MERGED,
            ),
            parsed_term("TR:2", "survivor"),
            parsed_term(
                "TR:3",
                None,
                is_obsolete=True,
                obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_DEPRECATED,
            ),
        ],
    )
    terminology_idx = load.terminology_idx
    created_terminologies.append(terminology_idx)

    rows = await postgres_pool.fetch(
        "SELECT term_id, label FROM qiita.terminology_term"
        " WHERE terminology_idx = $1 ORDER BY term_id",
        terminology_idx,
    )
    expected = [
        {"term_id": "TR:1", "label": "TR:1"},
        {"term_id": "TR:2", "label": "survivor"},
        {"term_id": "TR:3", "label": "TR:3"},
    ]
    assert [dict(row) for row in rows] == expected


@pytest.mark.db
async def test_import_terminology_release_unnamed_term_is_not_a_label_update(
    postgres_pool, created_terminologies
):
    """Tests the case where a reload merges away a term it does not name: the
    carried-forward label is the one already stored, so the load reports no
    label change."""
    load = await _load_release(
        postgres_pool,
        name="tr_unnamed_no_count",
        version="1.0.0",
        terms=[parsed_term("TR:1", "mouth"), parsed_term("TR:2", "oral opening")],
    )
    terminology_idx = load.terminology_idx
    created_terminologies.append(terminology_idx)

    async with postgres_pool.acquire() as conn, conn.transaction():
        result = await import_terminology_release(
            conn,
            name="tr_unnamed_no_count",
            version="2.0.0",
            parsed_terms=[
                parsed_term(
                    "TR:1",
                    None,
                    is_obsolete=True,
                    replaced_by_term_id="TR:2",
                    obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_MERGED,
                ),
                parsed_term("TR:2", "oral opening"),
            ],
            parsed_closure=[],
        )

    assert result.terms_label_updated == 0
