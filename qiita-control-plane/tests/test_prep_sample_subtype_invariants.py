"""Tests for the disjoint-subtype contract on prep_sample.

Each value in qiita.processing_kind must map to exactly one subtype
table. Each subtype table must FK to qiita.prep_sample via the composite
(prep_sample_idx, processing_kind) key, must carry a processing_kind
column pinned to a literal via GENERATED ALWAYS AS STORED, must have
UNIQUE (prep_sample_idx) for 1:1 cardinality, and must have NOT NULL on
prep_sample_idx. Catches regressions where a new subtype is added with
a single-column FK or with a non-pinned processing_kind, either of
which would silently break the disjointness guarantee.
"""

import pytest

pytestmark = pytest.mark.db


# Map of qiita.processing_kind enum value -> subtype table name. Adding a
# new subtype: extend the qiita.processing_kind enum, create the subtype
# table following the composite-FK + GENERATED ALWAYS AS pattern, and add
# its (enum value, table name) pair here. The tests below then enforce
# the disjoint-subtype contract on every entry.
EXPECTED_SUBTYPE_BY_KIND = {
    "sequenced": "sequenced_sample",
}


# ---------------------------------------------------------------------------
# Enum / subtype-table coverage
# ---------------------------------------------------------------------------


async def test_processing_kind_enum_matches_expected_subtypes(postgres_pool):
    # Every enum value must have a registered subtype table; conversely,
    # every registered subtype must correspond to a live enum value.
    rows = await postgres_pool.fetch(
        "SELECT enumlabel"
        "  FROM pg_enum e"
        "  JOIN pg_type t ON t.oid = e.enumtypid"
        "  JOIN pg_namespace n ON n.oid = t.typnamespace"
        " WHERE n.nspname = 'qiita' AND t.typname = 'processing_kind'"
        " ORDER BY e.enumsortorder"
    )
    enum_values = {r["enumlabel"] for r in rows}
    assert enum_values == set(EXPECTED_SUBTYPE_BY_KIND), (
        f"qiita.processing_kind enum {enum_values} does not match "
        f"EXPECTED_SUBTYPE_BY_KIND {set(EXPECTED_SUBTYPE_BY_KIND)}"
    )


# ---------------------------------------------------------------------------
# Parent-side: prep_sample must expose the composite-FK target
# ---------------------------------------------------------------------------


async def test_prep_sample_has_composite_unique_target(postgres_pool):
    # The composite (idx, processing_kind) tuple has to be UNIQUE on the
    # parent so child tables can FK against it.
    rows = await postgres_pool.fetch(
        "SELECT array_agg(a.attname ORDER BY u.ord) AS cols"
        "  FROM pg_constraint c"
        "  JOIN pg_class t ON t.oid = c.conrelid"
        "  JOIN pg_namespace n ON n.oid = t.relnamespace"
        "  CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY u(attnum, ord)"
        "  JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = u.attnum"
        " WHERE c.contype = 'u'"
        "   AND n.nspname = 'qiita'"
        "   AND t.relname = 'prep_sample'"
        " GROUP BY c.oid"
    )
    constraints = [tuple(r["cols"]) for r in rows]
    assert ("idx", "processing_kind") in constraints, (
        "qiita.prep_sample missing UNIQUE (idx, processing_kind) — required "
        "as the FK target for subtype tables"
    )


# ---------------------------------------------------------------------------
# Per-subtype contract
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("table", list(EXPECTED_SUBTYPE_BY_KIND.values()))
async def test_subtype_has_composite_fk_to_prep_sample(postgres_pool, table):
    # A single-column FK to prep_sample(idx) would silently let this row
    # attach to a parent whose processing_kind doesn't match. The FK must
    # be composite to enforce disjointness.
    rows = await postgres_pool.fetch(
        "SELECT array_agg(a.attname ORDER BY u.ord) AS child_cols,"
        "       array_agg(pa.attname ORDER BY u.ord) AS parent_cols"
        "  FROM pg_constraint c"
        "  JOIN pg_class ct ON ct.oid = c.conrelid"
        "  JOIN pg_namespace cn ON cn.oid = ct.relnamespace"
        "  JOIN pg_class pt ON pt.oid = c.confrelid"
        "  JOIN pg_namespace pn ON pn.oid = pt.relnamespace"
        "  CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY u(attnum, ord)"
        "  JOIN pg_attribute a ON a.attrelid = ct.oid AND a.attnum = u.attnum"
        "  JOIN pg_attribute pa ON pa.attrelid = pt.oid"
        "                      AND pa.attnum = c.confkey[u.ord]"
        " WHERE c.contype = 'f'"
        "   AND cn.nspname = 'qiita'"
        "   AND ct.relname = $1"
        "   AND pn.nspname = 'qiita'"
        "   AND pt.relname = 'prep_sample'"
        " GROUP BY c.oid",
        table,
    )
    fks = [(tuple(r["child_cols"]), tuple(r["parent_cols"])) for r in rows]
    expected = (
        ("prep_sample_idx", "processing_kind"),
        ("idx", "processing_kind"),
    )
    assert expected in fks, (
        f"qiita.{table} must FK (prep_sample_idx, processing_kind) -> "
        f"prep_sample (idx, processing_kind); found {fks}"
    )


@pytest.mark.parametrize(
    "kind,table",
    list(EXPECTED_SUBTYPE_BY_KIND.items()),
)
async def test_subtype_processing_kind_is_generated_literal(postgres_pool, kind, table):
    # Without GENERATED ALWAYS AS pinning the kind to a literal, a caller
    # could insert a row whose processing_kind matches a parent of a
    # different intended subtype, breaking disjointness through the FK.
    row = await postgres_pool.fetchrow(
        "SELECT a.attgenerated,"
        "       pg_get_expr(d.adbin, d.adrelid) AS gen_expr"
        "  FROM pg_attribute a"
        "  JOIN pg_class t ON t.oid = a.attrelid"
        "  JOIN pg_namespace n ON n.oid = t.relnamespace"
        "  LEFT JOIN pg_attrdef d ON d.adrelid = t.oid AND d.adnum = a.attnum"
        " WHERE n.nspname = 'qiita'"
        "   AND t.relname = $1"
        "   AND a.attname = 'processing_kind'"
        "   AND NOT a.attisdropped",
        table,
    )
    assert row is not None, f"qiita.{table} missing processing_kind column"
    # pg_attribute.attgenerated is a Postgres "char" — asyncpg returns it as
    # a single-byte bytes value ('s' for STORED, '' for non-generated).
    assert row["attgenerated"] == b"s", (
        f"qiita.{table}.processing_kind must be GENERATED ALWAYS AS ... STORED "
        f"(attgenerated={row['attgenerated']!r})"
    )
    # The generated expression must be a literal cast to the enum
    # specifically equal to the kind value this subtype claims. Postgres
    # normalizes the stored expression to drop the schema qualifier when
    # 'qiita' is on the search path, so we match the unqualified form.
    expected_fragment = f"'{kind}'::processing_kind"
    assert expected_fragment in (row["gen_expr"] or ""), (
        f"qiita.{table}.processing_kind generated expression "
        f"{row['gen_expr']!r} does not pin to {expected_fragment!r}"
    )


@pytest.mark.parametrize("table", list(EXPECTED_SUBTYPE_BY_KIND.values()))
async def test_subtype_has_unique_prep_sample_idx(postgres_pool, table):
    # 1:1 cardinality — at most one subtype row per prep_sample.
    rows = await postgres_pool.fetch(
        "SELECT array_agg(a.attname ORDER BY u.ord) AS cols"
        "  FROM pg_constraint c"
        "  JOIN pg_class t ON t.oid = c.conrelid"
        "  JOIN pg_namespace n ON n.oid = t.relnamespace"
        "  CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY u(attnum, ord)"
        "  JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = u.attnum"
        " WHERE c.contype = 'u'"
        "   AND n.nspname = 'qiita'"
        "   AND t.relname = $1"
        " GROUP BY c.oid",
        table,
    )
    constraints = [tuple(r["cols"]) for r in rows]
    assert ("prep_sample_idx",) in constraints, (
        f"qiita.{table} missing UNIQUE (prep_sample_idx) — required for 1:1 with prep_sample"
    )


@pytest.mark.parametrize("table", list(EXPECTED_SUBTYPE_BY_KIND.values()))
async def test_subtype_prep_sample_idx_not_null(postgres_pool, table):
    # No orphan subtype rows — every subtype row must reference some
    # prep_sample, even though the FK alone would not enforce non-null.
    not_null = await postgres_pool.fetchval(
        "SELECT a.attnotnull"
        "  FROM pg_attribute a"
        "  JOIN pg_class t ON t.oid = a.attrelid"
        "  JOIN pg_namespace n ON n.oid = t.relnamespace"
        " WHERE n.nspname = 'qiita'"
        "   AND t.relname = $1"
        "   AND a.attname = 'prep_sample_idx'"
        "   AND NOT a.attisdropped",
        table,
    )
    assert not_null is True, f"qiita.{table}.prep_sample_idx must be NOT NULL"
