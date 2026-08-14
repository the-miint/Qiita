"""What miint's `COPY … (FORMAT BIOM)` writer actually does, pinned against the real
extension.

Nothing else in this repo writes BIOM, so every claim the client-side feature-table
recipe makes about it starts out unverified — and three of this writer's behaviours
are decisions it makes silently on our behalf (it sums, it drops, it ignores). Each
test here corresponds to a line of `docs/duckdb-miint.md`'s BIOM entry and to a
decision somewhere in `qiita_common.feature_table`: if a future build changes one of
these, the claim that justified the decision fails here rather than in a published
file.

Written against mirror build `2b2841e`. The round trip goes through miint's own
`read_biom`, which returns `(sample_id, feature_id, value)` — the same triple the
writer takes. That is self-consistency, not independent validation; an independent
reader (`biom.load_table`) belongs to a tier that can carry the biom-format
dependency, and the file's conformance to BIOM 2.1 is upstream's claim, not ours.
"""

from __future__ import annotations

import duckdb
import pytest
from qiita_common import feature_table as ft

from qiita_control_plane.miint import connect_with_miint

# A relabelled table's three columns, with the types `LABELLED_SCHEMA` declares.
_TRIPLES = "sample_id, feature_id, value"


def _staged(conn, rows: list[tuple[str, str, float]]) -> None:
    """Materialize `rows` as the relation the writers copy, typed from
    `LABELLED_SCHEMA` — every one of those types is one the writer checks."""
    if not rows:
        casts = ", ".join(f"CAST(NULL AS {t}) AS {n}" for n, t in ft.LABELLED_SCHEMA.items())
        conn.execute(f"CREATE TABLE {ft.LABELLED_RELATION} AS SELECT {casts} WHERE false")
        return
    values = ", ".join("(?::VARCHAR, ?::VARCHAR, ?::DOUBLE)" for _ in rows)
    conn.execute(
        f"CREATE TABLE {ft.LABELLED_RELATION} AS SELECT * FROM (VALUES {values}) AS v({_TRIPLES})",
        [x for r in rows for x in r],
    )


def _write_and_read(rows, target) -> list[tuple]:
    """Write `rows` to `target` as BIOM through the shared builder, then read it back."""
    with connect_with_miint() as conn:
        _staged(conn, rows)
        conn.execute(ft.biom_copy_sql(target))
        return sorted(conn.execute(f"SELECT * FROM read_biom('{target}')").fetchall())


def test_a_relabelled_table_round_trips(tmp_path):
    """The baseline: what the writer takes is what the reader gives back, so nothing
    below is confused by an encoding surprise."""
    rows = [("QM1", "GCF_1", 1.0), ("QM1", "GCF_2", 2.5), ("QM2", "GCF_1", 3.0)]
    assert _write_and_read(rows, tmp_path / "t.biom") == sorted(rows)


def test_duplicate_sample_feature_pairs_are_SUMMED(tmp_path):
    """The behaviour `check_relabel_diagnostics` exists for. Two genomes relabelled to
    one `source_id` produce two rows for one `(sample_id, feature_id)` pair, and this
    is what happens to them: they are added together, with nothing in the file saying
    so. Refusing the collision before the write is the only place it can be caught.
    """
    rows = [("QM1", "GCF_1", 1.0), ("QM1", "GCF_1", 2.0)]
    assert _write_and_read(rows, tmp_path / "dup.biom") == [("QM1", "GCF_1", 3.0)]


def test_zero_values_are_dropped(tmp_path):
    """BIOM is sparse, so a zero is an absent entry rather than a stored 0.0 — the
    analytic never emits one (woltka counts what aligned), but a reader comparing a
    Parquet against its BIOM would otherwise see a row count mismatch and no reason
    for it."""
    rows = [("QM1", "GCF_1", 0.0), ("QM1", "GCF_2", 1.0)]
    assert _write_and_read(rows, tmp_path / "zero.biom") == [("QM1", "GCF_2", 1.0)]


def test_an_empty_table_writes_a_file_that_reads_back_empty(tmp_path):
    """An empty cohort is a legitimate result — every genome dropped by the coverage
    threshold, or an all-16S reference — and it must produce a real, readable artifact
    rather than an error or an absent file, which is what lets the empty path travel
    the same writer as a populated one."""
    target = tmp_path / "empty.biom"
    with connect_with_miint() as conn:
        _staged(conn, [])
        conn.execute(ft.biom_copy_sql(target))
        assert conn.execute(f"SELECT count(*) FROM read_biom('{target}')").fetchone()[0] == 0
    assert target.stat().st_size > 0


def test_the_writer_refuses_to_overwrite_an_existing_file(tmp_path):
    """Unlike the Parquet COPY, which replaces silently. This is why the bundle writer
    clears a stale `.partial` before writing: one left by a killed process would
    otherwise block every retry with this error, naming a path the user never chose.
    """
    target = tmp_path / "twice.biom"
    with connect_with_miint() as conn:
        _staged(conn, [("QM1", "GCF_1", 1.0)])
        conn.execute(ft.biom_copy_sql(target))
        with pytest.raises(duckdb.IOException, match="Cannot overwrite existing file"):
            conn.execute(ft.biom_copy_sql(target))


def test_the_parquet_copy_DOES_overwrite_silently(tmp_path):
    """The asymmetry stated as a test, because the bundle writer's own
    refuse-if-it-exists check is what makes the two formats behave the same for a
    user: without it, a second run would replace a Parquet and fail on a BIOM."""
    target = tmp_path / "t.parquet"
    with connect_with_miint() as conn:
        _staged(conn, [("QM1", "GCF_1", 1.0), ("QM2", "GCF_1", 2.0)])
        conn.execute("SET preserve_insertion_order=false")
        conn.execute(ft.parquet_copy_sql(target))
        conn.execute(f"DELETE FROM {ft.LABELLED_RELATION} WHERE sample_id = 'QM2'")
        conn.execute(ft.parquet_copy_sql(target))
        assert conn.execute(f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0] == 1


@pytest.mark.parametrize(
    ("sample_id", "expected"),
    [("NULL::VARCHAR", "NULL values not allowed"), ("''::VARCHAR", "empty sample_id")],
)
def test_an_unusable_identifier_is_refused_and_leaves_no_file(sample_id, expected, tmp_path):
    """A NULL id is the fault `check_relabel_diagnostics`' unlabelled checks catch
    earlier — the writer would catch it too, but only after the analytic has run, and
    only for BIOM (the Parquet writer would record the NULL id happily). An empty
    string is the writer catching what neither our schema nor those checks constrain.

    Both are asserted to leave **no file**, not merely to raise: the bundle's
    all-or-nothing commit assumes a failed write leaves nothing at the target it was
    given, and a stub file at a `.partial` would block the next attempt.
    """
    target = tmp_path / "unusable.biom"
    with connect_with_miint() as conn:
        conn.execute(
            f"CREATE TABLE {ft.LABELLED_RELATION} AS SELECT * FROM (VALUES "
            f"({sample_id}, 'GCF_1'::VARCHAR, 1.0::DOUBLE)) AS v({_TRIPLES})"
        )
        with pytest.raises(duckdb.InvalidInputException, match=expected):
            conn.execute(ft.biom_copy_sql(target))
    assert not target.exists()


@pytest.mark.parametrize("sql_type", ["BIGINT", "FLOAT", "DECIMAL(10,2)"])
def test_the_value_column_must_be_DOUBLE_exactly(sql_type, tmp_path):
    """Not merely numeric: FLOAT and DECIMAL are refused as firmly as BIGINT. This is
    what `LABELLED_SCHEMA`'s declared DOUBLE is for — an unquoted decimal literal in a
    hand-written VALUES list types as DECIMAL and would fail here, one step after the
    analytic that produced the numbers."""
    with connect_with_miint() as conn:
        conn.execute(
            f"CREATE TABLE {ft.LABELLED_RELATION} AS SELECT 'QM1' AS sample_id, "
            f"'GCF_1' AS feature_id, 1::{sql_type} AS value"
        )
        with pytest.raises(duckdb.BinderException, match="'value' must be DOUBLE"):
            conn.execute(ft.biom_copy_sql(tmp_path / "typed.biom"))


def test_a_missing_required_column_names_itself(tmp_path):
    """A BinderException before any work, naming the column — so a projection that
    dropped one is a bind error rather than a partial file."""
    with connect_with_miint() as conn:
        conn.execute(
            f"CREATE TABLE {ft.LABELLED_RELATION} AS "
            f"SELECT 'QM1' AS sample_id, 1.0::DOUBLE AS value"
        )
        with pytest.raises(duckdb.BinderException, match="requires 'feature_id' column"):
            conn.execute(ft.biom_copy_sql(tmp_path / "missing.biom"))


def test_an_EXTRA_column_is_silently_ignored(tmp_path):
    """Why the relabel's projection is not tidiness: a relation
    still carrying `genome_idx` writes a perfectly valid BIOM, with our internal
    identifier simply dropped on the floor. The writer would not have told anyone.
    """
    target = tmp_path / "extra.biom"
    with connect_with_miint() as conn:
        conn.execute(
            f"CREATE TABLE {ft.LABELLED_RELATION} AS SELECT * FROM (VALUES "
            f"('QM1'::VARCHAR, 'GCF_1'::VARCHAR, 1.0::DOUBLE, 400::BIGINT)) "
            f"AS v({_TRIPLES}, genome_idx)"
        )
        conn.execute(ft.biom_copy_sql(target))
        assert conn.execute(f"SELECT * FROM read_biom('{target}')").fetchall() == [
            ("QM1", "GCF_1", 1.0)
        ]


def test_an_unknown_copy_option_is_rejected_not_ignored(tmp_path):
    """So a typo in the writer's option list cannot silently no-op — which is what
    makes the explicit `COMPRESSION 'gzip'` in `biom_copy_sql` worth trusting."""
    with connect_with_miint() as conn:
        _staged(conn, [("QM1", "GCF_1", 1.0)])
        with pytest.raises(duckdb.BinderException, match="Unknown option"):
            conn.execute(
                f"COPY {ft.LABELLED_RELATION} TO '{tmp_path / 'opt.biom'}' "
                f"(FORMAT BIOM, ROW_GROUP_SIZE_BYTES '64MB')"
            )
