"""Contract tests for the per-contig attribute sidecar reader.

`register_contig_attribute_table` builds SQL and executes it on a caller's
connection, so — following this package's convention for `analytic` — its
behaviour is pinned here rather than in either consumer's suite. Both writers of
`qiita.assembly_membership` go through it, and neither can observe a
misread: the values land in nullable columns nothing else cross-checks.
"""

import duckdb
import pytest

from qiita_common.assembly_constants import (
    CONTIG_ATTRIBUTE_COLUMNS,
    register_contig_attribute_table,
)

_HEADER = "\t".join(CONTIG_ATTRIBUTE_COLUMNS)


def _read(tmp_path, text: str):
    path = tmp_path / "contig_attributes.tsv"
    path.write_text(text)
    conn = duckdb.connect(":memory:")
    register_contig_attribute_table(conn, path)
    return conn.execute("SELECT * FROM contig_attribute ORDER BY contig_id").fetchall()


def _types(conn) -> list[tuple[str, str]]:
    return conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns"
        " WHERE table_name = 'contig_attribute' ORDER BY ordinal_position"
    ).fetchall()


def test_absent_file_registers_the_table_empty_with_the_declared_types(tmp_path):
    """A run assembled before the sidecar existed — reachable by a resumed
    ticket — must still leave both writers one statement shape."""
    conn = duckdb.connect(":memory:")
    register_contig_attribute_table(conn, tmp_path / "nothing.tsv")
    assert conn.execute("SELECT count(*) FROM contig_attribute").fetchone() == (0,)
    assert _types(conn) == [
        ("contig_id", "VARCHAR"),
        ("raw_name", "VARCHAR"),
        ("circularity", "VARCHAR"),
        ("depth", "DOUBLE"),
        ("mult", "DOUBLE"),
    ]


def test_empty_cells_read_as_null_doubles(tmp_path):
    """The shape the DEFAULT assembler always writes.

    hifiasm_meta emits `mult` empty on every row and `depth` empty for any
    S-line with no `dp:f` tag. Left to `auto_detect` an all-empty column lands
    as VARCHAR, so the two writers' output types would depend on which assembler
    ran; the declared types are what make that not so.
    """
    path = tmp_path / "contig_attributes.tsv"
    path.write_text(f"{_HEADER}\ns1\ts1\tyes\t29\t\ns2\ts2\tno\t\t\n")
    conn = duckdb.connect(":memory:")
    register_contig_attribute_table(conn, path)
    assert _types(conn)[3:] == [("depth", "DOUBLE"), ("mult", "DOUBLE")]
    assert conn.execute("SELECT * FROM contig_attribute ORDER BY contig_id").fetchall() == [
        ("s1", "s1", "yes", 29.0, None),
        ("s2", "s2", "no", None, None),
    ]
    # The control that makes the declared types load-bearing rather than
    # decorative: without them this column is VARCHAR.
    assert conn.execute(
        "SELECT typeof(mult) FROM read_csv(?, delim='\t', header=true, auto_detect=true) LIMIT 1",
        [str(path)],
    ).fetchone() == ("VARCHAR",)


def test_a_reordered_header_is_rejected_rather_than_silently_transposed(tmp_path):
    """`columns=` binds by POSITION and does not look at the header, so without
    the check this file reads with `raw_name` holding the circularity call and
    `circularity` holding the name — five plausible values, all misfiled."""
    swapped = ["contig_id", "circularity", "raw_name", "depth", "mult"]
    with pytest.raises(ValueError, match="header"):
        _read(tmp_path, "\t".join(swapped) + "\ns1\tyes\trawA\t29\t1.5\n")


def test_an_unrecognised_header_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="header"):
        _read(tmp_path, "a\tb\tc\td\te\ns1\trawA\tyes\t29\t1.5\n")


def test_the_expected_header_is_accepted(tmp_path):
    """The control for the two rejections above: this is the byte sequence both
    entrypoints write, and it must read."""
    assert _read(tmp_path, f"{_HEADER}\ns1\trawA\tyes\t29\t1.5\n") == [
        ("s1", "rawA", "yes", 29.0, 1.5)
    ]


def test_a_repeated_contig_id_is_rejected(tmp_path):
    """Both callers LEFT JOIN this table after grouping down to the membership
    key, so a second row for one contig re-multiplies a key that was just
    collapsed: `cardinality_violation` from the Postgres write, and a silently
    duplicated row in the DuckLake twin, which has no primary key to refuse it."""
    with pytest.raises(ValueError, match="repeats"):
        _read(tmp_path, f"{_HEADER}\ns1\tA\tyes\t1\t1\ns1\tB\tno\t2\t2\n")
