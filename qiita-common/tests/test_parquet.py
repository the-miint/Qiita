"""The Parquet option sets, and what each one makes DuckDB write.

The sets exist because their files have different lives, so these pin what tells them
apart — the codec, and whether a compression level is set — and that the pinned DuckDB
accepts each one in a real COPY. They also pin the data plane's Rust copies of these
values, and the DuckDB behaviour the at-rest writers rest on: that a sorted COPY honours
the byte cap, and what `'64MB'` means.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from qiita_common import parquet
from qiita_common.parquet import (
    PARQUET_COMPRESSION_LEVEL_AT_REST,
    PARQUET_OPTS_AT_REST,
    PARQUET_OPTS_INTERMEDIATE,
    PARQUET_OPTS_TRANSFER,
    ROW_GROUP_SIZE_BYTES,
)

_SHARED = ("FORMAT PARQUET", "PARQUET_VERSION 'v2'", "ROW_GROUP_SIZE_BYTES '64MB'")


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    # ROW_GROUP_SIZE_BYTES is rejected at bind time while insertion order is preserved.
    con.execute("SET preserve_insertion_order = false")
    return con


def _codecs(con: duckdb.DuckDBPyConnection, path: Path) -> set[str]:
    rows = con.execute(
        "SELECT DISTINCT compression FROM parquet_metadata(?)", [str(path)]
    ).fetchall()
    return {r[0] for r in rows}


def _disk_bytes(con: duckdb.DuckDBPyConnection, path: Path) -> int:
    return con.execute(
        "SELECT sum(total_compressed_size) FROM parquet_metadata(?)", [str(path)]
    ).fetchone()[0]


def test_at_rest_level_is_16():
    assert PARQUET_COMPRESSION_LEVEL_AT_REST == 16


def test_at_rest_is_zstd_at_the_at_rest_level():
    for part in _SHARED:
        assert part in PARQUET_OPTS_AT_REST
    assert "COMPRESSION 'zstd'" in PARQUET_OPTS_AT_REST
    assert f"COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL_AT_REST}" in PARQUET_OPTS_AT_REST


def test_transfer_is_zstd_at_the_default_level():
    for part in _SHARED:
        assert part in PARQUET_OPTS_TRANSFER
    assert "COMPRESSION 'zstd'" in PARQUET_OPTS_TRANSFER
    assert "COMPRESSION_LEVEL" not in PARQUET_OPTS_TRANSFER


def test_intermediate_is_snappy_with_no_level():
    for part in _SHARED:
        assert part in PARQUET_OPTS_INTERMEDIATE
    assert "COMPRESSION 'snappy'" in PARQUET_OPTS_INTERMEDIATE
    assert "COMPRESSION_LEVEL" not in PARQUET_OPTS_INTERMEDIATE


def test_no_option_set_without_a_purpose_in_its_name():
    # A bare name invites a writer to take it without deciding where its file ends up.
    assert not hasattr(parquet, "PARQUET_OPTS")


@pytest.mark.parametrize(
    ("opts", "codec"),
    [
        (PARQUET_OPTS_AT_REST, "ZSTD"),
        (PARQUET_OPTS_TRANSFER, "ZSTD"),
        (PARQUET_OPTS_INTERMEDIATE, "SNAPPY"),
    ],
    ids=["at_rest", "transfer", "intermediate"],
)
def test_duckdb_writes_each_set(tmp_path: Path, opts: str, codec: str):
    out = tmp_path / "x.parquet"
    con = _connect()
    con.execute(f"COPY (SELECT range AS i FROM range(1000)) TO '{out}' ({opts})")
    assert _codecs(con, out) == {codec}


def test_at_rest_level_reaches_the_writer(tmp_path: Path):
    # The footer records the codec but not the level, so compare sizes. On random bases
    # the higher level's better parse is worth ~10%, far outside any noise.
    con = _connect()
    con.execute(
        "CREATE TABLE t AS SELECT array_to_string(list_transform(range(150), "
        "lambda i: substr('ACGT', 1 + floor(random() * 4)::INT, 1)), '') AS s FROM range(20000)"
    )
    at_rest = tmp_path / "at_rest.parquet"
    transfer = tmp_path / "transfer.parquet"
    con.execute(f"COPY t TO '{at_rest}' ({PARQUET_OPTS_AT_REST})")
    con.execute(f"COPY t TO '{transfer}' ({PARQUET_OPTS_TRANSFER})")
    assert _disk_bytes(con, at_rest) < _disk_bytes(con, transfer)


def _data_plane_src(name: str) -> str:
    return (Path(__file__).resolve().parents[2] / "qiita-data-plane" / "src" / name).read_text()


def test_ducklake_catalog_options_match_the_at_rest_set_exactly():
    # DuckLake's own writes (inserts, compaction) are data at rest too, so the data plane
    # sets the at-rest codec, level and Parquet version on the catalog. The values are
    # parsed out of the Rust source; drift would leave DuckLake-written files in another
    # format with nothing failing.
    import re

    src = _data_plane_src("ducklake.rs")
    level = re.search(r"const PARQUET_COMPRESSION_LEVEL_AT_REST: \w+ = (\d+);", src)
    assert level, "PARQUET_COMPRESSION_LEVEL_AT_REST not found in ducklake.rs"
    assert int(level.group(1)) == PARQUET_COMPRESSION_LEVEL_AT_REST
    codec = re.search(r"set_option\('parquet_compression', '(\w+)'\)", src)
    assert codec, "parquet_compression set_option not found in ducklake.rs"
    assert f"COMPRESSION '{codec.group(1)}'" in PARQUET_OPTS_AT_REST
    version = re.search(r"set_option\('parquet_version', (\d+)\)", src)
    assert version, "parquet_version set_option not found in ducklake.rs"
    assert f"PARQUET_VERSION 'v{version.group(1)}'" in PARQUET_OPTS_AT_REST


def test_ducklake_chunk_row_group_size_matches_the_python_one_exactly():
    import re

    from qiita_common.chunking import CHUNK_ROW_GROUP_SIZE

    m = re.search(r"const CHUNK_ROW_GROUP_SIZE: \w+ = (\d+);", _data_plane_src("ducklake.rs"))
    assert m, "CHUNK_ROW_GROUP_SIZE not found in ducklake.rs"
    assert int(m.group(1)) == CHUNK_ROW_GROUP_SIZE


def test_data_plane_read_export_options_match_transfer_exactly():
    # The data plane writes a transient reads export with its own copy of the transfer
    # set; drift would change that file's shape with nothing failing.
    import re

    src = _data_plane_src("flight_service/export.rs")
    m = re.search(r'const EXPORT_READ_PARQUET_OPTS: &str =\s*"([^"]+)";', src)
    assert m, "EXPORT_READ_PARQUET_OPTS not found in flight_service/export.rs"
    assert m.group(1) == PARQUET_OPTS_TRANSFER


# A sorted COPY: what DuckDB does with its row groups.


def _sorted_copy(con: duckdb.DuckDBPyConnection, tmp_path: Path, opts: str) -> Path:
    # ~20 MB of ~1 KB rows, scanned from a source of many row groups so the sort runs on
    # several threads; the byte-cap behaviour below only shows with more than one.
    src = tmp_path / "src.parquet"
    con.execute(
        "COPY (SELECT range AS k, repeat('ACGT', 250) AS s FROM range(20000) ORDER BY random()) "
        f"TO '{src}' (FORMAT PARQUET, ROW_GROUP_SIZE 2048)"
    )
    out = tmp_path / "sorted.parquet"
    con.execute("SET threads = 4")
    con.execute(f"COPY (SELECT * FROM read_parquet('{src}') ORDER BY k) TO '{out}' ({opts})")
    return out


def _row_groups(con: duckdb.DuckDBPyConnection, path: Path) -> list[tuple[int, int, int]]:
    """(rows, min k, max k) of each row group, in file order."""
    return con.execute(
        "SELECT row_group_num_rows, stats_min_value::BIGINT, stats_max_value::BIGINT "
        "FROM parquet_metadata(?) WHERE path_in_schema = 'k' ORDER BY row_group_id",
        [str(path)],
    ).fetchall()


def test_a_sorted_copy_honours_the_byte_cap(tmp_path: Path):
    # If a bump makes this fail, DuckDB no longer applies the byte cap to a sorted COPY,
    # which the at-rest writers' sorted COPYs rely on (`qiita_common.parquet`).
    con = _connect()
    out = _sorted_copy(con, tmp_path, "FORMAT PARQUET, ROW_GROUP_SIZE_BYTES '1MB'")
    groups = _row_groups(con, out)
    # ~20 MB under a 1 MB cap, each group a contiguous run of the key, in key order.
    assert len(groups) >= 10
    assert all(hi - lo + 1 == rows for rows, lo, hi in groups)
    assert all(a[2] < b[1] for a, b in zip(groups, groups[1:]))


def test_the_byte_cap_int_is_what_duckdb_reads_the_literal_as(tmp_path: Path):
    # DuckDB's MB is decimal, so '8MB' cuts where 8,000,000 does and not where 2^23 does.
    # The two caps are ~4.9% apart, which on 100-byte rows is about two 2,048-row vectors.
    con = _connect()
    con.execute("SET threads = 1")

    def rows_per_group(cap: str) -> list[int]:
        out = tmp_path / "capped.parquet"
        con.execute(
            "COPY (SELECT repeat('A', 92) || lpad(range::VARCHAR, 8, '0') AS s "
            f"FROM range(200000)) TO '{out}' (FORMAT PARQUET, COMPRESSION 'uncompressed', "
            f"ROW_GROUP_SIZE_BYTES {cap})"
        )
        return [
            r[0]
            for r in con.execute(
                "SELECT row_group_num_rows FROM parquet_metadata(?) ORDER BY row_group_id",
                [str(out)],
            ).fetchall()
        ]

    assert rows_per_group("'8MB'") == rows_per_group("8000000")
    assert rows_per_group("'8MB'") != rows_per_group(str(8 * 1024 * 1024))
    assert ROW_GROUP_SIZE_BYTES == 64 * 1000 * 1000
    for opts in (PARQUET_OPTS_AT_REST, PARQUET_OPTS_TRANSFER, PARQUET_OPTS_INTERMEDIATE):
        assert "ROW_GROUP_SIZE_BYTES '64MB'" in opts
