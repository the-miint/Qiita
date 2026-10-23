"""The Parquet option sets, and what each one makes DuckDB write.

The sets exist because their files have different lives, so these pin what tells them
apart — the codec, and whether a compression level is set — and that the pinned DuckDB
accepts each one in a real COPY.
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
    sorted_row_group_size,
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
        "i -> substr('ACGT', 1 + floor(random() * 4)::INT, 1)), '') AS s FROM range(20000)"
    )
    at_rest = tmp_path / "at_rest.parquet"
    transfer = tmp_path / "transfer.parquet"
    con.execute(f"COPY t TO '{at_rest}' ({PARQUET_OPTS_AT_REST})")
    con.execute(f"COPY t TO '{transfer}' ({PARQUET_OPTS_TRANSFER})")
    assert _disk_bytes(con, at_rest) < _disk_bytes(con, transfer)


def test_ducklake_catalog_level_matches_the_rust_one_exactly():
    # DuckLake's own writes (inserts, compaction) are data at rest too, so the data plane
    # sets the same level on the catalog. Rust cannot import the Python, so the value is
    # parsed out of the source; drift would leave compacted files at a different level
    # with nothing failing.
    import re

    src = (
        Path(__file__).resolve().parents[2] / "qiita-data-plane" / "src" / "ducklake.rs"
    ).read_text()
    m = re.search(r"const PARQUET_COMPRESSION_LEVEL_AT_REST: \w+ = (\d+);", src)
    assert m, "PARQUET_COMPRESSION_LEVEL_AT_REST not found in ducklake.rs"
    assert int(m.group(1)) == PARQUET_COMPRESSION_LEVEL_AT_REST


def test_data_plane_read_export_options_match_transfer_exactly():
    # The data plane writes a transient reads export with its own copy of the transfer
    # set; drift would change that file's shape with nothing failing.
    import re

    src = (
        Path(__file__).resolve().parents[2] / "qiita-data-plane" / "src" / "flight_service.rs"
    ).read_text()
    m = re.search(r'const EXPORT_READ_PARQUET_OPTS: &str =\s*"([^"]+)";', src)
    assert m, "EXPORT_READ_PARQUET_OPTS not found in flight_service.rs"
    assert m.group(1) == PARQUET_OPTS_TRANSFER


# A sorted COPY: what DuckDB does with its row groups, and the row count that keeps them
# under the cap.


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


def test_a_sorted_copy_ignores_the_byte_cap(tmp_path: Path):
    # The DuckDB behaviour sorted_row_group_size exists for. If a DuckDB bump makes this
    # fail, the byte cap may cut sorted writes again and the helper may be unneeded.
    con = _connect()
    out = _sorted_copy(con, tmp_path, "FORMAT PARQUET, ROW_GROUP_SIZE_BYTES '1MB'")
    assert [rows for rows, _, _ in _row_groups(con, out)] == [20000]


def test_a_sorted_copy_cuts_row_groups_on_whole_vectors_in_sort_order(tmp_path: Path):
    con = _connect()
    out = _sorted_copy(con, tmp_path, "FORMAT PARQUET, ROW_GROUP_SIZE 4097")
    groups = _row_groups(con, out)
    # 4,097 rows round up to three 2,048-row vectors.
    assert {rows for rows, _, _ in groups[:-1]} == {6144}
    # One sorted file: each row group a contiguous run of the key, in key order.
    assert all(hi - lo + 1 == rows for rows, lo, hi in groups)
    assert all(a[2] < b[1] for a, b in zip(groups, groups[1:]))


def test_sorted_row_group_size_takes_the_whole_vectors_under_the_cap():
    # 6,710 rows of 10 KB fit 64 MiB: three whole vectors.
    assert ROW_GROUP_SIZE_BYTES == 64 * 1024 * 1024
    assert sorted_row_group_size(10_000) == 3 * 2048


def test_sorted_row_group_size_is_at_least_one_vector():
    # 1,024 64 KB chunks fit the cap, but DuckDB writes no row group under one vector.
    assert sorted_row_group_size(64 * 1024) == 2048
    assert sorted_row_group_size(2 * ROW_GROUP_SIZE_BYTES) == 2048


def test_sorted_row_group_size_keeps_duckdbs_default_for_narrow_rows():
    assert sorted_row_group_size(8) == 122_880


def test_sorted_row_group_size_follows_the_cap(monkeypatch):
    monkeypatch.setattr(parquet, "ROW_GROUP_SIZE_BYTES", 1024 * 1024)
    assert sorted_row_group_size(100) == 5 * 2048


@pytest.mark.parametrize("bytes_per_row", [0, -1])
def test_sorted_row_group_size_rejects_a_width_that_is_not_positive(bytes_per_row: int):
    with pytest.raises(ValueError, match="bytes_per_row"):
        sorted_row_group_size(bytes_per_row)
