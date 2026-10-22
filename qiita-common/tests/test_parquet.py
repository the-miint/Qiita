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
