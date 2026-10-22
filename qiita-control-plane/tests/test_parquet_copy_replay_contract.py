"""Contract test for DuckDB's `COPY ... TO` onto a path that already exists.

`mint-features` and `mint-annotation-features` write their map to a fixed path in
the ticket's workspace, so a replay of the same attempt writes it again. The replay
is safe because the second COPY replaces the first file, with no `OVERWRITE`
option, under the options these writes use.
"""

import duckdb
from qiita_common.parquet import PARQUET_OPTS


def test_copy_to_an_existing_parquet_replaces_it(tmp_path):
    out = tmp_path / "feature_map.parquet"
    conn = duckdb.connect()
    # The setting the mint writes run under; PARQUET_OPTS needs it.
    conn.execute("SET preserve_insertion_order=false")
    conn.execute(f"COPY (SELECT 1 AS a) TO '{out}' ({PARQUET_OPTS})")

    conn.execute(f"COPY (SELECT 2 AS a UNION ALL SELECT 3) TO '{out}' ({PARQUET_OPTS})")

    rows = conn.execute(f"SELECT a FROM read_parquet('{out}') ORDER BY a").fetchall()
    assert rows == [(2,), (3,)]
