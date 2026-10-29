"""Tests for the shared myloasm provenance transform (`jobs/_myloasm_provenance.py`).

The input is the raw dump the assemble step writes (one Parquet per miint reader,
myloasm's own names), built here from the committed myloasm 0.6.0 and 0.7.0 fixture.
The output is the six staging Parquets register-files loads into the `myloasm_*`
lake tables. Both read-name resolutions are covered: names that already are
`sequence_idx` (a run whose assembler input was named that way), and names looked
up through a `(read_id, sequence_idx)` table (an earlier run's tree).
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from helpers import (
    MYLOASM_FIRST_SEQUENCE_IDX,
    MYLOASM_FIXTURE_VERSIONS,
    myloasm_raw_dir,
    myloasm_read_name_map,
)
from qiita_common.myloasm_constants import MYLOASM_TABLES

from qiita_compute_orchestrator.jobs._myloasm_provenance import (
    CONTIG_FEATURE_TABLE,
    READ_NAME_TABLE,
    read_names_from_sequence_idx,
    register_raw_tables,
    write_myloasm_staging,
)

_PREP_SAMPLE = 41
_RUN = 77
# The fixture's three primary contigs, given feature_idx values the way the run's
# assembly_hash -> mint-features would.
_PRIMARY_FEATURES = {"u491ctg": 1001, "u912ctg": 1002, "u982ctg": 1003}
_ROWS = {
    "myloasm_contig": 5,
    "myloasm_layout": 173,
    "myloasm_mapping": 1085,
    "myloasm_link": 2,
    "myloasm_split": 40,
    "myloasm_run": 1,
}


def _contig_feature(conn: duckdb.DuckDBPyConnection, features: dict[str, int]) -> None:
    conn.execute(
        f"CREATE TEMP TABLE {CONTIG_FEATURE_TABLE} (contig_id VARCHAR, feature_idx BIGINT)"
    )
    if features:
        conn.executemany(
            f"INSERT INTO {CONTIG_FEATURE_TABLE} VALUES (?, ?)", list(features.items())
        )


def _stage(
    tmp_path: Path,
    raw: Path,
    *,
    lookup: list[tuple[str, int]] | None = None,
    features: dict[str, int] = _PRIMARY_FEATURES,
) -> dict[str, Path]:
    staging = tmp_path / "staging"
    staging.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as conn:
        register_raw_tables(conn, raw)
        if lookup is None:
            read_names_from_sequence_idx(conn)
        else:
            conn.execute(
                f"CREATE TEMP TABLE {READ_NAME_TABLE} (read_id VARCHAR, sequence_idx BIGINT)"
            )
            conn.executemany(f"INSERT INTO {READ_NAME_TABLE} VALUES (?, ?)", lookup)
        _contig_feature(conn, features)
        return write_myloasm_staging(
            conn, staging_dir=staging, prep_sample_idx=_PREP_SAMPLE, processing_idx=_RUN
        )


def _q(sql: str) -> list[tuple]:
    with duckdb.connect() as conn:
        return conn.execute(sql).fetchall()


@pytest.mark.parametrize("version", MYLOASM_FIXTURE_VERSIONS)
def test_sequence_idx_named_reads_stage_all_six_tables_in_the_lake_shape(
    tmp_path: Path, version: str
) -> None:
    raw = myloasm_raw_dir(tmp_path / "in", version, sequence_idx_names=True)
    written = _stage(tmp_path, raw)

    assert set(written) == set(MYLOASM_TABLES)
    for table, spec in MYLOASM_TABLES.items():
        path = written[table]
        assert path == tmp_path / "staging" / f"{table}.parquet"
        assert [r[:2] for r in _q(f"DESCRIBE FROM '{path}'")] == list(spec.columns), table
        assert _q(f"SELECT count(*) FROM '{path}'") == [(_ROWS[table],)], table
        assert _q(f"SELECT DISTINCT prep_sample_idx, processing_idx FROM '{path}'") == [
            (_PREP_SAMPLE, _RUN)
        ], table


def test_contigs_carry_contig_num_feature_idx_and_their_secondary(tmp_path: Path) -> None:
    raw = myloasm_raw_dir(tmp_path / "in", "0.7.0", sequence_idx_names=True)
    contig = _stage(tmp_path, raw)["myloasm_contig"]
    assert _q(
        "SELECT contig_num, dereplication_status, feature_idx, secondary_to_contig_num "
        f"FROM '{contig}' ORDER BY contig_num"
    ) == [
        (491, "primary", 1001, None),
        (826, "alternate", None, 491),
        (840, "duplicated", None, 982),
        (912, "primary", 1002, None),
        (982, "primary", 1003, None),
    ]


def test_reads_become_sequence_idx_everywhere_they_appear(tmp_path: Path) -> None:
    raw = myloasm_raw_dir(tmp_path / "in", "0.7.0", sequence_idx_names=True)
    written = _stage(tmp_path, raw)
    for table, column in [
        ("myloasm_mapping", "sequence_idx"),
        ("myloasm_layout", "sequence_idx"),
        ("myloasm_split", "sequence_idx"),
        ("myloasm_link", "from_sequence_idx"),
        ("myloasm_link", "to_sequence_idx"),
    ]:
        ((low, nulls),) = _q(
            f"SELECT min({column}), count(*) - count({column}) FROM '{written[table]}'"
        )
        assert low >= MYLOASM_FIRST_SEQUENCE_IDX, (table, column)
        assert nulls == 0, (table, column)


def test_a_looked_up_name_lands_on_the_same_sequence_idx(tmp_path: Path) -> None:
    """An earlier run's tree names reads by read_id; resolved through a lookup it
    stages exactly what the same run named by sequence_idx stages."""
    named = myloasm_raw_dir(tmp_path / "a", "0.7.0", sequence_idx_names=True)
    by_id = myloasm_raw_dir(tmp_path / "b", "0.7.0", sequence_idx_names=False)
    expected = _stage(tmp_path / "x", named)
    looked_up = _stage(tmp_path / "y", by_id, lookup=myloasm_read_name_map(by_id))
    for table in MYLOASM_TABLES:
        assert _q(f"FROM '{expected[table]}' EXCEPT FROM '{looked_up[table]}'") == [], table
        assert _q(f"FROM '{looked_up[table]}' EXCEPT FROM '{expected[table]}'") == [], table


def test_0_6_0_links_keep_null_evidence(tmp_path: Path) -> None:
    raw = myloasm_raw_dir(tmp_path / "in", "0.6.0", sequence_idx_names=True)
    link = _stage(tmp_path, raw)["myloasm_link"]
    assert _q(
        "SELECT from_contig_num, to_contig_num, overlap_length, from_sequence_idx "
        f"FROM '{link}' ORDER BY 1"
    ) == [(840, 840, None, None), (982, 982, None, None)]


def test_the_run_row_keeps_only_the_log_name(tmp_path: Path) -> None:
    raw = myloasm_raw_dir(tmp_path / "in", "0.6.0", sequence_idx_names=True)
    run = _stage(tmp_path, raw)["myloasm_run"]
    assert _q(f"SELECT log_name, myloasm_version FROM '{run}'") == [("myloasm.log", "0.6.0")]


# --- what fails the step --------------------------------------------------------


def test_a_read_name_that_is_not_a_sequence_idx_fails(tmp_path: Path) -> None:
    """A tree whose reads still carry their sequencer names, handed to the
    sequence_idx resolution, is refused rather than loaded with NULL reads."""
    raw = myloasm_raw_dir(tmp_path / "in", "0.7.0", sequence_idx_names=False)
    with pytest.raises(ValueError, match="not a sequence_idx"):
        _stage(tmp_path, raw)


def test_a_read_the_lookup_does_not_cover_fails(tmp_path: Path) -> None:
    raw = myloasm_raw_dir(tmp_path / "in", "0.7.0", sequence_idx_names=False)
    with pytest.raises(ValueError, match="no sequence_idx"):
        _stage(tmp_path, raw, lookup=myloasm_read_name_map(raw)[1:])


def test_a_primary_contig_without_a_feature_fails(tmp_path: Path) -> None:
    raw = myloasm_raw_dir(tmp_path / "in", "0.7.0", sequence_idx_names=True)
    features = {k: v for k, v in _PRIMARY_FEATURES.items() if k != "u912ctg"}
    with pytest.raises(ValueError, match="u912ctg"):
        _stage(tmp_path, raw, features=features)


def test_a_feature_for_a_contig_that_is_not_primary_fails(tmp_path: Path) -> None:
    """Only primary contigs are hashed; a feature naming an alternate, or a contig
    the run does not have, means the feature map came from a different assembly."""
    raw = myloasm_raw_dir(tmp_path / "in", "0.7.0", sequence_idx_names=True)
    for stray in ("u826ctg", "u5ctg"):
        with pytest.raises(ValueError, match=stray):
            _stage(tmp_path / stray, raw, features={**_PRIMARY_FEATURES, stray: 9})


def test_a_contig_with_two_features_fails(tmp_path: Path) -> None:
    raw = myloasm_raw_dir(tmp_path / "in", "0.7.0", sequence_idx_names=True)
    staging = tmp_path / "staging"
    staging.mkdir()
    with duckdb.connect() as conn:
        register_raw_tables(conn, raw)
        read_names_from_sequence_idx(conn)
        _contig_feature(conn, _PRIMARY_FEATURES)
        conn.execute(f"INSERT INTO {CONTIG_FEATURE_TABLE} VALUES ('u491ctg', 2001)")
        with pytest.raises(ValueError, match="u491ctg"):
            write_myloasm_staging(
                conn, staging_dir=staging, prep_sample_idx=_PREP_SAMPLE, processing_idx=_RUN
            )


def _rewrite(path: Path, select: str) -> None:
    with duckdb.connect() as conn:
        conn.execute(
            f"COPY ({select.format(src=repr(str(path)))}) TO '{path}.tmp' (FORMAT PARQUET)"
        )
    Path(f"{path}.tmp").replace(path)


def test_two_contigs_with_one_contig_num_fail(tmp_path: Path) -> None:
    """`contig_num` identifies a contig within a run by construction, not by
    myloasm's contract (`qiita_common.myloasm_constants`); a repeat is refused."""
    raw = myloasm_raw_dir(tmp_path / "in", "0.7.0", sequence_idx_names=True)
    _rewrite(
        raw / "myloasm_contig.parquet",
        "SELECT * REPLACE (CASE WHEN contig_id = 'u826ctg' THEN 'u491ctg' ELSE contig_id END "
        "AS contig_id) FROM {src}",
    )
    with pytest.raises(ValueError, match="491"):
        _stage(tmp_path, raw)


def test_a_contig_name_that_is_not_myloasms_fails(tmp_path: Path) -> None:
    raw = myloasm_raw_dir(tmp_path / "in", "0.7.0", sequence_idx_names=True)
    _rewrite(
        raw / "myloasm_mapping.parquet",
        "SELECT * REPLACE ('contig_7' AS contig_id) FROM {src}",
    )
    with pytest.raises(ValueError, match="contig_7"):
        _stage(tmp_path, raw)


def test_a_mapping_to_a_contig_the_run_lacks_fails(tmp_path: Path) -> None:
    raw = myloasm_raw_dir(tmp_path / "in", "0.7.0", sequence_idx_names=True)
    _rewrite(
        raw / "myloasm_mapping.parquet",
        "SELECT * REPLACE ('u5ctg' AS contig_id) FROM {src}",
    )
    with pytest.raises(ValueError, match="u5ctg"):
        _stage(tmp_path, raw)


def test_a_missing_raw_table_fails(tmp_path: Path) -> None:
    raw = myloasm_raw_dir(tmp_path / "in", "0.7.0", sequence_idx_names=True)
    (raw / "myloasm_split.parquet").unlink()
    with pytest.raises(FileNotFoundError, match="myloasm_split"):
        _stage(tmp_path, raw)
