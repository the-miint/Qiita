"""Execute `scripts/dedup-lake-sequence-tables.sh`'s SQL against a real DuckLake.

The pure-unit guards in `test_deploy_scripts.py` assert the emitted SQL as TEXT,
against a stub `duckdb` that only prints its argument — enough to pin which
statements are emitted in which mode, and nothing at all about whether they run.
This runs them, on a catalog seeded with the duplication shape the deploy has, so
the parts a text assertion cannot reach are covered:

  * report mode's `CREATE OR REPLACE TEMP TABLE` and `DELETE FROM dup_feature`
    while `qiita_lake` is attached READ_ONLY (temp tables live in the `temp`
    database, so the read-only attach does not forbid them);
  * `SELECT DISTINCT * FROM t SEMI JOIN d USING (feature_idx)` projecting only
    `t`'s columns, so the `INSERT ... SELECT *` arity matches;
  * the convergence assertion committing on success.

Driven through the SQL the script itself emits — captured with the same stub
`test_deploy_scripts.py` uses — so the statements under test are the shipped ones
rather than a copy that can drift. The CLI dot-commands (`.bail`, `.print`) are
stripped, since the Python module is not the CLI; `.bail on`'s behaviour is
covered by the CLI's own exit status, not here.
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

from conftest import DUCKLAKE_CATALOG_CONNSTR, ducklake_connect

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEDUP_LAKE = _REPO_ROOT / "scripts" / "dedup-lake-sequence-tables.sh"

# The duplication shape, per table pair: feature 1 is this run's own, feature 2 is
# produced by two loads byte-identically (2 chunks), feature 3 by two loads whose
# bytes DIFFER (a reverse complement — same canonical hash, different bytes), and
# feature 4 arrives only in the second load.
_LOAD_A_SEQUENCES = "(1, '{u}01', 8), (2, '{u}02', 12), (3, '{u}03', 4)"
_LOAD_A_CHUNKS = (
    "(1, 0, 'ACGTACGT'), (2, 0, 'ACGTACGT'), (2, 1, 'ACGT'), (3, 0, 'AACC')"
)
_LOAD_B_SEQUENCES = "(2, '{u}02', 12), (3, '{u}03', 4), (4, '{u}04', 4)"
_LOAD_B_CHUNKS = "(2, 0, 'ACGTACGT'), (2, 1, 'ACGT'), (3, 0, 'GGTT'), (4, 0, 'TTTT')"

_PAIRS = [
    ("assembled_sequence", "assembled_sequence_chunks"),
    ("reference_sequences", "reference_sequence_chunks"),
]


def _emitted_sql(tmp_path: Path, data_path: Path, *, apply: bool) -> str:
    """Run the script against a stub duckdb that prints the SQL file it is handed,
    then strip the CLI dot-commands the Python duckdb module does not accept."""
    env_file = tmp_path / "data-plane.env"
    env_file.write_text(
        f"DUCKLAKE_CATALOG_CONNSTR='{DUCKLAKE_CATALOG_CONNSTR}'\n"
        f"PATH_PERSISTENT={data_path.parent}\n"
    )
    stub = tmp_path / "duckdb-stub"
    stub.write_text('#!/bin/bash\ncat "$2"\n')
    stub.chmod(0o755)

    env = {**os.environ, "DP_ENV": str(env_file), "QIITA_DUCKDB_BIN": str(stub)}
    if apply:
        env["APPLY"] = "1"
    else:
        env.pop("APPLY", None)
    sql = subprocess.run(
        ["bash", str(_DEDUP_LAKE)], capture_output=True, text=True, env=env, check=True
    ).stdout
    # Drop the dot-commands and the ATTACH/INSTALL preamble — this test drives the
    # statements through an already-attached connection.
    body = sql.split("AS qiita_lake", 1)[1].split(";", 1)[1]
    return re.sub(r"^\s*\.\w+.*$", "", body, flags=re.MULTILINE)


def _seed(conn) -> None:
    u = "00000000-0000-0000-0000-0000000000"
    for sequences, chunks in _PAIRS:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS qiita_lake.{sequences} ("
            " feature_idx BIGINT NOT NULL, sequence_hash UUID NOT NULL,"
            " sequence_length_bp BIGINT NOT NULL);"
            f"CREATE TABLE IF NOT EXISTS qiita_lake.{chunks} ("
            " feature_idx BIGINT NOT NULL, chunk_index INTEGER NOT NULL,"
            " chunk_data VARCHAR NOT NULL)"
        )
        conn.execute(f"DELETE FROM qiita_lake.{sequences}")
        conn.execute(f"DELETE FROM qiita_lake.{chunks}")
        # Two separate INSERTs so each lands as its own DuckLake data file, the way
        # two register_files loads would.
        conn.execute(
            f"INSERT INTO qiita_lake.{sequences} VALUES {_LOAD_A_SEQUENCES.format(u=u)}"
        )
        conn.execute(f"INSERT INTO qiita_lake.{chunks} VALUES {_LOAD_A_CHUNKS}")
        conn.execute(
            f"INSERT INTO qiita_lake.{sequences} VALUES {_LOAD_B_SEQUENCES.format(u=u)}"
        )
        conn.execute(f"INSERT INTO qiita_lake.{chunks} VALUES {_LOAD_B_CHUNKS}")


@pytest.fixture
def seeded_lake(data_plane, tmp_path):
    """A lake holding the duplication shape. Depends on `data_plane` so the
    catalog exists and DATA_PATH matches what the fixture pinned into it."""
    conn = ducklake_connect(data_plane["data_path"])
    _seed(conn)
    yield conn, Path(data_plane["data_path"])
    conn.close()


def test_report_sql_runs_and_finds_the_duplicates(seeded_lake, tmp_path):
    """Report mode's statements execute and classify correctly: feature 2 is
    collapsible, feature 3 is ambiguous (its copies differ), 1 and 4 are neither."""
    conn, data_path = seeded_lake
    for statement in _emitted_sql(tmp_path, data_path, apply=False).split(";"):
        if statement.strip():
            conn.execute(statement)
    assert [
        r[0]
        for r in conn.execute(
            "SELECT feature_idx FROM dup_feature ORDER BY 1"
        ).fetchall()
    ] == [2]
    assert [
        r[0]
        for r in conn.execute(
            "SELECT feature_idx FROM ambiguous_feature ORDER BY 1"
        ).fetchall()
    ] == [3]
    # And it wrote nothing: both loads' rows are still there.
    for sequences, _chunks in _PAIRS:
        assert (
            conn.execute(f"SELECT count(*) FROM qiita_lake.{sequences}").fetchone()[0]
            == 6
        )


def test_apply_sql_collapses_and_leaves_differing_copies_alone(seeded_lake, tmp_path):
    """The collapse runs, converges (its in-transaction assertion returns rather
    than raising), and leaves the ambiguous feature untouched."""
    conn, data_path = seeded_lake
    for statement in _emitted_sql(tmp_path, data_path, apply=True).split(";"):
        if statement.strip():
            conn.execute(statement)

    for sequences, chunks in _PAIRS:
        counts = dict(
            conn.execute(
                f"SELECT feature_idx, count(*) FROM qiita_lake.{sequences} GROUP BY 1"
            ).fetchall()
        )
        assert counts == {1: 1, 2: 1, 3: 2, 4: 1}, (
            f"{sequences}: feature 2 collapses, feature 3 (differing bytes) is left alone"
        )
        # The acceptance invariant, for every feature the script did collapse.
        mismatches = conn.execute(
            f"SELECT s.feature_idx FROM qiita_lake.{sequences} s "
            f"JOIN qiita_lake.{chunks} c USING (feature_idx) "
            " WHERE s.feature_idx <> 3 "
            " GROUP BY s.feature_idx, s.sequence_length_bp "
            "HAVING length(string_agg(c.chunk_data, '' ORDER BY c.chunk_index)) "
            "       <> s.sequence_length_bp"
        ).fetchall()
        assert mismatches == [], (
            f"{sequences}: reassembly disagrees with sequence_length_bp"
        )


def test_apply_sql_is_idempotent(seeded_lake, tmp_path):
    """A second collapse finds nothing left to do and still converges — the
    property that makes a part-way failure safe to re-run."""
    conn, data_path = seeded_lake
    sql = _emitted_sql(tmp_path, data_path, apply=True)
    for _ in range(2):
        for statement in sql.split(";"):
            if statement.strip():
                conn.execute(statement)
    assert conn.execute("SELECT count(*) FROM dup_feature").fetchone()[0] == 0
    for sequences, _chunks in _PAIRS:
        counts = dict(
            conn.execute(
                f"SELECT feature_idx, count(*) FROM qiita_lake.{sequences} GROUP BY 1"
            ).fetchall()
        )
        assert counts == {1: 1, 2: 1, 3: 2, 4: 1}
