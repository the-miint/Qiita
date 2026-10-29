"""Re-key a myloasm run's raw provenance dump onto our identifiers and write the six
staging Parquets register-files loads into the `myloasm_*` lake tables.

The input is the assemble step's dump: one Parquet per miint `read_myloasm_*`
reader, verbatim, under `genomes_dir/<MYLOASM_RAW_DIR>/<table>.parquet`
(`workflows/long-read-assembly/myloasm_provenance.py`). The tables, the lake shape
and why `contig_num` is a per-run key are `qiita_common.myloasm_constants`.

Two lookups the caller supplies, as TEMP TABLEs, because they come from different
places depending on how the run was made:

- `READ_NAME_TABLE (read_id VARCHAR, sequence_idx BIGINT)`: every read name the
  dump uses -> its `sequence_idx`. A run whose assembler input was named by
  `sequence_idx` builds it with `read_names_from_sequence_idx`; an earlier run's
  tree, named by the sequencer's `read_id`, needs a lookup against the lake.
- `CONTIG_FEATURE_TABLE (contig_id VARCHAR, feature_idx BIGINT)`: each primary
  contig's minted `feature_idx`, keyed on myloasm's `u<N>ctg` name. A repeated
  identical pair is allowed (the transform deduplicates); two different
  `feature_idx` for one contig are refused.

Everything that could make a row wrong is checked before anything is written, and
fails the step with the offending names: a dump with no `myloasm_run` row, a read
the lookup does not resolve, a contig name that is not myloasm's, a repeated
`contig_num`, a contig referenced but not in the run, a primary contig with no
feature or a feature on anything else.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
from qiita_common.myloasm_constants import MYLOASM_TABLES, myloasm_projection
from qiita_common.parquet import validate_parquet_path

from ..miint import PARQUET_OPTS_AT_REST

READ_NAME_TABLE = "myloasm_read_name"
CONTIG_FEATURE_TABLE = "myloasm_contig_feature"

# Every read name the dump uses, from every column that holds one. A NULL link end
# names no read (docs/duckdb-miint.md, the `read_myloasm_*` entry).
_RAW_READ_NAMES = "myloasm_raw_read_name"
READ_NAME_COLUMNS = (
    ("myloasm_mapping", "read_id"),
    ("myloasm_layout", "read_id"),
    ("myloasm_split", "read_id"),
    ("myloasm_link", "from_read_id"),
    ("myloasm_link", "to_read_id"),
)
# Every column that names a contig, beside the contig table's own `contig_id`.
_CONTIG_REFERENCE_COLUMNS = (
    ("myloasm_contig", "secondary_to_contig_id"),
    ("myloasm_layout", "contig_id"),
    ("myloasm_mapping", "contig_id"),
    ("myloasm_link", "from_contig_id"),
    ("myloasm_link", "to_contig_id"),
)
_CONTIG_NAME_RE = "^u([0-9]+)ctg$"
# miint's `dereplication_status` for the contigs a run hashes into features.
_PRIMARY = "primary"
# How many offending names an error quotes.
_SHOWN = 10


def _raw(table: str) -> str:
    return f"raw_{table}"


def _contig_num(column: str) -> str:
    return f"CAST(regexp_extract({column}, '{_CONTIG_NAME_RE}', 1) AS BIGINT)"


def staging_paths(staging_dir: Path) -> dict[str, Path]:
    """Where `write_myloasm_staging` writes each table, so a caller can clean up a
    partial write without knowing the names."""
    return {table: staging_dir / f"{table}.parquet" for table in MYLOASM_TABLES}


def register_raw_tables(conn: duckdb.DuckDBPyConnection, raw_dir: Path) -> None:
    """Register the dump's six Parquets as `raw_<table>` views, and every read name
    they use as one distinct list. All six are required: the dump writes all six
    for every myloasm run, so a missing one is a broken dump."""
    for table in MYLOASM_TABLES:
        path = raw_dir / f"{table}.parquet"
        if not path.is_file():
            raise FileNotFoundError(f"myloasm provenance dump has no {table}: {path}")
        conn.execute(
            f"CREATE TEMP VIEW {_raw(table)} AS "
            f"SELECT * FROM read_parquet('{validate_parquet_path(path)}')"
        )
    conn.execute(
        f"CREATE TEMP VIEW {_RAW_READ_NAMES} AS "
        + " UNION ".join(
            f"SELECT {column} AS read_id FROM {_raw(table)} WHERE {column} IS NOT NULL"
            for table, column in READ_NAME_COLUMNS
        )
    )


def read_names_from_sequence_idx(conn: duckdb.DuckDBPyConnection) -> None:
    """Build `READ_NAME_TABLE` for a run whose reads were named by `sequence_idx`:
    each name is its own `sequence_idx`. A name that is not exactly an integer's
    text is refused, which is what a tree named by `read_id` looks like here."""
    conn.execute(
        f"CREATE TEMP TABLE {READ_NAME_TABLE} AS "
        f"SELECT read_id, TRY_CAST(read_id AS BIGINT) AS sequence_idx FROM {_RAW_READ_NAMES}"
    )
    bad = conn.execute(
        f"SELECT read_id FROM {READ_NAME_TABLE} "
        "WHERE sequence_idx IS NULL OR CAST(sequence_idx AS VARCHAR) <> read_id "
        f"ORDER BY read_id LIMIT {_SHOWN}"
    ).fetchall()
    if bad:
        raise ValueError(
            "myloasm read name(s) not a sequence_idx: "
            f"{[r[0] for r in bad]}; the assembler's input was expected to name each "
            "read by its sequence_idx"
        )


def _require_none(conn: duckdb.DuckDBPyConnection, sql: str, message: str) -> None:
    rows = conn.execute(f"{sql} LIMIT {_SHOWN}").fetchall()
    if rows:
        shown = [r[0] if len(r) == 1 else r for r in rows]
        raise ValueError(f"{message}: {shown}")


def _validate(conn: duckdb.DuckDBPyConnection) -> None:
    # `myloasm_run` is the replace-key source for all six tables
    # (`qiita_common.myloasm_constants`), so a dump without a run row could not
    # supersede an earlier registration of the same run.
    (runs,) = conn.execute(f"SELECT count(*) FROM {_raw('myloasm_run')}").fetchone()
    if runs == 0:
        raise ValueError("myloasm provenance dump has no myloasm_run row")

    contig = _raw("myloasm_contig")
    every_name = " UNION ".join(
        [f"SELECT contig_id AS name FROM {contig}"]
        + [
            f"SELECT {column} FROM {_raw(table)} WHERE {column} IS NOT NULL"
            for table, column in _CONTIG_REFERENCE_COLUMNS
        ]
    )
    _require_none(
        conn,
        f"SELECT name FROM ({every_name}) WHERE NOT regexp_matches(name, '{_CONTIG_NAME_RE}') "
        "ORDER BY name",
        "contig name(s) not myloasm's u<N>ctg",
    )
    _require_none(
        conn,
        f"SELECT {_contig_num('contig_id')} AS n FROM {contig} GROUP BY n HAVING count(*) > 1 "
        "ORDER BY n",
        "contig_num repeated within one run",
    )
    _require_none(
        conn,
        f"SELECT name FROM ({every_name}) ANTI JOIN {contig} c ON c.contig_id = name ORDER BY name",
        "contig(s) referenced but not in the run's contigs",
    )

    _require_none(
        conn,
        f"SELECT read_id FROM {READ_NAME_TABLE} GROUP BY read_id HAVING count(*) > 1 "
        "ORDER BY read_id",
        "read name(s) with more than one sequence_idx",
    )
    _require_none(
        conn,
        f"SELECT r.read_id FROM {_RAW_READ_NAMES} r ANTI JOIN {READ_NAME_TABLE} n "
        "USING (read_id) ORDER BY r.read_id",
        "read name(s) with no sequence_idx",
    )

    _require_none(
        conn,
        f"SELECT contig_id FROM {CONTIG_FEATURE_TABLE} GROUP BY contig_id "
        "HAVING count(DISTINCT feature_idx) > 1 ORDER BY contig_id",
        "contig(s) with more than one feature_idx",
    )
    _require_none(
        conn,
        f"SELECT f.contig_id FROM {CONTIG_FEATURE_TABLE} f ANTI JOIN {contig} c "
        f"ON c.contig_id = f.contig_id AND c.dereplication_status = '{_PRIMARY}' "
        "ORDER BY f.contig_id",
        "feature_idx given for contig(s) that are not this run's primary contigs",
    )
    _require_none(
        conn,
        f"SELECT c.contig_id FROM {contig} c ANTI JOIN {CONTIG_FEATURE_TABLE} f "
        f"ON f.contig_id = c.contig_id WHERE c.dereplication_status = '{_PRIMARY}' "
        "ORDER BY c.contig_id",
        "primary contig(s) with no feature_idx",
    )


def _sources(table: str, prep_sample_idx: int, processing_idx: int) -> tuple[dict[str, str], str]:
    """(column -> SQL, FROM clause) for one table. Raw columns are addressed as `r.`;
    the read-name lookups are joined as `n` (and `nf` / `nt` for a link's ends)."""
    run = {"prep_sample_idx": str(prep_sample_idx), "processing_idx": str(processing_idx)}
    spec = MYLOASM_TABLES[table]
    sources = {**run, **{c: f"r.{c}" for c in spec.column_names if c not in run}}
    src = f"{_raw(table)} r"
    names = READ_NAME_TABLE
    if table == "myloasm_contig":
        sources["contig_num"] = _contig_num("r.contig_id")
        sources["secondary_to_contig_num"] = _contig_num("r.secondary_to_contig_id")
        sources["feature_idx"] = "f.feature_idx"
        src += f" LEFT JOIN (SELECT DISTINCT * FROM {CONTIG_FEATURE_TABLE}) f USING (contig_id)"
    elif table in ("myloasm_layout", "myloasm_mapping", "myloasm_split"):
        if table != "myloasm_split":
            sources["contig_num"] = _contig_num("r.contig_id")
        sources["sequence_idx"] = "n.sequence_idx"
        src += f" JOIN {names} n USING (read_id)"
    elif table == "myloasm_link":
        sources["from_contig_num"] = _contig_num("r.from_contig_id")
        sources["to_contig_num"] = _contig_num("r.to_contig_id")
        sources["from_sequence_idx"] = "nf.sequence_idx"
        sources["to_sequence_idx"] = "nt.sequence_idx"
        src += (
            f" LEFT JOIN {names} nf ON nf.read_id = r.from_read_id"
            f" LEFT JOIN {names} nt ON nt.read_id = r.to_read_id"
        )
    elif table == "myloasm_run":
        sources["log_name"] = "regexp_extract(r.log_file, '[^/]+$')"
    return sources, src


def write_myloasm_staging(
    conn: duckdb.DuckDBPyConnection,
    *,
    staging_dir: Path,
    prep_sample_idx: int,
    processing_idx: int,
) -> dict[str, Path]:
    """Validate the registered dump against the two caller-built lookups, then write
    one staging Parquet per lake table. Expects `register_raw_tables` and both TEMP
    TABLEs. Returns `staging_paths(staging_dir)`."""
    _validate(conn)
    written = staging_paths(staging_dir)
    for table, path in written.items():
        sources, src = _sources(table, prep_sample_idx, processing_idx)
        conn.execute(
            f"COPY (SELECT {myloasm_projection(table, sources)} FROM {src}) "
            f"TO '{validate_parquet_path(path)}' ({PARQUET_OPTS_AT_REST})"
        )
    return written
