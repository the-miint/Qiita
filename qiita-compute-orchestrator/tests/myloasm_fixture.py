"""The committed myloasm output fixture (`tests/data/myloasm/`), laid out and dumped the
way the assemble step lays out and dumps a real run."""

from __future__ import annotations

import gzip
import os
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb

from qiita_compute_orchestrator.jobs._myloasm_provenance import READ_NAME_COLUMNS

_REPO_ROOT = Path(__file__).resolve().parents[2]
_FIXTURES = Path(__file__).resolve().parent / "data" / "myloasm"
# Files myloasm itself writes gzipped; every other fixture file is gzipped for storage only.
_NATIVE_GZ = frozenset({"map_to_unitigs.paf.gz", "read_coverages.txt.gz"})

PROVENANCE_SCRIPT = _REPO_ROOT / "workflows" / "long-read-assembly" / "myloasm_provenance.py"
MYLOASM_FIXTURE_VERSIONS = ("0.6.0", "0.7.0")
# Rows each reader returns over the fixture, from duckdb-miint's own fixture README;
# the same for both versions.
MYLOASM_FIXTURE_ROWS = {
    "myloasm_contig": 5,
    "myloasm_layout": 173,
    "myloasm_mapping": 1085,
    "myloasm_link": 2,
    "myloasm_split": 40,
    "myloasm_run": 1,
}
# The first sequence_idx `myloasm_raw_dir` gives a renamed read; any value well clear of
# a small fixture's other identifiers does.
MYLOASM_FIRST_SEQUENCE_IDX = 9_000_000_000


def myloasm_output_dir(dest: Path, version: str) -> Path:
    """Lay out the committed myloasm fixture at `dest` the way myloasm writes its `-o`
    directory, for myloasm `version` (see `tests/data/myloasm/README.md`)."""
    if version not in MYLOASM_FIXTURE_VERSIONS:
        raise ValueError(f"no myloasm {version} fixture; have {MYLOASM_FIXTURE_VERSIONS}")
    sources = [_FIXTURES / "ecoli_sim"]
    if version == "0.6.0":
        sources.append(_FIXTURES / "ecoli_sim_0.6.0")
    for source in sources:
        for path in source.rglob("*"):
            if not path.is_file():
                continue
            target = dest / path.relative_to(source)
            target.parent.mkdir(parents=True, exist_ok=True)
            if path.name in _NATIVE_GZ:
                shutil.copyfile(path, target)
            else:
                with gzip.open(path, "rb") as src, target.with_suffix("").open("wb") as out:
                    shutil.copyfileobj(src, out)
    return dest


def run_provenance_dump(
    asm_dir: Path, out_dir: Path, *, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    """Run the assemble step's dump script the way assemble.sh does."""
    return subprocess.run(
        [sys.executable, str(PROVENANCE_SCRIPT), str(asm_dir), str(out_dir)],
        capture_output=True,
        text=True,
        env=dict(os.environ) if env is None else env,
    )


def myloasm_raw_dir(dest: Path, version: str, *, sequence_idx_names: bool) -> Path:
    """The raw provenance dump the assemble step writes, produced by its own script
    over the committed fixture.

    With `sequence_idx_names`, every read name is then replaced by an integer, the
    way a run whose assembler input was named by `sequence_idx` reports them: names
    sorted, numbered from `MYLOASM_FIRST_SEQUENCE_IDX`. `myloasm_read_name_map`
    returns the same numbering for the original names."""
    asm_dir = myloasm_output_dir(dest / "assembler", version)
    raw = dest / "raw"
    result = run_provenance_dump(asm_dir, raw)
    if result.returncode != 0:
        raise RuntimeError(f"provenance dump failed: {result.stderr}")
    if not sequence_idx_names:
        return raw
    with duckdb.connect() as conn:
        conn.execute(
            "CREATE TEMP TABLE name_map AS SELECT read_id, "
            "CAST(sequence_idx AS VARCHAR) AS seq FROM (" + _read_name_map_sql(raw) + ")"
        )
        renames: dict[str, list[str]] = {}
        for table, column in READ_NAME_COLUMNS:
            renames.setdefault(table, []).append(column)
        for table, columns in renames.items():
            path = raw / f"{table}.parquet"
            replace = ", ".join(
                f"(SELECT seq FROM name_map n WHERE n.read_id = t.{c}) AS {c}" for c in columns
            )
            conn.execute(
                f"COPY (SELECT * REPLACE ({replace}) FROM '{path}' t) "
                f"TO '{path}.tmp' (FORMAT PARQUET)"
            )
            Path(f"{path}.tmp").replace(path)
    return raw


def myloasm_read_name_map(raw: Path) -> list[tuple[str, int]]:
    """(original read name, sequence_idx) for every read the fixture's raw dump at
    `raw` names, numbered as `myloasm_raw_dir(..., sequence_idx_names=True)` does."""
    with duckdb.connect() as conn:
        return conn.execute(_read_name_map_sql(raw) + " ORDER BY read_id").fetchall()


def _read_name_map_sql(raw: Path) -> str:
    names = " UNION ".join(
        f"SELECT {column} AS read_id FROM '{raw}/{table}.parquet'"
        for table, column in READ_NAME_COLUMNS
    )
    return (
        "SELECT read_id, "
        f"{MYLOASM_FIRST_SEQUENCE_IDX} - 1 + row_number() OVER (ORDER BY read_id) AS sequence_idx "
        f"FROM ({names}) WHERE read_id IS NOT NULL"
    )
