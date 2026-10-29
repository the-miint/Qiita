"""Dump myloasm's per-read provenance out of its output directory, one Parquet per
table, using miint's `read_myloasm_*` readers.

Run by assemble.sh's myloasm branch, after the splitter:

    python3 /opt/qiita/myloasm_provenance.py <myloasm -o dir> <out dir>

The assembler's output tree is not a declared output of the assemble step, so no
later step can mount it. What `assembly_load` needs from it is written here, into
`genomes_dir`, which is. Each table is written verbatim: miint's columns, with
myloasm's own read and contig names. Re-keying onto our identifiers happens in the
orchestrator's `jobs/_myloasm_provenance.py`, where the run's `feature_idx` values
are known; keeping this side a plain dump lets a tree read outside the container
share that transform.

What the tables are, and the lake tables they become, is
`qiita_common.myloasm_constants`; this container cannot import it, so `READERS`
restates the table -> reader map and a test pins the two together.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
from miint_connect import connect, die, sql_path

PROG = "myloasm_provenance"

# Lake table -> the miint reader it comes from. One Parquet per table, named
# `<table>.parquet`.
READERS = {
    "myloasm_contig": "read_myloasm_contigs",
    "myloasm_layout": "read_myloasm_layout",
    "myloasm_mapping": "read_myloasm_mappings",
    "myloasm_link": "read_myloasm_links",
    "myloasm_split": "read_myloasm_splits",
    "myloasm_run": "read_myloasm_run",
}

# Every myloasm run that wrote a non-empty primary FASTA has contigs and a log.
# Zero rows from either is a reader or layout change, not an empty assembly (this
# script runs only for a non-empty one).
_NEVER_EMPTY = ("myloasm_contig", "myloasm_run")


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        die(PROG, f"usage: {argv[0]} <myloasm -o dir> <out dir>")
    asm_dir, out_dir = Path(argv[1]), Path(argv[2])
    if not asm_dir.is_dir():
        die(PROG, f"not a directory: {asm_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)

    src = sql_path(PROG, str(asm_dir))
    con = connect(PROG, temp_subdir="duckdb-myloasm-provenance")
    counts: dict[str, int] = {}
    try:
        for table, reader in READERS.items():
            dest = sql_path(PROG, str(out_dir / f"{table}.parquet"))
            try:
                (counts[table],) = con.execute(
                    f"COPY (FROM {reader}({src})) TO {dest} "
                    "(FORMAT PARQUET, COMPRESSION 'zstd')"
                ).fetchone()
            except duckdb.Error as exc:
                die(PROG, f"{reader} over {asm_dir} failed: {exc}")
    finally:
        con.close()

    for table in _NEVER_EMPTY:
        if counts[table] == 0:
            die(PROG, f"{READERS[table]} returned no rows for a non-empty assembly")

    print(f"{PROG}: " + ", ".join(f"{t} {n}" for t, n in counts.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
