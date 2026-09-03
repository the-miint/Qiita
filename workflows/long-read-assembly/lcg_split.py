"""Split the assemble step's `circular.fa` into one FASTA per contig, so CheckM
can score each circular genome as its own genome.

Run by checkm.sh:

    python3 /opt/qiita/lcg_split.py <circular.fa> <out_dir>

WHY A SPLIT AT ALL
------------------
`checkm lineage_wf` takes a DIRECTORY and treats each FASTA in it as one genome,
keying its output on the filename stem. The assemble step publishes every circular
contig as a single multi-FASTA, which CheckM would score as ONE genome of many
contigs. An LCG is a closed genome in its own right, so each record needs its own
file.

WHY THE STEM IS THE CONTIG ID, UNMODIFIED
-----------------------------------------
CheckM's `"Bin Id"` is the filename with its final extension removed — measured on
a deploy-host run, where `CONCOCT_bin.13_sub.fa` came back as
`CONCOCT_bin.13_sub`, so an embedded dot survives. assembly_load joins that value
to `assembly_membership.bin_id`, and an LCG's bin_id IS its contig id (assembly_hash
COALESCEs it from the read_fastx record). So the stem must be the contig id
byte-for-byte: this reads the id with `read_fastx`, the same reader assembly_hash
uses on the same file, which makes the two agree by construction rather than by
convention.

That is also why an id that cannot be a filename is REJECTED rather than sanitized;
`miint_connect.reject_unusable_contig_ids` is the guard and states the consequence.

ONE `COPY` PER CONTIG
---------------------
`COPY … (PARTITION_BY …)` writes `column=value/` subdirectories, which is not the
flat `<stem>.fa` layout `lineage_wf -x fa` reads, so the records are written one
file at a time. The reader still runs once: `read_fastx` parses the multi-FASTA
into a temp table up front and each COPY selects one row out of it.
"""

from __future__ import annotations

import sys
from pathlib import Path

# `miint_connect` sits beside this file, both in the repo and at /opt/qiita in the
# image. Running the script directly puts that directory on sys.path already; the
# insert is for the `.def` %test, which loads this module by spec and does not.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from miint_connect import (  # noqa: E402
    FASTA_SUFFIX,
    connect,
    die,
    require_non_empty_fasta,
    split_contigs_to_fasta,
    sql_path,
)

PROG = "lcg_split"


def _load(con, src: str) -> None:
    """Parse `circular.fa` once into a temp table keyed on the read_fastx record id.

    `contig_id`, not `read_id`: the alias must not shadow `read_fastx`'s own column,
    or a later WHERE clause's meaning would rest on DuckDB's alias-vs-column
    precedence.
    """
    con.execute(
        f"CREATE TEMP TABLE contig AS SELECT read_id AS contig_id, sequence1 FROM read_fastx({src})"
    )


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        die(PROG, f"usage: {argv[0]} <circular.fa> <out_dir>")
    circular, out_dir = argv[1], Path(argv[2])

    require_non_empty_fasta(PROG, circular)

    out_dir.mkdir(parents=True, exist_ok=True)
    con = connect(PROG, temp_subdir="duckdb-lcg-split")
    try:
        _load(con, sql_path(PROG, circular))
        written = split_contigs_to_fasta(PROG, con, out_dir, FASTA_SUFFIX)
    finally:
        con.close()

    print(f"lcg_split: {written} circular contig(s) written as one FASTA each")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
