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

That is also why an id that cannot be a filename is REJECTED rather than sanitized.
A sanitized stem would come back from CheckM as a bin_id that joins nothing, and
the quality row would land in `bin_quality` describing a genome no membership row
names.

ONE `COPY` PER CONTIG
---------------------
`COPY … (PARTITION_BY …)` writes `column=value/` subdirectories, which is not the
flat `<stem>.fa` layout `lineage_wf -x fa` reads, so the records are written one
file at a time. The reader still runs once: `read_fastx` parses the multi-FASTA
into a temp table up front and each COPY selects one row out of it.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# `miint_connect` sits beside this file, both in the repo and at /opt/qiita in the
# image. Running the script directly puts that directory on sys.path already; the
# insert is for the `.def` %test, which loads this module by spec and does not.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from miint_connect import (  # noqa: E402
    connect,
    die,
    reject_duplicate_contig_ids,
    require_non_empty_fasta,
    sql_path,
)

PROG = "lcg_split"

# The characters a contig id may use if it is to become a filename. Conservative
# on purpose — the point is that the stem reaches CheckM and comes back unchanged,
# not that every POSIX-legal name is accepted. Both assemblers observed here stay
# well inside it (`s0.ctg000001c`, `u713ctg`).
_SAFE_ID_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]*\Z")

# Longest id that can still become `<id>.fa`. Measured on the deploy host, whose
# ticket workspaces are lustre: a 252-character stem creates, 253 fails with
# ENAMETOOLONG. Bounded here so an over-long id is rejected with the message below,
# like every other unusable one, rather than failing part-way through the write loop
# with some files already created.
_MAX_ID_LENGTH = 255 - len(".fa")


def _load(con, src: str) -> None:
    """Parse `circular.fa` once into a temp table keyed on the read_fastx record id.

    `contig_id`, not `read_id`: the alias must not shadow `read_fastx`'s own column,
    or a later WHERE clause's meaning would rest on DuckDB's alias-vs-column
    precedence.
    """
    con.execute(
        f"CREATE TEMP TABLE contig AS SELECT read_id AS contig_id, sequence1 FROM read_fastx({src})"
    )


def _validate(con) -> list[str]:
    """Reject an unusable id, then return every contig id in sorted order.

    Duplicates first — the shared guard states why, including what the overwrite in
    this splitter costs.
    """
    reject_duplicate_contig_ids(PROG, con)

    ids = [
        row[0]
        for row in con.execute("SELECT contig_id FROM contig ORDER BY 1").fetchall()
    ]
    bad = [
        cid
        for cid in ids
        if not _SAFE_ID_RE.match(cid) or len(cid.encode()) > _MAX_ID_LENGTH
    ][:3]
    if bad:
        die(
            PROG,
            f"contig id(s) cannot be used as a filename stem: {bad}. CheckM keys its "
            "output on the stem and assembly_load joins that to "
            "assembly_membership.bin_id, so the id is written verbatim or not at all; "
            f"it must match {_SAFE_ID_RE.pattern} and be at most {_MAX_ID_LENGTH} bytes",
        )
    return ids


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        die(PROG, f"usage: {argv[0]} <circular.fa> <out_dir>")
    circular, out_dir = argv[1], Path(argv[2])

    require_non_empty_fasta(PROG, circular)

    out_dir.mkdir(parents=True, exist_ok=True)
    con = connect(PROG, temp_subdir="duckdb-lcg-split")
    try:
        _load(con, sql_path(PROG, circular))
        ids = _validate(con)
        for contig_id in ids:
            target = sql_path(PROG, str(out_dir / f"{contig_id}.fa"))
            con.execute(
                "COPY (SELECT contig_id AS read_id, sequence1 FROM contig"
                f" WHERE contig_id = ?) TO {target} (FORMAT FASTA)",
                [contig_id],
            )
    finally:
        con.close()

    written = len(list(out_dir.glob("*.fa")))
    # Counted off the directory rather than off the loop: what CheckM reads is the
    # set of files, and a stem colliding with one already in `out_dir` would leave
    # fewer files than records with every COPY having succeeded.
    if written != len(ids):
        die(
            PROG, f"wrote {written} FASTA file(s) for {len(ids)} contig(s) in {out_dir}"
        )

    print(f"lcg_split: {written} circular contig(s) written as one FASTA each")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
