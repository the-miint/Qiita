"""The DuckDB connection with the deploy-staged miint extension LOADed, plus the
contig-id guards and the per-contig FASTA write, for the Python helpers this
workflow's container entrypoints run.

Imported by this workflow's per-contig splitters, which read FASTA with `read_fastx`
and write it with `COPY … (FORMAT FASTA)`. A module rather than a copy in each: the
connect config below has to stay identical to what the three services run, and two
copies of it would drift on their own schedules. It also owns the guards and the
write loop the splitters share, which is where they diverged once already.

Sibling of its importers rather than under `workflows/_shared/`, because both live
in this workflow. `_shared/` is hashed into EVERY image's build-inputs digest, so a
file there rebuilds bcl-convert and lima too; and the importers resolve it by
same-directory import, which holds in the repo and in the image (both land in
/opt/qiita/) only while it sits beside them.

WHICH miint — THE DEPLOY-STAGED ONE
-----------------------------------
`connect()` LOADs the extension the deploy already staged, bind-mounted into the
container read-only via the step's `derived_inputs: MIINT_EXTENSION_DIRECTORY`. It
is the SAME build the control plane, the compute orchestrator and the data plane
run: a copy baked into an image would be a fourth miint that `make preflight`'s
byte-identity check does not compare, free to drift from the other three.

LOAD-only, never INSTALL — the standing service-side rule. A per-job INSTALL would
need the mirror reachable from every compute node and a writable `$HOME`.

DuckDB namespaces the staged directory by **engine version + platform**, so a
container using this module must run the DuckDB version the orchestrator staged
with. Each `.def` pins it and asserts it at build time; a mismatch surfaces as an
explicit LOAD failure rather than a wrong answer.

The config mirrors what `qiita_common.duckdb_miint` produces for the Python
services. It is duplicated from there rather than imported because `qiita-common`
is not installed in these containers; the Rust data plane carries the same
duplication for the same reason. A change to the canonical config belongs here too.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import duckdb

# 64 = EX_USAGE, for a contract violation the calling step cannot proceed past.
EXIT_CONTRACT_VIOLATION = 64

MIINT_EXTENSION_DIRECTORY_VAR = "MIINT_EXTENSION_DIRECTORY"


def die(prog: str, msg: str) -> None:
    """Print `<prog>: <msg>` to stderr and exit EX_USAGE. `prog` is the caller's own
    name, so a message names the helper that rejected the input rather than this
    module."""
    print(f"{prog}: {msg}", file=sys.stderr)
    raise SystemExit(EXIT_CONTRACT_VIOLATION)


def sql_path(prog: str, path: str) -> str:
    """A path safe to interpolate into a DuckDB SQL string literal.

    `COPY … TO`'s target is a string literal and cannot be parameter-bound, so it
    is interpolated — and REJECTED rather than escaped when it holds a quote,
    backslash or control character, matching `qiita_common.parquet`'s
    `validate_parquet_path`. Fail-fast beats a clever escape: no workspace path an
    entrypoint builds contains these, so one that does means something is already
    wrong.
    """
    if "'" in path or "\\" in path or any(ord(c) < 0x20 for c in path):
        die(prog, f"path contains characters unsafe to interpolate into SQL: {path!r}")
    return f"'{path}'"


def connect(prog: str, *, temp_subdir: str) -> duckdb.DuckDBPyConnection:
    """A miint connection over the deploy-staged extension directory, bounded by the
    step's own allocation.

    `memory_limit` / `threads` / `temp_directory` come from the step's resolved
    allocation, which `_shared/_lib.sh` exports as MEM_MB and THREADS (TMPDIR is
    pointed at the workspace by the SLURM payload). Without them DuckDB sizes
    itself from what it detects on the NODE, not from this step's cgroup, and a
    deep metagenome gets OOM-killed instead of spilling — the posture every native
    sibling takes via `apply_duckdb_settings`.

    `temp_subdir` names this helper's spill directory under TMPDIR, so two helpers
    sharing a workspace do not share a spill path.
    """
    ext_dir = os.environ.get(MIINT_EXTENSION_DIRECTORY_VAR)
    if not ext_dir:
        die(
            prog,
            f"{MIINT_EXTENSION_DIRECTORY_VAR} is not set. The step must declare it "
            "in the workflow YAML's `derived_inputs` so the deploy-staged miint "
            "extension is bind-mounted into this container.",
        )
    if not Path(ext_dir).is_dir():
        die(prog, f"{MIINT_EXTENSION_DIRECTORY_VAR}={ext_dir!r} is not a directory")

    config = {"allow_unsigned_extensions": "true", "extension_directory": ext_dir}
    # Leave headroom under the cgroup ceiling: DuckDB's limit governs its own
    # buffers, not the whole process, and the reader holds contig bytes outside it.
    mem_mb = os.environ.get("MEM_MB")
    if mem_mb and mem_mb.isdigit() and int(mem_mb) > 0:
        config["memory_limit"] = f"{max(1, (int(mem_mb) * 3) // 4)}MB"
    threads = os.environ.get("THREADS")
    if threads and threads.isdigit() and int(threads) > 0:
        config["threads"] = threads
    tmpdir = os.environ.get("TMPDIR")
    if tmpdir and Path(tmpdir).is_dir():
        config["temp_directory"] = str(Path(tmpdir) / temp_subdir)

    con = duckdb.connect(config=config)
    try:
        # LOAD-only, never INSTALL — see the module docstring.
        con.execute("LOAD miint")
    except duckdb.Error as exc:
        die(
            prog,
            f"LOAD miint failed from {ext_dir!r}: {exc}. DuckDB namespaces that "
            f"directory by engine version + platform (this container runs DuckDB "
            f"{duckdb.__version__}), so the usual cause is a DuckDB version skew "
            "between this image and the orchestrator that staged the extension.",
        )
    return con


def require_non_empty_fasta(prog: str, path: str) -> None:
    """Reject a zero-byte input before `read_fastx` sees it.

    `read_fastx` RAISES on a zero-record input ("Error Empty file: …") rather than
    returning no rows — the same trap `qiita_common.duckdb_miint`'s
    `is_empty_sequence_file` exists for on the native side. Each entrypoint already
    skips its splitter when the FASTA is empty; this is the second gate, so a direct
    invocation gets the caller's message instead of a DuckDB traceback.
    """
    try:
        size = Path(path).stat().st_size
    except OSError as exc:
        die(prog, f"cannot stat {path!r}: {exc}")
    if size == 0:
        die(prog, f"{path} is empty; read_fastx raises on a zero-record input")


def reject_duplicate_contig_ids(prog: str, con: duckdb.DuckDBPyConnection) -> None:
    """Raise if the TEMP TABLE `contig` repeats a `contig_id`.

    Both splitters build that table and both are broken by a repeat, with one
    consequence: two distinct contigs end up under one `assembly_membership` subject.
    An LCG's bin_id IS its contig id and so is an UNBINNED contig's (`assembly_hash`
    COALESCEs both from the read_fastx record), so the id is the subject. In the
    per-contig split the second `COPY` also overwrites the first, leaving CheckM one
    subject to score where the lake holds two memberships.
    """
    dupes = con.execute(
        "SELECT contig_id, count(*) AS n FROM contig GROUP BY 1 HAVING n > 1 ORDER BY 1 LIMIT 5"
    ).fetchall()
    if dupes:
        die(prog, f"duplicate contig id(s): {dupes}")


# The characters a contig id may use if it is to become a filename stem.
# Conservative on purpose — the point is that the stem reaches CheckM and comes back
# unchanged, not that every POSIX-legal name is accepted. Both assemblers observed
# here stay well inside it (`s0.ctg000001c`, `u713ctg`).
_SAFE_ID_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]*\Z")

# The extension CheckM reads: `lineage_wf -x fa` scores a directory of these, so it is
# what both splitters write and what the id-length bound below is computed against.
FASTA_SUFFIX = ".fa"

# Longest filename the workspace accepts, measured on the deploy host, whose ticket
# workspaces are lustre: a 252-character stem plus `.fa` creates, 253 fails with
# ENAMETOOLONG. The id bound is this MINUS the caller's suffix, computed per call
# rather than baked in — the write helper below takes the suffix as a parameter, so a
# caller writing `.fasta` under a `.fa`-derived bound would pass the check and then
# hit ENAMETOOLONG part-way through the write loop, which is the failure this rejects
# up front to avoid.
_MAX_NAME_BYTES = 255


def reject_unusable_contig_ids(prog: str, ids: list[str], suffix: str) -> None:
    """Reject a contig id that cannot be written as `<id><suffix>`.

    Both per-contig splitters key CheckM's output on the filename stem, and
    `assembly_load` joins that `"Bin Id"` to `assembly_membership.bin_id` — which,
    for the kinds these splitters feed, IS the contig id (`assembly_hash` COALESCEs
    it from the read_fastx record). So the id is written verbatim or not at all: a
    sanitized stem would come back from CheckM as a bin_id that joins nothing, and
    the quality row would describe a genome no membership row names.

    Rejected up front, before any file is written, so an unusable id fails with this
    message rather than part-way through the write loop with some files already
    created.
    """
    max_id_length = _MAX_NAME_BYTES - len(suffix)
    bad = [
        cid
        for cid in ids
        if not _SAFE_ID_RE.match(cid) or len(cid.encode()) > max_id_length
    ][:3]
    if bad:
        die(
            prog,
            f"contig id(s) cannot be used as a filename stem: {bad}. CheckM keys its "
            "output on the stem and assembly_load joins that to "
            "assembly_membership.bin_id, so the id is written verbatim or not at all; "
            f"it must match {_SAFE_ID_RE.pattern} and be at most {max_id_length} bytes",
        )


def split_contigs_to_fasta(prog: str, con, out_dir: Path, suffix: str) -> int:
    """Write the TEMP TABLE `contig` out as one FASTA per row, stem == `contig_id`.

    The shared tail of both per-contig splitters. Each builds `contig` differently —
    one reads circular.fa whole, the other subtracts the binned contigs from noLCG.fa
    and applies a length cut — but from there on what CheckM needs is identical, and
    two copies of it drifted on the id guards once already.

    `COPY … (PARTITION_BY …)` writes `column=value/` subdirectories, not the flat
    `<stem>.fa` layout `lineage_wf -x fa` reads, so the records go out one file at a
    time. The reader still runs once: the caller has already parsed its input into
    `contig` and each COPY selects one row out of it.

    Returns the number of files written.
    """
    reject_duplicate_contig_ids(prog, con)
    ids = [
        row[0]
        for row in con.execute("SELECT contig_id FROM contig ORDER BY 1").fetchall()
    ]
    reject_unusable_contig_ids(prog, ids, suffix)

    for contig_id in ids:
        target = sql_path(prog, str(out_dir / f"{contig_id}{suffix}"))
        con.execute(
            "COPY (SELECT contig_id AS read_id, sequence1 FROM contig"
            f" WHERE contig_id = ?) TO {target} (FORMAT FASTA)",
            [contig_id],
        )

    written = len(list(out_dir.glob(f"*{suffix}")))
    # Counted off the DIRECTORY rather than the loop: what CheckM reads is the set of
    # files, and a stem colliding with one already in `out_dir` would leave fewer
    # files than records with every COPY having succeeded.
    if written != len(ids):
        die(
            prog, f"wrote {written} FASTA file(s) for {len(ids)} contig(s) in {out_dir}"
        )
    return written
