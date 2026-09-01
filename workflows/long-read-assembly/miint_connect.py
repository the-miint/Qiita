"""A DuckDB connection with the deploy-staged miint extension LOADed, for the
Python helpers this workflow's container entrypoints run.

Imported by `myloasm_split.py` (assemble step) and `lcg_split.py` (checkm step),
which both read FASTA with `read_fastx` and write it with `COPY … (FORMAT FASTA)`.
A module rather than a copy in each: the connect config below has to stay
identical to what the three services run, and two copies of it would drift on
their own schedules.

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
