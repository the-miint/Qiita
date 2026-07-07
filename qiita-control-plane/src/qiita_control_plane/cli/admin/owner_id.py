"""qiita-admin CLI — owner-biosample-id export subcommand.

Split out of the former single-file ``cli.admin`` module; behavior unchanged.
"""

import argparse
import contextlib
import csv
import os
import sys
import tempfile
from pathlib import Path

import httpx
from qiita_common.api_paths import (
    PATH_ADMIN_PREFIX,
    PATH_ADMIN_STUDY_OWNER_BIOSAMPLE_ID,
)

from .. import _common

# Owner-biosample-id export. Column order is fixed so the TSV is stable across
# runs; the owner name goes last (the sensitive payload after the identifiers).
# The pool variant adds the sequencing-pathway columns the server only fills
# when filtered to a pool.
_OWNER_ID_BASE_COLUMNS = ("biosample_idx", "biosample_accession", "owner_biosample_id")

_OWNER_ID_POOL_COLUMNS = (
    "biosample_idx",
    "biosample_accession",
    "prep_sample_idx",
    "ena_experiment_accession",
    "ena_run_accession",
    "owner_biosample_id",
)


def _write_owner_biosample_id_tsv(body: dict, output: Path) -> int:
    """Write the export `body` (an OwnerBiosampleIdExportResponse) to `output`
    as a header + tab-separated rows. NULL JSON values become empty cells.

    Written to a temp file in the same directory (created mode 0600 — the rows
    hold the owner-submitted names, which are PII) then atomically `os.replace`d
    into place, so a mid-write failure (disk full, etc.) can never truncate an
    existing export: either the new file lands whole or the old one is untouched.
    The temp file is removed on any failure so a stray partial is never left.

    Returns the row count written (excluding the header).
    """
    columns = (
        _OWNER_ID_POOL_COLUMNS if body["sequenced_pool_idx"] is not None else _OWNER_ID_BASE_COLUMNS
    )
    rows = body["rows"]
    fd, tmp_name = tempfile.mkstemp(dir=output.parent, prefix=f".{output.name}.", suffix=".tmp")
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", newline="") as fh:
            writer = csv.writer(fh, delimiter="\t", lineterminator="\n")
            writer.writerow(columns)
            for row in rows:
                writer.writerow(["" if row.get(col) is None else row[col] for col in columns])
        os.replace(tmp_name, output)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_name)
        raise
    return len(rows)


def _handle_owner_biosample_id(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """GET the owner-id export and write it to --output as a TSV.

    Not routed through run_http_subcommand because it writes a file rather than
    printing to stdout: it owns its own token-read, HTTP-error, and write-error
    handling so each failure surfaces as a clean stderr message + non-zero exit
    (not a traceback). The owner names are PII, so they go only to the (0600)
    output file — stdout gets a row-count summary, never the names themselves.
    """
    output: Path = args.output
    if not output.parent.is_dir():
        print(f"error: output directory does not exist: {output.parent}", file=sys.stderr)
        return 2

    try:
        token = _common.read_token()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    path = (
        f"{PATH_ADMIN_PREFIX}{PATH_ADMIN_STUDY_OWNER_BIOSAMPLE_ID.format(study_idx=args.study_idx)}"
    )
    params = (
        {"sequenced_pool_idx": args.sequenced_pool_idx}
        if args.sequenced_pool_idx is not None
        else None
    )
    try:
        body = _common.call("GET", args.base_url, token, path, params=params)
    except httpx.HTTPStatusError as exc:
        print(f"http error {exc.response.status_code}: {exc.response.text}", file=sys.stderr)
        return 1

    try:
        n = _write_owner_biosample_id_tsv(body, output)
    except OSError as exc:
        print(f"error: could not write {output}: {exc}", file=sys.stderr)
        return 1
    print(f"wrote {n} rows to {output}")
    return 0
