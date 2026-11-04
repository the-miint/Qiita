"""`qiita-admin backfill myloasm-provenance`: plan the myloasm provenance recovery
and, with `--execute`, submit its tickets. The planner and the submit loop are
`backfill/myloasm_provenance.py`; this is the operator's view of them.

A one-off, kept in its own module so its removal touches `cli/admin/__init__.py`
only at the two lines that register the parser;
`workflows/myloasm-provenance-recover/1.0.0.yaml`'s header names the issue that
removes it.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections import Counter
from pathlib import Path

from qiita_common.api_paths import PATH_WORK_TICKET_PREFIX, PATH_WORK_TICKET_ROOT

from ...backfill.myloasm_provenance import (
    SOURCE_VERSIONS,
    BackfillPlan,
    RunStatus,
    plan_backfill,
    submit_runs,
)
from ...workspace import WORK_TICKET_SUBDIR
from .. import _common
from ._helpers import open_admin_pool


def add_myloasm_provenance_parser(backfill_sub: argparse._SubParsersAction) -> None:
    p = backfill_sub.add_parser(
        "myloasm-provenance",
        help="Submit recovery tickets for myloasm runs that predate the provenance tables",
        description=(
            "For every myloasm run of long-read-assembly"
            f" {'/'.join(SOURCE_VERSIONS)}, find the completed assemble attempt whose"
            " output is still in its ticket workspace, and submit one"
            " myloasm-provenance-recover ticket for it. A run whose recovery ticket"
            " completed is done; a run that cannot be recovered is listed as residue,"
            " with the reason. One ticket per prep_sample per pass: re-run the"
            " command until nothing is writable. Dry-run by default. Needs"
            " DATABASE_URL and PATH_SCRATCH, and a PAT with --execute."
        ),
    )
    p.add_argument(
        "--execute",
        action="store_true",
        help="Submit the tickets (default: dry-run, report only, nothing submitted).",
    )
    p.add_argument(
        "--retry-failed",
        action="store_true",
        help=(
            "Resubmit runs whose recovery ticket ended without completing: failed,"
            " cancelled or no_data (otherwise listed as residue, with the reason)."
        ),
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap the submissions this pass, refused ones included.",
    )
    p.add_argument(
        "--rate",
        type=float,
        default=0.0,
        dest="rate_seconds",
        help="Seconds to sleep between submissions so SLURM isn't flooded (default: 0).",
    )
    p.set_defaults(handler=_handle)


async def _plan(database_url: str, *, ticket_root: Path, retry_failed: bool) -> BackfillPlan:
    pool = await open_admin_pool(database_url)
    try:
        return await plan_backfill(pool, ticket_root=ticket_root, retry_failed=retry_failed)
    finally:
        await pool.close()


def _print_plan(plan: BackfillPlan) -> None:
    print(f"myloasm runs ({'/'.join(SOURCE_VERSIONS)}): {len(plan.runs)}")
    for status, label in (
        (RunStatus.WRITABLE, "writable "),
        (RunStatus.DONE, "done     "),
        (RunStatus.IN_FLIGHT, "in flight"),
        (RunStatus.SKIPPED, "skipped  "),
        (RunStatus.RESIDUE, "residue  "),
    ):
        print(f"  {label} : {len(plan.by_status(status))}")
    for reason, n in sorted(Counter(r.reason for r in plan.by_status(RunStatus.SKIPPED)).items()):
        print(f"    skipped, {reason}: {n}")
    residue = plan.by_status(RunStatus.RESIDUE)
    for kind, n in sorted(Counter(r.residue_kind for r in residue).items()):
        print(f"    residue, {kind}: {n}")
    for run in residue:
        print(f"    prep_sample {run.prep_sample_idx} run {run.processing_idx}: {run.reason}")


def _handle(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("error: DATABASE_URL not set", file=sys.stderr)
        return 2
    scratch = os.environ.get("PATH_SCRATCH")
    if not scratch or not Path(scratch).is_absolute():
        print("error: PATH_SCRATCH must be set to an absolute path", file=sys.stderr)
        return 2
    if args.limit is not None and args.limit <= 0:
        print("error: --limit must be positive", file=sys.stderr)
        return 2
    token = None
    if args.execute:
        try:
            token = _common.read_token()
        except RuntimeError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1

    try:
        plan = asyncio.run(
            _plan(
                database_url,
                ticket_root=Path(scratch) / WORK_TICKET_SUBDIR,
                retry_failed=args.retry_failed,
            )
        )
    except (RuntimeError, ValueError, OSError) as exc:
        # OSError: a source tree that this account cannot look at (PermissionError)
        # stops the plan rather than reading as missing.
        print(f"error: {exc}", file=sys.stderr)
        return 1
    _print_plan(plan)

    writable = plan.by_status(RunStatus.WRITABLE)
    if not args.execute:
        print(
            f"\nDRY RUN — nothing submitted. Pass --execute to submit up to {len(writable)}"
            " recovery ticket(s), one per prep_sample per pass."
        )
        return 0

    path = f"{PATH_WORK_TICKET_PREFIX}{PATH_WORK_TICKET_ROOT}"
    report = submit_runs(
        plan,
        submit=lambda body: _common.call("POST", args.base_url, token, path, json=body),
        limit=args.limit,
        rate_seconds=args.rate_seconds,
    )
    print(f"\nsubmitted: {len(report.submitted)}")
    for run, ticket in report.submitted:
        print(f"  prep_sample {run.prep_sample_idx} run {run.processing_idx} -> ticket {ticket}")
    print(f"deferred to the next pass: {len(report.deferred)}")
    print(f"not attempted (over --limit): {len(report.not_attempted)}")
    print(f"refused: {len(report.failed)}")
    for run, error in report.failed:
        print(f"  prep_sample {run.prep_sample_idx} run {run.processing_idx}: {error}")
    return 1 if report.failed else 0
