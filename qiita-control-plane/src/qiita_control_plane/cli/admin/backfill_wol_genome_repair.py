"""`qiita-admin backfill wol-genome-repair`: fold a Web of Life reference's `GCA_G<n>`
genomes into `G<n>` and record each genome's NCBI parent assembly. The work is
`backfill/wol_genome_repair.py`; this is the operator's view of it.

A one-off, kept in its own module so its removal touches `cli/admin/__init__.py` only
at the two lines that register the parser.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

from ...backfill.wol_genome_repair import (
    RepairCounts,
    RepairPlan,
    RepairPostconditionFailed,
    RepairRefused,
    apply_repair,
    plan_repair,
)
from ._helpers import open_admin_pool


def add_wol_genome_repair_parser(backfill_sub: argparse._SubParsersAction) -> None:
    p = backfill_sub.add_parser(
        "wol-genome-repair",
        help="Fold a WoL reference's GCA_G<n> genomes into G<n> and record their parents",
        description=(
            "A Web of Life reference loaded with GCA_ in front of each WoL ID left every"
            " one of its sequences linked to two genomes, G<n> and GCA_G<n>, so its"
            " feature tables split each genome's reads between the two. For each"
            " GCA_G<n>: link its sequence to G<n> (creating G<n> if missing), remove the"
            " sequence's links to other WoL IDs, then delete GCA_G<n>, which retires its"
            " published labels. Then record, from the WoL release's metadata.txt, the"
            " GenBank or RefSeq assembly each of the reference's genomes was derived"
            " from. One transaction; anything unexpected refuses the whole run, naming"
            " the rows. Dry-run by default. Needs DATABASE_URL."
        ),
    )
    p.add_argument(
        "--reference-idx",
        type=int,
        required=True,
        help="The WoL reference whose FASTA headers name each sequence's genome.",
    )
    p.add_argument(
        "--metadata",
        type=Path,
        required=True,
        help="The WoL release's metadata.txt (tab-separated; genome_id, assembly_accession).",
    )
    p.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help=(
            "With --execute: a new CSV listing every row deleted and label retired."
            " Never overwritten."
        ),
    )
    p.add_argument(
        "--execute",
        action="store_true",
        help="Write the repair (default: dry-run, report only, no writes).",
    )
    p.set_defaults(handler=_handle)


async def _run(
    database_url: str, args: argparse.Namespace
) -> tuple[RepairPlan, RepairCounts | None]:
    pool = await open_admin_pool(database_url)
    try:
        plan = await plan_repair(
            pool, reference_idx=args.reference_idx, metadata_path=args.metadata
        )
        _print_counts(f"reference {plan.reference_idx}", plan.counts)
        if not args.execute:
            return plan, None
        return plan, await apply_repair(pool, plan, manifest_path=args.manifest)
    finally:
        await pool.close()


def _print_counts(heading: str, c: RepairCounts) -> None:
    folded = c.already_linked + c.to_link + c.to_create
    print(f"{heading}: {folded} GCA_G genome(s) to fold")
    for label, n in (
        ("A, already linked to G<n>", c.already_linked),
        ("B, to link to G<n>", c.to_link),
        ("C, G<n> to create", c.to_create),
        ("stale WoL links to remove", c.stale_links),
        ("labels to retire", c.labels_to_retire),
        ("derivations to add", c.derivations_to_add),
        ("derivations present", c.derivations_present),
    ):
        print(f"  {label:<26}: {n}")


def _handle(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("error: DATABASE_URL not set", file=sys.stderr)
        return 2
    if args.execute and args.manifest is None:
        print("error: --execute needs --manifest PATH", file=sys.stderr)
        return 2
    if args.manifest is not None and not args.execute:
        print(
            "error: --manifest is only written with --execute; a dry run writes nothing",
            file=sys.stderr,
        )
        return 2

    try:
        plan, done = asyncio.run(_run(database_url, args))
    except RepairRefused as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 1
    except RepairPostconditionFailed as exc:
        print(f"postcondition failed, rolled back; nothing was written: {exc}", file=sys.stderr)
        return 1
    except (RuntimeError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if done is None:
        print(
            "\nDRY RUN — nothing written. Compare the counts with what you expect, then"
            " re-run with --execute --manifest PATH."
        )
        return 0
    print()
    _print_counts(f"repaired reference {plan.reference_idx}", done)
    print(f"manifest: {args.manifest}")
    return 0
