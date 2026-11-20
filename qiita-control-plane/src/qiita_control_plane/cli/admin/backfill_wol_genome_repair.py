"""`qiita-admin backfill wol-genome-repair`: fold a Web of Life reference's `GCA_G<n>`
genomes into `G<n>` and record each genome's NCBI parent assembly. The work is
`backfill/wol_genome_repair.py`; this is the operator's view of it.

A one-off (the work module says when it is retired), kept in its own module so its
removal touches `cli/admin/__init__.py` only at the two lines that register the parser.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import asyncpg

from ...backfill.wol_genome_repair import (
    RepairCounts,
    RepairPlan,
    RepairPostconditionFailed,
    RepairRefused,
    apply_repair,
    plan_repair,
)
from ._helpers import EXIT_PRECONDITION_FAILED, open_admin_pool, requires_database_url


def add_wol_genome_repair_parser(backfill_sub: argparse._SubParsersAction) -> None:
    p = backfill_sub.add_parser(
        "wol-genome-repair",
        help="Fold a WoL reference's GCA_G<n> genomes into G<n> and record their parents",
        description=(
            "A Web of Life reference loaded with GCA_ in front of each WoL ID gave each of"
            " its sequences a second genome, GCA_G<n>; most were already linked to G<n>,"
            " so its feature tables split those genomes' reads between the two. For each"
            " GCA_G<n>: link its sequence to G<n> (creating G<n> if missing), remove the"
            " sequence's links to other WoL IDs, then delete GCA_G<n>, which retires its"
            " published labels. Then record, from the WoL release's metadata.txt, the"
            " GenBank or RefSeq assembly that each of the reference's genomes was derived"
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
        _print_counts(f"reference {plan.reference_idx}", plan.counts, done=False)
        if not args.execute or plan.nothing_to_do:
            return plan, None
        return plan, await apply_repair(pool, plan, manifest_path=args.manifest)
    finally:
        await pool.close()


# Each count's label before the run, and after it.
_COUNT_LABELS = (
    ("already_linked", "already linked to G<n>", "already linked to G<n>"),
    ("to_link", "to link to G<n>", "linked to G<n>"),
    ("to_create", "G<n> to create", "G<n> created"),
    ("stale_links", "stale WoL links to remove", "stale WoL links removed"),
    ("labels_to_retire", "labels to retire", "labels retired"),
    ("derivations_to_add", "derivations to add", "derivations added"),
    ("derivations_present", "derivations present", "derivations already present"),
)


def _print_counts(heading: str, c: RepairCounts, *, done: bool) -> None:
    folded = c.already_linked + c.to_link + c.to_create
    print(f"{heading}: {folded} GCA_G genome(s) {'folded' if done else 'to fold'}")
    labels = [(after if done else before, getattr(c, f)) for f, before, after in _COUNT_LABELS]
    width = max(len(label) for label, _n in labels)
    for label, n in labels:
        print(f"  {label:<{width}} : {n}")


@requires_database_url
def _handle(args: argparse.Namespace, parser: argparse.ArgumentParser, database_url: str) -> int:
    if args.execute and args.manifest is None:
        print("error: --execute needs --manifest PATH", file=sys.stderr)
        return EXIT_PRECONDITION_FAILED
    if args.manifest is not None and not args.execute:
        print(
            "error: --manifest is only written with --execute; a dry run writes nothing",
            file=sys.stderr,
        )
        return EXIT_PRECONDITION_FAILED

    try:
        plan, done = asyncio.run(_run(database_url, args))
    except RepairRefused as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 1
    except RepairPostconditionFailed as exc:
        print(f"postcondition failed, rolled back; nothing was written: {exc}", file=sys.stderr)
        return 1
    except (RuntimeError, OSError, asyncpg.PostgresError, asyncpg.InterfaceError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if done is None and args.execute:
        print("\nnothing to do; no manifest written.")
        return 0
    if done is None:
        print(
            "\nDRY RUN — nothing written. Compare the counts with what you expect, then"
            " re-run with --execute --manifest PATH."
        )
        return 0
    print()
    _print_counts(f"repaired reference {plan.reference_idx}", done, done=True)
    print(f"manifest: {args.manifest}")
    return 0
