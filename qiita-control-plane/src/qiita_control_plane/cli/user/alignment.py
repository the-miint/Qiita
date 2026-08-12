"""qiita user CLI — alignment discovery.

`feature-table build` needs an `--alignment-idx`, and nothing in this CLI could
produce one: the two reads below are how a scientist finds out which alignments
were run over their pool and which of the pool's samples they may build a table
from. Both are one GET printed verbatim, so a new server-side field reaches the
user without a CLI change.

Both routes scope their answer to what the CALLER may read, which is what makes
them agree with the ticket mint: an alignment no sample of which is readable is
absent from the list entirely, and the cohort is a valid mint body by construction.

`_alignment_reference_idx` is the one piece of interpretation here, and it belongs
beside the response whose shape it reads.
"""

import argparse

from qiita_common.api_paths import (
    PATH_SEQUENCED_POOL_ALIGNMENT,
    PATH_SEQUENCED_POOL_ALIGNMENT_COHORT,
    PATH_SEQUENCING_RUN_PREFIX,
)

from .. import _common


def _fetch_pool_alignments(
    base_url: str, token: str, *, sequencing_run_idx: int, sequenced_pool_idx: int
) -> dict:
    """GET the alignments over a pool: per `alignment_idx`, the config `params` it ran
    with and how many of the caller's readable samples are completed for it."""
    sub_path = PATH_SEQUENCED_POOL_ALIGNMENT.format(
        sequencing_run_idx=sequencing_run_idx, sequenced_pool_idx=sequenced_pool_idx
    )
    return _common.call("GET", base_url, token, f"{PATH_SEQUENCING_RUN_PREFIX}{sub_path}")


def _fetch_alignment_cohort(
    base_url: str,
    token: str,
    *,
    sequencing_run_idx: int,
    sequenced_pool_idx: int,
    alignment_idx: int,
) -> dict:
    """GET the pool's samples that are BOTH readable by the caller and completed for
    this alignment — a valid mint body by construction, sorted so the same pool always
    produces the same request bytes.

    The whole body, not just its sample list: printed on its own a bare list says
    nothing about which alignment it belongs to.
    """
    sub_path = PATH_SEQUENCED_POOL_ALIGNMENT_COHORT.format(
        sequencing_run_idx=sequencing_run_idx,
        sequenced_pool_idx=sequenced_pool_idx,
        alignment_idx=alignment_idx,
    )
    return _common.call("GET", base_url, token, f"{PATH_SEQUENCING_RUN_PREFIX}{sub_path}")


def _alignment_reference_idx(alignments: dict, *, alignment_idx: int) -> int:
    """The reference an alignment ran against, read out of its own `params`.

    This is why `feature-table build` has no `--reference-idx` to get wrong. A
    user-supplied one that disagreed would fetch the genome map for a different
    reference, every `feature_idx` would miss it, and the relabel would then refuse on
    unlabelled genomes — loud, but a mistake the surface need not permit at all.
    """
    for summary in alignments.get("alignments", []):
        if summary["alignment_idx"] != alignment_idx:
            continue
        reference_idx = summary.get("params", {}).get("reference_idx")
        if reference_idx is None:
            raise ValueError(
                f"alignment {alignment_idx} records no reference_idx in its params "
                f"({sorted(summary.get('params', {}))}), so there is no genome map to "
                f"relabel its counts through."
            )
        return reference_idx
    present = sorted(s["alignment_idx"] for s in alignments.get("alignments", []))
    raise ValueError(
        f"alignment {alignment_idx} is not among this pool's alignments ({present}). An "
        f"alignment you can read no sample of is absent from that list entirely, so this "
        f"is either a wrong --alignment-idx or one over data you have no access to; "
        f"`qiita alignment list` shows the ones you can use."
    )


def _handle_alignment_list(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """List a pool's alignments. `params` is what distinguishes them — the reference,
    the aligner, and the mask each ran under — and the counts say which is complete
    enough to build a table from."""
    return _common.run_http_subcommand(
        lambda t: _fetch_pool_alignments(
            args.base_url,
            t,
            sequencing_run_idx=args.sequencing_run_idx,
            sequenced_pool_idx=args.sequenced_pool_idx,
        )
    )


def _handle_alignment_cohort(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Print the cohort a table would be built over. `feature-table build` resolves the
    same list itself when no `--prep-sample-idx` is given; this verb is for seeing it
    first, since the cohort is what coverage is measured over and therefore part of the
    scientific question."""
    return _common.run_http_subcommand(
        lambda t: _fetch_alignment_cohort(
            args.base_url,
            t,
            sequencing_run_idx=args.sequencing_run_idx,
            sequenced_pool_idx=args.sequenced_pool_idx,
            alignment_idx=args.alignment_idx,
        )
    )
