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
from qiita_common.hashing import canonical_params_hash

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


def _alignment_summary(alignments: dict, *, alignment_idx: int) -> dict:
    """One alignment's summary out of the pool listing, with its `params` verified
    against the digest the server reported for them.

    **The verification is the reason this is a function rather than a dict lookup.**
    Everything downstream is read off `params` — which reference the whole table is
    relabelled through, and the record a published manifest makes of how the table was
    produced — but `params` arrive as JSON, and a client that trusted them without
    checking would derive all of that from whatever survived the round trip.
    Recomputing the server's own dedup digest costs one hash and turns that into a
    checked fact. A manifest also cites the digest as its reproducibility key, so
    copying one we never verified would publish a claim we cannot support.

    An absent `params_hash` is refused rather than skipped: a server too old to report
    one is a server whose params this client cannot vouch for.
    """
    for summary in alignments.get("alignments", []):
        if summary["alignment_idx"] != alignment_idx:
            continue
        params = summary.get("params", {})
        reported = summary.get("params_hash")
        computed = canonical_params_hash(params).hex()
        if reported is None:
            raise ValueError(
                f"alignment {alignment_idx} reports no params_hash, so its params cannot "
                f"be checked against what the server recorded. That field is required "
                f"here; a server too old to send one is a server this client cannot "
                f"vouch for."
            )
        if reported != computed:
            raise ValueError(
                f"alignment {alignment_idx} reports params_hash {reported!r} but its "
                f"params hash to {computed!r}. The config this table would be built "
                f"from is not the one the server recorded, so nothing derived from it "
                f"can be published. Report this rather than working around it — the "
                f"server refuses to store a config whose digest could drift this way, "
                f"so a mismatch here means something upstream of that check changed."
            )
        return summary
    present = sorted(s["alignment_idx"] for s in alignments.get("alignments", []))
    raise ValueError(
        f"alignment {alignment_idx} is not among this pool's alignments ({present}). An "
        f"alignment you can read no sample of is absent from that list entirely, so this "
        f"is either a wrong --alignment-idx or one over data you have no access to; "
        f"`qiita alignment list` shows the ones you can use."
    )


def _alignment_reference_idx(alignments: dict, *, alignment_idx: int) -> int:
    """The reference an alignment ran against, read out of its own `params`.

    This is why `feature-table build` has no `--reference-idx` to get wrong. A
    user-supplied one that disagreed would fetch the genome map for a different
    reference, every `feature_idx` would miss it, and the relabel would then refuse on
    unlabelled genomes — loud, but a mistake the surface need not permit at all.
    """
    params = _alignment_summary(alignments, alignment_idx=alignment_idx).get("params", {})
    reference_idx = params.get("reference_idx")
    if reference_idx is None:
        raise ValueError(
            f"alignment {alignment_idx} records no reference_idx in its params "
            f"({sorted(params)}), so there is no genome map to relabel its counts "
            f"through."
        )
    return reference_idx


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
