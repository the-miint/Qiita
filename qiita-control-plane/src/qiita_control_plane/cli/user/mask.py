"""qiita user CLI — read-only mask-definition subcommands.

`qiita mask list` / `show` / `samples` are the client-side answer to "which mask
is this pool filtered under, and which of its samples are ready to act on?" — the
three questions that otherwise need a psql shell on the deploy host and
DATABASE_URL. A `mask_idx` is a required input to `long-read-assembly`, whose
audience includes a plain `user`, so the reads sit in this CLI rather than
`qiita-admin` (which keeps the destructive `mask delete` / `purge-failed`).

Thin clients: each verb is one GET, printed verbatim, so a new server-side field
reaches the user without a CLI change.
"""

import argparse

from qiita_common.api_paths import PATH_MASK_DEFINITION_PREFIX

from .. import _common


def _list_mask_definitions(
    base_url: str,
    token: str,
    *,
    sequenced_pool_idx: int | None,
    prep_sample_idx: int | None,
) -> dict:
    """GET /api/v1/mask-definition. Returns each mask's config plus its
    completed / pending sample tally under the same filters."""
    return _common.call(
        "GET",
        base_url,
        token,
        PATH_MASK_DEFINITION_PREFIX,
        params=_common.filter_params(
            sequenced_pool_idx=sequenced_pool_idx, prep_sample_idx=prep_sample_idx
        ),
    )


def _get_mask_definition(base_url: str, token: str, mask_idx: int) -> dict:
    """GET /api/v1/mask-definition/{mask_idx}. Returns the mask's config blob —
    host/spike-in reference idxs and the resolved QC constants."""
    return _common.call("GET", base_url, token, f"{PATH_MASK_DEFINITION_PREFIX}/{mask_idx}")


def _list_mask_prep_samples(
    base_url: str,
    token: str,
    mask_idx: int,
    *,
    sequenced_pool_idx: int | None,
) -> dict:
    """GET /api/v1/mask-definition/{mask_idx}/prep-sample. Returns one row per
    sample masked under this mask, with its state and which masking path
    resolved it."""
    return _common.call(
        "GET",
        base_url,
        token,
        f"{PATH_MASK_DEFINITION_PREFIX}/{mask_idx}/prep-sample",
        params=_common.filter_params(sequenced_pool_idx=sequenced_pool_idx),
    )


def _handle_mask_list(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """List masks with their sample tallies. Filter by --sequenced-pool-idx to
    separate the masks one pool carries: `params` distinguishes them by config
    (a non-null host_rype_reference_idx is the human-filtered one) and the tally
    says which is usable."""
    return _common.run_http_subcommand(
        lambda t: _list_mask_definitions(
            args.base_url,
            t,
            sequenced_pool_idx=args.sequenced_pool_idx,
            prep_sample_idx=args.prep_sample_idx,
        )
    )


def _handle_mask_show(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Print one mask's config, so what a filter ran with is quotable rather than
    read out of the orchestrator source."""
    return _common.run_http_subcommand(
        lambda t: _get_mask_definition(args.base_url, t, args.mask_idx)
    )


def _handle_mask_samples(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """List the samples masked under one mask. The `completed` rows are the set a
    masked-read pull or an assembly submission can act on."""
    return _common.run_http_subcommand(
        lambda t: _list_mask_prep_samples(
            args.base_url,
            t,
            args.mask_idx,
            sequenced_pool_idx=args.sequenced_pool_idx,
        )
    )
