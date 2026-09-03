"""qiita user CLI — read-only processing-run subcommands.

`qiita processing list` / `show` / `samples` are the client-side answer to "which
assembly run is this, and are its samples ready to submit against?" — the
questions that otherwise need a psql shell on the deploy host and DATABASE_URL.
A `processing_idx` is a required input to `align-denovo` (the runner reads it as
`assembly_processing_idx`). The reads sit in this CLI because they are
credentialed API calls the server's own scope guard gates, which is the
placement rule in `cli/user/__init__.py` — the three routes are
`Scope.PREP_SAMPLE_READ`, below what submitting the workflow itself takes.

The mask twin of this module is `cli/user/mask.py`: the two surfaces answer the
same shape of question at the two identities an `align-denovo` submission names.

Thin clients: each verb is one GET, printed verbatim, so a new server-side field
reaches the user without a CLI change.
"""

import argparse

from qiita_common.api_paths import (
    PATH_PROCESSING_BY_IDX,
    PATH_PROCESSING_PREFIX,
    PATH_PROCESSING_PREP_SAMPLE,
    PATH_PROCESSING_ROOT,
)

from .. import _common


def _list_processing(
    base_url: str,
    token: str,
    *,
    sequenced_pool_idx: int | None,
    prep_sample_idx: int | None,
    status: str | None,
) -> dict:
    """GET /api/v1/processing. Returns each run's `params` — the `mask_idx` whose
    pass-set was assembled and the assembler that ran — plus its completed /
    pending / no_data / invalidated sample tally under the same filters."""
    return _common.call(
        "GET",
        base_url,
        token,
        f"{PATH_PROCESSING_PREFIX}{PATH_PROCESSING_ROOT}",
        params=_common.filter_params(
            sequenced_pool_idx=sequenced_pool_idx,
            prep_sample_idx=prep_sample_idx,
            status=status,
        ),
    )


def _get_processing(base_url: str, token: str, processing_idx: int) -> dict:
    """GET /api/v1/processing/{processing_idx}. Returns the run's params blob and
    its config lifecycle."""
    sub_path = PATH_PROCESSING_BY_IDX.format(processing_idx=processing_idx)
    return _common.call("GET", base_url, token, f"{PATH_PROCESSING_PREFIX}{sub_path}")


def _list_processing_prep_samples(
    base_url: str,
    token: str,
    processing_idx: int,
    *,
    sequenced_pool_idx: int | None,
) -> dict:
    """GET /api/v1/processing/{processing_idx}/prep-sample. Returns one row per
    sample assembled under this run, with the `assembly_sample` gate state a
    submission is admitted or refused on."""
    sub_path = PATH_PROCESSING_PREP_SAMPLE.format(processing_idx=processing_idx)
    return _common.call(
        "GET",
        base_url,
        token,
        f"{PATH_PROCESSING_PREFIX}{sub_path}",
        params=_common.filter_params(sequenced_pool_idx=sequenced_pool_idx),
    )


def _handle_processing_list(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """List assembly runs with their sample tallies. Filter by
    --sequenced-pool-idx to separate the several identities one pool can carry;
    `routes/processing.py` states what separates them."""
    return _common.run_http_subcommand(
        lambda t: _list_processing(
            args.base_url,
            t,
            sequenced_pool_idx=args.sequenced_pool_idx,
            prep_sample_idx=args.prep_sample_idx,
            status=args.status,
        )
    )


def _handle_processing_show(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Print one run's params, so what a set of contigs was assembled from is
    quotable without reading `qiita.processing` directly."""
    return _common.run_http_subcommand(
        lambda t: _get_processing(args.base_url, t, args.processing_idx)
    )


def _handle_processing_samples(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """List the samples assembled under one run, each with its `assembly_sample`
    gate state. What a submission does with each state is mapped in
    `runner/_alignment.py`, over the contract on
    `repositories.assembly.fetch_assembly_sample_state`."""
    return _common.run_http_subcommand(
        lambda t: _list_processing_prep_samples(
            args.base_url,
            t,
            args.processing_idx,
            sequenced_pool_idx=args.sequenced_pool_idx,
        )
    )
