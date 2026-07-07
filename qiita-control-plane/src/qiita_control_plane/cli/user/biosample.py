"""qiita user CLI — biosample subcommand.

Split out of the former single-file ``cli.user`` module; behavior unchanged.
"""

import argparse

from qiita_common.models import (
    BiosampleImportRequest,
)

from .. import _common
from ._helpers import _build_body


def _post_biosample(base_url: str, token: str, study_idx: int, body: dict) -> dict:
    """POST /api/v1/study/{study_idx}/biosample with the (already-pruned) body.

    The route currently requires owner_idx in the body explicitly; the CLI
    handler resolves it via whoami when --owner-idx is omitted so the
    caller can run `qiita biosample create` for themselves without first
    chasing their own principal_idx.
    """
    return _common.call("POST", base_url, token, f"/study/{study_idx}/biosample", json=body)


def _handle_biosample_create(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Mint a biosample on a study, auto-defaulting owner_idx to the caller.

    The whoami lookup that resolves a missing --owner-idx shares the
    token-read path with the POST itself, so both calls happen inside the
    same `run_http_subcommand` invocation. Body construction runs after
    that resolution so BiosampleImportRequest sees a populated owner_idx.
    """
    args.metadata = _common.parse_kv_pairs(args.metadata, parser, flag="--metadata")

    def _run(token: str) -> dict:
        _common.resolve_owner_idx(args, args.base_url, token)
        body = _build_body(BiosampleImportRequest, args, parser)
        return _post_biosample(args.base_url, token, args.study_idx, body)

    return _common.run_http_subcommand(_run)
