"""qiita user CLI — study subcommand.

Split out of the former single-file ``cli.user`` module; behavior unchanged.
"""

import argparse

from qiita_common.models import (
    StudyCreate,
)

from .. import _common
from ._helpers import _build_body


def _post_study(base_url: str, token: str, body: dict) -> dict:
    """POST /api/v1/study with the (already-pruned) body. Owner defaults to
    the caller server-side; the CLI does not surface --owner-idx because
    naming a different owner requires wet_lab_admin+ (lab-tech-on-behalf),
    out of scope for the regular-user CLI."""
    return _common.call("POST", base_url, token, "/study", json=body)


def _handle_study_create(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Mint a study owned by the caller. --extra-metadata is parsed from
    JSON before Pydantic validation so a malformed paste surfaces as a
    clean argparse exit 2."""
    args.extra_metadata = _common.parse_json_arg(
        args.extra_metadata, parser, flag="--extra-metadata"
    )
    body = _build_body(StudyCreate, args, parser)
    return _common.run_http_subcommand(lambda t: _post_study(args.base_url, t, body))
