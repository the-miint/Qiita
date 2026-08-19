"""qiita user CLI — login / whoami / profile subcommands.

Split out of the former single-file ``cli.user`` module; behavior unchanged.
"""

import argparse

from qiita_common.models import (
    UserUpdate,
)

from .. import _common
from ._helpers import _build_body

# ---------------------------------------------------------------------------
# HTTP helpers (call sites for individual endpoints)
# ---------------------------------------------------------------------------


def _patch_user_me(base_url: str, token: str, updates: dict) -> dict:
    """PATCH /api/v1/user/me with the (already-pruned) updates dict.

    Only the fields the caller actually set are sent; unset ones stay
    absent so the server's `exclude_unset` SET-clause builder never
    UPDATEs a field the user didn't ask about. With an empty dict, the
    route round-trips the current profile — handy as a side-effect
    "show" but argparse requires at least one --flag here so empty
    bodies don't slip through silently.
    """
    return _common.call("PATCH", base_url, token, "/user/me", json=updates)


# ---------------------------------------------------------------------------
# Subcommand handlers (registered via parser.set_defaults(handler=...))
# ---------------------------------------------------------------------------


def _handle_login(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.do_login(
        base_url=args.base_url,
        token_file=args.token_file,
        cli_command="qiita login",
    )


def _handle_whoami(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(lambda t: _common.whoami(args.base_url, t))


def _handle_profile_set(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    updates = _build_body(UserUpdate, args, parser)
    if not updates:
        parser.error(
            "qiita profile set requires at least one of"
            " --affiliation / --address / --phone / --orcid /"
            " --receive-processing-emails / --no-receive-processing-emails"
        )
    return _common.run_http_subcommand(lambda t: _patch_user_me(args.base_url, t, updates))
