"""qiita-admin CLI — login / whoami / token revoke-all subcommands.

Split out of the former single-file ``cli.admin`` module; behavior unchanged.
"""

import argparse

from .. import _common

# ---------------------------------------------------------------------------
# HTTP subcommand helpers
# ---------------------------------------------------------------------------


def _token_revoke_all(base_url: str, token: str, principal_idx: int) -> dict:
    return _common.call(
        "POST",
        base_url,
        token,
        f"/admin/principal/{principal_idx}/revoke-all-tokens",
    )


def _handle_whoami(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(lambda t: _common.whoami(args.base_url, t))


def _handle_token_revoke_all(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(
        lambda t: _token_revoke_all(args.base_url, t, args.principal_idx)
    )


def _handle_login(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.do_login(
        base_url=args.base_url,
        token_file=args.token_file,
        cli_command="qiita-admin login",
    )
