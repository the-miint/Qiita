"""qiita — end-user CLI for the Qiita control plane.

Scope: actions a regular user performs against a running deployment.
The parallel operator/admin CLI is `qiita-admin`; that one stays
scoped to principal/role/token management and is deliberately separate.

Shared with `qiita-admin`: the LoginRocket loopback flow, PAT file I/O,
and the generic token-read + HTTP + JSON-print runner all live in
`cli._common`. This module owns the user-facing argparse surface and
its subcommand handlers.

Authentication: HTTP subcommands read the PAT from QIITA_TOKEN env or
from ~/.qiita/token (mode 0600).
"""

import argparse
import os
import sys
from pathlib import Path

import httpx
from qiita_common.auth_constants import API_PREFIX

from . import _common

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
    resp = httpx.patch(
        f"{base_url.rstrip('/')}{API_PREFIX}/user/me",
        headers={"Authorization": f"Bearer {token}"},
        json=updates,
        timeout=_common.CLI_HTTP_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# argparse entry point
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qiita", description="Qiita end-user CLI")
    parser.add_argument(
        "--base-url",
        default=os.environ.get("QIITA_CONTROL_PLANE_URL", "http://localhost:8080"),
        help="Control-plane base URL (default from QIITA_CONTROL_PLANE_URL or http://localhost:8080)",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_login = sub.add_parser(
        "login",
        help="AuthRocket LoginRocket Web flow with localhost loopback",
    )
    p_login.add_argument(
        "--token-file",
        type=Path,
        default=_common.TOKEN_FILE_DEFAULT,
        help=f"Where to write the PAT (default {_common.TOKEN_FILE_DEFAULT})",
    )

    sub.add_parser("whoami", help="Print the authenticated principal")

    p_profile = sub.add_parser("profile", help="User profile operations")
    p_profile_sub = p_profile.add_subparsers(dest="profile_cmd", required=True)
    p_profile_set = p_profile_sub.add_parser(
        "set",
        help="Update affiliation / address / phone / orcid / mail prefs (PATCH /user/me)",
    )
    # All optional; argparse default None lets main() prune unset fields out
    # of the JSON body, matching the server's exclude_unset semantics.
    p_profile_set.add_argument("--affiliation")
    p_profile_set.add_argument("--address")
    p_profile_set.add_argument("--phone")
    p_profile_set.add_argument(
        "--orcid",
        help="ORCID iD (format NNNN-NNNN-NNNN-NNNX); server-side regex enforces shape",
    )
    p_profile_set.add_argument(
        "--receive-processing-emails",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Opt in/out of processing-status emails (use --no- to opt out)",
    )

    return parser


def _profile_set_updates(args: argparse.Namespace) -> dict:
    """Build the PATCH body from parsed args. Only fields the caller actually
    supplied appear in the dict — argparse's default of None marks unset for
    every flag here, including the BooleanOptionalAction one."""
    updates: dict = {}
    if args.affiliation is not None:
        updates["affiliation"] = args.affiliation
    if args.address is not None:
        updates["address"] = args.address
    if args.phone is not None:
        updates["phone"] = args.phone
    if args.orcid is not None:
        updates["orcid"] = args.orcid
    if args.receive_processing_emails is not None:
        updates["receive_processing_emails"] = args.receive_processing_emails
    return updates


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "login":
        return _common.do_login(
            base_url=args.base_url,
            token_file=args.token_file,
            cli_command="qiita login",
        )

    if args.cmd == "whoami":
        return _common.run_http_subcommand(lambda t: _common.whoami(args.base_url, t))

    if args.cmd == "profile":
        if args.profile_cmd == "set":
            updates = _profile_set_updates(args)
            if not updates:
                parser.error(
                    "qiita profile set requires at least one of"
                    " --affiliation / --address / --phone / --orcid /"
                    " --receive-processing-emails / --no-receive-processing-emails"
                )
            return _common.run_http_subcommand(lambda t: _patch_user_me(args.base_url, t, updates))

    parser.error(f"unknown command: {args.cmd}")
    return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
