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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qiita", description="Qiita end-user CLI")
    parser.add_argument(
        "--base-url",
        default=os.environ.get("QIITA_CONTROL_PLANE_URL", "http://localhost:8080"),
        help="Control-plane base URL (default from QIITA_CONTROL_PLANE_URL or http://localhost:8080)",
    )
    parser.add_subparsers(dest="cmd", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    # No subcommands wired up yet — required=True on the subparser means
    # argparse handles "missing cmd" before we reach this point. The fall-
    # through below exists for the brief window where a subcommand has
    # been declared in the parser but no handler is wired here.
    parser.error(f"unknown command: {args.cmd}")
    return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
