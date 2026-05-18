"""qiita-admin — operator (admin-only) CLI for principal/role/token management.

Scope: operator/admin tasks only. End-user interactions with qiita (data-plane
operations, study/sample management, etc.) will live in a separate `qiita`
CLI; this module deliberately does not grow user-facing subcommands.

Subcommands:
  set-system-role  — direct DB UPDATE of qiita.principal.system_role.
                     Used for the bootstrap path (first system_admin) and
                     when the operator has DB access but no PAT yet. Refuses
                     to operate on the system principal (idx=1).
  whoami           — calls GET /api/v1/auth/whoami via the configured PAT.
  token revoke-all — calls POST /api/v1/admin/principal/{idx}/revoke-all-tokens.
  login            — drives the AuthRocket LoginRocket Web flow end-to-end.
                     Spawns a localhost loopback HTTP server, opens a
                     browser to /api/v1/auth/login?cli=1&port=N, waits for
                     the handoff to redirect back with a one-time code,
                     exchanges the code at /api/v1/auth/cli-exchange for
                     a PAT, and writes the PAT to ~/.qiita/token (0600).
  actions sync     — read every action YAML under --workflows-dir and upsert
                     YAML-authoritative columns into qiita.action. Direct DB
                     write; reads DATABASE_URL from env. Idempotent: re-runs
                     converge to the YAML state without touching operational
                     columns (enabled / first_seen_at / disabled_*).

Authentication for HTTP subcommands: read PAT from QIITA_TOKEN env var or
from ~/.qiita/token (mode 0600 expected). Loopback login flow, token I/O,
and the generic HTTP runner live in `cli._common` and are shared with the
end-user `qiita` CLI.
"""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import asyncpg
import httpx
from pydantic import ValidationError
from qiita_common.auth_constants import API_PREFIX, SYSTEM_PRINCIPAL_IDX, SystemRole

from qiita_control_plane.actions import (
    DuplicateActionError,
    load_actions,
    sync_actions,
)

from . import _common

# Direct-DB connection timeout for the bootstrap subcommand. Short because the
# DB is expected to be reachable on the operator's network; a multi-second
# stall here masks misconfiguration.
_DB_CONNECT_TIMEOUT_SECONDS = 5

# Derived from SystemRole so the role list isn't repeated anywhere in this
# file — adding `SystemRole.X` widens validation, error message, and `--help`
# automatically.
_VALID_ROLE_VALUES = tuple(r.value for r in SystemRole)


# ---------------------------------------------------------------------------
# Bootstrap subcommand: set-system-role (direct DB)
# ---------------------------------------------------------------------------


async def _set_system_role(database_url: str, email: str, role: str) -> int:
    """Update the principal's system_role by email lookup.

    Returns the principal_idx that was updated. Refuses to operate on
    idx=1 (the system principal). Raises with a clear message if the
    email is not found (the operator probably hasn't logged in via OIDC
    yet, which is what creates the principal+user pair).
    """
    if role not in _VALID_ROLE_VALUES:
        raise ValueError(f"role must be one of {' / '.join(_VALID_ROLE_VALUES)} (got {role!r})")
    try:
        conn = await asyncpg.connect(database_url, timeout=_DB_CONNECT_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001 — show full reason, including OS errors
        raise RuntimeError(
            f"could not connect to DATABASE_URL: {type(exc).__name__}: {exc}"
        ) from exc
    try:
        idx = await conn.fetchval(
            "SELECT u.principal_idx FROM qiita.user u WHERE u.email = $1",
            email,
        )
        if idx is None:
            raise RuntimeError(
                f"no user with email {email!r} — has this user logged in"
                " via OIDC at least once? First login creates the principal+user"
                " rows; only then can their role be set."
            )
        if idx == SYSTEM_PRINCIPAL_IDX:
            raise RuntimeError(
                f"refusing to modify the system principal (idx={SYSTEM_PRINCIPAL_IDX})"
            )
        await conn.execute(
            "UPDATE qiita.principal SET system_role = $1 WHERE idx = $2",
            role,
            idx,
        )
        return idx
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# actions sync — direct-DB upsert of YAML-authoritative columns
# ---------------------------------------------------------------------------


async def _sync_actions(database_url: str, workflows_dir: Path) -> dict:
    """Load every action YAML under workflows_dir, then upsert into
    qiita.action inside one transaction. Returns a dict with counts of
    inserted, updated, and total actions found."""
    actions = load_actions(workflows_dir)
    if not actions:
        return {"found": 0, "inserted": 0, "updated": 0}
    try:
        conn = await asyncpg.connect(database_url, timeout=_DB_CONNECT_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001 — show full reason, including OS errors
        raise RuntimeError(
            f"could not connect to DATABASE_URL: {type(exc).__name__}: {exc}"
        ) from exc
    try:
        result = await sync_actions(conn, actions)
    finally:
        await conn.close()
    return {"found": len(actions), **result}


# ---------------------------------------------------------------------------
# HTTP subcommands: token revoke-all
# ---------------------------------------------------------------------------


def _token_revoke_all(base_url: str, token: str, principal_idx: int) -> dict:
    resp = httpx.post(
        f"{base_url.rstrip('/')}{API_PREFIX}/admin/principal/{principal_idx}/revoke-all-tokens",
        headers={"Authorization": f"Bearer {token}"},
        timeout=_common.CLI_HTTP_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# argparse entry point
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qiita-admin", description="Qiita admin CLI")
    parser.add_argument(
        "--base-url",
        default=os.environ.get("QIITA_CONTROL_PLANE_URL", "http://localhost:8080"),
        help="Control-plane base URL (default from QIITA_CONTROL_PLANE_URL or http://localhost:8080)",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_role = sub.add_parser(
        "set-system-role",
        help="Direct-DB role update (bootstrap path)",
    )
    p_role.add_argument("--email", required=True)
    p_role.add_argument(
        "--role",
        required=True,
        choices=list(_VALID_ROLE_VALUES),
    )

    sub.add_parser("whoami", help="Print the authenticated principal")

    p_token = sub.add_parser("token", help="Token operations")
    p_token_sub = p_token.add_subparsers(dest="token_cmd", required=True)
    p_revoke = p_token_sub.add_parser("revoke-all", help="Bulk-revoke all of a principal's tokens")
    p_revoke.add_argument("--principal-idx", required=True, type=int)

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

    p_actions = sub.add_parser("actions", help="Action registry operations")
    p_actions_sub = p_actions.add_subparsers(dest="actions_cmd", required=True)
    p_actions_sync = p_actions_sub.add_parser(
        "sync",
        help="Upsert workflows YAMLs into qiita.action (YAML-authoritative columns only)",
    )
    p_actions_sync.add_argument(
        "--workflows-dir",
        type=Path,
        default=Path("workflows"),
        help="Directory to scan for action YAMLs (default: ./workflows)",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "set-system-role":
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            print("error: DATABASE_URL not set", file=sys.stderr)
            return 2
        try:
            idx = asyncio.run(_set_system_role(database_url, args.email, args.role))
        except (RuntimeError, ValueError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        print(f"updated principal idx={idx} system_role={args.role}")
        return 0

    if args.cmd == "whoami":
        return _common.run_http_subcommand(lambda t: _common.whoami(args.base_url, t))

    if args.cmd == "token":
        if args.token_cmd == "revoke-all":
            return _common.run_http_subcommand(
                lambda t: _token_revoke_all(args.base_url, t, args.principal_idx)
            )

    if args.cmd == "login":
        return _common.do_login(
            base_url=args.base_url,
            token_file=args.token_file,
            cli_command="qiita-admin login",
        )

    if args.cmd == "actions":
        if args.actions_cmd == "sync":
            database_url = os.environ.get("DATABASE_URL")
            if not database_url:
                print("error: DATABASE_URL not set", file=sys.stderr)
                return 2
            try:
                result = asyncio.run(_sync_actions(database_url, args.workflows_dir))
            except (FileNotFoundError, DuplicateActionError, ValidationError) as exc:
                print(f"error: {exc}", file=sys.stderr)
                return 1
            except RuntimeError as exc:
                print(f"error: {exc}", file=sys.stderr)
                return 1
            print(json.dumps(result, indent=2))
            return 0

    parser.error(f"unknown command: {args.cmd}")
    return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
