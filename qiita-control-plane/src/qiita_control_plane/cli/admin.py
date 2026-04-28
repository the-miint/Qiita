"""qiita-admin — operator CLI for principal/role/token management.

Subcommands:
  set-system-role  — direct DB UPDATE of qiita.principal.system_role.
                     Used for the bootstrap path (first system_admin) and
                     when the operator has DB access but no PAT yet. Refuses
                     to operate on the system principal (idx=1).
  whoami           — calls GET /api/v1/auth/whoami via the configured PAT.
  token revoke-all — calls POST /api/v1/admin/principals/{idx}/revoke-all-tokens.
  login            — DEFERRED to Phase J. The full PKCE + code-exchange
                     flow needs an OIDC test harness for code-exchange that
                     isn't built yet. For now, obtain an AuthRocket JWT
                     out-of-band and call POST /api/v1/auth/pat directly.

Authentication for HTTP subcommands: read PAT from QIITA_TOKEN env var or
from ~/.qiita/token (mode 0600 expected).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

import asyncpg
import httpx
from qiita_common.auth_constants import API_PREFIX, SYSTEM_PRINCIPAL_IDX, SystemRole

_TOKEN_FILE_DEFAULT = Path.home() / ".qiita" / "token"

# Direct-DB connection timeout for the bootstrap subcommand. Short because the
# DB is expected to be reachable on the operator's network; a multi-second
# stall here masks misconfiguration.
_DB_CONNECT_TIMEOUT_SECONDS = 5
# HTTP timeout for CLI-driven control-plane calls. Generous enough to tolerate
# transient network blips without papering over a hung server.
_CLI_HTTP_TIMEOUT_SECONDS = 10

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
# HTTP subcommands: whoami, token revoke-all
# ---------------------------------------------------------------------------


def _read_token(token_file: Path | None = None) -> str:
    """Read PAT from QIITA_TOKEN env or from a token file (default ~/.qiita/token).
    Raises with a clear actionable message if neither is set."""
    env = os.environ.get("QIITA_TOKEN")
    if env:
        return env.strip()
    path = token_file or _TOKEN_FILE_DEFAULT
    if path.is_file():
        return path.read_text().strip()
    raise RuntimeError(
        f"no PAT found — set QIITA_TOKEN or write a token to {path}"
        f" (use POST {API_PREFIX}/auth/pat to mint one)"
    )


def _whoami(base_url: str, token: str) -> dict:
    resp = httpx.get(
        f"{base_url.rstrip('/')}{API_PREFIX}/auth/whoami",
        headers={"Authorization": f"Bearer {token}"},
        timeout=_CLI_HTTP_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    return resp.json()


def _token_revoke_all(base_url: str, token: str, principal_idx: int) -> dict:
    resp = httpx.post(
        f"{base_url.rstrip('/')}{API_PREFIX}/admin/principals/{principal_idx}/revoke-all-tokens",
        headers={"Authorization": f"Bearer {token}"},
        timeout=_CLI_HTTP_TIMEOUT_SECONDS,
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
        help="OIDC PKCE login (DEFERRED — see Phase J)",
    )
    p_login.add_argument(
        "--token-file",
        type=Path,
        default=_TOKEN_FILE_DEFAULT,
        help=f"Where to write the PAT (default {_TOKEN_FILE_DEFAULT})",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "set-system-role":
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            print("DATABASE_URL not set", file=sys.stderr)
            return 2
        try:
            idx = asyncio.run(_set_system_role(database_url, args.email, args.role))
        except (RuntimeError, ValueError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        print(f"updated principal idx={idx} system_role={args.role}")
        return 0

    if args.cmd == "whoami":
        try:
            token = _read_token()
        except RuntimeError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        try:
            body = _whoami(args.base_url, token)
        except httpx.HTTPStatusError as exc:
            print(f"http error {exc.response.status_code}: {exc.response.text}", file=sys.stderr)
            return 1
        import json as _json

        print(_json.dumps(body, indent=2))
        return 0

    if args.cmd == "token":
        if args.token_cmd == "revoke-all":
            try:
                token = _read_token()
            except RuntimeError as exc:
                print(f"error: {exc}", file=sys.stderr)
                return 1
            try:
                body = _token_revoke_all(args.base_url, token, args.principal_idx)
            except httpx.HTTPStatusError as exc:
                print(
                    f"http error {exc.response.status_code}: {exc.response.text}",
                    file=sys.stderr,
                )
                return 1
            import json as _json

            print(_json.dumps(body, indent=2))
            return 0

    if args.cmd == "login":
        print(
            "qiita-admin login is deferred to Phase J. For now: obtain an"
            f" AuthRocket JWT out-of-band and call POST {API_PREFIX}/auth/pat to"
            f" mint a PAT, then write it to {args.token_file} (mode 0600).",
            file=sys.stderr,
        )
        return 2

    parser.error(f"unknown command: {args.cmd}")
    return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
