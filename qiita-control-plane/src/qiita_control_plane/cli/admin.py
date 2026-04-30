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
  token revoke-all — calls POST /api/v1/admin/principals/{idx}/revoke-all-tokens.
  login            — drives the AuthRocket LoginRocket Web flow end-to-end.
                     Spawns a localhost loopback HTTP server, opens a
                     browser to /api/v1/auth/login?cli=1&port=N, waits for
                     the handoff to redirect back with a one-time code,
                     exchanges the code at /api/v1/auth/cli-exchange for
                     a PAT, and writes the PAT to ~/.qiita/token (0600).

Authentication for HTTP subcommands: read PAT from QIITA_TOKEN env var or
from ~/.qiita/token (mode 0600 expected).
"""

import argparse
import asyncio
import http.server
import json
import os
import sys
import threading
import urllib.parse
import webbrowser
from collections.abc import Callable
from pathlib import Path

import asyncpg
import httpx
from qiita_common.auth_constants import API_PREFIX, SYSTEM_PRINCIPAL_IDX, SystemRole

_TOKEN_FILE_DEFAULT = Path.home() / ".qiita" / "token"

# How long to wait for the AuthRocket round-trip + browser bounce. 5 minutes
# is generous; longer values mostly hide bugs (browser crashed, user gave up).
_LOGIN_WAIT_TIMEOUT_SECONDS = 300

# HTML rendered to the browser at the loopback after the handoff redirect
# delivers the ot_code. Friendly "you can close this tab now" message. The
# page must NOT include any script that touches the URL — we don't want
# accidental cross-site reads of the ot_code from extensions etc. The
# server consumes the code immediately upon receipt, so even if the URL
# leaks the code is dead within the cli_login_code_ttl window.
_LOOPBACK_DONE_HTML = b"""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>qiita login complete</title>
<style>
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
       max-width: 540px; margin: 4em auto; padding: 0 1em; color: #1a1a1a;
       text-align: center; }
h1   { margin-bottom: 0.4em; }
.muted { color: #555; font-size: 0.95em; }
</style>
</head>
<body>
<h1>You are logged in.</h1>
<p>Return to your terminal &mdash; the CLI has captured your token.</p>
<p class="muted">You can close this tab.</p>
</body>
</html>
"""

# Direct-DB connection timeout for the bootstrap subcommand. Short because the
# DB is expected to be reachable on the operator's network; a multi-second
# stall here masks misconfiguration.
_DB_CONNECT_TIMEOUT_SECONDS = 5
# HTTP timeout for CLI-driven control-plane calls. Generous enough to tolerate
# transient network blips without papering over a hung server.
_CLI_HTTP_TIMEOUT_SECONDS = 10

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


def _run_http_subcommand(call: Callable[[str], dict]) -> int:
    """Token-read + httpx invoke + json print, common to every HTTP subcommand.

    `call` accepts a PAT string and returns the parsed JSON body. Any
    RuntimeError from token-read or HTTPStatusError from the request is
    converted to a stderr message + exit code 1.
    """
    try:
        token = _read_token()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    try:
        body = call(token)
    except httpx.HTTPStatusError as exc:
        print(f"http error {exc.response.status_code}: {exc.response.text}", file=sys.stderr)
        return 1
    print(json.dumps(body, indent=2))
    return 0


# ---------------------------------------------------------------------------
# login — AuthRocket LoginRocket Web flow with localhost loopback
# ---------------------------------------------------------------------------


class _LoopbackResult:
    """Mailbox for the loopback HTTP handler to deposit the captured ot_code
    or an error. The main thread blocks on `event` until the handler fires."""

    # __slots__ strings stay as literals: they're a metaprogramming declaration
    # that must match the bare-identifier attribute accesses below (`self.event`,
    # `result.ot_code`, …). Promoting them to constants would force `getattr`
    # everywhere, which is strictly worse than dot access.
    __slots__ = ("event", "ot_code", "error")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.ot_code: str | None = None
        self.error: str | None = None


def _loopback_handler_factory(result: _LoopbackResult):
    """Build a one-shot http.server handler that captures `?ot_code=<value>`.

    Anything that's not exactly `GET /` (with the right query) is met with a
    short 404 — favicon probes etc. shouldn't end the loop. Once we capture
    a code, we set the event so the main thread can move on.
    """

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 (BaseHTTPRequestHandler convention)
            url = urllib.parse.urlparse(self.path)
            if url.path != "/":
                self.send_error(404, "not found")
                return
            params = urllib.parse.parse_qs(url.query)
            ot_code_values = params.get("ot_code")
            if not ot_code_values:
                # Probably a stray probe (favicon, etc.) — ignore quietly.
                self.send_error(404, "missing ot_code")
                return
            result.ot_code = ot_code_values[0]
            # Signal the main thread *before* writing the response: the event
            # means "ot_code is captured," not "browser has the done-page."
            # If we set it after wfile.write the test client (or a fast user)
            # can return from .read() and check the event before this line
            # runs — a race CI exposes reliably. server.shutdown() in the
            # caller still waits for this handler to finish, so the browser
            # gets the full response either way.
            result.event.set()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(_LOOPBACK_DONE_HTML)))
            self.end_headers()
            self.wfile.write(_LOOPBACK_DONE_HTML)

        def log_message(self, *args, **kwargs):
            # Stay quiet; the CLI prints its own status.
            pass

    return Handler


def _bind_loopback(*, preferred_ports: tuple[int, ...] = ()) -> tuple[http.server.HTTPServer, int]:
    """Bind a loopback HTTP server.

    AuthRocket realms vary in how strictly they validate `redirect_uri`:
    qiita-dev accepts arbitrary `http://127.0.0.1:<port>` callbacks (so an
    OS-picked free port works), but a stricter realm (e.g. RC/prod) may
    pre-register a fixed set. In the latter case operators populate
    `preferred_ports` with the registered ports; this function tries each
    in turn and falls back to OS-picked when none is preferred or all are
    taken. Returns (server, bound_port).
    """
    for port in preferred_ports:
        try:
            srv = http.server.HTTPServer(("127.0.0.1", port), http.server.BaseHTTPRequestHandler)
            return srv, port
        except OSError:
            continue
    srv = http.server.HTTPServer(("127.0.0.1", 0), http.server.BaseHTTPRequestHandler)
    return srv, srv.server_address[1]


def _write_token(path: Path, plaintext: str) -> None:
    """Write the PAT plaintext to `path` with mode 0600.

    Creates the parent directory if missing (with mode 0700). Overwrites
    any existing token; the caller chose to log in, so a stale token at the
    target is being deliberately replaced.
    """
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    # Write to a temp file then atomic-rename so an interrupted write doesn't
    # leave a half-token on disk.
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(plaintext)
    os.chmod(tmp, 0o600)
    tmp.replace(path)


def _do_login(*, base_url: str, token_file: Path) -> int:
    """Drive the LoginRocket Web flow end-to-end.

    Steps:
      1. Bind localhost loopback HTTP server.
      2. Open browser to {base_url}/api/v1/auth/login?cli=1&port=N.
      3. Wait for the handoff to redirect back with `?ot_code=<value>`.
      4. POST the ot_code to /api/v1/auth/cli-exchange, receive the PAT.
      5. Write PAT to `token_file`, print whoami summary.
    """
    base = base_url.rstrip("/")
    result = _LoopbackResult()
    server, port = _bind_loopback()
    server.RequestHandlerClass = _loopback_handler_factory(result)

    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    login_url = f"{base}{API_PREFIX}/auth/login?cli=1&port={port}"
    print("Opening browser for AuthRocket login...", file=sys.stderr)
    print(f"  If the browser doesn't open, visit: {login_url}", file=sys.stderr)
    opened = False
    try:
        opened = webbrowser.open(login_url)
    except Exception:
        opened = False
    if not opened:
        print(
            "  webbrowser.open() returned False — paste the URL above into a browser manually.",
            file=sys.stderr,
        )

    try:
        if not result.event.wait(timeout=_LOGIN_WAIT_TIMEOUT_SECONDS):
            print(
                f"error: timed out after {_LOGIN_WAIT_TIMEOUT_SECONDS}s waiting for the"
                " browser callback. Re-run `qiita-admin login`. If the browser"
                " never reached the qiita server, check your QIITA_CONTROL_PLANE_URL.",
                file=sys.stderr,
            )
            return 1
    finally:
        # Stop the loopback server promptly; we don't need it after the
        # event fires (or after timeout — either way we're done).
        server.shutdown()
        server.server_close()

    if result.ot_code is None:
        print(
            f"error: loopback received no ot_code (handler error: {result.error})",
            file=sys.stderr,
        )
        return 1

    # Exchange the code for the PAT plaintext.
    try:
        resp = httpx.post(
            f"{base}{API_PREFIX}/auth/cli-exchange",
            json={"ot_code": result.ot_code},
            timeout=_CLI_HTTP_TIMEOUT_SECONDS,
        )
    except httpx.HTTPError as exc:
        print(f"error: failed to reach control plane at {base}: {exc}", file=sys.stderr)
        return 1
    if resp.status_code == 404:
        print(
            "error: the one-time code was not recognized. It may have"
            " expired or been used already. Re-run `qiita-admin login`.",
            file=sys.stderr,
        )
        return 1
    if resp.status_code != 200:
        print(
            f"error: cli-exchange returned http {resp.status_code}: {resp.text}",
            file=sys.stderr,
        )
        return 1

    body = resp.json()
    pat = body.get("token")
    if not isinstance(pat, str) or not pat:
        print("error: cli-exchange response missing 'token' field", file=sys.stderr)
        return 1

    _write_token(token_file, pat)

    # Report identity via /auth/whoami so the operator sees who they're
    # logged in as without having to chase a separate command.
    try:
        me = _whoami(base, pat)
    except httpx.HTTPError as exc:
        # Token mint succeeded; whoami failure is not fatal.
        print(f"warning: token saved to {token_file} but whoami failed: {exc}", file=sys.stderr)
        return 0

    print(f"Logged in. Token saved to {token_file} (mode 0600).", file=sys.stderr)
    print(json.dumps(me, indent=2))
    return 0


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
        return _run_http_subcommand(lambda t: _whoami(args.base_url, t))

    if args.cmd == "token":
        if args.token_cmd == "revoke-all":
            return _run_http_subcommand(
                lambda t: _token_revoke_all(args.base_url, t, args.principal_idx)
            )

    if args.cmd == "login":
        return _do_login(base_url=args.base_url, token_file=args.token_file)

    parser.error(f"unknown command: {args.cmd}")
    return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
