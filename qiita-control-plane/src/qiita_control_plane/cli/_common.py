"""Shared CLI helpers used by both `qiita-admin` and the end-user `qiita`
CLI: PAT file I/O, the LoginRocket loopback flow, the generic
token-read + HTTP + JSON-print runner, and `/auth/whoami`.

Package-private (the filename's leading underscore signals "import only
from inside qiita_control_plane.cli"). Names do not carry the prefix —
within the package this module is the public surface.
"""

from __future__ import annotations

import argparse
import http.server
import json
import os
import sys
import threading
import urllib.parse
import webbrowser
from collections.abc import Callable
from pathlib import Path

import httpx
from qiita_common.api_paths import LOOPBACK_HOST
from qiita_common.auth_constants import API_PREFIX

# Environment-variable names and CLI defaults are CLI conventions (not part of
# the wire protocol — those live in qiita_common.auth_constants). Keep them
# centralized here so admin/user CLIs and the tests reference one string.
QIITA_TOKEN_ENV = "QIITA_TOKEN"
QIITA_CONTROL_PLANE_URL_ENV = "QIITA_CONTROL_PLANE_URL"
DEFAULT_CONTROL_PLANE_URL = "http://localhost:8080"

TOKEN_FILE_DEFAULT = Path.home() / ".qiita" / "token"

# How long to wait for the AuthRocket round-trip + browser bounce. 5 minutes
# is generous; longer values mostly hide bugs (browser crashed, user gave up).
LOGIN_WAIT_TIMEOUT_SECONDS = 300

# HTTP timeout for CLI-driven control-plane calls. Generous enough to tolerate
# transient network blips without papering over a hung server.
CLI_HTTP_TIMEOUT_SECONDS = 10

# HTML rendered to the browser at the loopback after the handoff redirect
# delivers the ot_code. Friendly "you can close this tab now" message. The
# page must NOT include any script that touches the URL — we don't want
# accidental cross-site reads of the ot_code from extensions etc. The
# server consumes the code immediately upon receipt, so even if the URL
# leaks the code is dead within the cli_login_code_ttl window.
LOOPBACK_DONE_HTML = b"""\
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


# ---------------------------------------------------------------------------
# argparse helpers
# ---------------------------------------------------------------------------


def add_base_url_arg(parser: argparse.ArgumentParser) -> None:
    """Add the standard --base-url flag. Default is $QIITA_CONTROL_PLANE_URL or
    DEFAULT_CONTROL_PLANE_URL."""
    parser.add_argument(
        "--base-url",
        default=os.environ.get(QIITA_CONTROL_PLANE_URL_ENV, DEFAULT_CONTROL_PLANE_URL),
        help=(
            f"Control-plane base URL (default from ${QIITA_CONTROL_PLANE_URL_ENV} or"
            f" {DEFAULT_CONTROL_PLANE_URL})"
        ),
    )


def add_token_file_arg(parser: argparse.ArgumentParser) -> None:
    """Add the standard --token-file flag. Default is TOKEN_FILE_DEFAULT
    (~/.qiita/token)."""
    parser.add_argument(
        "--token-file",
        type=Path,
        default=TOKEN_FILE_DEFAULT,
        help=f"Where to write the PAT (default {TOKEN_FILE_DEFAULT})",
    )


# ---------------------------------------------------------------------------
# Token I/O
# ---------------------------------------------------------------------------


def read_token(token_file: Path | None = None) -> str:
    """Read PAT from $QIITA_TOKEN env or from a token file (default ~/.qiita/token).
    Raises with a clear actionable message if neither is set."""
    env = os.environ.get(QIITA_TOKEN_ENV)
    if env:
        return env.strip()
    path = token_file or TOKEN_FILE_DEFAULT
    if path.is_file():
        return path.read_text().strip()
    raise RuntimeError(
        f"no PAT found — set ${QIITA_TOKEN_ENV} or write a token to {path}"
        f" (use POST {API_PREFIX}/auth/pat to mint one)"
    )


def write_token(path: Path, plaintext: str) -> None:
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


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def whoami(base_url: str, token: str) -> dict:
    resp = httpx.get(
        f"{base_url.rstrip('/')}{API_PREFIX}/auth/whoami",
        headers={"Authorization": f"Bearer {token}"},
        timeout=CLI_HTTP_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    return resp.json()


def run_http_subcommand(call: Callable[[str], dict]) -> int:
    """Token-read + httpx invoke + json print, common to every HTTP subcommand.

    `call` accepts a PAT string and returns the parsed JSON body. Any
    RuntimeError from token-read or HTTPStatusError from the request is
    converted to a stderr message + exit code 1.
    """
    try:
        token = read_token()
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
# Loopback login flow
# ---------------------------------------------------------------------------


class LoopbackResult:
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


def loopback_handler_factory(result: LoopbackResult):
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
            self.send_header("Content-Length", str(len(LOOPBACK_DONE_HTML)))
            self.end_headers()
            self.wfile.write(LOOPBACK_DONE_HTML)

        def log_message(self, *args, **kwargs):
            # Stay quiet; the CLI prints its own status.
            pass

    return Handler


def bind_loopback(*, preferred_ports: tuple[int, ...] = ()) -> tuple[http.server.HTTPServer, int]:
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
            srv = http.server.HTTPServer((LOOPBACK_HOST, port), http.server.BaseHTTPRequestHandler)
            return srv, port
        except OSError:
            continue
    srv = http.server.HTTPServer((LOOPBACK_HOST, 0), http.server.BaseHTTPRequestHandler)
    return srv, srv.server_address[1]


def do_login(*, base_url: str, token_file: Path, cli_command: str) -> int:
    """Drive the LoginRocket Web flow end-to-end.

    `cli_command` is the verbatim "re-run this" string embedded in error
    messages (e.g. `"qiita-admin login"` or `"qiita login"`); both
    consumers of this helper pass their own.

    Steps:
      1. Bind localhost loopback HTTP server.
      2. Open browser to {base_url}/api/v1/auth/login?cli=1&port=N.
      3. Wait for the handoff to redirect back with `?ot_code=<value>`.
      4. POST the ot_code to /api/v1/auth/cli-exchange, receive the PAT.
      5. Write PAT to `token_file`, print whoami summary.
    """
    base = base_url.rstrip("/")
    result = LoopbackResult()
    server, port = bind_loopback()
    server.RequestHandlerClass = loopback_handler_factory(result)

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
        if not result.event.wait(timeout=LOGIN_WAIT_TIMEOUT_SECONDS):
            print(
                f"error: timed out after {LOGIN_WAIT_TIMEOUT_SECONDS}s waiting for the"
                f" browser callback. Re-run `{cli_command}`. If the browser"
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
            timeout=CLI_HTTP_TIMEOUT_SECONDS,
        )
    except httpx.HTTPError as exc:
        print(f"error: failed to reach control plane at {base}: {exc}", file=sys.stderr)
        return 1
    if resp.status_code == 404:
        print(
            "error: the one-time code was not recognized. It may have"
            f" expired or been used already. Re-run `{cli_command}`.",
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

    write_token(token_file, pat)

    # Report identity via /auth/whoami so the operator sees who they're
    # logged in as without having to chase a separate command.
    try:
        me = whoami(base, pat)
    except httpx.HTTPError as exc:
        # Token mint succeeded; whoami failure is not fatal.
        print(f"warning: token saved to {token_file} but whoami failed: {exc}", file=sys.stderr)
        return 0

    print(f"Logged in. Token saved to {token_file} (mode 0600).", file=sys.stderr)
    print(json.dumps(me, indent=2))
    return 0
